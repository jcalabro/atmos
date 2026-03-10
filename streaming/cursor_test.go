// Some tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileCursorStore_RoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cursor")
	store := NewFileCursorStore(path)

	// No file yet — returns 0.
	cur, err := store.LoadCursor(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), cur)

	// Save and load.
	require.NoError(t, store.SaveCursor(context.Background(), 42))
	cur, err = store.LoadCursor(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(42), cur)

	// Overwrite.
	require.NoError(t, store.SaveCursor(context.Background(), 999))
	cur, err = store.LoadCursor(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(999), cur)
}

func TestFileCursorStore_ConcurrentSafety(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cursor")
	store := NewFileCursorStore(path)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.SaveCursor(ctx, int64(i*100))
			_, _ = store.LoadCursor(ctx)
		}()
	}
	wg.Wait()

	// Should have a valid value at the end.
	cur, err := store.LoadCursor(ctx)
	require.NoError(t, err)
	assert.True(t, cur >= 0)
}

func TestFileCursorStore_AtomicWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "cursor")
	store := NewFileCursorStore(path)

	require.NoError(t, store.SaveCursor(context.Background(), 123))

	// Verify no temp files left behind.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "cursor", entries[0].Name())
}

type mockCursorStore struct {
	mu        sync.Mutex
	cursor    int64
	saveCount int
}

func (m *mockCursorStore) LoadCursor(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursor, nil
}

func (m *mockCursorStore) SaveCursor(_ context.Context, cursor int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursor = cursor
	m.saveCount++
	return nil
}

func TestCursorAutoLoad(t *testing.T) {
	t.Parallel()
	store := &mockCursorStore{cursor: 42}

	client, err := NewClient(Options{
		URL:         "wss://example.com",
		CursorStore: gt.Some[CursorStore](store),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(42), client.Cursor())
}

func TestCursorExplicitOverridesStore(t *testing.T) {
	t.Parallel()
	store := &mockCursorStore{cursor: 42}

	client, err := NewClient(Options{
		URL:         "wss://example.com",
		Cursor:      gt.Some(int64(99)),
		CursorStore: gt.Some[CursorStore](store),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(99), client.Cursor())
}

func TestCursorCheckpointInterval(t *testing.T) {
	t.Parallel()

	store := &mockCursorStore{}
	interval := int64(5)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= 12; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewClient(Options{
		URL:            wsURL(srv),
		CursorStore:    gt.Some[CursorStore](store),
		CursorInterval: gt.Some(interval),
	})
	require.NoError(t, err)

	var count int
	for _, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			continue
		}
		count++
		if count == 12 {
			cancel()
		}
	}

	// 12 events with interval 5 → saves at event 5 and 10 = 2 saves from readLoop.
	store.mu.Lock()
	saveCount := store.saveCount
	store.mu.Unlock()
	assert.Equal(t, 2, saveCount)
}

func TestCursorSaveOnClose(t *testing.T) {
	t.Parallel()

	store := &mockCursorStore{}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(7, "did:plc:test")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use a large interval so readLoop doesn't save.
	client, err := NewClient(Options{
		URL:            wsURL(srv),
		CursorStore:    gt.Some[CursorStore](store),
		CursorInterval: gt.Some(int64(1000)),
	})
	require.NoError(t, err)

	for _, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			continue
		}
		cancel()
	}

	// No saves during readLoop due to large interval.
	store.mu.Lock()
	assert.Equal(t, 0, store.saveCount)
	store.mu.Unlock()

	// Close should trigger final save (websocket may already be closed).
	_ = client.Close()

	store.mu.Lock()
	assert.Equal(t, 1, store.saveCount)
	assert.Equal(t, int64(7), store.cursor)
	store.mu.Unlock()
}
