package backfill_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestRepoCAR(t *testing.T, did string, n int) []byte {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &repo.Repo{
		DID:   atmos.DID(did),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	for i := range n {
		record := map[string]any{"text": fmt.Sprintf("record %d", i)}
		require.NoError(t, r.Create("app.bsky.feed.post", fmt.Sprintf("rec%d", i), record))
	}
	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))
	return buf.Bytes()
}

type testServer struct {
	srv     *httptest.Server
	repos   map[string][]byte // did -> CAR bytes
	allDIDs []string
}

func newTestServer(t *testing.T, repos map[string][]byte, allDIDs []string) *testServer {
	t.Helper()
	ts := &testServer{repos: repos, allDIDs: allDIDs}
	ts.srv = httptest.NewServer(http.HandlerFunc(ts.handle))
	t.Cleanup(ts.srv.Close)
	return ts
}

type listPage struct {
	Cursor string     `json:"cursor,omitempty"`
	Repos  []listRepo `json:"repos"`
}

type listRepo struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

func (ts *testServer) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/xrpc/com.atproto.sync.listRepos":
		cursor := r.URL.Query().Get("cursor")
		start := 0
		if cursor != "" {
			for i, d := range ts.allDIDs {
				if d == cursor {
					start = i + 1
					break
				}
			}
		}
		end := start + 3
		if end > len(ts.allDIDs) {
			end = len(ts.allDIDs)
		}

		page := listPage{}
		for _, d := range ts.allDIDs[start:end] {
			page.Repos = append(page.Repos, listRepo{
				DID:    d,
				Head:   "bafytest",
				Rev:    "rev1",
				Active: true,
			})
		}
		if end < len(ts.allDIDs) {
			page.Cursor = ts.allDIDs[end-1]
		}
		_ = json.NewEncoder(w).Encode(page)

	case "/xrpc/com.atproto.sync.getRepo":
		did := r.URL.Query().Get("did")
		carData, ok := ts.repos[did]
		if !ok {
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RepoNotFound"})
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carData)

	default:
		w.WriteHeader(404)
	}
}

func TestEngine_BasicBackfill(t *testing.T) {
	t.Parallel()

	numRepos := 10
	repos := make(map[string][]byte)
	var dids []string
	for i := range numRepos {
		did := fmt.Sprintf("did:plc:repo%d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 3)
	}

	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var mu gosync.Mutex
	recordCount := make(map[string]int)

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(3),
		Handler: backfill.HandlerFunc(func(_ context.Context, did atmos.DID, rec sync.Record) error {
			mu.Lock()
			recordCount[string(did)]++
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(numRepos), engine.Completed())

	mu.Lock()
	defer mu.Unlock()
	for _, did := range dids {
		assert.Equal(t, 3, recordCount[did], "did=%s", did)
	}
}

// memCheckpoint is a simple in-memory checkpoint for testing.
type memCheckpoint struct {
	mu        gosync.Mutex
	cursor    string
	completed map[string]string // did -> rev
}

func newMemCheckpoint() *memCheckpoint {
	return &memCheckpoint{completed: make(map[string]string)}
}

func (m *memCheckpoint) LoadCursor(_ context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursor, nil
}

func (m *memCheckpoint) SaveCursor(_ context.Context, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursor = cursor
	return nil
}

func (m *memCheckpoint) IsComplete(_ context.Context, did atmos.DID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.completed[string(did)]
	return ok, nil
}

func (m *memCheckpoint) MarkComplete(_ context.Context, did atmos.DID, rev string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[string(did)] = rev
	return nil
}

func TestEngine_Checkpoint(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{
		"did:plc:aaa": buildTestRepoCAR(t, "did:plc:aaa", 2),
		"did:plc:bbb": buildTestRepoCAR(t, "did:plc:bbb", 2),
		"did:plc:ccc": buildTestRepoCAR(t, "did:plc:ccc", 2),
	}
	dids := []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	cp := newMemCheckpoint()
	// Pre-mark one as complete.
	require.NoError(t, cp.MarkComplete(context.Background(), "did:plc:aaa", "rev1"))

	var mu gosync.Mutex
	seen := make(map[string]bool)

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(1),
		Checkpoint: gt.Some[backfill.Checkpoint](cp),
		Handler: backfill.HandlerFunc(func(_ context.Context, did atmos.DID, _ sync.Record) error {
			mu.Lock()
			seen[string(did)] = true
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	assert.False(t, seen["did:plc:aaa"], "already-complete repo should be skipped")
	assert.True(t, seen["did:plc:bbb"])
	assert.True(t, seen["did:plc:ccc"])
}

func TestEngine_CollectionFilter(t *testing.T) {
	t.Parallel()

	// Build a repo with records in two collections.
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	store := mst.NewMemBlockStore()
	r := &repo.Repo{
		DID:   atmos.DID("did:plc:test"),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	require.NoError(t, r.Create("app.bsky.feed.post", "post1", map[string]any{"text": "hello"}))
	require.NoError(t, r.Create("app.bsky.feed.like", "like1", map[string]any{"subject": "x"}))
	require.NoError(t, r.Create("app.bsky.feed.post", "post2", map[string]any{"text": "world"}))

	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))

	repos := map[string][]byte{"did:plc:test": buf.Bytes()}
	ts := newTestServer(t, repos, []string{"did:plc:test"})

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var mu gosync.Mutex
	var records []sync.Record

	engine := backfill.NewEngine(backfill.Options{
		SyncClient:  sc,
		Workers:     gt.Some(1),
		Collections: gt.Some([]string{"app.bsky.feed.post"}),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, rec sync.Record) error {
			mu.Lock()
			records = append(records, rec)
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, records, 2)
	for _, rec := range records {
		assert.Equal(t, "app.bsky.feed.post", rec.Collection)
	}
}

func TestEngine_ErrorHandling(t *testing.T) {
	t.Parallel()

	// One valid repo, one that will 404.
	repos := map[string][]byte{
		"did:plc:good": buildTestRepoCAR(t, "did:plc:good", 2),
		// "did:plc:bad" not in map -> 404
	}
	dids := []string{"did:plc:good", "did:plc:bad"}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var errorDIDs []atmos.DID
	var mu gosync.Mutex

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			return nil
		}),
		OnError: gt.Some(func(did atmos.DID, _ error) {
			mu.Lock()
			errorDIDs = append(errorDIDs, did)
			mu.Unlock()
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	// Good repo processed, bad repo errored.
	assert.GreaterOrEqual(t, engine.Completed(), int64(1))
	mu.Lock()
	assert.NotEmpty(t, errorDIDs)
	mu.Unlock()
}

func TestEngine_Cancellation(t *testing.T) {
	t.Parallel()

	// Create many repos so cancellation hits mid-run.
	repos := make(map[string][]byte)
	var dids []string
	for i := range 100 {
		did := fmt.Sprintf("did:plc:repo%d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 1)
	}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	ctx, cancel := context.WithCancel(context.Background())
	var once gosync.Once

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(2),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			once.Do(func() {
				// Cancel after first record.
				cancel()
			})
			return nil
		}),
	})

	err := engine.Run(ctx)
	// Should return ctx error or nil (depends on timing).
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
	// Should not have processed all 100.
	assert.Less(t, engine.Completed(), int64(100))
}

func TestEngine_Concurrency(t *testing.T) {
	t.Parallel()

	repos := make(map[string][]byte)
	var dids []string
	for i := range 20 {
		did := fmt.Sprintf("did:plc:repo%d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 5)
	}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var maxConcurrent atomic.Int32
	var current atomic.Int32

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(10),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			current.Add(-1)
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(20), engine.Completed())
	// Should have seen some concurrency.
	assert.Greater(t, maxConcurrent.Load(), int32(1))
}
