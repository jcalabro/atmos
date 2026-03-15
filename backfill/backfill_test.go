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

func TestEngine_BatchShuffle(t *testing.T) {
	t.Parallel()

	numRepos := 50
	repos := make(map[string][]byte)
	var dids []string
	for i := range numRepos {
		did := fmt.Sprintf("did:plc:repo%04d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 1)
	}

	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var mu gosync.Mutex
	var order []string

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(1), // Single worker to observe dispatch order.
		BatchSize:  gt.Some(numRepos),
		Handler: backfill.HandlerFunc(func(_ context.Context, did atmos.DID, _ sync.Record) error {
			mu.Lock()
			order = append(order, string(did))
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(numRepos), engine.Completed())

	// The shuffled order should differ from enumeration order.
	// Probability of identical order is 1/50! ≈ 0.
	mu.Lock()
	defer mu.Unlock()
	assert.NotEqual(t, dids, order, "shuffled order should differ from enumeration order")
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

// flakyTestServer returns a transient error for the first N requests to
// getRepo for a given DID, then serves the real CAR data.
type flakyTestServer struct {
	srv       *httptest.Server
	repos     map[string][]byte
	allDIDs   []string
	failCode  int // HTTP status to return for transient failures
	failCount int // how many times to fail before succeeding
	attempts  map[string]*atomic.Int32
}

func newFlakyTestServer(t *testing.T, repos map[string][]byte, dids []string, failCode, failCount int) *flakyTestServer {
	t.Helper()
	fs := &flakyTestServer{
		repos:     repos,
		allDIDs:   dids,
		failCode:  failCode,
		failCount: failCount,
		attempts:  make(map[string]*atomic.Int32),
	}
	for _, d := range dids {
		fs.attempts[d] = &atomic.Int32{}
	}
	fs.srv = httptest.NewServer(http.HandlerFunc(fs.handle))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *flakyTestServer) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/xrpc/com.atproto.sync.listRepos":
		cursor := r.URL.Query().Get("cursor")
		start := 0
		if cursor != "" {
			for i, d := range fs.allDIDs {
				if d == cursor {
					start = i + 1
					break
				}
			}
		}
		end := start + 3
		if end > len(fs.allDIDs) {
			end = len(fs.allDIDs)
		}
		page := listPage{}
		for _, d := range fs.allDIDs[start:end] {
			page.Repos = append(page.Repos, listRepo{
				DID: d, Head: "bafytest", Rev: "rev1", Active: true,
			})
		}
		if end < len(fs.allDIDs) {
			page.Cursor = fs.allDIDs[end-1]
		}
		_ = json.NewEncoder(w).Encode(page)

	case "/xrpc/com.atproto.sync.getRepo":
		did := r.URL.Query().Get("did")
		counter, ok := fs.attempts[did]
		if !ok {
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RepoNotFound"})
			return
		}

		n := int(counter.Add(1))
		if n <= fs.failCount {
			w.WriteHeader(fs.failCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "TransientError"})
			return
		}

		carData, ok := fs.repos[did]
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

func TestEngine_RetryTransientError(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{
		"did:plc:retry": buildTestRepoCAR(t, "did:plc:retry", 2),
	}
	dids := []string{"did:plc:retry"}

	// Fail with 503 once, then succeed.
	fs := newFlakyTestServer(t, repos, dids, 503, 1)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var mu gosync.Mutex
	recordCount := 0

	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(3),
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(10 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			mu.Lock()
			recordCount++
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(1), engine.Completed())

	mu.Lock()
	assert.Equal(t, 2, recordCount)
	mu.Unlock()
}

func TestEngine_RetryExhausted(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{
		"did:plc:always503": buildTestRepoCAR(t, "did:plc:always503", 2),
	}
	dids := []string{"did:plc:always503"}

	// Always fail with 503 (failCount > maxRetries).
	fs := newFlakyTestServer(t, repos, dids, 503, 100)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var errorCalled atomic.Bool

	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(2),
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(10 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			return nil
		}),
		OnError: gt.Some(func(_ atmos.DID, _ error) {
			errorCalled.Store(true)
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(0), engine.Completed())
	assert.True(t, errorCalled.Load(), "OnError should be called after retries exhausted")

	// Should have attempted 1 initial + 2 retries = 3 total.
	assert.Equal(t, int32(3), fs.attempts["did:plc:always503"].Load())
}

func TestEngine_NoPermanentRetry(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{
		"did:plc:bad400": buildTestRepoCAR(t, "did:plc:bad400", 2),
	}
	dids := []string{"did:plc:bad400"}

	// Fail with 400 (permanent error).
	fs := newFlakyTestServer(t, repos, dids, 400, 100)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var errorCalled atomic.Bool

	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(5),
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(10 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			return nil
		}),
		OnError: gt.Some(func(_ atmos.DID, _ error) {
			errorCalled.Store(true)
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(0), engine.Completed())
	assert.True(t, errorCalled.Load(), "OnError should be called immediately for permanent errors")

	// Should have only attempted once — no retries for 400.
	assert.Equal(t, int32(1), fs.attempts["did:plc:bad400"].Load())
}

func TestEngine_RetryDisabled(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{
		"did:plc:noretry": buildTestRepoCAR(t, "did:plc:noretry", 2),
	}
	dids := []string{"did:plc:noretry"}

	// Always fail with 503, but retries disabled.
	fs := newFlakyTestServer(t, repos, dids, 503, 100)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var errorCalled atomic.Bool

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(0),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ sync.Record) error {
			return nil
		}),
		OnError: gt.Some(func(_ atmos.DID, _ error) {
			errorCalled.Store(true)
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	assert.Equal(t, int64(0), engine.Completed())
	assert.True(t, errorCalled.Load())

	// Exactly 1 attempt, no retries.
	assert.Equal(t, int32(1), fs.attempts["did:plc:noretry"].Load())
}
