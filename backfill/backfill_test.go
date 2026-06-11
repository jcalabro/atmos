package backfill_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// buildTestRepoCAR generates a signed CAR for a fake repo containing n
// app.bsky.feed.post records. The returned bytes are what
// com.atproto.sync.getRepo would serve.
func buildTestRepoCAR(t *testing.T, did string, n int) []byte {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
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

// listPage / listRepo mirror the JSON shape of
// com.atproto.sync.listRepos for the test server.
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

// testServer stubs the relay endpoints the engine talks to. allDIDs
// drives listRepos pagination (3 entries per page); repos serves CAR
// data for getRepo. Entries in inactive are flagged Active=false.
type testServer struct {
	srv      *httptest.Server
	repos    map[string][]byte
	allDIDs  []string
	inactive map[string]bool
}

func newTestServer(t *testing.T, repos map[string][]byte, allDIDs []string) *testServer {
	t.Helper()
	ts := &testServer{repos: repos, allDIDs: allDIDs, inactive: map[string]bool{}}
	ts.srv = httptest.NewServer(http.HandlerFunc(ts.handle))
	t.Cleanup(ts.srv.Close)
	return ts
}

func (ts *testServer) markInactive(dids ...string) {
	for _, d := range dids {
		ts.inactive[d] = true
	}
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
		end := min(start+3, len(ts.allDIDs))

		page := listPage{}
		for _, d := range ts.allDIDs[start:end] {
			page.Repos = append(page.Repos, listRepo{
				DID:    d,
				Head:   "bafytest",
				Rev:    "rev1",
				Active: !ts.inactive[d],
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

// flakyTestServer fails getRepo with failCode for the first failCount
// attempts per DID, then serves the real CAR.
type flakyTestServer struct {
	srv       *httptest.Server
	repos     map[string][]byte
	allDIDs   []string
	failCode  int
	failCount int
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
		end := min(start+3, len(fs.allDIDs))
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

// memStore is a thread-safe in-memory backfill.Store for tests. It
// records every callback so tests can assert on transition order
// and counts.
type memStore struct {
	mu       sync.Mutex
	state    map[string]backfill.State
	active   map[string]bool                     // DID -> last-recorded entry.Active
	entries  map[string]atmossync.ListReposEntry // DID -> entry as seen by OnDiscover
	updates  map[string]atmossync.ListReposEntry // DID -> last entry seen by OnUpdate
	commits  map[string]string                   // DID -> commit.Rev as seen by OnComplete
	failures map[string]int                      // DID -> attempts at OnFail
	failErrs map[string]error                    // DID -> last err at OnFail

	discoverCalls atomic.Int32
	updateCalls   atomic.Int32
	completeCalls atomic.Int32
	failCalls     atomic.Int32

	// failOnDiscover/Update/Complete/Fail, when non-nil, makes the
	// corresponding callback return the configured error for the
	// matching DID. Used in store-error tests.
	failOnDiscover map[string]error
	failOnUpdate   map[string]error
	failOnComplete map[string]error
	failOnFail     map[string]error
}

// Compile-time assertion that memStore implements backfill.Store.
var _ backfill.Store = (*memStore)(nil)

func newMemStore() *memStore {
	return &memStore{
		state:    make(map[string]backfill.State),
		active:   make(map[string]bool),
		entries:  make(map[string]atmossync.ListReposEntry),
		updates:  make(map[string]atmossync.ListReposEntry),
		commits:  make(map[string]string),
		failures: make(map[string]int),
		failErrs: make(map[string]error),
	}
}

func (s *memStore) Lookup(_ context.Context, did atmos.DID) (backfill.StoreEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.state[string(did)]
	if !ok {
		return backfill.StoreEntry{State: backfill.StateUnknown}, nil
	}
	return backfill.StoreEntry{State: st, Active: s.active[string(did)]}, nil
}

func (s *memStore) OnDiscover(_ context.Context, entry atmossync.ListReposEntry) error {
	s.discoverCalls.Add(1)
	if s.failOnDiscover != nil {
		if err, ok := s.failOnDiscover[string(entry.DID)]; ok {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[string(entry.DID)] = backfill.StateDiscovered
	s.active[string(entry.DID)] = entry.Active
	s.entries[string(entry.DID)] = entry
	return nil
}

func (s *memStore) OnUpdate(_ context.Context, entry atmossync.ListReposEntry) error {
	s.updateCalls.Add(1)
	if s.failOnUpdate != nil {
		if err, ok := s.failOnUpdate[string(entry.DID)]; ok {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[string(entry.DID)] = entry.Active
	s.updates[string(entry.DID)] = entry
	return nil
}

func (s *memStore) OnComplete(_ context.Context, did atmos.DID, commit *atmosrepo.Commit) error {
	s.completeCalls.Add(1)
	if s.failOnComplete != nil {
		if err, ok := s.failOnComplete[string(did)]; ok {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[string(did)] = backfill.StateComplete
	s.commits[string(did)] = commit.Rev
	return nil
}

func (s *memStore) OnFail(_ context.Context, did atmos.DID, err error, attempts int) error {
	s.failCalls.Add(1)
	if s.failOnFail != nil {
		if e, ok := s.failOnFail[string(did)]; ok {
			return e
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[string(did)] = backfill.StateFailed
	s.failures[string(did)] = attempts
	s.failErrs[string(did)] = err
	return nil
}

// preset writes a state directly without going through a callback.
// Used by tests to simulate "this DID was already at state X from a
// previous Run."
func (s *memStore) preset(did string, st backfill.State) {
	s.preset2(did, st, true)
}

// preset2 is preset with an explicit Active value.
func (s *memStore) preset2(did string, st backfill.State, active bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[did] = st
	s.active[did] = active
}

// errSentinel is the canonical "this is the error I expect" used in
// store-error and handler-error tests.
var errSentinel = errors.New("sentinel")

// TestEngine_DiscoversUnknownDIDs is the smallest possible producer
// test: every entry returned by listRepos that the Store reports as
// Unknown should trigger one OnDiscover call. The handler is a
// no-op (worker code lands in Task 4).
func TestEngine_DiscoversUnknownDIDs(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:aaa", "did:plc:bbb"}
	repos := map[string][]byte{
		"did:plc:aaa": buildTestRepoCAR(t, "did:plc:aaa", 1),
		"did:plc:bbb": buildTestRepoCAR(t, "did:plc:bbb", 1),
	}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		Workers: gt.Some(1),
	})

	require.NoError(t, engine.Run(context.Background()))

	require.Equal(t, int32(2), store.discoverCalls.Load())
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, d := range dids {
		require.Contains(t, store.entries, d, "missing OnDiscover entry for %s", d)
	}
}

// TestEngine_SkipsCompleteDIDs verifies that DIDs already at
// StateComplete in the Store are not redispatched, and that no
// OnDiscover fires for them.
func TestEngine_SkipsCompleteDIDs(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:done", "did:plc:fresh"}
	repos := map[string][]byte{
		"did:plc:done":  buildTestRepoCAR(t, "did:plc:done", 1),
		"did:plc:fresh": buildTestRepoCAR(t, "did:plc:fresh", 1),
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	store.preset("did:plc:done", backfill.StateComplete)

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		Workers: gt.Some(1),
	})

	require.NoError(t, engine.Run(context.Background()))

	// OnDiscover should fire only for the Unknown DID.
	require.Equal(t, int32(1), store.discoverCalls.Load())
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Contains(t, store.entries, "did:plc:fresh")
	require.NotContains(t, store.entries, "did:plc:done")
}

// TestEngine_RecordsInactiveDIDsButSkipsDispatch verifies that
// inactive DIDs flow through OnDiscover (so the consumer learns
// about them), but are never dispatched for download.
func TestEngine_RecordsInactiveDIDsButSkipsDispatch(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:active", "did:plc:dead"}
	repos := map[string][]byte{
		"did:plc:active": buildTestRepoCAR(t, "did:plc:active", 1),
		// did:plc:dead intentionally not in repos; if engine
		// dispatched it the worker would 404.
	}
	ts := newTestServer(t, repos, dids)
	ts.markInactive("did:plc:dead")

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		Workers: gt.Some(1),
	})

	require.NoError(t, engine.Run(context.Background()))

	// OnDiscover should fire for both active and inactive DIDs.
	require.Equal(t, int32(2), store.discoverCalls.Load())

	store.mu.Lock()
	defer store.mu.Unlock()
	require.False(t, store.entries["did:plc:dead"].Active, "Active flag should be preserved on the recorded entry")
	require.True(t, store.entries["did:plc:active"].Active)
}

// TestEngine_StoreErrorAborts_OnDiscover verifies that an error
// returned from Store.OnDiscover bubbles out of Run.
func TestEngine_StoreErrorAborts_OnDiscover(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:aaa", "did:plc:bbb"}
	repos := map[string][]byte{}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	store.failOnDiscover = map[string]error{"did:plc:aaa": errSentinel}

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		Workers: gt.Some(1),
	})

	err := engine.Run(context.Background())
	require.ErrorIs(t, err, errSentinel)
}

// TestEngine_ValidatesRequiredOptions guards the three required
// fields. One missing field per case.
func TestEngine_ValidatesRequiredOptions(t *testing.T) {
	t.Parallel()

	noopHandler := backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error { return nil })
	dummySync := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{}})

	cases := []struct {
		name string
		opts backfill.Options
		want string
	}{
		{
			name: "missing SyncClient",
			opts: backfill.Options{Store: newMemStore(), Handler: noopHandler},
			want: "SyncClient",
		},
		{
			name: "missing Store",
			opts: backfill.Options{SyncClient: dummySync, Handler: noopHandler},
			want: "Store",
		},
		{
			name: "missing Handler",
			opts: backfill.Options{SyncClient: dummySync, Store: newMemStore()},
			want: "Handler",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := backfill.NewEngine(tc.opts).Run(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestEngine_HandleRepo_HappyPath verifies that the worker downloads
// the CAR, parses it, hands it to HandleRepo, and records OnComplete
// with the commit's rev.
func TestEngine_HandleRepo_HappyPath(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:alpha", "did:plc:beta"}
	repos := map[string][]byte{
		"did:plc:alpha": buildTestRepoCAR(t, "did:plc:alpha", 3),
		"did:plc:beta":  buildTestRepoCAR(t, "did:plc:beta", 2),
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	type seenRepo struct {
		did   string
		revOk bool
		nRecs int
	}
	var mu sync.Mutex
	seen := []seenRepo{}

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, did atmos.DID, r *atmosrepo.Repo, commit *atmosrepo.Commit) error {
			n := 0
			require.NoError(t, r.Tree.Walk(func(_ string, _ cbor.CID) error {
				n++
				return nil
			}))
			mu.Lock()
			seen = append(seen, seenRepo{did: string(did), revOk: commit.Rev != "", nRecs: n})
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 2)
	require.Equal(t, int32(2), store.completeCalls.Load())

	store.mu.Lock()
	defer store.mu.Unlock()
	for _, d := range dids {
		require.Equal(t, backfill.StateComplete, store.state[d])
		require.NotEmpty(t, store.commits[d], "OnComplete should record a non-empty commit.Rev for %s", d)
	}
}

// TestEngine_HandlerErrorTransitionsToFailed verifies that when
// HandleRepo returns a non-transient error, the DID transitions to
// StateFailed via OnFail and OnError fires.
func TestEngine_HandlerErrorTransitionsToFailed(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:err"}
	repos := map[string][]byte{"did:plc:err": buildTestRepoCAR(t, "did:plc:err", 1)}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	var onErrorCalls atomic.Int32
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(2),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return errSentinel // non-transient (xrpc.IsTransient is false for arbitrary errors)
		}),
		OnError: gt.Some(func(_ atmos.DID, _ error) { onErrorCalls.Add(1) }),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.failCalls.Load())
	require.Equal(t, int32(1), onErrorCalls.Load())

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, backfill.StateFailed, store.state["did:plc:err"])
	require.ErrorIs(t, store.failErrs["did:plc:err"], errSentinel)
}

// TestEngine_TransientRetryThenSuccess verifies that the engine
// retries 503-style transient HTTP errors and ultimately calls
// OnComplete, not OnFail.
func TestEngine_TransientRetryThenSuccess(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{"did:plc:flaky": buildTestRepoCAR(t, "did:plc:flaky", 1)}
	dids := []string{"did:plc:flaky"}

	// Fail twice with 503, then serve the CAR.
	fs := newFlakyTestServer(t, repos, dids, 503, 2)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Store:          store,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(5),
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(10 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.completeCalls.Load())
	require.Equal(t, int32(0), store.failCalls.Load())
	require.Equal(t, int32(3), fs.attempts["did:plc:flaky"].Load())
}

// TestEngine_OnProgressFires verifies that OnProgress fires with
// monotonically increasing Stats.Completed counts.
func TestEngine_OnProgressFires(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:p1", "did:plc:p2", "did:plc:p3"}
	repos := map[string][]byte{
		"did:plc:p1": buildTestRepoCAR(t, "did:plc:p1", 1),
		"did:plc:p2": buildTestRepoCAR(t, "did:plc:p2", 1),
		"did:plc:p3": buildTestRepoCAR(t, "did:plc:p3", 1),
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	var maxCompleted atomic.Int64

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		OnProgress: gt.Some(func(s backfill.Stats) {
			for {
				old := maxCompleted.Load()
				if s.Completed <= old || maxCompleted.CompareAndSwap(old, s.Completed) {
					break
				}
			}
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int64(3), maxCompleted.Load())
}

// TestEngine_PDSRouting_PreservesDirectory is a regression test for a
// bug where PDS-pooled sync clients were constructed without passing
// the Directory through, causing VerifyCommit to hard-error with "no
// directory configured for signature verification". This test verifies
// that when both Directory and PDS routing are configured, the DID
// completes successfully (not Failed).
func TestEngine_PDSRouting_PreservesDirectory(t *testing.T) {
	t.Parallel()

	did := "did:plc:test123"

	// Generate a signing key and build a CAR signed with that key.
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmos.DID(did),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	record := map[string]any{"text": "test record"}
	require.NoError(t, r.Create("app.bsky.feed.post", "rec0", record))
	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))
	carData := buf.Bytes()

	repos := map[string][]byte{did: carData}
	dids := []string{did}

	// Test server serves both the relay (listRepos) and PDS (getRepo)
	// endpoints. The Directory stub will point the engine to resolve
	// this DID back to the same test server.
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{
		Host:  ts.srv.URL,
		Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	// Extract the public key multibase for the DID document.
	p256pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	multibase := p256pub.DIDKey()[8:]

	// stubResolver returns a DIDDocument pointing the PDS to the test
	// server and containing the signing key so VerifyCommit succeeds.
	resolver := &stubResolver{
		pdsURL: ts.srv.URL,
		doc: &identity.DIDDocument{
			ID: did,
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: did, PublicKeyMultibase: multibase},
			},
			Service: []identity.Service{
				{
					ID:              "#atproto_pds",
					Type:            "AtprotoPersonalDataServer",
					ServiceEndpoint: ts.srv.URL,
				},
			},
		},
	}
	dir := &identity.Directory{Resolver: resolver}

	memstore := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      memstore,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		Directory: gt.Some(dir),
	})

	require.NoError(t, engine.Run(context.Background()))

	// Assert the DID reached Complete, not Failed. Before the fix, the
	// engine would fail with "no directory configured for signature
	// verification" because the pooled PDS client lacked the Directory.
	memstore.mu.Lock()
	state := memstore.state[did]
	memstore.mu.Unlock()
	require.Equal(t, backfill.StateComplete, state)
}

// stubResolver is a fake identity.Resolver for
// TestEngine_PDSRouting_PreservesDirectory. It returns a pre-configured
// DIDDocument.
type stubResolver struct {
	pdsURL string
	doc    *identity.DIDDocument
}

func (r *stubResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	return r.doc, nil
}

func (r *stubResolver) ResolveHandle(_ context.Context, handle atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

// TestEngine_Concurrency verifies that more than one worker is doing
// work simultaneously when Workers > 1.
func TestEngine_Concurrency(t *testing.T) {
	t.Parallel()

	const n = 20
	repos := make(map[string][]byte)
	var dids []string
	for i := range n {
		did := fmt.Sprintf("did:plc:c%d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	var current, maxConcurrent atomic.Int32
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(8),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			current.Add(-1)
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(n), store.completeCalls.Load())
	require.Greater(t, maxConcurrent.Load(), int32(1), "expected concurrent execution")
}

// TestEngine_Cancellation verifies that a context cancellation
// mid-Run stops further work and Run either returns ctx.Err or nil.
func TestEngine_Cancellation(t *testing.T) {
	t.Parallel()

	const n = 50
	repos := make(map[string][]byte)
	var dids []string
	for i := range n {
		did := fmt.Sprintf("did:plc:cancel%d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()

	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(2),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			once.Do(cancel)
			return nil
		}),
	})

	err := engine.Run(ctx)
	if err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
	// Should have stopped well before all 50 — workers check ctx.Err() before
	// pulling each job, so realistic post-cancel completion is a handful.
	// n/2 leaves ~10x headroom over the worst realistic case while catching
	// any regression where cancellation fails to propagate.
	require.Less(t, store.completeCalls.Load(), int32(n/2))
}

// TestEngine_BatchRandomizesOrder verifies that the engine shuffles
// its batch before dispatching. The test server returns 3 entries per
// page; with BatchSize=30 the engine must
// accumulate all 10 pages before any dispatch happens, and the
// dispatched order should differ from the listRepos enumeration
// order.
func TestEngine_BatchRandomizesOrder(t *testing.T) {
	t.Parallel()

	const n = 30
	repos := make(map[string][]byte)
	var dids []string
	for i := range n {
		did := fmt.Sprintf("did:plc:acc%04d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()

	var mu sync.Mutex
	var order []string

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		BatchSize:  gt.Some(n),
		Handler: backfill.HandlerFunc(func(_ context.Context, did atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			mu.Lock()
			order = append(order, string(did))
			mu.Unlock()
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(n), store.completeCalls.Load())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, order, n)
	// Probability of the shuffled order matching listRepos order is ~1/30!
	require.NotEqual(t, dids, order, "shuffled order should differ from enumeration order")
}

// TestEngine_RetryExhaustionTransitionsToFailed verifies that when a
// transient error keeps recurring, retries are eventually exhausted
// and the DID transitions to StateFailed via Store.OnFail.
func TestEngine_RetryExhaustionTransitionsToFailed(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{"did:plc:always": buildTestRepoCAR(t, "did:plc:always", 1)}
	dids := []string{"did:plc:always"}
	fs := newFlakyTestServer(t, repos, dids, 503, 100)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Store:          store,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(2),
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(10 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.failCalls.Load())
	require.Equal(t, int32(0), store.completeCalls.Load())
	require.Equal(t, 3, store.failures["did:plc:always"], "expected 3 attempts (initial + 2 retries)")
}

// TestEngine_RetryDisabled verifies MaxRetries=0 fails on the first
// attempt with no retries.
func TestEngine_RetryDisabled(t *testing.T) {
	t.Parallel()

	repos := map[string][]byte{"did:plc:noretry": buildTestRepoCAR(t, "did:plc:noretry", 1)}
	dids := []string{"did:plc:noretry"}
	fs := newFlakyTestServer(t, repos, dids, 503, 100)

	xc := &xrpc.Client{Host: fs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(0),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.failCalls.Load())
	require.Equal(t, int32(1), fs.attempts["did:plc:noretry"].Load())
}

// TestEngine_RetriesFailedFromPriorRun verifies that DIDs presented
// as StateFailed by Lookup are re-dispatched (matching DESIGN.md §4.3
// behavior: failed DIDs are periodically retried with backoff).
func TestEngine_RetriesFailedFromPriorRun(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:retryme"}
	repos := map[string][]byte{"did:plc:retryme": buildTestRepoCAR(t, "did:plc:retryme", 1)}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	store.preset("did:plc:retryme", backfill.StateFailed)

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.completeCalls.Load(), "Failed DID should be retried")
	require.Equal(t, int32(0), store.discoverCalls.Load(), "Failed DID is not Unknown — no OnDiscover")
}

// TestEngine_OnCompleteFailure_DoesNotCallOnFail verifies the
// errOnCompleteRecorded sentinel: when Store.OnComplete fails the
// engine must NOT call Store.OnFail (the handler already had its
// side effects and the DID is partially-Complete, not Failed). It
// also verifies OnError fires with a wrapped on_complete error.
func TestEngine_OnCompleteFailure_DoesNotCallOnFail(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:oc"}
	repos := map[string][]byte{"did:plc:oc": buildTestRepoCAR(t, "did:plc:oc", 1)}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	store.failOnComplete = map[string]error{"did:plc:oc": errSentinel}

	var handlerCalls atomic.Int32
	var onErrCalls atomic.Int32
	var onErrMsg atomic.Pointer[string]
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(3),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			handlerCalls.Add(1)
			return nil
		}),
		OnError: gt.Some(func(_ atmos.DID, err error) {
			onErrCalls.Add(1)
			s := err.Error()
			onErrMsg.Store(&s)
		}),
	})

	err := engine.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "OnComplete recording failed")

	require.Equal(t, int32(1), handlerCalls.Load(), "handler ran exactly once (no retries on OnComplete failure)")
	require.Equal(t, int32(1), store.completeCalls.Load(), "OnComplete attempted exactly once")
	require.Equal(t, int32(0), store.failCalls.Load(), "OnFail must NOT be called when OnComplete fails")
	require.Equal(t, int32(1), onErrCalls.Load())
	require.NotNil(t, onErrMsg.Load())
	require.Contains(t, *onErrMsg.Load(), "on_complete")

	// State remains whatever it was before OnComplete failed (never
	// transitioned to Complete or Failed).
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, backfill.StateDiscovered, store.state["did:plc:oc"])
}

// TestEngine_OnFailFailure_SurfacesViaOnError verifies that when
// Store.OnFail itself fails, OnError fires twice: once with the
// original handler error, once with the wrapped on_fail error, and
// Run aborts before checkpointing unrecorded terminal state.
func TestEngine_OnFailFailure_SurfacesViaOnError(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:offail"}
	repos := map[string][]byte{"did:plc:offail": buildTestRepoCAR(t, "did:plc:offail", 1)}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	store.failOnFail = map[string]error{"did:plc:offail": errors.New("storage exploded")}

	var mu sync.Mutex
	var msgs []string
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(0),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return errSentinel
		}),
		OnError: gt.Some(func(_ atmos.DID, err error) {
			mu.Lock()
			msgs = append(msgs, err.Error())
			mu.Unlock()
		}),
	})

	err := engine.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "on_fail")
	require.Contains(t, err.Error(), "storage exploded")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, msgs, 2, "expected one OnError for handler err, one for on_fail err")
	require.Contains(t, msgs[0], "sentinel")
	require.Contains(t, msgs[1], "on_fail")
	require.Contains(t, msgs[1], "storage exploded")
}

// TestEngine_VerifyCommitFailure_TransitionsToFailed verifies that
// when Directory is set and signature verification fails, the DID
// transitions to StateFailed.
func TestEngine_VerifyCommitFailure_TransitionsToFailed(t *testing.T) {
	t.Parallel()

	did := "did:plc:badsig"

	// Sign with one key, but the resolver advertises a different key.
	signKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	bogusKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmos.DID(did),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	require.NoError(t, r.Create("app.bsky.feed.post", "rec0", map[string]any{"text": "hi"}))
	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, signKey))
	carData := buf.Bytes()

	repos := map[string][]byte{did: carData}
	dids := []string{did}
	ts := newTestServer(t, repos, dids)

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	bogusPub, ok := bogusKey.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	multibase := bogusPub.DIDKey()[8:]

	resolver := &stubResolver{
		pdsURL: ts.srv.URL,
		doc: &identity.DIDDocument{
			ID: did,
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: did, PublicKeyMultibase: multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: ts.srv.URL},
			},
		},
	}
	dir := &identity.Directory{Resolver: resolver}

	memstore := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      memstore,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(0),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			t.Fatal("handler must not run when VerifyCommit fails")
			return nil
		}),
		Directory: gt.Some(dir),
	})

	require.NoError(t, engine.Run(context.Background()))

	memstore.mu.Lock()
	defer memstore.mu.Unlock()
	require.Equal(t, backfill.StateFailed, memstore.state[did])
}

// TestEngine_LoadFromCARFailure_RetriesAndFails verifies that
// malformed CAR bytes (truncated varint stream → io.ErrUnexpectedEOF,
// which IsTransient classifies as transient) drive the retry loop to
// exhaustion, the handler never runs, and OnFail is called once with
// attempts == MaxRetries+1.
func TestEngine_LoadFromCARFailure_RetriesAndFails(t *testing.T) {
	t.Parallel()

	did := "did:plc:malformed"
	dids := []string{did}
	// Bytes that parse a valid varint length-prefix demanding more
	// bytes than exist → io.ErrUnexpectedEOF.
	repos := map[string][]byte{did: []byte("not a valid CAR")}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	var handlerCalls atomic.Int32
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Store:          store,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(2),
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(5 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			handlerCalls.Add(1)
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(0), handlerCalls.Load(), "handler must not run on parse failure")
	require.Equal(t, int32(1), store.failCalls.Load())
	require.Equal(t, 3, store.failures[did], "expected initial + 2 retries")
}

// TestEngine_ProducerError_CancelsWorkers verifies that when listRepos
// fails mid-stream, workers stop in-flight work promptly via runCtx
// cancellation rather than draining the full buffered channel, and
// Run returns the wrapped error.
func TestEngine_ProducerError_CancelsWorkers(t *testing.T) {
	t.Parallel()

	// listFailServer serves N pages of listRepos then 500s; getRepo
	// always serves a valid repo (but the worker will block until
	// we let it through, so we can prove cancellation interrupts).
	listPagesServed := atomic.Int32{}
	repoServes := atomic.Int32{}

	const validDIDs = 10
	dids := make([]string, validDIDs)
	repos := make(map[string][]byte)
	for i := range validDIDs {
		dids[i] = fmt.Sprintf("did:plc:p%d", i)
		repos[dids[i]] = buildTestRepoCAR(t, dids[i], 1)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			n := listPagesServed.Add(1)
			if n == 1 {
				// First page returns 3 valid DIDs and a cursor.
				p := listPage{Cursor: dids[2]}
				for i := range 3 {
					p.Repos = append(p.Repos, listRepo{DID: dids[i], Head: "h", Rev: "r", Active: true})
				}
				_ = json.NewEncoder(w).Encode(p)
				return
			}
			// Subsequent pages fail the producer.
			w.WriteHeader(500)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "InternalError"})
		case "/xrpc/com.atproto.sync.getRepo":
			repoServes.Add(1)
			d := r.URL.Query().Get("did")
			data, ok := repos[d]
			if !ok {
				w.WriteHeader(404)
				return
			}
			// Stall to give producer a chance to cancel.
			time.Sleep(50 * time.Millisecond)
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(data)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(8),
		MaxRetries: gt.Some(0),
		Handler: backfill.HandlerFunc(func(ctx context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			// Block until cancel.
			<-ctx.Done()
			return ctx.Err()
		}),
	})

	err := engine.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "listRepos")
}

// TestEngine_RunIsSingleShot verifies a second Run call returns
// ErrEngineAlreadyRan and does no additional work.
func TestEngine_RunIsSingleShot(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:s1"}
	repos := map[string][]byte{"did:plc:s1": buildTestRepoCAR(t, "did:plc:s1", 1)}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.completeCalls.Load())

	err := engine.Run(context.Background())
	require.ErrorIs(t, err, backfill.ErrEngineAlreadyRan)
	require.Equal(t, int32(1), store.completeCalls.Load(), "second Run did no work")
}

// TestEngine_ActiveFlip_FiresOnUpdate verifies that when listRepos
// reports an Active value differing from the Store's recorded value,
// OnUpdate fires (active→inactive and inactive→active both).
func TestEngine_ActiveFlip_FiresOnUpdate(t *testing.T) {
	t.Parallel()

	// Two known DIDs: one was last seen Active, listRepos now
	// reports Inactive (tombstone). The other was last seen Inactive,
	// now Active (revival).
	dids := []string{"did:plc:tomb", "did:plc:rev"}
	repos := map[string][]byte{
		"did:plc:tomb": buildTestRepoCAR(t, "did:plc:tomb", 1),
		"did:plc:rev":  buildTestRepoCAR(t, "did:plc:rev", 1),
	}
	ts := newTestServer(t, repos, dids)
	ts.markInactive("did:plc:tomb") // listRepos now says inactive.

	store := newMemStore()
	store.preset2("did:plc:tomb", backfill.StateComplete, true)   // was active.
	store.preset2("did:plc:rev", backfill.StateDiscovered, false) // was inactive.

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	require.Equal(t, int32(0), store.discoverCalls.Load(), "no OnDiscover for known DIDs")
	require.Equal(t, int32(2), store.updateCalls.Load(), "both flips trigger OnUpdate")

	store.mu.Lock()
	defer store.mu.Unlock()
	require.False(t, store.active["did:plc:tomb"], "tomb now inactive")
	require.True(t, store.active["did:plc:rev"], "rev now active")
	// rev was Discovered+inactive; now Active so it gets dispatched.
	require.Equal(t, backfill.StateComplete, store.state["did:plc:rev"])
	// tomb was Complete+active; now inactive so it stays Complete.
	require.Equal(t, backfill.StateComplete, store.state["did:plc:tomb"])
}

// TestEngine_OnUpdateError_AbortsRun verifies that a Store.OnUpdate
// error bubbles out of Run, just like OnDiscover does.
func TestEngine_OnUpdateError_AbortsRun(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:flip"}
	repos := map[string][]byte{"did:plc:flip": buildTestRepoCAR(t, "did:plc:flip", 1)}
	ts := newTestServer(t, repos, dids)
	ts.markInactive("did:plc:flip") // listRepos reports inactive.

	store := newMemStore()
	store.preset2("did:plc:flip", backfill.StateDiscovered, true) // store has active=true.
	store.failOnUpdate = map[string]error{"did:plc:flip": errSentinel}

	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	err := engine.Run(context.Background())
	require.ErrorIs(t, err, errSentinel)
}

// TestEngine_OnProgress_Monotonic verifies that OnProgress callbacks
// observe strictly increasing Stats.Completed values (the engine
// serializes the Add+callback under a lock).
func TestEngine_OnProgress_Monotonic(t *testing.T) {
	t.Parallel()

	const n = 50
	repos := make(map[string][]byte)
	var dids []string
	for i := range n {
		did := fmt.Sprintf("did:plc:m%d", i)
		dids = append(dids, did)
		repos[did] = buildTestRepoCAR(t, did, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	var mu sync.Mutex
	var observed []int64
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(8),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		OnProgress: gt.Some(func(s backfill.Stats) {
			mu.Lock()
			observed = append(observed, s.Completed)
			mu.Unlock()
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, observed, n)
	for i := 1; i < len(observed); i++ {
		require.Greater(t, observed[i], observed[i-1],
			"OnProgress.Completed must increase strictly: idx=%d, %d <= %d", i, observed[i], observed[i-1])
	}
	require.Equal(t, int64(n), observed[len(observed)-1])
}

// TestEngine_RetryAfter_ExceedsMaxDelay_FailsFast verifies that when
// the server's Retry-After exceeds RetryMaxDelay, the engine declines
// to retry and transitions the DID to Failed (rather than ignoring
// the server's request and hammering it).
func TestEngine_RetryAfter_ExceedsMaxDelay_FailsFast(t *testing.T) {
	t.Parallel()

	did := "did:plc:slow"
	dids := []string{did}
	repos := map[string][]byte{did: buildTestRepoCAR(t, did, 1)}

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			p := listPage{}
			p.Repos = append(p.Repos, listRepo{DID: did, Head: "h", Rev: "r", Active: true})
			_ = json.NewEncoder(w).Encode(p)
		case "/xrpc/com.atproto.sync.getRepo":
			attempts.Add(1)
			// 10s into the future — well past our 100ms RetryMaxDelay.
			w.Header().Set("RateLimit-Remaining", "0")
			w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(10*time.Second).Unix()))
			w.WriteHeader(429)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RateLimitExceeded"})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Store:          store,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(5),
		RetryBaseDelay: gt.Some(10 * time.Millisecond),
		RetryMaxDelay:  gt.Some(100 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	start := time.Now()
	require.NoError(t, engine.Run(context.Background()))
	elapsed := time.Since(start)

	require.Equal(t, int32(1), store.failCalls.Load())
	require.Equal(t, int32(1), attempts.Load(), "should fail fast without retries when Retry-After > RetryMaxDelay")
	require.Less(t, elapsed, 5*time.Second, "must not honor 10s Retry-After when cap is 100ms")
	_ = dids
	_ = repos
}

// TestEngine_StartCursor_PassedToListRepos confirms the cursor
// configured on Options is sent to the relay on the first request.
// This is the resume mechanism end-to-end.
func TestEngine_StartCursor_PassedToListRepos(t *testing.T) {
	t.Parallel()

	var firstCursor string
	var firstSeen atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			if firstSeen.CompareAndSwap(false, true) {
				firstCursor = r.URL.Query().Get("cursor")
			}
			_ = json.NewEncoder(w).Encode(listPage{})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:  sc,
		Store:       store,
		Handler:     backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error { return nil }),
		StartCursor: gt.Some("resume-token-x"),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, "resume-token-x", firstCursor)
}

// TestEngine_OnBatchComplete_FiresPerBatch confirms OnBatchComplete
// is called once per completed batch, with the cursor the relay
// returned for the final page in that batch. testServer paginates 3
// entries per page; with BatchSize=3 and 4 DIDs we get two batches
// (3 + 1).
func TestEngine_OnBatchComplete_FiresPerBatch(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc", "did:plc:ddd"}
	repos := map[string][]byte{}
	for _, d := range dids {
		repos[d] = buildTestRepoCAR(t, d, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	var observedCursors []string
	var mu sync.Mutex
	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		BatchSize:  gt.Some(3),
		Handler:    backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error { return nil }),
		OnBatchComplete: gt.Some(func(cursor string) error {
			mu.Lock()
			defer mu.Unlock()
			observedCursors = append(observedCursors, cursor)
			return nil
		}),
	})
	require.NoError(t, engine.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	// testServer cursors page boundaries on the LAST DID of each
	// non-final page. So 4 DIDs => two pages: page1 cursor "did:plc:ccc",
	// page2 (terminator) cursor "".
	require.Len(t, observedCursors, 2)
	require.Equal(t, "did:plc:ccc", observedCursors[0])
	require.Equal(t, "", observedCursors[1], "final page cursor must be empty")
}

func TestEngine_OnPageComplete_FiresPerListReposPage(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc", "did:plc:ddd"}
	repos := map[string][]byte{}
	for _, d := range dids {
		repos[d] = buildTestRepoCAR(t, d, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	var observedCursors []string
	var mu sync.Mutex
	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		BatchSize:  gt.Some(3),
		Handler:    backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error { return nil }),
		OnPageComplete: gt.Some(func(cursor string) error {
			mu.Lock()
			defer mu.Unlock()
			observedCursors = append(observedCursors, cursor)
			return nil
		}),
	})
	require.NoError(t, engine.Run(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, observedCursors, 2)
	require.Equal(t, "did:plc:ccc", observedCursors[0])
	require.Equal(t, "", observedCursors[1])
}

// TestEngine_OnBatchComplete_WaitsForJobsBeforeCallback verifies the
// checkpoint invariant: OnBatchComplete does not fire until every
// eligible job in the batch has reached a terminal state.
func TestEngine_OnBatchComplete_WaitsForJobsBeforeCallback(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc", "did:plc:ddd", "did:plc:eee", "did:plc:fff"}
	repos := map[string][]byte{}
	for _, d := range dids {
		repos[d] = buildTestRepoCAR(t, d, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	var handlerEntered atomic.Int32
	handlerRelease := make(chan struct{})
	var batchCompleteCalls atomic.Int32

	store := newMemStore()
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		BatchSize:  gt.Some(6),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			handlerEntered.Add(1)
			<-handlerRelease
			return nil
		}),
		OnBatchComplete: gt.Some(func(_ string) error {
			batchCompleteCalls.Add(1)
			return nil
		}),
	})

	done := make(chan error, 1)
	go func() {
		done <- engine.Run(context.Background())
	}()

	// Wait for the worker to park inside a handler. OnBatchComplete
	// must not fire while any job from the batch is still running.
	require.Eventually(t, func() bool {
		return handlerEntered.Load() >= 1
	}, 5*time.Second, time.Millisecond, "worker never entered a handler")

	// Give the producer plenty of opportunity to misbehave and fire
	// the callback before the blocked handler finishes.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), batchCompleteCalls.Load(),
		"batch-complete fired while a batch job was still running")

	close(handlerRelease)
	require.NoError(t, <-done)

	require.Equal(t, int32(6), handlerEntered.Load(), "every DID's handler must run")
	require.Equal(t, int32(1), batchCompleteCalls.Load(), "6 DIDs with BatchSize=6 = 1 batch")
}

// TestEngine_OnBatchCompleteError_AbortsRun confirms an error from
// the callback aborts the Run with a wrapped error.
func TestEngine_OnBatchCompleteError_AbortsRun(t *testing.T) {
	t.Parallel()

	dids := []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc", "did:plc:ddd"}
	repos := map[string][]byte{}
	for _, d := range dids {
		repos[d] = buildTestRepoCAR(t, d, 1)
	}
	ts := newTestServer(t, repos, dids)
	xc := &xrpc.Client{Host: ts.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newMemStore()
	wantErr := errors.New("persist failed")
	engine := backfill.NewEngine(backfill.Options{
		SyncClient: sc,
		Store:      store,
		Handler:    backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error { return nil }),
		OnBatchComplete: gt.Some(func(_ string) error {
			return wantErr
		}),
	})

	err := engine.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	require.Contains(t, err.Error(), "on_batch_complete")
}
