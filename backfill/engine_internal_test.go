package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

type recordingRetrySleeper struct {
	mu     stdsync.Mutex
	delays []time.Duration
}

func (s *recordingRetrySleeper) Sleep(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.delays = append(s.delays, delay)
	s.mu.Unlock()
	return nil
}

func (s *recordingRetrySleeper) Delays() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.delays...)
}

type engineInternalStore struct {
	mu       stdsync.Mutex
	state    map[string]State
	active   map[string]bool
	complete atomic.Int32
	fail     atomic.Int32
}

func newEngineInternalStore() *engineInternalStore {
	return &engineInternalStore{
		state:  make(map[string]State),
		active: make(map[string]bool),
	}
}

func (s *engineInternalStore) Lookup(_ context.Context, did atmos.DID) (StoreEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.state[string(did)]
	if !ok {
		return StoreEntry{State: StateUnknown}, nil
	}
	return StoreEntry{State: st, Active: s.active[string(did)]}, nil
}

func (s *engineInternalStore) OnDiscover(_ context.Context, entry atmossync.ListReposEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[string(entry.DID)] = StateDiscovered
	s.active[string(entry.DID)] = entry.Active
	return nil
}

func (s *engineInternalStore) OnUpdate(_ context.Context, entry atmossync.ListReposEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[string(entry.DID)] = entry.Active
	return nil
}

func (s *engineInternalStore) OnComplete(_ context.Context, did atmos.DID, _ string, _ *atmosrepo.Commit) error {
	s.complete.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[string(did)] = StateComplete
	return nil
}

func (s *engineInternalStore) OnFail(_ context.Context, did atmos.DID, _ string, _ error, _ int) error {
	s.fail.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[string(did)] = StateFailed
	return nil
}

func buildEngineInternalRepoCAR(t *testing.T, did string) []byte {
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
	require.NoError(t, r.Create("app.bsky.feed.post", "rec0", map[string]any{"text": "record 0"}))

	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))
	return buf.Bytes()
}

func TestEngine_RateLimitServerResetHonoredWithoutWallClockSleep(t *testing.T) {
	t.Parallel()

	did := "did:plc:serverreset"
	carData := buildEngineInternalRepoCAR(t, did)

	const rateLimited = 3
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			_ = json.NewEncoder(w).Encode(struct {
				Repos []struct {
					DID    string `json:"did"`
					Head   string `json:"head"`
					Rev    string `json:"rev"`
					Active bool   `json:"active"`
				} `json:"repos"`
			}{
				Repos: []struct {
					DID    string `json:"did"`
					Head   string `json:"head"`
					Rev    string `json:"rev"`
					Active bool   `json:"active"`
				}{{DID: did, Head: "h", Rev: "r", Active: true}},
			})
		case "/xrpc/com.atproto.sync.getRepo":
			n := attempts.Add(1)
			if n <= rateLimited {
				// Remaining stays positive so xrpc's client-level
				// proactive limiter does not sleep; this test is
				// specifically about the backfill engine's handling of
				// server-directed retry delays.
				w.Header().Set("RateLimit-Remaining", "1")
				w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(5*time.Second).Unix()))
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "RateLimitExceeded"})
				return
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(carData)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	sleeper := &recordingRetrySleeper{}
	store := newEngineInternalStore()
	engine := NewEngine(Options{
		SyncClient:                sc,
		Store:                     store,
		Workers:                   gt.Some(1),
		MaxRetries:                gt.Some(0),
		RetryRateLimitMaxAttempts: gt.Some(5),
		RetryBaseDelay:            gt.Some(time.Millisecond),
		RetryMaxDelay:             gt.Some(10 * time.Millisecond),
		Handler: HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})
	engine.retrySleeper = sleeper

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(rateLimited+1), attempts.Load())
	require.Equal(t, int32(1), store.complete.Load())
	require.Equal(t, int32(0), store.fail.Load())

	delays := sleeper.Delays()
	require.Len(t, delays, rateLimited)
	for _, delay := range delays {
		require.Greater(t, delay, 10*time.Millisecond, "server-directed 429 waits must not be capped by RetryMaxDelay")
	}
}

// listReposOnce serializes a single-DID listRepos page for the timeout
// tests, which only exercise the getRepo download path.
func listReposOnce(w http.ResponseWriter, did string) {
	_ = json.NewEncoder(w).Encode(listReposJSON{
		Repos: []listRepoJSON{{DID: did, Head: "h", Rev: "r", Active: true}},
	})
}

type listReposJSON struct {
	Repos []listRepoJSON `json:"repos"`
}

type listRepoJSON struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

// TestEngine_DownloadTimeout_HangingServerFailsWithoutRetry is the
// regression test for the production stall: a PDS that accepts the getRepo
// connection and then never sends the body must not pin a worker. With a
// short DownloadTimeout the attempt is aborted, the DID transitions to
// StateFailed, and — critically — it is NOT retried in the per-DID loop
// (a naive timeout would be net.Error/Timeout and burn MaxRetries+1
// attempts, each hanging for the full deadline).
func TestEngine_DownloadTimeout_HangingServerFailsWithoutRetry(t *testing.T) {
	t.Parallel()

	did := "did:plc:hang"
	// release unblocks the hung handler at cleanup so the server can close
	// and the test goroutine doesn't leak past the run.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var getRepoAttempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			listReposOnce(w, did)
		case "/xrpc/com.atproto.sync.getRepo":
			getRepoAttempts.Add(1)
			// Send the success status + content-type so the response
			// headers arrive (defeating ResponseHeaderTimeout), then hang
			// before writing the CAR body. This is exactly the observed
			// failure mode: headers OK, body never streams.
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newEngineInternalStore()
	var onFailMu stdsync.Mutex
	var onFailErr error
	engine := NewEngine(Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		// A non-zero retry budget that MUST NOT be spent: a download
		// timeout is terminal, not transient.
		MaxRetries:      gt.Some(3),
		RetryBaseDelay:  gt.Some(time.Millisecond),
		DownloadTimeout: gt.Some(100 * time.Millisecond),
		Handler: HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
		OnError: gt.Some(func(_ atmos.DID, err error) {
			onFailMu.Lock()
			onFailErr = err
			onFailMu.Unlock()
		}),
	})

	// The whole run must finish in well under the time a retrying loop would
	// take (4 attempts * 100ms deadline = 400ms+); a generous ceiling that
	// still proves "no retry storm" and "no indefinite hang."
	done := make(chan error, 1)
	go func() { done <- engine.Run(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("engine.Run did not return: download timeout did not abort the hung getRepo")
	}

	require.Equal(t, int32(1), store.fail.Load(), "DID should fail exactly once")
	require.Equal(t, int32(0), store.complete.Load())
	require.Equal(t, StateFailed, store.state[did])
	require.Equal(t, int32(1), getRepoAttempts.Load(),
		"download timeout must be terminal: getRepo must be attempted exactly once, not retried")

	onFailMu.Lock()
	gotErr := onFailErr
	onFailMu.Unlock()
	require.Error(t, gotErr, "OnError should have fired for the timed-out download")
	require.ErrorIs(t, gotErr, errDownloadTimeout,
		"the failure surfaced to OnError must be the download-timeout sentinel")
}

// TestEngine_DownloadTimeout_ParentCancelIsNotARepoFailure verifies the
// parent-vs-derived-context distinction: when the parent ctx is cancelled
// mid-download (process shutdown / batch cancel), the engine must unwind
// without recording a spurious StateFailed for the in-flight DID.
func TestEngine_DownloadTimeout_ParentCancelIsNotARepoFailure(t *testing.T) {
	t.Parallel()

	did := "did:plc:parentcancel"
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	reqStarted := make(chan struct{})
	var once stdsync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			listReposOnce(w, did)
		case "/xrpc/com.atproto.sync.getRepo":
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			once.Do(func() { close(reqStarted) })
			select {
			case <-release:
			case <-r.Context().Done():
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newEngineInternalStore()
	ctx, cancel := context.WithCancel(context.Background())
	engine := NewEngine(Options{
		SyncClient: sc,
		Store:      store,
		Workers:    gt.Some(1),
		MaxRetries: gt.Some(3),
		// Long download timeout so it is the PARENT cancel, not our derived
		// deadline, that ends the download.
		DownloadTimeout: gt.Some(30 * time.Second),
		Handler: HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()

	// Cancel the parent once the download is genuinely in flight.
	select {
	case <-reqStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("getRepo never started")
	}
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("engine.Run did not return after parent cancel")
	}

	// Parent cancellation is infrastructure, not a repo verdict: the DID
	// must NOT be recorded failed (it should be retried on a fresh Run).
	require.Equal(t, int32(0), store.fail.Load(),
		"a parent-context cancel must not be recorded as a repo failure")
	require.Equal(t, int32(0), store.complete.Load())
}

// TestEngine_DownloadTimeout_DisabledAllowsSlowDownload verifies that
// DownloadTimeout: gt.Some(0) disables the per-attempt deadline, so a slow
// (but progressing) download still completes rather than being killed.
func TestEngine_DownloadTimeout_DisabledAllowsSlowDownload(t *testing.T) {
	t.Parallel()

	did := "did:plc:slow"
	carData := buildEngineInternalRepoCAR(t, did)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			listReposOnce(w, did)
		case "/xrpc/com.atproto.sync.getRepo":
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			w.WriteHeader(http.StatusOK)
			// Deliberately slower than the (disabled) deadline would allow,
			// but steadily progressing.
			time.Sleep(150 * time.Millisecond)
			_, _ = w.Write(carData)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	store := newEngineInternalStore()
	engine := NewEngine(Options{
		SyncClient:      sc,
		Store:           store,
		Workers:         gt.Some(1),
		DownloadTimeout: gt.Some(time.Duration(0)), // disabled
		Handler: HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))
	require.Equal(t, int32(1), store.complete.Load())
	require.Equal(t, int32(0), store.fail.Load())
	require.Equal(t, StateComplete, store.state[did])
}

// TestEngine_translateDownloadDeadline_Classification is a focused unit test
// of the parent-vs-derived deadline mapping that download() relies on.
func TestEngine_translateDownloadDeadline_Classification(t *testing.T) {
	t.Parallel()

	// We reconstruct the same predicate download() applies, asserting the
	// three branches: nil passthrough, parent-cancel passthrough, and
	// derived-deadline -> errDownloadTimeout.
	classify := func(parent, dl context.Context, err error) error {
		if err == nil || parent.Err() != nil {
			return err
		}
		if dl.Err() != nil && errors.Is(dl.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", errDownloadTimeout, err)
		}
		return err
	}

	t.Run("nil error passes through", func(t *testing.T) {
		require.NoError(t, classify(context.Background(), context.Background(), nil))
	})

	t.Run("parent cancelled is not a timeout", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()
		// Even if the derived ctx also expired, parent cancel wins.
		dl, dlCancel := context.WithTimeout(parent, time.Nanosecond)
		defer dlCancel()
		time.Sleep(time.Millisecond)
		got := classify(parent, dl, context.Canceled)
		require.NotErrorIs(t, got, errDownloadTimeout)
		require.ErrorIs(t, got, context.Canceled)
	})

	t.Run("derived deadline maps to errDownloadTimeout", func(t *testing.T) {
		parent := context.Background()
		dl, cancel := context.WithTimeout(parent, time.Nanosecond)
		defer cancel()
		time.Sleep(time.Millisecond)
		got := classify(parent, dl, context.DeadlineExceeded)
		require.ErrorIs(t, got, errDownloadTimeout)
	})
}
