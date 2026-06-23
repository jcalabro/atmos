package backfill

import (
	"bytes"
	"context"
	"encoding/json"
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
