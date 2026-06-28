package backfill_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/cbor"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// boundaryTruncatingServer serves a getRepo CAR truncated at a block boundary
// (so it parses cleanly but is incomplete) for the first failCount attempts
// per DID, then serves the full CAR. listRepos always advertises every DID as
// active. It models a PDS whose getRepo connection is severed mid-stream on a
// block boundary — the case that, before the fix, completed the download
// "successfully" with a partial repo and then failed NON-transiently in the
// handler's MST walk, permanently failing an otherwise-recoverable repo.
type boundaryTruncatingServer struct {
	srv         *httptest.Server
	full        map[string][]byte // complete CAR per DID
	truncated   map[string][]byte // block-boundary-truncated CAR per DID
	allDIDs     []string
	failCount   int
	attempts    map[string]*atomic.Int32
	servedTrunc map[string]*atomic.Int32
}

func newBoundaryTruncatingServer(t *testing.T, full, truncated map[string][]byte, dids []string, failCount int) *boundaryTruncatingServer {
	t.Helper()
	bs := &boundaryTruncatingServer{
		full:        full,
		truncated:   truncated,
		allDIDs:     dids,
		failCount:   failCount,
		attempts:    make(map[string]*atomic.Int32),
		servedTrunc: make(map[string]*atomic.Int32),
	}
	for _, d := range dids {
		bs.attempts[d] = &atomic.Int32{}
		bs.servedTrunc[d] = &atomic.Int32{}
	}
	bs.srv = httptest.NewServer(http.HandlerFunc(bs.handle))
	t.Cleanup(bs.srv.Close)
	return bs
}

func (bs *boundaryTruncatingServer) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/xrpc/com.atproto.sync.listRepos":
		cursor := r.URL.Query().Get("cursor")
		start := 0
		if cursor != "" {
			for i, d := range bs.allDIDs {
				if d == cursor {
					start = i + 1
					break
				}
			}
		}
		end := min(start+3, len(bs.allDIDs))
		page := listPage{}
		for _, d := range bs.allDIDs[start:end] {
			page.Repos = append(page.Repos, listRepo{DID: d, Head: "bafytest", Rev: "rev1", Active: true})
		}
		if end < len(bs.allDIDs) {
			page.Cursor = bs.allDIDs[end-1]
		}
		_ = json.NewEncoder(w).Encode(page)

	case "/xrpc/com.atproto.sync.getRepo":
		did := r.URL.Query().Get("did")
		counter, ok := bs.attempts[did]
		if !ok {
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RepoNotFound"})
			return
		}
		n := int(counter.Add(1))
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		if n <= bs.failCount {
			bs.servedTrunc[did].Add(1)
			_, _ = w.Write(bs.truncated[did]) // boundary-truncated: parses clean, incomplete
			return
		}
		_, _ = w.Write(bs.full[did])
	default:
		w.WriteHeader(404)
	}
}

// boundaryTruncate returns the longest prefix of a full CAR that (a) parses
// cleanly via LoadFromCAR yet (b) is incomplete per CheckComplete — i.e. a cut
// landing exactly on a block boundary. It fails the test if no such prefix
// exists (the repo is too small to have an interior gap).
func boundaryTruncate(t *testing.T, full []byte) []byte {
	t.Helper()
	for n := len(full) - 1; n > 0; n-- {
		rp, _, err := atmosrepo.LoadFromCAR(bytes.NewReader(full[:n]))
		if err != nil {
			continue
		}
		if rp.CheckComplete() != nil {
			return full[:n]
		}
	}
	t.Fatalf("no block-boundary truncation prefix found for CAR of %d bytes", len(full))
	return nil
}

// TestEngine_BoundaryTruncatedCAR_RetriesAndCompletes is the regression for
// the oracle "mst: loading node ...: block not found" failure. A block-
// boundary-truncated CAR served on the first attempts must now be classified
// transient (via repo completeness verification in download) and retried
// in-loop, so the repo COMPLETES once a clean CAR is served — rather than
// being permanently failed on the partial download.
func TestEngine_BoundaryTruncatedCAR_RetriesAndCompletes(t *testing.T) {
	t.Parallel()

	did := "did:plc:boundarytrunc0000000000"
	full := buildTestRepoCAR(t, did, 150) // multi-level MST
	truncated := boundaryTruncate(t, full)
	require.Less(t, len(truncated), len(full))

	// Sanity: the truncated CAR loads clean but is incomplete — the exact
	// silent-partial condition the engine must catch.
	rp, _, err := atmosrepo.LoadFromCAR(bytes.NewReader(truncated))
	require.NoError(t, err, "boundary-truncated CAR must LoadFromCAR without error")
	require.ErrorIs(t, rp.CheckComplete(), io.ErrUnexpectedEOF)

	const failCount = 2 // serve truncated twice, then full
	bs := newBoundaryTruncatingServer(t,
		map[string][]byte{did: full},
		map[string][]byte{did: truncated},
		[]string{did}, failCount)

	xc := &xrpc.Client{Host: bs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})
	store := newMemStore()

	var handlerCalls atomic.Int32
	var walkedRecords atomic.Int32
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Store:          store,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(3), // 4 attempts: trunc, trunc, full -> completes on 3rd
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(5 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, r *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			handlerCalls.Add(1)
			// The handler must only ever see a COMPLETE repo: walk every
			// record block to prove no partial repo reached it.
			return r.Tree.Walk(func(_ string, cid cbor.CID) error {
				if _, err := r.Store.GetBlock(cid); err != nil {
					return err
				}
				walkedRecords.Add(1)
				return nil
			})
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, backfill.StateComplete, store.state[did],
		"boundary-truncated repo must COMPLETE after retry, not fail permanently")
	require.Equal(t, int32(2), bs.servedTrunc[did].Load(), "expected 2 truncated serves before the clean one")
	require.Equal(t, int32(1), handlerCalls.Load(), "handler runs exactly once, on the complete CAR")
	require.Positive(t, walkedRecords.Load(), "handler walked a complete repo")
	require.Equal(t, 0, store.failures[did], "no permanent failure was recorded")
}

// TestEngine_BoundaryTruncatedCAR_ExhaustsBudget verifies the loud-failure
// boundary: if EVERY attempt serves a boundary-truncated CAR, the repo fails
// (StateFailed) after exhausting the transient retry budget — it does not
// loop forever and does not silently complete a partial repo.
func TestEngine_BoundaryTruncatedCAR_ExhaustsBudget(t *testing.T) {
	t.Parallel()

	did := "did:plc:boundarytruncalways000"
	full := buildTestRepoCAR(t, did, 150)
	truncated := boundaryTruncate(t, full)

	// failCount large enough that all attempts are truncated.
	bs := newBoundaryTruncatingServer(t,
		map[string][]byte{did: full},
		map[string][]byte{did: truncated},
		[]string{did}, 1000)

	xc := &xrpc.Client{Host: bs.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})
	store := newMemStore()
	var handlerCalls atomic.Int32
	engine := backfill.NewEngine(backfill.Options{
		SyncClient:     sc,
		Store:          store,
		Workers:        gt.Some(1),
		MaxRetries:     gt.Some(2), // 3 attempts, all truncated
		RetryBaseDelay: gt.Some(time.Millisecond),
		RetryMaxDelay:  gt.Some(5 * time.Millisecond),
		Handler: backfill.HandlerFunc(func(_ context.Context, _ atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
			handlerCalls.Add(1)
			return nil
		}),
	})

	require.NoError(t, engine.Run(context.Background()))

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, backfill.StateFailed, store.state[did])
	require.Equal(t, int32(0), handlerCalls.Load(), "handler must never run on an always-truncated repo")
	require.Equal(t, 3, store.failures[did], "expected initial + 2 retries before failing")
}
