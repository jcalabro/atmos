package sync_test

// Tests for the resync fetch/apply lock split (jetstream#299): the
// getRepo CAR download must NOT hold the per-DID verification lock.
// Holding it wedges every VerifyCommit for that DID behind a fetch
// that can legitimately take many minutes (multi-hundred-MB repos),
// which in production stalled ALL live ingest for 10+ hours: the
// scheduler's workers piled up in lockDID behind slow podping.at
// repo downloads.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// stallServer serves getRepo with headers immediately but stalls the
// body until release is closed. entered is signalled (once) when the
// handler starts stalling, so tests know the fetch is in flight.
func stallServer(t *testing.T, carBytes []byte) (client *xrpc.Client, entered <-chan struct{}, release chan<- struct{}) {
	t.Helper()
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var once bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.getRepo" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if !once {
			once = true
			close(enteredCh)
		}
		select {
		case <-releaseCh:
			_, _ = w.Write(carBytes)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	return &xrpc.Client{
		Host:  srv.URL,
		Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}, enteredCh, releaseCh
}

// buildChainBreak returns everything needed to trigger async
// chain-break resyncs for one DID: the signing key, the exported CAR,
// a state store primed with a mismatched chain (so the next commit
// chain-breaks), the identity directory, and a trigger func that
// mints a fresh chain-breaking commit each call.
func buildChainBreak(t *testing.T, did atmos.DID) (key crypto.PrivateKey, carBytes []byte, cs sync.StateStore, dir *identity.Directory, trigger func() *comatproto.SyncSubscribeRepos_Commit) {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	otherCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	cs = sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir = &identity.Directory{Resolver: resolver}

	n := 0
	trigger = func() *comatproto.SyncSubscribeRepos_Commit {
		n++
		return testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
			Action:     testutil.ActionUpdate,
			Collection: "app.bsky.feed.post",
			RKey:       "x",
			Record:     map[string]any{"text": string(rune('a' + n))},
		}})
	}
	return key, carBuf.Bytes(), cs, dir, trigger
}

// TestAsyncResync_FetchDoesNotHoldDIDLock is the regression test for
// the production wedge (bluesky-social/jetstream#299): while the
// resync worker's CAR download is in flight, the per-DID lock must be
// free so live VerifyCommit/OnAccountEvent traffic for that DID keeps
// flowing (into the pending buffer) instead of parking scheduler
// workers in lockDID for the duration of a possibly-minutes-long
// download.
func TestAsyncResync_FetchDoesNotHoldDIDLock(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:lockfree1")
	_, carBytes, cs, dir, trigger := buildChainBreak(t, did)
	xc, entered, release := stallServer(t, carBytes)
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Chain break -> async resync enqueued; worker begins the fetch.
	ops, err := v.VerifyCommit(context.Background(), trigger())
	require.NoError(t, err)
	require.Nil(t, ops)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("resync fetch never reached the CAR server")
	}

	// THE regression assertion: with the fetch stalled mid-body, the
	// per-DID lock must be acquirable promptly.
	acquired := make(chan func(), 1)
	go func() { acquired <- sync.LockDIDForTest(v, did) }()
	select {
	case unlock := <-acquired:
		unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("per-DID lock is held across the resync CAR fetch (jetstream#299 wedge)")
	}

	// A same-DID commit must also complete promptly (buffered into
	// pending by the resyncing FSM), not block behind the download.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ops2, err2 := v.VerifyCommit(context.Background(), trigger())
		assert.NoError(t, err2)
		assert.Nil(t, ops2)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("VerifyCommit for the resyncing DID blocked behind the CAR fetch")
	}

	// Release the download; the resync completes and delivers the
	// resync ops. The buffered pending commit's replay may
	// legitimately fail verification (its inverted pre-state is the
	// post-trigger tree, not the CAR state) — that surfaces on
	// AsyncErrors and is irrelevant to what this test asserts, so
	// drain rather than fail on it.
	close(release)
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-v.ResyncEvents():
			require.Equal(t, did, ev.DID)
			require.Equal(t, sync.ReasonChainBreak, ev.Reason)
			require.NotEmpty(t, ev.Ops)
			return
		case err := <-v.AsyncErrors():
			t.Logf("async error while waiting for ResyncEvent (tolerated): %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for ResyncEvent after releasing the download")
		}
	}
}
