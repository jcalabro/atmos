package sync_test

// Tests for the apply phase of the resync fetch/apply split: because
// the CAR download runs without the per-DID lock, the world may move
// while it is in flight. resyncApplyLocked must re-validate against
// the then-current state rather than trusting fetch-time checks.

import (
	"context"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestResync_ApplyRejectsFetchMadeStaleDuringDownload: chain state
// that advances past the fetched commit's rev while the download is
// in flight must cause the apply to reject the fetch (rev-regression
// gate) instead of rolling the chain backwards.
func TestResync_ApplyRejectsFetchMadeStaleDuringDownload(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:staleapply1")
	_, carBytes, cs, dir, _ := buildChainBreak(t, did)
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

	// Start the resync; it stalls inside the CAR download.
	type result struct {
		ops []sync.VerifierOp
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		ops, rerr := v.Resync(context.Background(), did)
		resCh <- result{ops, rerr}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("resync fetch never reached the CAR server")
	}

	// While the fetch is stalled, chain state advances beyond any rev
	// the CAR can carry ("z..." sorts above every TID the test repo
	// generates). In production this is a concurrent commit or #sync
	// resync landing during the download.
	someCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "zzzzzzzzzzzzz", Data: someCID}))

	close(release)
	res := <-resCh

	var rf *sync.ResyncFailedError
	require.ErrorAs(t, res.err, &rf, "stale fetch must be rejected, got ops=%v err=%v", res.ops, res.err)
	var rr *sync.RevRegressionError
	require.ErrorAs(t, rf.Cause, &rr, "rejection must be the rev-regression gate")

	// The advanced state must be untouched.
	st, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Equal(t, "zzzzzzzzzzzzz", st.Rev, "apply must not roll chain state back to the stale fetch")
	require.Equal(t, uint64(0), v.Stats().Resyncs, "a rejected apply is not a successful resync")
}

// TestResync_ApplyRechecksHostingGate: an account deactivated while
// the download is in flight (HostingGate policy) must be rejected at
// apply time, not archived on the strength of the fetch-time check.
func TestResync_ApplyRechecksHostingGate(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:gateapply1")
	_, carBytes, cs, dir, _ := buildChainBreak(t, did)
	xc, entered, release := stallServer(t, carBytes)
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sc),
		Directory:     dir,
		StateStore:    cs,
		Policy:        gt.Some(sync.PolicyResync),
		HostingPolicy: gt.Some(sync.HostingGate),
		ResyncLimit:   gt.Some(rate.Inf),
		ResyncBurst:   gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Active at fetch time.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Seq: 1, Active: true, Time: "2026-01-01T00:00:00Z",
	}))

	type result struct{ err error }
	resCh := make(chan result, 1)
	go func() {
		_, rerr := v.Resync(context.Background(), did)
		resCh <- result{rerr}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("resync fetch never reached the CAR server")
	}

	// Deactivated mid-download.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Seq: 2, Active: false,
		Status: gt.Some("deactivated"), Time: "2026-01-01T00:00:01Z",
	}))

	close(release)
	res := <-resCh

	var rf *sync.ResyncFailedError
	require.ErrorAs(t, res.err, &rf)
	require.Equal(t, sync.ReasonAccountInactive, rf.Reason,
		"apply must re-check the hosting gate after the unlocked fetch")
	require.Equal(t, uint64(0), v.Stats().Resyncs)
}
