package sync_test

import (
	"context"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestVerifier_Resync_TimeoutBoundsStalledFetch: a getRepo response
// that sends headers and then stalls forever must not hang a resync
// indefinitely. With ResyncTimeout set, Resync returns a
// *ResyncFailedError once the per-attempt budget elapses.
//
// In production (jetstream#299) unbounded fetches of multi-hundred-MB
// repos held resync workers for the transport ceiling (up to 30
// minutes per attempt, retried indefinitely); the verifier-level
// budget bounds each attempt regardless of transport configuration.
func TestVerifier_Resync_TimeoutBoundsStalledFetch(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:rstimeout1")
	_, carBytes, cs, dir, _ := buildChainBreak(t, did)
	xc, entered, release := stallServer(t, carBytes)
	t.Cleanup(func() { close(release) })
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sc),
		Directory:     dir,
		StateStore:    cs,
		Policy:        gt.Some(sync.PolicyResync),
		ResyncLimit:   gt.Some(rate.Inf),
		ResyncBurst:   gt.Some(1),
		ResyncTimeout: gt.Some(300 * time.Millisecond),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	start := time.Now()
	_, err = v.Resync(context.Background(), did)
	elapsed := time.Since(start)

	var rf *sync.ResyncFailedError
	require.ErrorAs(t, err, &rf, "stalled fetch must surface as ResyncFailedError, got: %v", err)
	require.Equal(t, did, rf.DID)
	require.Less(t, elapsed, 10*time.Second,
		"resync must be bounded by ResyncTimeout, not hang for the transport ceiling")

	select {
	case <-entered:
	default:
		t.Fatal("fetch never reached the server; test asserted nothing")
	}

	stats := v.Stats()
	require.Equal(t, uint64(1), stats.ResyncFailures)
}

// TestVerifier_Resync_ZeroTimeoutDisablesBudget: an explicit zero
// disables the per-attempt budget (documented escape hatch, matching
// backfill's DownloadTimeout semantics). The fetch then runs until
// the caller's ctx cancels it.
func TestVerifier_Resync_ZeroTimeoutDisablesBudget(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:rstimeout2")
	_, carBytes, cs, dir, _ := buildChainBreak(t, did)
	xc, _, release := stallServer(t, carBytes)
	t.Cleanup(func() { close(release) })
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sc),
		Directory:     dir,
		StateStore:    cs,
		Policy:        gt.Some(sync.PolicyResync),
		ResyncLimit:   gt.Some(rate.Inf),
		ResyncBurst:   gt.Some(1),
		ResyncTimeout: gt.Some(time.Duration(0)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Bound the test with a caller ctx; with the budget disabled the
	// fetch must still be running when it fires.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = v.Resync(ctx, did)
	require.Error(t, err)
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond,
		"zero ResyncTimeout must disable the internal budget (only the caller ctx ended the fetch)")
}
