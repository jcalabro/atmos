package sync_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestMemChainStore_LoadMissingReturnsNilNil(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	state, err := store.Load(context.Background(), atmos.DID("did:plc:abc"))
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemChainStore_SaveThenLoad(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	want := sync.ChainState{Rev: "3l3qo2vutsw2b", Data: cid}
	require.NoError(t, store.Save(context.Background(), did, want))

	got, err := store.Load(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.Rev, got.Rev)
	assert.True(t, got.Data.Equal(want.Data))
}

func TestMemChainStore_Delete(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	require.NoError(t, store.Save(context.Background(), did, sync.ChainState{Rev: "r1", Data: cid}))
	require.NoError(t, store.Delete(context.Background(), did))

	state, err := store.Load(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemChainStore_DeleteMissingNoError(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	require.NoError(t, store.Delete(context.Background(), atmos.DID("did:plc:never-saved")))
}

func TestErrorTypes_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	t.Run("ChainBreakError", func(t *testing.T) {
		cid, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
		cause := errors.New("cause goes here")
		err := &sync.ChainBreakError{
			DID:          atmos.DID("did:plc:abc"),
			SeenRev:      "r1",
			SeenData:     cid,
			GotRev:       "r2",
			GotPrevData:  cid,
			InvertedData: cid,
			Cause:        cause,
		}
		assert.Contains(t, err.Error(), "chain break")
		assert.Contains(t, err.Error(), "did:plc:abc")
		assert.ErrorIs(t, err, cause)

		var target *sync.ChainBreakError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("InversionError", func(t *testing.T) {
		cause := errors.New("missing block")
		err := &sync.InversionError{DID: "did:plc:x", Rev: "r1", Cause: cause}
		assert.Contains(t, err.Error(), "inversion failed")
		assert.ErrorIs(t, err, cause)

		var target *sync.InversionError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("SignatureError", func(t *testing.T) {
		cause := errors.New("bad sig")
		err := &sync.SignatureError{DID: "did:plc:x", Rev: "r1", KeyDID: "did:key:z...", Cause: cause}
		assert.Contains(t, err.Error(), "signature")
		assert.ErrorIs(t, err, cause)

		var target *sync.SignatureError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("ResyncFailedError", func(t *testing.T) {
		cause := errors.New("PDS down")
		err := &sync.ResyncFailedError{DID: "did:plc:x", Reason: sync.ReasonChainBreak, Cause: cause}
		assert.Contains(t, err.Error(), "resync failed")
		assert.ErrorIs(t, err, cause)

		var target *sync.ResyncFailedError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("ResyncRateLimitedError", func(t *testing.T) {
		err := &sync.ResyncRateLimitedError{DID: "did:plc:x"}
		assert.Contains(t, err.Error(), "rate limited")
		assert.Contains(t, err.Error(), "did:plc:x")

		var target *sync.ResyncRateLimitedError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("Wrapping with fmt.Errorf", func(t *testing.T) {
		inner := &sync.SignatureError{DID: "did:plc:x", Rev: "r1"}
		wrapped := fmt.Errorf("verifier: %w", inner)
		var target *sync.SignatureError
		assert.True(t, errors.As(wrapped, &target))
	})

	t.Run("InversionError nil cause", func(t *testing.T) {
		err := &sync.InversionError{DID: "did:plc:x", Rev: "r1"}
		assert.Contains(t, err.Error(), "inversion failed")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("SignatureError nil cause", func(t *testing.T) {
		err := &sync.SignatureError{DID: "did:plc:x", Rev: "r1", KeyDID: "did:key:z..."}
		assert.Contains(t, err.Error(), "signature")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("ResyncFailedError nil cause", func(t *testing.T) {
		err := &sync.ResyncFailedError{DID: "did:plc:x", Reason: sync.ReasonChainBreak}
		assert.Contains(t, err.Error(), "resync failed")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("ChainBreakError first sighting and zero inverted", func(t *testing.T) {
		// SeenRev empty + zero SeenData + zero InvertedData should not produce bare "b".
		err := &sync.ChainBreakError{
			DID:    atmos.DID("did:plc:x"),
			GotRev: "r2",
		}
		msg := err.Error()
		assert.Contains(t, msg, "first-sighting")
		assert.Contains(t, msg, "inverted=n/a")
		assert.NotContains(t, msg, "data=b ")
		assert.NotContains(t, msg, "data=b,")
		assert.NotContains(t, msg, "data=b)")
		assert.NotContains(t, msg, "inverted=b)")
	})
}

func TestResyncReason_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "chain_break", sync.ReasonChainBreak.String())
	assert.Equal(t, "inversion_failure", sync.ReasonInversionFailure.String())
	assert.Equal(t, "sync_event", sync.ReasonSyncEvent.String())
	assert.Equal(t, "unknown_reason(99)", sync.ResyncReason(99).String())
}

func TestVerifierPolicy_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "resync", sync.PolicyResync.String())
	assert.Equal(t, "error", sync.PolicyError.String())
	assert.Equal(t, "unknown_policy(99)", sync.VerifierPolicy(99).String())
}

func TestVerifierOptions_ZeroValuePolicyIsResync(t *testing.T) {
	t.Parallel()
	var o sync.VerifierOptions
	assert.Equal(t, sync.PolicyResync, o.Policy)
}

func TestVerifierStatsZero(t *testing.T) {
	t.Parallel()
	var s sync.VerifierStats
	assert.Equal(t, uint64(0), s.EventsVerified)
	assert.Equal(t, uint64(0), s.ChainBreaks)
	assert.Equal(t, uint64(0), s.Resyncs)
	assert.Equal(t, uint64(0), s.InversionFailures)
	assert.Equal(t, uint64(0), s.SignatureFailures)
	assert.Equal(t, uint64(0), s.ResyncFailures)
	assert.Equal(t, uint64(0), s.RevReplaysDropped)
}

func TestNewVerifier_RequiredFields(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://example.invalid"}
	sc := sync.NewClient(sync.Options{Client: xc})

	t.Run("missing ChainStore", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: sc,
			Directory:  dir,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ChainStore")
	})

	t.Run("missing Directory", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: sc,
			ChainStore: sync.NewMemChainStore(),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Directory")
	})

	t.Run("PolicyResync requires SyncClient", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			Directory:  dir,
			ChainStore: sync.NewMemChainStore(),
			Policy:     sync.PolicyResync,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SyncClient")
	})

	t.Run("PolicyError works without SyncClient", func(t *testing.T) {
		v, err := sync.NewVerifier(sync.VerifierOptions{
			Directory:  dir,
			ChainStore: sync.NewMemChainStore(),
			Policy:     sync.PolicyError,
		})
		require.NoError(t, err)
		assert.NotNil(t, v)
	})

	t.Run("happy path with all required", func(t *testing.T) {
		v, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: sc,
			Directory:  dir,
			ChainStore: sync.NewMemChainStore(),
		})
		require.NoError(t, err)
		require.NotNil(t, v)
	})
}

func TestNewVerifier_StatsStartAtZero(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://example.invalid"}
	sc := sync.NewClient(sync.Options{Client: xc})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sc,
		Directory:  dir,
		ChainStore: sync.NewMemChainStore(),
	})
	require.NoError(t, err)
	assert.NotNil(t, v)
	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.EventsVerified)
}

func TestNewVerifier_DoesNotMutateCallerOptions(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://example.invalid"}
	sc := sync.NewClient(sync.Options{Client: xc})

	opts := sync.VerifierOptions{
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		ResyncLimit: 0,
		ResyncBurst: 0,
	}
	_, err := sync.NewVerifier(opts)
	require.NoError(t, err)
	// NewVerifier defaults ResyncLimit and ResyncBurst internally; the
	// caller's struct must not be mutated.
	assert.Equal(t, rate.Limit(0), opts.ResyncLimit)
	assert.Equal(t, 0, opts.ResyncBurst)
}
