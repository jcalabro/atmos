package streaming

import (
	"reflect"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// TestConvertVerifierOps_FieldRoundTrip is a field-drift guard for
// convertVerifierOps. It builds a sync.VerifierOp with a recognizable,
// non-zero value in every field, runs the conversion, and asserts each
// field round-trips into the corresponding streaming.Operation field.
//
// If a future change adds a field to sync.VerifierOp without updating
// convertVerifierOps, this test fails: either the field-count assertion
// trips, or the new field's value is missing in the converted Operation.
func TestConvertVerifierOps_FieldRoundTrip(t *testing.T) {
	t.Parallel()

	cid, err := cbor.ParseCIDString("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")
	require.NoError(t, err)

	in := []sync.VerifierOp{{
		Action:     atmos.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "3kf2abcxyz",
		Repo:       "did:plc:fielddrift",
		Rev:        "3kf2abcxyz",
		CID:        cid,
		BlockData:  []byte{0xCA, 0xFE, 0xBA, 0xBE},
	}}

	got := convertVerifierOps(in)
	require.Len(t, got, 1)

	op := got[0]
	require.Equal(t, in[0].Action, op.Action)
	require.Equal(t, in[0].Collection, op.Collection)
	require.Equal(t, in[0].RKey, op.RKey)
	require.Equal(t, in[0].Repo, op.Repo)
	require.Equal(t, in[0].Rev, op.Rev)
	require.Equal(t, in[0].CID, op.CID)
	require.Equal(t, in[0].BlockData, op.blockData)

	// Field-count guard. If sync.VerifierOp gains a field, this assertion
	// trips and the contributor is forced to update convertVerifierOps
	// AND extend this test. Subtract one from streaming.Operation's
	// field count to account for blockData (an unexported re-name of
	// VerifierOp.BlockData).
	vopFields := reflect.TypeFor[sync.VerifierOp]().NumField()
	opFields := reflect.TypeFor[Operation]().NumField()
	require.Equal(t, vopFields, opFields,
		"sync.VerifierOp and streaming.Operation field counts have drifted; update convertVerifierOps and this test")
}

func TestEventFromAsyncResync_UsesNormalEventShape(t *testing.T) {
	t.Parallel()

	cid, err := cbor.ParseCIDString("bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily")
	require.NoError(t, err)

	evt := eventFromAsyncResync(sync.ResyncEvent{
		DID:    "did:plc:asyncresync",
		OldRev: "3old",
		NewRev: "3new",
		Reason: sync.ReasonChainBreak,
		Ops: []sync.VerifierOp{{
			Action:     atmos.ActionResync,
			Collection: "app.bsky.feed.post",
			RKey:       "3kf2abcxyz",
			Repo:       "did:plc:asyncresync",
			Rev:        "3new",
			CID:        cid,
			BlockData:  []byte{0x01, 0x02},
		}},
	})

	require.Nil(t, evt.Commit)
	require.NotNil(t, evt.Sync)
	require.Equal(t, "did:plc:asyncresync", evt.Sync.DID)
	require.Equal(t, "3new", evt.Sync.Rev)
	require.Equal(t, int64(0), evt.Seq)
	require.Equal(t, ResyncAsync, evt.Resync)

	var ops []Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		ops = append(ops, op)
	}
	require.Len(t, ops, 1)
	require.Equal(t, ActionResync, ops[0].Action)
	require.Equal(t, atmos.DID("did:plc:asyncresync"), ops[0].Repo)
	require.Equal(t, atmos.TID("3new"), ops[0].Rev)
	require.Equal(t, []byte{0x01, 0x02}, ops[0].BlockData())
}

func TestEventFromAsyncResync_EmptyRepoStillProducesResyncEvent(t *testing.T) {
	t.Parallel()

	evt := eventFromAsyncResync(sync.ResyncEvent{
		DID:    "did:plc:emptyresync",
		NewRev: "3empty",
		Reason: sync.ReasonInversionFailure,
		Ops:    []sync.VerifierOp{},
	})

	require.NotNil(t, evt.Sync)
	require.Equal(t, "did:plc:emptyresync", evt.Sync.DID)
	require.Equal(t, "3empty", evt.Sync.Rev)
	require.Equal(t, ResyncAsync, evt.Resync)

	count := 0
	for _, err := range evt.Operations() {
		require.NoError(t, err)
		count++
	}
	require.Zero(t, count)
}
