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
