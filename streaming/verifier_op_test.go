package streaming_test

import (
	"testing"

	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvent_OperationsUsesVerifiedOpsWhenSet(t *testing.T) {
	t.Parallel()

	ops := []streaming.Operation{
		{Action: streaming.ActionResync, Collection: "app.bsky.feed.post", RKey: "rec1", Repo: "did:plc:abc"},
		{Action: streaming.ActionResync, Collection: "app.bsky.feed.post", RKey: "rec2", Repo: "did:plc:abc"},
	}

	evt := streaming.NewEventWithVerifiedOpsForTest(ops)

	var got []streaming.Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		got = append(got, op)
	}
	assert.Equal(t, ops, got)
}

func TestEvent_OperationsUsesEmptyVerifiedOpsWhenVerifierRan(t *testing.T) {
	t.Parallel()
	// A verified empty-ops commit must NOT fall through to CAR decoding.
	// verifierRan = true with empty slice means "verifier saw zero ops"
	// and Operations() should yield nothing.
	evt := streaming.NewEventWithVerifiedOpsForTest(nil)

	count := 0
	for range evt.Operations() {
		count++
	}
	assert.Equal(t, 0, count)
}

// TestEvent_OperationsStrictValidationPasses asserts that ops with
// well-formed typed fields are yielded unchanged when strict
// validation is enabled.
func TestEvent_OperationsStrictValidationPasses(t *testing.T) {
	t.Parallel()
	good := []streaming.Operation{
		{
			Action:     streaming.ActionCreate,
			Collection: "app.bsky.feed.post",
			RKey:       "3jqfcqzm3fp2j",
			Repo:       "did:plc:abc123",
			Rev:        "3jqfcqzm3fp2j",
		},
	}
	evt := streaming.NewStrictEventWithVerifiedOpsForTest(good)

	var got []streaming.Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		got = append(got, op)
	}
	assert.Len(t, got, 1)
	assert.Equal(t, "did:plc:abc123", string(got[0].Repo))
}

// TestEvent_OperationsStrictValidationRejectsBadFields asserts that
// strict-validation surfaces a typed atmos syntax error when one of
// an op's fields fails to validate. The op itself is replaced with a
// zero Operation alongside the error, matching the existing
// (Operation{}, err) failure shape.
func TestEvent_OperationsStrictValidationRejectsBadFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		op   streaming.Operation
		want string // substring expected in the error message
	}{
		{
			name: "bad DID",
			op: streaming.Operation{
				Action:     streaming.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       "3jqfcqzm3fp2j",
				Repo:       "not-a-did",
				Rev:        "3jqfcqzm3fp2j",
			},
			want: "op.Repo",
		},
		{
			name: "bad NSID",
			op: streaming.Operation{
				Action:     streaming.ActionCreate,
				Collection: "BadNSID",
				RKey:       "3jqfcqzm3fp2j",
				Repo:       "did:plc:abc123",
				Rev:        "3jqfcqzm3fp2j",
			},
			want: "op.Collection",
		},
		{
			name: "bad RecordKey",
			op: streaming.Operation{
				Action:     streaming.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       "has spaces",
				Repo:       "did:plc:abc123",
				Rev:        "3jqfcqzm3fp2j",
			},
			want: "op.RKey",
		},
		{
			name: "bad TID",
			op: streaming.Operation{
				Action:     streaming.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       "3jqfcqzm3fp2j",
				Repo:       "did:plc:abc123",
				Rev:        "not-a-tid",
			},
			want: "op.Rev",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			evt := streaming.NewStrictEventWithVerifiedOpsForTest([]streaming.Operation{tc.op})

			var (
				yields int
				gotErr error
				gotOp  streaming.Operation
				zeroOp streaming.Operation
			)
			for op, err := range evt.Operations() {
				yields++
				gotOp = op
				gotErr = err
			}
			require.Equal(t, 1, yields)
			require.Error(t, gotErr)
			assert.Contains(t, gotErr.Error(), tc.want,
				"error must name the offending field")
			assert.Equal(t, zeroOp, gotOp,
				"op must be zero on validation failure (matches existing parse-error shape)")
		})
	}
}
