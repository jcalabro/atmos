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
