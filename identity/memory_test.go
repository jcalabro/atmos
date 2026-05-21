package identity

import (
	"context"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInMemoryDirectory_CacheRoundtrips(t *testing.T) {
	t.Parallel()
	d := NewInMemoryDirectory()
	ctx := context.Background()

	id := &Identity{DID: atmos.DID("did:plc:abc")}
	d.Cache.Set(ctx, "did:did:plc:abc", id)

	got, ok := d.Cache.Get(ctx, "did:did:plc:abc")
	require.True(t, ok)
	assert.Equal(t, id.DID, got.DID)
}

// Firehose-scale consumers must NOT pay the bidirectional handle round
// trip per cache miss, so the helper sets SkipHandleVerification by
// default. Callers needing verified handles should construct a Directory
// directly.
func TestNewInMemoryDirectory_SkipsHandleVerification(t *testing.T) {
	t.Parallel()
	d := NewInMemoryDirectory()
	require.NotNil(t, d)
	assert.True(t, d.SkipHandleVerification, "the in-memory helper is the firehose default; handle verification must be off")
}
