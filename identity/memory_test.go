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
