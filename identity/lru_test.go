package identity

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The deeper LRU semantics (eviction order, TTL boundary, pinning, soft
// overflow) live in internal/lru. These tests cover the identity
// adapter's adherence to the Cache interface contract.

func makeIdentity(did string) *Identity {
	return &Identity{DID: atmos.DID(did)}
}

func TestLRU_GetSet(t *testing.T) {
	t.Parallel()
	c := NewLRUCache(10, time.Hour)
	ctx := context.Background()

	_, ok := c.Get(ctx, "a")
	assert.False(t, ok)

	id := makeIdentity("did:plc:abc")
	c.Set(ctx, "a", id)

	got, ok := c.Get(ctx, "a")
	require.True(t, ok)
	assert.Equal(t, id.DID, got.DID)
}

func TestLRU_Eviction(t *testing.T) {
	t.Parallel()
	c := NewLRUCache(2, time.Hour)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:a"))
	c.Set(ctx, "b", makeIdentity("did:plc:b"))
	c.Set(ctx, "c", makeIdentity("did:plc:c")) // evicts "a"

	_, ok := c.Get(ctx, "a")
	assert.False(t, ok, "a should be evicted")

	_, ok = c.Get(ctx, "b")
	assert.True(t, ok)
	_, ok = c.Get(ctx, "c")
	assert.True(t, ok)
}

func TestLRU_UpdateExisting(t *testing.T) {
	t.Parallel()
	c := NewLRUCache(10, time.Hour)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:old"))
	c.Set(ctx, "a", makeIdentity("did:plc:new"))

	got, ok := c.Get(ctx, "a")
	require.True(t, ok)
	assert.Equal(t, atmos.DID("did:plc:new"), got.DID)
}

func TestLRU_Delete(t *testing.T) {
	t.Parallel()
	c := NewLRUCache(10, time.Hour)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:a"))
	c.Delete(ctx, "a")

	_, ok := c.Get(ctx, "a")
	assert.False(t, ok)

	// Deleting non-existent key should not panic.
	c.Delete(ctx, "nonexistent")
}

func TestLRU_Concurrent(t *testing.T) {
	t.Parallel()
	c := NewLRUCache(100, time.Hour)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			key := string(rune('a' + i%26))
			c.Set(ctx, key, makeIdentity("did:plc:"+key))
			c.Get(ctx, key)
			c.Delete(ctx, key)
		})
	}
	wg.Wait()
}
