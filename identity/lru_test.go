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

func makeIdentity(did string) *Identity {
	return &Identity{DID: atmos.DID(did)}
}

func TestLRU_GetSet(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(10, time.Hour).(*lruCache)
	require.True(t, ok)
	ctx := context.Background()

	_, ok = c.Get(ctx, "a")
	assert.False(t, ok)

	id := makeIdentity("did:plc:abc")
	c.Set(ctx, "a", id)

	got, ok := c.Get(ctx, "a")
	require.True(t, ok)
	assert.Equal(t, id.DID, got.DID)
}

func TestLRU_Eviction(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(2, time.Hour).(*lruCache)
	require.True(t, ok)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:a"))
	c.Set(ctx, "b", makeIdentity("did:plc:b"))
	c.Set(ctx, "c", makeIdentity("did:plc:c")) // evicts "a"

	_, ok = c.Get(ctx, "a")
	assert.False(t, ok, "a should be evicted")

	_, ok = c.Get(ctx, "b")
	assert.True(t, ok)
	_, ok = c.Get(ctx, "c")
	assert.True(t, ok)
}

func TestLRU_EvictionOrder(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(2, time.Hour).(*lruCache)
	require.True(t, ok)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:a"))
	c.Set(ctx, "b", makeIdentity("did:plc:b"))

	// Access "a" to make it most recent.
	c.Get(ctx, "a")

	c.Set(ctx, "c", makeIdentity("did:plc:c")) // evicts "b" (LRU)

	_, ok = c.Get(ctx, "a")
	assert.True(t, ok, "a should survive (recently used)")
	_, ok = c.Get(ctx, "b")
	assert.False(t, ok, "b should be evicted")
}

func TestLRU_TTLExpiry(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(10, time.Minute).(*lruCache)
	require.True(t, ok)
	ctx := context.Background()

	now := time.Now()
	c.now = func() time.Time { return now }

	c.Set(ctx, "a", makeIdentity("did:plc:a"))

	_, ok = c.Get(ctx, "a")
	assert.True(t, ok)

	// Advance past TTL.
	c.now = func() time.Time { return now.Add(2 * time.Minute) }

	_, ok = c.Get(ctx, "a")
	assert.False(t, ok, "should be expired")
}

func TestLRU_UpdateExisting(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(10, time.Hour).(*lruCache)
	require.True(t, ok)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:old"))
	c.Set(ctx, "a", makeIdentity("did:plc:new"))

	got, ok := c.Get(ctx, "a")
	require.True(t, ok)
	assert.Equal(t, atmos.DID("did:plc:new"), got.DID)
	assert.Equal(t, 1, len(c.items))
}

func TestLRU_Delete(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(10, time.Hour).(*lruCache)
	require.True(t, ok)
	ctx := context.Background()

	c.Set(ctx, "a", makeIdentity("did:plc:a"))
	c.Delete(ctx, "a")

	_, ok = c.Get(ctx, "a")
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

func TestLRU_MinCapacity(t *testing.T) {
	t.Parallel()
	c, ok := NewLRUCache(0, time.Hour).(*lruCache)
	require.True(t, ok)
	assert.Equal(t, 1, c.capacity)
}
