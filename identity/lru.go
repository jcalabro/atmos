package identity

import (
	"context"
	"time"

	"github.com/jcalabro/atmos/internal/lru"
)

// lruCache is a thread-safe LRU cache backed by [internal/lru.Cache].
// Adapts the generic primitive to the [Cache] interface.
type lruCache struct {
	c *lru.Cache[string, *Identity]
}

// NewLRUCache returns a Cache backed by an in-memory LRU with the
// given capacity and TTL. Capacity values <1 are clamped to 1.
func NewLRUCache(capacity int, ttl time.Duration) Cache {
	return &lruCache{c: lru.New[string, *Identity](capacity, ttl)}
}

func (c *lruCache) Get(_ context.Context, key string) (*Identity, bool) {
	return c.c.Get(key)
}

func (c *lruCache) Set(_ context.Context, key string, val *Identity) {
	c.c.Set(key, val)
}

func (c *lruCache) Delete(_ context.Context, key string) {
	c.c.Delete(key)
}
