// Package lru is an internal generic LRU cache used by atmos for
// bounded per-key state — identity caches, per-DID mutexes,
// per-DID rate limiters. Concurrency-safe; entries support an optional
// TTL and an optional pin count that vetoes eviction.
//
// Pinning matters when an entry's identity is what makes correctness
// hold (e.g. a [sync.Mutex] used to serialize work for a key).
// Evicting a pinned entry would let two callers acquire two different
// mutexes for the same key, breaking serialization. Pinned entries
// are skipped during eviction; if every entry is pinned the cache
// soft-overflows past its capacity until callers Unpin.
package lru

import (
	"container/list"
	"sync"
	"time"
)

// Cache is a thread-safe size-bounded LRU with optional TTL and
// optional per-entry pinning.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	items    map[K]*list.Element
	order    *list.List // front = most recent, back = least recent
	now      func() time.Time
}

// entry is the value stored in each list element.
type entry[K comparable, V any] struct {
	key     K
	val     V
	expires time.Time // zero = never expires
	refs    int       // pin count; >0 vetoes eviction
}

// New constructs a Cache with the given capacity and optional TTL.
// Capacity values <1 are clamped to 1. A ttl of 0 disables expiration.
func New[K comparable, V any](capacity int, ttl time.Duration) *Cache[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &Cache[K, V]{
		capacity: capacity,
		ttl:      ttl,
		items:    make(map[K]*list.Element),
		order:    list.New(),
		now:      time.Now,
	}
}

// Capacity returns the configured capacity. Pinned entries can push
// transient size above this watermark; see [Cache.Len].
func (c *Cache[K, V]) Capacity() int {
	return c.capacity
}

// Len returns the number of entries currently in the cache, including
// any soft-overflow caused by pinning.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Get returns the cached value and true on hit, or zero and false on
// miss or expiry. A hit promotes the entry to most-recently-used.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getLocked(key)
}

func (c *Cache[K, V]) getLocked(key K) (V, bool) {
	var zero V
	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if c.expired(e) {
		c.removeLocked(el)
		return zero, false
	}
	c.order.MoveToFront(el)
	return e.val, true
}

// Set stores val for key. Replaces any existing entry. Updates the
// LRU position to most-recently-used. Triggers eviction if size
// exceeds capacity.
//
// If an existing entry for key is pinned, Set updates the value but
// does not change the pin count.
func (c *Cache[K, V]) Set(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry[K, V])
		e.val = val
		e.expires = c.expiryLocked()
		c.order.MoveToFront(el)
		return
	}

	e := &entry[K, V]{
		key:     key,
		val:     val,
		expires: c.expiryLocked(),
	}
	el := c.order.PushFront(e)
	c.items[key] = el
	c.evictLocked()
}

// GetOrAdd returns the existing entry for key, or creates one by
// invoking factory under the cache's lock and inserting it. The
// returned bool is true when an existing entry was found, false when
// a new one was created. The factory must not call back into the
// same cache: it runs while the lock is held.
func (c *Cache[K, V]) GetOrAdd(key K, factory func() V) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.getLocked(key); ok {
		return v, true
	}
	v := factory()
	e := &entry[K, V]{
		key:     key,
		val:     v,
		expires: c.expiryLocked(),
	}
	el := c.order.PushFront(e)
	c.items[key] = el
	c.evictLocked()
	return v, false
}

// Pin increments the refcount on key, vetoing eviction. Returns the
// value and true on hit, or the zero value and false if key is absent.
// A pinned entry stays in the cache until Unpin returns the count
// to zero AND the entry is selected for eviction.
//
// Hits do NOT promote the entry's LRU position — pinning is an
// orthogonal signal.
func (c *Cache[K, V]) Pin(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if c.expired(e) {
		c.removeLocked(el)
		return zero, false
	}
	e.refs++
	return e.val, true
}

// PinOrAdd is GetOrAdd combined with Pin: returns (value, true) on hit
// or (newly-created value, false) on miss, with the pin count
// incremented in both cases. Use to atomically obtain-and-pin.
func (c *Cache[K, V]) PinOrAdd(key K, factory func() V) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry[K, V])
		if !c.expired(e) {
			c.order.MoveToFront(el)
			e.refs++
			return e.val, true
		}
		c.removeLocked(el)
	}
	v := factory()
	e := &entry[K, V]{
		key:     key,
		val:     v,
		expires: c.expiryLocked(),
		refs:    1,
	}
	el := c.order.PushFront(e)
	c.items[key] = el
	c.evictLocked()
	return v, false
}

// Unpin decrements the refcount on key. Calling Unpin without a
// matching Pin is a usage error and panics: the cache is internal
// state and Pin/Unpin pairing is the caller's responsibility.
func (c *Cache[K, V]) Unpin(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		// Entry was evicted while pinned? Impossible: eviction skips
		// pinned entries. If we get here it's a Pin/Unpin mismatch.
		panic("lru: Unpin of unknown key")
	}
	e := el.Value.(*entry[K, V])
	if e.refs <= 0 {
		panic("lru: Unpin without matching Pin")
	}
	e.refs--
}

// Delete removes the entry for key, if present. Pinned entries can be
// deleted: callers that hold a reference to the value (e.g. a *sync.Mutex)
// may continue to use it; the entry is just removed from the cache.
// This makes Delete a hard remove, not a soft one.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeLocked(el)
	}
}

// SetNow overrides the wall clock used for TTL evaluation. For
// deterministic tests; not safe to call concurrently with Get/Set.
func (c *Cache[K, V]) SetNow(fn func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = fn
}

// evictLocked drops oldest unpinned entries until size is at or below
// capacity. If every entry past the watermark is pinned, the cache
// soft-overflows.
func (c *Cache[K, V]) evictLocked() {
	for len(c.items) > c.capacity {
		// Walk back→front, evict first unpinned candidate.
		var victim *list.Element
		for el := c.order.Back(); el != nil; el = el.Prev() {
			e := el.Value.(*entry[K, V])
			if e.refs == 0 {
				victim = el
				break
			}
		}
		if victim == nil {
			// All entries pinned; soft-overflow.
			return
		}
		c.removeLocked(victim)
	}
}

// removeLocked drops el from both the order list and the items map.
func (c *Cache[K, V]) removeLocked(el *list.Element) {
	e := el.Value.(*entry[K, V])
	delete(c.items, e.key)
	c.order.Remove(el)
}

// expired reports whether e has passed its TTL. Always false when
// TTL is disabled (ttl == 0).
func (c *Cache[K, V]) expired(e *entry[K, V]) bool {
	if c.ttl == 0 {
		return false
	}
	return c.now().After(e.expires)
}

// expiryLocked computes the expiry timestamp for a fresh entry, or
// the zero time when TTL is disabled.
func (c *Cache[K, V]) expiryLocked() time.Time {
	if c.ttl == 0 {
		return time.Time{}
	}
	return c.now().Add(c.ttl)
}
