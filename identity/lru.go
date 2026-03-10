package identity

import (
	"context"
	"sync"
	"time"
)

// lruNode is an entry in the doubly-linked list.
type lruNode struct {
	key     string
	val     *Identity
	expires time.Time
	prev    *lruNode
	next    *lruNode
}

// lruCache is a thread-safe LRU cache with TTL-based expiration.
type lruCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	items    map[string]*lruNode
	head     *lruNode // most recently used
	tail     *lruNode // least recently used
	now      func() time.Time
}

// NewLRUCache returns a Cache backed by an in-memory LRU with the given
// capacity and TTL. Capacity must be >= 1.
func NewLRUCache(capacity int, ttl time.Duration) Cache {
	if capacity < 1 {
		capacity = 1
	}
	return &lruCache{
		capacity: capacity,
		ttl:      ttl,
		items:    make(map[string]*lruNode),
		now:      time.Now,
	}
}

func (c *lruCache) Get(_ context.Context, key string) (*Identity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if c.now().After(node.expires) {
		c.removeLocked(node)
		return nil, false
	}
	c.moveToFrontLocked(node)
	return node.val, true
}

func (c *lruCache) Set(_ context.Context, key string, val *Identity) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.items[key]; ok {
		node.val = val
		node.expires = c.now().Add(c.ttl)
		c.moveToFrontLocked(node)
		return
	}

	// Evict if at capacity.
	for len(c.items) >= c.capacity {
		c.removeLocked(c.tail)
	}

	node := &lruNode{
		key:     key,
		val:     val,
		expires: c.now().Add(c.ttl),
	}
	c.items[key] = node
	c.pushFrontLocked(node)
}

func (c *lruCache) Delete(_ context.Context, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.items[key]; ok {
		c.removeLocked(node)
	}
}

func (c *lruCache) removeLocked(node *lruNode) {
	delete(c.items, node.key)
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
}

func (c *lruCache) moveToFrontLocked(node *lruNode) {
	if c.head == node {
		return
	}
	// Unlink.
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
	// Push front.
	c.pushFrontLocked(node)
}

func (c *lruCache) pushFrontLocked(node *lruNode) {
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
	if c.tail == nil {
		c.tail = node
	}
}
