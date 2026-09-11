package declaration

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrCacheMiss indicates that a declaration cache has no live entry.
	ErrCacheMiss = errors.New("space declaration: cache miss")
	// ErrNotFound indicates that the configured resolver authoritatively did not find a declaration.
	ErrNotFound = errors.New("space declaration: not found")
	// ErrCacheEntryTooLarge indicates that one entry exceeds a cache's byte bound.
	ErrCacheEntryTooLarge = errors.New("space declaration: cache entry too large")
)

// CacheValue is either a resolved declaration or an explicit negative result.
type CacheValue struct {
	Resolved *Resolved
	NotFound bool
}

// Cache is an error-returning declaration cache. Get returns ErrCacheMiss for a miss.
type Cache interface {
	Get(ctx context.Context, key string) (CacheValue, error)
	Set(ctx context.Context, key string, value CacheValue, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

type memoryEntry struct {
	key     string
	value   CacheValue
	size    int
	expires time.Time
}

// MemoryCache is a concurrency-safe LRU bounded by both entries and bytes.
type MemoryCache struct {
	mu         sync.Mutex
	entries    map[string]*list.Element
	lru        *list.List
	maxEntries int
	maxBytes   int
	bytes      int
	now        func() time.Time
}

// NewMemoryCache creates a bounded in-memory declaration cache.
func NewMemoryCache(maxEntries, maxBytes int) (*MemoryCache, error) {
	if maxEntries < 1 || maxBytes < 1 {
		return nil, fmt.Errorf("space declaration: cache bounds must be positive")
	}
	return &MemoryCache{
		entries: make(map[string]*list.Element), lru: list.New(),
		maxEntries: maxEntries, maxBytes: maxBytes, now: time.Now,
	}, nil
}

// Get implements Cache.
func (c *MemoryCache) Get(ctx context.Context, key string) (CacheValue, error) {
	if err := ctx.Err(); err != nil {
		return CacheValue{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return CacheValue{}, ErrCacheMiss
	}
	entry, ok := element.Value.(*memoryEntry)
	if !ok {
		panic("space declaration: corrupt LRU entry")
	}
	if !c.now().Before(entry.expires) {
		c.remove(element)
		return CacheValue{}, ErrCacheMiss
	}
	c.lru.MoveToFront(element)
	return cloneCacheValue(entry.value), nil
}

// Set implements Cache.
func (c *MemoryCache) Set(ctx context.Context, key string, value CacheValue, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == "" || ttl <= 0 || value.NotFound == (value.Resolved != nil) {
		return fmt.Errorf("space declaration: invalid cache entry")
	}
	value = cloneCacheValue(value)
	size := cacheValueSize(key, value)
	if size > c.maxBytes {
		return ErrCacheEntryTooLarge
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.entries[key]; ok {
		c.remove(old)
	}
	entry := &memoryEntry{key: key, value: value, size: size, expires: c.now().Add(ttl)}
	element := c.lru.PushFront(entry)
	c.entries[key] = element
	c.bytes += size
	for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
		c.remove(c.lru.Back())
	}
	return nil
}

// Delete implements Cache.
func (c *MemoryCache) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.remove(element)
	}
	return nil
}

func (c *MemoryCache) remove(element *list.Element) {
	if element == nil {
		return
	}
	entry, ok := element.Value.(*memoryEntry)
	if !ok {
		panic("space declaration: corrupt LRU entry")
	}
	delete(c.entries, entry.key)
	c.bytes -= entry.size
	c.lru.Remove(element)
}

func cloneCacheValue(value CacheValue) CacheValue {
	value.Resolved = cloneResolved(value.Resolved)
	return value
}

func cacheValueSize(key string, value CacheValue) int {
	size := len(key) + 1
	if value.Resolved == nil {
		return size
	}
	size += len(value.Resolved.URI) + len(value.Resolved.CID)
	declaration := value.Resolved.Declaration
	if declaration == nil {
		return size
	}
	size += len(declaration.Type) + len(declaration.Name) + len(declaration.Description) + len(declaration.Key)
	for language, name := range declaration.Names {
		size += len(language) + len(name)
	}
	for _, collection := range declaration.Collections {
		size += len(collection)
	}
	return size
}
