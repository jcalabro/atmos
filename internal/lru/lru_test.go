package lru_test

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos/internal/lru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_GetSetMissingHit(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)

	_, ok := c.Get("a")
	assert.False(t, ok)

	c.Set("a", 1)

	got, ok := c.Get("a")
	require.True(t, ok)
	assert.Equal(t, 1, got)
}

func TestCache_EvictsOldest(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](2, 0)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3) // evicts a

	_, ok := c.Get("a")
	assert.False(t, ok, "a should be evicted")
	_, ok = c.Get("b")
	assert.True(t, ok)
	_, ok = c.Get("c")
	assert.True(t, ok)
}

func TestCache_PromotionOnGet(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](2, 0)
	c.Set("a", 1)
	c.Set("b", 2)
	// Touch a, making b the LRU.
	c.Get("a")
	c.Set("c", 3) // evicts b

	_, ok := c.Get("a")
	assert.True(t, ok, "a should survive (recently used)")
	_, ok = c.Get("b")
	assert.False(t, ok)
}

func TestCache_TTLExpiry(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, time.Minute)
	now := time.Now()
	c.SetNow(func() time.Time { return now })

	c.Set("a", 1)
	_, ok := c.Get("a")
	require.True(t, ok)

	c.SetNow(func() time.Time { return now.Add(2 * time.Minute) })
	_, ok = c.Get("a")
	assert.False(t, ok, "should be expired")
}

func TestCache_TTLDisabledByZero(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	c.Set("a", 1)
	c.SetNow(func() time.Time { return time.Now().Add(100 * 365 * 24 * time.Hour) })
	_, ok := c.Get("a")
	assert.True(t, ok, "ttl=0 disables expiration")
}

func TestCache_UpdateExisting(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	c.Set("a", 1)
	c.Set("a", 2)
	got, ok := c.Get("a")
	require.True(t, ok)
	assert.Equal(t, 2, got)
	assert.Equal(t, 1, c.Len())
}

func TestCache_Delete(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	c.Set("a", 1)
	c.Delete("a")
	_, ok := c.Get("a")
	assert.False(t, ok)

	// Idempotent: deleting absent key is a no-op.
	c.Delete("nonexistent")
}

func TestCache_MinCapacity(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](0, 0)
	assert.Equal(t, 1, c.Capacity(), "capacity below 1 must be clamped")
}

func TestCache_GetOrAddCreatesOnMiss(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	calls := 0
	v, found := c.GetOrAdd("a", func() int {
		calls++
		return 42
	})
	assert.False(t, found, "expected miss on first call")
	assert.Equal(t, 42, v)
	assert.Equal(t, 1, calls)

	v2, found2 := c.GetOrAdd("a", func() int {
		calls++
		return 999
	})
	assert.True(t, found2, "expected hit on second call")
	assert.Equal(t, 42, v2, "factory must not run on hit")
	assert.Equal(t, 1, calls)
}

// TestCache_PinVetoesEviction is the headline pinning test: a pinned
// entry survives even when adding new entries would normally evict
// the LRU. The cache soft-overflows past capacity.
func TestCache_PinVetoesEviction(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](2, 0)
	c.Set("a", 1)
	c.Set("b", 2)

	// Pin a — must not be evicted on subsequent inserts.
	val, ok := c.Pin("a")
	require.True(t, ok)
	assert.Equal(t, 1, val)

	c.Set("c", 3)
	// Even though "a" was the LRU, it must still be present.
	got, ok := c.Get("a")
	require.True(t, ok, "pinned entry must not be evicted")
	assert.Equal(t, 1, got)

	// b was the next-LRU candidate and should have been evicted.
	_, ok = c.Get("b")
	assert.False(t, ok, "unpinned LRU entry should have been evicted in a's place")

	// Soft-overflow check: a, c are present; that's 2 entries at cap=2.
	assert.Equal(t, 2, c.Len())

	c.Unpin("a")
	// After unpin, a is now eligible for eviction.
	c.Set("d", 4)
	c.Set("e", 5)
	_, ok = c.Get("a")
	assert.False(t, ok, "a should be evictable after Unpin")
}

func TestCache_PinReturnsValue(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	c.Set("k", 7)
	v, ok := c.Pin("k")
	require.True(t, ok)
	assert.Equal(t, 7, v)
	c.Unpin("k")
}

func TestCache_PinMissingKey(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	_, ok := c.Pin("absent")
	assert.False(t, ok)
}

func TestCache_PinExpiredEntryEvictsAndMisses(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, time.Minute)
	now := time.Now()
	c.SetNow(func() time.Time { return now })

	c.Set("a", 1)

	c.SetNow(func() time.Time { return now.Add(2 * time.Minute) })

	_, ok := c.Pin("a")
	assert.False(t, ok, "pinning an expired entry must report a miss")

	// And confirm it's been removed.
	c.SetNow(func() time.Time { return now })
	_, ok = c.Get("a")
	assert.False(t, ok)
}

func TestCache_PinOrAddCreatesAndPins(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](2, 0)
	v, found := c.PinOrAdd("a", func() int { return 1 })
	assert.False(t, found)
	assert.Equal(t, 1, v)

	c.Set("b", 2)
	c.Set("c", 3) // would evict a, but a is pinned

	got, ok := c.Get("a")
	assert.True(t, ok, "PinOrAdd must pin the newly-created entry")
	assert.Equal(t, 1, got)

	c.Unpin("a")
}

func TestCache_PinOrAddExistingEntryIncrementsRefs(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](2, 0)
	c.Set("a", 1)

	v1, found1 := c.PinOrAdd("a", func() int {
		t.Fatal("factory must not run for existing entry")
		return 0
	})
	require.True(t, found1)
	assert.Equal(t, 1, v1)

	v2, found2 := c.PinOrAdd("a", func() int {
		t.Fatal("factory must not run for existing entry")
		return 0
	})
	require.True(t, found2)
	assert.Equal(t, 1, v2)

	// Two pins outstanding; eviction on overflow must not remove a.
	c.Set("b", 2)
	c.Set("c", 3)
	_, ok := c.Get("a")
	assert.True(t, ok)

	c.Unpin("a")
	c.Unpin("a")
}

func TestCache_UnpinPanicsOnUnknownKey(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	assert.Panics(t, func() { c.Unpin("absent") })
}

func TestCache_UnpinPanicsOnUnpinnedKey(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](10, 0)
	c.Set("a", 1)
	assert.Panics(t, func() { c.Unpin("a") })
}

// TestCache_AllPinnedSoftOverflows asserts the soft-cap behavior:
// when every entry is pinned, the cache exceeds its capacity rather
// than incorrectly evicting a held entry.
func TestCache_AllPinnedSoftOverflows(t *testing.T) {
	t.Parallel()
	c := lru.New[string, int](2, 0)

	_, _ = c.PinOrAdd("a", func() int { return 1 })
	_, _ = c.PinOrAdd("b", func() int { return 2 })
	// At cap with both pinned. Adding a third must succeed and grow the cache.
	_, _ = c.PinOrAdd("c", func() int { return 3 })

	assert.Equal(t, 3, c.Len(), "all-pinned cap must soft-overflow")

	for _, k := range []string{"a", "b", "c"} {
		_, ok := c.Get(k)
		assert.True(t, ok, "%s must still be present under soft-overflow", k)
	}

	// Unpin everything and trigger a settling eviction.
	c.Unpin("a")
	c.Unpin("b")
	c.Unpin("c")
	c.Set("d", 4)
	// After settling, cap should be honored: 2 entries, oldest evicted.
	assert.LessOrEqual(t, c.Len(), 2)
}

// TestCache_DeleteOfPinnedEntry exercises the documented escape hatch:
// Delete is hard, so a pinned entry can be removed from the cache
// while a caller still holds a reference to the value. The
// previously-cached value remains usable; future GetOrAdd creates a
// fresh entry.
func TestCache_DeleteOfPinnedEntry(t *testing.T) {
	t.Parallel()
	c := lru.New[string, *int](2, 0)

	first := 1
	v, _ := c.PinOrAdd("a", func() *int { return &first })
	assert.Equal(t, &first, v)

	c.Delete("a") // hard delete despite the pin

	_, ok := c.Get("a")
	assert.False(t, ok, "Delete is hard")

	// A subsequent Pin or GetOrAdd produces a different entry.
	second := 2
	v2, _ := c.PinOrAdd("a", func() *int { return &second })
	assert.NotSame(t, v, v2, "post-Delete factory should run and produce a fresh value")
}

// TestCache_Concurrent fuzzes Pin/Unpin/Get/Set/GetOrAdd from many
// goroutines. The race detector and the usage-error panics cover
// correctness; we just require that nothing crashes.
func TestCache_Concurrent(t *testing.T) {
	t.Parallel()
	c := lru.New[int, int](32, 0)

	const goroutines = 16
	const iters = 500

	var wg sync.WaitGroup
	var pinErrors atomic.Int64
	for g := range goroutines {
		wg.Go(func() {
			for i := range iters {
				k := (g*iters + i) % 64
				switch i % 4 {
				case 0:
					c.Set(k, i)
				case 1:
					_, _ = c.Get(k)
				case 2:
					_, _ = c.GetOrAdd(k, func() int { return i })
				case 3:
					if _, ok := c.Pin(k); ok {
						c.Unpin(k)
					} else {
						pinErrors.Add(1) // tracked but not failed
					}
				}
			}
		})
	}
	wg.Wait()

	// Sanity: cache hasn't blown up. With cap=32 and no leaked pins,
	// size should be at the watermark.
	assert.LessOrEqual(t, c.Len(), 32)
}

// TestCache_BoundedGrowthBenchmarkShape exercises the headline use
// case: insert N >> capacity entries, asserting that the cache size
// never exceeds capacity (no soft-overflow when nothing is pinned).
func TestCache_BoundedGrowthBenchmarkShape(t *testing.T) {
	t.Parallel()
	const cap = 100
	c := lru.New[string, int](cap, 0)
	for i := range 10_000 {
		c.Set("k"+strconv.Itoa(i), i)
		require.LessOrEqual(t, c.Len(), cap)
	}
	assert.Equal(t, cap, c.Len())
}
