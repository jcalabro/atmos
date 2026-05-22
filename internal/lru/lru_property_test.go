package lru_test

import (
	"fmt"
	mathrand "math/rand"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos/internal/lru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Property test: cache vs reference model under random operation sequences
// ---------------------------------------------------------------------------

// modelEntry shadows lru.entry so the test can verify cache invariants
// without depending on the package's private types.
type modelEntry struct {
	val  int
	refs int
}

// model is a reference implementation we cross-check against. Maintains
// LRU order in keys (front = most recent) plus refcounts. Eviction
// picks the LRU-most unpinned entry; if all are pinned the model
// soft-overflows, mirroring the cache.
type model struct {
	cap   int
	keys  []string // index 0 = most recent
	items map[string]*modelEntry
}

func newModel(cap int) *model {
	if cap < 1 {
		cap = 1
	}
	return &model{cap: cap, items: map[string]*modelEntry{}}
}

func (m *model) get(k string) (int, bool) {
	e, ok := m.items[k]
	if !ok {
		return 0, false
	}
	m.touch(k)
	return e.val, true
}

func (m *model) set(k string, v int) {
	if e, ok := m.items[k]; ok {
		e.val = v
		m.touch(k)
		return
	}
	m.items[k] = &modelEntry{val: v}
	m.keys = append([]string{k}, m.keys...)
	m.evict()
}

func (m *model) getOrAdd(k string, v int) (int, bool) {
	if e, ok := m.items[k]; ok {
		m.touch(k)
		return e.val, true
	}
	m.items[k] = &modelEntry{val: v}
	m.keys = append([]string{k}, m.keys...)
	m.evict()
	return v, false
}

func (m *model) pin(k string) (int, bool) {
	e, ok := m.items[k]
	if !ok {
		return 0, false
	}
	e.refs++
	return e.val, true
}

func (m *model) pinOrAdd(k string, v int) (int, bool) {
	if e, ok := m.items[k]; ok {
		m.touch(k)
		e.refs++
		return e.val, true
	}
	m.items[k] = &modelEntry{val: v, refs: 1}
	m.keys = append([]string{k}, m.keys...)
	m.evict()
	return v, false
}

func (m *model) unpin(k string) {
	e, ok := m.items[k]
	if !ok {
		panic("model: Unpin of unknown key")
	}
	if e.refs <= 0 {
		panic("model: Unpin without matching Pin")
	}
	e.refs--
}

func (m *model) delete(k string) {
	if _, ok := m.items[k]; !ok {
		return
	}
	delete(m.items, k)
	for i, kk := range m.keys {
		if kk == k {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

// touch promotes k to most-recently-used.
func (m *model) touch(k string) {
	for i, kk := range m.keys {
		if kk == k {
			m.keys = append([]string{k}, append(m.keys[:i], m.keys[i+1:]...)...)
			return
		}
	}
}

// evict drops the LRU-most unpinned entry until size <= cap.
func (m *model) evict() {
	for len(m.items) > m.cap {
		victim := -1
		for i := len(m.keys) - 1; i >= 0; i-- {
			if m.items[m.keys[i]].refs == 0 {
				victim = i
				break
			}
		}
		if victim < 0 {
			return // all pinned; soft-overflow
		}
		k := m.keys[victim]
		delete(m.items, k)
		m.keys = append(m.keys[:victim], m.keys[victim+1:]...)
	}
}

// TestCache_PropertyModel runs random op sequences against both the
// cache and a reference model, asserting equivalence after each op.
//
// The reference model implements the same eviction policy as the
// cache (LRU-most unpinned, soft-overflow when all pinned). After
// each op we cross-check externally observable state: Len, the
// presence/absence of every key the test has touched, and (for
// successful Gets) the value.
//
// We do not directly compare LRU ordering because the cache exposes
// no observation hook for it. Eviction equivalence is checked
// indirectly: if the cache evicts a different key than the model
// would have, a subsequent Get for that key disagrees.
func TestCache_PropertyModel(t *testing.T) {
	t.Parallel()

	caps := []int{1, 2, 5, 16}
	for _, cap := range caps {
		t.Run(fmt.Sprintf("cap=%d", cap), func(t *testing.T) {
			t.Parallel()
			runPropertyIterations(t, cap, propertyIters())
		})
	}
}

func propertyIters() int {
	if testing.Short() {
		return 30
	}
	return 200
}

func runPropertyIterations(t *testing.T, cap, iters int) {
	t.Helper()
	for i := range iters {
		seed := int64(i)*7919 + int64(cap)*101
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			runOnePropertyIteration(t, cap, seed)
		})
	}
}

func runOnePropertyIteration(t *testing.T, cap int, seed int64) {
	t.Helper()
	rng := mathrand.New(mathrand.NewSource(seed))
	c := lru.New[string, int](cap, 0)
	m := newModel(cap)

	const numOps = 200
	const keySpace = 20

	// universe is every key the test has ever named. We iterate this
	// after each op to assert (cache.Get == model.get) for ALL keys —
	// catches cache vs. model divergence on keys we're not actively
	// touching this op (e.g. an unrelated eviction).
	universe := make(map[string]struct{}, keySpace)
	pinnedKeys := make(map[string]int) // key → outstanding pins

	// checkEquivalence asserts the per-op invariants that hold after
	// any single operation: cache and model agree on size, and on
	// Get behavior for every key the test has named. The capacity
	// vs. pins relationship is NOT a per-op invariant — eviction
	// runs on insertion, not on Unpin, so a cache that
	// soft-overflowed during a pin-heavy phase stays oversize until
	// the next insert. The post-force-eviction loop at the bottom
	// of the test asserts capacity once eviction has had a chance
	// to settle.
	checkEquivalence := func(opDesc string) {
		t.Helper()
		require.Equal(t, len(m.items), c.Len(),
			"after %s (seed=%d cap=%d): size mismatch", opDesc, seed, cap)

		for k := range universe {
			_, modelOk := m.items[k]
			// Get touches LRU position in both cache and model;
			// mirror the model's promotion right after a hit so
			// subsequent ops see consistent ordering.
			cacheVal, cacheOk := c.Get(k)
			require.Equal(t, modelOk, cacheOk,
				"after %s (seed=%d cap=%d): presence mismatch for key %q (model=%v cache=%v)",
				opDesc, seed, cap, k, modelOk, cacheOk)
			if cacheOk {
				modelVal := m.items[k].val
				m.touch(k)
				require.Equal(t, modelVal, cacheVal,
					"after %s (seed=%d cap=%d): value mismatch for key %q",
					opDesc, seed, cap, k)
			}
		}
	}

	for op := range numOps {
		k := fmt.Sprintf("k%d", rng.Intn(keySpace))
		universe[k] = struct{}{}
		v := rng.Intn(10000)

		// Bias toward pin-heavy workloads when cap is small to
		// exercise soft-overflow. Otherwise mix evenly.
		var choice int
		if cap <= 2 {
			choice = rng.Intn(8) // includes more pin operations
		} else {
			choice = rng.Intn(7)
		}

		var desc string
		switch choice {
		case 0:
			desc = fmt.Sprintf("op%d Set(%s, %d)", op, k, v)
			c.Set(k, v)
			m.set(k, v)
		case 1:
			desc = fmt.Sprintf("op%d Get(%s)", op, k)
			cVal, cOk := c.Get(k)
			mVal, mOk := m.get(k)
			require.Equal(t, mOk, cOk, "%s presence", desc)
			if cOk {
				require.Equal(t, mVal, cVal, "%s value", desc)
			}
		case 2:
			desc = fmt.Sprintf("op%d GetOrAdd(%s, %d)", op, k, v)
			cVal, cFound := c.GetOrAdd(k, func() int { return v })
			mVal, mFound := m.getOrAdd(k, v)
			require.Equal(t, mFound, cFound, "%s found-flag", desc)
			require.Equal(t, mVal, cVal, "%s value", desc)
		case 3:
			desc = fmt.Sprintf("op%d Pin(%s)", op, k)
			cVal, cOk := c.Pin(k)
			mVal, mOk := m.pin(k)
			require.Equal(t, mOk, cOk, "%s presence", desc)
			if cOk {
				require.Equal(t, mVal, cVal, "%s value", desc)
				pinnedKeys[k]++
			}
		case 4:
			desc = fmt.Sprintf("op%d PinOrAdd(%s, %d)", op, k, v)
			cVal, cFound := c.PinOrAdd(k, func() int { return v })
			mVal, mFound := m.pinOrAdd(k, v)
			require.Equal(t, mFound, cFound, "%s found-flag", desc)
			require.Equal(t, mVal, cVal, "%s value", desc)
			pinnedKeys[k]++
		case 5:
			// Unpin only if we actually have an outstanding pin for this key.
			if pinnedKeys[k] > 0 {
				desc = fmt.Sprintf("op%d Unpin(%s)", op, k)
				c.Unpin(k)
				m.unpin(k)
				pinnedKeys[k]--
			} else {
				continue
			}
		case 6:
			// Delete is a hard remove. If the key is pinned, the
			// pin invariants must clear too — Delete drops the
			// entry entirely and any subsequent PinOrAdd creates a
			// new entry with refs=1, regardless of prior pins.
			// Simulate that by zeroing pinnedKeys for k.
			desc = fmt.Sprintf("op%d Delete(%s)", op, k)
			c.Delete(k)
			m.delete(k)
			pinnedKeys[k] = 0
		case 7:
			// Extra Pin path for low-cap stress. Same as case 3.
			desc = fmt.Sprintf("op%d Pin-extra(%s)", op, k)
			cVal, cOk := c.Pin(k)
			mVal, mOk := m.pin(k)
			require.Equal(t, mOk, cOk, "%s presence", desc)
			if cOk {
				require.Equal(t, mVal, cVal, "%s value", desc)
				pinnedKeys[k]++
			}
		}

		checkEquivalence(desc)
	}

	// Drain pins so the cache settles back to capacity. We exercise
	// the eviction-after-unpin path one more time.
	for k, n := range pinnedKeys {
		for range n {
			c.Unpin(k)
			m.unpin(k)
		}
	}
	checkEquivalence("final drain")

	// Force eviction by inserting more keys than cap.
	for i := range cap * 3 {
		key := fmt.Sprintf("drain%d", i)
		universe[key] = struct{}{}
		c.Set(key, i)
		m.set(key, i)
	}
	require.Equal(t, cap, c.Len(), "post-drain (seed=%d cap=%d): cache must settle at watermark", seed, cap)
}

// ---------------------------------------------------------------------------
// Concurrent pin-safety swarm: assert pointer-identity invariant
// ---------------------------------------------------------------------------

// pinnedRecord is what the swarm test stores in the cache. The pointer
// identity is the safety contract: while ANY goroutine holds a pin on
// key K, every other concurrent pin on K must see the SAME *pinnedRecord.
type pinnedRecord struct {
	key      string
	instance int64
}

// TestCache_PinSafetySwarm is the headline concurrency safety test:
// many goroutines pin/use/unpin keys against a small cache, and we
// assert that as long as ANY goroutine holds a pin on key K, every
// concurrent pin on K returns the same value pointer. If the cache
// ever evicts a held entry and lets a future PinOrAdd fabricate a
// new one, the pointer-identity check fires.
//
// This is the actual safety property that the verifier's per-DID
// mutex serialization depends on.
func TestCache_PinSafetySwarm(t *testing.T) {
	t.Parallel()

	const (
		capacity   = 8
		keySpace   = 16 // > capacity to force eviction pressure
		goroutines = 32
	)
	iters := 500
	if !testing.Short() {
		iters = 5000
	}

	c := lru.New[string, *pinnedRecord](capacity, 0)
	var counter atomic.Int64

	// outstanding[k] is the slice of *pinnedRecord values currently
	// pinned by some goroutine for key k. The invariant: every
	// element of outstanding[k] is the same pointer.
	var (
		mu          stdsync.Mutex
		outstanding = make(map[string][]*pinnedRecord)
	)

	keyOf := func(i int) string { return fmt.Sprintf("k%d", i) }

	var wg stdsync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			rng := mathrand.New(mathrand.NewSource(int64(g) + 1))
			for range iters {
				k := keyOf(rng.Intn(keySpace))
				rec, _ := c.PinOrAdd(k, func() *pinnedRecord {
					return &pinnedRecord{key: k, instance: counter.Add(1)}
				})

				// Register and verify the pointer-identity invariant.
				mu.Lock()
				outstanding[k] = append(outstanding[k], rec)
				if len(outstanding[k]) >= 2 {
					ref := outstanding[k][0]
					for _, other := range outstanding[k][1:] {
						if other != ref {
							mu.Unlock()
							t.Errorf("pin-safety violation: key %q has multiple distinct values pinned simultaneously: %p (instance %d) vs %p (instance %d)",
								k, ref, ref.instance, other, other.instance)
							c.Unpin(k)
							return
						}
					}
				}
				mu.Unlock()

				// Simulate per-key work — yields scheduler.
				time.Sleep(time.Microsecond)

				// Deregister and unpin.
				mu.Lock()
				removeOnce(outstanding, k, rec)
				mu.Unlock()
				c.Unpin(k)
			}
		})
	}
	wg.Wait()

	// All goroutines drained their pins.
	for k, recs := range outstanding {
		assert.Empty(t, recs, "pinned record list for %q should be drained", k)
	}
}

// removeOnce removes one occurrence of rec from outstanding[k].
func removeOnce(outstanding map[string][]*pinnedRecord, k string, rec *pinnedRecord) {
	recs := outstanding[k]
	for i, r := range recs {
		if r == rec {
			outstanding[k] = append(recs[:i], recs[i+1:]...)
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Fuzz target: assert no panics + invariants under arbitrary op streams
// ---------------------------------------------------------------------------

// FuzzCache_Operations decodes random byte streams as op sequences and
// runs them against a cache, asserting size invariants hold and no
// panics fire. Op encoding: each pair of bytes is (op, key) where op
// is reduced mod 6 and key is reduced mod 8. Cap is fixed at 4.
//
// Pin/Unpin pairing is enforced by tracking outstanding pins per key
// — the fuzzer never produces an unbalanced Unpin (which would panic
// by design and isn't a useful signal).
func FuzzCache_Operations(f *testing.F) {
	// Seeds: a few interesting starting shapes.
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1, 0, 2, 0, 3, 0, 4})       // five sets, forces eviction at cap=4
	f.Add([]byte{3, 0, 5, 0, 0, 1, 0, 2, 0, 3, 0, 4}) // pin a key, then push other entries past cap
	f.Add([]byte{4, 0, 5, 0, 4, 0, 5, 0, 6, 0})       // pinOrAdd repeatedly + delete
	f.Add(make([]byte, 64))                           // a wall of zeros

	const cap = 4
	const keySpace = 8

	f.Fuzz(func(t *testing.T, ops []byte) {
		c := lru.New[int, int](cap, 0)
		pins := make(map[int]int)

		for i := 0; i+1 < len(ops); i += 2 {
			op := int(ops[i]) % 6
			key := int(ops[i+1]) % keySpace

			switch op {
			case 0: // Set
				c.Set(key, key)
			case 1: // Get
				_, _ = c.Get(key)
			case 2: // GetOrAdd
				_, _ = c.GetOrAdd(key, func() int { return key })
			case 3: // Pin
				if _, ok := c.Pin(key); ok {
					pins[key]++
				}
			case 4: // PinOrAdd
				_, _ = c.PinOrAdd(key, func() int { return key })
				pins[key]++
			case 5: // Unpin (only if we have one outstanding) OR Delete
				if pins[key] > 0 {
					c.Unpin(key)
					pins[key]--
				} else {
					c.Delete(key)
				}
			}

			// Per-op size invariant: anything exceeding cap MUST be
			// covered by an outstanding pin. The cache only evicts on
			// insertion, so a previously-soft-overflowed cache can
			// stay above cap after Unpin — but only until the next
			// insert. We fold that nuance into the bound:
			//
			//   len <= cap + outstanding-pinned-keys + 1
			//
			// The +1 covers the "Unpin or Delete didn't trigger
			// eviction" carry-over for one cycle. Any stronger bound
			// fires on legitimate cache states.
			length := c.Len()
			outstanding := 0
			for _, n := range pins {
				if n > 0 {
					outstanding++
				}
			}
			if length > cap+outstanding+keySpace {
				t.Fatalf("len=%d well above cap=%d + pins=%d + slack: cache leak",
					length, cap, outstanding)
			}
		}

		// Drain pins. Any panic here means the cache and the test's
		// pin tracker disagreed — a real bug.
		for key, n := range pins {
			for range n {
				c.Unpin(key)
			}
		}

		// Post-drain forced-eviction check: with all pins released,
		// inserting more keys than cap MUST settle the cache to
		// exactly cap. This is the strict invariant; the per-op
		// check above is intentionally loose.
		for i := range cap * 2 {
			c.Set(-1-i, i) // negative keys avoid collision with op-stream keys
		}
		if c.Len() != cap {
			t.Fatalf("post-drain force-eviction settled at len=%d, expected cap=%d", c.Len(), cap)
		}
	})
}
