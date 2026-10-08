package mst

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// This file holds model-based tests: random operation sequences run against
// a Tree and a plain map, with the tree checked after each step against an
// oracle that shares none of the tree's mutation code.

type kv struct {
	key string
	val cbor.CID
}

func sortedKVs(m map[string]cbor.CID) []kv {
	var out []kv
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, kv{k, m[k]})
	}
	return out
}

// canonicalRoot builds the MST holding m straight from the spec's definition
// and returns its root CID: the top layer holds every key of the greatest
// height, and each gap between them (and before the first) is a subtree
// built the same way one layer down. It uses none of Insert, Remove, split
// or merge, so it is an independent oracle for the shape those produce. If
// store is non-nil, every node is written to it.
func canonicalRoot(t testing.TB, store BlockStore, m map[string]cbor.CID) cbor.CID {
	t.Helper()
	kvs := sortedKVs(m)
	if len(kvs) == 0 {
		return putNode(t, store, &NodeData{Entries: []EntryData{}})
	}
	var top uint8
	for _, e := range kvs {
		top = max(top, HeightForKey(e.key))
	}
	return canonicalLayer(t, store, kvs, top).Val()
}

func canonicalLayer(t testing.TB, store BlockStore, kvs []kv, height uint8) gt.Option[cbor.CID] {
	t.Helper()
	if len(kvs) == 0 {
		return gt.None[cbor.CID]()
	}
	sub := func(lo, hi int) gt.Option[cbor.CID] {
		if lo == hi {
			return gt.None[cbor.CID]()
		}
		require.NotZero(t, height, "keys below layer 0")
		return canonicalLayer(t, store, kvs[lo:hi], height-1)
	}
	nd := &NodeData{Entries: []EntryData{}}
	start, prev := 0, ""
	for i, e := range kvs {
		if HeightForKey(e.key) != height {
			continue
		}
		if child := sub(start, i); len(nd.Entries) == 0 {
			nd.Left = child
		} else {
			nd.Entries[len(nd.Entries)-1].Right = child
		}
		p := sharedPrefixLen(prev, e.key)
		nd.Entries = append(nd.Entries, EntryData{PrefixLen: p, KeySuffix: []byte(e.key[p:]), Value: e.val})
		start, prev = i+1, e.key
	}
	if child := sub(start, len(kvs)); len(nd.Entries) == 0 {
		nd.Left = child
	} else {
		nd.Entries[len(nd.Entries)-1].Right = child
	}
	return gt.Some(putNode(t, store, nd))
}

// putNode encodes nd and, if store is non-nil, writes it there.
func putNode(t testing.TB, store BlockStore, nd *NodeData) cbor.CID {
	t.Helper()
	data, err := encodeNodeData(nd)
	require.NoError(t, err)
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	if store != nil {
		require.NoError(t, store.PutBlock(cid, data))
	}
	return cid
}

// checkInvariants checks the in-memory shape of every loaded node: keys in
// order and inside the range their position allows, every key at its
// node's height, no node below layer 0, no empty node other than an empty
// root, and no root that is only a pointer. It also checks that the CID
// cache is coherent: a dirty node's parent is dirty, and every clean node's
// cached CID matches the CID recomputed from scratch.
func checkInvariants(t testing.TB, tr *Tree) {
	t.Helper()
	checkShape(t, tr, true)
}

// checkShape is checkInvariants, with the CID cache checks optional, and
// returns the root CID recomputed from the tree's contents.
func checkShape(t testing.TB, tr *Tree, checkCache bool) cbor.CID {
	t.Helper()
	if tr.root == nil {
		return putNode(t, nil, &NodeData{Entries: []EntryData{}})
	}
	var visit func(n *node, isRoot bool, lo, hi string) cbor.CID
	visit = func(n *node, isRoot bool, lo, hi string) cbor.CID {
		if !n.dirty && !n.loaded && len(n.entries) == 0 && n.left == nil {
			require.True(t, n.cid.Defined(), "unloaded node without a CID")
			return n.cid // an unloaded stub: nothing in memory to check
		}
		if isRoot {
			require.False(t, len(n.entries) == 0 && n.left != nil, "root is only a pointer")
		} else {
			require.False(t, len(n.entries) == 0 && n.left == nil, "empty non-root node")
		}
		for i, e := range n.entries {
			require.Equal(t, n.height, HeightForKey(e.key), "key %q in a node at height %d", e.key, n.height)
			if i > 0 {
				require.Less(t, n.entries[i-1].key, e.key, "entries out of order")
			}
			require.True(t, lo == "" || lo < e.key, "key %q not above its range floor %q", e.key, lo)
			require.True(t, hi == "" || e.key < hi, "key %q not below its range ceiling %q", e.key, hi)
		}
		child := func(c *node, clo, chi string) gt.Option[cbor.CID] {
			if c == nil {
				return gt.None[cbor.CID]()
			}
			require.NotZero(t, n.height, "child below layer 0")
			require.Equal(t, n.height-1, c.height, "child height")
			if checkCache && c.dirty {
				require.True(t, n.dirty, "dirty child under a clean parent")
			}
			return gt.Some(visit(c, false, clo, chi))
		}
		nd := &NodeData{Entries: make([]EntryData, len(n.entries))}
		firstHi := hi
		if len(n.entries) > 0 {
			firstHi = n.entries[0].key
		}
		nd.Left = child(n.left, lo, firstHi)
		prev := ""
		for i, e := range n.entries {
			chi := hi
			if i+1 < len(n.entries) {
				chi = n.entries[i+1].key
			}
			p := sharedPrefixLen(prev, e.key)
			nd.Entries[i] = EntryData{PrefixLen: p, KeySuffix: []byte(e.key[p:]), Value: e.val, Right: child(e.right, e.key, chi)}
			prev = e.key
		}
		fresh := putNode(t, nil, nd)
		if checkCache && !n.dirty && n.cid.Defined() {
			require.True(t, fresh.Equal(n.cid), "stale cached CID on a clean node")
		}
		return fresh
	}
	return visit(tr.root, true, "", "")
}

// heightKeys returns keys bucketed by height, mined from a few collection
// prefixes so they share prefixes the way real repo keys do.
var heightKeys = sync.OnceValue(func() [][]string {
	const maxHeight, perHeight = 6, 48
	buckets := make([][]string, maxHeight+1)
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "com.example.record"}
	for i := 0; ; i++ {
		k := fmt.Sprintf("%s/3k%06d", colls[i%len(colls)], i)
		if h := HeightForKey(k); int(h) <= maxHeight && len(buckets[h]) < perHeight {
			buckets[h] = append(buckets[h], k)
		}
		full := true
		for _, b := range buckets {
			full = full && len(b) == perHeight
		}
		if full {
			return buckets
		}
	}
})

// chooser is the source of every random decision in a model run, so the
// same run can be driven by a seeded PRNG or by fuzzer-provided bytes.
type chooser interface {
	IntN(n int) int
	Done() bool
}

type rngChooser struct{ *rand.Rand }

func (rngChooser) Done() bool { return false }

// byteChooser draws decisions from fuzzer bytes, then zeros once they run out.
type byteChooser struct{ data []byte }

func (b *byteChooser) IntN(n int) int {
	if n <= 1 || len(b.data) == 0 {
		return 0
	}
	v := int(b.data[0])
	b.data = b.data[1:]
	if n > 256 && len(b.data) > 0 {
		v = v<<8 | int(b.data[0])
		b.data = b.data[1:]
	}
	return v % n
}

func (b *byteChooser) Done() bool { return len(b.data) == 0 }

// randomPool draws a key pool spread across heights, weighted toward the
// low heights real trees are made of but always reaching a few layers up.
func randomPool(c chooser) []string {
	buckets := heightKeys()
	size := 1 + c.IntN(64)
	seen := map[string]bool{}
	var pool []string
	for range size {
		h := 0
		for h < len(buckets)-1 && c.IntN(3) == 0 {
			h++
		}
		k := buckets[h][c.IntN(len(buckets[h]))]
		if !seen[k] {
			seen[k] = true
			pool = append(pool, k)
		}
	}
	return pool
}

type snapshot struct {
	root  cbor.CID
	model map[string]cbor.CID
}

type modelRun struct {
	t     testing.TB
	c     chooser
	store *MemBlockStore
	tree  *Tree
	model map[string]cbor.CID
	pool  []string
	vals  []cbor.CID
	snaps []snapshot
	log   []string
}

func (r *modelRun) fail(format string, args ...any) {
	r.t.Helper()
	r.t.Fatalf("%s\nops:\n%v", fmt.Sprintf(format, args...), r.log)
}

func (r *modelRun) key() string {
	if r.c.IntN(8) == 0 {
		// A key outside the pool, absent from the tree.
		return fmt.Sprintf("com.example.absent/%d", r.c.IntN(1000))
	}
	return r.pool[r.c.IntN(len(r.pool))]
}

// write persists the tree and checks the blocks it wrote are a complete,
// canonical tree holding exactly the model.
func (r *modelRun) write() cbor.CID {
	r.t.Helper()
	root, err := r.tree.WriteBlocks(r.store)
	if err != nil {
		r.fail("WriteBlocks: %v", err)
	}
	if want := canonicalRoot(r.t, nil, r.model); !root.Equal(want) {
		r.fail("WriteBlocks root %s, want %s", root.String(), want.String())
	}
	r.checkWalk(LoadTree(r.store, root), "fresh load of written root")
	return root
}

func (r *modelRun) checkWalk(tr *Tree, what string) {
	r.t.Helper()
	var got []kv
	err := tr.Walk(func(k string, v cbor.CID) error {
		got = append(got, kv{k, v})
		return nil
	})
	if err != nil {
		r.fail("%s: Walk: %v", what, err)
	}
	if want := sortedKVs(r.model); !slices.Equal(got, want) {
		r.fail("%s: Walk = %v, want %v", what, got, want)
	}
}

func (r *modelRun) step() {
	r.t.Helper()
	switch op := r.c.IntN(20); {
	case op < 8:
		k, v := r.key(), r.vals[r.c.IntN(len(r.vals))]
		r.log = append(r.log, "insert "+k)
		if err := r.tree.Insert(k, v); err != nil {
			r.fail("Insert(%q): %v", k, err)
		}
		r.model[k] = v
	case op < 13:
		k := r.key()
		if len(r.model) > 0 && r.c.IntN(4) != 0 {
			keys := slices.Sorted(maps.Keys(r.model))
			k = keys[r.c.IntN(len(keys))]
		}
		r.log = append(r.log, "remove "+k)
		if err := r.tree.Remove(k); err != nil {
			r.fail("Remove(%q): %v", k, err)
		}
		delete(r.model, k)
	case op < 15:
		k := r.key()
		r.log = append(r.log, "get "+k)
		got, err := r.tree.Get(k)
		if err != nil {
			r.fail("Get(%q): %v", k, err)
		}
		want, ok := r.model[k]
		if ok != (got != nil) || (ok && !got.Equal(want)) {
			r.fail("Get(%q) = %v, want %v (present=%v)", k, got, want, ok)
		}
	case op == 15:
		r.log = append(r.log, "rootcid")
		got, err := r.tree.RootCID()
		if err != nil {
			r.fail("RootCID: %v", err)
		}
		if want := canonicalRoot(r.t, nil, r.model); !got.Equal(want) {
			r.fail("RootCID %s, want %s", got.String(), want.String())
		}
	case op == 16:
		r.log = append(r.log, "walk")
		r.checkWalk(r.tree, "Walk")
	case op == 17:
		r.log = append(r.log, "write")
		r.snaps = append(r.snaps, snapshot{r.write(), maps.Clone(r.model)})
	case op == 18:
		r.log = append(r.log, "reload")
		r.tree = LoadTree(r.store, r.write())
		if r.c.IntN(4) == 0 {
			r.log = append(r.log, "loadall")
			if err := r.tree.LoadAll(); err != nil {
				r.fail("LoadAll: %v", err)
			}
		}
	default:
		if len(r.snaps) == 0 {
			return
		}
		s := r.snaps[r.c.IntN(len(r.snaps))]
		r.log = append(r.log, "diff")
		cur := r.write()
		got, err := Diff(r.store, s.root, cur)
		if err != nil {
			r.fail("Diff: %v", err)
		}
		if want := modelDiff(s.model, r.model); !diffOpsEqual(got, want) {
			r.fail("Diff = %v, want %v", got, want)
		}
	}
	checkInvariants(r.t, r.tree)
}

// modelDiff is the diff between two models, in key order.
func modelDiff(from, to map[string]cbor.CID) []DiffOp {
	keys := slices.Sorted(maps.Keys(maps.Collect(func(yield func(string, bool) bool) {
		for k := range from {
			yield(k, true)
		}
		for k := range to {
			yield(k, true)
		}
	})))
	var ops []DiffOp
	for _, k := range keys {
		o, inOld := from[k]
		n, inNew := to[k]
		switch {
		case inOld && !inNew:
			ops = append(ops, DiffOp{Key: k, Old: &o})
		case !inOld && inNew:
			ops = append(ops, DiffOp{Key: k, New: &n})
		case !o.Equal(n):
			ops = append(ops, DiffOp{Key: k, Old: &o, New: &n})
		}
	}
	return ops
}

func diffOpsEqual(a, b []DiffOp) bool {
	eq := func(x, y *cbor.CID) bool { return (x == nil) == (y == nil) && (x == nil || x.Equal(*y)) }
	return slices.EqualFunc(a, b, func(x, y DiffOp) bool {
		return x.Key == y.Key && eq(x.Old, y.Old) && eq(x.New, y.New)
	})
}

// runModel drives one random operation sequence. It starts from an empty
// tree, a tree built by Insert, or a tree loaded lazily from canonical
// blocks the tree code did not write.
func runModel(t testing.TB, c chooser, maxSteps int) {
	t.Helper()
	r := &modelRun{
		t:     t,
		c:     c,
		store: NewMemBlockStore(),
		model: map[string]cbor.CID{},
		pool:  randomPool(c),
	}
	for i := range 3 {
		r.vals = append(r.vals, cbor.ComputeCID(cbor.CodecRaw, []byte{byte(i)}))
	}
	for _, k := range r.pool {
		if c.IntN(2) == 0 {
			r.model[k] = r.vals[c.IntN(len(r.vals))]
		}
	}
	switch c.IntN(3) {
	case 0:
		r.tree = NewTree(r.store)
		for _, e := range sortedKVs(r.model) {
			require.NoError(t, r.tree.Insert(e.key, e.val))
		}
	case 1:
		r.tree = LoadTree(r.store, canonicalRoot(t, r.store, r.model))
	default:
		r.model = map[string]cbor.CID{}
		r.tree = NewTree(r.store)
	}
	for i := 0; i < maxSteps && !c.Done(); i++ {
		r.step()
	}
	r.checkWalk(r.tree, "final Walk")
	r.write()
}

// Random operation sequences, including persistence and reloads, must keep
// the tree identical to a map and its root identical to the canonical one.
func TestTreeMatchesModel(t *testing.T) {
	t.Parallel()
	seeds := uint64(1_000)
	if testing.Short() {
		seeds = 300
	}
	for seed := range seeds {
		runModel(t, rngChooser{rand.New(rand.NewPCG(seed, 0x6d6f64656c))}, 200)
	}
}

// FuzzTreeOps runs the model-based test with every decision drawn from the
// fuzzer's bytes, letting coverage guidance steer the operation mix.
func FuzzTreeOps(f *testing.F) {
	for seed := range uint64(16) {
		rng := rand.New(rand.NewPCG(seed, 1))
		data := make([]byte, 256)
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runModel(t, &byteChooser{data: data}, 1_000)
	})
}

// The oracle must itself agree with the interop vectors, or it would only
// be checking the tree against itself.
func TestCanonicalRootMatchesInterop(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)
	for _, tc := range []struct {
		keys []string
		want string
	}{
		{nil, "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},
		{[]string{"com.example.record/3jqfcqzm3fo2j"}, "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu"},
		{[]string{"com.example.record/3jqfcqzm3fx2j"}, "bafyreih7wfei65pxzhauoibu3ls7jgmkju4bspy4t2ha2qdjnzqvoy33ai"},
		{[]string{
			"com.example.record/3jqfcqzm3fp2j", "com.example.record/3jqfcqzm3fr2j", "com.example.record/3jqfcqzm3fs2j",
			"com.example.record/3jqfcqzm3ft2j", "com.example.record/3jqfcqzm4fc2j",
		}, "bafyreicmahysq4n6wfuxo522m6dpiy7z7qzym3dzs756t5n7nfdgccwq7m"},
		{[]string{
			"com.example.record/3jqfcqzm3fo2j", "com.example.record/3jqfcqzm3fp2j", "com.example.record/3jqfcqzm3fr2j",
			"com.example.record/3jqfcqzm3fs2j", "com.example.record/3jqfcqzm3ft2j", "com.example.record/3jqfcqzm3fx2j",
			"com.example.record/3jqfcqzm3fz2j", "com.example.record/3jqfcqzm4fc2j", "com.example.record/3jqfcqzm4fd2j",
			"com.example.record/3jqfcqzm4ff2j", "com.example.record/3jqfcqzm4fg2j", "com.example.record/3jqfcqzm4fh2j",
		}, "bafyreid2x5eqs4w4qxvc5jiwda4cien3gw2q6cshofxwnvv7iucrmfohpm"},
		{[]string{
			"com.example.record/3jqfcqzm3ft2j", "com.example.record/3jqfcqzm3fx2j",
			"com.example.record/3jqfcqzm3fz2j", "com.example.record/3jqfcqzm4fd2j",
		}, "bafyreig4jv3vuajbsybhyvb7gggvpwh2zszwfyttjrj6qwvcsp24h6popu"},
	} {
		m := map[string]cbor.CID{}
		for _, k := range tc.keys {
			m[k] = val
		}
		require.Equal(t, tc.want, canonicalRoot(t, nil, m).String())
	}
}
