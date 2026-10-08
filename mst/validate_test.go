package mst

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// keysAtHeight mines n keys of the given height from format, which takes
// one integer, and returns them sorted.
func keysAtHeight(t testing.TB, height uint8, n int, format string) []string {
	t.Helper()
	var keys []string
	for i := 0; len(keys) < n; i++ {
		require.Less(t, i, 1<<24, "no keys at height %d", height)
		if k := fmt.Sprintf(format, i); HeightForKey(k) == height {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

// leafEntries returns entries for sorted keys, prefix-compressed as the
// spec requires.
func leafEntries(val cbor.CID, keys ...string) []EntryData {
	entries := make([]EntryData, len(keys))
	prev := ""
	for i, k := range keys {
		p := sharedPrefixLen(prev, k)
		entries[i] = EntryData{PrefixLen: p, KeySuffix: []byte(k[p:]), Value: val}
		prev = k
	}
	return entries
}

// Loading must reject every block graph that is not a canonical MST, since
// blocks come from untrusted CARs and sync peers. Each case breaks exactly
// one rule; all must fail with ErrInvalidTree from every entry point that
// reads the bad node, and none may panic.
func TestLoadRejectsInvalidTrees(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)
	h0 := keysAtHeight(t, 0, 4, "com.example.record/a%04d")
	h1 := keysAtHeight(t, 1, 3, "com.example.record/b%04d")
	h2 := keysAtHeight(t, 2, 1, "com.example.record/c%04d")

	cases := []struct {
		name  string
		build func(s *MemBlockStore) cbor.CID
	}{
		{"non-maximal prefix compression", func(s *MemBlockStore) cbor.CID {
			e := leafEntries(val, h0[0], h0[1])
			e[1] = EntryData{PrefixLen: 2, KeySuffix: []byte(h0[1][2:]), Value: val}
			return putNode(t, s, &NodeData{Entries: e})
		}},
		{"key without a collection", func(s *MemBlockStore) cbor.CID {
			return putNode(t, s, &NodeData{Entries: leafEntries(val, "asdf")})
		}},
		{"key with a nested collection", func(s *MemBlockStore) cbor.CID {
			return putNode(t, s, &NodeData{Entries: leafEntries(val, "nested/collection/asdf")})
		}},
		{"non-ascii key", func(s *MemBlockStore) cbor.CID {
			return putNode(t, s, &NodeData{Entries: leafEntries(val, "coll/jalapeño")})
		}},
		{"empty key", func(s *MemBlockStore) cbor.CID {
			return putNode(t, s, &NodeData{Entries: leafEntries(val, "")})
		}},
		{"mixed heights in one node", func(s *MemBlockStore) cbor.CID {
			keys := []string{h0[0], h1[0]}
			slices.Sort(keys)
			return putNode(t, s, &NodeData{Entries: leafEntries(val, keys...)})
		}},
		{"child at the parent's height", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, h1[0])})
			root := leafEntries(val, h1[1])
			root[0].Right = gt.Some(child)
			return putNode(t, s, &NodeData{Entries: root})
		}},
		{"child two layers down", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, h0[0])})
			return putNode(t, s, &NodeData{Left: gt.Some(child), Entries: leafEntries(val, h2[0])})
		}},
		{"child below layer 0", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, h0[1])})
			root := leafEntries(val, h0[0])
			root[0].Right = gt.Some(child)
			return putNode(t, s, &NodeData{Entries: root})
		}},
		{"empty node below the root", func(s *MemBlockStore) cbor.CID {
			empty := putNode(t, s, &NodeData{Entries: []EntryData{}})
			return putNode(t, s, &NodeData{Left: gt.Some(empty), Entries: leafEntries(val, h1[0])})
		}},
		{"empty chain ending in an empty node", func(s *MemBlockStore) cbor.CID {
			empty := putNode(t, s, &NodeData{Entries: []EntryData{}})
			passthrough := putNode(t, s, &NodeData{Left: gt.Some(empty), Entries: []EntryData{}})
			return putNode(t, s, &NodeData{Left: gt.Some(passthrough), Entries: leafEntries(val, h2[0])})
		}},
		{"root that is only a pointer", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, h0[0])})
			return putNode(t, s, &NodeData{Left: gt.Some(child), Entries: []EntryData{}})
		}},
		{"left subtree key above its parent's key", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, "zzz.example/a")})
			return putNode(t, s, &NodeData{Left: gt.Some(child), Entries: leafEntries(val, h1[0])})
		}},
		{"right subtree key below its parent's key", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, "aaa.example/a")})
			root := leafEntries(val, h1[0])
			root[0].Right = gt.Some(child)
			return putNode(t, s, &NodeData{Entries: root})
		}},
		{"right subtree key above the next key", func(s *MemBlockStore) cbor.CID {
			child := putNode(t, s, &NodeData{Entries: leafEntries(val, "zzz.example/a")})
			root := leafEntries(val, h1[0], h1[1])
			root[0].Right = gt.Some(child)
			return putNode(t, s, &NodeData{Entries: root})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemBlockStore()
			root := tc.build(store)

			err := LoadTree(store, root).Walk(func(string, cbor.CID) error { return nil })
			require.ErrorIs(t, err, ErrInvalidTree)
			require.ErrorIs(t, LoadTree(store, root).LoadAll(), ErrInvalidTree)

			// Mutations and lookups through the bad tree must not panic.
			tree := LoadTree(store, root)
			for _, k := range []string{h0[0], h0[3], h1[2], "zzz.example/a"} {
				_, _ = tree.Get(k)
				_ = tree.Insert(k, val)
				_ = tree.Remove(k)
			}
		})
	}
}

// A node block with bytes after the node map would give the same node a
// second CID.
func TestDecodeNodeData_TrailingBytes_Rejected(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)
	data, err := encodeNodeData(&NodeData{Entries: leafEntries(val, "com.example/a")})
	require.NoError(t, err)
	_, err = DecodeNodeData(data)
	require.NoError(t, err)

	for _, tail := range [][]byte{{0x00}, {0xf6}, {0xa0}, data} {
		_, err := DecodeNodeData(append(slices.Clone(data), tail...))
		require.ErrorContains(t, err, "trailing bytes")
	}

	store := NewMemBlockStore()
	bad := append(slices.Clone(data), 0x00)
	root := cbor.ComputeCID(cbor.CodecDagCBOR, bad)
	require.NoError(t, store.PutBlock(root, bad))
	require.ErrorIs(t, LoadTree(store, root).Walk(func(string, cbor.CID) error { return nil }), ErrInvalidTree)
}

// buildDiamond builds the block graph from the reference implementation's
// DAG tests, with keys mined to the right heights so every node passes the
// checks loading makes one node at a time: two nodes per level, each
// pointing at both nodes of the level below. A walk from the root would
// describe 2^depth keys from only 2*depth blocks.
func buildDiamond(t *testing.T, store *MemBlockStore, depth int) cbor.CID {
	t.Helper()
	val := testValueCID(t)
	var level [2]cbor.CID
	for i := range depth {
		keys := keysAtHeight(t, uint8(i), 2, fmt.Sprintf("com.example.lvl%02d/%%d", i))
		var next [2]cbor.CID
		for j := range 2 {
			nd := &NodeData{Entries: leafEntries(val, keys[j])}
			if i > 0 {
				nd.Left = gt.Some(level[j])
				nd.Entries[0].Right = gt.Some(level[(j+1)%2])
			}
			next[j] = putNode(t, store, nd)
		}
		level = next
	}
	return level[0]
}

// buildSharedChain builds one wide node whose every entry points at the
// same chain of entry-less nodes, ending in a single leaf: a walk would
// make wide*height node visits from wide+height blocks.
func buildSharedChain(t *testing.T, store *MemBlockStore, height uint8, wide int) cbor.CID {
	t.Helper()
	val := testValueCID(t)
	chain := putNode(t, store, &NodeData{Entries: leafEntries(val, keysAtHeight(t, 0, 1, "com.example.bottom/%d")[0])})
	for range height - 1 {
		chain = putNode(t, store, &NodeData{Left: gt.Some(chain), Entries: []EntryData{}})
	}
	entries := leafEntries(val, keysAtHeight(t, height, wide, "com.example.wide/%06d")...)
	for i := range entries {
		entries[i].Right = gt.Some(chain)
	}
	return putNode(t, store, &NodeData{Entries: entries})
}

// An MST never references the same subtree twice. A hostile block graph
// that does must be rejected by every full traversal before the shared
// subtrees blow the work up exponentially.
func TestLoadRejectsSharedSubtrees(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		build func(*MemBlockStore) cbor.CID
	}{
		{"diamond", func(s *MemBlockStore) cbor.CID { return buildDiamond(t, s, 9) }},
		{"shared leafless chain", func(s *MemBlockStore) cbor.CID { return buildSharedChain(t, s, 4, 50) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemBlockStore()
			root := tc.build(store)

			start := time.Now()
			var keys int
			err := LoadTree(store, root).Walk(func(string, cbor.CID) error {
				keys++
				return nil
			})
			require.ErrorIs(t, err, ErrInvalidTree)
			require.ErrorContains(t, err, "is not greater than previous key")
			require.Less(t, keys, 64, "walk yielded keys from repeated subtrees")
			require.ErrorIs(t, LoadTree(store, root).LoadAll(), ErrInvalidTree)

			_, err = Diff(store, putNode(t, store, &NodeData{Entries: []EntryData{}}), root)
			require.ErrorIs(t, err, ErrInvalidTree)
			require.Less(t, time.Since(start), 5*time.Second)
		})
	}
}

// Keys must sit at the height their hash gives them, which bounds a
// diamond's depth: one deeper than the keys allow fails on load. Without
// that bound, a diamond of 2^depth paths needs only depth levels of
// otherwise valid nodes.
func TestDiamondDepthIsBoundedByKeyHeights(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	root := buildDiamond(t, store, 6)
	nd, err := DecodeNodeData(mustGetBlock(t, store, root))
	require.NoError(t, err)
	// Re-point the bottom of the diamond one level lower than its keys allow.
	val := testValueCID(t)
	leaf := putNode(t, store, &NodeData{Entries: leafEntries(val, keysAtHeight(t, 0, 1, "com.example.lvlxx/%d")[0])})
	nd.Left = gt.Some(leaf)
	bad := putNode(t, store, &nd)
	require.ErrorIs(t, LoadTree(store, bad).Walk(func(string, cbor.CID) error { return nil }), ErrInvalidTree)
}

func mustGetBlock(t *testing.T, store BlockStore, cid cbor.CID) []byte {
	t.Helper()
	data, err := store.GetBlock(cid)
	require.NoError(t, err)
	return data
}

// Insert must reject keys that are not valid MST keys, leaving the tree
// unchanged. Mirrors the reference implementation's "MST Interop Allowable
// Keys" tests.
func TestInsertRejectsInvalidKeys(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)
	tree := NewTree(NewMemBlockStore())
	require.NoError(t, tree.Insert("com.example/existing", val))
	before, err := tree.RootCID()
	require.NoError(t, err)

	reject := []string{
		"",
		"asdf",
		"nested/collection/asdf",
		"coll/",
		"/rkey",
		"coll/jalapeñoA",
		"coll/coöperative",
		"coll/abc💩",
		"coll/key$", "coll/key%", "coll/key(", "coll/key)", "coll/key+", "coll/key=",
		"coll/@handle", "coll/any space", "coll/#extra", "coll/any+space",
		"coll/number[3]", "coll/number(3)", "coll/dHJ1ZQ==", `coll/"quote"`,
		"coll/" + strings.Repeat("a", 1020),
	}
	for _, k := range reject {
		require.ErrorIs(t, tree.Insert(k, val), ErrInvalidKey, "key %q", k)
	}
	after, err := tree.RootCID()
	require.NoError(t, err)
	require.True(t, before.Equal(after), "a rejected insert changed the tree")

	allow := []string{
		"coll/3jui7kd54zh2y", "coll/self", "coll/example.com", "com.example/rkey",
		"coll/~1.2-3_", "coll/dHJ1ZQ", "coll/pre:fix", "coll/_",
		"coll/" + strings.Repeat("a", 1019),
	}
	for _, k := range allow {
		require.NoError(t, tree.Insert(k, val), "key %q", k)
		got, err := tree.Get(k)
		require.NoError(t, err)
		require.NotNil(t, got, "key %q", k)
	}
}
