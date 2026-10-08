package mst

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// FuzzDecodeNodeData tests that the specialized MST node decoder never panics
// on arbitrary input and that valid input round-trips.
func FuzzDecodeNodeData(f *testing.F) {
	// Seed with valid encoded nodes.
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	for _, nd := range []*NodeData{
		{Entries: []EntryData{}},
		{Entries: []EntryData{{PrefixLen: 0, KeySuffix: []byte("key"), Value: cid}}},
		{Left: gt.Some(cid), Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("abc"), Value: cid},
			{PrefixLen: 2, KeySuffix: []byte("d"), Value: cid, Right: gt.Some(cid)},
		}},
	} {
		data, err := encodeNodeData(nd)
		if err == nil {
			f.Add(data)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic, regardless of input.
		_, _ = DecodeNodeData(data)
	})
}

// FuzzInsertGet tests that any key inserted into the tree can be retrieved.
func FuzzInsertGet(f *testing.F) {
	f.Add("com.example.record/abc123")
	f.Add("app.bsky.feed.post/3jqfcqzm3fo2j")
	f.Add("a/b")

	f.Fuzz(func(t *testing.T, key string) {
		if !IsValidMstKey(key) {
			return
		}

		store := NewMemBlockStore()
		tree := NewTree(store)
		val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))

		if err := tree.Insert(key, val); err != nil {
			t.Fatalf("insert failed: %v", err)
		}

		got, err := tree.Get(key)
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if got == nil {
			t.Fatalf("key %q not found after insert", key)
		} else if !got.Equal(val) {
			t.Fatalf("value mismatch for key %q", key)
		}
	})
}

// FuzzIsValidMstKey tests that key validation never panics.
func FuzzIsValidMstKey(f *testing.F) {
	f.Add("")
	f.Add("a/b")
	f.Add("com.example.record/3jqfcqzm3fo2j")
	f.Add("/")
	f.Add("a/b/c")
	f.Add("\x00/\x00")

	f.Fuzz(func(t *testing.T, key string) {
		// Should never panic.
		_ = IsValidMstKey(key)
	})
}

// FuzzDecodeNodeDataRoundTrip tests that successfully decoded MST nodes can be
// re-encoded and decoded again to produce identical data. This verifies the
// integrity of the specialized fast-path codec.
func FuzzDecodeNodeDataRoundTrip(f *testing.F) {
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	for _, nd := range []*NodeData{
		{Entries: []EntryData{}},
		{Entries: []EntryData{{PrefixLen: 0, KeySuffix: []byte("key"), Value: cid}}},
		{Left: gt.Some(cid), Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("abc"), Value: cid},
			{PrefixLen: 2, KeySuffix: []byte("d"), Value: cid, Right: gt.Some(cid)},
		}},
	} {
		data, err := encodeNodeData(nd)
		if err == nil {
			f.Add(data)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		nd, err := DecodeNodeData(data)
		if err != nil {
			return
		}
		// Re-encode. The decoder accepts only the canonical encoding, so a
		// node that decodes must re-encode to the very same bytes; anything
		// else would give one node two CIDs.
		encoded, err := encodeNodeData(&nd)
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
		if !bytes.Equal(encoded, data) {
			t.Fatalf("decoded a non-canonical encoding:\n got %x\nwant %x", data, encoded)
		}
		// Re-decode.
		nd2, err := DecodeNodeData(encoded)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		// Compare structurally.
		if nd.Left != nd2.Left {
			t.Fatalf("left CID mismatch")
		}
		if len(nd.Entries) != len(nd2.Entries) {
			t.Fatalf("entry count mismatch: %d vs %d", len(nd.Entries), len(nd2.Entries))
		}
		for i := range nd.Entries {
			e1, e2 := &nd.Entries[i], &nd2.Entries[i]
			if e1.PrefixLen != e2.PrefixLen {
				t.Fatalf("entry %d: prefix len mismatch", i)
			}
			if string(e1.KeySuffix) != string(e2.KeySuffix) {
				t.Fatalf("entry %d: key suffix mismatch", i)
			}
			if !e1.Value.Equal(e2.Value) {
				t.Fatalf("entry %d: value CID mismatch", i)
			}
			if e1.Right != e2.Right {
				t.Fatalf("entry %d: right CID mismatch", i)
			}
		}
	})
}

// FuzzLoadAndWalk feeds arbitrary blocks through the load + traverse path
// (ensureLoaded), which reconstructs entry keys from prefix-compressed data.
// Unlike FuzzDecodeNodeData, this exercises the reslice that a malformed
// PrefixLen could otherwise panic on. It must never panic, only error.
func FuzzLoadAndWalk(f *testing.F) {
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	for _, nd := range []*NodeData{
		{Entries: []EntryData{}},
		{Entries: []EntryData{{PrefixLen: 0, KeySuffix: []byte("app.bsky.feed.post/a"), Value: cid}}},
		{Left: gt.Some(cid), Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("app.bsky.feed.post/a"), Value: cid},
			{PrefixLen: 19, KeySuffix: []byte("b"), Value: cid, Right: gt.Some(cid)},
		}},
	} {
		if data, err := encodeNodeData(nd); err == nil {
			f.Add(data)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		store := NewMemBlockStore()
		root := cbor.ComputeCID(cbor.CodecDagCBOR, data)
		if err := store.PutBlock(root, data); err != nil {
			t.Fatal(err)
		}
		tree := LoadTree(store, root)
		// Must never panic regardless of block contents.
		var walked []kv
		err := tree.Walk(func(k string, v cbor.CID) error {
			walked = append(walked, kv{k, v})
			return nil
		})
		_, _ = tree.Get("app.bsky.feed.post/a")
		if err != nil {
			return
		}
		// A node that loads is the one canonical encoding of its keys.
		m := map[string]cbor.CID{}
		for _, e := range walked {
			m[e.key] = e.val
		}
		if want := canonicalRoot(t, nil, m); !want.Equal(root) {
			t.Fatalf("loaded a non-canonical node: root %s, canonical %s", root.String(), want.String())
		}
	})
}

// FuzzLoadCorruptedTree builds a canonical multi-layer tree, overwrites one
// of its blocks with a fuzzer-corrupted copy, and loads it. Loading may
// fail, but whatever loads without error must still have the canonical
// shape for the keys it holds, and no operation on it may panic.
func FuzzLoadCorruptedTree(f *testing.F) {
	f.Add(uint64(0), uint8(0), []byte{10, 1})
	f.Add(uint64(1), uint8(3), []byte{40, 0x20})
	f.Add(uint64(2), uint8(1), []byte{7, 0x01, 30, 0x40})
	f.Fuzz(func(t *testing.T, seed uint64, block uint8, edits []byte) {
		rng := rand.New(rand.NewPCG(seed, 0))
		model := map[string]cbor.CID{}
		for _, k := range randomPool(rngChooser{rng}) {
			model[k] = cbor.ComputeCID(cbor.CodecRaw, []byte(k))
		}
		store := NewMemBlockStore()
		root := canonicalRoot(t, store, model)

		cids := slices.SortedFunc(func(yield func(cbor.CID) bool) {
			for c := range store.All() {
				if !yield(c) {
					return
				}
			}
		}, func(a, b cbor.CID) int { return strings.Compare(a.String(), b.String()) })
		target := cids[int(block)%len(cids)]
		data := slices.Clone(mustGetBlock(t, store, target))
		for i := 0; i+1 < len(edits); i += 2 {
			data[int(edits[i])%len(data)] ^= edits[i+1]
		}
		if len(edits)%2 == 1 {
			data = data[:int(edits[len(edits)-1])%(len(data)+1)]
		}
		require.NoError(t, store.PutBlock(target, data))

		tree := LoadTree(store, root)
		if err := tree.LoadAll(); err == nil {
			var walked []kv
			require.NoError(t, tree.Walk(func(k string, v cbor.CID) error {
				walked = append(walked, kv{k, v})
				return nil
			}))
			got := map[string]cbor.CID{}
			for _, e := range walked {
				got[e.key] = e.val
			}
			fresh := checkShape(t, tree, false)
			if want := canonicalRoot(t, nil, got); !want.Equal(fresh) {
				t.Fatalf("loaded a non-canonical tree: shape %s, canonical %s", fresh.String(), want.String())
			}
		}

		// Operations on whatever loaded must not panic.
		tree = LoadTree(store, root)
		for k := range model {
			_, _ = tree.Get(k)
			_ = tree.Remove(k)
			_ = tree.Insert(k, cbor.ComputeCID(cbor.CodecRaw, nil))
			break
		}
		_, _ = tree.RootCID()
	})
}

// FuzzMutatePartialStore drives checkMutationsAgainstModel from
// fuzzer-chosen seeds: Insert and Remove through a store that drops or
// flakes on blocks must either apply the change or leave the tree unchanged.
func FuzzMutatePartialStore(f *testing.F) {
	for seed := range uint64(8) {
		f.Add(seed, uint64(0))
	}
	f.Fuzz(func(t *testing.T, seed1, seed2 uint64) {
		checkMutationsAgainstModel(t, seed1, seed2)
	})
}

// FuzzHeightForKey tests that height computation never panics and is deterministic.
func FuzzHeightForKey(f *testing.F) {
	f.Add("")
	f.Add("blue")
	f.Add("com.example.record/3jqfcqzm3fo2j")

	f.Fuzz(func(t *testing.T, key string) {
		h1 := HeightForKey(key)
		h2 := HeightForKey(key)
		if h1 != h2 {
			t.Fatalf("non-deterministic height for %q: %d vs %d", key, h1, h2)
		}
		if h1 > 128 {
			t.Fatalf("height out of range for %q: %d", key, h1)
		}
	})
}
