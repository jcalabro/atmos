package mst

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
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
		}
		if !got.Equal(val) {
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
		// Re-encode.
		encoded, err := encodeNodeData(&nd)
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
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
		_ = tree.Walk(func(string, cbor.CID) error { return nil })
		_, _ = tree.Get("app.bsky.feed.post/a")
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
