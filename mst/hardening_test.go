package mst

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// storeBlock writes data under its computed dag-cbor CID and returns that CID.
func storeBlock(t *testing.T, store *MemBlockStore, data []byte) cbor.CID {
	t.Helper()
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	require.NoError(t, store.PutBlock(cid, data))
	return cid
}

// C2: a node whose first entry declares a non-zero prefix length is malformed
// (the first entry shares no prefix with a predecessor). Loading it must return
// an error, never panic on the keyBuf[:PrefixLen] reslice.
func TestEnsureLoaded_FirstEntryNonZeroPrefix_NoPanic(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))
	data, err := encodeNodeData(&NodeData{
		Entries: []EntryData{{PrefixLen: 5, KeySuffix: []byte("key"), Value: val}},
	})
	require.NoError(t, err)

	store := NewMemBlockStore()
	root := storeBlock(t, store, data)
	tree := LoadTree(store, root)

	_, err = tree.Get("anything")
	require.Error(t, err)
}

// C2: a prefix length larger than the previously reconstructed key must error,
// not panic.
func TestEnsureLoaded_PrefixLenExceedsPrevKey_NoPanic(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))
	data, err := encodeNodeData(&NodeData{
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("ab"), Value: val},
			{PrefixLen: 99, KeySuffix: []byte("x"), Value: val},
		},
	})
	require.NoError(t, err)

	store := NewMemBlockStore()
	root := storeBlock(t, store, data)
	tree := LoadTree(store, root)

	err = tree.Walk(func(string, cbor.CID) error { return nil })
	require.Error(t, err)
}

// C2: a prefix length encoded as a value that overflows int (2^64-1) must be
// rejected by the decoder, not silently wrapped to a negative int.
func TestDecodeNodeData_PrefixLenOverflow_Rejected(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))

	// Hand-build an entry with p = 0xFFFFFFFFFFFFFFFF.
	// map(2) "e" array(1) { map(4) "k" bytes("x") "p" uint64(max) "t" null "v" cid } "l" null
	buf := []byte{0xa2, 0x61, 'e'}
	buf = cbor.AppendArrayHeader(buf, 1)
	buf = append(buf, 0xa4, 0x61, 'k')
	buf = cbor.AppendBytes(buf, []byte("x"))
	buf = append(buf, 0x61, 'p')
	buf = append(buf, 0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // uint64 max
	buf = append(buf, 0x61, 't', 0xf6)                                      // null
	buf = append(buf, 0x61, 'v')
	buf = cbor.AppendCIDLink(buf, &val)
	buf = append(buf, 0x61, 'l', 0xf6) // "l" null

	_, err := DecodeNodeData(buf)
	require.Error(t, err)
}

// Nit/correctness: a node whose entries are not in strictly ascending key
// order is structurally invalid. Loading it must error rather than silently
// yielding unsorted keys (which would make Get return wrong results).
func TestEnsureLoaded_OutOfOrderEntries_Rejected(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))
	// Two full keys (PrefixLen 0) in descending order.
	data, err := encodeNodeData(&NodeData{
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("zzz/aaa"), Value: val},
			{PrefixLen: 0, KeySuffix: []byte("aaa/bbb"), Value: val},
		},
	})
	require.NoError(t, err)

	store := NewMemBlockStore()
	root := storeBlock(t, store, data)
	tree := LoadTree(store, root)

	err = tree.Walk(func(string, cbor.CID) error { return nil })
	require.Error(t, err)
}

// A maliciously deep block graph (a long chain of nodes each pointing to the
// next via Left) must be rejected with ErrMaxDepthExceeded rather than
// recursing until the goroutine stack overflows (a fatal, unrecoverable crash).
func TestLoadAndWalk_ExcessiveDepth_Rejected(t *testing.T) {
	t.Parallel()

	store := NewMemBlockStore()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))

	// Build a chain bottom-up: the deepest node holds one entry; each node above
	// points to the one below via Left. Chain length exceeds MaxDepth.
	deepest, err := encodeNodeData(&NodeData{
		Entries: []EntryData{{PrefixLen: 0, KeySuffix: []byte("k"), Value: val}},
	})
	require.NoError(t, err)
	childCID := storeBlock(t, store, deepest)

	for range MaxDepth + 50 {
		data, err := encodeNodeData(&NodeData{Left: gt.Some(childCID)})
		require.NoError(t, err)
		childCID = storeBlock(t, store, data)
	}

	tree := LoadTree(store, childCID)

	// Both the eager load path (used by repo.LoadFromCAR / backfill) and the
	// Walk path must bail with the typed depth error, not crash.
	require.ErrorIs(t, tree.LoadAll(), ErrMaxDepthExceeded)

	tree2 := LoadTree(store, childCID)
	require.ErrorIs(t, tree2.Walk(func(string, cbor.CID) error { return nil }), ErrMaxDepthExceeded)
}

// A node map whose keys are not in canonical DAG-CBOR order (l before e) must be
// rejected — accepting it would let a non-canonical block share a content
// address with the canonical form (an integrity violation).
func TestDecodeNodeData_NonCanonicalKeyOrder_Rejected(t *testing.T) {
	t.Parallel()
	// map(2) "l" null "e" array(0)  — reversed key order.
	buf := []byte{0xa2, 0x61, 'l', 0xf6, 0x61, 'e'}
	buf = cbor.AppendArrayHeader(buf, 0)
	_, err := DecodeNodeData(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-canonical")
}

// A node map with a duplicate key (e, e) must be rejected, not silently load the
// last occurrence.
func TestDecodeNodeData_DuplicateKey_Rejected(t *testing.T) {
	t.Parallel()
	// map(2) "e" array(0) "e" array(0) — duplicate "e", missing "l".
	buf := []byte{0xa2, 0x61, 'e'}
	buf = cbor.AppendArrayHeader(buf, 0)
	buf = append(buf, 0x61, 'e')
	buf = cbor.AppendArrayHeader(buf, 0)
	_, err := DecodeNodeData(buf)
	require.Error(t, err)
}

// An entry whose fields are not in canonical order (k, t, p, v) must be rejected.
func TestDecodeEntryData_NonCanonicalFieldOrder_Rejected(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))
	// map(2) "e" array(1) { map(4) "k" bytes("x") "t" null "p" uint(0) "v" cid } "l" null
	buf := []byte{0xa2, 0x61, 'e'}
	buf = cbor.AppendArrayHeader(buf, 1)
	buf = append(buf, 0xa4, 0x61, 'k')
	buf = cbor.AppendBytes(buf, []byte("x"))
	buf = append(buf, 0x61, 't', 0xf6) // "t" null — out of order (should be "p")
	buf = append(buf, 0x61, 'p', 0x00) // "p" 0
	buf = append(buf, 0x61, 'v')
	buf = cbor.AppendCIDLink(buf, &val)
	buf = append(buf, 0x61, 'l', 0xf6)

	_, err := DecodeNodeData(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-canonical")
}

// A node with a valid prefix structure must still load and traverse correctly.
func TestEnsureLoaded_ValidPrefix_Works(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))
	data, err := encodeNodeData(&NodeData{
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("app.bsky.feed.post/aaa"), Value: val},
			{PrefixLen: 19, KeySuffix: []byte("bbb"), Value: val},
		},
	})
	require.NoError(t, err)

	store := NewMemBlockStore()
	root := storeBlock(t, store, data)
	tree := LoadTree(store, root)

	var keys []string
	require.NoError(t, tree.Walk(func(k string, _ cbor.CID) error {
		keys = append(keys, k)
		return nil
	}))
	require.Equal(t, []string{"app.bsky.feed.post/aaa", "app.bsky.feed.post/bbb"}, keys)
}
