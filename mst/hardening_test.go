package mst

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
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
