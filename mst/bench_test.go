package mst

import (
	"fmt"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
)

func BenchmarkHeightForKey(b *testing.B) {
	key := "app.bsky.feed.post/3jqfcqzm3fo2j"
	for b.Loop() {
		_ = HeightForKey(key)
	}
}

func BenchmarkInsert_100(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("com.example.record/%06d", i)
	}
	b.ResetTimer()
	for b.Loop() {
		store := NewMemBlockStore()
		tree := NewTree(store)
		for _, k := range keys {
			_ = tree.Insert(k, val)
		}
	}
}

func BenchmarkInsert_1000(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("com.example.record/%06d", i)
	}
	b.ResetTimer()
	for b.Loop() {
		store := NewMemBlockStore()
		tree := NewTree(store)
		for _, k := range keys {
			_ = tree.Insert(k, val)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	store := NewMemBlockStore()
	tree := NewTree(store)
	for i := range 1000 {
		_ = tree.Insert(fmt.Sprintf("com.example.record/%06d", i), val)
	}
	b.ResetTimer()
	for b.Loop() {
		_, _ = tree.Get("com.example.record/000500")
	}
}

func BenchmarkWalk_1000(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	store := NewMemBlockStore()
	tree := NewTree(store)
	for i := range 1000 {
		_ = tree.Insert(fmt.Sprintf("com.example.record/%06d", i), val)
	}
	b.ResetTimer()
	for b.Loop() {
		_ = tree.Walk(func(_ string, _ cbor.CID) error { return nil })
	}
}

func BenchmarkRootCID_1000(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	store := NewMemBlockStore()
	tree := NewTree(store)
	for i := range 1000 {
		_ = tree.Insert(fmt.Sprintf("com.example.record/%06d", i), val)
	}
	b.ResetTimer()
	for b.Loop() {
		// Mark dirty to force recomputation.
		tree.root.dirty = true
		_, _ = tree.RootCID()
	}
}

func BenchmarkWriteBlocks_1000(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	store := NewMemBlockStore()
	tree := NewTree(store)
	for i := range 1000 {
		_ = tree.Insert(fmt.Sprintf("com.example.record/%06d", i), val)
	}
	b.ResetTimer()
	for b.Loop() {
		tree.root.dirty = true
		_, _ = tree.WriteBlocks(store)
	}
}

func BenchmarkRemove_500(b *testing.B) {
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("com.example.record/%06d", i)
	}
	b.ResetTimer()
	for b.Loop() {
		store := NewMemBlockStore()
		tree := NewTree(store)
		for _, k := range keys {
			_ = tree.Insert(k, val)
		}
		// Remove every other key.
		for i := 0; i < len(keys); i += 2 {
			_ = tree.Remove(keys[i])
		}
	}
}

func BenchmarkEncodeNodeData(b *testing.B) {
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	nd := &NodeData{
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("com.example.record/aaa"), Value: cid},
			{PrefixLen: 19, KeySuffix: []byte("bbb"), Value: cid, Right: gt.Some(cid)},
			{PrefixLen: 19, KeySuffix: []byte("ccc"), Value: cid},
		},
	}
	b.ResetTimer()
	for b.Loop() {
		_, _ = encodeNodeData(nd)
	}
}

func BenchmarkDecodeNodeData(b *testing.B) {
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	nd := &NodeData{
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("com.example.record/aaa"), Value: cid},
			{PrefixLen: 19, KeySuffix: []byte("bbb"), Value: cid, Right: gt.Some(cid)},
			{PrefixLen: 19, KeySuffix: []byte("ccc"), Value: cid},
		},
	}
	data, _ := encodeNodeData(nd)
	b.ResetTimer()
	for b.Loop() {
		_, _ = DecodeNodeData(data)
	}
}

func BenchmarkDiff_1000(b *testing.B) {
	val1 := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v1"))
	val2 := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v2"))

	store := NewMemBlockStore()
	tree1 := NewTree(store)
	tree2 := NewTree(store)
	for i := range 1000 {
		k := fmt.Sprintf("com.example.record/%06d", i)
		_ = tree1.Insert(k, val1)
		if i%10 == 0 {
			// 10% of keys have a different value.
			_ = tree2.Insert(k, val2)
		} else if i%20 != 1 {
			// Skip 5% of keys to create deletes.
			_ = tree2.Insert(k, val1)
		}
	}
	// Add some creates in tree2.
	for i := 1000; i < 1050; i++ {
		_ = tree2.Insert(fmt.Sprintf("com.example.record/%06d", i), val1)
	}

	root1, _ := tree1.WriteBlocks(store)
	root2, _ := tree2.WriteBlocks(store)

	b.ResetTimer()
	for b.Loop() {
		_, _ = Diff(store, root1, root2)
	}
}

func BenchmarkIsValidMstKey(b *testing.B) {
	key := "app.bsky.feed.post/3jqfcqzm3fo2j"
	for b.Loop() {
		_ = IsValidMstKey(key)
	}
}
