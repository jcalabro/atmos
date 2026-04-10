// Package mst implements the Merkle Search Tree used by AT Protocol repositories.
package mst

import (
	"fmt"
	"unsafe"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
)

// BlockStore is a content-addressed block storage interface.
type BlockStore interface {
	// GetBlock retrieves a block by its CID. Returns an error if not found.
	GetBlock(cid cbor.CID) ([]byte, error)
	// PutBlock stores a block at the given CID.
	PutBlock(cid cbor.CID, data []byte) error
}

// MemBlockStore is a simple in-memory BlockStore implementation.
// Uses CID structs directly as map keys (comparable value type) to avoid
// allocating byte slices for key encoding.
//
// MemBlockStore is NOT internally safe for concurrent use. Callers must provide
// their own synchronization if the store will be accessed from multiple goroutines.
type MemBlockStore struct {
	blocks map[cbor.CID][]byte
}

// NewMemBlockStore creates a new empty MemBlockStore.
func NewMemBlockStore() *MemBlockStore {
	return &MemBlockStore{blocks: make(map[cbor.CID][]byte)}
}

// GetBlock retrieves a block by its CID.
func (s *MemBlockStore) GetBlock(cid cbor.CID) ([]byte, error) {
	data, ok := s.blocks[cid]
	if !ok {
		return nil, fmt.Errorf("block not found: %s", cid.String())
	}
	return data, nil
}

// PutBlock stores a block at the given CID.
func (s *MemBlockStore) PutBlock(cid cbor.CID, data []byte) error {
	s.blocks[cid] = data
	return nil
}

// entry is an in-memory MST entry: a key/value pair with optional right subtree.
//
// Field order places the hot traversal fields (key, right) in the first 24
// bytes so they share a cache line regardless of slice alignment. The cold
// val (only read on an exact key match) trails behind.
type entry struct {
	key   string   // 16B — hot: comparison in every scan iteration
	right *node    // 8B  — hot: subtree descent
	val   cbor.CID // 33B — cold: only on match
}

// node is an in-memory MST node.
//
// Field order is chosen for cache-line locality: the hot traversal fields
// (left, entries, height, dirty) sit in the first 34 bytes so that
// ensureLoaded's guard check and getNode's descent stay within a single
// 64-byte cache line. The cold CID (only touched during serialization /
// loading) trails at the end and spills to a second line.
type node struct {
	left    *node    // 8B  — hot: every traversal
	entries []entry  // 24B — hot: every traversal
	height  uint8    // 1B  — hot: insert level checks
	dirty   bool     // 1B  — hot: ensureLoaded guard
	cid     cbor.CID // 33B — cold: serialization / loading only
}

// Tree is an in-memory Merkle Search Tree.
//
// Tree is NOT internally safe for concurrent use. All operations (including reads like
// Get and Walk) may mutate internal state via lazy loading.
type Tree struct {
	root  *node
	store BlockStore
}

// NewTree creates a new empty MST backed by the given store.
func NewTree(store BlockStore) *Tree {
	return &Tree{store: store}
}

// LoadTree loads an MST from a root CID using the given store.
func LoadTree(store BlockStore, root cbor.CID) *Tree {
	return &Tree{
		store: store,
		root:  &node{cid: root},
	}
}

// LoadAll eagerly loads every node in the tree from the block store,
// decoding all CBOR upfront. After LoadAll, operations like Walk and
// Get become pure in-memory pointer traversals with no further I/O
// or decoding.
func (t *Tree) LoadAll() error {
	if t.root == nil {
		return nil
	}
	return t.loadAllNode(t.root)
}

func (t *Tree) loadAllNode(n *node) error {
	if err := t.ensureLoaded(n); err != nil {
		return err
	}
	if n.left != nil {
		if err := t.loadAllNode(n.left); err != nil {
			return err
		}
	}
	for i := range n.entries {
		if n.entries[i].right != nil {
			if err := t.loadAllNode(n.entries[i].right); err != nil {
				return err
			}
		}
	}
	return nil
}

// Get looks up a key and returns its value CID, or nil if not found.
func (t *Tree) Get(key string) (*cbor.CID, error) {
	if t.root == nil {
		return nil, nil
	}
	return t.getNode(t.root, key)
}

func (t *Tree) getNode(n *node, key string) (*cbor.CID, error) {
	if err := t.ensureLoaded(n); err != nil {
		return nil, err
	}

	for i := range n.entries {
		if key < n.entries[i].key {
			// Check left subtree (or right subtree of previous entry).
			child := n.left
			if i > 0 {
				child = n.entries[i-1].right
			}
			if child != nil {
				return t.getNode(child, key)
			}
			return nil, nil
		}
		if key == n.entries[i].key {
			return &n.entries[i].val, nil
		}
	}

	// Check rightmost subtree.
	if len(n.entries) > 0 {
		child := n.entries[len(n.entries)-1].right
		if child != nil {
			return t.getNode(child, key)
		}
	} else if n.left != nil {
		return t.getNode(n.left, key)
	}
	return nil, nil
}

// Insert inserts or updates a key/value pair.
func (t *Tree) Insert(key string, val cbor.CID) error {
	h := HeightForKey(key)
	newRoot, err := t.insertNode(t.root, key, val, h)
	if err != nil {
		return err
	}
	t.root = newRoot
	return nil
}

func (t *Tree) insertNode(n *node, key string, val cbor.CID, height uint8) (*node, error) {
	if n == nil {
		entries := make([]entry, 1, 4)
		entries[0] = entry{key: key, val: val}
		return &node{
			entries: entries,
			height:  height,
			dirty:   true,
		}, nil
	}

	if err := t.ensureLoaded(n); err != nil {
		return nil, err
	}

	if height > n.height {
		// Step up one level at a time, wrapping the current node as a child.
		// This creates intermediate nodes when the height jump is > 1.
		parent := &node{
			left:   n,
			height: n.height + 1,
			dirty:  true,
		}
		return t.insertNode(parent, key, val, height)
	}

	if height < n.height {
		// Descend into the appropriate subtree.
		return t.insertBelow(n, key, val, height)
	}

	// Same height — insert into this node's entries.
	return t.insertAtLevel(n, key, val, height)
}

// insertBelow inserts a key into a subtree of n (key height < n.height).
func (t *Tree) insertBelow(n *node, key string, val cbor.CID, height uint8) (*node, error) {
	idx := t.findChildIndex(n, key)

	var child *node
	if idx == 0 {
		child = n.left
	} else {
		child = n.entries[idx-1].right
	}

	// If no child exists and we're exactly one level above, create the
	// final leaf node directly instead of creating an empty intermediate.
	if child == nil {
		if n.height-1 == height {
			child = &node{
				entries: make([]entry, 1, 4),
				height:  height,
				dirty:   true,
			}
			child.entries[0] = entry{key: key, val: val}
			n.dirty = true
			if idx == 0 {
				n.left = child
			} else {
				n.entries[idx-1].right = child
			}
			return n, nil
		}
		child = &node{
			height: n.height - 1,
			dirty:  true,
		}
	}

	newChild, err := t.insertNode(child, key, val, height)
	if err != nil {
		return nil, err
	}

	n.dirty = true
	if idx == 0 {
		n.left = newChild
	} else {
		n.entries[idx-1].right = newChild
	}
	return n, nil
}

// insertAtLevel inserts a key at the same height level as n.
func (t *Tree) insertAtLevel(n *node, key string, val cbor.CID, _ uint8) (*node, error) {
	// Binary search for insertion point.
	entries := n.entries
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	i := lo

	// Check for update of existing key.
	if i < len(entries) && entries[i].key == key {
		n.entries[i].val = val
		n.dirty = true
		return n, nil
	}

	// Split the child between entries[i-1] and entries[i].
	var childToSplit *node
	if i == 0 {
		childToSplit = n.left
	} else {
		childToSplit = entries[i-1].right
	}

	left, right, err := t.splitNode(childToSplit, key)
	if err != nil {
		return nil, err
	}

	newEntry := entry{key: key, val: val, right: right}

	// Insert newEntry at position i.
	n.entries = append(n.entries, entry{})
	copy(n.entries[i+1:], n.entries[i:])
	n.entries[i] = newEntry

	// Update the left pointer or previous entry's right.
	if i == 0 {
		n.left = left
	} else {
		n.entries[i-1].right = left
	}

	n.dirty = true
	return n, nil
}

// splitNode splits a node at key, returning (left, right) subtrees.
// Left contains everything < key, right contains everything > key.
// Child subtrees at the split boundary are recursively split.
func (t *Tree) splitNode(n *node, key string) (*node, *node, error) {
	if n == nil {
		return nil, nil, nil
	}

	if err := t.ensureLoaded(n); err != nil {
		return nil, nil, err
	}

	// Binary search for split point: first entry with key >= key.
	entries := n.entries
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	splitIdx := -1
	if lo < len(entries) {
		splitIdx = lo
	}

	if splitIdx == -1 {
		// All entries < key. The rightmost child may still need splitting.
		var lastChild *node
		if len(n.entries) > 0 {
			lastChild = n.entries[len(n.entries)-1].right
		} else {
			lastChild = n.left
		}
		childLeft, childRight, err := t.splitNode(lastChild, key)
		if err != nil {
			return nil, nil, err
		}
		if len(n.entries) > 0 {
			n.entries[len(n.entries)-1].right = childLeft
		} else {
			n.left = childLeft
		}
		n.dirty = true
		// Wrap childRight at this node's height.
		var rightNode *node
		if childRight != nil {
			rightNode = &node{left: childRight, height: n.height, dirty: true}
		}
		return trimNode(n), trimNode(rightNode), nil
	}

	if splitIdx == 0 {
		// All entries >= key. The left child may still need splitting.
		childLeft, childRight, err := t.splitNode(n.left, key)
		if err != nil {
			return nil, nil, err
		}
		n.left = childRight
		n.dirty = true
		// Wrap childLeft at this node's height.
		var leftNode *node
		if childLeft != nil {
			leftNode = &node{left: childLeft, height: n.height, dirty: true}
		}
		return trimNode(leftNode), trimNode(n), nil
	}

	// Split in the middle.
	leftEntries := make([]entry, splitIdx)
	copy(leftEntries, n.entries[:splitIdx])

	rightEntries := make([]entry, len(n.entries)-splitIdx)
	copy(rightEntries, n.entries[splitIdx:])

	leftNode := &node{
		left:    n.left,
		entries: leftEntries,
		height:  n.height,
		dirty:   true,
	}

	// The child between the two halves needs to be recursively split.
	midChild := leftNode.entries[len(leftEntries)-1].right
	midLeft, midRight, err := t.splitNode(midChild, key)
	if err != nil {
		return nil, nil, err
	}
	leftNode.entries[len(leftEntries)-1].right = midLeft

	rightNode := &node{
		left:    midRight,
		entries: rightEntries,
		height:  n.height,
		dirty:   true,
	}

	return trimNode(leftNode), trimNode(rightNode), nil
}

// trimNode removes completely empty nodes (no entries and no children).
func trimNode(n *node) *node {
	if n == nil {
		return nil
	}
	if len(n.entries) == 0 && n.left == nil {
		return nil
	}
	return n
}

// findChildIndex returns the entry index where key would be found.
// Returns 0 if key < all entries (meaning use n.left).
// Returns i if key should be in the subtree after entries[i-1].
func (t *Tree) findChildIndex(n *node, key string) int {
	entries := n.entries
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if entries[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Remove deletes a key from the tree.
func (t *Tree) Remove(key string) error {
	if t.root == nil {
		return nil
	}
	newRoot, err := t.removeNode(t.root, key)
	if err != nil {
		return err
	}
	// Trim the top: collapse empty root nodes that only have a left child.
	for newRoot != nil && len(newRoot.entries) == 0 {
		if newRoot.left != nil {
			newRoot = newRoot.left
		} else {
			newRoot = nil
		}
	}
	t.root = newRoot
	return nil
}

func (t *Tree) removeNode(n *node, key string) (*node, error) {
	if n == nil {
		return nil, nil
	}
	if err := t.ensureLoaded(n); err != nil {
		return nil, err
	}

	for i, e := range n.entries {
		if key == e.key {
			// Found it. Merge left and right children around this entry.
			var leftChild, rightChild *node
			if i == 0 {
				leftChild = n.left
			} else {
				leftChild = n.entries[i-1].right
			}
			rightChild = e.right

			merged, err := t.mergeNodes(leftChild, rightChild)
			if err != nil {
				return nil, err
			}

			// Remove entry i in-place (shift left, truncate).
			copy(n.entries[i:], n.entries[i+1:])
			n.entries[len(n.entries)-1] = entry{} // clear for GC
			n.entries = n.entries[:len(n.entries)-1]

			if i == 0 {
				n.left = merged
			} else {
				n.entries[i-1].right = merged
			}
			n.dirty = true

			if len(n.entries) == 0 {
				return n.left, nil
			}
			return n, nil
		}
		if key < e.key {
			// Descend into left child.
			idx := i
			var child *node
			if idx == 0 {
				child = n.left
			} else {
				child = n.entries[idx-1].right
			}
			newChild, err := t.removeNode(child, key)
			if err != nil {
				return nil, err
			}
			if newChild != child {
				if idx == 0 {
					n.left = newChild
				} else {
					n.entries[idx-1].right = newChild
				}
				n.dirty = true
			}
			return n, nil
		}
	}

	// Key > all entries (or no entries), descend into rightmost child.
	if len(n.entries) > 0 {
		last := len(n.entries) - 1
		child := n.entries[last].right
		newChild, err := t.removeNode(child, key)
		if err != nil {
			return nil, err
		}
		if newChild != child {
			n.entries[last].right = newChild
			n.dirty = true
		}
	} else if n.left != nil {
		child := n.left
		newChild, err := t.removeNode(child, key)
		if err != nil {
			return nil, err
		}
		if newChild != child {
			n.left = newChild
			n.dirty = true
		}
	}
	return n, nil
}

// mergeNodes merges two sibling subtrees back together.
func (t *Tree) mergeNodes(left, right *node) (*node, error) {
	if left == nil {
		return right, nil
	}
	if right == nil {
		return left, nil
	}

	if err := t.ensureLoaded(left); err != nil {
		return nil, err
	}
	if err := t.ensureLoaded(right); err != nil {
		return nil, err
	}

	// Merge the rightmost child of left with the left child of right recursively.
	var leftRightChild *node
	if len(left.entries) > 0 {
		leftRightChild = left.entries[len(left.entries)-1].right
	} else {
		leftRightChild = left.left
	}

	merged, err := t.mergeNodes(leftRightChild, right.left)
	if err != nil {
		return nil, err
	}

	if len(left.entries) > 0 {
		left.entries[len(left.entries)-1].right = merged
	} else {
		left.left = merged
	}

	// Append right's entries to left.
	left.entries = append(left.entries, right.entries...)
	left.dirty = true

	return left, nil
}

// Walk traverses all key/value pairs in sorted order.
func (t *Tree) Walk(fn func(key string, val cbor.CID) error) error {
	if t.root == nil {
		return nil
	}
	return t.walkNode(t.root, fn)
}

func (t *Tree) walkNode(n *node, fn func(key string, val cbor.CID) error) error {
	if n == nil {
		return nil
	}
	if err := t.ensureLoaded(n); err != nil {
		return err
	}

	// Visit left subtree first.
	if err := t.walkNode(n.left, fn); err != nil {
		return err
	}

	for _, e := range n.entries {
		if err := fn(e.key, e.val); err != nil {
			return err
		}
		if err := t.walkNode(e.right, fn); err != nil {
			return err
		}
	}
	return nil
}

// RootCID computes and returns the root CID of the tree.
// Returns an error if the tree is empty.
func (t *Tree) RootCID() (cbor.CID, error) {
	if t.root == nil {
		// Empty tree: encode an empty node.
		nd := &NodeData{Entries: []EntryData{}}
		data, err := encodeNodeData(nd)
		if err != nil {
			return cbor.CID{}, err
		}
		return cbor.ComputeCID(cbor.CodecDagCBOR, data), nil
	}
	return t.computeCID(t.root)
}

func (t *Tree) computeCID(n *node) (cbor.CID, error) {
	if !n.dirty && n.cid.Defined() {
		return n.cid, nil
	}

	if err := t.ensureLoaded(n); err != nil {
		return cbor.CID{}, err
	}

	nd, err := t.nodeToData(n)
	if err != nil {
		return cbor.CID{}, err
	}
	data, err := encodeNodeData(nd)
	if err != nil {
		return cbor.CID{}, err
	}
	n.cid = cbor.ComputeCID(cbor.CodecDagCBOR, data)
	n.dirty = false
	return n.cid, nil
}

// WriteBlocks serializes all dirty nodes and writes them to the store.
// Returns the root CID.
func (t *Tree) WriteBlocks(store BlockStore) (cbor.CID, error) {
	if t.root == nil {
		nd := &NodeData{Entries: []EntryData{}}
		data, err := encodeNodeData(nd)
		if err != nil {
			return cbor.CID{}, err
		}
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
		if err := store.PutBlock(cid, data); err != nil {
			return cbor.CID{}, err
		}
		return cid, nil
	}
	return t.writeNode(store, t.root)
}

func (t *Tree) writeNode(store BlockStore, n *node) (cbor.CID, error) {
	if !n.dirty && n.cid.Defined() {
		return n.cid, nil
	}

	if err := t.ensureLoaded(n); err != nil {
		return cbor.CID{}, err
	}

	// Recursively write children first.
	if n.left != nil {
		cid, err := t.writeNode(store, n.left)
		if err != nil {
			return cbor.CID{}, err
		}
		n.left.cid = cid
	}
	for i := range n.entries {
		if n.entries[i].right != nil {
			cid, err := t.writeNode(store, n.entries[i].right)
			if err != nil {
				return cbor.CID{}, err
			}
			n.entries[i].right.cid = cid
		}
	}

	nd, err := t.nodeToData(n)
	if err != nil {
		return cbor.CID{}, err
	}
	data, err := encodeNodeData(nd)
	if err != nil {
		return cbor.CID{}, err
	}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	if err := store.PutBlock(cid, data); err != nil {
		return cbor.CID{}, err
	}
	n.cid = cid
	n.dirty = false
	return cid, nil
}

// nodeToData converts an in-memory node to the serializable NodeData.
func (t *Tree) nodeToData(n *node) (*NodeData, error) {
	nd := &NodeData{
		Entries: make([]EntryData, len(n.entries)),
	}

	if n.left != nil {
		cid, err := t.computeCID(n.left)
		if err != nil {
			return nil, err
		}
		nd.Left = gt.Some(cid)
	}

	prevKey := ""
	for i, e := range n.entries {
		// Compute prefix compression.
		prefixLen := sharedPrefixLen(prevKey, e.key)
		nd.Entries[i] = EntryData{
			PrefixLen: prefixLen,
			// uses unsafe to avoid unnecessary allocations
			KeySuffix: unsafe.Slice(unsafe.StringData(e.key[prefixLen:]), len(e.key)-prefixLen),
			Value:     e.val,
		}
		if e.right != nil {
			cid, err := t.computeCID(e.right)
			if err != nil {
				return nil, err
			}
			nd.Entries[i].Right = gt.Some(cid)
		}
		prevKey = e.key
	}

	return nd, nil
}

// ensureLoaded loads a node from the store if it hasn't been loaded yet.
func (t *Tree) ensureLoaded(n *node) error {
	if n.dirty || len(n.entries) > 0 || n.left != nil {
		return nil // already loaded or newly created
	}
	if !n.cid.Defined() {
		return nil // empty node
	}

	data, err := t.store.GetBlock(n.cid)
	if err != nil {
		return fmt.Errorf("mst: loading node %s: %w", n.cid.String(), err)
	}

	nd, err := DecodeNodeData(data)
	if err != nil {
		return err
	}

	// Batch-allocate child nodes into a single slice. Without this, each
	// &node{} for left/right children is a separate heap allocation (~N+1
	// per node). A single make([]node, N) turns these into one allocation.
	childCount := 0
	if nd.Left.HasVal() {
		childCount++
	}
	for i := range nd.Entries {
		if nd.Entries[i].Right.HasVal() {
			childCount++
		}
	}
	children := make([]node, childCount)
	ci := 0

	// Reconstruct in-memory node.
	if nd.Left.HasVal() {
		children[ci].cid = nd.Left.Val()
		n.left = &children[ci]
		ci++
	}

	// Reconstruct entry keys using a shared buffer. Without this,
	// prevKey[:pfx] + string(suffix) would allocate twice per entry: once
	// for string(suffix) and once for the concatenation result. The buffer
	// approach does one alloc per entry (the string(keyBuf) conversion).
	var keyBuf []byte
	n.entries = make([]entry, len(nd.Entries))
	for i, ed := range nd.Entries {
		keyBuf = append(keyBuf[:ed.PrefixLen], ed.KeySuffix...)
		n.entries[i] = entry{
			key: string(keyBuf),
			val: ed.Value,
		}
		if ed.Right.HasVal() {
			children[ci].cid = ed.Right.Val()
			n.entries[i].right = &children[ci]
			ci++
		}
	}

	// Determine height from entries (all entries at same level have same height).
	if len(n.entries) > 0 {
		n.height = HeightForKey(n.entries[0].key)
	}

	return nil
}

// sharedPrefixLen returns the length of the common prefix between two strings.
func sharedPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// IsValidMstKey checks if a string is a valid MST key.
// Valid keys have the format "collection/rkey", are at most 1024 bytes,
// and contain only [a-zA-Z0-9_~\-:.] characters.
func IsValidMstKey(key string) bool {
	if len(key) == 0 || len(key) > 1024 {
		return false
	}
	slash := -1
	for i := range len(key) {
		if key[i] == '/' {
			if slash >= 0 {
				return false // multiple slashes
			}
			slash = i
			continue
		}
		if !isValidMstKeyChar(key[i]) {
			return false
		}
	}
	return slash > 0 && slash < len(key)-1
}

func isValidMstKeyChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '_' || c == '~' || c == '-' || c == ':' || c == '.'
}
