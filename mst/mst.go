// Package mst implements the Merkle Search Tree used by AT Protocol repositories.
package mst

import (
	"errors"
	"fmt"
	"iter"
	"unsafe"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
)

// ErrBlockNotFound is returned (wrapped) by a BlockStore.GetBlock when the
// requested CID is absent. MemBlockStore wraps it; other BlockStore
// implementations should do the same so callers can classify a missing block
// with errors.Is rather than string-matching. A missing block reached while
// walking an MST loaded from a downloaded CAR almost always means the CAR was
// truncated on a block boundary (a complete-looking but partial stream), which
// callers treat as a transient download corruption.
var ErrBlockNotFound = errors.New("block not found")

// ErrInvalidTree is returned (wrapped) when blocks loaded from a store do
// not form a valid MST: a malformed node, keys out of order or at the wrong
// height, or a node the canonical tree shape does not allow. Blocks usually
// come from an untrusted CAR or sync peer, so loading checks all of these
// rather than trusting the structure it is handed.
var ErrInvalidTree = errors.New("mst: invalid tree")

// ErrInvalidKey is returned (wrapped) by Insert for a key that is not a
// valid MST key (see IsValidMstKey).
var ErrInvalidKey = errors.New("mst: invalid key")

// BlockStore is a content-addressed block storage interface.
type BlockStore interface {
	// GetBlock retrieves a block by its CID. Returns an error wrapping
	// ErrBlockNotFound if the CID is absent.
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
		return nil, fmt.Errorf("%w: %s", ErrBlockNotFound, cid.String())
	}
	return data, nil
}

// PutBlock stores a block at the given CID.
func (s *MemBlockStore) PutBlock(cid cbor.CID, data []byte) error {
	s.blocks[cid] = data
	return nil
}

// All returns an iterator over every (cid, data) pair currently in
// the store. Order is unspecified.
//
// Like all MemBlockStore methods, not safe for concurrent use;
// concurrent mutation during iteration will panic per Go's map-
// iteration semantics.
func (s *MemBlockStore) All() iter.Seq2[cbor.CID, []byte] {
	return func(yield func(cbor.CID, []byte) bool) {
		for cid, data := range s.blocks {
			if !yield(cid, data) {
				return
			}
		}
	}
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
// (left, entries, height, dirty, cidFresh, loaded) sit in the first 37 bytes
// so that ensureLoaded's guard check and getNode's descent stay within a
// single 64-byte cache line. The cold CID (only touched during serialization
// / loading) trails at the end and spills to a second line.
//
// A clean node (!dirty) is stored as-is under cid. A dirty node has changed
// since it was loaded or last written, so WriteBlocks must write it; its cid
// describes its contents only while cidFresh is set, which RootCID does
// without writing anything.
type node struct {
	left     *node    // 8B  — hot: every traversal
	entries  []entry  // 24B — hot: every traversal
	height   uint8    // 1B  — hot: insert level checks
	dirty    bool     // 1B  — hot: ensureLoaded guard
	cidFresh bool     // 1B  — cold: cid is current for a dirty node
	loaded   bool     // 1B  — hot: ensureLoaded guard; an empty node looks like a stub without it
	isRoot   bool     // 1B  — cold: loaded as a stored tree's root, so its keys set its height
	cid      cbor.CID // 33B — cold: serialization / loading only
}

// markDirty records that n changed: it must be written, and its cached CID
// no longer describes it.
func (n *node) markDirty() {
	n.dirty = true
	n.cidFresh = false
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
		root:  &node{cid: root, isRoot: true},
	}
}

// LoadAll eagerly loads every node in the tree from the block store,
// decoding all CBOR upfront. After LoadAll, operations like Walk and
// Get become pure in-memory pointer traversals with no further I/O
// or decoding. Like Walk, it checks key order across the whole tree.
func (t *Tree) LoadAll() error {
	return t.Walk(nil)
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

// Insert inserts or updates a key/value pair. It returns an error wrapping
// ErrInvalidKey if key is not a valid MST key. If it returns an error, the
// tree is unchanged.
func (t *Tree) Insert(key string, val cbor.CID) error {
	if !IsValidMstKey(key) {
		return fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
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
			n.markDirty()
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

	n.markDirty()
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
		n.markDirty()
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

	n.markDirty()
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
		n.markDirty()
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
		n.markDirty()
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

// Remove deletes a key from the tree. If it returns an error, the tree
// is unchanged.
func (t *Tree) Remove(key string) error {
	if t.root == nil {
		return nil
	}
	// Load what the trim below needs before removeNode mutates anything.
	// A root that keeps an entry stays the root, so skip the call for it.
	if r := t.root; len(r.entries) == 0 || (len(r.entries) == 1 && r.entries[0].key == key) {
		if err := t.loadRootAfterRemove(key); err != nil {
			return err
		}
	}
	newRoot, changed, err := t.removeNode(t.root, key)
	if err != nil || !changed {
		return err
	}
	// Trim the top: collapse empty-passthrough root nodes that only
	// have a left child. ensureLoaded each candidate before testing —
	// an unloaded stub looks like an empty node (entries==nil and
	// left==nil) at the Go level, but on disk it may carry a real
	// child subtree. Without loading it first, the loop would walk
	// off the end of the chain and produce an empty tree, dropping
	// every record below the removed key.
	for newRoot != nil {
		if err := t.ensureLoaded(newRoot); err != nil {
			return err
		}
		if len(newRoot.entries) > 0 {
			break
		}
		newRoot = newRoot.left
	}
	t.root = newRoot
	return nil
}

// loadRootAfterRemove loads the nodes Remove's trim will visit, so a
// missing block errors before the tree is mutated. It stops at the first
// level where either side of the removed entry holds an entry: the merge
// roots there, and loading deeper could fail a removal that would succeed.
func (t *Tree) loadRootAfterRemove(key string) error {
	// Walk down any entry-less nodes above the topmost entry, as the trim does.
	n := t.root
	for {
		if err := t.ensureLoaded(n); err != nil {
			return err
		}
		if len(n.entries) > 0 {
			break
		}
		if n.left == nil {
			return nil
		}
		n = n.left
	}
	// Any other entry stays put and the trim stops at n.
	if len(n.entries) > 1 || n.entries[0].key != key {
		return nil
	}

	// mergeNodes folds the two sides together level by level through each
	// side's left child, until a level where either side holds an entry.
	left, right := n.left, n.entries[0].right
	for left != nil || right != nil {
		entries := 0
		for _, side := range [2]*node{left, right} {
			if side == nil {
				continue
			}
			if err := t.ensureLoaded(side); err != nil {
				return err
			}
			entries += len(side.entries)
		}
		if entries > 0 {
			return nil
		}
		if left != nil {
			left = left.left
		}
		if right != nil {
			right = right.left
		}
	}
	return nil
}

// removeNode removes key from the subtree rooted at n. It returns the
// subtree's new root, nil once the subtree holds nothing, and whether
// anything changed.
func (t *Tree) removeNode(n *node, key string) (*node, bool, error) {
	if n == nil {
		return nil, false, nil
	}
	if err := t.ensureLoaded(n); err != nil {
		return nil, false, err
	}

	i := t.findChildIndex(n, key)
	if i < len(n.entries) && n.entries[i].key == key {
		// Found it. Merge left and right children around this entry.
		leftChild := n.left
		if i > 0 {
			leftChild = n.entries[i-1].right
		}
		merged, err := t.mergeNodes(leftChild, n.entries[i].right)
		if err != nil {
			return nil, false, err
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
		n.markDirty()
	} else {
		// Descend into the child that would hold key.
		child := n.left
		if i > 0 {
			child = n.entries[i-1].right
		}
		newChild, changed, err := t.removeNode(child, key)
		if err != nil || !changed {
			return n, false, err
		}
		if i == 0 {
			n.left = newChild
		} else {
			n.entries[i-1].right = newChild
		}
		n.markDirty()
	}

	// A node left with no entries and no child holds nothing, so the parent
	// drops it. One with no entries but a left child stays: the canonical
	// shape needs that empty node to fill the height gap between its parent
	// and the child. Returning n.left directly would skip a level.
	if len(n.entries) == 0 && n.left == nil {
		return nil, true, nil
	}
	return n, true, nil
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
	left.markDirty()

	return left, nil
}

// Walk traverses all key/value pairs in sorted order. It returns an error
// wrapping ErrInvalidTree if the keys are not in strictly ascending order
// across the whole tree, which also stops a walk that reaches the same
// subtree twice through a hostile block graph.
func (t *Tree) Walk(fn func(key string, val cbor.CID) error) error {
	if t.root == nil {
		return nil
	}
	var prev string // valid keys are non-empty, so every key sorts after ""
	return t.walkNode(t.root, fn, 0, &prev)
}

// walkNode visits n's subtree in order, calling fn if it is non-nil. prev
// holds the last key visited.
func (t *Tree) walkNode(n *node, fn func(key string, val cbor.CID) error, depth int, prev *string) error {
	if n == nil {
		return nil
	}
	if depth > MaxDepth {
		return ErrMaxDepthExceeded
	}
	if err := t.ensureLoaded(n); err != nil {
		return err
	}

	// Visit left subtree first.
	if err := t.walkNode(n.left, fn, depth+1, prev); err != nil {
		return err
	}

	for _, e := range n.entries {
		// Keys within a node are checked on load; this catches a subtree
		// holding keys outside the range its position allows.
		if e.key <= *prev {
			return fmt.Errorf("%w: key %q is not greater than previous key %q", ErrInvalidTree, e.key, *prev)
		}
		*prev = e.key
		if fn != nil {
			if err := fn(e.key, e.val); err != nil {
				return err
			}
		}
		if err := t.walkNode(e.right, fn, depth+1, prev); err != nil {
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
	if n.cidFresh || (!n.dirty && n.cid.Defined()) {
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
	// Cache the CID, but leave the node dirty: it is not written yet.
	n.cid = cbor.ComputeCID(cbor.CodecDagCBOR, data)
	n.cidFresh = true
	return n.cid, nil
}

// WriteBlocks serializes every node changed since the tree was loaded or
// last written, writes them to the store, and returns the root CID. Nodes
// left unchanged since loading are not copied.
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
//
// Blocks are untrusted, so it checks everything about the node that can be
// checked without its neighbours: every key valid, in order, compressed
// against the previous key as far as possible, and at the height its parent
// expects; no child below layer 0; and no empty node except the root of an
// empty tree. Walk checks key order across nodes.
func (t *Tree) ensureLoaded(n *node) error {
	if n.dirty || len(n.entries) > 0 || n.left != nil || n.loaded {
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
		return invalidNode(n.cid, "%w", err)
	}

	// Empty nodes are pruned from the top and bottom of the tree. The only
	// entry-less nodes are the root of an empty tree, and nodes below the
	// root that bridge a height gap down to their left child. (A root with
	// no entries but a left child fails the height check below: with no
	// keys to give it a height, it sits at layer 0, where nothing has a
	// child.)
	if len(nd.Entries) == 0 && !n.isRoot && !nd.Left.HasVal() {
		return invalidNode(n.cid, "empty node below the root")
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

	// Reconstruct into locals and publish to n only once every entry has
	// validated. A node with entries or a left child passes the guard above
	// as loaded, so publishing as we go would leave a half-built node behind
	// on a bad block, and every later call would trust it.
	var left *node
	if nd.Left.HasVal() {
		children[ci].cid = nd.Left.Val()
		left = &children[ci]
		ci++
	}

	// A child's height is seeded by its parent: every edge in the canonical
	// tree spans exactly one layer. Only a stored tree's root learns its
	// height from its own keys.
	height := n.height

	// Reconstruct entry keys using a shared buffer. Without this,
	// prevKey[:pfx] + string(suffix) would allocate twice per entry: once
	// for string(suffix) and once for the concatenation result. The buffer
	// approach does one alloc per entry (the string(keyBuf) conversion).
	var keyBuf []byte
	entries := make([]entry, len(nd.Entries))
	for i, ed := range nd.Entries {
		// The prefix length must reference bytes that actually exist in the
		// previously reconstructed key. A hostile or corrupt block can declare
		// a prefix longer than the running key (or non-zero on the first
		// entry, which has no predecessor); reject it rather than panicking on
		// the reslice below.
		if ed.PrefixLen > len(keyBuf) {
			return invalidNode(n.cid, "entry %d: prefix length %d exceeds previous key length %d", i, ed.PrefixLen, len(keyBuf))
		}
		// Prefix compression is mandatory, so the prefix must cover every
		// byte shared with the previous key. A shorter one would give the
		// same node a second encoding, and so a second CID.
		if ed.PrefixLen < len(keyBuf) && len(ed.KeySuffix) > 0 && ed.KeySuffix[0] == keyBuf[ed.PrefixLen] {
			return invalidNode(n.cid, "entry %d: prefix length %d is shorter than the prefix shared with the previous key", i, ed.PrefixLen)
		}
		keyBuf = append(keyBuf[:ed.PrefixLen], ed.KeySuffix...)
		key := string(keyBuf)
		// Entries within a node must be in strictly ascending key order.
		// Accepting an out-of-order block would silently corrupt lookups (Get
		// relies on this ordering), so reject it on load.
		if i > 0 && key <= entries[i-1].key {
			return invalidNode(n.cid, "entry %d: key %q is not greater than previous key %q", i, key, entries[i-1].key)
		}
		if !IsValidMstKey(key) {
			return invalidNode(n.cid, "entry %d: invalid key %q", i, key)
		}
		keyHeight := HeightForKey(key)
		if i == 0 && n.isRoot {
			height = keyHeight
		}
		if keyHeight != height {
			return invalidNode(n.cid, "entry %d: key %q has height %d in a node at height %d", i, key, keyHeight, height)
		}
		entries[i] = entry{
			key: key,
			val: ed.Value,
		}
		if ed.Right.HasVal() {
			children[ci].cid = ed.Right.Val()
			entries[i].right = &children[ci]
			ci++
		}
	}
	if childCount > 0 && height == 0 {
		return invalidNode(n.cid, "node at height 0 has a child")
	}

	// Seed each child stub with the height it must have when it loads.
	for i := range children {
		children[i].height = height - 1
	}
	n.left = left
	n.entries = entries
	n.height = height
	n.loaded = true
	return nil
}

// invalidNode returns an error wrapping ErrInvalidTree for the node at cid.
func invalidNode(cid cbor.CID, format string, args ...any) error {
	return fmt.Errorf("%w: node %s: %w", ErrInvalidTree, cid.String(), fmt.Errorf(format, args...))
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
	if len(key) == 0 || len(key) > maxKeyLen {
		return false
	}
	slash := -1
	for i := range len(key) {
		if mstKeyChars[key[i]] {
			continue
		}
		if key[i] != '/' || slash >= 0 {
			return false // invalid char, or multiple slashes
		}
		slash = i
	}
	return slash > 0 && slash < len(key)-1
}

// mstKeyChars marks the bytes allowed in an MST key, other than the slash
// separating collection from rkey. Every loaded key is checked, so a table
// lookup beats a chain of range comparisons.
var mstKeyChars = func() (chars [256]bool) {
	for c := range len(chars) {
		chars[c] = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '_' || c == '~' || c == '-' || c == ':' || c == '.'
	}
	return chars
}()
