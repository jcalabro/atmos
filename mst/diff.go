package mst

import "github.com/jcalabro/atmos/cbor"

// DiffOp represents a single difference between two trees.
type DiffOp struct {
	Key string
	Old *cbor.CID // nil for creates
	New *cbor.CID // nil for deletes
}

// Diff computes the differences between two MST roots.
// Returns creates, updates, and deletes needed to go from oldRoot to newRoot.
//
// Both trees are walked in sorted order simultaneously (merge-walk) so that
// the algorithm is purely linear — no map allocations, no hash lookups, and
// sequential memory access patterns that the CPU prefetcher can predict.
func Diff(store BlockStore, oldRoot, newRoot cbor.CID) ([]DiffOp, error) {
	if oldRoot.Equal(newRoot) {
		return nil, nil
	}

	oldTree := LoadTree(store, oldRoot)
	newTree := LoadTree(store, newRoot)

	// Collect sorted entries from both trees. Walk already yields in order.
	type kv struct {
		key string
		val cbor.CID
	}

	var oldEntries []kv
	if err := oldTree.Walk(func(key string, val cbor.CID) error {
		oldEntries = append(oldEntries, kv{key, val})
		return nil
	}); err != nil {
		return nil, err
	}

	var newEntries []kv
	if err := newTree.Walk(func(key string, val cbor.CID) error {
		newEntries = append(newEntries, kv{key, val})
		return nil
	}); err != nil {
		return nil, err
	}

	// Merge-walk: both slices are sorted, advance whichever is behind.
	var ops []DiffOp
	oi, ni := 0, 0
	for oi < len(oldEntries) && ni < len(newEntries) {
		o, n := oldEntries[oi], newEntries[ni]
		switch {
		case o.key < n.key:
			// Old key missing from new tree → delete.
			v := o.val
			ops = append(ops, DiffOp{Key: o.key, Old: &v})
			oi++
		case o.key > n.key:
			// New key missing from old tree → create.
			v := n.val
			ops = append(ops, DiffOp{Key: n.key, New: &v})
			ni++
		default:
			// Same key in both — check for update.
			if !o.val.Equal(n.val) {
				ov, nv := o.val, n.val
				ops = append(ops, DiffOp{Key: o.key, Old: &ov, New: &nv})
			}
			oi++
			ni++
		}
	}

	// Remaining old entries are deletes.
	for ; oi < len(oldEntries); oi++ {
		v := oldEntries[oi].val
		ops = append(ops, DiffOp{Key: oldEntries[oi].key, Old: &v})
	}

	// Remaining new entries are creates.
	for ; ni < len(newEntries); ni++ {
		v := newEntries[ni].val
		ops = append(ops, DiffOp{Key: newEntries[ni].key, New: &v})
	}

	return ops, nil
}
