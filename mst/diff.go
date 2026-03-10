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
func Diff(store BlockStore, oldRoot, newRoot cbor.CID) ([]DiffOp, error) {
	if oldRoot.Equal(newRoot) {
		return nil, nil
	}

	oldTree := LoadTree(store, oldRoot)
	newTree := LoadTree(store, newRoot)

	// Collect all entries from both trees.
	oldEntries := make(map[string]cbor.CID)
	if err := oldTree.Walk(func(key string, val cbor.CID) error {
		oldEntries[key] = val
		return nil
	}); err != nil {
		return nil, err
	}

	var ops []DiffOp

	// Walk new tree: find creates and updates.
	if err := newTree.Walk(func(key string, val cbor.CID) error {
		oldVal, existed := oldEntries[key]
		if !existed {
			v := val
			ops = append(ops, DiffOp{Key: key, New: &v})
		} else if !oldVal.Equal(val) {
			o, n := oldVal, val
			ops = append(ops, DiffOp{Key: key, Old: &o, New: &n})
		}
		delete(oldEntries, key)
		return nil
	}); err != nil {
		return nil, err
	}

	// Remaining oldEntries are deletes.
	for key, val := range oldEntries {
		v := val
		ops = append(ops, DiffOp{Key: key, Old: &v})
	}

	return ops, nil
}
