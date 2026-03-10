package repo

import (
	"fmt"
	"io"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
)

// ExportCAR exports the repository as a CAR v1 file.
// Creates a new signed commit using the given private key.
func (r *Repo) ExportCAR(w io.Writer, key crypto.PrivateKey) error {
	commit, err := r.Commit(key)
	if err != nil {
		return err
	}

	// Retrieve the commit block that Commit() already encoded and stored.
	commitData, err := encodeCommit(commit)
	if err != nil {
		return fmt.Errorf("repo: encoding commit: %w", err)
	}
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)

	// TODO: avoid this double-encode by having Commit() return the CID.

	// Collect all blocks: commit + MST nodes + record data.
	var blocks []car.Block
	blocks = append(blocks, car.Block{CID: commitCID, Data: commitData})

	// Walk the MST and collect all blocks.
	seen := make(map[string]bool)
	if err := collectBlocks(r.Store, commit.Data, &blocks, seen); err != nil {
		return fmt.Errorf("repo: collecting MST blocks: %w", err)
	}

	return car.WriteAll(w, []cbor.CID{commitCID}, blocks)
}

// collectBlocks recursively collects MST node blocks and their referenced record blocks.
// Uses seen to deduplicate blocks.
func collectBlocks(store mst.BlockStore, cid cbor.CID, blocks *[]car.Block, seen map[string]bool) error {
	cidKey := string(cid.Bytes())
	if seen[cidKey] {
		return nil
	}
	seen[cidKey] = true

	data, err := store.GetBlock(cid)
	if err != nil {
		return err
	}
	*blocks = append(*blocks, car.Block{CID: cid, Data: data})

	// Try to decode as MST node to find child CIDs.
	// Decode failure means this is not an MST node (e.g. a record block); skip children.
	nd, err := mst.DecodeNodeData(data)
	if err != nil {
		return nil //nolint:nilerr // expected: non-MST blocks fail to decode
	}

	// Collect left child.
	if nd.Left.HasVal() {
		if err := collectBlocks(store, nd.Left.Val(), blocks, seen); err != nil {
			return err
		}
	}

	// Collect entries.
	for i := range nd.Entries {
		// Value CID — record block.
		valCID := nd.Entries[i].Value
		valKey := string(valCID.Bytes())
		if !seen[valKey] {
			seen[valKey] = true
			if valData, err := store.GetBlock(valCID); err == nil {
				*blocks = append(*blocks, car.Block{CID: valCID, Data: valData})
			}
		}
		// Right subtree.
		if nd.Entries[i].Right.HasVal() {
			if err := collectBlocks(store, nd.Entries[i].Right.Val(), blocks, seen); err != nil {
				return err
			}
		}
	}

	return nil
}
