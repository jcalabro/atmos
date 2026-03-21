package repo

import (
	"errors"
	"fmt"
	"io"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

// LoadFromCAR loads a repository from a CAR v1 stream.
// Returns the Repo and the decoded Commit.
func LoadFromCAR(r io.Reader) (*Repo, *Commit, error) {
	cr, err := car.NewReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("repo: reading CAR header: %w", err)
	}

	header := cr.Header()
	if len(header.Roots) == 0 {
		return nil, nil, errors.New("repo: CAR has no roots")
	}

	store := mst.NewMemBlockStore()
	commitCID := header.Roots[0]

	// Read all blocks into the store.
	for {
		block, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("repo: reading CAR block: %w", err)
		}
		if err := store.PutBlock(block.CID, block.Data); err != nil {
			return nil, nil, fmt.Errorf("repo: storing block: %w", err)
		}
	}

	// Load and decode commit.
	commitData, err := store.GetBlock(commitCID)
	if err != nil {
		return nil, nil, fmt.Errorf("repo: commit block not found: %w", err)
	}
	commit, err := decodeCommit(commitData)
	if err != nil {
		return nil, nil, err
	}

	// Parse DID.
	did, err := atmos.ParseDID(commit.DID)
	if err != nil {
		return nil, nil, fmt.Errorf("repo: invalid DID in commit: %w", err)
	}

	// Reconstruct TID clock from rev if available.
	var clock *atmos.TIDClock
	if commit.Rev != "" {
		tid, err := atmos.ParseTID(commit.Rev)
		if err != nil {
			return nil, nil, fmt.Errorf("repo: invalid rev TID: %w", err)
		}
		clock = atmos.ClockFromTID(tid)
	} else {
		clock = atmos.NewTIDClock(0)
	}

	// Load MST from the data root.
	tree := mst.LoadTree(store, commit.Data)

	return &Repo{
		DID:   did,
		Clock: clock,
		Store: store,
		Tree:  tree,
	}, commit, nil
}

// LoadBlocksFromCAR reads a CAR file and returns all blocks in a MemBlockStore
// along with the root CID.
func LoadBlocksFromCAR(r io.Reader) (*mst.MemBlockStore, cbor.CID, error) {
	cr, err := car.NewReader(r)
	if err != nil {
		return nil, cbor.CID{}, err
	}

	header := cr.Header()
	if len(header.Roots) == 0 {
		return nil, cbor.CID{}, errors.New("repo: CAR has no roots")
	}

	store := mst.NewMemBlockStore()
	for {
		block, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, cbor.CID{}, err
		}
		if err := store.PutBlock(block.CID, block.Data); err != nil {
			return nil, cbor.CID{}, err
		}
	}

	return store, header.Roots[0], nil
}
