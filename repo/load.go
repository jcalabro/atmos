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

// LoadCompleteFromCAR loads a repository from a CAR v1 stream like
// [LoadFromCAR], then verifies the loaded repo is COMPLETE — every MST node
// and every referenced record block reachable from the commit's data root is
// present in the decoded block set (see [Repo.CheckComplete]).
//
// Use this for a FULL-repo CAR (getRepo with no `since`), where a missing
// block means the stream was truncated rather than a legitimate diff. A CAR
// truncated exactly on a block boundary is indistinguishable, at the framing
// layer, from a complete smaller CAR: the reader sees a clean io.EOF and
// LoadFromCAR returns successfully with a partial block set. CheckComplete is
// the semantic check that catches that case, surfacing it (wrapped in
// io.ErrUnexpectedEOF) so download/retry logic treats it as the transient
// truncation it is.
//
// Do NOT use this to load an intentional diff CAR (getRepo with `since`),
// whose referenced-but-unchanged blocks are legitimately absent; use
// [LoadFromCAR] there.
func LoadCompleteFromCAR(r io.Reader) (*Repo, *Commit, error) {
	rp, commit, err := LoadFromCAR(r)
	if err != nil {
		return nil, nil, err
	}
	if err := rp.CheckComplete(); err != nil {
		return nil, nil, err
	}
	return rp, commit, nil
}

// LoadFromCAR loads a repository from a CAR v1 stream.
// Returns the Repo and the decoded Commit.
//
// LoadFromCAR is permissive about completeness: it loads whatever blocks the
// stream carried and returns successfully as long as the commit block itself
// decoded. A CAR truncated on a block boundary therefore loads without error
// but yields a repo whose MST references blocks that are absent. Callers that
// downloaded a FULL repo (no `since` diff) and need to detect such truncation
// should use [LoadCompleteFromCAR] or call [Repo.CheckComplete].
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

	// Load and decode commit. The commit block is the CAR's declared root, so
	// a CAR that omits it is never a legitimate (e.g. diff) repo — it is a
	// truncated or corrupt stream. A boundary-aligned truncation that cuts the
	// body before the root block arrives reaches here with a clean parse (the
	// reader saw a legitimate-looking io.EOF) yet no root. Classify it as the
	// transient truncation it is (wrap io.ErrUnexpectedEOF) so download/retry
	// logic re-fetches rather than permanently failing the repo.
	commitData, err := store.GetBlock(commitCID)
	if err != nil {
		return nil, nil, fmt.Errorf("repo: commit block not found: %w: %w", io.ErrUnexpectedEOF, err)
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
