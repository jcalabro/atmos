// Package repo implements AT Protocol repository operations.
//
// A repository is a collection of records organized in a Merkle Search Tree,
// wrapped in signed commits, and stored as CAR files.
package repo

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
)

// SplitMSTKey splits a record path of the form "collection/rkey"
// (e.g. "app.bsky.feed.post/3jqfcqzm3fp2j") into its collection and
// rkey components, splitting at the FIRST '/' so a malformed key
// with multiple slashes still puts the intended NSID on the left.
//
// Returns (path, "") when path contains no '/' — callers can treat
// that as "not a valid MST key" via [mst.IsValidMstKey] if they need
// strict validation.
func SplitMSTKey(path string) (collection, rkey string) {
	collection, rkey, _ = strings.Cut(path, "/")
	return collection, rkey
}

// Repo is an in-memory ATProto repository.
type Repo struct {
	DID   atmos.DID
	Clock *atmos.TIDClock
	Store mst.BlockStore
	Tree  *mst.Tree
}

// validateRecordPath validates the collection/rkey pair and returns the MST key.
func validateRecordPath(collection, rkey string) (string, error) {
	if _, err := atmos.ParseNSID(collection); err != nil {
		return "", fmt.Errorf("repo: invalid record path: %w", err)
	}
	if _, err := atmos.ParseRecordKey(rkey); err != nil {
		return "", fmt.Errorf("repo: invalid record path: %w", err)
	}
	return collection + "/" + rkey, nil
}

// Get retrieves a record by collection and rkey.
// Returns the record CID, raw CBOR bytes, and any error.
func (r *Repo) Get(collection, rkey string) (cbor.CID, []byte, error) {
	key, err := validateRecordPath(collection, rkey)
	if err != nil {
		return cbor.CID{}, nil, err
	}
	cid, err := r.Tree.Get(key)
	if err != nil {
		return cbor.CID{}, nil, fmt.Errorf("repo: getting record: %w", err)
	}
	if cid == nil {
		return cbor.CID{}, nil, fmt.Errorf("repo: record not found: %s", key)
	}
	data, err := r.Store.GetBlock(*cid)
	if err != nil {
		return cbor.CID{}, nil, fmt.Errorf("repo: reading record block: %w", err)
	}
	return *cid, data, nil
}

// Create creates a new record.
func (r *Repo) Create(collection, rkey string, record any) error {
	key, err := validateRecordPath(collection, rkey)
	if err != nil {
		return err
	}
	data, err := cbor.Marshal(record)
	if err != nil {
		return fmt.Errorf("repo: encoding record: %w", err)
	}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	if err := r.Store.PutBlock(cid, data); err != nil {
		return fmt.Errorf("repo: storing record block: %w", err)
	}
	return r.Tree.Insert(key, cid)
}

// Update updates an existing record.
func (r *Repo) Update(collection, rkey string, record any) error {
	return r.Create(collection, rkey, record)
}

// Delete deletes a record.
func (r *Repo) Delete(collection, rkey string) error {
	key, err := validateRecordPath(collection, rkey)
	if err != nil {
		return err
	}
	return r.Tree.Remove(key)
}

// CheckComplete verifies the repo is structurally complete: every MST node
// and every referenced record block reachable from the data root resolves in
// the backing block store. It is the read-side analogue of the producer-side
// completeness check in ExportCAR (which refuses to emit a CAR omitting a
// referenced record).
//
// The intended use is right after loading a FULL-repo CAR (getRepo with no
// `since`). A CAR truncated exactly on a block boundary loads without error —
// the framing reader sees a clean io.EOF — but leaves the MST referencing
// blocks that never arrived. CheckComplete catches that by walking the tree:
//
//   - Tree.Walk loads every interior MST node via the store, so a missing
//     node surfaces from the walk itself.
//   - Each visited entry's record-block CID is fetched explicitly, since the
//     walk only hands back the value CID without loading the block.
//
// A missing block is reported wrapped in both [mst.ErrBlockNotFound] (so the
// specific cause is matchable) and io.ErrUnexpectedEOF (so transport/retry
// logic — e.g. xrpc transient classification — treats it as the truncated
// download it almost always is, symmetric with a mid-block CAR truncation).
//
// Do NOT call this on an intentional diff CAR (getRepo with `since`): its
// unchanged blocks are legitimately absent and the walk would fail.
func (r *Repo) CheckComplete() error {
	if r.Tree == nil {
		return nil
	}
	err := r.Tree.Walk(func(key string, val cbor.CID) error {
		if _, err := r.Store.GetBlock(val); err != nil {
			if errors.Is(err, mst.ErrBlockNotFound) {
				return fmt.Errorf("repo: incomplete CAR: record block %s for %q missing: %w: %w",
					val.String(), key, io.ErrUnexpectedEOF, err)
			}
			return fmt.Errorf("repo: reading record block %s for %q: %w", val.String(), key, err)
		}
		return nil
	})
	if err == nil {
		return nil
	}
	// A missing interior MST node surfaces from Walk itself (ensureLoaded ->
	// store.GetBlock). Classify it the same way as a missing record block so a
	// boundary-truncated CAR is uniformly retryable regardless of which level
	// of the tree the truncation severed.
	if errors.Is(err, mst.ErrBlockNotFound) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("repo: incomplete CAR: MST node missing: %w: %w", io.ErrUnexpectedEOF, err)
	}
	return err
}

// Commit creates a signed commit for the current state.
func (r *Repo) Commit(key crypto.PrivateKey) (*Commit, error) {
	rootCID, err := r.Tree.WriteBlocks(r.Store)
	if err != nil {
		return nil, fmt.Errorf("repo: writing MST blocks: %w", err)
	}

	rev := r.Clock.Next()
	c := &Commit{
		DID:     string(r.DID),
		Version: 3,
		Data:    rootCID,
		Rev:     string(rev),
	}

	if err := c.Sign(key); err != nil {
		return nil, err
	}

	// Store the commit block.
	commitData, err := encodeCommit(c)
	if err != nil {
		return nil, fmt.Errorf("repo: encoding commit: %w", err)
	}
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)
	if err := r.Store.PutBlock(commitCID, commitData); err != nil {
		return nil, fmt.Errorf("repo: storing commit: %w", err)
	}

	return c, nil
}
