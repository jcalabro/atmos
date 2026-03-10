// Package repo implements AT Protocol repository operations.
//
// A repository is a collection of records organized in a Merkle Search Tree,
// wrapped in signed commits, and stored as CAR files.
package repo

import (
	"fmt"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
)

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
