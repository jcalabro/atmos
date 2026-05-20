package sync

import (
	"context"
	stdsync "sync"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
)

// ChainState is the per-DID state the verifier tracks: the last
// commit rev and the last MST root data CID we successfully verified.
type ChainState struct {
	Rev  string
	Data cbor.CID
}

// ChainStore persists per-DID chain state across firehose events.
//
// Production consumers MUST implement this against durable storage
// (pebble, sqlite, etc.); the in-memory default shipped with atmos is
// suitable only for tests and dev. State loss on restart means
// previously-verified chain links are forgotten and the next event
// for each DID will be accepted as ground truth.
//
// Returning (nil, nil) from Load means "no state for this DID yet";
// the verifier treats that as a first-sighting and accepts whatever
// the next commit declares as ground truth, advancing state to it.
//
// Implementations MAY skip fsync per Save call: a crash that loses
// recent saves is recovered by the verifier's rev-replay gate, which
// silently drops re-delivered events whose rev <= the persisted rev.
type ChainStore interface {
	Load(ctx context.Context, did atmos.DID) (*ChainState, error)
	Save(ctx context.Context, did atmos.DID, state ChainState) error
	Delete(ctx context.Context, did atmos.DID) error
}

// MemChainStore is a sync.Map-backed ChainStore. NOT suitable for
// production: state is lost on restart.
type MemChainStore struct {
	m stdsync.Map // map[atmos.DID]ChainState
}

// NewMemChainStore returns an empty in-memory ChainStore.
func NewMemChainStore() *MemChainStore {
	return &MemChainStore{}
}

// Load returns the chain state for did, or (nil, nil) if no state
// has been saved yet.
func (s *MemChainStore) Load(_ context.Context, did atmos.DID) (*ChainState, error) {
	v, ok := s.m.Load(did)
	if !ok {
		return nil, nil
	}
	state, ok := v.(ChainState)
	if !ok {
		// We are the sole writer of this map; a non-ChainState value
		// means memory corruption or a programming error elsewhere.
		// Crash rather than silently lose state.
		panic("MemChainStore: stored value is not ChainState")
	}
	return &state, nil
}

// Save records the chain state for did.
func (s *MemChainStore) Save(_ context.Context, did atmos.DID, state ChainState) error {
	s.m.Store(did, state)
	return nil
}

// Delete removes any chain state for did. No-op if absent.
func (s *MemChainStore) Delete(_ context.Context, did atmos.DID) error {
	s.m.Delete(did)
	return nil
}
