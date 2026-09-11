package space

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/zeebo/blake3"
)

const (
	// LtHashLanes is the number of uint16 lanes in an LtHash state.
	LtHashLanes = 1024
	// LtHashStateSize is the serialized size of an LtHash state.
	LtHashStateSize = LtHashLanes * 2
)

// LtHash is the homomorphic multiset hash used by permissioned repositories.
// Each element expands through BLAKE3 XOF into 1024 little-endian uint16 lanes;
// lanes are added or subtracted modulo 2^16.
//
// LtHash itself does not enforce set semantics. Its caller must maintain one
// current CID per record path and apply each transition exactly once.
type LtHash struct {
	state [LtHashStateSize]byte
}

var ltHashExpandedPool = sync.Pool{
	New: func() any { return new([LtHashStateSize]byte) },
}

// NewLtHash returns an empty LtHash.
func NewLtHash() *LtHash { return &LtHash{} }

// NewLtHashFromState restores an LtHash from its exact 2048-byte state. The
// input is copied.
func NewLtHashFromState(state []byte) (*LtHash, error) {
	if len(state) != LtHashStateSize {
		return nil, fmt.Errorf("space: LtHash state must be %d bytes, got %d", LtHashStateSize, len(state))
	}
	var out LtHash
	copy(out.state[:], state)
	return &out, nil
}

// Add adds an element to the multiset.
func (h *LtHash) Add(element []byte) { h.apply(element, false) }

// Remove subtracts an element from the multiset.
func (h *LtHash) Remove(element []byte) { h.apply(element, true) }

func (h *LtHash) apply(element []byte, subtract bool) {
	hasher := blake3.New()
	_, _ = hasher.Write(element)
	expanded, ok := ltHashExpandedPool.Get().(*[LtHashStateSize]byte)
	if !ok {
		panic("space: LtHash scratch pool contained an invalid value")
	}
	if _, err := io.ReadFull(hasher.Digest(), expanded[:]); err != nil {
		panic("space: BLAKE3 XOF unexpectedly failed: " + err.Error())
	}
	for offset := 0; offset < LtHashStateSize; offset += 2 {
		current := binary.LittleEndian.Uint16(h.state[offset : offset+2])
		value := binary.LittleEndian.Uint16(expanded[offset : offset+2])
		if subtract {
			current -= value
		} else {
			current += value
		}
		binary.LittleEndian.PutUint16(h.state[offset:offset+2], current)
	}
	ltHashExpandedPool.Put(expanded)
}

// State returns a copy of the full state for durable persistence.
func (h *LtHash) State() [LtHashStateSize]byte { return h.state }

// Digest returns SHA-256 of the full state.
func (h *LtHash) Digest() [sha256.Size]byte { return sha256.Sum256(h.state[:]) }

// IsEmpty reports whether every state byte is zero.
func (h *LtHash) IsEmpty() bool {
	var zero [LtHashStateSize]byte
	return subtle.ConstantTimeCompare(h.state[:], zero[:]) == 1
}

// Equal reports whether two LtHash states are identical. It uses a
// constant-time comparison so callers need not reason about whether a state is
// sensitive in their application.
func (h *LtHash) Equal(other *LtHash) bool {
	return h != nil && other != nil && subtle.ConstantTimeCompare(h.state[:], other.state[:]) == 1
}

// Clone returns an independent copy of h.
func (h *LtHash) Clone() *LtHash {
	if h == nil {
		return nil
	}
	clone := *h
	return &clone
}
