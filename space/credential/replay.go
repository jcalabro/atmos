package credential

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrReplay indicates that a replay identifier has already been consumed.
	ErrReplay = errors.New("credential: replay detected")
	// ErrReplayCapacity indicates that bounded storage cannot safely retain a
	// new identifier for its entire acceptance window.
	ErrReplayCapacity = errors.New("credential: replay store capacity exhausted")
)

const maxReplayIDBytes = 256

// ReplayNamespace prevents identifiers from unrelated authentication
// profiles from colliding.
type ReplayNamespace string

const (
	// ReplayDelegation is the space-delegation token namespace.
	ReplayDelegation ReplayNamespace = "delegation"
	// ReplayAttestation is the client-attestation token namespace.
	ReplayAttestation ReplayNamespace = "attestation"
	// ReplayDPoP is the DPoP proof namespace.
	ReplayDPoP ReplayNamespace = "dpop"
	// ReplayServiceAuth is reserved for service-auth JWTs.
	ReplayServiceAuth ReplayNamespace = "service-auth"
)

// ReplayStore atomically records a replay identifier until expiresAt. A store
// must never evict a live entry to make room. Implementations shared by an
// accepting fleet must provide the same atomic guarantee across replicas.
type ReplayStore interface {
	Consume(ctx context.Context, namespace ReplayNamespace, id string, expiresAt time.Time) error
}

// MemoryReplayStore is a concurrency-safe bounded replay store. It is suitable
// for tests and single-process deployments, not a substitute for durable shared
// storage in a replicated authority.
type MemoryReplayStore struct {
	mu         sync.Mutex
	maxEntries int
	entries    map[replayKey]time.Time
	now        func() time.Time
}

type replayKey struct {
	namespace ReplayNamespace
	id        string
}

// NewMemoryReplayStore constructs a store that retains at most maxEntries live
// identifiers.
func NewMemoryReplayStore(maxEntries int) (*MemoryReplayStore, error) {
	if maxEntries <= 0 {
		return nil, errors.New("credential: replay store maximum entries must be positive")
	}
	return &MemoryReplayStore{
		maxEntries: maxEntries,
		entries:    make(map[replayKey]time.Time),
		now:        time.Now,
	}, nil
}

// Consume implements [ReplayStore]. Expired entries are removed before the
// capacity check; unexpired entries are never evicted.
func (s *MemoryReplayStore) Consume(ctx context.Context, namespace ReplayNamespace, id string, expiresAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validReplayNamespace(namespace) {
		return fmt.Errorf("credential: invalid replay namespace %q", namespace)
	}
	if id == "" || len(id) > maxReplayIDBytes {
		return fmt.Errorf("credential: replay identifier must contain 1..%d bytes", maxReplayIDBytes)
	}
	if expiresAt.IsZero() {
		return errors.New("credential: replay expiration is required")
	}
	if s == nil {
		return errors.New("credential: nil replay store")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !expiresAt.After(now) {
		return errors.New("credential: replay expiration must be in the future")
	}
	for key, expiry := range s.entries {
		if !expiry.After(now) {
			delete(s.entries, key)
		}
	}
	key := replayKey{namespace: namespace, id: id}
	if _, exists := s.entries[key]; exists {
		return ErrReplay
	}
	if len(s.entries) >= s.maxEntries {
		return ErrReplayCapacity
	}
	s.entries[key] = expiresAt
	return nil
}

func validReplayNamespace(namespace ReplayNamespace) bool {
	switch namespace {
	case ReplayDelegation, ReplayAttestation, ReplayDPoP, ReplayServiceAuth:
		return true
	default:
		return false
	}
}
