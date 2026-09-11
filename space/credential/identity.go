package credential

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
)

// ErrRefreshRateLimited indicates that a forced DID refresh was rejected by
// the configured active-DID bound or cooldown.
var ErrRefreshRateLimited = errors.New("credential: forced DID refresh is rate limited")

// DIDDocumentRefreshFunc bypasses an ordinary resolver cache and returns fresh
// raw DID-document evidence. Implementations must apply the same trust and
// network-safety policy as their normal resolver.
type DIDDocumentRefreshFunc func(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error)

// KeyRefresh bounds and coalesces signature-failure DID refreshes. It tracks at
// most maxActiveDIDs within the cooldown window, preventing invalid signatures
// from creating unbounded resolver traffic or state.
type KeyRefresh struct {
	refresh   DIDDocumentRefreshFunc
	maxActive int
	cooldown  time.Duration
	now       func() time.Time

	mu     sync.Mutex
	states map[atmos.DID]*keyRefreshState
}

type keyRefreshState struct {
	attemptedAt time.Time
	inFlight    bool
	done        chan struct{}
	doc         *identity.DIDDocument
	err         error
}

// NewKeyRefresh constructs a forced-refresh controller. cooldown is measured
// from the start of an attempt, including failed attempts.
func NewKeyRefresh(refresh DIDDocumentRefreshFunc, maxActiveDIDs int, cooldown time.Duration) (*KeyRefresh, error) {
	if refresh == nil {
		return nil, errors.New("credential: DID refresh function is required")
	}
	if maxActiveDIDs <= 0 || cooldown <= 0 {
		return nil, errors.New("credential: refresh maximum DIDs and cooldown must be positive")
	}
	return &KeyRefresh{
		refresh: refresh, maxActive: maxActiveDIDs, cooldown: cooldown,
		now: time.Now, states: make(map[atmos.DID]*keyRefreshState),
	}, nil
}

// ResolveKey performs a coalesced, rate-limited refresh and strictly selects
// kid from the returned raw document.
func (r *KeyRefresh) ResolveKey(ctx context.Context, did atmos.DID, kid string) (crypto.PublicKey, error) {
	if r == nil {
		return nil, errors.New("credential: nil forced-refresh controller")
	}
	if err := did.Validate(); err != nil {
		return nil, fmt.Errorf("credential: invalid refresh DID: %w", err)
	}
	if kid == "" || kid[0] != '#' {
		return nil, errors.New("credential: refresh key id must be a DID fragment beginning with #")
	}
	now := r.now()
	r.mu.Lock()
	for trackedDID, state := range r.states {
		if !state.inFlight && !state.attemptedAt.Add(r.cooldown).After(now) {
			delete(r.states, trackedDID)
		}
	}
	if state, exists := r.states[did]; exists {
		if state.inFlight {
			done := state.done
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				return refreshedKey(state, did, kid)
			}
		}
		r.mu.Unlock()
		return nil, ErrRefreshRateLimited
	}
	if len(r.states) >= r.maxActive {
		r.mu.Unlock()
		return nil, ErrRefreshRateLimited
	}
	state := &keyRefreshState{attemptedAt: now, inFlight: true, done: make(chan struct{})}
	r.states[did] = state
	r.mu.Unlock()

	state.doc, state.err = r.refresh(ctx, did)
	if state.err != nil {
		state.err = fmt.Errorf("credential: force-refresh DID %s: %w", did, state.err)
	}
	r.mu.Lock()
	state.inFlight = false
	close(state.done)
	r.mu.Unlock()
	return refreshedKey(state, did, kid)
}

func refreshedKey(state *keyRefreshState, did atmos.DID, kid string) (crypto.PublicKey, error) {
	if state.err != nil {
		return nil, state.err
	}
	if state.doc == nil {
		return nil, errors.New("credential: forced DID refresh returned no document")
	}
	_, key, err := identity.SelectVerificationMethod(state.doc, did, kid)
	if err != nil {
		return nil, fmt.Errorf("credential: select refreshed DID key %s: %w", kid, err)
	}
	return key, nil
}

// ResolveNamedDIDKey resolves a raw DID document and strictly selects the
// requested verification method. It preserves duplicate/full-ID/controller
// evidence that is lost by map-based identity projections.
func ResolveNamedDIDKey(ctx context.Context, resolver identity.Resolver, did atmos.DID, kid string) (crypto.PublicKey, error) {
	if resolver == nil {
		return nil, errors.New("credential: identity resolver is required")
	}
	if err := did.Validate(); err != nil {
		return nil, fmt.Errorf("credential: invalid DID: %w", err)
	}
	if kid == "" || kid[0] != '#' {
		return nil, errors.New("credential: key id must be a DID fragment beginning with #")
	}
	doc, err := resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("credential: resolve DID %s: %w", did, err)
	}
	_, key, err := identity.SelectVerificationMethod(doc, did, kid)
	if err != nil {
		return nil, fmt.Errorf("credential: select DID key %s: %w", kid, err)
	}
	return key, nil
}

// ResolveDelegationVerificationKey selects only the account #atproto key.
func ResolveDelegationVerificationKey(ctx context.Context, resolver identity.Resolver, did atmos.DID, kid string) (crypto.PublicKey, error) {
	if kid != "#atproto" {
		return nil, errors.New("credential: delegation key id must be #atproto")
	}
	return ResolveNamedDIDKey(ctx, resolver, did, kid)
}

// ResolveCredentialVerificationKey selects only the exact #atproto or
// #atproto_space key named by a credential. It never substitutes a fallback.
func ResolveCredentialVerificationKey(ctx context.Context, resolver identity.Resolver, did atmos.DID, kid string) (crypto.PublicKey, error) {
	if kid != "#atproto" && kid != "#atproto_space" {
		return nil, errors.New("credential: credential key id must be #atproto or #atproto_space")
	}
	return ResolveNamedDIDKey(ctx, resolver, did, kid)
}

// ResolveCredentialIssuanceKey selects #atproto_space when it exists and is
// valid, falling back to #atproto only when the dedicated entry is absent.
func ResolveCredentialIssuanceKey(ctx context.Context, resolver identity.Resolver, did atmos.DID) (crypto.PublicKey, string, error) {
	if resolver == nil {
		return nil, "", errors.New("credential: identity resolver is required")
	}
	if err := did.Validate(); err != nil {
		return nil, "", fmt.Errorf("credential: invalid DID: %w", err)
	}
	doc, err := resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, "", fmt.Errorf("credential: resolve DID %s: %w", did, err)
	}
	_, key, err := identity.SelectVerificationMethod(doc, did, "#atproto_space")
	if err == nil {
		return key, "#atproto_space", nil
	}
	if !errors.Is(err, identity.ErrSelectedEntryNotFound) {
		return nil, "", fmt.Errorf("credential: select dedicated issuance key: %w", err)
	}
	_, key, err = identity.SelectVerificationMethod(doc, did, "#atproto")
	if err != nil {
		return nil, "", fmt.Errorf("credential: select fallback issuance key: %w", err)
	}
	return key, "#atproto", nil
}
