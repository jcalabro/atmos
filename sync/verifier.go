package sync

import (
	"context"
	"fmt"
	stdsync "sync"

	"golang.org/x/time/rate"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
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

// ResyncReason names why a resync was triggered.
type ResyncReason int

const (
	// ReasonChainBreak indicates the verifier detected a prevData
	// mismatch between the incoming commit and locally-tracked state.
	ReasonChainBreak ResyncReason = iota

	// ReasonInversionFailure indicates the commit's CAR diff could not
	// be inverted to recover the prior MST root (malformed CAR, missing
	// blocks, structural breakage).
	ReasonInversionFailure

	// ReasonSyncEvent indicates an upstream #sync event triggered the
	// resync — not a verification failure on our end, just a PDS
	// telling us its repo state changed out of band.
	ReasonSyncEvent
)

// String returns a stable name for use in error messages and metrics labels.
func (r ResyncReason) String() string {
	switch r {
	case ReasonChainBreak:
		return "chain_break"
	case ReasonInversionFailure:
		return "inversion_failure"
	case ReasonSyncEvent:
		return "sync_event"
	default:
		return fmt.Sprintf("unknown_reason(%d)", r)
	}
}

// ChainBreakError is returned when a #commit's prevData doesn't match
// the locally-tracked data CID for the DID, or inversion produces a
// root that doesn't match the prior state.
type ChainBreakError struct {
	DID          atmos.DID
	SeenRev      string   // rev we last accepted for this DID, or "" if first sighting
	SeenData     cbor.CID // data CID we last accepted for this DID
	GotRev       string   // rev on the offending commit
	GotPrevData  cbor.CID // prevData claim on the offending commit
	InvertedData cbor.CID // CID we computed by inverting; zero if inversion itself failed
	Cause        error
}

func (e *ChainBreakError) Error() string {
	seen := "first-sighting"
	if e.SeenRev != "" || e.SeenData.Defined() {
		seen = fmt.Sprintf("rev=%s data=%s", e.SeenRev, e.SeenData)
	}
	inverted := "n/a"
	if e.InvertedData.Defined() {
		inverted = e.InvertedData.String()
	}
	return fmt.Sprintf("sync: chain break for %s: seen (%s), got (rev=%s prevData=%s inverted=%s)",
		e.DID, seen, e.GotRev, e.GotPrevData, inverted)
}

func (e *ChainBreakError) Unwrap() error { return e.Cause }

// InversionError is returned when MST inversion itself failed —
// malformed CAR, op references a CID not present in the diff, etc.
// Distinct from ChainBreakError: the commit is broken on its own
// terms rather than failing to continue our chain.
type InversionError struct {
	DID   atmos.DID
	Rev   string
	Cause error
}

func (e *InversionError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("sync: inversion failed for %s rev=%s", e.DID, e.Rev)
	}
	return fmt.Sprintf("sync: inversion failed for %s rev=%s: %v", e.DID, e.Rev, e.Cause)
}

func (e *InversionError) Unwrap() error { return e.Cause }

// SignatureError is returned when commit signature verification fails
// even after one identity-cache purge + re-resolution.
type SignatureError struct {
	DID    atmos.DID
	Rev    string
	KeyDID string // the resolved did:key, if any
	Cause  error
}

func (e *SignatureError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("sync: signature verification failed for %s rev=%s key=%s",
			e.DID, e.Rev, e.KeyDID)
	}
	return fmt.Sprintf("sync: signature verification failed for %s rev=%s key=%s: %v",
		e.DID, e.Rev, e.KeyDID, e.Cause)
}

func (e *SignatureError) Unwrap() error { return e.Cause }

// ResyncFailedError is returned when a chain break or inversion
// failure was detected and policy was PolicyResync, but the resync
// itself failed (PDS unreachable, returned an invalid CAR, etc.).
type ResyncFailedError struct {
	DID    atmos.DID
	Reason ResyncReason
	Cause  error
}

func (e *ResyncFailedError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("sync: resync failed for %s (reason=%s)", e.DID, e.Reason)
	}
	return fmt.Sprintf("sync: resync failed for %s (reason=%s): %v", e.DID, e.Reason, e.Cause)
}

func (e *ResyncFailedError) Unwrap() error { return e.Cause }

// ResyncRateLimitedError is returned when a DID has hit its resync
// rate limit. Per Sync 1.1's "abusive accounts get throttled by
// consumers" directive.
type ResyncRateLimitedError struct {
	DID atmos.DID
}

func (e *ResyncRateLimitedError) Error() string {
	return fmt.Sprintf("sync: resync rate limited for %s", e.DID)
}

// VerifierPolicy controls what happens when chain or inversion verification
// fails. The zero value is PolicyResync. Signature failures bypass policy
// and always surface as SignatureError, since a resync would not repair them.
type VerifierPolicy int

const (
	// PolicyResync (default): on chain break or inversion failure,
	// transparently fetch the repo via getRepo, diff against local
	// state, and yield diffed ops as ActionResync. Consumers see a
	// continuous valid stream. Signature failures still surface as
	// SignatureError because resync would not fix them.
	PolicyResync VerifierPolicy = iota

	// PolicyError: on chain break or inversion failure, yield a typed
	// error. Consumers may call Verifier.Resync(ctx, did) themselves
	// if they want repaired ops. State still advances through the
	// failure (matching the Bluesky relay's lenient behavior); a
	// consumer that wants to truly stop processing must drop the DID
	// itself.
	PolicyError
)

func (p VerifierPolicy) String() string {
	switch p {
	case PolicyResync:
		return "resync"
	case PolicyError:
		return "error"
	default:
		return fmt.Sprintf("unknown_policy(%d)", p)
	}
}

// VerifierStats is a snapshot of a Verifier's counter state at the moment
// Stats() was called. The struct itself is a plain value — atomic load
// semantics belong to Verifier.Stats(), which performs an atomic Load on
// each counter before returning a snapshot. Two snapshots taken
// concurrently may observe slightly different counter combinations, but
// each individual counter is always coherent.
type VerifierStats struct {
	EventsVerified    uint64
	ChainBreaks       uint64
	InversionFailures uint64
	SignatureFailures uint64
	Resyncs           uint64
	ResyncFailures    uint64
	RevReplaysDropped uint64
}

// VerifierOptions configures a Verifier. Most fields are required
// for PolicyResync (the default); ChainStore and Directory are
// always required.
type VerifierOptions struct {
	// SyncClient is used to fetch repos via getRepo during resync.
	// Required when Policy is PolicyResync. Allowed but unused under
	// PolicyError unless the consumer calls Verifier.Resync directly.
	SyncClient *Client

	// Directory is used to resolve DIDs to signing keys for
	// signature verification. Required.
	Directory *identity.Directory

	// ChainStore persists per-DID state. Required. Use
	// NewMemChainStore() for tests; bring your own for production.
	ChainStore ChainStore

	// Policy selects PolicyResync (default) or PolicyError.
	Policy VerifierPolicy

	// ResyncLimit is the per-DID resync rate (token bucket). Zero
	// means rate.Limit(5.0/60.0) — five resyncs per minute. Set to
	// rate.Inf for tests that don't care about throttling.
	ResyncLimit rate.Limit

	// ResyncBurst is the burst size for the per-DID rate limiter.
	// Zero means 5.
	ResyncBurst int

	// OnResync, if non-nil, fires once per successful resync, after chain
	// state has been advanced and just before the resynced ops are returned
	// to the caller. Invoked synchronously on the verifier's goroutine —
	// keep the callback fast or hand off to a worker. oldRev is "" when
	// this is the first resync we've ever performed for did (Load returned
	// nil) or the resync was triggered by a #sync event from an upstream we
	// hadn't seen before.
	OnResync func(did atmos.DID, oldRev, newRev string, reason ResyncReason)

	// OnVerificationFailure, if non-nil, fires once per verification failure
	// regardless of policy AND regardless of whether a subsequent resync
	// repaired the chain. ChainBreakError, InversionError, and
	// SignatureError each invoke this hook before the verifier decides
	// whether to attempt resync; ResyncFailedError and
	// ResyncRateLimitedError do NOT — they are downstream consequences of a
	// failure we already reported. Invoked synchronously on the verifier's
	// goroutine.
	OnVerificationFailure func(did atmos.DID, err error)
}
