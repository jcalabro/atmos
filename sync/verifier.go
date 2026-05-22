package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/lru"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/gt"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/time/rate"
)

// Default per-DID resync rate limit applied when the corresponding
// VerifierOptions fields are unset: 5 resyncs per minute, burst 5.
const (
	DefaultResyncLimit = rate.Limit(5.0 / 60.0)
	DefaultResyncBurst = 5
)

// DefaultFutureRevTolerance is the maximum a rev's TID timestamp may
// lead wall clock before the event is rejected as future-dated.
// Matches indigo's relay (cmd/relay/relay/verify.go futureRevTolerance).
const DefaultFutureRevTolerance = 5 * time.Minute

// DefaultMutexCapacity bounds the per-DID serialization-mutex cache.
// Pinned entries (i.e. mutexes currently held) are never evicted, so
// the cache may transiently exceed this watermark under burst.
//
// Sized to comfortably accommodate the concurrency level of a busy
// firehose consumer: a few thousand simultaneously-active DIDs
// without churn, with eviction kicking in for the long tail. Tunable
// via VerifierOptions.MutexCapacity.
const DefaultMutexCapacity = 4096

// DefaultLimiterCapacity bounds the per-DID resync-rate-limiter
// cache. Larger than DefaultMutexCapacity because limiter lifetime
// extends beyond a single event: a DID's token-bucket state is
// remembered as long as the entry stays in cache, so a more generous
// cap reduces the chance that a DID returning to activity loses its
// (mostly-empty) bucket. Tunable via VerifierOptions.LimiterCapacity.
const DefaultLimiterCapacity = 16384

// Spec-mandated resource bounds on #commit events. Match indigo's
// MaxMessageBlocksBytes and MaxCommitOps in cmd/relay/relay/verify.go.
const (
	// MaxCommitBlocksBytes caps the byte size of the CAR diff in a
	// #commit event's `blocks` field. The streaming layer also caps
	// WebSocket frames; this is defense-in-depth.
	MaxCommitBlocksBytes = 2_000_000

	// MaxCommitOps caps the number of repo ops in a single #commit.
	MaxCommitOps = 200
)

// ChainState is per-DID chain-continuity state: the rev and MST root
// CID of the last commit accepted.
type ChainState struct {
	Rev  string
	Data cbor.CID
}

// HostingState is per-DID hosting status, tracked from #account
// events. Active is the visibility decision; Status is the optional
// reason when !Active. Status is intentionally a free string: the
// spec defines an open vocabulary, and unknown values must be
// tolerated. See the Status* constants for well-known values.
type HostingState struct {
	Active bool
	Status string // optional reason when !Active; empty when Active
	Seq    int64  // seq of the source #account event (replay protection)
	// Time is the verbatim event timestamp from the firehose
	// envelope. Stored unchanged for diagnostics; the verifier does
	// NOT validate that it parses as an ISO-8601 datetime. Consumers
	// that want a parsed time should call atmos.ParseDatetime
	// themselves and tolerate malformed inputs.
	Time string
}

// IsActive reports whether s permits commit/sync events. A nil
// receiver is treated as active (first sighting).
func (s *HostingState) IsActive() bool {
	if s == nil {
		return true
	}
	return s.Active
}

// Well-known #account status values. The spec's vocabulary is open;
// callers may see other values and should preserve them verbatim.
const (
	StatusTakendown      = "takendown"      // host/service-initiated removal (terms violation)
	StatusSuspended      = "suspended"      // time-limited variant of takendown
	StatusDeactivated    = "deactivated"    // user-initiated removal; also the migration starting state
	StatusDeleted        = "deleted"        // user/host-initiated; conventionally permanent
	StatusDesynchronized = "desynchronized" // sync 1.1: lost sync, repair pending; can co-exist with Active=true
	StatusThrottled      = "throttled"      // sync 1.1: rate-limited; can co-exist with Active=true
)

// StateStore persists per-DID state across firehose events. The
// verifier reads and writes through this interface.
//
// Load methods return (nil, nil) when no state is recorded; the
// verifier treats that as a first sighting and assumes safe defaults
// (active hosting, no chain link).
//
// Production deployments need durable storage (pebble, sqlite, etc.).
// The in-memory MemStateStore loses state on restart, after which the
// next event for each DID is accepted as ground truth.
//
// Implementations may store chain and hosting state under separate
// keys, separate columns, or any other layout — the interface is
// field-targeted so callers never read-modify-write on the hot path.
// Delete removes both fields atomically.
type StateStore interface {
	LoadChain(ctx context.Context, did atmos.DID) (*ChainState, error)
	SaveChain(ctx context.Context, did atmos.DID, state ChainState) error

	LoadHosting(ctx context.Context, did atmos.DID) (*HostingState, error)
	SaveHosting(ctx context.Context, did atmos.DID, state HostingState) error

	Delete(ctx context.Context, did atmos.DID) error
}

// MemStateStore is an in-memory StateStore for tests and development.
// State is lost on restart. Construct via [NewMemStateStore]; the zero
// value is unsafe.
type MemStateStore struct {
	chain   *xsync.Map[atmos.DID, ChainState]
	hosting *xsync.Map[atmos.DID, HostingState]
}

// NewMemStateStore returns an empty MemStateStore.
func NewMemStateStore() *MemStateStore {
	return &MemStateStore{
		chain:   xsync.NewMap[atmos.DID, ChainState](),
		hosting: xsync.NewMap[atmos.DID, HostingState](),
	}
}

// LoadChain returns the chain state for did, or (nil, nil) if absent.
func (s *MemStateStore) LoadChain(_ context.Context, did atmos.DID) (*ChainState, error) {
	state, ok := s.chain.Load(did)
	if !ok {
		return nil, nil
	}
	return &state, nil
}

// SaveChain records the chain state for did.
func (s *MemStateStore) SaveChain(_ context.Context, did atmos.DID, state ChainState) error {
	s.chain.Store(did, state)
	return nil
}

// LoadHosting returns the hosting state for did, or (nil, nil) if absent.
func (s *MemStateStore) LoadHosting(_ context.Context, did atmos.DID) (*HostingState, error) {
	state, ok := s.hosting.Load(did)
	if !ok {
		return nil, nil
	}
	return &state, nil
}

// SaveHosting records the hosting state for did.
func (s *MemStateStore) SaveHosting(_ context.Context, did atmos.DID, state HostingState) error {
	s.hosting.Store(did, state)
	return nil
}

// Delete removes both chain and hosting state for did; no-op for
// fields that weren't set.
func (s *MemStateStore) Delete(_ context.Context, did atmos.DID) error {
	s.chain.Delete(did)
	s.hosting.Delete(did)
	return nil
}

// ResyncReason names why a resync was triggered. Surfaced via
// OnResync and embedded in ResyncFailedError.
type ResyncReason int

const (
	// ReasonChainBreak: incoming commit's prevData (or inverted root)
	// did not match locally-tracked state.
	ReasonChainBreak ResyncReason = iota

	// ReasonInversionFailure: the commit's CAR diff could not be
	// inverted (malformed CAR, missing blocks, structural breakage).
	ReasonInversionFailure

	// ReasonSyncEvent: an upstream #sync event indicated a state
	// change. Also used for resyncs initiated via Verifier.Resync,
	// since the operator's intent is identical (re-fetch authoritative
	// state). To distinguish the two on metrics, use the seq on the
	// triggering event, or call resync from a wrapper that records
	// its own counter.
	ReasonSyncEvent

	// ReasonLegacyCommit: an upstream emitted a Sync-1.0-shape commit
	// (no prevData, no op.Prev on update/delete) on the 1.1 firehose.
	// Only fires under LegacyReject; under LegacyAccept the commit
	// passes through without a resync.
	ReasonLegacyCommit

	// ReasonAccountInactive: the verifier was asked to resync a DID
	// whose persisted hosting status is not active. Only fires under
	// HostingGate; surfaces as the cause of a ResyncFailedError so
	// callers can distinguish "couldn't resync" from "won't resync".
	ReasonAccountInactive
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
	case ReasonLegacyCommit:
		return "legacy_commit"
	case ReasonAccountInactive:
		return "account_inactive"
	default:
		return fmt.Sprintf("unknown_reason(%d)", r)
	}
}

// ChainBreakError is returned when an incoming #commit fails the
// chain-continuity check. Both the commit's declared prevData and
// the verifier's inversion of the new commit MUST equal SeenData;
// either disagreeing yields this error.
type ChainBreakError struct {
	DID          atmos.DID
	SeenRev      string   // rev last accepted for DID, or "" on first sighting
	SeenData     cbor.CID // data CID last accepted for DID
	GotRev       string   // rev on the offending commit
	GotPrevData  cbor.CID // prevData declared by the offending commit
	InvertedData cbor.CID // root recomputed by inverting; zero if inversion failed
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

// InversionIncompleteError signals a specific lenient-mode pass-through:
// the upstream commit's prevData claim matches our stored chain state
// (so the upstream IS honestly continuing the chain), but our local
// MST inversion produced a different root because the CAR diff did not
// include enough blocks for a complete inversion. The spec acknowledges
// this case ("blocks adjacent to changed keys must be included; in
// rare cases this is violated") and Bluesky's production relay logs +
// passes such commits through under its LenientSyncValidation config.
//
// Surfaced via OnVerificationFailure for visibility, but the verifier
// advances state as if the commit verified normally; ops flow through
// to the consumer. Disable by setting VerifierOptions.LenientInversion
// to gt.Some(false), in which case the same condition surfaces as a
// ChainBreakError that triggers resync under PolicyResync.
type InversionIncompleteError struct {
	DID          atmos.DID
	SeenRev      string
	SeenData     cbor.CID
	GotRev       string
	GotPrevData  cbor.CID
	InvertedData cbor.CID
}

func (e *InversionIncompleteError) Error() string {
	return fmt.Sprintf("sync: inversion incomplete for %s: seen (rev=%s data=%s), got (rev=%s prevData=%s inverted=%s) — accepted under lenient mode",
		e.DID, e.SeenRev, e.SeenData, e.GotRev, e.GotPrevData, e.InvertedData)
}

// InversionError is returned when MST inversion fails on its own
// terms: malformed CAR, op references a CID absent from the diff,
// structurally broken partial MST. Distinct from ChainBreakError,
// which fires when a structurally valid commit fails to continue
// the existing chain.
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

// SignatureError is returned when commit signature verification
// fails even after one identity-cache purge and re-resolution.
// Bypasses VerifierPolicy: a resync would not repair a bad signature.
type SignatureError struct {
	DID    atmos.DID
	Rev    string
	KeyDID string // resolved did:key on the failure path, if any
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

// ResyncFailedError is returned when the verifier attempted a resync
// (under PolicyResync, or via Verifier.Resync) and the fetch or
// validation of the served repo failed.
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

// LegacyCommitError signals a Sync-1.0-shape commit on the 1.1
// firehose: no envelope prevData, no op.Prev on update/delete ops.
// The producer is typically a PDS that hasn't deployed 1.1 yet.
//
// Only fires when LegacyCommitPolicy is LegacyReject. Under
// LegacyAccept (the default) such commits pass through with the
// chain-link check skipped; signature and op-CID checks still apply.
//
// Not fired on first sighting (no prior state to chain against).
type LegacyCommitError struct {
	DID      atmos.DID
	Rev      string
	SeenRev  string   // last accepted rev for DID
	SeenData cbor.CID // last accepted data CID for DID
}

func (e *LegacyCommitError) Error() string {
	return fmt.Sprintf("sync: legacy 1.0-shape commit for %s rev=%s (seen rev=%s data=%s)",
		e.DID, e.Rev, e.SeenRev, e.SeenData)
}

// CommitTooLargeError is returned when a #commit exceeds a
// spec-mandated resource bound (MaxCommitBlocksBytes, MaxCommitOps).
// Checked before any CAR parse so oversized payloads can't force
// expensive processing. Bypasses VerifierPolicy.
//
// Field is "blocks" (Got is byte length) or "ops" (Got is op count).
type CommitTooLargeError struct {
	DID   atmos.DID
	Rev   string
	Field string // "blocks" | "ops"
	Got   int
	Limit int
}

func (e *CommitTooLargeError) Error() string {
	return fmt.Sprintf("sync: commit too large for %s rev=%s: %s=%d exceeds limit=%d",
		e.DID, e.Rev, e.Field, e.Got, e.Limit)
}

// DuplicatePathError is returned when a #commit's ops list contains
// multiple ops on the same path. A well-formed commit folds
// intra-commit duplicates into a single op (e.g. delete-then-create
// collapses to an update); multiple ops on one path indicates a
// producer bug or an attempt to race consumer state machines. Indigo
// rejects this in atproto/repo/operation.go's NormalizeOps.
//
// Routed via the inversion-failure path: PolicyResync recovers
// transparently; PolicyError surfaces this typed error.
type DuplicatePathError struct {
	DID  atmos.DID
	Rev  string
	Path string // the path that appeared more than once
}

func (e *DuplicatePathError) Error() string {
	return fmt.Sprintf("sync: duplicate op path in commit for %s rev=%s path=%q",
		e.DID, e.Rev, e.Path)
}

// OpCIDMismatchError is returned when a commit's ops list disagrees
// with the post-state MST: a create/update op's CID must equal the
// tree value at the op's path; a delete op's path must be absent.
// Mirrors indigo's invariants in atproto/repo/sync.go's
// VerifyCommitMessage.
//
// Routed via the inversion-failure path: PolicyResync recovers
// transparently; PolicyError surfaces this typed error and advances
// state to the offending commit's data CID.
//
// Reason values:
//   - "create_cid_mismatch":  op.CID != tree.Get(path) on a create
//   - "update_cid_mismatch":  op.CID != tree.Get(path) on an update
//   - "delete_path_present":  delete op's path is still in the tree
//   - "create_missing_cid":   create op has no CID
//   - "update_missing_cid":   update op has no CID
//   - "delete_unexpected_cid": delete op carries a CID it shouldn't
type OpCIDMismatchError struct {
	DID    atmos.DID
	Rev    string
	Path   string
	Reason string
	OpCID  cbor.CID // CID claimed by the op (zero if absent)
	MSTCID cbor.CID // CID present in the post-state MST (zero if absent)
}

func (e *OpCIDMismatchError) Error() string {
	return fmt.Sprintf("sync: op cid mismatch for %s rev=%s path=%q reason=%s op=%s mst=%s",
		e.DID, e.Rev, e.Path, e.Reason, formatCIDOrMissing(e.OpCID), formatCIDOrMissing(e.MSTCID))
}

func formatCIDOrMissing(c cbor.CID) string {
	if !c.Defined() {
		return "<missing>"
	}
	return c.String()
}

// FieldMismatchError is returned when a firehose #commit envelope
// field disagrees with the corresponding field in the signed inner
// commit (or, for Field=="commit", the CAR's root). Per the Sync
// 1.1 validation checklist, outer fields MUST match the signed
// commit; per the spec, the CAR's first root MUST be the commit
// CID. Mismatches indicate a misbehaving PDS (or, more concerningly,
// a misattribution attempt: a valid commit for one DID wrapped in
// an envelope claiming another).
//
// Bypasses VerifierPolicy. Chain state is not advanced.
//
// Field values:
//   - "did":     envelope.Repo != inner.DID
//   - "rev":     envelope.Rev != inner.Rev
//   - "version": inner.Version != 3 (Sync 1.1 mandates v3; envelope
//     has no version field, so we render Envelope="3")
//   - "commit":  envelope.Commit (the link to the inner commit)
//     disagrees with the CAR's first root. "Inner" is the CAR root.
type FieldMismatchError struct {
	DID      atmos.DID
	Field    string // "did" | "rev" | "version" | "commit"
	Envelope string // value on the firehose envelope
	Inner    string // value decoded from the signed commit block (or CAR root for Field=="commit")
}

func (e *FieldMismatchError) Error() string {
	return fmt.Sprintf("sync: %s field mismatch for %s: envelope=%q inner=%q",
		e.Field, e.DID, e.Envelope, e.Inner)
}

// FutureRevError is returned when an event's rev TID timestamp leads
// the verifier's wall clock by more than FutureRevTolerance. Per
// spec, future-timestamped revs MUST be ignored; otherwise a malicious
// or clock-broken upstream could persist a far-future rev and starve
// out every legitimate follow-on event.
//
// Bypasses VerifierPolicy. Chain state is not advanced. Unparseable
// revs are not flagged as future-rev; the gate yields and downstream
// gates handle them.
type FutureRevError struct {
	DID       atmos.DID
	Rev       string
	RevTime   time.Time // timestamp decoded from the TID
	Now       time.Time // wall clock at rejection
	Tolerance time.Duration
}

func (e *FutureRevError) Error() string {
	return fmt.Sprintf("sync: future rev for %s: rev=%s revTime=%s now=%s tolerance=%s",
		e.DID, e.Rev, e.RevTime.Format(time.RFC3339Nano),
		e.Now.Format(time.RFC3339Nano), e.Tolerance)
}

// ResyncRateLimitedError is returned when a DID has exceeded its
// per-DID resync rate limit (see VerifierOptions.ResyncLimit/Burst).
type ResyncRateLimitedError struct {
	DID atmos.DID
}

func (e *ResyncRateLimitedError) Error() string {
	return fmt.Sprintf("sync: resync rate limited for %s", e.DID)
}

// RevRegressionError is returned (wrapped in [ResyncFailedError]) when
// a resync's served commit has a rev that regresses or contradicts
// the locally-tracked state for the DID:
//
//   - GotRev < SeenRev: a strict regression. Accepting would mean
//     subsequent legitimate #commit events get rejected as rev replays
//     or chain breaks against state we just rolled backward.
//   - GotRev == SeenRev with GotData != SeenData: the upstream is
//     serving a different commit at the same rev. Either the upstream
//     is corrupt or our persisted SeenData is. Either way, rolling
//     forward without distinguishing them risks state corruption.
//
// First sighting (no SeenRev) is permitted: there's no prior state
// to regress against. Equal rev with equal data is also permitted —
// the resync is idempotent.
type RevRegressionError struct {
	DID      atmos.DID
	SeenRev  string
	SeenData cbor.CID
	GotRev   string
	GotData  cbor.CID
}

func (e *RevRegressionError) Error() string {
	return fmt.Sprintf("sync: resync rev regression for %s: seen (rev=%s data=%s), got (rev=%s data=%s)",
		e.DID, e.SeenRev, e.SeenData, e.GotRev, e.GotData)
}

// AccountInactiveError is returned under HostingGate when a #commit
// or #sync arrives for a DID whose persisted HostingState marks the
// account as not active (takendown, suspended, deactivated, deleted,
// or any other non-active status).
//
// Bypasses VerifierPolicy: a resync against the same upstream can't
// undo a takedown. Status is the persisted reason string (open
// vocabulary; see Status* constants for well-known values).
type AccountInactiveError struct {
	DID    atmos.DID
	Status string
}

func (e *AccountInactiveError) Error() string {
	if e.Status == "" {
		return fmt.Sprintf("sync: account inactive for %s", e.DID)
	}
	return fmt.Sprintf("sync: account inactive for %s (status=%s)", e.DID, e.Status)
}

// LegacyCommitPolicy controls how the verifier handles Sync-1.0-shape
// commits arriving on a 1.1 firehose (no envelope prevData, no op.Prev
// on update/delete ops). The chain-continuity check is impossible on
// such commits, so the choice is whether to accept what can still be
// validated or reject the event entirely.
type LegacyCommitPolicy int

const (
	// LegacyAccept (default) lets a legacy commit through after
	// signature and op-CID checks pass; chain state advances to the
	// new commit's data CID and ops flow to the consumer normally.
	// VerifierStats.LegacyCommits still increments so operators can
	// see non-upgraded upstreams.
	//
	// Tradeoff: a misbehaving upstream that intentionally regresses
	// to 1.0 shape can replay an old signed commit without the
	// chain-continuity gate catching it. Signature and op-CID checks
	// still rule out cross-DID rebroadcast and fabricated records.
	// Use LegacyReject if that residual exposure is unacceptable.
	LegacyAccept LegacyCommitPolicy = iota

	// LegacyReject routes legacy commits through the failure path.
	// VerifierPolicy then decides: PolicyResync triggers a transparent
	// resync; PolicyError surfaces *LegacyCommitError.
	LegacyReject
)

// String returns a stable name suitable for metric labels.
func (p LegacyCommitPolicy) String() string {
	switch p {
	case LegacyAccept:
		return "accept"
	case LegacyReject:
		return "reject"
	default:
		return fmt.Sprintf("unknown_legacy_policy(%d)", p)
	}
}

// HostingPolicy controls whether the verifier gates #commit and
// #sync events on the persisted HostingState for the DID.
type HostingPolicy int

const (
	// HostingTrack (default) persists hosting state via OnAccountEvent
	// but does not gate downstream events. Consumers can read state
	// from StateStore.LoadHosting and apply their own filtering.
	// Suitable for moderation tools, archivers, and any pipeline that
	// wants to observe takendown accounts.
	HostingTrack HostingPolicy = iota

	// HostingGate persists hosting state AND drops #commit/#sync for
	// non-active DIDs (returning *AccountInactiveError). Resyncs for
	// non-active DIDs return ResyncFailedError with
	// ReasonAccountInactive instead of hitting getRepo. Suitable for
	// content-distribution pipelines that must honor takedowns.
	HostingGate
)

// String returns a stable name suitable for metric labels.
func (p HostingPolicy) String() string {
	switch p {
	case HostingTrack:
		return "track"
	case HostingGate:
		return "gate"
	default:
		return fmt.Sprintf("unknown_hosting_policy(%d)", p)
	}
}

// VerifierPolicy selects how chain breaks and inversion failures are
// handled. SignatureError and FutureRevError bypass policy: no
// resync against the same upstream can repair them.
type VerifierPolicy int

const (
	// PolicyResync (default): on chain break or inversion failure,
	// fetch the repo via getRepo and yield diffed ops as ActionResync.
	// Consumers see a continuous stream.
	PolicyResync VerifierPolicy = iota

	// PolicyError: on chain break or inversion failure, return a
	// typed error. State still advances through the failure
	// (matching indigo's relay), so subsequent re-deliveries don't
	// re-report. Consumers may call Verifier.Resync to repair.
	PolicyError
)

// String returns a stable name suitable for metric labels.
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

// VerifierStats is a snapshot of a Verifier's counters. Each field
// is loaded atomically inside Verifier.Stats; concurrent Stats calls
// may observe different cross-counter combinations, but each
// individual counter is always coherent.
type VerifierStats struct {
	// EventsVerified counts #commit events accepted on the happy path.
	EventsVerified uint64

	// ChainBreaks counts events whose prevData or inverted root
	// disagreed with persisted state. PolicyResync recovers via
	// resync; PolicyError surfaces ChainBreakError.
	ChainBreaks uint64

	// InversionFailures counts events whose CAR diff could not be
	// inverted (malformed CAR, missing blocks, structural breakage).
	// PolicyResync recovers via resync.
	InversionFailures uint64

	// InversionIncomplete counts events accepted under
	// VerifierOptions.LenientInversion: the upstream's prevData
	// matched our state, but our local inversion produced a different
	// root (the CAR was missing blocks needed for full inversion).
	// State advances and ops flow through; *InversionIncompleteError
	// fires via OnVerificationFailure for visibility. Strict mode
	// accounts these as ChainBreaks instead.
	InversionIncomplete uint64

	// SignatureFailures counts typed *SignatureError outcomes only
	// (after the purge+retry path). Resolver/network errors during
	// signature verification do not increment this counter; they
	// surface as wrapped infrastructure errors so "couldn't check" is
	// distinguishable from "checked and it's bad".
	SignatureFailures uint64

	// Resyncs counts successful getRepo-driven resyncs.
	Resyncs uint64

	// ResyncFailures counts resync attempts that failed (network,
	// signature on the fetched commit, etc.).
	ResyncFailures uint64

	// RevReplaysDropped counts events whose rev was at or below the
	// persisted rev (re-deliveries or out-of-order arrivals).
	RevReplaysDropped uint64

	// ChainStateSaveFailures counts StateStore.SaveChain failures
	// during PolicyError state-advance. The primary verification
	// error still surfaces; this counter signals that future events
	// for the DID may re-report the same break until state catches up.
	ChainStateSaveFailures uint64

	// FutureRevsRejected counts events whose rev TID timestamp led
	// wall clock by more than FutureRevTolerance. Bypasses policy.
	FutureRevsRejected uint64

	// FieldMismatches counts events whose envelope fields
	// (did/rev/version) disagreed with the signed inner commit.
	// Bypasses policy.
	FieldMismatches uint64

	// OpCIDMismatches counts events whose ops list disagreed with the
	// post-state MST. Routed via the inversion-failure path.
	OpCIDMismatches uint64

	// LegacyCommits counts Sync-1.0-shape events. Increments under
	// both LegacyAccept (event passes through) and LegacyReject
	// (event routed to the failure path).
	LegacyCommits uint64

	// MissingRecordBlocksOps counts individual create/update ops
	// whose record block was absent from the CAR (one increment per
	// affected op, not per event). The op still flows to the consumer
	// with empty BlockData; this counter signals upstreams shipping
	// incomplete CARs.
	MissingRecordBlocksOps uint64

	// DuplicatePaths counts events whose ops list contained two or
	// more ops on the same path. Routed via the inversion-failure
	// path.
	DuplicatePaths uint64

	// OversizedCommits counts events rejected for exceeding
	// MaxCommitBlocksBytes or MaxCommitOps. Bypasses policy.
	OversizedCommits uint64

	// SyncNoOps counts #sync events whose embedded data CID matched
	// persisted state; the verifier advanced rev and skipped getRepo.
	SyncNoOps uint64

	// AccountsInactive counts events dropped because the persisted
	// HostingState for the DID is not active. Only fires under
	// HostingGate; under HostingTrack the verifier passes events
	// through regardless of status.
	AccountsInactive uint64

	// AccountEventReplaysDropped counts #account events with a seq
	// at or below the persisted seq for that DID (re-deliveries).
	AccountEventReplaysDropped uint64
}

// VerifierOp is one operation produced by the Verifier. Mirrors
// streaming.Operation; the streaming layer converts these when
// populating Event.verifiedOps.
//
// CID is the record content hash for create/update/resync ops; the
// zero value (use [cbor.CID.Defined] to check) on delete ops.
type VerifierOp struct {
	Action     atmos.Action
	Collection atmos.NSID
	RKey       atmos.RecordKey
	Repo       atmos.DID
	Rev        atmos.TID
	CID        cbor.CID
	BlockData  []byte
}

// VerifierOptions configures a Verifier. Directory and StateStore
// are required. SyncClient is required under PolicyResync (the
// default). Optional fields are wrapped in gt.Option so an unset
// field is distinguishable from a zero-valued field.
type VerifierOptions struct {
	// Directory resolves DIDs to signing keys. Required.
	Directory *identity.Directory

	// StateStore persists per-DID chain and hosting state. Required.
	// Use NewMemStateStore for tests; provide a durable
	// implementation for production.
	StateStore StateStore

	// SyncClient fetches repos via getRepo during resync. Required
	// under PolicyResync (the default policy). Unused under
	// PolicyError unless the caller invokes Verifier.Resync directly.
	SyncClient gt.Option[*Client]

	// Policy selects the failure-handling mode. None → PolicyResync.
	Policy gt.Option[VerifierPolicy]

	// ResyncLimit is the per-DID token-bucket refill rate. None or
	// zero → DefaultResyncLimit. Use rate.Inf in tests.
	ResyncLimit gt.Option[rate.Limit]

	// ResyncBurst is the per-DID token-bucket capacity. None or zero
	// → DefaultResyncBurst. (A zero burst would be permanent throttling
	// and is almost always a misconfiguration; the default is applied
	// in that case.)
	ResyncBurst gt.Option[int]

	// OnResync fires once per successful resync, after state has
	// advanced and before ops return to the caller. oldRev is "" on
	// the first resync ever performed for did. Invoked synchronously
	// while the per-DID mutex is held; keep it fast or hand off to a
	// worker.
	OnResync gt.Option[func(did atmos.DID, oldRev, newRev string, reason ResyncReason)]

	// OnVerificationFailure fires once per verification failure,
	// regardless of policy and regardless of whether a subsequent
	// resync repaired the chain. ChainBreakError, InversionError,
	// SignatureError, FieldMismatchError, FutureRevError,
	// CommitTooLargeError, OpCIDMismatchError, DuplicatePathError, and
	// LegacyCommitError (under LegacyReject) all invoke this hook;
	// ResyncFailedError and ResyncRateLimitedError do not (they're
	// downstream consequences of a failure already reported).
	//
	// Infrastructure errors do NOT invoke this hook — they surface
	// only via the return value. The set of "infrastructure" failures:
	//   - Resolver / network errors during signature verification.
	//   - StateStore.LoadHosting / LoadChain failures (including the
	//     hosting-gate read in checkHostingGate).
	//   - StateStore.SaveChain failures on the success path.
	// The rationale is the same in each case: "couldn't check" is
	// distinct from "checked and it's bad", and only the latter is
	// what the hook is for. Operators who want to react to
	// infrastructure failures should consult the verifier's return
	// value or a separate logging path.
	//
	// Invoked synchronously on the verifier's goroutine, AFTER the
	// per-DID mutex has been released. Callers may invoke other
	// Verifier methods (e.g. [Verifier.Resync]) from inside the hook
	// without deadlocking.
	OnVerificationFailure gt.Option[func(did atmos.DID, err error)]

	// FutureRevTolerance is the maximum a rev TID timestamp may lead
	// wall clock before the event is rejected as future-dated. None →
	// DefaultFutureRevTolerance (5 minutes). A negative value disables
	// the check; not recommended.
	FutureRevTolerance gt.Option[time.Duration]

	// Now overrides the wall clock used by FutureRevTolerance. None
	// → time.Now. For deterministic tests.
	Now gt.Option[func() time.Time]

	// LegacyCommitPolicy selects how Sync-1.0-shape commits are
	// handled. None → LegacyAccept (lenient default).
	LegacyCommitPolicy gt.Option[LegacyCommitPolicy]

	// LenientInversion controls handling of commits where the
	// upstream's prevData matches our stored chain state but our
	// local MST inversion produces a different root. This happens
	// when an upstream PDS ships a CAR that's missing blocks needed
	// for full inversion — the spec acknowledges this is rare but
	// permitted, and Bluesky's production relay forwards such commits
	// under its LenientSyncValidation config.
	//
	// None or true (default): accept the commit under
	// *InversionIncompleteError surfaced via OnVerificationFailure;
	// state advances normally and ops flow through to the consumer.
	// Matches the production relay's behavior and avoids triggering
	// expensive resyncs against accounts whose PDS sometimes ships
	// inversion-incomplete CARs.
	//
	// Set to gt.Some(false) for strict verification: the same
	// condition becomes a ChainBreakError that triggers resync under
	// PolicyResync (or surfaces directly under PolicyError).
	LenientInversion gt.Option[bool]

	// HostingPolicy selects whether the verifier gates #commit/#sync
	// events on persisted hosting status. None → HostingTrack (track
	// state, don't gate). Set to HostingGate for content-distribution
	// pipelines that must drop events for takendown accounts.
	HostingPolicy gt.Option[HostingPolicy]

	// OnAccountStateChanged fires once per #account event the
	// consumer passes to [Verifier.OnAccountEvent], after the verifier
	// has derived and persisted the new HostingState. Re-deliveries
	// (events with seq <= persisted seq) do not fire the callback.
	// Invoked synchronously while the per-DID mutex is held; keep
	// the callback fast or hand off to a worker.
	//
	// Renamed from OnAccountEvent to disambiguate from the method
	// of the same name on Verifier.
	OnAccountStateChanged gt.Option[func(did atmos.DID, state HostingState)]

	// MutexCapacity bounds the per-DID serialization-mutex cache.
	// None or zero → DefaultMutexCapacity. Currently-held mutexes
	// veto eviction, so the cache may transiently exceed this value
	// under burst.
	MutexCapacity gt.Option[int]

	// LimiterCapacity bounds the per-DID resync-rate-limiter cache.
	// None or zero → DefaultLimiterCapacity. Eviction loses an
	// inactive DID's token-bucket state — its next resync gets a
	// fresh full bucket, equivalent to a long-quiet account.
	LimiterCapacity gt.Option[int]

	// AsyncResyncWorkers is the goroutine pool size processing
	// chain-break-driven resyncs. None or zero → DefaultAsyncResyncWorkers
	// (32). Sized so independent slow DIDs proceed in parallel; one slow
	// DID still serializes inside its own per-DID mutex.
	AsyncResyncWorkers gt.Option[int]

	// PendingCap is the per-DID ring-buffer capacity for commits that
	// arrive while a resync is in flight for that DID. None or zero →
	// DefaultPendingCap (2048). Overflow drops the oldest entry and
	// surfaces *BufferOverflowError on AsyncErrors().
	PendingCap gt.Option[int]

	// ResyncEventBuffer is the capacity of the channel returned by
	// ResyncEvents(). None or zero → DefaultResyncEventBuffer (2048).
	// Workers block on full; the streaming readLoop drains it
	// alongside firehose reads.
	ResyncEventBuffer gt.Option[int]

	// AsyncErrorBuffer is the capacity of the channel returned by
	// AsyncErrors(). None or zero → DefaultAsyncErrorBuffer (256).
	// Smaller than ResyncEventBuffer because errors are rare; workers
	// blocking on full delivery is the correct back-pressure (a
	// consumer that has stalled long enough for 256 errors to queue
	// up should not have those errors silently dropped).
	AsyncErrorBuffer gt.Option[int]
}

// Verifier performs Sync 1.1 verification of firehose events. Safe
// for concurrent use across DIDs; per-DID work is serialized
// internally to prevent racing chain-state advances.
//
// Construct with NewVerifier; Verifier values must not be copied.
//
// Async resync. Chain-break and inversion-failure events under the
// default PolicyResync are NOT resolved synchronously inside
// VerifyAndExpand. Instead, VerifyAndExpand returns (nil, nil), the
// affected DID is marked as resyncing, and a worker pool processes
// the resync (HTTP getRepo + MST walk) in the background.
//
// Resync results arrive via two channels exposed for consumption:
//
//   - ResyncEvents() yields ResyncEvent records — one per completed
//     resync, with the full set of [ActionResync] ops.
//   - AsyncErrors() yields errors that occur off the readLoop:
//     ResyncFailedError, ResyncRateLimitedError,
//     BufferOverflowError, etc.
//
// Commits arriving for a DID while its resync is in flight are
// buffered (capacity VerifierOptions.PendingCap, default 2048) and
// replayed against post-resync state. Buffer overflow drops the
// oldest commit and surfaces *BufferOverflowError; the verifier does
// NOT auto-trigger a follow-up resync (consumers should log/alert).
//
// The streaming layer (package streaming) drains both channels
// transparently; consumers using streaming.Client.Events() see
// resync ops flow through Event.Operations() as ActionResync and
// async errors flow through the iterator's error slot, so the async
// machinery is invisible at the streaming-consumer API.
//
// Call Close() to shut down the worker pool. Close is idempotent;
// calling it multiple times is safe. Outstanding workers' contexts
// are cancelled (so a stuck getRepo unblocks on a subsequent ctx
// check), and ResyncEvents / AsyncErrors are closed once all
// workers exit.
type Verifier struct {
	_ noCopy

	opts VerifierOptions

	// per-DID serialization. Two events for the same DID never run
	// through verification concurrently. Bounded by MutexCapacity;
	// pinning vetoes eviction of currently-held mutexes (see
	// internal/lru).
	didMu *lru.Cache[atmos.DID, *sync.Mutex]

	// per-DID resync rate limiter. Bounded by LimiterCapacity. No
	// pinning — an evicted limiter just loses its token-bucket state,
	// equivalent to a long-quiet DID.
	limiters *lru.Cache[atmos.DID, *rate.Limiter]

	eventsVerified         atomic.Uint64
	chainBreaks            atomic.Uint64
	inversionFailures      atomic.Uint64
	inversionIncomplete    atomic.Uint64
	signatureFailures      atomic.Uint64
	resyncs                atomic.Uint64
	resyncFailures         atomic.Uint64
	revReplaysDropped      atomic.Uint64
	chainStateSaveFailures atomic.Uint64
	futureRevsRejected     atomic.Uint64
	fieldMismatches        atomic.Uint64
	opCIDMismatches        atomic.Uint64
	legacyCommits          atomic.Uint64
	syncNoOps              atomic.Uint64
	missingRecordBlocksOps atomic.Uint64
	duplicatePaths         atomic.Uint64
	oversizedCommits       atomic.Uint64
	accountsInactive       atomic.Uint64
	accountEventReplays    atomic.Uint64

	// Async resync subsystem. resyncStates maps DID -> per-DID state;
	// resyncQueue is what verifyCommit pushes jobs onto; workers read
	// it. resyncDone is exposed as ResyncEvents(); asyncErrs as
	// AsyncErrors(). workerCtx/workerCancel/workerWG together
	// implement clean shutdown via Close().
	resyncStates *xsync.Map[atmos.DID, *didResyncState]
	resyncQueue  chan resyncJob
	resyncDone   chan ResyncEvent
	asyncErrs    chan error
	pendingCap   int
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerWG     sync.WaitGroup
	closeOnce    sync.Once

	// Per-stage debug timings. See verifier_debug.go.
	timers verifierDebugTimers
}

// NewVerifier constructs a Verifier. Returns an error if required
// options are missing: StateStore, Directory, and (under the default
// PolicyResync) SyncClient.
func NewVerifier(opts VerifierOptions) (*Verifier, error) {
	if opts.StateStore == nil {
		return nil, fmt.Errorf("sync: NewVerifier: StateStore is required")
	}
	if opts.Directory == nil {
		return nil, fmt.Errorf("sync: NewVerifier: Directory is required")
	}
	policy := opts.Policy.ValOr(PolicyResync)
	if policy == PolicyResync {
		if !opts.SyncClient.HasVal() || opts.SyncClient.Val() == nil {
			return nil, fmt.Errorf("sync: NewVerifier: SyncClient is required for PolicyResync")
		}
	}
	if !opts.ResyncLimit.HasVal() || opts.ResyncLimit.Val() == 0 {
		opts.ResyncLimit = gt.Some(DefaultResyncLimit)
	}
	if !opts.ResyncBurst.HasVal() || opts.ResyncBurst.Val() == 0 {
		opts.ResyncBurst = gt.Some(DefaultResyncBurst)
	}
	if !opts.FutureRevTolerance.HasVal() {
		opts.FutureRevTolerance = gt.Some(DefaultFutureRevTolerance)
	}
	if !opts.Now.HasVal() {
		opts.Now = gt.Some(time.Now)
	}
	mutexCap := opts.MutexCapacity.ValOr(DefaultMutexCapacity)
	if mutexCap <= 0 {
		mutexCap = DefaultMutexCapacity
	}
	limiterCap := opts.LimiterCapacity.ValOr(DefaultLimiterCapacity)
	if limiterCap <= 0 {
		limiterCap = DefaultLimiterCapacity
	}
	workers := opts.AsyncResyncWorkers.ValOr(DefaultAsyncResyncWorkers)
	if workers <= 0 {
		workers = DefaultAsyncResyncWorkers
	}
	pendingCap := opts.PendingCap.ValOr(DefaultPendingCap)
	if pendingCap <= 0 {
		pendingCap = DefaultPendingCap
	}
	resyncBuf := opts.ResyncEventBuffer.ValOr(DefaultResyncEventBuffer)
	if resyncBuf <= 0 {
		resyncBuf = DefaultResyncEventBuffer
	}
	errBuf := opts.AsyncErrorBuffer.ValOr(DefaultAsyncErrorBuffer)
	if errBuf <= 0 {
		errBuf = DefaultAsyncErrorBuffer
	}

	ctx, cancel := context.WithCancel(context.Background())

	v := &Verifier{
		opts:         opts,
		didMu:        lru.New[atmos.DID, *sync.Mutex](mutexCap, 0),
		limiters:     lru.New[atmos.DID, *rate.Limiter](limiterCap, 0),
		resyncStates: xsync.NewMap[atmos.DID, *didResyncState](),
		resyncQueue:  make(chan resyncJob, 2*workers),
		resyncDone:   make(chan ResyncEvent, resyncBuf),
		asyncErrs:    make(chan error, errBuf),
		pendingCap:   pendingCap,
		workerCtx:    ctx,
		workerCancel: cancel,
	}
	v.startWorkers(workers)
	return v, nil
}

// lockDID acquires the per-DID mutex and returns an unlock function.
// The mutex is lazy-initialized via the LRU cache and pinned for the
// duration of the lock so eviction can't replace it underneath us
// (which would let two callers each acquire a distinct mutex for the
// same DID, breaking serialization). The unlock function unpins,
// making the entry eligible for eviction once nobody else holds it.
//
// Non-reentrant. A goroutine already holding the per-DID lock for did
// must not call lockDID(did) again, and helpers invoked under the
// lock must not call back into lockDID for the same DID.
func (v *Verifier) lockDID(did atmos.DID) func() {
	mu, _ := v.didMu.PinOrAdd(did, func() *sync.Mutex { return &sync.Mutex{} })
	mu.Lock()
	return func() {
		mu.Unlock()
		v.didMu.Unpin(did)
	}
}

// verifyCommitSignature looks up the DID's signing key, verifies the
// commit signature, and on first failure purges the directory cache
// and retries once (to handle key rotation).
//
// Returns *SignatureError on permanent failure (mismatch or unparseable
// public key after the purge+retry). Resolver/network errors are
// returned as wrapped infrastructure errors so callers can distinguish
// "couldn't check" from "checked and it's bad".
//
// When the directory has no cache configured, Purge is a no-op and the
// retry sees the same data as the first pass; cacheless deployments
// double their resolver load on signature failures but cannot recover
// from a key rotation via this path.
//
// Does not fire OnVerificationFailure; the orchestrating caller does.
func (v *Verifier) verifyCommitSignature(ctx context.Context, did atmos.DID, c *repo.Commit) error {
	id, err := v.opts.Directory.LookupDID(ctx, did)
	if err != nil {
		return fmt.Errorf("verifier: resolve %s: %w", did, err)
	}
	pubkey, keyErr := id.PublicKey()
	if keyErr == nil {
		if sigErr := c.VerifySignature(pubkey); sigErr == nil {
			return nil
		}
	}

	// First check failed; purge cache and retry once for key rotation.
	v.opts.Directory.Purge(ctx, did)
	id2, err := v.opts.Directory.LookupDID(ctx, did)
	if err != nil {
		return fmt.Errorf("verifier: re-resolve %s after purge: %w", did, err)
	}
	pubkey2, keyErr2 := id2.PublicKey()
	if keyErr2 != nil {
		return &SignatureError{DID: did, Rev: c.Rev, Cause: keyErr2}
	}
	if err := c.VerifySignature(pubkey2); err != nil {
		return &SignatureError{DID: did, Rev: c.Rev,
			KeyDID: pubkey2.DIDKey(), Cause: err}
	}
	return nil
}

// allowResync reports whether a resync for did is permitted under
// the per-DID rate limit. The limiter is lazy-initialized full to
// ResyncBurst, so the first ResyncBurst calls succeed immediately.
//
// The limiter cache is bounded by LimiterCapacity; an evicted entry's
// next call gets a fresh limiter (equivalent to a long-quiet DID
// returning to activity).
func (v *Verifier) allowResync(did atmos.DID) bool {
	lim, _ := v.limiters.GetOrAdd(did, func() *rate.Limiter {
		return rate.NewLimiter(v.opts.ResyncLimit.Val(), v.opts.ResyncBurst.Val())
	})
	return lim.Allow()
}

// resync fetches the authoritative repo via getRepo, verifies the
// fetched commit's signature, walks its MST, and yields every record
// as an ActionResync op. Advances chain state to the fetched
// (rev, data). Subject to the per-DID resync rate limit. Caller must
// hold the per-DID mutex.
//
// Under HostingGate, resyncs for non-active DIDs are short-circuited
// with ResyncFailedError{Reason: ReasonAccountInactive} — the PDS
// would refuse the getRepo anyway and we save the round-trip.
func (v *Verifier) resync(ctx context.Context, did atmos.DID, reason ResyncReason) ([]VerifierOp, error) {
	if aiErr, err := v.checkHostingGate(ctx, did); err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: err}
	} else if aiErr != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: ReasonAccountInactive, Cause: aiErr}
	}

	if !v.allowResync(did) {
		return nil, &ResyncRateLimitedError{DID: did}
	}

	body, err := v.opts.SyncClient.Val().GetRepoStream(ctx, did, "")
	if err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: err}
	}
	defer func() { _ = body.Close() }()

	rp, commit, err := repo.LoadFromCAR(body)
	if err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: err}
	}

	// Verify the fetched commit's signature. No chain check: the
	// whole point of resync is that the chain is broken.
	if err := v.verifyCommitSignature(ctx, did, commit); err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: err}
	}

	// Load prior state for both the OnResync callback (oldRev) and the
	// validation gates below (rev-regression check).
	old, _ := v.opts.StateStore.LoadChain(ctx, did)
	oldRev := ""
	if old != nil {
		oldRev = old.Rev
	}

	// Defense-in-depth gates on the served commit. A misconfigured or
	// hostile PDS that serves the wrong DID's repo, an old (v2)
	// commit, or a strictly older rev would otherwise corrupt our
	// chain state. Signature verify already rules out commits signed
	// by some other key, so DID/version checks are belt-and-suspenders;
	// the rev-regression check is the substantive guard.
	if err := validateFetchedCommit(did, commit, old); err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: err}
	}

	// Walk MST, build ops.
	var ops []VerifierOp
	walkErr := rp.Tree.Walk(func(key string, val cbor.CID) error {
		col, rkey := repo.SplitMSTKey(key)
		data, err := rp.Store.GetBlock(val)
		if err != nil {
			return fmt.Errorf("walk %s: %w", key, err)
		}
		ops = append(ops, VerifierOp{
			Action:     atmos.ActionResync,
			Collection: atmos.NSID(col),
			RKey:       atmos.RecordKey(rkey),
			Repo:       did,
			Rev:        atmos.TID(commit.Rev),
			CID:        val,
			BlockData:  data,
		})
		return nil
	})
	if walkErr != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: walkErr}
	}

	if err := v.opts.StateStore.SaveChain(ctx, did, ChainState{Rev: commit.Rev, Data: commit.Data}); err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason,
			Cause: fmt.Errorf("save chain state: %w", err)}
	}

	v.resyncs.Add(1)
	if v.opts.OnResync.HasVal() {
		v.opts.OnResync.Val()(did, oldRev, commit.Rev, reason)
	}
	return ops, nil
}

// Resync forces an immediate resync of one DID. Useful for consumers
// running PolicyError that want to repair a chain break or inversion
// failure themselves. Subject to the per-DID resync rate limit and
// per-DID mutex.
//
// Reports ReasonSyncEvent to OnResync, conflated with upstream-driven
// #sync events: the operator's intent (re-fetch authoritative state)
// is identical.
func (v *Verifier) Resync(ctx context.Context, did atmos.DID) ([]VerifierOp, error) {
	unlock := v.lockDID(did)
	defer unlock()
	return v.resync(ctx, did, ReasonSyncEvent)
}

// StateStore returns the configured state store, for read-only
// inspection (e.g. tests, or consumers filtering events on persisted
// hosting status).
//
// Callers must not Save through the returned store; the verifier owns
// all writes, and external writes desynchronize tracking and produce
// spurious chain-break errors. Load methods are safe.
func (v *Verifier) StateStore() StateStore {
	return v.opts.StateStore
}

// OnAccountEvent processes a #account event: derives the new
// HostingState from (Active, Status), persists it via
// StateStore.SaveHosting, fires the OnAccountStateChanged callback
// (if configured), and applies replay protection — events with seq
// at or below the persisted seq are silently dropped (counter:
// AccountEventReplaysDropped).
//
// Returns an error only on infrastructure failure (DID parse,
// StateStore.Load/Save). Replay drops are not errors.
//
// Streaming consumers should call this for every #account event
// observed on the firehose, in seq order. Per-DID locking
// serializes account-state updates against #commit/#sync gating, so
// the next commit for a DID sees the just-recorded HostingState.
func (v *Verifier) OnAccountEvent(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Account) error {
	if evt == nil {
		return nil
	}

	did, err := atmos.ParseDID(evt.DID)
	if err != nil {
		return fmt.Errorf("verifier: invalid account DID %q: %w", evt.DID, err)
	}

	unlock := v.lockDID(did)
	defer unlock()

	// Replay drop: account events arriving at or below the persisted
	// seq are re-deliveries from a reconnect. Not an error; just ignore them.
	prev, err := v.opts.StateStore.LoadHosting(ctx, did)
	if err != nil {
		return fmt.Errorf("verifier: load hosting state: %w", err)
	}
	if prev != nil && evt.Seq <= prev.Seq {
		v.accountEventReplays.Add(1)
		return nil
	}

	state := HostingState{
		Active: evt.Active,
		Seq:    evt.Seq,
		Time:   evt.Time,
	}
	if evt.Status.HasVal() {
		state.Status = evt.Status.Val()
	}

	if err := v.opts.StateStore.SaveHosting(ctx, did, state); err != nil {
		return fmt.Errorf("verifier: save hosting state: %w", err)
	}
	if v.opts.OnAccountStateChanged.HasVal() {
		v.opts.OnAccountStateChanged.Val()(did, state)
	}
	return nil
}

// Stats returns a snapshot of the verifier's counters. Safe to call
// concurrently. See VerifierStats for cross-counter coherence
// semantics.
func (v *Verifier) Stats() VerifierStats {
	return VerifierStats{
		EventsVerified:             v.eventsVerified.Load(),
		ChainBreaks:                v.chainBreaks.Load(),
		InversionFailures:          v.inversionFailures.Load(),
		InversionIncomplete:        v.inversionIncomplete.Load(),
		SignatureFailures:          v.signatureFailures.Load(),
		Resyncs:                    v.resyncs.Load(),
		ResyncFailures:             v.resyncFailures.Load(),
		RevReplaysDropped:          v.revReplaysDropped.Load(),
		ChainStateSaveFailures:     v.chainStateSaveFailures.Load(),
		FutureRevsRejected:         v.futureRevsRejected.Load(),
		FieldMismatches:            v.fieldMismatches.Load(),
		OpCIDMismatches:            v.opCIDMismatches.Load(),
		LegacyCommits:              v.legacyCommits.Load(),
		SyncNoOps:                  v.syncNoOps.Load(),
		MissingRecordBlocksOps:     v.missingRecordBlocksOps.Load(),
		DuplicatePaths:             v.duplicatePaths.Load(),
		OversizedCommits:           v.oversizedCommits.Load(),
		AccountsInactive:           v.accountsInactive.Load(),
		AccountEventReplaysDropped: v.accountEventReplays.Load(),
	}
}

// checkCommitSize enforces the spec-mandated bounds on a #commit
// envelope. Blocks is checked first so oversized CARs surface as
// "blocks" even when ops is also over-limit (deterministic metric
// labels). Caller increments counters and fires hooks.
func checkCommitSize(did atmos.DID, commit *comatproto.SyncSubscribeRepos_Commit) *CommitTooLargeError {
	if n := len(commit.Blocks); n > MaxCommitBlocksBytes {
		return &CommitTooLargeError{
			DID: did, Rev: commit.Rev,
			Field: "blocks", Got: n, Limit: MaxCommitBlocksBytes,
		}
	}
	if n := len(commit.Ops); n > MaxCommitOps {
		return &CommitTooLargeError{
			DID: did, Rev: commit.Rev,
			Field: "ops", Got: n, Limit: MaxCommitOps,
		}
	}
	return nil
}

// checkHostingGate consults persisted hosting state for did and
// returns a non-nil *AccountInactiveError when HostingPolicy is
// HostingGate and the persisted state is non-active. Returns nil
// (passes through) under HostingTrack regardless of state, on first
// sighting, or when the persisted state is active.
//
// Storage failures are wrapped and returned so callers can surface
// "couldn't check" distinct from "checked and inactive".
func (v *Verifier) checkHostingGate(ctx context.Context, did atmos.DID) (*AccountInactiveError, error) {
	if v.opts.HostingPolicy.ValOr(HostingTrack) != HostingGate {
		return nil, nil
	}
	state, err := v.opts.StateStore.LoadHosting(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("verifier: load hosting state: %w", err)
	}
	if state.IsActive() {
		return nil, nil
	}
	return &AccountInactiveError{DID: did, Status: state.Status}, nil
}

// findDuplicateOpPath returns the first path that appears more than
// once in commit.Ops, or "" if every path is unique. O(n) with a map;
// MaxCommitOps caps the map at 200 entries.
func findDuplicateOpPath(commit *comatproto.SyncSubscribeRepos_Commit) string {
	if len(commit.Ops) < 2 {
		return ""
	}
	seen := make(map[string]struct{}, len(commit.Ops))
	for _, op := range commit.Ops {
		if _, dup := seen[op.Path]; dup {
			return op.Path
		}
		seen[op.Path] = struct{}{}
	}
	return ""
}

// CommitVersion is the repo-commit version atmos accepts on the
// firehose. Sync 1.1 mandates v3. Exported so test helpers and
// downstream consumers writing synthetic commits don't have to
// hard-code the literal alongside this package.
const CommitVersion = 3

// isLegacyCommit reports whether commit has the Sync-1.0 shape:
// envelope prevData absent and no update/delete op carries op.Prev.
// Returns false for empty-ops commits (let them surface as a chain
// break) and for commits where any update/delete op has op.Prev set
// (the producer is clearly on 1.1; the missing prevData is a chain
// break, not a legacy signal).
func isLegacyCommit(commit *comatproto.SyncSubscribeRepos_Commit) bool {
	if commit.PrevData.HasVal() {
		return false
	}
	if len(commit.Ops) == 0 {
		return false
	}
	for _, op := range commit.Ops {
		if (op.Action == "update" || op.Action == "delete") && op.Prev.HasVal() {
			return false
		}
	}
	return true
}

// checkOpCIDs asserts every op in commit agrees with the post-state
// MST: create/update CIDs must equal tree.Get(path); delete paths
// must be absent. Returns the first mismatch or nil on success.
// Caller increments counters and fires hooks.
//
// store and dataCID are the pre-decoded block store and post-state
// root from decodeCommitFromCAR; passing them avoids re-parsing the
// CAR. Mirrors indigo's atproto/repo/sync.go VerifyCommitMessage.
func checkOpCIDs(commit *comatproto.SyncSubscribeRepos_Commit, dataCID cbor.CID, store *mst.MemBlockStore) *OpCIDMismatchError {
	if len(commit.Ops) == 0 {
		return nil
	}
	tree := mst.LoadTree(store, dataCID)
	did := atmos.DID(commit.Repo)
	for _, op := range commit.Ops {
		switch op.Action {
		case "create", "update":
			if !op.CID.HasVal() {
				reason := "create_missing_cid"
				if op.Action == "update" {
					reason = "update_missing_cid"
				}
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path, Reason: reason,
				}
			}
			claimed, err := cidFromLink(op.CID.Val())
			if err != nil {
				// Malformed CID is surfaced as a structural mismatch
				// rather than a parse error: the caller's recovery
				// path (resync) is the same.
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: op.Action + "_cid_mismatch",
				}
			}
			treeVal, err := tree.Get(op.Path)
			if err != nil {
				// Tree load error — same recovery as a real mismatch.
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: op.Action + "_cid_mismatch", OpCID: claimed,
				}
			}
			if treeVal == nil || !treeVal.Equal(claimed) {
				e := &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: op.Action + "_cid_mismatch", OpCID: claimed,
				}
				if treeVal != nil {
					e.MSTCID = *treeVal
				}
				return e
			}
		case "delete":
			treeVal, err := tree.Get(op.Path)
			if err != nil {
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: "delete_path_present",
				}
			}
			if treeVal != nil {
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: "delete_path_present", MSTCID: *treeVal,
				}
			}
			if op.CID.HasVal() {
				// Per spec, delete ops have no CID; indigo's
				// parseCommitOps rejects this.
				claimed, _ := cidFromLink(op.CID.Val())
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: "delete_unexpected_cid", OpCID: claimed,
				}
			}
		default:
			// Unknown actions are rejected upstream by inversion;
			// any commit reaching this gate has a known action set.
		}
	}
	return nil
}

// checkCommitFields asserts the firehose envelope and signed inner
// commit agree on did, rev, and version. Returns the first mismatch
// in priority order (version → did → rev) or nil. Caller increments
// counters and fires hooks.
//
// The version check is one-sided: the envelope carries no version
// field, so we require the inner commit's version to equal
// CommitVersion (3). A v2 commit surfaces as Field="version" with
// Envelope="3" so the error shape stays consistent.
func checkCommitFields(envelope *comatproto.SyncSubscribeRepos_Commit, inner *repo.Commit) *FieldMismatchError {
	did := atmos.DID(envelope.Repo)
	if inner.Version != CommitVersion {
		return &FieldMismatchError{
			DID:      did,
			Field:    "version",
			Envelope: fmt.Sprintf("%d", CommitVersion),
			Inner:    fmt.Sprintf("%d", inner.Version),
		}
	}
	if inner.DID != envelope.Repo {
		return &FieldMismatchError{
			DID:      did,
			Field:    "did",
			Envelope: envelope.Repo,
			Inner:    inner.DID,
		}
	}
	if inner.Rev != envelope.Rev {
		return &FieldMismatchError{
			DID:      did,
			Field:    "rev",
			Envelope: envelope.Rev,
			Inner:    inner.Rev,
		}
	}
	return nil
}

// checkSyncCommitFields asserts a #sync event's envelope fields agree
// with the signed inner commit decoded from its embedded CAR. Mirrors
// checkCommitFields for #commit events. Returns the first mismatch in
// priority order (version → did → rev) or nil. Caller increments
// counters and fires hooks.
func checkSyncCommitFields(envelope *comatproto.SyncSubscribeRepos_Sync, inner *repo.Commit) *FieldMismatchError {
	did := atmos.DID(envelope.DID)
	if inner.Version != CommitVersion {
		return &FieldMismatchError{
			DID:      did,
			Field:    "version",
			Envelope: fmt.Sprintf("%d", CommitVersion),
			Inner:    fmt.Sprintf("%d", inner.Version),
		}
	}
	if inner.DID != envelope.DID {
		return &FieldMismatchError{
			DID:      did,
			Field:    "did",
			Envelope: envelope.DID,
			Inner:    inner.DID,
		}
	}
	if inner.Rev != envelope.Rev {
		return &FieldMismatchError{
			DID:      did,
			Field:    "rev",
			Envelope: envelope.Rev,
			Inner:    inner.Rev,
		}
	}
	return nil
}

// validateFetchedCommit applies the resync-time invariants on an
// already signature-verified commit served by getRepo:
//
//   - inner.Version == CommitVersion (Sync 1.1 mandates v3)
//   - inner.DID matches the DID we requested (defense in depth: a
//     misconfigured PDS that serves another account's repo passes the
//     signature check only if we'd resolved the served DID's key,
//     which we don't, so signature verify already catches the obvious
//     case — but this surfaces the failure as a precise typed error)
//   - inner.Rev does not regress (or contradict at equal rev) the
//     locally-tracked state
//
// Returns nil on success, or a typed cause suitable for wrapping in
// [ResyncFailedError]. prev may be nil (first sighting); regression
// checks are skipped in that case.
func validateFetchedCommit(did atmos.DID, inner *repo.Commit, prev *ChainState) error {
	if inner.Version != CommitVersion {
		return &FieldMismatchError{
			DID:      did,
			Field:    "version",
			Envelope: fmt.Sprintf("%d", CommitVersion),
			Inner:    fmt.Sprintf("%d", inner.Version),
		}
	}
	if inner.DID != string(did) {
		return &FieldMismatchError{
			DID:      did,
			Field:    "did",
			Envelope: string(did),
			Inner:    inner.DID,
		}
	}
	if prev == nil {
		return nil
	}
	if inner.Rev < prev.Rev {
		return &RevRegressionError{
			DID:      did,
			SeenRev:  prev.Rev,
			SeenData: prev.Data,
			GotRev:   inner.Rev,
			GotData:  inner.Data,
		}
	}
	if inner.Rev == prev.Rev && !inner.Data.Equal(prev.Data) {
		return &RevRegressionError{
			DID:      did,
			SeenRev:  prev.Rev,
			SeenData: prev.Data,
			GotRev:   inner.Rev,
			GotData:  inner.Data,
		}
	}
	return nil
}

// checkFutureRev returns a non-nil *FutureRevError when rev's TID
// timestamp leads wall clock by more than FutureRevTolerance.
// Returns nil for unparseable revs (downstream gates handle them)
// and when tolerance is negative (operator opt-out).
func (v *Verifier) checkFutureRev(did atmos.DID, rev string) *FutureRevError {
	tolerance := v.opts.FutureRevTolerance.Val()
	if tolerance < 0 {
		return nil
	}
	tid, parseErr := atmos.ParseTID(rev)
	if parseErr != nil {
		// Malformed revs are intentionally not treated as future-rev;
		// downstream gates handle them.
		return nil //nolint:nilerr
	}
	revTime := tid.Time()
	now := v.opts.Now.Val()()
	if !revTime.After(now.Add(tolerance)) {
		return nil
	}
	return &FutureRevError{
		DID:       did,
		Rev:       rev,
		RevTime:   revTime,
		Now:       now,
		Tolerance: tolerance,
	}
}

// noCopy is a zero-size sentinel that go vet's copylocks pass treats
// as non-copyable. Embed in types holding non-copyable state.
// See https://golang.org/issues/8005.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// InvertCommit recovers the previous commit's MST root by inverting
// every op in commit against the partial MST in commit.Blocks.
// Starts from the post-state root (decoded from the commit block
// referenced by commit.Commit). For a structurally valid commit, the
// returned CID equals the previous commit's data CID.
//
// Returns *InversionError on a malformed CAR, missing commit block,
// op referencing a CID absent from the diff, or structurally broken
// partial MST. Does NOT error on a non-matching root: that's a chain
// break, detected by the caller comparing the returned CID against
// the expected prevData.
//
// Order semantics: ops are applied in commit-list order. MST inverse
// operations are individually commutative on disjoint paths, so the
// result is order-independent for well-formed commits. Commits with
// multiple ops on the same path are caught earlier by the
// duplicate-path gate; if invoked here directly with such a commit,
// the result is undefined.
//
// Verifier internals call invertCommitFromStore directly with a
// pre-decoded CAR.
func InvertCommit(commit *comatproto.SyncSubscribeRepos_Commit) (cbor.CID, error) {
	if commit == nil {
		return cbor.CID{}, &InversionError{Cause: fmt.Errorf("nil commit")}
	}
	did := atmos.DID(commit.Repo)
	innerCommit, store, carRoot, decErr := decodeCommitFromCAR(commit)
	if decErr != nil {
		return cbor.CID{}, &InversionError{DID: did, Rev: commit.Rev, Cause: decErr}
	}
	if mErr := checkCARRoot(commit, carRoot); mErr != nil {
		// Surface as InversionError so the public API's error contract
		// stays uniform; callers can still reach the typed mismatch via
		// errors.As on the wrapped Cause.
		return cbor.CID{}, &InversionError{DID: did, Rev: commit.Rev, Cause: mErr}
	}
	return invertCommitFromStore(commit, innerCommit, store)
}

// invertCommitFromStore is InvertCommit with a pre-decoded CAR, used
// by verifyCommit (which already has innerCommit and store from
// signature verification) to avoid re-parsing.
func invertCommitFromStore(commit *comatproto.SyncSubscribeRepos_Commit, innerCommit *repo.Commit, store *mst.MemBlockStore) (cbor.CID, error) {
	did := atmos.DID(commit.Repo)

	tree := mst.LoadTree(store, innerCommit.Data)

	for _, op := range commit.Ops {
		key := op.Path
		switch op.Action {
		case "create":
			if err := tree.Remove(key); err != nil {
				return cbor.CID{}, newInversionErr(did, commit.Rev, "invert create %q: %w", key, err)
			}
		case "delete", "update":
			if !op.Prev.HasVal() {
				return cbor.CID{}, newInversionErr(did, commit.Rev, "%s op %q missing prev CID", op.Action, key)
			}
			prevCID, err := cidFromLink(op.Prev.Val())
			if err != nil {
				return cbor.CID{}, newInversionErr(did, commit.Rev, "%s op %q bad prev CID: %w", op.Action, key, err)
			}
			if err := tree.Insert(key, prevCID); err != nil {
				return cbor.CID{}, newInversionErr(did, commit.Rev, "invert %s %q: %w", op.Action, key, err)
			}
		default:
			return cbor.CID{}, newInversionErr(did, commit.Rev, "unknown op action %q", op.Action)
		}
	}

	newRoot, err := tree.RootCID()
	if err != nil {
		return cbor.CID{}, newInversionErr(did, commit.Rev, "compute inverted root: %w", err)
	}
	return newRoot, nil
}

// newInversionErr wraps a formatted error in *InversionError.
// Free-function rather than an inline closure to avoid the per-call
// closure heap allocation on the verifyCommit hot path.
func newInversionErr(did atmos.DID, rev, format string, args ...any) error {
	return &InversionError{DID: did, Rev: rev, Cause: fmt.Errorf(format, args...)}
}

// cidFromLink parses a lextypes.LexCIDLink as a cbor.CID.
func cidFromLink(link lextypes.LexCIDLink) (cbor.CID, error) {
	return cbor.ParseCIDString(link.Link)
}

// VerifyAndExpand runs Sync 1.1 verification on one firehose event.
// Pass exactly one of commitEvt or syncEvt; passing both nil returns
// (nil, nil). The split signature avoids a streaming↔sync import
// cycle.
//
// Returns the operations the consumer should observe, or:
//   - (nil, nil) for silent drops (rev replay; #sync no-op).
//   - (nil, typed error) for rejections that bypass policy
//     (signature, future-rev, field mismatch, oversized commit).
//   - Under PolicyResync, chain breaks and inversion failures
//     trigger transparent resync; ResyncFailedError or
//     ResyncRateLimitedError surface only when the resync itself
//     fails.
//   - Under PolicyError, chain breaks and inversion failures yield a
//     typed error (ChainBreakError, InversionError,
//     OpCIDMismatchError, DuplicatePathError, or LegacyCommitError
//     under LegacyReject).
func (v *Verifier) VerifyAndExpand(
	ctx context.Context,
	commitEvt *comatproto.SyncSubscribeRepos_Commit,
	syncEvt *comatproto.SyncSubscribeRepos_Sync,
) ([]VerifierOp, error) {
	if commitEvt != nil {
		return v.verifyCommit(ctx, commitEvt)
	}
	if syncEvt != nil {
		return v.verifySync(ctx, syncEvt)
	}
	return nil, nil
}

// verifyCommit handles the #commit branch of VerifyAndExpand. Runs
// the gate sequence:
//
//  1. Parse DID
//  2. Future-rev check (bypasses policy, before per-DID lock)
//  3. Size limits  (bypasses policy, before per-DID lock)
//  4. Per-DID lock acquired
//  5. Rev-replay drop
//  6. Duplicate-path check
//  7. CAR decode (single parse, reused below)
//  8. Legacy-commit detection (route via policy if LegacyReject)
//  9. Inversion + chain check
//  10. Outer/inner field consistency (bypasses policy)
//  11. Signature verification
//  12. Op-CID consistency
//  13. State advance and op emission.
func (v *Verifier) verifyCommit(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit) ([]VerifierOp, error) {
	did, err := atmos.ParseDID(commit.Repo)
	if err != nil {
		return nil, fmt.Errorf("verifier: invalid DID %q: %w", commit.Repo, err)
	}

	// Future-rev check runs before the per-DID lock and before chain
	// load. Accepting a future-dated rev would advance state and
	// starve out legitimate follow-on events. Bypasses policy.
	if frErr := v.checkFutureRev(did, commit.Rev); frErr != nil {
		v.futureRevsRejected.Add(1)
		if v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, frErr)
		}
		return nil, frErr
	}

	// Size limits run before the per-DID lock so oversized commits
	// can't block other events for the same DID. Bypasses policy.
	if tlErr := checkCommitSize(did, commit); tlErr != nil {
		v.oversizedCommits.Add(1)
		if v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, tlErr)
		}
		return nil, tlErr
	}

	lockStart := time.Now()
	unlock := v.lockDID(did)
	addStageNs(&v.timers.lockWaitNs, &v.timers.lockWaitCount, lockStart)

	// hookErr captures a verification failure to surface to
	// OnVerificationFailure AFTER unlock(), so a hook implementation
	// that calls back into the verifier (e.g. Resync) doesn't
	// deadlock on the per-DID mutex.
	var hookErr error
	defer func() {
		unlock()
		if hookErr != nil && v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, hookErr)
		}
	}()

	defer addStageNs(&v.timers.totalNs, &v.timers.totalCount, time.Now())
	ops, hkErr, retErr := v.verifyCommitLocked(ctx, did, commit, true)
	hookErr = hkErr
	return ops, retErr
}

// verifyCommitLocked is the inner body of verifyCommit, called while
// the per-DID mutex is already held. allowAsyncResync controls whether
// chain-break / inversion-failure routes through the async enqueue
// (true, called from verifyCommit) or returns the typed error directly
// (false, called from the worker during replay).
//
// Returns (ops, hookErr, retErr):
//   - ops:     verified ops on success; nil otherwise.
//   - hookErr: the typed verification failure that should fire
//     OnVerificationFailure once the per-DID lock is
//     released. nil for success or for infrastructure
//     errors that don't fit the hook contract.
//   - retErr:  the error to return to the caller. Distinct from
//     hookErr when the failure was routed through
//     handleVerificationFailure under PolicyResync (which
//     returns nil, nil because the resync was enqueued —
//     the consumer sees no error).
func (v *Verifier) verifyCommitLocked(
	ctx context.Context,
	did atmos.DID,
	commit *comatproto.SyncSubscribeRepos_Commit,
	allowAsyncResync bool,
) (ops []VerifierOp, hookErr, retErr error) {
	// Hosting gate (HostingGate only): drop events for non-active
	// DIDs before any chain-state I/O. Bypasses VerifierPolicy. Runs
	// under the per-DID lock so it serializes against concurrent
	// OnAccountEvent updates. Ordered ahead of LoadChain so a
	// takedown-heavy upstream doesn't pay a chain-store round trip
	// per gated event.
	if aiErr, err := v.checkHostingGate(ctx, did); err != nil {
		return nil, hookErr, err
	} else if aiErr != nil {
		v.accountsInactive.Add(1)
		hookErr = aiErr
		return nil, hookErr, aiErr
	}

	loadStart := time.Now()
	state, err := v.opts.StateStore.LoadChain(ctx, did)
	addStageNs(&v.timers.loadChainNs, &v.timers.loadChainCount, loadStart)
	if err != nil {
		return nil, hookErr, fmt.Errorf("verifier: load chain state: %w", err)
	}

	// Rev-replay drop: a commit at or below persisted rev is a
	// re-delivery (or out-of-order) and silently dropped.
	if state != nil && commit.Rev <= state.Rev {
		v.revReplaysDropped.Add(1)
		return nil, hookErr, nil
	}

	// Duplicate-path check uses only the ops list, no CAR parse.
	// Earliest reject for malformed/malicious input. Pass zero
	// dataCID; handleVerificationFailure skips state-advance for a
	// commit we can't trust.
	if dupPath := findDuplicateOpPath(commit); dupPath != "" {
		v.duplicatePaths.Add(1)
		dupErr := &DuplicatePathError{DID: did, Rev: commit.Rev, Path: dupPath}
		hookErr = dupErr
		ops, retErr = v.handleVerificationFailure(ctx, did, commit, cbor.CID{}, ReasonInversionFailure, dupErr, allowAsyncResync)
		return ops, hookErr, retErr
	}

	// Decode the CAR once; every downstream stage reuses this parse.
	decodeStart := time.Now()
	innerCommit, store, carRoot, decErr := decodeCommitFromCAR(commit)
	addStageNs(&v.timers.decodeCARNs, &v.timers.decodeCARCount, decodeStart)
	if decErr != nil {
		v.inversionFailures.Add(1)
		invErr := &InversionError{DID: did, Rev: commit.Rev, Cause: decErr}
		hookErr = invErr
		ops, retErr = v.handleVerificationFailure(ctx, did, commit, cbor.CID{}, ReasonInversionFailure, invErr, allowAsyncResync)
		return ops, hookErr, retErr
	}
	dataCID := innerCommit.Data

	// CAR root MUST equal the envelope's Commit link. Surface as a
	// FieldMismatchError; bypasses policy (a misbehaving upstream
	// serving inconsistent CAR metadata isn't repaired by resync).
	// State is not advanced — the producer is malformed.
	if mErr := checkCARRoot(commit, carRoot); mErr != nil {
		v.fieldMismatches.Add(1)
		hookErr = mErr
		return nil, hookErr, mErr
	}

	// Legacy 1.0-shape detection runs before inversion so a
	// non-upgraded upstream surfaces as the precise typed error
	// rather than a misleading "missing prev CID" InversionError.
	// First sighting is unaffected.
	legacy := state != nil && isLegacyCommit(commit)
	if legacy {
		v.legacyCommits.Add(1)
		if v.opts.LegacyCommitPolicy.ValOr(LegacyAccept) == LegacyReject {
			lcErr := &LegacyCommitError{
				DID:      did,
				Rev:      commit.Rev,
				SeenRev:  state.Rev,
				SeenData: state.Data,
			}
			hookErr = lcErr
			ops, retErr = v.handleVerificationFailure(ctx, did, commit, dataCID, ReasonLegacyCommit, lcErr, allowAsyncResync)
			return ops, hookErr, retErr
		}
		// LegacyAccept: fall through; the chain-link check below is
		// skipped (impossible without prevData), but signature and
		// op-CID checks still apply.
	}

	// Inversion + chain check. Skipped on first sighting (nothing to
	// chain against) and on legacy-accepted commits (no prevData).
	if state != nil && !legacy {
		invStart := time.Now()
		inverted, err := invertCommitFromStore(commit, innerCommit, store)
		addStageNs(&v.timers.invertNs, &v.timers.invertCount, invStart)
		if err != nil {
			v.inversionFailures.Add(1)
			hookErr = err
			ops, retErr = v.handleVerificationFailure(ctx, did, commit, dataCID, ReasonInversionFailure, err, allowAsyncResync)
			return ops, hookErr, retErr
		}

		var prevDataCID cbor.CID
		if commit.PrevData.HasVal() {
			prevDataCID, _ = cidFromLink(commit.PrevData.Val())
		}
		invertedMatches := inverted.Equal(state.Data)
		prevDataMatches := prevDataCID.Equal(state.Data)
		if !invertedMatches || !prevDataMatches {
			// Lenient carve-out: the upstream's prevData matches our
			// stored chain state — i.e. the upstream IS honestly
			// continuing the chain — but our local inversion produced
			// a different root because the CAR was missing blocks
			// needed for full inversion. Bluesky's production relay
			// runs in this lenient mode (RELAY_LENIENT_SYNC_VALIDATION)
			// and forwards such commits with a "failed to invert" log
			// rather than dropping them. Match that behavior here:
			// surface *InversionIncompleteError via the hook for
			// visibility, but advance state normally and let the
			// commit's ops reach the consumer. Triggering a resync
			// here would be both expensive AND no more correct than
			// trusting the prevData claim, since the upstream is
			// already trusted to ship correctly-prevData'd events.
			lenient := v.opts.LenientInversion.ValOr(true)
			if lenient && prevDataMatches && !invertedMatches {
				v.inversionIncomplete.Add(1)
				hookErr = &InversionIncompleteError{
					DID:          did,
					SeenRev:      state.Rev,
					SeenData:     state.Data,
					GotRev:       commit.Rev,
					GotPrevData:  prevDataCID,
					InvertedData: inverted,
				}
				// Fall through to signature check + state advance.
			} else {
				v.chainBreaks.Add(1)
				cbErr := &ChainBreakError{
					DID:          did,
					SeenRev:      state.Rev,
					SeenData:     state.Data,
					GotRev:       commit.Rev,
					GotPrevData:  prevDataCID,
					InvertedData: inverted,
				}
				hookErr = cbErr
				ops, retErr = v.handleVerificationFailure(ctx, did, commit, dataCID, ReasonChainBreak, cbErr, allowAsyncResync)
				return ops, hookErr, retErr
			}
		}
	}

	// Outer/inner field consistency runs before signature verify so a
	// relabeled envelope is rejected without paying the P-256 cost.
	// Bypasses policy.
	if fmErr := checkCommitFields(commit, innerCommit); fmErr != nil {
		v.fieldMismatches.Add(1)
		hookErr = fmErr
		return nil, hookErr, fmErr
	}
	sigStart := time.Now()
	sigVerifyErr := v.verifyCommitSignature(ctx, did, innerCommit)
	addStageNs(&v.timers.sigVerifyNs, &v.timers.sigVerifyCount, sigStart)
	if err := sigVerifyErr; err != nil {
		// Count true signature mismatches only. Resolver/network
		// errors are infrastructure failures and surface as wrapped
		// errors so operators can distinguish "couldn't check" from
		// "checked and it's bad".
		var sigErr *SignatureError
		if errors.As(err, &sigErr) {
			v.signatureFailures.Add(1)
			hookErr = sigErr
			return nil, hookErr, sigErr
		}
		return nil, hookErr, fmt.Errorf("verifier: signature verification: %w", err)
	}

	// Op-CID consistency: ops list must agree with the post-state MST.
	// Routed via the inversion-failure path so PolicyResync recovers
	// transparently.
	opCIDStart := time.Now()
	opCIDErr := checkOpCIDs(commit, dataCID, store)
	addStageNs(&v.timers.opCIDCheckNs, &v.timers.opCIDCheckCount, opCIDStart)
	if opErr := opCIDErr; opErr != nil {
		v.opCIDMismatches.Add(1)
		hookErr = opErr
		ops, retErr = v.handleVerificationFailure(ctx, did, commit, dataCID, ReasonInversionFailure, opErr, allowAsyncResync)
		return ops, hookErr, retErr
	}

	// Success: build verified ops and advance state.
	buildStart := time.Now()
	ops, err = v.buildOpsFromCommit(commit, store)
	addStageNs(&v.timers.buildOpsNs, &v.timers.buildOpsCount, buildStart)
	if err != nil {
		v.inversionFailures.Add(1)
		ops, retErr = v.handleVerificationFailure(ctx, did, commit, dataCID, ReasonInversionFailure,
			&InversionError{DID: did, Rev: commit.Rev, Cause: err}, allowAsyncResync)
		return ops, hookErr, retErr
	}
	saveStart := time.Now()
	saveErr := v.opts.StateStore.SaveChain(ctx, did, ChainState{Rev: commit.Rev, Data: dataCID})
	addStageNs(&v.timers.saveChainNs, &v.timers.saveChainCount, saveStart)
	if err := saveErr; err != nil {
		return nil, hookErr, fmt.Errorf("verifier: save chain state: %w", err)
	}
	v.eventsVerified.Add(1)
	return ops, hookErr, nil
}

// decodeCommitFromCAR loads the CAR diff, decodes the commit block
// referenced by commit.Commit, and returns the inner commit, the
// parsed block store, and the CAR's first root. Callers reuse the
// store to avoid re-parsing; callers that care about CAR-root
// integrity (the verifier hot path) compare the returned root to
// commit.Commit via [checkCARRoot].
func decodeCommitFromCAR(commit *comatproto.SyncSubscribeRepos_Commit) (*repo.Commit, *mst.MemBlockStore, cbor.CID, error) {
	commitCID, err := cidFromLink(commit.Commit)
	if err != nil {
		return nil, nil, cbor.CID{}, fmt.Errorf("commit CID: %w", err)
	}
	store, carRoot, err := repo.LoadBlocksFromCAR(bytes.NewReader(commit.Blocks))
	if err != nil {
		return nil, nil, cbor.CID{}, fmt.Errorf("read CAR: %w", err)
	}
	data, err := store.GetBlock(commitCID)
	if err != nil {
		return nil, nil, cbor.CID{}, fmt.Errorf("commit block missing from CAR: %w", err)
	}
	c, err := repo.DecodeCommitCBOR(data)
	if err != nil {
		return nil, nil, cbor.CID{}, err
	}
	return c, store, carRoot, nil
}

// checkCARRoot asserts the CAR's first root equals the envelope's
// Commit link. Per the Sync 1.1 spec, the CAR carrying a #commit
// event MUST have the commit CID as its first root; an upstream that
// disagrees is malformed. Mirrors indigo's atproto/repo's reliance
// on the CAR root for commit identity.
//
// Returns a typed *FieldMismatchError on mismatch, nil on agreement.
// Caller bypasses VerifierPolicy: a misbehaving upstream serving
// inconsistent CAR metadata isn't repaired by a resync against the
// same upstream.
func checkCARRoot(envelope *comatproto.SyncSubscribeRepos_Commit, carRoot cbor.CID) *FieldMismatchError {
	envelopeCID, err := cidFromLink(envelope.Commit)
	if err != nil {
		// Malformed envelope link; surface as a mismatch with empty
		// envelope side. The caller handles the same way regardless.
		return &FieldMismatchError{
			DID: atmos.DID(envelope.Repo), Field: "commit",
			Envelope: envelope.Commit.Link, Inner: carRoot.String(),
		}
	}
	if envelopeCID.Equal(carRoot) {
		return nil
	}
	return &FieldMismatchError{
		DID: atmos.DID(envelope.Repo), Field: "commit",
		Envelope: envelopeCID.String(), Inner: carRoot.String(),
	}
}

// decodeCommitFromSyncCAR decodes a #sync event's embedded commit.
// Per com.atproto.sync.subscribeRepos#sync the CAR's first root is
// the commit CID; the CAR contains only the commit block (no MST,
// no records), so this is cheaper than decodeCommitFromCAR.
func decodeCommitFromSyncCAR(syncEvt *comatproto.SyncSubscribeRepos_Sync) (*repo.Commit, error) {
	if len(syncEvt.Blocks) == 0 {
		return nil, errors.New("sync event has no blocks")
	}
	store, rootCID, err := repo.LoadBlocksFromCAR(bytes.NewReader(syncEvt.Blocks))
	if err != nil {
		return nil, fmt.Errorf("read sync CAR: %w", err)
	}
	data, err := store.GetBlock(rootCID)
	if err != nil {
		return nil, fmt.Errorf("commit block missing from sync CAR: %w", err)
	}
	c, err := repo.DecodeCommitCBOR(data)
	if err != nil {
		return nil, fmt.Errorf("decode sync commit block: %w", err)
	}
	return c, nil
}

// buildOpsFromCommit decodes commit.Ops into VerifierOp values.
// Record blocks are pulled from store when present; missing blocks
// are not a failure (deletes have no CID, and partial CARs are
// permitted) but increment Stats.MissingRecordBlocksOps so operators
// can spot upstreams shipping incomplete CARs.
//
// store must be the block store returned by decodeCommitFromCAR for
// this same commit.
func (v *Verifier) buildOpsFromCommit(commit *comatproto.SyncSubscribeRepos_Commit, store *mst.MemBlockStore) ([]VerifierOp, error) {
	// Empty-but-non-nil so the streaming layer can distinguish a
	// successful zero-ops verification (returns []VerifierOp{}) from
	// a rev-replay drop (returns nil at a higher level).
	if len(commit.Ops) == 0 {
		return []VerifierOp{}, nil
	}
	ops := make([]VerifierOp, 0, len(commit.Ops))
	for _, op := range commit.Ops {
		col, rkey := repo.SplitMSTKey(op.Path)
		o := VerifierOp{
			// Wire→typed conversions. The lexicon's RepoOp fields are
			// plain strings; here we lift them into the typed domain.
			// commit.Repo's parseability was already asserted by
			// verifyCommit's atmos.ParseDID call. Collection, RKey,
			// and Rev are NOT re-validated against their type's
			// strict syntax — see the streaming.Operation doc.
			Action:     atmos.Action(op.Action),
			Collection: atmos.NSID(col),
			RKey:       atmos.RecordKey(rkey),
			Repo:       atmos.DID(commit.Repo),
			Rev:        atmos.TID(commit.Rev),
		}
		if op.CID.HasVal() {
			cid, err := cidFromLink(op.CID.Val())
			if err != nil {
				return nil, fmt.Errorf("op %s: parse CID: %w", op.Path, err)
			}
			o.CID = cid
			if data, err := store.GetBlock(cid); err == nil {
				o.BlockData = data
			} else {
				v.missingRecordBlocksOps.Add(1)
			}
		}
		ops = append(ops, o)
	}
	return ops, nil
}

// handleVerificationFailure dispatches a chain-break, inversion, or
// equivalent failure per the verifier's policy. Caller must hold the
// per-DID mutex.
//
// Does NOT fire OnVerificationFailure — the caller is responsible for
// arranging that to fire after the per-DID lock is released, so a
// hook implementation that calls back into the verifier doesn't
// deadlock. The caller typically captures origErr in a deferred
// dispatcher; see verifyCommit.
//
// Under PolicyError, state advances to the offending commit's
// (rev, data) so re-deliveries don't re-report the same failure.
// Subsequent verified events therefore chain off a commit that
// already failed validation; consumers that want to truly stop
// processing a misbehaving DID must call StateStore.Delete(did)
// themselves.
//
// A zero dataCID signals that the caller couldn't decode the inner
// commit (malformed CAR, etc.); state-advance is skipped to avoid
// corrupting state with a value we don't trust.
func (v *Verifier) handleVerificationFailure(
	ctx context.Context,
	did atmos.DID,
	commit *comatproto.SyncSubscribeRepos_Commit,
	dataCID cbor.CID,
	reason ResyncReason,
	origErr error,
	allowAsyncResync bool,
) ([]VerifierOp, error) {
	policy := v.opts.Policy.ValOr(PolicyResync)
	if policy == PolicyResync && !allowAsyncResync {
		// Worker replay context: don't re-enqueue; behave like
		// PolicyError. The caller (the worker) surfaces origErr to
		// AsyncErrors().
		policy = PolicyError
	}
	switch policy {
	case PolicyResync:
		// Async path: mark the DID as resyncing under its FSM lock
		// and enqueue a job. The worker pool runs resync() off the
		// readLoop. When the worker finishes, ops flow through
		// ResyncEvents(); if the worker fails, the error flows through
		// AsyncErrors().
		//
		// The caller (verifyCommit) holds the per-DID serialization
		// lock from lockDID(); that lock is released in verifyCommit's
		// defer once we return. The brief window between unlock and
		// the worker re-acquiring the same lock is safe because the
		// FSM transition to statusResyncing — which we performed under
		// state.mu before returning — directs any concurrent
		// verifyCommit for the same DID into the pending-buffer
		// branch.
		state := v.lookupOrCreateResyncState(did)
		state.mu.Lock()
		switch state.status {
		case statusIdle:
			state.status = statusResyncing
			state.mu.Unlock()
			// Block on full queue: workerWG.Wait() bounds the wait,
			// and workerCtx cancellation interrupts it.
			select {
			case v.resyncQueue <- resyncJob{
				ctx:     ctx,
				did:     did,
				trigger: commit,
				reason:  reason,
			}:
			case <-v.workerCtx.Done():
				// Verifier closed mid-enqueue. Surface the original
				// error directly — same shape as PolicyError.
				return nil, origErr
			}
			return nil, nil

		case statusResyncing:
			// Append to pending; ring-shift on overflow.
			if len(state.pending) >= v.pendingCap {
				// Allocation-free ring shift: drop the oldest, append the new
				// in-place. append(state.pending[1:], commit) would allocate
				// because state.pending[1:] has cap == len.
				copy(state.pending, state.pending[1:])
				state.pending[len(state.pending)-1] = commit
				state.mu.Unlock()
				v.sendAsyncError(&BufferOverflowError{DID: did, Dropped: 1})
			} else {
				state.pending = append(state.pending, commit)
				state.mu.Unlock()
			}
			return nil, nil

		default:
			state.mu.Unlock()
			return nil, fmt.Errorf("verifier: unknown resync status %d", state.status)
		}
	case PolicyError:
		if dataCID.Defined() {
			if err := v.opts.StateStore.SaveChain(ctx, did, ChainState{Rev: commit.Rev, Data: dataCID}); err != nil {
				v.chainStateSaveFailures.Add(1)
			}
		}
		return nil, origErr
	default:
		return nil, fmt.Errorf("verifier: unknown policy %v", policy)
	}
}

// verifySync handles the #sync branch of VerifyAndExpand. A #sync
// event is a directive to re-fetch authoritative state.
//
// Fast path: when the embedded commit's data CID matches persisted
// state, the upstream is just confirming current state. Advance the
// persisted rev and skip getRepo. Indigo's tap takes a similar
// approach (cmd/tap/firehose.go).
//
// The fast path validates the embedded commit's envelope fields and
// signature before advancing rev. Without this, a hostile upstream
// that observed our SeenData could send #sync{Data: ourSeenData,
// Rev: <forged future rev>} and poison our rev tracker — every
// subsequent legitimate event below the forged rev would be silently
// dropped as a replay. Field/signature checks bypass policy: a
// forged commit isn't repaired by resync against the same upstream.
//
// CAR decode failures (truncated CAR, transport glitch) keep the
// existing fall-through-to-resync behavior; resync against the
// authoritative source is the right recovery for transport noise.
// Field or signature failures, by contrast, are intentional malformation
// and surface as typed errors directly.
//
// Replays (rev ≤ persisted rev) are silently dropped.
func (v *Verifier) verifySync(ctx context.Context, syncEvt *comatproto.SyncSubscribeRepos_Sync) ([]VerifierOp, error) {
	did, err := atmos.ParseDID(syncEvt.DID)
	if err != nil {
		return nil, fmt.Errorf("verifier: invalid sync DID %q: %w", syncEvt.DID, err)
	}

	// Future-rev check: same rationale as verifyCommit.
	if frErr := v.checkFutureRev(did, syncEvt.Rev); frErr != nil {
		v.futureRevsRejected.Add(1)
		if v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, frErr)
		}
		return nil, frErr
	}

	unlock := v.lockDID(did)

	// hookErr captures a verification failure to surface to
	// OnVerificationFailure AFTER unlock(); see verifyCommit for
	// the rationale.
	var hookErr error
	defer func() {
		unlock()
		if hookErr != nil && v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, hookErr)
		}
	}()

	// Hosting gate (HostingGate only): drop sync events for non-active
	// DIDs. Same rationale as verifyCommit.
	if aiErr, err := v.checkHostingGate(ctx, did); err != nil {
		return nil, err
	} else if aiErr != nil {
		v.accountsInactive.Add(1)
		hookErr = aiErr
		return nil, aiErr
	}

	state, err := v.opts.StateStore.LoadChain(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("verifier: load chain state for sync: %w", err)
	}
	if state != nil && syncEvt.Rev <= state.Rev {
		v.revReplaysDropped.Add(1)
		return nil, nil
	}

	// No-op fast path: state non-nil + embedded data CID matches
	// persisted SeenData. First sighting and empty-Blocks events
	// skip this and fall through to resync.
	if state != nil && len(syncEvt.Blocks) > 0 {
		inner, decErr := decodeCommitFromSyncCAR(syncEvt)
		switch {
		case decErr != nil:
			// Decode failure: transport-level glitch (truncated CAR,
			// etc.). Fall through to resync; the authoritative
			// getRepo response is the right recovery.
		case inner.Data.Equal(state.Data):
			// Data matches — the upstream is confirming our state.
			// Validate envelope/inner fields and signature before
			// advancing rev. A forged commit (any rev, our SeenData)
			// trying to poison our rev tracker is rejected here.
			if mErr := checkSyncCommitFields(syncEvt, inner); mErr != nil {
				v.fieldMismatches.Add(1)
				hookErr = mErr
				return nil, mErr
			}
			if sigErr := v.verifyCommitSignature(ctx, did, inner); sigErr != nil {
				// Same shape as verifyCommit's signature handling:
				// resolver/network errors surface as wrapped infra
				// errors; only true mismatches count and fire the hook.
				var typed *SignatureError
				if errors.As(sigErr, &typed) {
					v.signatureFailures.Add(1)
					hookErr = typed
					return nil, typed
				}
				return nil, fmt.Errorf("verifier: signature verification: %w", sigErr)
			}
			if err := v.opts.StateStore.SaveChain(ctx, did, ChainState{Rev: syncEvt.Rev, Data: state.Data}); err != nil {
				// Don't fall through to resync — that would try to
				// Save and likely fail the same way. Surface so
				// operators see the chain-store breakage.
				return nil, fmt.Errorf("verifier: save chain state on sync no-op: %w", err)
			}
			v.syncNoOps.Add(1)
			return nil, nil
		default:
			// Data CID mismatch: our state and the upstream's claim
			// disagree. Fall through to resync; getRepo is authoritative.
		}
	}

	return v.resync(ctx, did, ReasonSyncEvent)
}
