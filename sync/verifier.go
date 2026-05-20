package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/gt"
	"golang.org/x/time/rate"
)

// Default per-DID resync rate-limit values applied by NewVerifier when
// ResyncLimit/ResyncBurst are left at their zero values. Five resyncs
// per minute with a burst of five — conservative for production but
// override-able per the Sync 1.1 directive on consumer-side throttling.
const (
	DefaultResyncLimit = rate.Limit(5.0 / 60.0)
	DefaultResyncBurst = 5
)

// DefaultFutureRevTolerance is the maximum amount the rev's TID timestamp
// is allowed to lead the verifier's wall clock before we reject the event
// as future-dated. Matches indigo's relay (`futureRevTolerance` at
// `cmd/relay/relay/verify.go:22`). The spec ("inductive firehose") says
// future-timestamped revs MUST be ignored; this constant defines what
// counts as "future" once clock-skew slack is applied.
const DefaultFutureRevTolerance = 5 * time.Minute

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
	m sync.Map // map[atmos.DID]ChainState
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

	// ReasonLegacyCommit indicates an upstream emitted a Sync-1.0-shape
	// commit (no prevData; no op.Prev on update/delete ops) on the 1.1
	// firehose. Distinct from ReasonChainBreak so operators can tell
	// "non-compliant upstream" apart from genuine chain corruption.
	ReasonLegacyCommit
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

// LegacyCommitError is returned when a #commit looks like a Sync-1.0
// shape commit on a Sync-1.1 firehose: prevData is absent AND no
// update/delete op carries the new `prev` field. The producer is most
// likely a PDS that hasn't been upgraded for 1.1 yet, or has a known
// regression — the event is structurally valid by 1.0 rules but the
// 1.1 chain-validation invariants we depend on (prevData equality,
// MST inversion via op.Prev) cannot be checked.
//
// We surface this as a distinct typed error rather than letting it
// fall through to InversionError ("missing prev CID") so operators
// can build dashboards distinguishing "non-compliant 1.1 upstream"
// from "actively malformed commit." The recovery is identical to a
// chain break: under PolicyResync the verifier transparently
// re-fetches via getRepo (Reason: ReasonLegacyCommit); under
// PolicyError this typed error surfaces.
//
// Not fired on first sighting (state == nil). With no prior chain
// state, prevData absence alone is not a reliable legacy signal — a
// well-formed 1.1 producer simply doesn't have a previous commit to
// reference. The first-sighting accept path is unchanged here; A4
// in the tracker handles that case separately.
type LegacyCommitError struct {
	DID      atmos.DID
	Rev      string
	SeenRev  string   // last accepted rev for this DID
	SeenData cbor.CID // last accepted data CID for this DID
}

func (e *LegacyCommitError) Error() string {
	return fmt.Sprintf("sync: legacy 1.0-shape commit for %s rev=%s (seen rev=%s data=%s)",
		e.DID, e.Rev, e.SeenRev, e.SeenData)
}

// OpCIDMismatchError is returned when a commit's ops list claims
// CIDs (or absences) that disagree with the post-state MST. Per the
// Sync 1.1 validation checklist, the ops list is part of what
// "describes" the commit, and an internally inconsistent commit (ops
// list lies about MST contents) must be rejected.
//
// Indigo enforces the same invariant in `atproto/repo/sync.go`'s
// `VerifyCommitMessage`: each create/update op's CID must equal the
// MST tree value at the op's path; each delete op's path must be
// absent from the post-state tree. Atmos surfaces these as a typed
// error so downstream consumers can distinguish "envelope says
// something the MST doesn't" from a bare CAR/MST decode failure.
//
// Routed via the same path as InversionError: the commit is
// structurally inconsistent and a resync against authoritative state
// is the recovery. Under PolicyResync the consumer sees ActionResync
// ops; under PolicyError this typed error surfaces and chain state
// advances to the offending commit's data CID (matching the existing
// inversion-failure semantics).
//
// Reason describes the specific shape of the mismatch:
//   - "create_cid_mismatch":  op.CID != tree.Get(path) on a create
//   - "update_cid_mismatch":  op.CID != tree.Get(path) on an update
//   - "delete_path_present":  delete op's path is still present in tree
//   - "create_missing_cid":   create op has no CID
//   - "update_missing_cid":   update op has no CID
//   - "delete_unexpected_cid": delete op carries a CID it shouldn't
type OpCIDMismatchError struct {
	DID    atmos.DID
	Rev    string
	Path   string
	Reason string
	OpCID  cbor.CID // CID claimed by the op (zero if absent)
	MSTCID cbor.CID // CID actually present in the post-state MST (zero if absent)
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

// FieldMismatchError is returned when one or more fields in the
// firehose #commit envelope disagree with the corresponding field in
// the signed inner commit object (decoded from commit.Blocks). Per
// Sync 1.1's validation checklist, outer fields MUST match the signed
// commit; mismatches indicate a malformed PDS at best and a
// misattribution attack at worst — an attacker controlling any PDS
// can sign a valid commit for their own DID and wrap it in an
// envelope claiming a victim's DID, which would otherwise pass
// signature verification (the signature is over the inner commit) and
// land in chain state under the wrong identity.
//
// Like SignatureError and FutureRevError, FieldMismatchError bypasses
// VerifierPolicy: a resync against the same untrustworthy upstream
// would return the same garbage. Chain state is NOT advanced.
//
// Field is one of "did", "rev", or "version". For "version" mismatches
// we always require Sync 1.1's commit version 3; older versions are
// rejected unconditionally.
type FieldMismatchError struct {
	DID      atmos.DID
	Field    string // "did" | "rev" | "version"
	Envelope string // value seen on the firehose envelope
	Inner    string // value decoded from the signed commit block
}

func (e *FieldMismatchError) Error() string {
	return fmt.Sprintf("sync: %s field mismatch for %s: envelope=%q inner=%q",
		e.Field, e.DID, e.Envelope, e.Inner)
}

// FutureRevError is returned when a #commit or #sync event carries a
// rev whose TID timestamp is more than [VerifierOptions.FutureRevTolerance]
// ahead of the verifier's wall clock. Per the Sync 1.1 spec, future-
// timestamped revs MUST be ignored; this error surfaces the rejection
// to consumers for observability.
//
// Like SignatureError, FutureRevError bypasses [VerifierPolicy] — the
// event is rejected outright under both PolicyError and PolicyResync
// because no resync can recover from a clock-broken or malicious
// upstream. Chain state is NOT advanced (the offending rev would
// then be persisted, starving out every legitimate event for this
// DID until wall-clock catches up).
//
// Revs that fail TID parsing entirely are NOT surfaced as
// FutureRevError; the verifier conservatively skips the future-rev
// gate in that case and lets the rest of the pipeline (chain check,
// signature) handle the malformed event.
type FutureRevError struct {
	DID       atmos.DID
	Rev       string
	RevTime   time.Time     // timestamp decoded from the TID
	Now       time.Time     // verifier's wall clock at rejection time
	Tolerance time.Duration // configured grace period
}

func (e *FutureRevError) Error() string {
	return fmt.Sprintf("sync: future rev for %s: rev=%s revTime=%s now=%s tolerance=%s",
		e.DID, e.Rev, e.RevTime.Format(time.RFC3339Nano),
		e.Now.Format(time.RFC3339Nano), e.Tolerance)
}

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
	// SignatureFailures counts true signature mismatches — i.e. cases
	// where verifyCommitSignature returned a typed *SignatureError after
	// purge+retry. Infrastructure failures during signature verification
	// (e.g. resolver/network errors looking up the DID's signing key) do
	// NOT increment this counter; they surface as wrapped errors from
	// VerifyAndExpand so operators can distinguish "we couldn't check"
	// from "we checked and it's bad."
	SignatureFailures uint64
	Resyncs           uint64
	ResyncFailures    uint64
	RevReplaysDropped uint64

	// ChainStateSaveFailures counts the number of times ChainStore.Save
	// failed during PolicyError state-advance after a verification
	// failure. The original verification error (ChainBreakError /
	// InversionError) was reported via OnVerificationFailure and the
	// return value, but the secondary save failure means future events
	// for this DID may re-report the same break until state catches up.
	ChainStateSaveFailures uint64

	// FutureRevsRejected counts events whose rev's TID timestamp was more
	// than the configured tolerance ahead of the verifier's wall clock.
	// Spec-mandated rejection; bypasses policy.
	FutureRevsRejected uint64

	// FieldMismatches counts events whose firehose envelope fields
	// (did/rev/version) disagreed with the corresponding fields in the
	// signed inner commit. Spec-mandated rejection; bypasses policy.
	FieldMismatches uint64

	// OpCIDMismatches counts events whose ops list disagreed with the
	// post-state MST (claimed CID != tree value, or delete left the
	// path present). Routed via the inversion-failure path, so under
	// PolicyResync it triggers a transparent resync.
	OpCIDMismatches uint64

	// LegacyCommits counts events that arrived in Sync-1.0 shape (no
	// prevData, no op.Prev on update/delete) on the 1.1 firehose.
	// Routed through the resync path so PolicyResync recovers
	// transparently. A persistently nonzero LegacyCommits counter
	// alongside a high resync rate is the signature of a non-upgraded
	// upstream PDS.
	LegacyCommits uint64
}

// VerifierOp is the operation shape the Verifier produces. It mirrors
// streaming.Operation but lives in this package to avoid an import
// cycle. streaming converts these into streaming.Operation values
// when populating Event.verifiedOps.
type VerifierOp struct {
	Action     string // "create", "update", "delete", "resync"
	Collection string
	RKey       string
	Repo       string // DID
	Rev        string
	CID        []byte
	BlockData  []byte
}

// VerifierOptions configures a Verifier. ChainStore and Directory are
// always required; everything else is optional and wrapped in
// [gt.Option] so callers can express "unset" distinctly from a zero
// value.
type VerifierOptions struct {
	// Directory is used to resolve DIDs to signing keys for
	// signature verification. Required.
	Directory *identity.Directory

	// ChainStore persists per-DID state. Required. Use
	// NewMemChainStore() for tests; bring your own for production.
	ChainStore ChainStore

	// SyncClient is used to fetch repos via getRepo during resync.
	// Required when Policy is PolicyResync (or unset, since PolicyResync
	// is the default). Allowed but unused under PolicyError unless the
	// consumer calls Verifier.Resync directly.
	SyncClient gt.Option[*Client]

	// Policy selects the failure-handling mode. None means PolicyResync.
	Policy gt.Option[VerifierPolicy]

	// ResyncLimit is the per-DID resync rate (token bucket). None means
	// DefaultResyncLimit (5 per minute). Set to gt.Some(rate.Inf) for
	// tests that don't care about throttling.
	ResyncLimit gt.Option[rate.Limit]

	// ResyncBurst is the burst size for the per-DID rate limiter.
	// None means DefaultResyncBurst (5).
	ResyncBurst gt.Option[int]

	// OnResync, when set, fires once per successful resync, after chain
	// state has been advanced and just before the resynced ops are returned
	// to the caller. Invoked synchronously on the verifier's goroutine —
	// keep the callback fast or hand off to a worker. oldRev is "" when
	// this is the first resync we've ever performed for did (Load returned
	// nil) or the resync was triggered by a #sync event from an upstream we
	// hadn't seen before.
	//
	// Invoked while the per-DID mutex is held — a slow callback delays all
	// verification for that DID. Keep it fast or hand off to a worker
	// goroutine.
	OnResync gt.Option[func(did atmos.DID, oldRev, newRev string, reason ResyncReason)]

	// OnVerificationFailure, when set, fires once per verification failure
	// regardless of policy AND regardless of whether a subsequent resync
	// repaired the chain. ChainBreakError, InversionError, and
	// SignatureError each invoke this hook before the verifier decides
	// whether to attempt resync; ResyncFailedError and
	// ResyncRateLimitedError do NOT — they are downstream consequences of a
	// failure we already reported. Invoked synchronously on the verifier's
	// goroutine.
	//
	// Resolver/network errors that surface during signature verification
	// (without being typed *SignatureError) also do NOT fire this hook —
	// they propagate as wrapped infrastructure errors via the return value
	// only.
	OnVerificationFailure gt.Option[func(did atmos.DID, err error)]

	// FutureRevTolerance is the grace period for clock skew between this
	// verifier and the upstream PDS. Events whose rev's TID timestamp is
	// more than this far ahead of [Now] are rejected with
	// [*FutureRevError]. None means [DefaultFutureRevTolerance] (5
	// minutes). Set to a negative duration to disable the check entirely
	// (not recommended for production — the spec mandates rejection).
	FutureRevTolerance gt.Option[time.Duration]

	// Now overrides the wall clock used for the future-rev check. None
	// means [time.Now]. Primarily for deterministic tests; production
	// callers should leave it unset.
	Now gt.Option[func() time.Time]
}

// Verifier performs Sync 1.1 verification of firehose events. Safe
// for concurrent use across DIDs; per-DID work is serialized
// internally to prevent racing chain-state advances.
//
// Must not be copied; use NewVerifier and pass *Verifier.
type Verifier struct {
	_ noCopy

	opts VerifierOptions

	// per-DID serialization. Two events for the same DID never run
	// through verification concurrently.
	didMu sync.Map // map[atmos.DID]*sync.Mutex

	// per-DID resync rate limiter. Lazy-initialized.
	limiters sync.Map // map[atmos.DID]*rate.Limiter

	eventsVerified         atomic.Uint64
	chainBreaks            atomic.Uint64
	inversionFailures      atomic.Uint64
	signatureFailures      atomic.Uint64
	resyncs                atomic.Uint64
	resyncFailures         atomic.Uint64
	revReplaysDropped      atomic.Uint64
	chainStateSaveFailures atomic.Uint64
	futureRevsRejected     atomic.Uint64
	fieldMismatches        atomic.Uint64
	opCIDMismatches        atomic.Uint64
	legacyCommits          atomic.Uint64
}

// NewVerifier returns a Verifier with the given options. Returns an
// error if required fields are missing or inconsistent with the
// chosen Policy.
func NewVerifier(opts VerifierOptions) (*Verifier, error) {
	if opts.ChainStore == nil {
		return nil, fmt.Errorf("sync: NewVerifier: ChainStore is required")
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
	return &Verifier{opts: opts}, nil
}

// lockDID acquires the per-DID mutex for did, returning an unlock
// function. The mutex is lazy-initialized via LoadOrStore so the first
// caller for a DID materializes the mutex and any concurrent
// late-arrivals reuse it.
//
// sync.Mutex is non-reentrant — a goroutine that already holds the
// per-DID lock for did MUST NOT call lockDID(did) again or it will
// deadlock. Verification flows are structured to take the lock once
// at entry and release it on return; any helper invoked under that
// lock must not call back into lockDID for the same DID.
func (v *Verifier) lockDID(did atmos.DID) func() {
	val, _ := v.didMu.LoadOrStore(did, &sync.Mutex{})
	mu, ok := val.(*sync.Mutex)
	if !ok {
		// We are the sole writer of this map; a non-Mutex value means
		// memory corruption or a programming error elsewhere. Crash
		// rather than silently lose serialization.
		panic("Verifier.lockDID: stored value is not *sync.Mutex")
	}
	mu.Lock()
	return mu.Unlock
}

// verifyCommitSignature looks up the DID's signing key, verifies the
// commit's signature against it, and on first failure purges the
// directory cache and retries once (handling key rotation).
//
// Returns *SignatureError on permanent failure (signature still doesn't
// verify after purge+retry, or the post-purge re-resolution returns a
// key the commit can't be verified against).
//
// Errors from the directory itself (e.g. network failure during
// resolution) are wrapped without a typed error to distinguish "we
// couldn't check" from "we checked and it's bad."
//
// A first-pass PublicKey() parse failure is treated identically to a
// signature mismatch: we purge and retry. Only the second-pass
// PublicKey() parse failure surfaces as *SignatureError.
//
// When the directory has no cache configured, Purge is a no-op and
// the retry will see the same data as the first pass — key rotation
// won't be detected. Cacheless deployments will double their resolver
// load on every signature failure but cannot recover via this path.
//
// Does not fire OnVerificationFailure; the orchestrating caller is
// responsible for that, in line with the callback contract documented
// on VerifierOptions.
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

// allowResync returns true if a resync for did is allowed under the
// per-DID rate limit. The limiter is lazy-initialized: the very first
// call for a given DID materializes a fresh token bucket already full
// to ResyncBurst, so the first ResyncBurst calls succeed immediately
// without waiting for the bucket to refill.
func (v *Verifier) allowResync(did atmos.DID) bool {
	val, _ := v.limiters.LoadOrStore(did,
		rate.NewLimiter(v.opts.ResyncLimit.Val(), v.opts.ResyncBurst.Val()))
	lim, ok := val.(*rate.Limiter)
	if !ok {
		// We are the sole writer of this map; a non-*rate.Limiter value
		// means memory corruption or a programming error elsewhere.
		// Crash rather than silently mis-throttle.
		panic("Verifier.allowResync: stored value is not *rate.Limiter")
	}
	return lim.Allow()
}

// resync fetches the repo via getRepo, verifies the signature of the
// fetched commit, walks the MST, and yields all current records as
// ActionResync ops. Advances chain state to the fetched (rev, data).
//
// Subject to the per-DID resync rate limit. Caller must hold the
// per-DID mutex.
func (v *Verifier) resync(ctx context.Context, did atmos.DID, reason ResyncReason) ([]VerifierOp, error) {
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

	// Verify signature on fetched commit. We do NOT chain-check against
	// the old state — the whole point of resync is that the chain is
	// broken.
	if err := v.verifyCommitSignature(ctx, did, commit); err != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: err}
	}

	// Capture old state for the OnResync callback.
	old, _ := v.opts.ChainStore.Load(ctx, did)
	oldRev := ""
	if old != nil {
		oldRev = old.Rev
	}

	// Walk MST, build ops.
	var ops []VerifierOp
	walkErr := rp.Tree.Walk(func(key string, val cbor.CID) error {
		col, rkey := splitKey(key)
		data, err := rp.Store.GetBlock(val)
		if err != nil {
			return fmt.Errorf("walk %s: %w", key, err)
		}
		ops = append(ops, VerifierOp{
			Action:     "resync",
			Collection: col,
			RKey:       rkey,
			Repo:       string(did),
			Rev:        commit.Rev,
			CID:        val.Bytes(),
			BlockData:  data,
		})
		return nil
	})
	if walkErr != nil {
		v.resyncFailures.Add(1)
		return nil, &ResyncFailedError{DID: did, Reason: reason, Cause: walkErr}
	}

	if err := v.opts.ChainStore.Save(ctx, did, ChainState{Rev: commit.Rev, Data: commit.Data}); err != nil {
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
// running PolicyError that have decided to repair a chain break or
// inversion failure themselves.
//
// Subject to the per-DID resync rate limit and the per-DID mutex (so
// concurrent VerifyAndExpand calls for the same DID will not race).
//
// Resyncs initiated through this method use ReasonSyncEvent for callback /
// metrics labeling, since the operator's intent is identical to acting on
// an upstream #sync event (re-fetch authoritative state).
func (v *Verifier) Resync(ctx context.Context, did atmos.DID) ([]VerifierOp, error) {
	unlock := v.lockDID(did)
	defer unlock()
	return v.resync(ctx, did, ReasonSyncEvent)
}

// ChainStore returns the configured chain store. Useful for inspection
// from tests and for consumers that need to read state outside the
// verifier (e.g. to filter events on hosting status).
//
// Consumers MUST NOT call Save on the returned store; the verifier
// owns all writes. Calling Save externally will desynchronize the
// in-memory rev/data tracking the verifier maintains and produce
// spurious chain-break errors. Read-only access (Load) is safe.
func (v *Verifier) ChainStore() ChainStore {
	return v.opts.ChainStore
}

// Stats returns a snapshot of this verifier's counters. Safe to call
// concurrently with verification. See VerifierStats for the across-counter
// coherence caveat: each individual counter Load is atomic, but two
// counters may not be simultaneously consistent.
func (v *Verifier) Stats() VerifierStats {
	return VerifierStats{
		EventsVerified:         v.eventsVerified.Load(),
		ChainBreaks:            v.chainBreaks.Load(),
		InversionFailures:      v.inversionFailures.Load(),
		SignatureFailures:      v.signatureFailures.Load(),
		Resyncs:                v.resyncs.Load(),
		ResyncFailures:         v.resyncFailures.Load(),
		RevReplaysDropped:      v.revReplaysDropped.Load(),
		ChainStateSaveFailures: v.chainStateSaveFailures.Load(),
		FutureRevsRejected:     v.futureRevsRejected.Load(),
		FieldMismatches:        v.fieldMismatches.Load(),
		OpCIDMismatches:        v.opCIDMismatches.Load(),
		LegacyCommits:          v.legacyCommits.Load(),
	}
}

// commitVersion is the only repo-commit version atmos accepts on the
// firehose. Sync 1.1 mandates v3; older v2 commits exist in archived
// data but should not appear on a 1.1-compliant subscribeRepos stream.
const commitVersion = 3

// isLegacyCommit reports whether the commit looks like a Sync-1.0
// shape commit: no prevData on the envelope, and no update/delete op
// declares the new `prev` field.
//
// Sole-create commits trigger if prevData is absent — without
// prevData, a 1.1-compliant chain check is impossible and every
// 1.1 producer is supposed to emit it. Update/delete with prev
// present is a clear 1.1 signal that overrides the prevData absence.
//
// Returns false on the empty-ops case: a no-op commit with no
// prevData is just a malformed 1.1 commit, not a legacy 1.0 commit
// (1.0 still required prevData on every commit, just under a
// different encoding); let it surface as a chain break instead.
func isLegacyCommit(commit *comatproto.SyncSubscribeRepos_Commit) bool {
	if commit.PrevData.HasVal() {
		return false
	}
	if len(commit.Ops) == 0 {
		return false
	}
	hasMutation := false
	for _, op := range commit.Ops {
		if op.Action == "update" || op.Action == "delete" {
			hasMutation = true
			if op.Prev.HasVal() {
				// At least one mutation op carries the new field —
				// the producer is on 1.1, just happened to omit
				// prevData. Surface as a chain break, not legacy.
				return false
			}
		}
	}
	// Pure-create commit (no update/delete to inspect): treat as
	// legacy when prevData is also absent. Without prevData we have
	// no way to validate the chain link, and a 1.1-compliant producer
	// would have emitted it.
	if !hasMutation {
		return true
	}
	// Has mutations and none carry prev — classic 1.0 shape.
	return true
}

// checkOpCIDs walks the commit's ops list and asserts each op's claim
// agrees with the post-state MST: create/update ops must declare a CID
// equal to the tree value at the op's path, delete ops must reference
// a path that is absent from the tree. Returns the first mismatch as
// a typed [*OpCIDMismatchError] (caller decides on counters and
// hooks); returns nil if every op is consistent.
//
// store and dataCID are the post-state block store and MST root that
// callers already have decoded — reusing them avoids a redundant CAR
// parse on the happy path.
//
// Mirrors indigo's invariants in `atproto/repo/sync.go`'s
// `VerifyCommitMessage`.
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
				// Malformed CID surfaces as missing — caller's typed
				// error reporting is more useful than a parse-error
				// detail here, since this gate's job is structural
				// consistency, not parsing.
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: op.Action + "_cid_mismatch",
				}
			}
			treeVal, err := tree.Get(op.Path)
			if err != nil {
				// Tree load error — surface as a structural mismatch
				// rather than a tree error, since the caller's
				// recovery path (resync) is the same.
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
				// Spec: delete ops have no CID. Indigo's parseCommitOps
				// rejects this; we surface it as a structural mismatch.
				claimed, _ := cidFromLink(op.CID.Val())
				return &OpCIDMismatchError{
					DID: did, Rev: commit.Rev, Path: op.Path,
					Reason: "delete_unexpected_cid", OpCID: claimed,
				}
			}
		default:
			// Unknown action — let buildOpsFromCommit handle (it'll
			// emit the op verbatim, downstream consumers ignore).
			// Inversion already rejects unknown actions, so any commit
			// reaching this gate has a known action set.
		}
	}
	return nil
}

// checkCommitFields enforces the spec MUST that firehose envelope
// fields agree with the signed inner commit. Returns a non-nil
// [*FieldMismatchError] iff a mismatch is found, naming the first
// offending field in the order: version, did, rev. Caller is
// responsible for incrementing counters and invoking hooks.
//
// The version check is asymmetric: we don't compare envelope-vs-inner
// (the envelope has no version field) — we just require the inner
// commit's version to be the Sync-1.1 commitVersion. A v2 commit
// surfaces as a "version" mismatch with envelope="3" so consumers see
// a consistent error shape.
func checkCommitFields(envelope *comatproto.SyncSubscribeRepos_Commit, inner *repo.Commit) *FieldMismatchError {
	did := atmos.DID(envelope.Repo)
	if inner.Version != commitVersion {
		return &FieldMismatchError{
			DID:      did,
			Field:    "version",
			Envelope: fmt.Sprintf("%d", commitVersion),
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

// checkFutureRev tests whether rev's TID timestamp is more than the
// configured tolerance ahead of the verifier's wall clock. Returns a
// non-nil [*FutureRevError] iff the rev is in the rejectable future.
//
// Returns nil for unparseable revs: the future-rev gate is best-effort
// in the face of malformed input, and downstream string-comparison gates
// (rev replay, chain check) handle that case adequately.
//
// Returns nil when the tolerance is configured to a negative duration —
// an explicit operator opt-out for environments where TID timestamps
// can't be trusted as a clock at all.
func (v *Verifier) checkFutureRev(did atmos.DID, rev string) *FutureRevError {
	tolerance := v.opts.FutureRevTolerance.Val()
	if tolerance < 0 {
		return nil
	}
	tid, parseErr := atmos.ParseTID(rev)
	if parseErr != nil {
		// Best-effort gate: malformed revs are deliberately skipped here
		// and handled by downstream gates (rev-replay, chain check,
		// field consistency). nilerr lint suppressed: the discarded
		// error is the documented intent, not a bug.
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

// noCopy is a zero-size sentinel that go vet's copylocks pass treats as
// non-copyable. Embed it in types whose state must not be duplicated
// (sync.Map / atomic.Uint64 fields). See https://golang.org/issues/8005.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// InvertCommit applies the inverse of every op in commit against the
// partial MST in commit.Blocks (the CAR diff), starting from the
// commit's post-state MST root (read from the commit block referenced
// by commit.Commit). Returns the resulting MST root CID, which — for a
// structurally valid commit — equals the previous commit's data CID.
//
// Returns *InversionError if the CAR is malformed, the commit block is
// missing or undecodable, an op references a CID not present in the
// diff, or the partial MST is structurally broken. Does NOT error on a
// non-matching root: that's a chain break, detected by the caller
// comparing the returned CID against the expected prevData.
func InvertCommit(commit *comatproto.SyncSubscribeRepos_Commit) (cbor.CID, error) {
	if commit == nil {
		return cbor.CID{}, &InversionError{Cause: fmt.Errorf("nil commit")}
	}
	did := atmos.DID(commit.Repo)
	wrapErr := func(format string, args ...any) error {
		return &InversionError{DID: did, Rev: commit.Rev, Cause: fmt.Errorf(format, args...)}
	}

	// Read CAR diff blocks into a fresh in-memory store.
	store := mst.NewMemBlockStore()
	if len(commit.Blocks) > 0 {
		cr, err := car.NewReader(bytes.NewReader(commit.Blocks))
		if err != nil {
			return cbor.CID{}, wrapErr("read CAR header: %w", err)
		}
		for {
			b, err := cr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return cbor.CID{}, wrapErr("read CAR block: %w", err)
			}
			if err := store.PutBlock(b.CID, b.Data); err != nil {
				return cbor.CID{}, wrapErr("store CAR block: %w", err)
			}
		}
	}

	// Resolve the post-state MST root by decoding the commit block from
	// the CAR. The firehose Commit message carries only a CID link to
	// the commit object; the actual data CID lives inside that block.
	commitCID, err := cidFromLink(commit.Commit)
	if err != nil {
		return cbor.CID{}, wrapErr("parse commit CID: %w", err)
	}
	commitData, err := store.GetBlock(commitCID)
	if err != nil {
		return cbor.CID{}, wrapErr("commit block missing from CAR: %w", err)
	}
	innerCommit, err := repo.DecodeCommitCBOR(commitData)
	if err != nil {
		return cbor.CID{}, wrapErr("decode commit block: %w", err)
	}

	// Load partial MST rooted at the post-state data CID.
	tree := mst.LoadTree(store, innerCommit.Data)

	// Apply inverse of each op.
	for _, op := range commit.Ops {
		key := op.Path
		switch op.Action {
		case "create":
			if err := tree.Remove(key); err != nil {
				return cbor.CID{}, wrapErr("invert create %q: %w", key, err)
			}
		case "delete", "update":
			if !op.Prev.HasVal() {
				return cbor.CID{}, wrapErr("%s op %q missing prev CID", op.Action, key)
			}
			prevCID, err := cidFromLink(op.Prev.Val())
			if err != nil {
				return cbor.CID{}, wrapErr("%s op %q bad prev CID: %w", op.Action, key, err)
			}
			if err := tree.Insert(key, prevCID); err != nil {
				return cbor.CID{}, wrapErr("invert %s %q: %w", op.Action, key, err)
			}
		default:
			return cbor.CID{}, wrapErr("unknown op action %q", op.Action)
		}
	}

	// Compute new root.
	newRoot, err := tree.RootCID()
	if err != nil {
		return cbor.CID{}, wrapErr("compute inverted root: %w", err)
	}
	return newRoot, nil
}

// cidFromLink converts a LexCIDLink to a cbor.CID. The link's
// underlying string is the CID's string form.
func cidFromLink(link lextypes.LexCIDLink) (cbor.CID, error) {
	return cbor.ParseCIDString(link.Link)
}

// VerifyAndExpand runs Sync 1.1 verification on a single firehose
// event. Called by the streaming layer once per #commit or #sync
// event before the event reaches the consumer's batch.
//
// Exactly one of commitEvt or syncEvt should be non-nil per call;
// passing both nil is a no-op returning (nil, nil). The split
// signature avoids a streaming->sync->streaming import cycle: the
// streaming layer already has the decoded `evt.Commit` and `evt.Sync`
// fields available and passes them in directly.
//
// Returns the operations the consumer should observe, or:
//   - (nil, nil) for silent drops (rev replay).
//   - (nil, *ChainBreakError|*InversionError|*SignatureError|*ResyncFailedError|*ResyncRateLimitedError)
//     under PolicyError, or for failures even PolicyResync can't recover from.
//
// Under PolicyResync, chain breaks and inversion failures trigger
// transparent resync; only resync failures or signature failures
// reach the consumer.
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

// verifyCommit handles the #commit branch of VerifyAndExpand: parses
// the DID, takes the per-DID lock, runs rev-replay/inversion/chain
// checks, signature checks, and on success advances chain state and
// emits per-op VerifierOp values.
func (v *Verifier) verifyCommit(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit) ([]VerifierOp, error) {
	did, err := atmos.ParseDID(commit.Repo)
	if err != nil {
		return nil, fmt.Errorf("verifier: invalid DID %q: %w", commit.Repo, err)
	}

	// Future-rev check runs before the per-DID lock and before chain-state
	// load: a future-dated rev that we accepted would advance state and
	// starve out every legitimate event for this DID, so we reject before
	// any state can be touched. Bypasses policy — no resync helps here.
	if frErr := v.checkFutureRev(did, commit.Rev); frErr != nil {
		v.futureRevsRejected.Add(1)
		if v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, frErr)
		}
		return nil, frErr
	}

	unlock := v.lockDID(did)
	defer unlock()

	state, err := v.opts.ChainStore.Load(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("verifier: load chain state: %w", err)
	}

	// Rev-replay drop: any commit at or below the persisted rev is a
	// re-delivery (or out-of-order) and silently dropped.
	if state != nil && commit.Rev <= state.Rev {
		v.revReplaysDropped.Add(1)
		return nil, nil
	}

	// Inversion + chain check (only meaningful when state is non-nil).
	var inverted cbor.CID
	if state != nil {
		// Legacy 1.0-shape detection runs before InvertCommit so a
		// non-upgraded upstream surfaces as the precise typed error
		// rather than a misleading "missing prev CID" InversionError.
		// First sighting (state == nil) is unaffected — without prior
		// state we have nothing to chain against either way.
		if isLegacyCommit(commit) {
			v.legacyCommits.Add(1)
			lcErr := &LegacyCommitError{
				DID:      did,
				Rev:      commit.Rev,
				SeenRev:  state.Rev,
				SeenData: state.Data,
			}
			return v.handleVerificationFailure(ctx, did, commit, ReasonLegacyCommit, lcErr)
		}

		inverted, err = InvertCommit(commit)
		if err != nil {
			v.inversionFailures.Add(1)
			return v.handleVerificationFailure(ctx, did, commit, ReasonInversionFailure, err)
		}

		var prevDataCID cbor.CID
		if commit.PrevData.HasVal() {
			prevDataCID, _ = cidFromLink(commit.PrevData.Val())
		}
		// Both the inverted post-state and the commit's declared
		// prevData must agree with locally-tracked state. Either one
		// disagreeing is a chain break.
		if !inverted.Equal(state.Data) || !prevDataCID.Equal(state.Data) {
			v.chainBreaks.Add(1)
			cbErr := &ChainBreakError{
				DID:          did,
				SeenRev:      state.Rev,
				SeenData:     state.Data,
				GotRev:       commit.Rev,
				GotPrevData:  prevDataCID,
				InvertedData: inverted,
			}
			return v.handleVerificationFailure(ctx, did, commit, ReasonChainBreak, cbErr)
		}
	}

	// Decode commit block + signature check. We need the decoded
	// commit both to verify the signature and to advance chain state.
	// The store returned here is reused by buildOpsFromCommit below to
	// avoid re-parsing the CAR on every accepted commit.
	innerCommit, store, decErr := decodeCommitFromCAR(commit)
	if decErr != nil {
		v.inversionFailures.Add(1)
		return v.handleVerificationFailure(ctx, did, commit, ReasonInversionFailure,
			&InversionError{DID: did, Rev: commit.Rev, Cause: decErr})
	}
	// Outer/inner field consistency. Runs before signature verification:
	// a relabeled envelope rev (e.g. to bypass our rev-replay drop) is
	// cheaper to reject here than after a P-256 verify, and a "did" or
	// "version" mismatch is a definite reject regardless of signature.
	// Bypasses policy — a misbehaving upstream cannot be repaired by
	// re-fetching from the same upstream.
	if fmErr := checkCommitFields(commit, innerCommit); fmErr != nil {
		v.fieldMismatches.Add(1)
		if v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, fmErr)
		}
		return nil, fmErr
	}
	if err := v.verifyCommitSignature(ctx, did, innerCommit); err != nil {
		// Only count true signature mismatches (typed *SignatureError).
		// Wrapped resolver/network errors are infrastructure failures, not
		// signature failures, and would otherwise pollute the counter.
		var sigErr *SignatureError
		if errors.As(err, &sigErr) {
			v.signatureFailures.Add(1)
			if v.opts.OnVerificationFailure.HasVal() {
				v.opts.OnVerificationFailure.Val()(did, sigErr)
			}
			return nil, sigErr
		}
		// Non-typed (e.g. resolver network error) — wrap and return; not a
		// signature failure.
		return nil, fmt.Errorf("verifier: signature verification: %w", err)
	}

	// Op-CID consistency: each create/update op must declare a CID
	// equal to the post-state MST tree value at the op's path; deletes
	// must reference an absent path. Reuses the already-decoded MST
	// root + block store. Routed via handleVerificationFailure with
	// ReasonInversionFailure so PolicyResync recovers transparently —
	// an internally-inconsistent commit is just as broken as a bad
	// CAR diff and the same recovery applies.
	if opErr := checkOpCIDs(commit, innerCommit.Data, store); opErr != nil {
		v.opCIDMismatches.Add(1)
		return v.handleVerificationFailure(ctx, did, commit, ReasonInversionFailure, opErr)
	}

	// Success: build verified ops, advance state.
	ops, err := buildOpsFromCommit(commit, store)
	if err != nil {
		v.inversionFailures.Add(1)
		return v.handleVerificationFailure(ctx, did, commit, ReasonInversionFailure,
			&InversionError{DID: did, Rev: commit.Rev, Cause: err})
	}
	if err := v.opts.ChainStore.Save(ctx, did, ChainState{Rev: commit.Rev, Data: innerCommit.Data}); err != nil {
		return nil, fmt.Errorf("verifier: save chain state: %w", err)
	}
	v.eventsVerified.Add(1)
	return ops, nil
}

// decodeCommitFromCAR loads the commit's CAR diff, looks up the
// commit block by commit.Commit's CID, and decodes it into a
// *repo.Commit. Used both for signature verification and for
// extracting the post-state MST data CID.
//
// Also returns the parsed block store so callers that subsequently
// need the CAR's record blocks (e.g. buildOpsFromCommit) can reuse it
// rather than parsing the CAR a second time.
func decodeCommitFromCAR(commit *comatproto.SyncSubscribeRepos_Commit) (*repo.Commit, *mst.MemBlockStore, error) {
	commitCID, err := cidFromLink(commit.Commit)
	if err != nil {
		return nil, nil, fmt.Errorf("commit CID: %w", err)
	}
	store, _, err := repo.LoadBlocksFromCAR(bytes.NewReader(commit.Blocks))
	if err != nil {
		return nil, nil, fmt.Errorf("read CAR: %w", err)
	}
	data, err := store.GetBlock(commitCID)
	if err != nil {
		return nil, nil, fmt.Errorf("commit block missing from CAR: %w", err)
	}
	c, err := repo.DecodeCommitCBOR(data)
	if err != nil {
		return nil, nil, err
	}
	return c, store, nil
}

// buildOpsFromCommit decodes the commit's ops list into VerifierOp
// values. For non-delete ops with a CID, the corresponding record
// block is pulled from the CAR diff if present; missing blocks are
// not an error here (deletes legitimately have no CID, and partial
// CARs may legitimately omit blocks the consumer doesn't need).
//
// store must be the block store returned by decodeCommitFromCAR for
// this same commit. Reusing the pre-parsed store avoids the per-event
// cost of re-parsing the CAR.
func buildOpsFromCommit(commit *comatproto.SyncSubscribeRepos_Commit, store *mst.MemBlockStore) ([]VerifierOp, error) {
	// Empty-but-non-nil so the streaming layer can distinguish a
	// successful zero-ops verification from a rev-replay drop, both
	// of which surface here as "no ops produced." Rev-replay returns
	// (nil, nil) higher up; this branch returns ([]VerifierOp{}, nil).
	if len(commit.Ops) == 0 {
		return []VerifierOp{}, nil
	}
	ops := make([]VerifierOp, 0, len(commit.Ops))
	for _, op := range commit.Ops {
		col, rkey := splitKey(op.Path)
		o := VerifierOp{
			Action:     op.Action,
			Collection: col,
			RKey:       rkey,
			Repo:       commit.Repo,
			Rev:        commit.Rev,
		}
		if op.CID.HasVal() {
			cid, err := cidFromLink(op.CID.Val())
			if err != nil {
				return nil, fmt.Errorf("op %s: parse CID: %w", op.Path, err)
			}
			o.CID = cid.Bytes()
			if data, err := store.GetBlock(cid); err == nil {
				o.BlockData = data
			}
		}
		ops = append(ops, o)
	}
	return ops, nil
}

// handleVerificationFailure dispatches a chain-break or inversion
// failure per the verifier's policy. Caller must hold the per-DID
// mutex.
//
// Always invokes OnVerificationFailure with the original typed error
// before consulting policy — consumers see every break, regardless of
// whether the verifier subsequently repairs it via resync.
//
// PolicyError advances state to the offending commit's (rev, data)
// so we don't perpetually re-report the same break for re-deliveries.
// If the commit is malformed enough that we can't decode its data
// CID, we skip the advance — a malformed commit can't be replayed
// identically and we'd rather leave state untouched than corrupt it.
func (v *Verifier) handleVerificationFailure(
	ctx context.Context,
	did atmos.DID,
	commit *comatproto.SyncSubscribeRepos_Commit,
	reason ResyncReason,
	origErr error,
) ([]VerifierOp, error) {
	if v.opts.OnVerificationFailure.HasVal() {
		v.opts.OnVerificationFailure.Val()(did, origErr)
	}
	policy := v.opts.Policy.ValOr(PolicyResync)
	switch policy {
	case PolicyResync:
		ops, rerr := v.resync(ctx, did, reason)
		if rerr != nil {
			return nil, rerr
		}
		return ops, nil
	case PolicyError:
		if dataCID := dataCIDFromCommit(commit); dataCID.Defined() {
			if err := v.opts.ChainStore.Save(ctx, did, ChainState{Rev: commit.Rev, Data: dataCID}); err != nil {
				v.chainStateSaveFailures.Add(1)
			}
		}
		return nil, origErr
	default:
		return nil, fmt.Errorf("verifier: unknown policy %v", policy)
	}
}

// dataCIDFromCommit returns the post-state MST root CID by decoding
// the commit block from the CAR. Returns the zero CID if any step
// fails — used by handleVerificationFailure when we may already be
// looking at a malformed commit.
func dataCIDFromCommit(commit *comatproto.SyncSubscribeRepos_Commit) cbor.CID {
	c, _, err := decodeCommitFromCAR(commit)
	if err != nil {
		return cbor.CID{}
	}
	return c.Data
}

// verifySync handles the #sync branch of VerifyAndExpand. A #sync event
// from upstream signals that the repo state changed out of band — the
// upstream is telling us "I no longer have a continuous chain to give
// you; here is my current rev." There is no commit body to verify
// against locally-tracked state, so we cannot incrementally advance
// the chain. The only sound response, regardless of policy, is to
// resync against authoritative state via getRepo and reconcile from
// there. PolicyError consumers do not get a typed error here: the
// event itself is not a verification failure, just a directive to
// re-fetch.
//
// Replays (rev <= persisted rev) are silently dropped, mirroring the
// rev-replay gate on #commit.
func (v *Verifier) verifySync(ctx context.Context, syncEvt *comatproto.SyncSubscribeRepos_Sync) ([]VerifierOp, error) {
	did, err := atmos.ParseDID(syncEvt.DID)
	if err != nil {
		return nil, fmt.Errorf("verifier: invalid sync DID %q: %w", syncEvt.DID, err)
	}

	// Future-rev check: same rationale as in verifyCommit. A #sync at a
	// future rev would also advance state and starve out legitimate
	// follow-on events.
	if frErr := v.checkFutureRev(did, syncEvt.Rev); frErr != nil {
		v.futureRevsRejected.Add(1)
		if v.opts.OnVerificationFailure.HasVal() {
			v.opts.OnVerificationFailure.Val()(did, frErr)
		}
		return nil, frErr
	}

	unlock := v.lockDID(did)
	defer unlock()

	state, err := v.opts.ChainStore.Load(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("verifier: load chain state for sync: %w", err)
	}
	if state != nil && syncEvt.Rev <= state.Rev {
		v.revReplaysDropped.Add(1)
		return nil, nil
	}

	return v.resync(ctx, did, ReasonSyncEvent)
}
