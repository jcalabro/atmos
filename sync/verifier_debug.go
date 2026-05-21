package sync

import (
	"sync/atomic"
	"time"
)

// VerifierDebugTimings is a snapshot of per-stage cumulative wall time
// inside [Verifier.verifyCommitLocked]. Exported for diagnostic tools
// (e.g. atpbench) that want to attribute readLoop-blocking time to a
// specific verification stage.
//
// All values are nanoseconds aggregated across calls; the corresponding
// *Count field is the number of samples that contributed to the sum.
// Mean per stage is Ns/Count. Counters are loaded with relaxed atomics;
// concurrent calls may observe slightly inconsistent totals but each
// value is individually coherent.
//
// This API is intentionally not part of [VerifierStats]: it adds
// per-stage clock reads to the hot path and is intended for ad-hoc
// performance triage rather than production observability. The hot path
// uses [time.Now] which is monotonic and cheap (~25ns on Linux/amd64);
// the stage-timing fields short-circuit when the stage is skipped, so
// overhead is bounded by ~9 clock reads per verified commit.
type VerifierDebugTimings struct {
	// LockWaitNs is time spent blocked in lockDID waiting for the
	// per-DID mutex. High values indicate cross-event contention for
	// the same DID (rare on the firehose).
	LockWaitNs    uint64
	LockWaitCount uint64

	// LoadChainNs is time spent in StateStore.LoadChain. Dominates for
	// callers backing the state store with a remote KV.
	LoadChainNs    uint64
	LoadChainCount uint64

	// DecodeCARNs is time spent in decodeCommitFromCAR (CAR parse +
	// commit-block CBOR decode). Allocation-heavy.
	DecodeCARNs    uint64
	DecodeCARCount uint64

	// InvertNs is time spent in invertCommitFromStore (MST inversion).
	// Skipped on first sighting and on legacy-accepted commits.
	InvertNs    uint64
	InvertCount uint64

	// SigVerifyNs is time spent in verifyCommitSignature (directory
	// lookup + P-256 verify). The dominant cost when the directory
	// cache misses; near-zero on hit.
	SigVerifyNs    uint64
	SigVerifyCount uint64

	// OpCIDCheckNs is time spent in checkOpCIDs (op-list ↔ MST
	// consistency walk). Skipped under PolicyResync when the prior
	// stage routed through handleVerificationFailure.
	OpCIDCheckNs    uint64
	OpCIDCheckCount uint64

	// BuildOpsNs is time spent in buildOpsFromCommit (per-op record
	// block lookup + decode).
	BuildOpsNs    uint64
	BuildOpsCount uint64

	// SaveChainNs is time spent in StateStore.SaveChain. Like
	// LoadChain, dominates for remote-backed state stores.
	SaveChainNs    uint64
	SaveChainCount uint64

	// TotalNs is time spent in verifyCommitLocked end-to-end (from
	// the moment the per-DID lock is acquired through the post-stage
	// SaveChain). Excludes LockWaitNs.
	TotalNs    uint64
	TotalCount uint64
}

// verifierDebugTimers is the live atomic backing for
// [VerifierDebugTimings]. Stored on the Verifier struct rather than as
// package globals so multiple Verifier instances in the same process
// (e.g. tests) don't cross-contaminate.
type verifierDebugTimers struct {
	lockWaitNs, lockWaitCount       atomic.Uint64
	loadChainNs, loadChainCount     atomic.Uint64
	decodeCARNs, decodeCARCount     atomic.Uint64
	invertNs, invertCount           atomic.Uint64
	sigVerifyNs, sigVerifyCount     atomic.Uint64
	opCIDCheckNs, opCIDCheckCount   atomic.Uint64
	buildOpsNs, buildOpsCount       atomic.Uint64
	saveChainNs, saveChainCount     atomic.Uint64
	totalNs, totalCount             atomic.Uint64
}

// addStageNs records a single sample for a stage timer. The pattern is
// `defer addStageNs(&t.stageNs, &t.stageCount, time.Now())` at the top
// of the stage's section so a panic still records the elapsed time and
// the call site stays a one-liner.
//
// Caller passes time.Now()'s return value (start), not a duration, so
// stages that early-return on error still account for the partial work.
func addStageNs(ns, count *atomic.Uint64, start time.Time) {
	ns.Add(uint64(time.Since(start).Nanoseconds()))
	count.Add(1)
}

// DebugTimings returns a snapshot of the verifier's per-stage timing
// counters. See [VerifierDebugTimings] for caveats.
func (v *Verifier) DebugTimings() VerifierDebugTimings {
	return VerifierDebugTimings{
		LockWaitNs:      v.timers.lockWaitNs.Load(),
		LockWaitCount:   v.timers.lockWaitCount.Load(),
		LoadChainNs:     v.timers.loadChainNs.Load(),
		LoadChainCount:  v.timers.loadChainCount.Load(),
		DecodeCARNs:     v.timers.decodeCARNs.Load(),
		DecodeCARCount:  v.timers.decodeCARCount.Load(),
		InvertNs:        v.timers.invertNs.Load(),
		InvertCount:     v.timers.invertCount.Load(),
		SigVerifyNs:     v.timers.sigVerifyNs.Load(),
		SigVerifyCount:  v.timers.sigVerifyCount.Load(),
		OpCIDCheckNs:    v.timers.opCIDCheckNs.Load(),
		OpCIDCheckCount: v.timers.opCIDCheckCount.Load(),
		BuildOpsNs:      v.timers.buildOpsNs.Load(),
		BuildOpsCount:   v.timers.buildOpsCount.Load(),
		SaveChainNs:     v.timers.saveChainNs.Load(),
		SaveChainCount:  v.timers.saveChainCount.Load(),
		TotalNs:         v.timers.totalNs.Load(),
		TotalCount:      v.timers.totalCount.Load(),
	}
}
