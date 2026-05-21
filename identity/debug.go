package identity

import (
	"sync/atomic"
	"time"
)

// DirectoryDebug is a snapshot of a Directory's diagnostic counters.
// Like [sync.VerifierDebugTimings], this is intended for ad-hoc
// performance triage and is not part of any production observability
// contract.
type DirectoryDebug struct {
	// LookupDIDCalls is the total number of LookupDID calls received.
	LookupDIDCalls uint64

	// LookupDIDCacheHits is calls that returned from the LRU cache
	// without a resolver round-trip. CacheHits/Calls is the hit rate
	// — the single most important number for verifier hot-path
	// throughput.
	LookupDIDCacheHits uint64

	// LookupDIDCoalescedHits is calls that joined an in-flight
	// resolver request rather than starting their own. Counts as a
	// near-miss (no resolver work, but still blocks until the leader
	// finishes).
	LookupDIDCoalescedHits uint64

	// LookupDIDResolveNs is cumulative wall-time spent in the
	// underlying Resolver.ResolveDID call (the leader of a coalesced
	// group only). LookupDIDResolveNs / (Calls - CacheHits -
	// CoalescedHits) is mean resolve latency.
	LookupDIDResolveNs    uint64
	LookupDIDResolveCount uint64

	// PurgeCalls is the number of Purge invocations (signature
	// retry / key rotation path). Spikes here mean churn.
	PurgeCalls uint64
}

// directoryDebug is the live atomic backing for [DirectoryDebug].
type directoryDebug struct {
	lookupDIDCalls         atomic.Uint64
	lookupDIDCacheHits     atomic.Uint64
	lookupDIDCoalescedHits atomic.Uint64
	lookupDIDResolveNs     atomic.Uint64
	lookupDIDResolveCount  atomic.Uint64
	purgeCalls             atomic.Uint64
}

// addResolveNs records a resolver round-trip latency sample.
func (d *directoryDebug) addResolveNs(start time.Time) {
	d.lookupDIDResolveNs.Add(uint64(time.Since(start).Nanoseconds()))
	d.lookupDIDResolveCount.Add(1)
}

// Debug returns a snapshot of the directory's diagnostic counters.
func (d *Directory) Debug() DirectoryDebug {
	return DirectoryDebug{
		LookupDIDCalls:         d.debug.lookupDIDCalls.Load(),
		LookupDIDCacheHits:     d.debug.lookupDIDCacheHits.Load(),
		LookupDIDCoalescedHits: d.debug.lookupDIDCoalescedHits.Load(),
		LookupDIDResolveNs:     d.debug.lookupDIDResolveNs.Load(),
		LookupDIDResolveCount:  d.debug.lookupDIDResolveCount.Load(),
		PurgeCalls:             d.debug.purgeCalls.Load(),
	}
}
