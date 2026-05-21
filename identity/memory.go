package identity

import "time"

// InMemoryDirectoryCapacity is the LRU capacity used by
// [NewInMemoryDirectory]. Sized for a firehose-scale consumer's
// active-DID working set with generous headroom; a busy day on
// Bluesky tops out under ~100k unique DIDs per hour at peak.
const InMemoryDirectoryCapacity = 100_000

// InMemoryDirectoryTTL is the cache TTL used by [NewInMemoryDirectory].
// PLC documents change rarely; signing-key rotation must be picked
// up eventually, but the verifier already calls [Directory.Purge]
// on signature failure to recover within one event. Six hours is a
// balance between staleness and resolver load.
const InMemoryDirectoryTTL = 6 * time.Hour

// NewInMemoryDirectory returns a [Directory] suitable for firehose-scale
// consumers: the [DefaultResolver] paired with an in-memory LRU cache
// of [InMemoryDirectoryCapacity] entries and a [InMemoryDirectoryTTL]
// expiry.
//
// Without a cache, a [Directory] resolves every DID via the network
// on every lookup — at firehose line rate (~500 events/second on
// Bluesky's relay) that's an HTTP request to plc.directory and an
// HTTPS GET to a stranger's domain per commit, which collapses
// throughput to a small fraction of line rate within seconds. Cached
// lookups are sub-microsecond and reuse one round trip per active DID.
//
// Callers that need different sizing, an explicit [Resolver]
// configuration (e.g. a custom PLC URL or a non-default
// [http.Client]), or a non-LRU cache should construct a
// [Directory] directly — this helper exists so the common case is a
// one-liner and the defaults match production needs.
func NewInMemoryDirectory() *Directory {
	return &Directory{
		Resolver: &DefaultResolver{},
		Cache:    NewLRUCache(InMemoryDirectoryCapacity, InMemoryDirectoryTTL),
	}
}
