package backfill

import (
	"context"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
)

// StoreEntry is the per-DID record returned by Store.Lookup. It
// captures both the engine-tracked lifecycle state and the last
// listRepos.Active value the Store has recorded, so the engine can
// detect an active-flip and emit OnUpdate without an extra round-trip.
type StoreEntry struct {
	// State is the lifecycle state. StateUnknown means no row exists.
	State State

	// Active is the last-recorded entry.Active value. Meaningless for
	// StateUnknown (the producer treats it as zero in that case).
	Active bool
}

// HostInfo is the relay-observed metadata for one upstream PDS. RelayAccounts
// is a floor and must not be treated as the PDS's authoritative repo count.
type HostInfo struct {
	Hostname      string
	RelayStatus   string
	RelayAccounts int64
	Seq           int64
}

// HostState is a fleet-level lifecycle state.
type HostState string

const (
	HostStatePending   HostState = "pending"
	HostStateRunning   HostState = "running"
	HostStateBackoff   HostState = "backoff"
	HostStateDrained   HostState = "drained"
	HostStateExhausted HostState = "exhausted"
)

// Store persists per-DID and per-host backfill state. Implementations must be
// safe for concurrent calls. The engine serializes callbacks for a given DID
// and runs host callbacks independently.
//
// Store implementations are responsible for being fast: Lookup is
// called once per listRepos entry on the producer goroutine, which is
// the hot path of enumeration.
type Store interface {
	// Lookup reports the current state and last-recorded Active flag
	// for did. Returns StoreEntry{State: StateUnknown} (not an error)
	// if the DID has never been seen.
	Lookup(ctx context.Context, did atmos.DID) (StoreEntry, error)

	// OnDiscover is called the first time the engine sees an entry
	// whose DID Lookup reported as StateUnknown. Implementations must
	// durably persist a row at StateDiscovered (recording
	// entry.Active) before returning. An error here aborts the Run.
	OnDiscover(ctx context.Context, host string, entry sync.ListReposEntry) error

	// OnUpdate is called when the engine sees a known DID whose
	// listRepos.Active value differs from the value the Store last
	// recorded. Implementations must durably persist the new Active
	// flag (without changing the lifecycle State) before returning.
	// An error here aborts the Run.
	//
	// OnUpdate fires regardless of whether the new value is true or
	// false: an account flipping inactive→active or active→inactive
	// both reach this callback exactly once per flip.
	OnUpdate(ctx context.Context, host string, entry sync.ListReposEntry) error

	// OnComplete is called when a DID's repo has been downloaded and
	// Handler.HandleRepo returned nil. Implementations must durably
	// persist StateComplete before returning. commit.Rev is the rev
	// to record as the BackfillRev.
	//
	// host is the validated roster hostname used to enumerate and route the
	// repo. Redirect-aware rate-limit attribution remains an xrpc concern.
	OnComplete(ctx context.Context, did atmos.DID, host string, commit *repo.Commit) error

	// OnFail is called when the engine exhausts its retry budget for
	// a DID within the current Run. attempts is the total number of
	// download attempts made (initial + retries). Implementations
	// must durably persist StateFailed before returning; a future
	// Run will see StateFailed via Lookup and re-enqueue the DID.
	//
	// host is the validated roster hostname used to enumerate and route the
	// repo, including failures before a response is received.
	OnFail(ctx context.Context, did atmos.DID, host string, err error, attempts int) error

	// OnHost upserts relay metadata each time listHosts reports the host.
	OnHost(ctx context.Context, host HostInfo) error
	// HostCursor returns the last terminal batch cursor and whether this host
	// was fully enumerated by a prior run.
	HostCursor(ctx context.Context, hostname string) (cursor string, drained bool, err error)
	// SaveHostCursor persists a cursor only after every covered repo reached a
	// terminal state. Consumer durability ordering remains the Store's job.
	SaveHostCursor(ctx context.Context, hostname, cursor string) error
	OnHostDrained(ctx context.Context, hostname, lastNonEmptyCursor string) error
	OnHostExhausted(ctx context.Context, hostname string, err error, attempts int) error
}
