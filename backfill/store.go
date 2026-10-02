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
// The enumeration callbacks take batches: the engine looks up and records a
// whole listRepos or listHosts page per call rather than one entry at a time,
// because they run serially on a host's producer goroutine and, for a Store
// backed by a remote database, each call is at least one network round trip.
// A batch of one means exactly what the per-entry call it replaces did. Batches
// are never empty, and no DID or hostname appears twice in one call.
type Store interface {
	// Lookup reports, for each DID, the current state and last-recorded
	// Active flag. It returns exactly one StoreEntry per DID, in order, with
	// State StateUnknown (not an error) for a DID never seen.
	Lookup(ctx context.Context, dids []atmos.DID) ([]StoreEntry, error)

	// OnDiscover is called the first time the engine sees entries whose
	// DIDs Lookup reported as StateUnknown, all listed by host.
	// Implementations must durably persist a row at StateDiscovered
	// (recording entry.Active) for every entry before returning. An error
	// here aborts the Run; some of the rows may already be durable.
	OnDiscover(ctx context.Context, host string, entries []sync.ListReposEntry) error

	// OnUpdate is called with known DIDs whose listRepos.Active value
	// differs from the value the Store last recorded. Implementations must
	// durably persist each new Active flag (without changing the lifecycle
	// State) before returning. An error here aborts the Run.
	//
	// OnUpdate fires regardless of whether the new value is true or
	// false: an account flipping inactive→active or active→inactive
	// both reach this callback exactly once per flip. When one listRepos
	// page both discovers and updates DIDs, OnDiscover runs first.
	OnUpdate(ctx context.Context, host string, entries []sync.ListReposEntry) error

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
	// must durably persist StateFailed before returning.
	//
	// StateFailed is terminal for cursor purposes: the host's cursor
	// advances past a failed DID. Because a subsequent Run resumes
	// from that cursor (and skips drained hosts entirely), the engine
	// does not re-list failed DIDs; retrying them from Store state is
	// the consumer's responsibility. The engine only re-dispatches a
	// StateFailed DID if enumeration happens to revisit it (a lagging
	// cursor after a crash, or the DID appearing on another host).
	//
	// host is the validated roster hostname used to enumerate and route the
	// repo, including failures before a response is received.
	OnFail(ctx context.Context, did atmos.DID, host string, err error, attempts int) error

	// OnHost upserts relay metadata each time listHosts reports the hosts.
	OnHost(ctx context.Context, hosts []HostInfo) error
	// HostCursor returns, for each host, the last terminal batch cursor and
	// whether a prior run fully enumerated it: exactly one HostCursorState
	// per hostname, in order.
	HostCursor(ctx context.Context, hostnames []string) ([]HostCursorState, error)
	// SaveHostCursor persists a cursor only after every covered repo reached a
	// terminal state. Consumer durability ordering remains the Store's job.
	SaveHostCursor(ctx context.Context, hostname, cursor string) error
	OnHostDrained(ctx context.Context, hostname, lastNonEmptyCursor string) error
	OnHostExhausted(ctx context.Context, hostname string, err error, attempts int) error
}

// HostCursorState is Store.HostCursor's answer for one host.
type HostCursorState struct {
	// Cursor is the last terminal batch cursor.
	Cursor string
	// Drained reports whether a prior run fully enumerated the host.
	Drained bool
}
