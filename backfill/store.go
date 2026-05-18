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

// Store persists per-DID backfill state. Implementations must be safe
// for concurrent use across distinct DIDs. The engine guarantees no
// two callbacks are in flight for the same DID simultaneously.
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
	OnDiscover(ctx context.Context, entry sync.ListReposEntry) error

	// OnUpdate is called when the engine sees a known DID whose
	// listRepos.Active value differs from the value the Store last
	// recorded. Implementations must durably persist the new Active
	// flag (without changing the lifecycle State) before returning.
	// An error here aborts the Run.
	//
	// OnUpdate fires regardless of whether the new value is true or
	// false: an account flipping inactive→active or active→inactive
	// both reach this callback exactly once per flip.
	OnUpdate(ctx context.Context, entry sync.ListReposEntry) error

	// OnComplete is called when a DID's repo has been downloaded and
	// Handler.HandleRepo returned nil. Implementations must durably
	// persist StateComplete before returning. commit.Rev is the rev
	// to record as the BackfillRev.
	OnComplete(ctx context.Context, did atmos.DID, commit *repo.Commit) error

	// OnFail is called when the engine exhausts its retry budget for
	// a DID within the current Run. attempts is the total number of
	// download attempts made (initial + retries). Implementations
	// must durably persist StateFailed before returning; a future
	// Run will see StateFailed via Lookup and re-enqueue the DID.
	OnFail(ctx context.Context, did atmos.DID, err error, attempts int) error
}
