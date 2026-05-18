package backfill

import (
	"context"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
)

// Handler receives each fully-downloaded repo. Implementations must be
// safe for concurrent use: the engine calls HandleRepo from its worker
// pool.
//
// By the time HandleRepo fires, the engine has already:
//   - downloaded the full CAR stream from the relay or the DID's PDS
//   - parsed it via repo.LoadFromCAR
//   - if Options.Directory was set, verified the commit signature
//
// The handler decides what to do with the repo. AppView indexers walk
// repo.Tree and persist records; archive consumers may simply read
// commit metadata.
//
// A nil return advances the DID to StateComplete via Store.OnComplete,
// passing commit.Rev as the BackfillRev. A non-nil return feeds the
// engine's retry/backoff path; if retries are exhausted the DID
// transitions to StateFailed via Store.OnFail.
//
// The handler owns r and commit only for the duration of the call.
// The engine does not reuse the underlying buffers across calls
// today, but consumers should copy anything they retain past return
// in case that changes.
//
// Side effects from a partially-completed handler call (e.g. records
// already written to a downstream store before a later return error)
// are the handler's responsibility to clean up. The engine does not
// roll back handler effects.
type Handler interface {
	HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error
}

// HandlerFunc is an adapter to allow ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error

// HandleRepo calls f(ctx, did, r, commit).
func (f HandlerFunc) HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	return f(ctx, did, r, commit)
}
