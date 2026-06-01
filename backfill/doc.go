// Package backfill drives bulk enumeration and download of repos from
// an atproto relay or PDS.
//
// # Lifecycle
//
// The engine assigns every DID it encounters to one of four states.
// Transitions are driven by the engine and persisted by a
// caller-supplied Store:
//
//	Unknown ──OnDiscover──> Discovered ──OnComplete──> Complete
//	                            │
//	                            └──OnFail──> Failed ──(re-run)──> Discovered/Complete
//
// On Run() the engine paginates listRepos, calls Store.Lookup for
// each entry, dispatches Discovered/Failed DIDs whose entry.Active is
// true, and skips Complete/inactive DIDs from download. Workers
// download each dispatched DID's repo, parse it, optionally verify
// the commit signature, and invoke Handler.HandleRepo.
//
// # Active-flip tracking
//
// Each Store row also records the last entry.Active value the engine
// observed. When listRepos reports a value that differs from what
// the Store has on file, the engine fires Store.OnUpdate. This lets
// AppViews track tombstoning (active→inactive) and revival
// (inactive→active) as it happens, without polling. To distinguish
// "discovered but not yet backfilled" rows from inactive accounts
// stuck at Discovered, query `State == Discovered AND Active == true`.
//
// # Single-shot
//
// Engines are single-shot: a Run() call enumerates listRepos to
// completion and returns. A second Run() returns ErrEngineAlreadyRan.
// Construct a new Engine to start another pass.
//
// # Resume across Runs
//
// By default each Run() walks listRepos from the beginning. To resume
// from a prior Run's progress, set Options.StartCursor to the cursor
// last persisted via Options.OnBatchComplete.
//
// Cursor advancement granularity is controlled by Options.BatchSize.
// BatchSize counts every listRepos entry, including inactive and
// already-complete repos. The engine still fetches listRepos in pages
// of 1000 (the remote protocol cap), so batch boundaries are aligned
// to page boundaries. When OnBatchComplete fires, every eligible DID
// covered by that batch has reached StateComplete or StateFailed for
// this Run. If the Store cannot persist one of those terminal states,
// Run aborts before advancing the cursor. A new Run with that cursor
// starts at the page after the saved one.
//
// The cursor is opaque; treat it as a string. Persist it durably
// (e.g., in your Store's underlying database) before returning from
// the OnBatchComplete callback if you want crash-after-this-batch to
// skip the same work on restart.
//
// # Extension surface
//
// Two interfaces - Store and Handler - cover the full extension
// surface. Neither is provided by atmos; consumers ship their own
// implementations.
package backfill
