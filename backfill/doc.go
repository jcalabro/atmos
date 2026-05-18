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
// # Extension surface
//
// Two interfaces - Store and Handler - cover the full extension
// surface. Neither is provided by atmos; consumers ship their own
// implementations.
package backfill
