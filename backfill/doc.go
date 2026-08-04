// Package backfill drives fleet-wide repository enumeration and direct-PDS
// downloads from an atproto relay's listHosts roster.
//
// # Lifecycle
//
// The engine assigns every DID it encounters to one of four states.
// Transitions are driven by the engine and persisted by a
// caller-supplied Store:
//
//	Unknown ──OnDiscover──> Discovered ──OnComplete──> Complete
//	                            │                       ▲
//	                            └──OnFail──> Failed ────┘ (later Run)
//
// On Run() the engine paginates listHosts on the relay, then runs bounded,
// independent listRepos pipelines against each eligible PDS. It dispatches
// Discovered/Failed active DIDs for direct getRepo download and skips
// Complete or inactive DIDs. Fleet-wide download slots bound aggregate I/O;
// per-host workers and backoff prevent one unhealthy PDS from starving the
// rest of the roster. Run proves termination with a final listHosts pass;
// producer failures that exhaust their host budget are reported but do not
// block healthy hosts from draining.
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
// Each PDS has an independent opaque listRepos cursor owned by Store.
// Cursor advancement granularity is controlled by Options.BatchSize and is
// page-aligned. SaveHostCursor is called only after every eligible DID covered
// by the checkpoint reached OnComplete or OnFail. Store implementations must
// order cursor durability after any data those terminal callbacks describe;
// a lagging cursor is safe because restart re-lists and Lookup deduplicates.
//
// # Extension surface
//
// Two interfaces - Store and Handler - cover the full extension
// surface. Neither is provided by atmos; consumers ship their own
// implementations.
package backfill
