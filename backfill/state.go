package backfill

// State is the per-DID lifecycle value the engine tracks.
type State string

const (
	// StateUnknown is the implicit value of a DID that has never
	// been recorded by the Store. It is never written; it is the
	// "no row exists" return value from Store.Lookup.
	StateUnknown State = ""

	// StateDiscovered means the engine has seen the DID via
	// listRepos at least once and has not yet successfully
	// downloaded its repo. Writes happen via Store.OnDiscover.
	StateDiscovered State = "discovered"

	// StateComplete means the engine has downloaded the DID's repo
	// and Handler.HandleRepo returned nil. Writes happen via
	// Store.OnComplete.
	StateComplete State = "complete"

	// StateFailed means the engine exhausted its retry budget for
	// the DID within some prior Run. A subsequent Run will see this
	// via Store.Lookup and re-enqueue the DID for another attempt.
	// Writes happen via Store.OnFail.
	StateFailed State = "failed"
)
