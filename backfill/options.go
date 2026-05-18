package backfill

import (
	"net/http"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
)

// Options configures the backfill engine.
type Options struct {
	// SyncClient is the listRepos source and the default per-repo
	// download path. Required.
	//
	// The engine has its own retry/backoff loop (see MaxRetries). To
	// avoid compounding retries, the underlying xrpc.Client should
	// have Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})
	// — otherwise a transient 503 will be retried by xrpc *and* by
	// the engine, multiplying the request count against the upstream.
	SyncClient *sync.Client

	// Store persists per-DID lifecycle transitions. Required.
	Store Store

	// Handler receives each fully-downloaded repo. Required.
	Handler Handler

	// Workers is the number of concurrent repo download goroutines.
	// None = 50.
	Workers gt.Option[int]

	// OnError fires when a DID's retry budget is exhausted or its
	// error is non-transient. None = errors are silently dropped
	// (the Store still records StateFailed). The engine logs the
	// error and Store transition independently of OnError.
	OnError gt.Option[func(did atmos.DID, err error)]

	// OnProgress fires after each repo completes, carrying summary
	// stats. Stats.Completed is monotonically non-decreasing across
	// callbacks: the engine serializes the increment with the
	// callback, so two concurrent completions deliver in some order
	// but each callback observes a strictly larger Completed than
	// the previous one. None = no progress callbacks.
	OnProgress gt.Option[func(stats Stats)]

	// MaxRetries is the number of retry attempts for transient
	// errors per repo. The initial attempt is not counted.
	// None = 5. Set to 0 to disable retries.
	MaxRetries gt.Option[int]

	// RetryBaseDelay is the initial backoff before the first retry.
	// Subsequent retries use exponential backoff with jitter.
	// None = 1s.
	RetryBaseDelay gt.Option[time.Duration]

	// RetryMaxDelay caps the backoff between retries. If a server
	// asks for a longer Retry-After than this cap, the engine
	// declines to retry rather than ignoring the server's request.
	// None = 30s.
	RetryMaxDelay gt.Option[time.Duration]

	// Directory enables DID-to-PDS resolution. When set, repos are
	// downloaded directly from the account's PDS rather than the
	// relay, which is dramatically faster. The SyncClient is still
	// used for ListRepos and as fallback when resolution fails.
	// None = all repos via SyncClient (relay).
	//
	// When set, the engine also verifies each repo's commit
	// signature via the resolved DID document.
	Directory gt.Option[*identity.Directory]

	// HTTPClient is shared with per-PDS sync clients created when
	// Directory is set. Should use the same transport as the
	// SyncClient for connection pooling. None = default 30s timeout
	// client.
	HTTPClient gt.Option[*http.Client]

	// ShuffleBatchSize is the number of repos to accumulate before
	// shuffling and dispatching to workers. listRepos returns DIDs
	// roughly in creation order, so small batches cluster on the
	// same few PDS hosts; accumulating more before shuffle spreads
	// work across many more PDSes in parallel. None = 100_000.
	ShuffleBatchSize gt.Option[int]

	// StartCursor is the starting cursor passed to
	// SyncClient.ListRepos. None = "" (start from the beginning).
	// Set this to the value last persisted via OnPageComplete to
	// resume past the last fully-reconciled page from a prior Run.
	StartCursor gt.Option[string]

	// OnPageComplete fires after every entry on a listRepos page has
	// been reconciled (Store.OnDiscover/OnUpdate calls landed) AND
	// every eligible job from that page has been pushed onto the
	// worker channel. The cursor argument is the relay's NextCursor
	// for this page — pass it as StartCursor on the next Run() to
	// skip every entry up through this page.
	//
	// Workers may still be downloading in the background when this
	// fires; that's fine because anything not at StateComplete on
	// restart will be re-dispatched once it's seen again. The
	// guarantee is: any DID the persisted cursor "covers" has been
	// queued at least once.
	//
	// Errors from this callback abort the Run with a wrapped error.
	//
	// None = no callback. Engines that don't need cursor persistence
	// pay no cost.
	OnPageComplete gt.Option[func(cursor string) error]
}

// Stats summarises engine progress, delivered to Options.OnProgress
// after each repo completes. The struct shape exists so future fields
// (Failed, InFlight, etc.) can be added without breaking callers.
type Stats struct {
	// Completed is the running count of DIDs that have transitioned
	// to StateComplete in the current Run.
	Completed int64
}
