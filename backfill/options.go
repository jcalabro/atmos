package backfill

import (
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
	// None = DefaultMaxRetries. Set to 0 to disable retries.
	MaxRetries gt.Option[int]

	// RetryBaseDelay is the initial backoff before the first retry.
	// Subsequent retries use exponential backoff with jitter.
	// None = 1s.
	RetryBaseDelay gt.Option[time.Duration]

	// RetryMaxDelay caps the backoff between non-rate-limit retries
	// (connection errors, 5xx, timeouts). It does NOT cap waits the
	// server itself dictates via a 429 Retry-After / RateLimit-Reset:
	// those are honored up to RetryRateLimitCeiling regardless of this
	// value, because a 429 is backpressure to obey, not a failure to
	// cap. None = 30s.
	RetryMaxDelay gt.Option[time.Duration]

	// RetryRateLimitMaxAttempts is the number of additional download
	// attempts the engine makes for a repo that keeps returning 429
	// (rate limited), on top of the initial attempt. Unlike MaxRetries
	// (which governs ordinary transient errors), a 429 is expected
	// backpressure during a bulk crawl: the engine sleeps for the
	// server-directed Retry-After (capped at RetryRateLimitCeiling)
	// and tries again, rather than failing the repo. Only after this
	// budget is exhausted by continuous 429s does the repo transition
	// to StateFailed (to be retried much later by a higher layer).
	//
	// 429 attempts are counted and budgeted independently of MaxRetries
	// so a rate-limited host cannot consume the ordinary-transient
	// budget and vice versa. None = DefaultRetryRateLimitMaxAttempts.
	// Set to 0 to fail a repo on the first 429 (no rate-limit retries).
	RetryRateLimitMaxAttempts gt.Option[int]

	// Directory enables commit signature verification: when VerifyCommits
	// is true, each downloaded repo's commit signature is checked against
	// the signing key resolved from this Directory. Required when
	// VerifyCommits is true; otherwise unused.
	//
	// It does NOT affect routing. Repos are always downloaded via
	// SyncClient (the relay), which 302-redirects to the account's PDS;
	// the engine does not resolve DID→PDS itself. (This is a deliberate
	// change from earlier versions, where setting Directory also enabled
	// per-DID PLC resolution for direct-PDS routing — that resolution
	// serialized a bulk crawl against the PLC directory and is gone.)
	Directory gt.Option[*identity.Directory]

	// VerifyCommits, when true, makes the engine verify each downloaded
	// repo's commit signature (via Directory) before handing it to the
	// Handler. A verification failure transitions the DID to StateFailed.
	// Requires Directory. None/false = no signature verification (the
	// archive is trusted to the relay/PDS the CAR came from). Verifying
	// every historical commit during a whole-network bulk backfill is
	// expensive; leaving this off is the common choice for bootstrap.
	VerifyCommits gt.Option[bool]

	// BatchSize is the target number of listRepos entries to
	// reconcile before shuffling eligible repos, dispatching them to
	// workers, waiting for those workers to finish, and firing
	// OnBatchComplete. It counts every entry returned by listRepos,
	// including inactive and already-complete repos.
	//
	// listRepos itself is still fetched in pages of 1000, which is the
	// remote protocol cap. Batch boundaries therefore occur only at
	// page boundaries: values below 1000 behave like 1000, and the
	// actual batch size can exceed BatchSize by up to 999 entries.
	//
	// Larger values improve PDS load spreading during full-network
	// backfills because listRepos is roughly creation-ordered and
	// small batches can cluster on the same large PDS hosts.
	// None = 1000.
	BatchSize gt.Option[int]

	// StartCursor is the starting cursor passed to
	// SyncClient.ListRepos. None = "" (start from the beginning).
	// Set this to the value last persisted via OnBatchComplete to
	// resume past the last fully-completed batch from a prior Run.
	StartCursor gt.Option[string]

	// OnBatchComplete fires after a batch has been fully reconciled
	// and every eligible repo scheduled by that batch has reached a
	// terminal state for this Run: StateComplete after a successful
	// handler call, or StateFailed after retry exhaustion or a
	// non-transient error.
	//
	// If the Store cannot persist a terminal state, Run aborts before
	// this callback fires for the affected batch.
	//
	// The cursor argument is the relay's NextCursor for the final
	// listRepos page included in the completed batch. Pass it as
	// StartCursor on the next Run() to skip every entry covered by
	// this batch.
	//
	// Errors from this callback abort the Run with a wrapped error.
	//
	// None = no callback. Engines that don't need cursor persistence
	// pay no cost.
	OnBatchComplete gt.Option[func(cursor string) error]

	// OnPageComplete fires after each listRepos page is fetched and its
	// entries are reconciled. The cursor argument is that page's
	// NextCursor, including the final empty cursor.
	//
	// This callback is intentionally weaker than OnBatchComplete: eligible
	// repos from the page may still be queued for a later batch and may not
	// have reached a terminal state. Use OnBatchComplete for durable
	// per-batch checkpointing. OnPageComplete is kept for callers that need
	// page-level cursor visibility.
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
