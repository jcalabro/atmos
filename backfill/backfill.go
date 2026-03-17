// Package backfill provides a concurrent engine for downloading and processing
// all repositories from an ATProto relay or PDS.
package backfill

import (
	"context"
	"net/http"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
)

// Handler processes records during backfill. Must be safe for concurrent use.
type Handler interface {
	HandleRecord(ctx context.Context, did atmos.DID, rec sync.Record) error
}

// HandlerFunc is an adapter to allow ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, did atmos.DID, rec sync.Record) error

// HandleRecord calls f(ctx, did, rec).
func (f HandlerFunc) HandleRecord(ctx context.Context, did atmos.DID, rec sync.Record) error {
	return f(ctx, did, rec)
}

// Checkpoint stores durable progress for crash recovery.
// All methods must be safe for concurrent use.
type Checkpoint interface {
	// LoadCursor returns the last saved pagination cursor, or "" if none.
	LoadCursor(ctx context.Context) (string, error)
	// SaveCursor persists the pagination cursor for crash recovery.
	SaveCursor(ctx context.Context, cursor string) error
	// IsComplete reports whether the given repo has already been processed.
	IsComplete(ctx context.Context, did atmos.DID) (bool, error)
	// MarkComplete records that the repo has been fully processed at the given rev.
	MarkComplete(ctx context.Context, did atmos.DID, rev string) error
}

// Options configures the backfill engine.
type Options struct {
	SyncClient *sync.Client // required
	Handler    Handler      // required

	// Workers is the number of concurrent repo download goroutines.
	// None = 50.
	Workers gt.Option[int]

	// Checkpoint enables crash recovery. None = no checkpointing.
	Checkpoint gt.Option[Checkpoint]

	// OnError is called when a repo fails. None = errors silently skipped.
	OnError gt.Option[func(did atmos.DID, err error)]

	// OnProgress is called after each repo completes with total count.
	OnProgress gt.Option[func(completed int64)]

	// Collections filters to only yield records from these collections.
	// None = all collections.
	Collections gt.Option[[]string]

	// BatchSize is the number of repos to collect before shuffling and
	// dispatching to workers. Shuffling breaks up PDS clustering from relay
	// enumeration order, spreading worker load across many PDS hosts.
	// None = 1000.
	BatchSize gt.Option[int]

	// MaxRetries is the number of retry attempts for transient errors
	// (429, 5xx, timeouts, connection resets) per repo. The initial
	// attempt is not counted. None = 5. Set to 0 to disable retries.
	//
	// Note: xrpc.Client has its own retry logic (default 3 attempts).
	// To avoid compounding retries, set xrpc.RetryPolicy{MaxAttempts: 1}
	// on the xrpc.Client used by the SyncClient, and let the backfill
	// engine handle all retry logic.
	MaxRetries gt.Option[int]

	// RetryBaseDelay is the initial backoff duration before the first retry.
	// Subsequent retries use exponential backoff with jitter.
	// None = 1s.
	RetryBaseDelay gt.Option[time.Duration]

	// RetryMaxDelay is the maximum backoff duration between retries.
	// None = 30s.
	RetryMaxDelay gt.Option[time.Duration]

	// Directory enables DID-to-PDS resolution. When set, repos are
	// downloaded directly from the account's PDS instead of through the
	// relay, dramatically improving throughput. The SyncClient is still
	// used for ListRepos and as fallback when resolution fails.
	// None = all repos via SyncClient (relay).
	Directory gt.Option[*identity.Directory]

	// HTTPClient is shared with per-PDS sync clients created when
	// Directory is set. Should use the same transport as the SyncClient
	// for connection pooling. None = default 30s timeout client.
	HTTPClient gt.Option[*http.Client]
}
