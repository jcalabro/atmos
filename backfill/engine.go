package backfill

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
)

// errOnCompleteRecorded is returned from tryRepo when Store.OnComplete
// itself failed. The handler has already run (and its side effects
// landed). Calling Store.OnFail at that point would be incorrect: the
// DID is in a partially-Complete state, not a Failed one.
// processRepo recognizes this sentinel and returns without further
// store transitions.
var errOnCompleteRecorded = errors.New("backfill: OnComplete recording failed; handler already ran")

// ErrEngineAlreadyRan is returned by Run when called a second time on
// the same Engine. Engines are single-shot to avoid silently sharing
// the per-Run progress counter across multiple concurrent or sequential
// Runs. Construct a new Engine to start another pass.
var ErrEngineAlreadyRan = errors.New("backfill: Engine.Run already invoked; engines are single-shot")

// listReposPageLimit is the page size requested from listRepos. The
// XRPC protocol caps this at 1000.
const listReposPageLimit = 1000

// defaultWorkers is the default value of Options.Workers.
const defaultWorkers = 50

// defaultBatchSize is the default value of Options.BatchSize.
const defaultBatchSize = listReposPageLimit

// DefaultMaxRetries is the default value of Options.MaxRetries: the number
// of retry attempts (so total attempts = DefaultMaxRetries + 1) the engine
// makes for an ordinary transient per-DID getRepo failure (connection reset,
// 5xx, timeout) before parking the DID. 429 rate limiting is NOT counted
// here — it has its own, larger budget (see DefaultRetryRateLimitMaxAttempts).
// Backfill is resumable — a DID that exhausts its retries lands in a
// Failed state and is retried on the next pass rather than parking a
// worker through many attempts. 3 retries (4 attempts) keeps the per-DID
// worker-occupancy ceiling bounded while still riding out ordinary transient
// blips.
//
// Exported so callers that bound their own fault budget against the engine
// (e.g. test fault-injection schedules) can key off the real value instead
// of duplicating the literal.
const DefaultMaxRetries = 3

// DefaultRetryRateLimitMaxAttempts is the default value of
// Options.RetryRateLimitMaxAttempts: how many additional attempts the engine
// makes for a repo that keeps returning 429, sleeping for the server-directed
// Retry-After (capped at retryRateLimitCeiling) between each. A 429 is
// expected backpressure during a bulk crawl, not a failure, so this budget is
// deliberately large: only continuous rate limiting across all 20 attempts
// fails the repo. Exported for the same reason as DefaultMaxRetries.
const DefaultRetryRateLimitMaxAttempts = 20

// defaultRetryBaseDelay is the default value of
// Options.RetryBaseDelay.
const defaultRetryBaseDelay = time.Second

// defaultRetryMaxDelay is the default value of Options.RetryMaxDelay.
const defaultRetryMaxDelay = 30 * time.Second

// retryRateLimitCeiling caps how long the engine will sleep for a single
// server-directed 429 Retry-After / RateLimit-Reset. Managed PDS hosts use a
// 300s rate window, so a depleted bucket's reset is commonly up to ~300s out;
// 330s leaves a small margin above that without letting a pathological or
// hostile reset value park a worker indefinitely. A reset further out than
// this is clamped down to it (we still wait, just not forever).
const retryRateLimitCeiling = 330 * time.Second

type retrySleeper interface {
	Sleep(context.Context, time.Duration) error
}

type timerRetrySleeper struct{}

func (timerRetrySleeper) Sleep(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Engine drives the backfill pipeline. Construct with NewEngine and
// drive with Run. Engines are single-shot: a Run() call enumerates
// listRepos to completion and returns; create a new Engine to start
// another pass. A second Run on the same Engine returns
// ErrEngineAlreadyRan.
type Engine struct {
	opts Options

	// retrySleeper is an internal test hook. Production engines use a
	// real timer; tests can observe retry delays without waiting on wall
	// clock time.
	retrySleeper retrySleeper

	// completed counts DIDs transitioned to StateComplete in this Run.
	completed atomic.Int64

	// progressMu serializes the completed.Add+OnProgress callback so
	// successive callbacks observe strictly increasing Stats.Completed.
	// Only acquired when OnProgress is set.
	progressMu sync.Mutex

	// started flips to true on the first Run() to enforce single-shot.
	started atomic.Bool
}

// NewEngine constructs an Engine from opts.
func NewEngine(opts Options) *Engine {
	return &Engine{
		opts: opts,
	}
}

// Run drives the engine to completion. It enumerates listRepos via
// the SyncClient, reconciles each entry against the Store, dispatches
// Discovered/Failed DIDs whose entry.Active is true to workers,
// downloads each one, and records the result via Store.OnComplete or
// Store.OnFail. Run blocks until enumeration drains and all workers
// idle, or ctx is cancelled. On producer error the run-level context
// is cancelled, draining workers promptly.
//
// Returns ErrEngineAlreadyRan if Run is invoked a second time on the
// same Engine.
func (e *Engine) Run(ctx context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return ErrEngineAlreadyRan
	}
	if err := e.validate(); err != nil {
		return err
	}

	// runCtx is cancelled when the producer returns (success or error)
	// so workers stop pulling new jobs and abandon in-flight downloads
	// promptly. Without this, a fatal listRepos error would still let
	// workers drain the buffered channel before exiting.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffer one batch per worker so the producer can stay one batch
	// ahead — when workers are saturated, the producer blocks on
	// channel send rather than spinning.
	jobs := make(chan repoJob, e.workerCount()*2)

	var wg sync.WaitGroup
	for range e.workerCount() {
		wg.Go(func() {
			e.workerLoop(runCtx, jobs)
		})
	}

	var producerErr error
	func() {
		defer close(jobs)
		producerErr = e.producerLoop(runCtx, jobs)
		// On producer error cancel the run context so workers
		// abandon in-flight downloads promptly. On clean producer
		// return we let workers drain the buffered channel — those
		// jobs were already accepted, finishing them is the correct
		// behaviour.
		if producerErr != nil {
			cancel()
		}
	}()

	wg.Wait()
	return producerErr
}

type repoJob struct {
	entry atmossync.ListReposEntry
	done  chan<- error
}

func (e *Engine) validate() error {
	if e.opts.SyncClient == nil {
		return fmt.Errorf("backfill: SyncClient is required")
	}
	if e.opts.Store == nil {
		return fmt.Errorf("backfill: Store is required")
	}
	if e.opts.Handler == nil {
		return fmt.Errorf("backfill: Handler is required")
	}
	if e.opts.VerifyCommits.ValOr(false) && !e.opts.Directory.HasVal() {
		return fmt.Errorf("backfill: Directory is required when VerifyCommits is set")
	}
	return nil
}

func (e *Engine) workerCount() int {
	if e.opts.Workers.HasVal() && e.opts.Workers.Val() > 0 {
		return e.opts.Workers.Val()
	}
	return defaultWorkers
}

func (e *Engine) batchSize() int {
	if e.opts.BatchSize.HasVal() && e.opts.BatchSize.Val() > 0 {
		return e.opts.BatchSize.Val()
	}
	return defaultBatchSize
}

// producerLoop walks the listRepos pages, reconciles every entry
// against the Store, accumulates pages until at least batchSize()
// listRepos entries have been seen, then dispatches eligible DIDs in
// a shuffled batch. It waits for every dispatched job in the batch to
// finish before firing OnBatchComplete, so a persisted cursor only
// covers work that has reached StateComplete or StateFailed.
//
// BatchSize counts all listRepos entries, not only dispatched jobs.
// Boundaries are page-aligned because the relay cursor only resumes
// at page boundaries.
func (e *Engine) producerLoop(ctx context.Context, jobs chan<- repoJob) error {
	startCursor := e.opts.StartCursor.ValOr("")
	batch := make([]repoJob, 0, e.batchSize())
	batchEntries := 0
	batchCursor := ""

	for page, err := range e.opts.SyncClient.ListRepos(ctx, listReposPageLimit, startCursor) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("backfill: listRepos: %w", err)
		}
		batchEntries += len(page.Entries)
		batchCursor = page.NextCursor

		for _, entry := range page.Entries {
			job, dispatch, err := e.reconcile(ctx, entry)
			if err != nil {
				return err
			}
			if !dispatch {
				continue
			}
			batch = append(batch, job)
		}
		if cb := e.opts.OnPageComplete; cb.HasVal() {
			if err := cb.Val()(page.NextCursor); err != nil {
				return fmt.Errorf("backfill: on_page_complete: %w", err)
			}
		}

		if batchEntries >= e.batchSize() {
			if err := e.finishBatch(ctx, jobs, batch, batchCursor); err != nil {
				return err
			}
			batch = batch[:0]
			batchEntries = 0
			batchCursor = ""
		}
	}

	if batchEntries > 0 {
		if err := e.finishBatch(ctx, jobs, batch, batchCursor); err != nil {
			return err
		}
	}
	return nil
}

// reconcile applies the producer-side rules:
//
//   - StateUnknown -> OnDiscover, then dispatch if entry.Active
//   - StateDiscovered/StateFailed -> dispatch if entry.Active;
//     OnUpdate if entry.Active flipped vs Store's recorded value
//   - StateComplete -> never dispatch; OnUpdate if Active flipped
//   - !entry.Active -> never dispatch (regardless of state)
//
// OnDiscover fires unconditionally for Unknown DIDs (active or not),
// so the consumer Store sees every DID the relay knows about. OnUpdate
// fires when a known DID's listRepos.Active value differs from the
// last value the Store persisted, so the Store can track liveness
// flips without polling.
func (e *Engine) reconcile(ctx context.Context, entry atmossync.ListReposEntry) (repoJob, bool, error) {
	rec, err := e.opts.Store.Lookup(ctx, entry.DID)
	if err != nil {
		return repoJob{}, false, fmt.Errorf("backfill: store lookup %s: %w", entry.DID, err)
	}

	if rec.State == StateUnknown {
		if err := e.opts.Store.OnDiscover(ctx, entry); err != nil {
			return repoJob{}, false, fmt.Errorf("backfill: store on_discover %s: %w", entry.DID, err)
		}
	} else if rec.Active != entry.Active {
		if err := e.opts.Store.OnUpdate(ctx, entry); err != nil {
			return repoJob{}, false, fmt.Errorf("backfill: store on_update %s: %w", entry.DID, err)
		}
	}

	if !entry.Active {
		return repoJob{}, false, nil
	}
	if rec.State == StateComplete {
		return repoJob{}, false, nil
	}
	return repoJob{entry: entry}, true, nil
}

// finishBatch shuffles and pushes each job onto the workers channel,
// waits for every pushed job to finish, then invokes OnBatchComplete.
func (e *Engine) finishBatch(ctx context.Context, jobs chan<- repoJob, batch []repoJob, cursor string) error {
	if err := e.dispatchBatch(ctx, jobs, batch); err != nil {
		return err
	}
	if cb := e.opts.OnBatchComplete; cb.HasVal() {
		if err := cb.Val()(cursor); err != nil {
			return fmt.Errorf("backfill: on_batch_complete: %w", err)
		}
	}
	return nil
}

// dispatchBatch shuffles, pushes each job onto the workers channel,
// and waits for each pushed job to finish.
func (e *Engine) dispatchBatch(ctx context.Context, jobs chan<- repoJob, batch []repoJob) error {
	rand.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
	done := make(chan error, len(batch))
	for _, job := range batch {
		job.done = done
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- job:
		}
	}
	for range len(batch) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// workerLoop pulls jobs off the channel and processes each via
// processRepo. The channel close (signalled by the producer when
// enumeration drains or fails) is what unwinds workers.
func (e *Engine) workerLoop(ctx context.Context, jobs <-chan repoJob) {
	for job := range jobs {
		if ctx.Err() != nil {
			return
		}
		err := e.processRepo(ctx, job)
		if job.done != nil {
			job.done <- err
		}
	}
}

// processRepo runs the retry/backoff loop around tryRepo. Nil means
// the final outcome was durably reported via Store.OnComplete
// (success path inside tryRepo) or Store.OnFail (here, after retries
// exhaust or the error is non-transient). A non-nil error means the
// job did not reach a persisted terminal state and the batch must not
// checkpoint.
func (e *Engine) processRepo(ctx context.Context, job repoJob) error {
	maxRetries := e.opts.MaxRetries.ValOr(DefaultMaxRetries)
	rlMaxAttempts := e.opts.RetryRateLimitMaxAttempts.ValOr(DefaultRetryRateLimitMaxAttempts)
	baseDelay := e.opts.RetryBaseDelay.ValOr(defaultRetryBaseDelay)
	maxDelay := e.opts.RetryMaxDelay.ValOr(defaultRetryMaxDelay)

	// Two independent budgets. transientAttempt counts ordinary
	// transient failures (conn reset, 5xx, timeout) toward maxRetries.
	// rlAttempt counts 429 rate-limit responses toward rlMaxAttempts.
	// A 429 is expected backpressure during a bulk crawl — we sleep for
	// the server-directed reset and try again, rather than spending the
	// (small) transient budget on it. attempts is the running total of
	// download attempts, reported to OnFail.
	transientAttempt := 0
	rlAttempt := 0
	attempts := 0

	for {
		err := e.tryRepo(ctx, job)
		attempts++
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errOnCompleteRecorded) {
			return err
		}

		host := hostFromErr(err)

		var delay time.Duration
		if xrpc.IsRateLimited(err) {
			// 429: honor the server's Retry-After/RateLimit-Reset.
			// Never fail for "the reset is too far out" — sleeping is
			// the correct response to backpressure. Only give up after
			// the dedicated rate-limit budget is exhausted by
			// continuous 429s.
			if rlAttempt >= rlMaxAttempts {
				return e.recordFail(ctx, job.entry.DID, host, fmt.Errorf("backfill: still rate limited after %d attempts: %w", rlAttempt+1, err), attempts)
			}
			rlAttempt++
			delay = rateLimitDelay(err, baseDelay, rlAttempt)
		} else {
			// Ordinary transient error (or a permanent one). Permanent
			// errors and an exhausted transient budget both fail now.
			if !xrpc.IsTransient(err) || transientAttempt >= maxRetries {
				return e.recordFail(ctx, job.entry.DID, host, err, attempts)
			}
			delay = backoffDelay(baseDelay, maxDelay, transientAttempt)
			transientAttempt++
		}

		if err := e.sleep(ctx, delay); err != nil {
			return err
		}
	}
}

func (e *Engine) sleep(ctx context.Context, delay time.Duration) error {
	sleeper := e.retrySleeper
	if sleeper == nil {
		sleeper = timerRetrySleeper{}
	}
	return sleeper.Sleep(ctx, delay)
}

// hostFromErr extracts the host an xrpc error came from (the
// post-redirect host the request landed on), or "" if err is not an
// xrpc.Error (e.g. a dial failure that never reached a server).
func hostFromErr(err error) string {
	var xerr *xrpc.Error
	if errors.As(err, &xerr) {
		return xerr.Host
	}
	return ""
}

// rateLimitDelay returns how long to wait before retrying a 429. It
// honors the server-directed reset time when present (clamped to
// retryRateLimitCeiling so a pathological reset can't park a worker
// forever), and otherwise falls back to exponential backoff on
// baseDelay. A small floor keeps a missing/zero reset from busy-looping.
func rateLimitDelay(err error, baseDelay time.Duration, rlAttempt int) time.Duration {
	if ra := xrpc.RetryAfter(err); !ra.IsZero() {
		if wait := time.Until(ra); wait > 0 {
			if wait > retryRateLimitCeiling {
				wait = retryRateLimitCeiling
			}
			return wait
		}
	}
	// No usable reset header: back off exponentially on baseDelay,
	// capped at the ceiling. rlAttempt is 1-based here.
	delay := backoffDelay(baseDelay, retryRateLimitCeiling, rlAttempt-1)
	if delay < baseDelay {
		delay = baseDelay
	}
	return delay
}

// backoffDelay returns base * 2^attempt, capped at maxDelay, with
// jitter of up to 50% of the base delay added on top. Saturates
// instead of overflowing when attempt is large.
func backoffDelay(base, maxDelay time.Duration, attempt int) time.Duration {
	delay := maxDelay
	// math/bits.LeadingZeros64 gives us a quick saturation check:
	// base << attempt overflows when attempt >= leading zeros of base.
	if base > 0 && attempt < bits.LeadingZeros64(uint64(base)) {
		shifted := base << attempt
		if shifted < maxDelay {
			delay = shifted
		}
	}
	if half := int64(delay) / 2; half > 0 {
		delay += time.Duration(rand.Int64N(half))
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

// recordFail reports a final failure: fires OnError, persists via
// Store.OnFail, and surfaces a Store.OnFail error via OnError too. host
// is the server the failing request was sent to (post-redirect), or ""
// if no response was received. It returns a non-nil error only when
// Store.OnFail could not persist the terminal state.
func (e *Engine) recordFail(ctx context.Context, did atmos.DID, host string, err error, attempts int) error {
	if onErr := e.opts.OnError; onErr.HasVal() {
		onErr.Val()(did, err)
	}
	if storeErr := e.opts.Store.OnFail(ctx, did, host, err, attempts); storeErr != nil {
		// The Store rejected the failure write. There's no good
		// recovery — surface via OnError so the operator at least
		// sees it.
		if onErr := e.opts.OnError; onErr.HasVal() {
			onErr.Val()(did, fmt.Errorf("backfill: store on_fail: %w", storeErr))
		}
		return fmt.Errorf("backfill: store on_fail %s: %w", did, storeErr)
	}
	return nil
}

// tryRepo executes a single download+parse+handle attempt. On nil
// return, the DID is transitioned to Complete via Store.OnComplete.
// Errors flow back to processRepo for retry handling.
//
// The repo is always downloaded via the relay SyncClient, which
// 302-redirects to the account's PDS; the engine does not resolve
// DID→PDS itself. The host the CAR actually came from (the post-redirect
// host) is threaded into Store.OnComplete for per-host attribution.
func (e *Engine) tryRepo(ctx context.Context, job repoJob) error {
	body, host, err := e.opts.SyncClient.GetRepoStreamHost(ctx, job.entry.DID, "")
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	rp, commit, err := atmosrepo.LoadFromCAR(bufio.NewReader(body))
	if err != nil {
		return err
	}

	if e.opts.VerifyCommits.ValOr(false) {
		if err := atmossync.VerifyCommitWithDirectory(ctx, e.opts.Directory.ValOr(nil), commit); err != nil {
			return err
		}
	}

	if err := e.opts.Handler.HandleRepo(ctx, job.entry.DID, rp, commit); err != nil {
		return err
	}

	if err := e.opts.Store.OnComplete(ctx, job.entry.DID, host, commit); err != nil {
		// Treat OnComplete failure as a hard error: the handler has
		// already had its side effects but the durability marker
		// failed. Surface via OnError. Do NOT call OnFail here —
		// that would conflict with the partially-Complete state.
		if onErr := e.opts.OnError; onErr.HasVal() {
			onErr.Val()(job.entry.DID, fmt.Errorf("backfill: store on_complete: %w", err))
		}
		return errOnCompleteRecorded
	}

	e.notifyProgress()
	return nil
}

// notifyProgress increments the completed counter and fires
// OnProgress under a lock so successive callbacks see monotonically
// increasing Stats.Completed.
func (e *Engine) notifyProgress() {
	if !e.opts.OnProgress.HasVal() {
		e.completed.Add(1)
		return
	}
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	n := e.completed.Add(1)
	e.opts.OnProgress.Val()(Stats{Completed: n})
}
