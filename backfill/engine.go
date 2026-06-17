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
	"github.com/jcalabro/gt"
	"github.com/puzpuzpuz/xsync/v4"
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
// the per-Run progress counter and pdsClients pool across multiple
// concurrent or sequential Runs. Construct a new Engine to start
// another pass.
var ErrEngineAlreadyRan = errors.New("backfill: Engine.Run already invoked; engines are single-shot")

// listReposPageLimit is the page size requested from listRepos. The
// XRPC protocol caps this at 1000.
const listReposPageLimit = 1000

// defaultWorkers is the default value of Options.Workers.
const defaultWorkers = 50

// defaultBatchSize is the default value of Options.BatchSize.
const defaultBatchSize = listReposPageLimit

// defaultMaxRetries is the default value of Options.MaxRetries.
// Backfill is resumable — a DID that exhausts its retries lands in a
// Failed state and is retried on the next pass rather than parking a
// worker through six attempts (~5 min worst case for a TTFB-stalling
// host). 3 retries (4 attempts) keeps the per-DID worker-occupancy
// ceiling bounded while still riding out ordinary transient blips.
const defaultMaxRetries = 3

// defaultRetryBaseDelay is the default value of
// Options.RetryBaseDelay.
const defaultRetryBaseDelay = time.Second

// defaultRetryMaxDelay is the default value of Options.RetryMaxDelay.
const defaultRetryMaxDelay = 30 * time.Second

// Engine drives the backfill pipeline. Construct with NewEngine and
// drive with Run. Engines are single-shot: a Run() call enumerates
// listRepos to completion and returns; create a new Engine to start
// another pass. A second Run on the same Engine returns
// ErrEngineAlreadyRan.
type Engine struct {
	opts Options

	// pdsClients pools per-PDS sync clients when Options.Directory is
	// set, keyed by the PDS endpoint URL. Lazily populated by
	// syncClientForRepo.
	pdsClients *xsync.Map[string, *atmossync.Client]

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
		opts:       opts,
		pdsClients: xsync.NewMap[string, *atmossync.Client](),
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
	maxRetries := e.opts.MaxRetries.ValOr(defaultMaxRetries)
	baseDelay := e.opts.RetryBaseDelay.ValOr(defaultRetryBaseDelay)
	maxDelay := e.opts.RetryMaxDelay.ValOr(defaultRetryMaxDelay)

	for attempt := range maxRetries + 1 {
		err := e.tryRepo(ctx, job)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errOnCompleteRecorded) {
			return err
		}

		if !xrpc.IsTransient(err) || attempt >= maxRetries {
			return e.recordFail(ctx, job.entry.DID, err, attempt+1)
		}

		delay := backoffDelay(baseDelay, maxDelay, attempt)
		// Honor server-side Retry-After when it asks for longer than
		// our own backoff. If the server wants more than maxDelay,
		// give up rather than ignoring its request and hammering it.
		if ra := xrpc.RetryAfter(err); !ra.IsZero() {
			wait := time.Until(ra)
			if wait > maxDelay {
				return e.recordFail(ctx, job.entry.DID, fmt.Errorf("server requested %s delay exceeds RetryMaxDelay %s: %w", wait, maxDelay, err), attempt+1)
			}
			if wait > delay {
				delay = wait
			}
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
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
// Store.OnFail, and surfaces a Store.OnFail error via OnError too. It
// returns a non-nil error only when Store.OnFail could not persist the
// terminal state.
func (e *Engine) recordFail(ctx context.Context, did atmos.DID, err error, attempts int) error {
	if onErr := e.opts.OnError; onErr.HasVal() {
		onErr.Val()(did, err)
	}
	if storeErr := e.opts.Store.OnFail(ctx, did, err, attempts); storeErr != nil {
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
func (e *Engine) tryRepo(ctx context.Context, job repoJob) error {
	sc := e.syncClientForRepo(ctx, job.entry.DID)

	body, err := sc.GetRepoStream(ctx, job.entry.DID, "")
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	rp, commit, err := atmosrepo.LoadFromCAR(bufio.NewReader(body))
	if err != nil {
		return err
	}

	if e.opts.Directory.HasVal() {
		if err := sc.VerifyCommit(ctx, commit); err != nil {
			return err
		}
	}

	if err := e.opts.Handler.HandleRepo(ctx, job.entry.DID, rp, commit); err != nil {
		return err
	}

	if err := e.opts.Store.OnComplete(ctx, job.entry.DID, commit); err != nil {
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

// syncClientForRepo returns a sync client for the given DID. If a
// Directory is configured, attempt to resolve to the DID's PDS; on
// success, use a per-PDS pooled client. On any failure, fall back to
// the relay SyncClient.
//
// We use the Directory's Resolver directly (single PLC HTTP GET)
// rather than the full LookupDID, because the latter does
// bi-directional handle verification which is far too slow for bulk
// backfill.
//
// Pooled clients are constructed with MaxAttempts=1 so the engine's
// retry loop is the only retry source — preventing xrpc and the
// engine from compounding retries against a slow PDS.
func (e *Engine) syncClientForRepo(ctx context.Context, did atmos.DID) *atmossync.Client {
	if !e.opts.Directory.HasVal() {
		return e.opts.SyncClient
	}

	doc, err := e.opts.Directory.Val().Resolver.ResolveDID(ctx, did)
	if err != nil {
		return e.opts.SyncClient
	}

	var pds string
	for _, svc := range doc.Service {
		if svc.Type == "AtprotoPersonalDataServer" {
			pds = svc.ServiceEndpoint
			break
		}
	}
	if pds == "" {
		return e.opts.SyncClient
	}

	if sc, ok := e.pdsClients.Load(pds); ok {
		return sc
	}
	xc := &xrpc.Client{
		Host:       pds,
		HTTPClient: e.opts.HTTPClient,
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{
		Client:    xc,
		Directory: e.opts.Directory,
	})
	actual, _ := e.pdsClients.LoadOrStore(pds, sc)
	return actual
}
