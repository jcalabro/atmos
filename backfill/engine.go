package backfill

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	atmosSync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
)

type repoJob struct {
	DID atmos.DID
	Rev string
}

// Engine runs the backfill process.
type Engine struct {
	opts       Options
	completed  atomic.Int64
	pdsClients sync.Map // map[string]*sync.Client — pooled by PDS host URL
}

// NewEngine creates a new backfill engine.
func NewEngine(opts Options) *Engine {
	return &Engine{opts: opts}
}

// Completed returns the number of repos processed so far.
func (e *Engine) Completed() int64 {
	return e.completed.Load()
}

// Run enumerates all repos and processes each with bounded concurrency.
// Blocks until complete or ctx is cancelled.
//
// The default client ([xrpc.NewHTTPClient], backed by jttp) uses
// MaxIdleConnsPerHost=50, which matches the default worker count. For more
// than 50 workers, configure the xrpc.Client's HTTPClient with a jttp client
// that has jttp.WithMaxIdleConnsPerHost(workers) or higher.
func (e *Engine) Run(ctx context.Context) error {
	workers := 50
	if e.opts.Workers.HasVal() && e.opts.Workers.Val() > 0 {
		workers = e.opts.Workers.Val()
	}

	// Build collection filter once.
	var collections map[string]bool
	if e.opts.Collections.HasVal() {
		cols := e.opts.Collections.Val()
		collections = make(map[string]bool, len(cols))
		for _, c := range cols {
			collections[c] = true
		}
	}

	jobs := make(chan repoJob, workers*2)

	var wg sync.WaitGroup

	// Workers.
	for range workers {
		wg.Go(func() {
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				e.processRepo(ctx, job, collections)
			}
		})
	}

	// Producer: enumerate repos, accumulate a large batch, shuffle to break
	// up PDS clustering from relay enumeration order, then dispatch.
	var producerErr error
	func() {
		defer close(jobs)

		var cursor string
		if e.opts.Checkpoint.HasVal() {
			c, err := e.opts.Checkpoint.Val().LoadCursor(ctx)
			if err != nil {
				producerErr = err
				return
			}
			cursor = c
		}

		shuffleSize := e.opts.ShuffleBatchSize.ValOr(100_000)
		batch := make([]repoJob, 0, shuffleSize)

		// listRepos page size is always 1000 (the protocol maximum).
		const listReposPageLimit = 1000

		for entry, err := range e.opts.SyncClient.ListRepos(ctx, listReposPageLimit) {
			if ctx.Err() != nil {
				producerErr = ctx.Err()
				return
			}
			if err != nil {
				if e.opts.OnError.HasVal() {
					e.opts.OnError.Val()(atmos.DID(""), err)
				}
				continue
			}

			if !entry.Active {
				continue
			}

			if e.opts.Checkpoint.HasVal() {
				done, err := e.opts.Checkpoint.Val().IsComplete(ctx, entry.DID)
				if err != nil {
					producerErr = err
					return
				}
				if done {
					continue
				}
			}

			batch = append(batch, repoJob{DID: entry.DID, Rev: entry.Rev})
			cursor = string(entry.DID)

			if len(batch) >= shuffleSize {
				if err := e.dispatchBatch(ctx, jobs, batch, cursor); err != nil {
					producerErr = err
					return
				}
				batch = batch[:0]
			}
		}

		// Flush remaining partial batch.
		if len(batch) > 0 {
			if err := e.dispatchBatch(ctx, jobs, batch, cursor); err != nil {
				producerErr = err
				return
			}
		}
	}()

	wg.Wait()
	return producerErr
}

// dispatchBatch shuffles the batch and sends each job to the workers channel,
// then saves the checkpoint cursor.
func (e *Engine) dispatchBatch(ctx context.Context, jobs chan<- repoJob, batch []repoJob, cursor string) error {
	rand.Shuffle(len(batch), func(i, j int) {
		batch[i], batch[j] = batch[j], batch[i]
	})

	for _, job := range batch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- job:
		}
	}

	if e.opts.Checkpoint.HasVal() && cursor != "" {
		_ = e.opts.Checkpoint.Val().SaveCursor(ctx, cursor)
	}

	return nil
}

func (e *Engine) processRepo(ctx context.Context, job repoJob, collections map[string]bool) {
	maxRetries := e.opts.MaxRetries.ValOr(5)
	baseDelay := e.opts.RetryBaseDelay.ValOr(time.Second)
	maxDelay := e.opts.RetryMaxDelay.ValOr(30 * time.Second)

	for attempt := range maxRetries + 1 {
		err := e.tryRepo(ctx, job, collections)
		if err == nil {
			break
		}

		if ctx.Err() != nil {
			return
		}

		if !xrpc.IsTransient(err) || attempt >= maxRetries {
			if e.opts.OnError.HasVal() {
				e.opts.OnError.Val()(job.DID, err)
			}
			return
		}

		// Exponential backoff with jitter.
		delay := baseDelay << attempt
		if delay <= 0 || delay > maxDelay {
			delay = maxDelay
		}
		if half := int64(delay) / 2; half > 0 {
			delay += time.Duration(rand.Int64N(half))
		}

		// Respect Retry-After if available and reasonable.
		// Use as a floor — never sleep less than the server requested.
		if ra := xrpc.RetryAfter(err); !ra.IsZero() {
			if wait := time.Until(ra); wait > delay && wait < maxDelay {
				delay = wait
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// syncClientForRepo returns a sync client for downloading the given repo.
// If a Directory is configured, the DID is resolved to its PDS endpoint and
// a per-PDS client is used. Falls back to the relay SyncClient on failure.
//
// Uses the Directory's Resolver directly (single PLC HTTP GET) instead of
// the full LookupDID which does bi-directional handle verification — that
// requires connecting to every user's handle server and is far too slow for
// bulk backfill.
func (e *Engine) syncClientForRepo(ctx context.Context, did atmos.DID) *atmosSync.Client {
	if !e.opts.Directory.HasVal() {
		return e.opts.SyncClient
	}

	doc, err := e.opts.Directory.Val().Resolver.ResolveDID(ctx, did)
	if err != nil {
		return e.opts.SyncClient
	}

	// Extract PDS endpoint from the DID document's service list.
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

	if v, ok := e.pdsClients.Load(pds); ok {
		sc, _ := v.(*atmosSync.Client)
		return sc
	}

	xc := &xrpc.Client{Host: pds, HTTPClient: e.opts.HTTPClient}
	sc := atmosSync.NewClient(atmosSync.Options{Client: xc})
	actual, _ := e.pdsClients.LoadOrStore(pds, sc)
	stored, _ := actual.(*atmosSync.Client)
	return stored
}

// tryRepo downloads and processes a single repo, returning the first error.
func (e *Engine) tryRepo(ctx context.Context, job repoJob, collections map[string]bool) error {
	sc := e.syncClientForRepo(ctx, job.DID)
	for rec, err := range sc.IterRecords(ctx, job.DID) {
		if err != nil {
			return err
		}

		if collections != nil && !collections[rec.Collection] {
			continue
		}

		if err := e.opts.Handler.HandleRecord(ctx, job.DID, rec); err != nil {
			return err
		}
	}

	if e.opts.Checkpoint.HasVal() {
		_ = e.opts.Checkpoint.Val().MarkComplete(ctx, job.DID, job.Rev)
	}

	n := e.completed.Add(1)
	if e.opts.OnProgress.HasVal() {
		e.opts.OnProgress.Val()(n)
	}

	return nil
}
