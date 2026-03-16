package backfill

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/xrpc"
)

type repoJob struct {
	DID atmos.DID
	Rev string
}

// Engine runs the backfill process.
type Engine struct {
	opts      Options
	completed atomic.Int64
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
// The default transport ([xrpc.NewTransport]) uses MaxIdleConnsPerHost=50,
// which matches the default worker count. For more than 50 workers, configure
// the xrpc.Client's HTTPClient with a transport that has
// MaxIdleConnsPerHost >= workers.
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

	// Producer: enumerate repos in batches, shuffle each batch to break up
	// PDS clustering from relay enumeration order.
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

		batchSize := e.opts.BatchSize.ValOr(1000)
		batch := make([]repoJob, 0, batchSize)

		for entry, err := range e.opts.SyncClient.ListRepos(ctx) {
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

			if len(batch) >= batchSize {
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

// tryRepo downloads and processes a single repo, returning the first error.
func (e *Engine) tryRepo(ctx context.Context, job repoJob, collections map[string]bool) error {
	for rec, err := range e.opts.SyncClient.IterRecords(ctx, job.DID) {
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
