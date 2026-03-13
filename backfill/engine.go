package backfill

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/jcalabro/atmos"
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
// For optimal performance with many workers, configure the xrpc.Client's
// HTTPClient with a Transport that has MaxIdleConnsPerHost >= workers.
// The default http.Transport only keeps 2 idle connections per host.
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

	// Producer: enumerate repos.
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

		count := 0
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

			select {
			case jobs <- repoJob{DID: entry.DID, Rev: entry.Rev}:
			case <-ctx.Done():
				producerErr = ctx.Err()
				return
			}

			cursor = string(entry.DID)
			count++
			if e.opts.Checkpoint.HasVal() && count%500 == 0 {
				_ = e.opts.Checkpoint.Val().SaveCursor(ctx, cursor)
			}
		}

		if e.opts.Checkpoint.HasVal() && cursor != "" {
			_ = e.opts.Checkpoint.Val().SaveCursor(ctx, cursor)
		}
	}()

	wg.Wait()
	return producerErr
}

func (e *Engine) processRepo(ctx context.Context, job repoJob, collections map[string]bool) {
	for rec, err := range e.opts.SyncClient.IterRecords(ctx, job.DID) {
		if err != nil {
			if e.opts.OnError.HasVal() {
				e.opts.OnError.Val()(job.DID, err)
			}
			return
		}

		if collections != nil && !collections[rec.Collection] {
			continue
		}

		if err := e.opts.Handler.HandleRecord(ctx, job.DID, rec); err != nil {
			if e.opts.OnError.HasVal() {
				e.opts.OnError.Val()(job.DID, err)
			}
			return
		}
	}

	if e.opts.Checkpoint.HasVal() {
		_ = e.opts.Checkpoint.Val().MarkComplete(ctx, job.DID, job.Rev)
	}

	n := e.completed.Add(1)
	if e.opts.OnProgress.HasVal() {
		e.opts.OnProgress.Val()(n)
	}
}
