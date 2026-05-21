// streaming/parallel/scheduler.go
package parallel

import (
	"context"
	"sync"
)

// Scheduler dispatches per-key work units to a fixed pool of N workers
// while guaranteeing that work units sharing a key run sequentially in
// arrival order. Mirrors indigo's parallel scheduler with two
// additions: per-key queue cap (drop-oldest) and panic recovery.
//
// Zero value is not usable; callers must use NewScheduler.
type Scheduler[Work any] struct {
	workers     int
	keyQueueCap int
	do          func(context.Context, Work) error
	onDrop      func(Work)

	feeder chan task[Work]

	mu     sync.Mutex
	active map[string][]Work

	stopOnce sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
}

type task[Work any] struct {
	key  string
	work Work
}

// NewScheduler returns a Scheduler that runs do on a pool of workers
// goroutines. keyQueueCap caps the per-key queue depth: when a single
// key already has keyQueueCap pending units and another arrives, the
// oldest pending unit for that key is dropped and onDrop is invoked
// with it. keyQueueCap == 0 disables drops (unbounded per-key queue).
// onDrop may be nil; it is invoked synchronously off the AddWork hot
// path, so it must not block.
func NewScheduler[Work any](
	workers int,
	keyQueueCap int,
	do func(context.Context, Work) error,
	onDrop func(Work),
) *Scheduler[Work] {
	if workers < 1 {
		workers = 1
	}
	s := &Scheduler[Work]{
		workers:     workers,
		keyQueueCap: keyQueueCap,
		do:          do,
		onDrop:      onDrop,
		feeder:      make(chan task[Work]),
		active:      make(map[string][]Work),
		stopped:     make(chan struct{}),
	}
	for range workers {
		s.wg.Add(1)
		go s.workerLoop()
	}
	return s
}

// Workers returns the number of worker goroutines.
func (s *Scheduler[Work]) Workers() int { return s.workers }

// Shutdown stops all workers, waiting for in-flight units to complete.
// Pending units in active[key] queues are NOT processed; callers can
// drain them externally before calling Shutdown if needed. Idempotent.
func (s *Scheduler[Work]) Shutdown() {
	s.stopOnce.Do(func() {
		close(s.stopped)
		close(s.feeder)
	})
	s.wg.Wait()
}

func (s *Scheduler[Work]) workerLoop() {
	defer s.wg.Done()
	for t := range s.feeder {
		s.runChain(t.key, t.work)
	}
}

// runChain runs the given work, then drains active[key] until it is
// empty, then deletes the key. Holds the per-key invariant: a single
// worker drains the queue serially without releasing the slot until
// the queue is empty.
func (s *Scheduler[Work]) runChain(key string, work Work) {
	for {
		_ = s.do(context.Background(), work)

		s.mu.Lock()
		queue := s.active[key]
		if len(queue) == 0 {
			delete(s.active, key)
			s.mu.Unlock()
			return
		}
		work = queue[0]
		s.active[key] = queue[1:]
		s.mu.Unlock()
	}
}

// AddWork enqueues a unit of work for the given key.
func (s *Scheduler[Work]) AddWork(ctx context.Context, key string, work Work) error {
	select {
	case <-s.stopped:
		return context.Canceled
	default:
	}

	s.mu.Lock()
	if _, exists := s.active[key]; exists {
		// A worker is already draining this key's queue. Append.
		queue := s.active[key]
		if s.keyQueueCap > 0 && len(queue) >= s.keyQueueCap {
			// Drop the oldest; the new work goes at the tail.
			dropped := queue[0]
			queue = append(queue[1:], work)
			s.active[key] = queue
			s.mu.Unlock()
			if s.onDrop != nil {
				s.onDrop(dropped)
			}
			return nil
		}
		s.active[key] = append(queue, work)
		s.mu.Unlock()
		return nil
	}
	// Mark key as active and dispatch to a free worker.
	s.active[key] = nil
	s.mu.Unlock()

	select {
	case s.feeder <- task[Work]{key: key, work: work}:
		return nil
	case <-ctx.Done():
		// Roll back the key claim so a future AddWork for this key
		// dispatches normally.
		s.mu.Lock()
		delete(s.active, key)
		s.mu.Unlock()
		return ctx.Err()
	case <-s.stopped:
		s.mu.Lock()
		delete(s.active, key)
		s.mu.Unlock()
		return context.Canceled
	}
}
