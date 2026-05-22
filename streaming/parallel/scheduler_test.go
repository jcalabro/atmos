// streaming/parallel/scheduler_test.go
package parallel

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScheduler_StartStop(t *testing.T) {
	s := NewScheduler(2, 0, func(ctx context.Context, w int) error {
		return nil
	}, nil)
	defer s.Shutdown()

	// Smoke: zero work, just construct & shutdown.
	require.NotNil(t, s)
	require.Equal(t, 2, s.Workers())
}

func TestScheduler_ShutdownIdempotent(t *testing.T) {
	s := NewScheduler(2, 0, func(ctx context.Context, w int) error {
		return nil
	}, nil)
	s.Shutdown()
	// Second shutdown must not panic or hang.
	done := make(chan struct{})
	go func() {
		s.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second Shutdown blocked")
	}
}

func TestScheduler_PerKeyFIFO(t *testing.T) {
	const N = 100

	var (
		mu       sync.Mutex
		observed []int
	)
	s := NewScheduler(4, 0, func(ctx context.Context, n int) error {
		// Tiny sleep so concurrent dispatches are likely to interleave
		// across workers if FIFO weren't enforced.
		time.Sleep(time.Millisecond)
		mu.Lock()
		observed = append(observed, n)
		mu.Unlock()
		return nil
	}, nil)
	defer s.Shutdown()

	ctx := context.Background()
	for i := range N {
		require.NoError(t, s.AddWork(ctx, "did:plc:samekey", i))
	}

	// Wait for completion.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(observed) == N
	}, time.Second, time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for i, v := range observed {
		require.Equal(t, i, v, "out-of-order at index %d", i)
	}
}

func TestScheduler_CrossKeyParallel(t *testing.T) {
	const Workers = 8
	const Keys = 8

	// Each handler waits on a barrier so all 8 keys must be running
	// concurrently before any can complete. If parallelism were less
	// than Keys, this would deadlock and time out.
	var (
		started sync.WaitGroup
		release = make(chan struct{})
	)
	started.Add(Keys)

	s := NewScheduler(Workers, 0, func(ctx context.Context, k string) error {
		started.Done()
		<-release
		return nil
	}, nil)
	defer s.Shutdown()

	ctx := context.Background()
	for i := range Keys {
		require.NoError(t, s.AddWork(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}

	// All Keys handlers must run concurrently.
	allRunning := make(chan struct{})
	go func() {
		started.Wait()
		close(allRunning)
	}()

	select {
	case <-allRunning:
	case <-time.After(time.Second):
		t.Fatal("only some workers ran; cross-key parallelism not honored")
	}
	close(release)
}

func TestScheduler_DropOldest(t *testing.T) {
	const Cap = 10
	const Burst = 100

	// Single worker, one key, cap=10. We dispatch Burst units; the
	// first will be running, then 10 will queue, then each new arrival
	// drops the head of the queue.
	gate := make(chan struct{})
	var (
		mu      sync.Mutex
		seen    []int
		dropped []int
	)
	s := NewScheduler(1, Cap, func(ctx context.Context, n int) error {
		<-gate // hold the worker open until the test releases
		mu.Lock()
		seen = append(seen, n)
		mu.Unlock()
		return nil
	}, func(n int) {
		mu.Lock()
		dropped = append(dropped, n)
		mu.Unlock()
	})
	defer func() {
		close(gate)
		s.Shutdown()
	}()

	ctx := context.Background()
	for i := range Burst {
		require.NoError(t, s.AddWork(ctx, "didX", i))
	}

	// Wait for all drops to register. (Burst-1 in-flight slot - Cap queue = drops.)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(dropped) == Burst-1-Cap
	}, time.Second, time.Millisecond,
		"expected %d drops, got %d", Burst-1-Cap, len(dropped))

	mu.Lock()
	defer mu.Unlock()
	// Drops are the OLDEST queued items, in order: indices 1..Burst-1-Cap.
	// (Index 0 is in-flight; indices 1..Cap fill the initial queue;
	// index Cap+1 displaces 1, Cap+2 displaces 2, etc.)
	for i, d := range dropped {
		require.Equal(t, i+1, d)
	}
}

func TestScheduler_PanicRecovery(t *testing.T) {
	var (
		mu   sync.Mutex
		errs []error
	)
	onError := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	s := NewSchedulerWithErrorHook(2, 0, func(ctx context.Context, n int) error {
		if n%2 == 0 {
			panic(fmt.Errorf("boom on %d", n))
		}
		return nil
	}, nil, onError)
	defer s.Shutdown()

	ctx := context.Background()
	for i := range 10 {
		require.NoError(t, s.AddWork(ctx, fmt.Sprintf("k%d", i), i))
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(errs) == 5
	}, time.Second, time.Millisecond)
}

// TestScheduler_WorkContextCancellation verifies that when the
// scheduler's lifetime context is cancelled, in-flight do() invocations
// observe the cancellation. This is what gives consumers prompt
// shutdown when they cancel Events(ctx): the readLoop's parent ctx is
// the scheduler's ctx, so PLC lookups, CAR downloads, and StateStore
// I/O inside the verifier all unblock.
func TestScheduler_WorkContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	finished := make(chan error, 1)
	s := NewSchedulerWithContext(ctx, 1, 0, func(jctx context.Context, n int) error {
		close(started)
		select {
		case <-jctx.Done():
			finished <- jctx.Err()
			return jctx.Err()
		case <-time.After(5 * time.Second):
			finished <- nil
			return nil
		}
	}, nil)
	defer s.Shutdown()

	require.NoError(t, s.AddWork(context.Background(), "k", 1))
	<-started
	cancel()

	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("worker did not observe context cancellation")
	}
}

func TestScheduler_AddWorkCancelled(t *testing.T) {
	// One worker, hold it busy, fill so the next AddWork on a NEW key
	// blocks on the unbuffered feeder.
	gate := make(chan struct{})
	s := NewScheduler(1, 0, func(ctx context.Context, n int) error {
		<-gate
		return nil
	}, nil)
	defer s.Shutdown()

	ctx0 := context.Background()
	require.NoError(t, s.AddWork(ctx0, "k1", 1)) // claims the worker

	// Different key → must dispatch via feeder. Feeder is unbuffered;
	// worker is busy, so AddWork blocks.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.AddWork(ctx, "k2", 2)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// After cancellation, the key claim must be rolled back so a future
	// AddWork(k2) starts cleanly. First unblock the worker so k1 finishes.
	close(gate)

	// Drain k1; worker becomes free. New AddWork(k2) should now succeed.
	require.Eventually(t, func() bool {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel2()
		return s.AddWork(ctx2, "k2", 3) == nil
	}, time.Second, 10*time.Millisecond)
}
