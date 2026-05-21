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
