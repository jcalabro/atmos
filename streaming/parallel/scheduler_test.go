// streaming/parallel/scheduler_test.go
package parallel

import (
	"context"
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
