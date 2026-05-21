// streaming/parallel/scheduler_test.go
package parallel

import (
	"context"
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
