package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoffDelay(t *testing.T) {
	t.Parallel()

	t.Run("exponential growth", func(t *testing.T) {
		t.Parallel()
		b := &BackoffPolicy{
			InitialDelay: gt.Some(1 * time.Second),
			MaxDelay:     gt.Some(30 * time.Second),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(false),
		}
		assert.Equal(t, 1*time.Second, b.delay(0))
		assert.Equal(t, 2*time.Second, b.delay(1))
		assert.Equal(t, 4*time.Second, b.delay(2))
		assert.Equal(t, 8*time.Second, b.delay(3))
	})

	t.Run("capped at max", func(t *testing.T) {
		t.Parallel()
		b := &BackoffPolicy{
			InitialDelay: gt.Some(1 * time.Second),
			MaxDelay:     gt.Some(5 * time.Second),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(false),
		}
		assert.Equal(t, 4*time.Second, b.delay(2))
		assert.Equal(t, 5*time.Second, b.delay(3))
		assert.Equal(t, 5*time.Second, b.delay(10))
	})

	t.Run("with jitter", func(t *testing.T) {
		t.Parallel()
		b := &BackoffPolicy{
			InitialDelay: gt.Some(1 * time.Second),
			MaxDelay:     gt.Some(30 * time.Second),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(true),
		}
		// With full jitter, delay is in [0, computed).
		for range 100 {
			d := b.delay(0)
			assert.GreaterOrEqual(t, d, time.Duration(0))
			assert.Less(t, d, 1*time.Second)
		}
	})
}

func TestSleep(t *testing.T) {
	t.Parallel()

	t.Run("completes normally", func(t *testing.T) {
		t.Parallel()
		err := sleep(context.Background(), 1*time.Millisecond)
		require.NoError(t, err)
	})

	t.Run("cancelled context", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := sleep(ctx, 1*time.Hour)
		require.ErrorIs(t, err, context.Canceled)
	})
}
