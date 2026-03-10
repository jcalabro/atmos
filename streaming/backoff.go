package streaming

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"github.com/jcalabro/gt"
)

// BackoffPolicy controls exponential backoff for reconnection.
type BackoffPolicy struct {
	InitialDelay gt.Option[time.Duration]
	MaxDelay     gt.Option[time.Duration]
	Multiplier   gt.Option[float64]
	Jitter       gt.Option[bool]
}

var defaultBackoff = BackoffPolicy{
	InitialDelay: gt.Some(1 * time.Second),
	MaxDelay:     gt.Some(30 * time.Second),
	Multiplier:   gt.Some(2.0),
	Jitter:       gt.Some(true),
}

// delay returns the backoff duration for the given attempt (0-indexed).
func (b *BackoffPolicy) delay(attempt int) time.Duration {
	initial := b.InitialDelay.ValOr(defaultBackoff.InitialDelay.Val())
	maxDelay := b.MaxDelay.ValOr(defaultBackoff.MaxDelay.Val())
	mult := b.Multiplier.ValOr(defaultBackoff.Multiplier.Val())
	d := float64(initial) * math.Pow(mult, float64(attempt))
	if d > float64(maxDelay) {
		d = float64(maxDelay)
	}
	if b.Jitter.ValOr(defaultBackoff.Jitter.Val()) {
		d = rand.Float64() * d // full jitter: [0, d)
	}
	return time.Duration(d)
}

// sleep waits for the given duration or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
