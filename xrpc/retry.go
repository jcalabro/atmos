package xrpc

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"github.com/jcalabro/gt"
)

// RetryPolicy controls retry behavior for XRPC requests.
type RetryPolicy struct {
	MaxAttempts gt.Option[int]
	BaseDelay   gt.Option[time.Duration]
	MaxDelay    gt.Option[time.Duration]
	Jitter      gt.Option[float64]
}

// DefaultRetryPolicy is used when Client.Retry is None.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: gt.Some(3),
	BaseDelay:   gt.Some(500 * time.Millisecond),
	MaxDelay:    gt.Some(30 * time.Second),
	Jitter:      gt.Some(0.2),
}

// delay returns the backoff duration for the given attempt (0-indexed).
func (p *RetryPolicy) delay(attempt int) time.Duration {
	d := float64(p.BaseDelay.Val()) * math.Pow(2, float64(attempt))
	if p.Jitter.HasVal() && p.Jitter.Val() > 0 {
		d += d * p.Jitter.Val() * rand.Float64()
	}
	if d > float64(p.MaxDelay.Val()) {
		d = float64(p.MaxDelay.Val())
	}
	return time.Duration(d)
}

// sleep waits for the given duration or until ctx is cancelled.
// Returns ctx.Err() if cancelled.
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
