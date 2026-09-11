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

type effectiveRetryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	jitter      float64
}

// DefaultRetryPolicy is used when Client.Retry is None.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: gt.Some(3),
	BaseDelay:   gt.Some(500 * time.Millisecond),
	MaxDelay:    gt.Some(30 * time.Second),
	Jitter:      gt.Some(0.2),
}

// delay returns the backoff duration for the given attempt (0-indexed).
func (p RetryPolicy) normalized() effectiveRetryPolicy {
	maxAttempts := p.MaxAttempts.ValOr(DefaultRetryPolicy.MaxAttempts.ValOr(3))
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	baseDelay := p.BaseDelay.ValOr(DefaultRetryPolicy.BaseDelay.ValOr(500 * time.Millisecond))
	maxDelay := p.MaxDelay.ValOr(DefaultRetryPolicy.MaxDelay.ValOr(30 * time.Second))
	baseDelay = max(baseDelay, 0)
	maxDelay = max(maxDelay, 0)
	jitter := p.Jitter.ValOr(DefaultRetryPolicy.Jitter.ValOr(0.2))
	if math.IsNaN(jitter) || jitter < 0 {
		jitter = 0
	} else if jitter > 1 {
		jitter = 1
	}
	return effectiveRetryPolicy{maxAttempts: maxAttempts, baseDelay: baseDelay, maxDelay: maxDelay, jitter: jitter}
}

// delay returns the backoff duration for the given attempt (0-indexed).
// It remains a method on RetryPolicy for package compatibility in tests; request
// execution normalizes the policy once before entering its retry loop.
func (p *RetryPolicy) delay(attempt int) time.Duration {
	return p.normalized().delay(attempt)
}

func (p effectiveRetryPolicy) delay(attempt int) time.Duration {
	if attempt < 0 || p.baseDelay == 0 || p.maxDelay == 0 {
		return 0
	}
	d := float64(p.baseDelay) * math.Pow(2, float64(attempt))
	if p.jitter > 0 {
		d += d * p.jitter * rand.Float64()
	}
	if math.IsInf(d, 1) || d > float64(p.maxDelay) {
		return p.maxDelay
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
