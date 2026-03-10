package xrpc

import (
	"context"
	"sync"
	"time"
)

// rateLimitState tracks rate limit quota across requests for proactive
// throttling. When remaining hits 0 and the reset time is in the future,
// wait blocks until the reset time to avoid 429 responses.
type rateLimitState struct {
	mu        sync.Mutex
	remaining int
	reset     time.Time
	tracking  bool
}

// update records new rate limit information from response headers.
func (s *rateLimitState) update(rl *RateLimit) {
	s.mu.Lock()
	s.remaining = rl.Remaining
	s.reset = rl.Reset
	s.tracking = true
	s.mu.Unlock()
}

// wait sleeps until the rate limit reset time if remaining is 0.
// Returns nil immediately if not tracking or if there is remaining quota.
func (s *rateLimitState) wait(ctx context.Context) error {
	s.mu.Lock()
	if !s.tracking || s.remaining > 0 {
		s.mu.Unlock()
		return nil
	}
	d := time.Until(s.reset)
	s.mu.Unlock()

	if d <= 0 {
		return nil
	}
	return sleep(ctx, d)
}
