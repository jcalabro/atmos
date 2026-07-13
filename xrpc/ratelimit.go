package xrpc

import (
	"context"
	"sync"
	"time"
)

// rateLimitState tracks rate limit quota for proactive throttling, keyed by
// the host that reported it. When a host reports zero remaining quota with a
// reset in the future, requests bound for that host wait until the reset to
// avoid a 429 — without stalling requests bound for other hosts.
//
// Per-host keying matters because one Client's requests can be served by
// multiple hosts: a relay 302-redirects getRepo/listRepos to the account's
// PDS, so responses (and their RateLimit-* headers) come from many PDS hosts
// through a single front door. A previous version of this type kept one
// global remaining/reset pair, which let one exhausted PDS park every request
// the client sent anywhere until that host's window reset.
//
// Only exhausted hosts are retained; entries whose reset has passed are swept
// on every update, so the map is bounded by the number of currently-parked
// hosts.
type rateLimitState struct {
	mu        sync.Mutex
	exhausted map[string]time.Time // host -> quota reset time
}

// update records rate limit information reported by host. A report with
// remaining quota (or an already-passed reset) clears any parking for the
// host; a report of zero remaining with a future reset parks it. An empty
// host (no usable request URL on the response) is ignored — unattributable
// headers must not park anything.
func (s *rateLimitState) update(host string, rl *RateLimit) {
	if host == "" {
		return
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for h, reset := range s.exhausted {
		if !reset.After(now) {
			delete(s.exhausted, h)
		}
	}

	if rl.Remaining > 0 || !rl.Reset.After(now) {
		delete(s.exhausted, host)
		return
	}
	if s.exhausted == nil {
		s.exhausted = make(map[string]time.Time)
	}
	s.exhausted[host] = rl.Reset
}

// wait sleeps until host's quota reset if host is parked as exhausted.
// Returns nil immediately when host is unknown, not parked, or its reset has
// already passed.
func (s *rateLimitState) wait(ctx context.Context, host string) error {
	if host == "" {
		return nil
	}

	s.mu.Lock()
	reset, ok := s.exhausted[host]
	s.mu.Unlock()

	if !ok {
		return nil
	}
	d := time.Until(reset)
	if d <= 0 {
		return nil
	}
	return sleep(ctx, d)
}
