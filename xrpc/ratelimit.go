package xrpc

import (
	"context"
	"sync"
	"time"
)

// MaxServerDirectedDelay is the hard ceiling for Retry-After and
// RateLimit-Reset delays supplied by an untrusted server.
const MaxServerDirectedDelay = 30 * time.Second

// rateLimitState tracks server-directed backpressure by host. Retry-After or a
// reset in the future parks requests for that host without stalling requests
// bound for other hosts.
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

// update records backpressure reported by host. Retry-After does not require a
// RateLimit-Remaining companion header. Server reset times are clamped so an
// untrusted host cannot dictate an unbounded sleep.
func (s *rateLimitState) update(host string, rl *RateLimit, maxWait time.Duration) {
	if host == "" || rl == nil {
		return
	}
	maxWait = min(maxWait, MaxServerDirectedDelay)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for h, reset := range s.exhausted {
		if !reset.After(now) {
			delete(s.exhausted, h)
		}
	}

	if rl.RemainingSet && rl.Remaining > 0 {
		delete(s.exhausted, host)
		return
	}
	if !rl.Reset.After(now) || maxWait <= 0 {
		if rl.RemainingSet {
			delete(s.exhausted, host)
		}
		return
	}
	if s.exhausted == nil {
		s.exhausted = make(map[string]time.Time)
	}
	reset := rl.Reset
	if ceiling := now.Add(maxWait); reset.After(ceiling) {
		reset = ceiling
	}
	s.exhausted[host] = reset
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
