package xrpc

import (
	"context"
	"sync"
	"time"
)

// MaxServerDirectedDelay is the hard ceiling for Retry-After and
// RateLimit-Reset delays supplied by an untrusted server, unless a Client
// raises it with [Client.MaxRateLimitWait].
const MaxServerDirectedDelay = 30 * time.Second

// rateLimitKey names one server-side quota: a host and the method called on
// it. atproto servers keep separate buckets per endpoint (a PDS limits
// com.atproto.sync.getRepo apart from its global read limit), and response
// headers report only the bucket that answered, so a listRepos response with
// quota to spare must not release a parked getRepo. Endpoints that share a
// server-side global bucket are tracked apart; each one parks on its own
// first exhausted response.
type rateLimitKey struct {
	host string
	nsid string
}

// rateLimitPark is one exhausted quota: requests wait until until, which is
// the server's reset clamped to the client's ceiling. reset is the server's
// own value, kept to tell a stale response from the current window apart
// from one from a newer window.
type rateLimitPark struct {
	until time.Time
	reset time.Time
}

// rateLimitState tracks server-directed backpressure by host and method.
// Retry-After, or a reset in the future with no quota left, parks requests
// for that key without stalling requests bound anywhere else.
//
// Per-host keying matters because one Client's requests can be served by
// multiple hosts: a relay 302-redirects getRepo/listRepos to the account's
// PDS, so responses (and their RateLimit-* headers) come from many PDS hosts
// through a single front door. A previous version of this type kept one
// global remaining/reset pair, which let one exhausted PDS park every request
// the client sent anywhere until that host's window reset.
//
// Only exhausted keys are retained; entries whose park has passed are swept
// on every update, so the map is bounded by the number of currently-parked
// keys.
type rateLimitState struct {
	mu        sync.Mutex
	exhausted map[rateLimitKey]rateLimitPark
}

// update records backpressure reported for key. reserve is the fraction of
// RateLimit-Limit the client leaves unused: a response reporting that many or
// fewer remaining parks key, so other clients sharing the quota (the same
// address, another process) keep some of it. Retry-After does not require a
// RateLimit-Remaining companion header. Server reset times are clamped to
// maxWait so an untrusted host cannot dictate an unbounded sleep.
func (s *rateLimitState) update(key rateLimitKey, rl *RateLimit, maxWait time.Duration, reserve float64) {
	if key.host == "" || rl == nil {
		return
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for k, p := range s.exhausted {
		if !p.until.After(now) {
			delete(s.exhausted, k)
		}
	}

	if rl.RemainingSet && rl.Remaining > reserveFloor(rl.Limit, reserve) {
		// Quota left. Remaining only falls within a window, so a response
		// from the parked window (or an older one, arriving late) is stale
		// and must not release the park: only a later reset proves a new
		// window opened.
		if p, ok := s.exhausted[key]; !ok || rl.Reset.IsZero() || rl.Reset.After(p.reset) {
			delete(s.exhausted, key)
		}
		return
	}
	if !rl.Reset.After(now) || maxWait <= 0 {
		if rl.RemainingSet {
			delete(s.exhausted, key)
		}
		return
	}
	until := rl.Reset
	if ceiling := now.Add(maxWait); until.After(ceiling) {
		until = ceiling
	}
	if p, ok := s.exhausted[key]; ok && p.until.After(until) {
		until = p.until
	}
	if s.exhausted == nil {
		s.exhausted = make(map[rateLimitKey]rateLimitPark)
	}
	s.exhausted[key] = rateLimitPark{until: until, reset: rl.Reset}
}

// reserveFloor is the remaining quota at or below which a client keeping
// reserve of limit stops sending: 0 without a reserve or a known limit.
func reserveFloor(limit int, reserve float64) int {
	if limit <= 0 || reserve <= 0 {
		return 0
	}
	return int(float64(limit) * min(reserve, 1))
}

// parkedUntil reports when key's park ends, or the zero time if key is not
// parked.
func (s *rateLimitState) parkedUntil(key rateLimitKey) time.Time {
	if key.host == "" {
		return time.Time{}
	}
	s.mu.Lock()
	p, ok := s.exhausted[key]
	s.mu.Unlock()
	if !ok || !p.until.After(time.Now()) {
		return time.Time{}
	}
	return p.until
}

// wait sleeps while key is parked and returns how long it slept. A park that
// a later response extends while the caller sleeps is waited out too. It
// returns immediately when key is unknown, not parked, or its park already
// passed.
func (s *rateLimitState) wait(ctx context.Context, key rateLimitKey) (time.Duration, error) {
	var waited time.Duration
	for {
		until := s.parkedUntil(key)
		if until.IsZero() {
			return waited, nil
		}
		start := time.Now()
		err := sleep(ctx, time.Until(until))
		waited += time.Since(start)
		if err != nil {
			return waited, err
		}
	}
}
