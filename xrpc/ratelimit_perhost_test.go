package xrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProactiveRateLimit_PerHost_OtherHostNotBlocked(t *testing.T) {
	t.Parallel()
	if runtime.GOARCH == "wasm" {
		t.Skip("timing-sensitive test unreliable under WASM")
	}

	// Exhausting host A must not delay requests bound for host B.
	var s rateLimitState
	s.update("a.example", &RateLimit{Remaining: 0, Reset: time.Now().Add(1 * time.Second)})

	start := time.Now()
	require.NoError(t, s.wait(context.Background(), "b.example"))
	assert.Less(t, time.Since(start), 200*time.Millisecond,
		"host B must not wait on host A's exhausted quota")
}

func TestProactiveRateLimit_PerHost_SameHostStillWaits(t *testing.T) {
	t.Parallel()
	if runtime.GOARCH == "wasm" {
		t.Skip("timing-sensitive test unreliable under WASM")
	}

	var s rateLimitState
	s.update("a.example", &RateLimit{Remaining: 0, Reset: time.Now().Add(50 * time.Millisecond)})

	start := time.Now()
	require.NoError(t, s.wait(context.Background(), "a.example"))
	assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond,
		"exhausted host must still be waited on")
}

func TestProactiveRateLimit_PerHost_RemainingQuotaClears(t *testing.T) {
	t.Parallel()

	var s rateLimitState
	s.update("a.example", &RateLimit{Remaining: 0, Reset: time.Now().Add(10 * time.Second)})
	// A newer response with remaining quota clears the parked state.
	s.update("a.example", &RateLimit{Remaining: 5, Reset: time.Now().Add(10 * time.Second)})

	start := time.Now()
	require.NoError(t, s.wait(context.Background(), "a.example"))
	assert.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestProactiveRateLimit_PerHost_ExpiredResetIgnored(t *testing.T) {
	t.Parallel()

	var s rateLimitState
	s.update("a.example", &RateLimit{Remaining: 0, Reset: time.Now().Add(-1 * time.Second)})

	start := time.Now()
	require.NoError(t, s.wait(context.Background(), "a.example"))
	assert.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestProactiveRateLimit_PerHost_EmptyHostIgnored(t *testing.T) {
	t.Parallel()

	var s rateLimitState
	s.update("", &RateLimit{Remaining: 0, Reset: time.Now().Add(10 * time.Second)})

	start := time.Now()
	require.NoError(t, s.wait(context.Background(), ""))
	assert.Less(t, time.Since(start), 200*time.Millisecond,
		"unattributable rate-limit headers must not park anything")
}

func TestProactiveRateLimit_PerHost_ExpiredEntriesSwept(t *testing.T) {
	t.Parallel()

	var s rateLimitState
	s.update("a.example", &RateLimit{Remaining: 0, Reset: time.Now().Add(5 * time.Millisecond)})
	time.Sleep(10 * time.Millisecond)
	// Any later update sweeps entries whose reset has passed, keeping the
	// map bounded by currently-parked hosts.
	s.update("b.example", &RateLimit{Remaining: 3, Reset: time.Now().Add(10 * time.Second)})

	s.mu.Lock()
	_, ok := s.exhausted["a.example"]
	n := len(s.exhausted)
	s.mu.Unlock()
	assert.False(t, ok, "expired parking for a.example should have been swept")
	assert.Zero(t, n, "no live parkings should remain")
}

// TestProactiveRateLimit_RedirectedHostDoesNotBlockClient is the end-to-end
// regression for the global-state bug: a client whose Host 302-redirects some
// requests to a second server (relay -> PDS shape). The redirect target
// reporting an exhausted quota must not stall subsequent requests that the
// front host serves directly.
func TestProactiveRateLimit_RedirectedHostDoesNotBlockClient(t *testing.T) {
	t.Parallel()
	if runtime.GOARCH == "wasm" {
		t.Skip("timing-sensitive test unreliable under WASM")
	}

	pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("RateLimit-Limit", "6000")
		w.Header().Set("RateLimit-Remaining", "0")
		w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(3*time.Second).Unix()))
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(pds.Close)

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xrpc/test.redirected" {
			http.Redirect(w, r, pds.URL+r.URL.Path, http.StatusFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(relay.Close)

	c := &Client{Host: relay.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(relay.Client())}

	// First request redirects to the PDS, whose response reports zero
	// remaining quota.
	var out map[string]bool
	require.NoError(t, c.Query(context.Background(), "test.redirected", nil, &out))

	// A request served directly by the relay must not wait for the PDS reset.
	start := time.Now()
	require.NoError(t, c.Query(context.Background(), "test.direct", nil, &out))
	assert.Less(t, time.Since(start), 1*time.Second,
		"relay-served request must not wait on the redirected PDS host's quota")
}
