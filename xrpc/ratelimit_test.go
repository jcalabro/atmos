package xrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProactiveRateLimit_DelaysWhenExhausted(t *testing.T) {
	t.Parallel()
	if runtime.GOARCH == "wasm" {
		t.Skip("timing-sensitive test unreliable under WASM")
	}

	// Test wait() directly with millisecond precision to avoid the
	// Unix-second truncation that made the HTTP-based test flaky/slow.
	var s rateLimitState
	s.update(&RateLimit{Remaining: 0, Reset: time.Now().Add(50 * time.Millisecond)})

	start := time.Now()
	require.NoError(t, s.wait(context.Background()))
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond, "should have waited for rate limit reset")
	assert.Less(t, elapsed, 200*time.Millisecond, "should not wait much longer than reset time")
}

func TestProactiveRateLimit_NoDelayWhenResetInPast(t *testing.T) {
	t.Parallel()
	resetTime := time.Now().Add(-1 * time.Second) // already past

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("RateLimit-Limit", "100")
		w.Header().Set("RateLimit-Remaining", "0")
		w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", resetTime.Unix()))
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv.Close)

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(srv.Client())}

	var out map[string]bool
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))

	start := time.Now()
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))
	assert.Less(t, time.Since(start), 200*time.Millisecond, "should not wait for past reset")
}

func TestProactiveRateLimit_NoHeaders_NoTracking(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})

	var out map[string]bool
	start := time.Now()
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))
	assert.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestProactiveRateLimit_RemainingPositive_NoDelay(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("RateLimit-Limit", "100")
		w.Header().Set("RateLimit-Remaining", "50")
		w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(10*time.Second).Unix()))
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv.Close)

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(srv.Client())}

	var out map[string]bool
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))

	start := time.Now()
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))
	assert.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestProactiveRateLimit_ContextCanceled(t *testing.T) {
	t.Parallel()
	resetTime := time.Now().Add(10 * time.Second)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("RateLimit-Remaining", "0")
		w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", resetTime.Unix()))
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv.Close)

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(srv.Client())}

	var out map[string]bool
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))

	// Second request with cancelled context should return quickly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Query(ctx, "test.method", nil, &out)
	require.Error(t, err)
}

func TestProactiveRateLimit_429FallbackStillWorks(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(100*time.Millisecond).Unix()))
			w.WriteHeader(429)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RateLimitExceeded"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv.Close)

	c := &Client{
		Host:       srv.URL,
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(100 * time.Millisecond), MaxDelay: gt.Some(1 * time.Second), Jitter: gt.Some(0.0)}),
		HTTPClient: gt.Some(srv.Client()),
	}

	var out map[string]bool
	require.NoError(t, c.Query(context.Background(), "test.method", nil, &out))
	assert.True(t, out["ok"])
	assert.Equal(t, int32(2), calls.Load())
}
