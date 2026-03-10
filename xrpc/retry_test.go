package xrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryPolicy_Delay(t *testing.T) {
	t.Parallel()
	p := &RetryPolicy{BaseDelay: gt.Some(100 * time.Millisecond), MaxDelay: gt.Some(1 * time.Second), Jitter: gt.Some(0.0)}
	assert.Equal(t, 100*time.Millisecond, p.delay(0))
	assert.Equal(t, 200*time.Millisecond, p.delay(1))
	assert.Equal(t, 400*time.Millisecond, p.delay(2))
	assert.Equal(t, 800*time.Millisecond, p.delay(3))
	assert.Equal(t, 1*time.Second, p.delay(4)) // capped
}

func TestRetry_503Retried(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(503)
			_, _ = fmt.Fprintf(w, `{"error":"ServiceUnavailable"}`)
			return
		}
		w.WriteHeader(200)
		_, _ = fmt.Fprintf(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := &Client{
		Host:  srv.URL,
		Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
	}
	var out map[string]any
	err := c.Query(context.Background(), "test.method", nil, &out)
	require.NoError(t, err)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestRetry_429Retried(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.Header().Set("RateLimit-Limit", "100")
			w.Header().Set("RateLimit-Remaining", "0")
			w.WriteHeader(429)
			_, _ = fmt.Fprintf(w, `{"error":"RateLimitExceeded"}`)
			return
		}
		w.WriteHeader(200)
		_, _ = fmt.Fprintf(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := &Client{
		Host:  srv.URL,
		Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(2), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
	}
	var out map[string]any
	err := c.Query(context.Background(), "test.method", nil, &out)
	require.NoError(t, err)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestRetry_401NotRetried(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(401)
		_, _ = fmt.Fprintf(w, `{"error":"AuthRequired"}`)
	}))
	defer srv.Close()

	c := &Client{
		Host:  srv.URL,
		Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
	}
	err := c.Query(context.Background(), "test.method", nil, nil)
	require.Error(t, err)
	assert.Equal(t, int32(1), attempts.Load())
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, "AuthRequired", xErr.Name)
}

func TestRetry_MaxAttemptsExhausted(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(503)
		_, _ = fmt.Fprintf(w, `{"error":"ServiceUnavailable"}`)
	}))
	defer srv.Close()

	c := &Client{
		Host:  srv.URL,
		Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
	}
	err := c.Query(context.Background(), "test.method", nil, nil)
	require.Error(t, err)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestRetry_ContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = fmt.Fprintf(w, `{"error":"ServiceUnavailable"}`)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		Host:  srv.URL,
		Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(5), BaseDelay: gt.Some(5 * time.Second), MaxDelay: gt.Some(30 * time.Second), Jitter: gt.Some(0.0)}),
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := c.Query(ctx, "test.method", nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetry_RateLimitResetRespected(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			reset := time.Now().Add(50 * time.Millisecond).Unix()
			w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", reset))
			w.WriteHeader(429)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RateLimited"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	c := &Client{
		Host:  srv.URL,
		Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(2), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(5 * time.Second), Jitter: gt.Some(0.0)}),
	}
	var out map[string]any
	err := c.Query(context.Background(), "test.method", nil, &out)
	require.NoError(t, err)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestSleep_ContextDone(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sleep(ctx, time.Hour)
	assert.ErrorIs(t, err, context.Canceled)
}
