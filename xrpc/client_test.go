package xrpc

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: gt.Some(1)}
}

// testServer creates an httptest.Server and returns a Client that uses
// the server's own transport, avoiding http.DefaultTransport races when
// parallel tests call httptest.Server.Close (which calls CloseIdleConnections).
func testServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *Client) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(srv.Client())}
	return srv, c
}

func TestQuery_Success(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/xrpc/app.bsky.feed.getTimeline", r.URL.Path)
		assert.Equal(t, "abc", r.URL.Query().Get("cursor"))
		assert.Equal(t, "50", r.URL.Query().Get("limit"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		_ = json.NewEncoder(w).Encode(map[string]string{"feed": "data"})
	})

	var out map[string]string
	err := c.Query(context.Background(), "app.bsky.feed.getTimeline", map[string]any{
		"cursor": "abc",
		"limit":  int64(50),
	}, &out)
	require.NoError(t, err)
	assert.Equal(t, "data", out["feed"])
}

func TestQuery_NoParams(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.URL.RawQuery)
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})

	var out map[string]bool
	err := c.Query(context.Background(), "test.method", nil, &out)
	require.NoError(t, err)
	assert.True(t, out["ok"])
}

func TestQuery_NoOutput(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestProcedure_Success(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		body, _ := io.ReadAll(r.Body)
		var in map[string]string
		require.NoError(t, json.Unmarshal(body, &in))
		assert.Equal(t, "hello", in["text"])
		_ = json.NewEncoder(w).Encode(map[string]string{"uri": "at://..."})
	})

	var out map[string]string
	err := c.Procedure(context.Background(), "com.atproto.repo.createRecord", map[string]string{"text": "hello"}, &out)
	require.NoError(t, err)
	assert.Equal(t, "at://...", out["uri"])
}

func TestProcedure_NoBody(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		w.WriteHeader(200)
	})

	err := c.Procedure(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestProcedure_MarshalError(t *testing.T) {
	t.Parallel()
	c := &Client{Host: "https://example.com", Retry: gt.Some(noRetry())}
	// Channels cannot be marshaled to JSON.
	err := c.Procedure(context.Background(), "test.method", make(chan int), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal request body")
}

func TestQuery_AuthHeader(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer mytoken", r.Header.Get("Authorization"))
		w.WriteHeader(200)
	})

	c.SetAuth(&AuthInfo{AccessJwt: "mytoken"})
	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestQuery_NoAuthHeader(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		w.WriteHeader(200)
	})

	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestQuery_UserAgent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "custom-agent", r.Header.Get("User-Agent"))
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), UserAgent: gt.Some("custom-agent"), HTTPClient: gt.Some(srv.Client())}
	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestQuery_DefaultUserAgent(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, defaultUserAgent, r.Header.Get("User-Agent"))
		w.WriteHeader(200)
	})

	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestQuery_ContextCanceled(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Query(ctx, "test.method", nil, nil)
	require.Error(t, err)
}

func TestQuery_ErrorResponse(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "InvalidRequest", "message": "bad param"})
	})

	err := c.Query(context.Background(), "test.method", nil, nil)
	require.Error(t, err)
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, 400, xErr.StatusCode)
	assert.Equal(t, "InvalidRequest", xErr.Name)
	assert.Equal(t, "bad param", xErr.Message)
}

func TestQuery_CustomHTTPClient(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	custom := &http.Client{Timeout: 5 * time.Second}
	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(custom)}
	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
	// Verify the custom client was used (same pointer).
	assert.Same(t, custom, c.client())
}

func TestQuery_ResponseBodyLimit(t *testing.T) {
	t.Parallel()
	// Server sends more than maxResponseBody.
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Write a large JSON response that starts valid but exceeds limit.
		_, _ = w.Write([]byte(`{"data":"`))
		big := make([]byte, maxResponseBody+1000)
		for i := range big {
			big[i] = 'a'
		}
		_, _ = w.Write(big)
		_, _ = w.Write([]byte(`"}`))
	})

	var out map[string]string
	err := c.Query(context.Background(), "test.method", nil, &out)
	// Should fail to decode because body was truncated at 5MB.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}

func TestQuery_NetworkErrorRetried(t *testing.T) {
	t.Parallel()
	// Create a listener, accept one connection and immediately close it,
	// then serve normally on subsequent connections.
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv.Close)

	// Simulate network error by using a host that refuses connections on first try.
	// Instead, use a server that returns 503 to test retry (network errors are
	// harder to simulate reliably in unit tests; the 503 retry path exercises
	// the same loop mechanics).
	attempts.Store(0)
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			// Close the connection abruptly to simulate network error.
			hj, ok := w.(http.Hijacker)
			if !ok {
				w.WriteHeader(503)
				return
			}
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv2.Close)

	c := &Client{
		Host:       srv2.URL,
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
		HTTPClient: gt.Some(srv2.Client()),
	}
	var out map[string]bool
	err := c.Query(context.Background(), "test.method", nil, &out)
	require.NoError(t, err)
	assert.True(t, out["ok"])
	assert.Equal(t, int32(2), attempts.Load())
}

func TestQuery_NonSeekableBodyNotRetried(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		body, _ := io.ReadAll(r.Body)
		if n == 1 {
			// First attempt: body present, return 503.
			assert.Equal(t, "stream-data", string(body))
			w.WriteHeader(503)
			return
		}
		// Second attempt: body should be empty (non-seekable).
		assert.Empty(t, body)
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	c := &Client{
		Host:       srv.URL,
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(2), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
		HTTPClient: gt.Some(srv.Client()),
	}
	// strings.Reader is not *bytes.Reader, so it's treated as non-seekable.
	err := c.Do(context.Background(), "POST", "test.method", "text/plain", nil, strings.NewReader("stream-data"), nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestQuery_MaxAttemptsZero(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	c := &Client{
		Host:       srv.URL,
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(0), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond)}),
		HTTPClient: gt.Some(srv.Client()),
	}
	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
	// MaxAttempts 0 should be treated as 1.
	assert.Equal(t, int32(1), attempts.Load())
}

func TestQuery_AcceptHeader(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		w.WriteHeader(200)
	})

	err := c.Query(context.Background(), "test.method", nil, nil)
	require.NoError(t, err)
}

func TestEncodeParams(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]any
		check  func(t *testing.T, qs string)
	}{
		{
			"string",
			map[string]any{"cursor": "abc"},
			func(t *testing.T, qs string) { assert.Contains(t, qs, "cursor=abc") },
		},
		{
			"int64",
			map[string]any{"limit": int64(50)},
			func(t *testing.T, qs string) { assert.Contains(t, qs, "limit=50") },
		},
		{
			"bool",
			map[string]any{"includeNsfw": true},
			func(t *testing.T, qs string) { assert.Contains(t, qs, "includeNsfw=true") },
		},
		{
			"[]string",
			map[string]any{"tags": []string{"a", "b"}},
			func(t *testing.T, qs string) {
				assert.Contains(t, qs, "tags=a")
				assert.Contains(t, qs, "tags=b")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, encodeParams(tt.params))
		})
	}
}

func TestProcedure_RetryWithBody(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		body, _ := io.ReadAll(r.Body)
		var in map[string]string
		require.NoError(t, json.Unmarshal(body, &in))
		assert.Equal(t, "hello", in["text"])
		if attempts.Load() < 2 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	t.Cleanup(srv.Close)

	c := &Client{
		Host:       srv.URL,
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
		HTTPClient: gt.Some(srv.Client()),
	}
	var out map[string]bool
	err := c.Procedure(context.Background(), "test.method", map[string]string{"text": "hello"}, &out)
	require.NoError(t, err)
	assert.Equal(t, int32(2), attempts.Load())
}

// TestQuery_ConcurrentRequestsDuringRefresh verifies that concurrent requests
// are not affected by RefreshSession (no shared state mutation).
func TestQuery_ConcurrentRequestsDuringRefresh(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if r.URL.Path == "/xrpc/com.atproto.server.refreshSession" {
			assert.Equal(t, "Bearer refresh-jwt", auth)
			// Simulate slow refresh.
			time.Sleep(50 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(AuthInfo{
				AccessJwt:  "new-access",
				RefreshJwt: "new-refresh",
				Handle:     "user.bsky.social",
				DID:        "did:plc:abc",
			})
			return
		}
		// Normal requests should use the access JWT, never the refresh JWT.
		assert.Equal(t, "Bearer old-access", auth, "concurrent request used wrong JWT")
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry()), HTTPClient: gt.Some(srv.Client())}
	c.SetAuth(&AuthInfo{AccessJwt: "old-access", RefreshJwt: "refresh-jwt"})

	// Launch concurrent query during refresh.
	done := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond) // Start during refresh.
		done <- c.Query(context.Background(), "test.method", nil, nil)
	}()

	_, err := c.RefreshSession(context.Background())
	require.NoError(t, err)
	require.NoError(t, <-done)
}

// TestQuery_NetworkErrorNotRetried_ContextCanceled verifies network errors
// return context error when context is canceled.
func TestQueryRaw_Success(t *testing.T) {
	t.Parallel()
	want := []byte{0x00, 0x01, 0x02, 0xff}
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/xrpc/com.atproto.sync.getBlob", r.URL.Path)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(want)
	})

	got, err := c.QueryRaw(context.Background(), "com.atproto.sync.getBlob", nil)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestQueryRaw_Error(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "BlobNotFound", "message": "not found"})
	})

	got, err := c.QueryRaw(context.Background(), "com.atproto.sync.getBlob", nil)
	require.Error(t, err)
	assert.Nil(t, got)
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, 404, xErr.StatusCode)
}

func TestQueryRaw_AcceptHeader(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "*/*", r.Header.Get("Accept"))
		_, _ = w.Write([]byte("ok"))
	})

	_, err := c.QueryRaw(context.Background(), "test.method", nil)
	require.NoError(t, err)
}

func TestQueryRaw_WithParams(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "abc123", r.URL.Query().Get("cid"))
		assert.Equal(t, "did:plc:test", r.URL.Query().Get("did"))
		_, _ = w.Write([]byte("blob-data"))
	})

	got, err := c.QueryRaw(context.Background(), "com.atproto.sync.getBlob", map[string]any{
		"cid": "abc123",
		"did": "did:plc:test",
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("blob-data"), got)
}

func TestQueryRaw_WithAuth(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer mytoken", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("ok"))
	})

	c.SetAuth(&AuthInfo{AccessJwt: "mytoken"})
	_, err := c.QueryRaw(context.Background(), "test.method", nil)
	require.NoError(t, err)
}

func TestQueryRaw_RetryOn503(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte("blob-data"))
	}))
	t.Cleanup(srv.Close)

	c := &Client{
		Host:       srv.URL,
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(3), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
		HTTPClient: gt.Some(srv.Client()),
	}
	got, err := c.QueryRaw(context.Background(), "test.method", nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("blob-data"), got)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestQueryRaw_EmptyResponse(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	got, err := c.QueryRaw(context.Background(), "test.method", nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestQueryStream_Success(t *testing.T) {
	t.Parallel()
	want := []byte{0x00, 0x01, 0x02, 0xff}
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/xrpc/com.atproto.sync.getRepo", r.URL.Path)
		assert.Equal(t, "*/*", r.Header.Get("Accept"))
		assert.Equal(t, "did:plc:test", r.URL.Query().Get("did"))
		_, _ = w.Write(want)
	})

	body, err := c.QueryStream(context.Background(), "com.atproto.sync.getRepo", map[string]any{"did": "did:plc:test"})
	require.NoError(t, err)
	got, err := io.ReadAll(body)
	_ = body.Close()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestQueryStream_Error(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":"InvalidRequest","message":"bad param"}`))
	})

	body, err := c.QueryStream(context.Background(), "test.method", nil)
	require.Error(t, err)
	assert.Nil(t, body)
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, 400, xErr.StatusCode)
	assert.Equal(t, "InvalidRequest", xErr.Name)
}

func TestQueryStream_Auth(t *testing.T) {
	t.Parallel()
	_, c := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer mytoken", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("ok"))
	})

	c.SetAuth(&AuthInfo{AccessJwt: "mytoken"})
	body, err := c.QueryStream(context.Background(), "test.method", nil)
	require.NoError(t, err)
	_ = body.Close()
}

func TestQuery_NetworkErrorNotRetried_ContextCanceled(t *testing.T) {
	t.Parallel()
	// Use a listener that accepts but never responds.
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	c := &Client{
		Host:       "http://" + ln.Addr().String(),
		Retry:      gt.Some(RetryPolicy{MaxAttempts: gt.Some(5), BaseDelay: gt.Some(time.Millisecond), MaxDelay: gt.Some(10 * time.Millisecond), Jitter: gt.Some(0.0)}),
		HTTPClient: gt.Some(&http.Client{Timeout: 50 * time.Millisecond}),
	}
	err = c.Query(ctx, "test.method", nil, nil)
	require.Error(t, err)
}
