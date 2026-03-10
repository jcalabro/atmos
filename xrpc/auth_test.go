package xrpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetAuth_Auth_RoundTrip(t *testing.T) {
	t.Parallel()
	c := &Client{Host: "https://example.com"}
	assert.Nil(t, c.Auth())

	a := &AuthInfo{AccessJwt: "a", RefreshJwt: "r", Handle: "h", DID: "d"}
	c.SetAuth(a)
	got := c.Auth()
	require.NotNil(t, got)
	assert.Equal(t, "a", got.AccessJwt)
	assert.Equal(t, "r", got.RefreshJwt)
	assert.Equal(t, "h", got.Handle)
	assert.Equal(t, "d", got.DID)

	// Defensive copy — mutating returned value should not affect stored.
	got.AccessJwt = "modified"
	assert.Equal(t, "a", c.Auth().AccessJwt)

	// Setting nil clears auth.
	c.SetAuth(nil)
	assert.Nil(t, c.Auth())
}

func TestCreateSession_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/xrpc/com.atproto.server.createSession", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		assert.Empty(t, r.Header.Get("Authorization"))

		body, _ := io.ReadAll(r.Body)
		var req createSessionReq
		require.NoError(t, json.Unmarshal(body, &req))
		assert.Equal(t, "user.bsky.social", req.Identifier)
		assert.Equal(t, "pass123", req.Password)

		_ = json.NewEncoder(w).Encode(AuthInfo{
			AccessJwt:  "access-jwt",
			RefreshJwt: "refresh-jwt",
			Handle:     "user.bsky.social",
			DID:        "did:plc:abc",
		})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	auth, err := c.CreateSession(context.Background(), "user.bsky.social", "pass123")
	require.NoError(t, err)
	assert.Equal(t, "access-jwt", auth.AccessJwt)
	assert.Equal(t, "refresh-jwt", auth.RefreshJwt)
	assert.Equal(t, "did:plc:abc", auth.DID)

	// Session should be stored.
	assert.Equal(t, "access-jwt", c.Auth().AccessJwt)
}

func TestCreateSession_NoAuthHeaderSent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Even if client has an existing session, createSession must not send auth.
		assert.Empty(t, r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode(AuthInfo{AccessJwt: "new", RefreshJwt: "new-r"})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	c.SetAuth(&AuthInfo{AccessJwt: "existing-token"})
	_, err := c.CreateSession(context.Background(), "user", "pass")
	require.NoError(t, err)
}

func TestCreateSession_BadCreds(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "AuthenticationRequired", "message": "Invalid identifier or password"})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	_, err := c.CreateSession(context.Background(), "bad", "wrong")
	require.Error(t, err)
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, 401, xErr.StatusCode)
}

func TestRefreshSession_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/xrpc/com.atproto.server.refreshSession", r.URL.Path)
		// Must use refresh JWT, not access JWT.
		assert.Equal(t, "Bearer refresh-jwt", r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode(AuthInfo{
			AccessJwt:  "new-access",
			RefreshJwt: "new-refresh",
			Handle:     "user.bsky.social",
			DID:        "did:plc:abc",
		})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	c.SetAuth(&AuthInfo{AccessJwt: "old-access", RefreshJwt: "refresh-jwt"})

	auth, err := c.RefreshSession(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "new-access", auth.AccessJwt)
	assert.Equal(t, "new-refresh", auth.RefreshJwt)
	assert.Equal(t, "new-access", c.Auth().AccessJwt)
}

func TestRefreshSession_Failure_PreservesAuth(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "ExpiredToken"})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	c.SetAuth(&AuthInfo{AccessJwt: "old-access", RefreshJwt: "old-refresh"})

	_, err := c.RefreshSession(context.Background())
	require.Error(t, err)
	// Original auth should be preserved on failure.
	assert.Equal(t, "old-access", c.Auth().AccessJwt)
	assert.Equal(t, "old-refresh", c.Auth().RefreshJwt)
}

func TestRefreshSession_NoSession(t *testing.T) {
	t.Parallel()
	c := &Client{Host: "https://example.com", Retry: gt.Some(noRetry())}
	_, err := c.RefreshSession(context.Background())
	require.Error(t, err)
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, "NoSession", xErr.Name)
}

func TestDeleteSession_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/xrpc/com.atproto.server.deleteSession", r.URL.Path)
		assert.Equal(t, "Bearer refresh-jwt", r.Header.Get("Authorization"))
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	c.SetAuth(&AuthInfo{AccessJwt: "access", RefreshJwt: "refresh-jwt"})

	err := c.DeleteSession(context.Background())
	require.NoError(t, err)
	assert.Nil(t, c.Auth())
}

func TestDeleteSession_Failure_PreservesAuth(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "InternalError"})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	c.SetAuth(&AuthInfo{AccessJwt: "access", RefreshJwt: "refresh"})

	err := c.DeleteSession(context.Background())
	require.Error(t, err)
	// Auth should be preserved on failure.
	assert.Equal(t, "access", c.Auth().AccessJwt)
}

func TestDeleteSession_NoSession(t *testing.T) {
	t.Parallel()
	c := &Client{Host: "https://example.com", Retry: gt.Some(noRetry())}
	err := c.DeleteSession(context.Background())
	require.Error(t, err)
	var xErr *Error
	require.ErrorAs(t, err, &xErr)
	assert.Equal(t, "NoSession", xErr.Name)
}
