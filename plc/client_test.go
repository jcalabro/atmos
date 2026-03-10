package plc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	// Use srv.Client() to avoid sharing http.DefaultTransport across
	// parallel tests. httptest.Server.Close calls CloseIdleConnections
	// on the default transport, racing with other tests' in-flight requests.
	return NewClient(ClientConfig{
		DirectoryURL: gt.Some(srv.URL),
		HTTPClient:   gt.Some(srv.Client()),
	})
}

func TestClientResolve(t *testing.T) {
	t.Parallel()

	doc := `{
		"id": "did:plc:testdid123",
		"alsoKnownAs": ["at://alice.bsky.social"],
		"verificationMethod": [],
		"service": []
	}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/did:plc:testdid123", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		assert.Equal(t, "atmos/v0.1", r.Header.Get("User-Agent"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(doc))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	result, err := c.Resolve(context.Background(), "did:plc:testdid123")
	require.NoError(t, err)
	assert.Equal(t, "did:plc:testdid123", result.ID)
	assert.Equal(t, []string{"at://alice.bsky.social"}, result.AlsoKnownAs)
}

func TestClientOpLog(t *testing.T) {
	t.Parallel()

	ops := `[{"type":"plc_operation"},{"type":"plc_operation"}]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/did:plc:test123/log", r.URL.Path)
		assert.Equal(t, "atmos/v0.1", r.Header.Get("User-Agent"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(ops))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	result, err := c.OpLog(context.Background(), "did:plc:test123")
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestClientAuditLog(t *testing.T) {
	t.Parallel()

	entries := `[{"did":"did:plc:test123","operation":{},"cid":"bafytest","nullified":false,"createdAt":"2024-01-01T00:00:00Z"}]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/did:plc:test123/log/audit", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(entries))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	result, err := c.AuditLog(context.Background(), "did:plc:test123")
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "did:plc:test123", result[0].DID)
	assert.False(t, result[0].Nullified)
}

func TestClientSubmit(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/did:plc:test123", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "atmos/v0.1", r.Header.Get("User-Agent"))

		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "plc_operation", body["type"])

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := testClient(t, srv)
	op := &Operation{Type: "plc_operation"}
	err := c.Submit(context.Background(), "did:plc:test123", op)
	require.NoError(t, err)
}

func TestClientSubmitTombstone(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "plc_tombstone", body["type"])
		assert.Equal(t, "bafyreiabc123", body["prev"])
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := testClient(t, srv)
	ts := NewTombstoneOp("bafyreiabc123")
	err := c.Submit(context.Background(), "did:plc:test123", ts)
	require.NoError(t, err)
}

func TestClientResolveNotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := testClient(t, srv)
	_, err := c.Resolve(context.Background(), "did:plc:nonexistent")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestClientSubmitError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid operation"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	err := c.Submit(context.Background(), "did:plc:test123", &Operation{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestNewClientDefaults(t *testing.T) {
	t.Parallel()

	c := NewClient(ClientConfig{})
	assert.Equal(t, "https://plc.directory", c.directoryURL)
	assert.NotNil(t, c.httpClient)
	assert.Equal(t, "atmos/v0.1", c.userAgent)
}

func TestNewClientCustomUserAgent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "custom-agent/1.0", r.Header.Get("User-Agent"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"did:plc:test","alsoKnownAs":[],"verificationMethod":[],"service":[]}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{
		DirectoryURL: gt.Some(srv.URL),
		UserAgent:    gt.Some("custom-agent/1.0"),
		HTTPClient:   gt.Some(srv.Client()),
	})
	_, err := c.Resolve(context.Background(), "did:plc:test")
	require.NoError(t, err)
}

func TestClientNotFoundSentinel(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := testClient(t, srv)
	_, err := c.OpLog(context.Background(), "did:plc:nonexistent")
	assert.True(t, errors.Is(err, ErrNotFound))

	_, err = c.AuditLog(context.Background(), "did:plc:nonexistent")
	assert.True(t, errors.Is(err, ErrNotFound))
}
