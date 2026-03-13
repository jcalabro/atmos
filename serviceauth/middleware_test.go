package serviceauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiddleware_ValidToken(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:    "did:plc:alice",
		Audience:  "did:web:api.example.com",
		Exp:       time.Now().Add(60 * time.Second),
		LexMethod: "com.atproto.sync.getBlob",
	}, priv)
	require.NoError(t, err)

	var gotDID atmos.DID
	handler := Middleware(VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotDID = DIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.sync.getBlob", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, atmos.DID("did:plc:alice"), gotDID)
}

func TestMiddleware_MissingToken_Required(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	handler := Middleware(VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.sync.getBlob", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestMiddleware_MissingToken_Optional(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	var gotDID atmos.DID
	handler := Middleware(VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	}, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotDID = DIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.sync.getBlob", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, atmos.DID(""), gotDID)
}

func TestMiddleware_InvalidToken_Required(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	handler := Middleware(VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.sync.getBlob", nil)
	req.Header.Set("Authorization", "Bearer not.a.valid.jwt")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestMiddleware_InvalidToken_Optional(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	var called bool
	handler := Middleware(VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	}, false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.sync.getBlob", nil)
	req.Header.Set("Authorization", "Bearer garbage")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, called)
}

func TestMiddleware_ExtractsNSIDFromPath(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	// Token bound to specific method.
	token, err := CreateToken(TokenParams{
		Issuer:    "did:plc:alice",
		Audience:  "did:web:api.example.com",
		Exp:       time.Now().Add(60 * time.Second),
		LexMethod: "com.atproto.sync.getBlob",
	}, priv)
	require.NoError(t, err)

	handler := Middleware(VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Matching path — should succeed.
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.sync.getBlob", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Mismatching path — should fail.
	req2 := httptest.NewRequestWithContext(t.Context(), "GET", "/xrpc/com.atproto.repo.getRecord", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusUnauthorized, rec2.Code)
}

func TestDIDFromContext_Empty(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/", nil)
	assert.Equal(t, atmos.DID(""), DIDFromContext(req.Context()))
}

func TestExtractNSID(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "com.atproto.sync.getBlob", extractNSID("/xrpc/com.atproto.sync.getBlob"))
	assert.Equal(t, "", extractNSID("/other/path"))
	assert.Equal(t, "", extractNSID(""))
}
