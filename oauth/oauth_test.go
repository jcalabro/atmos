package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jcalabro/atmos/crypto"
)

// --- PKCE ---

func TestGeneratePKCE(t *testing.T) {
	t.Parallel()

	pkce, err := GeneratePKCE()
	require.NoError(t, err)

	assert.Equal(t, "S256", pkce.Method)
	assert.Len(t, pkce.Verifier, 43) // base64url(32 bytes) = 43 chars

	hash := sha256.Sum256([]byte(pkce.Verifier))
	expected := base64.RawURLEncoding.EncodeToString(hash[:])
	assert.Equal(t, expected, pkce.Challenge)
}

func TestGeneratePKCE_Uniqueness(t *testing.T) {
	t.Parallel()

	a, err := GeneratePKCE()
	require.NoError(t, err)
	b, err := GeneratePKCE()
	require.NoError(t, err)

	assert.NotEqual(t, a.Verifier, b.Verifier)
	assert.NotEqual(t, a.Challenge, b.Challenge)
}

// --- JWK ---

func TestPublicJWK(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	jwk := PublicJWK(pub)

	assert.Equal(t, "EC", jwk.KTY)
	assert.Equal(t, "P-256", jwk.CRV)

	x, err := base64.RawURLEncoding.DecodeString(jwk.X)
	require.NoError(t, err)
	assert.Len(t, x, 32)

	y, err := base64.RawURLEncoding.DecodeString(jwk.Y)
	require.NoError(t, err)
	assert.Len(t, y, 32)

	uncompressed := pub.UncompressedBytes()
	assert.Equal(t, uncompressed[1:33], x)
	assert.Equal(t, uncompressed[33:65], y)
}

func TestPublicJWK_Deterministic(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)

	jwk1 := PublicJWK(pub)
	jwk2 := PublicJWK(pub)
	assert.Equal(t, jwk1, jwk2)
}

// --- DPoP Proof ---

func TestCreateDPoPProof_Basic(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	proof, err := CreateDPoPProof(key, "POST", "https://as.example.com/token", "", "")
	require.NoError(t, err)
	assert.NotEmpty(t, proof)

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(proof, jwt.MapClaims{})
	require.NoError(t, err)

	assert.Equal(t, "dpop+jwt", token.Header["typ"])
	assert.Equal(t, "ES256", token.Header["alg"])

	jwkHeader, ok := token.Header["jwk"]
	require.True(t, ok)
	jwkMap, ok := jwkHeader.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "EC", jwkMap["kty"])
	assert.Equal(t, "P-256", jwkMap["crv"])
	assert.NotEmpty(t, jwkMap["x"])
	assert.NotEmpty(t, jwkMap["y"])

	claims, ok := token.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.Equal(t, "POST", claims["htm"])
	assert.Equal(t, "https://as.example.com/token", claims["htu"])
	assert.NotEmpty(t, claims["jti"])
	assert.NotNil(t, claims["iat"])
	assert.Nil(t, claims["nonce"])
	assert.Nil(t, claims["ath"])
}

func TestCreateDPoPProof_WithNonce(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	proof, err := CreateDPoPProof(key, "GET", "https://pds.example.com/xrpc/foo", "server-nonce-123", "")
	require.NoError(t, err)

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(proof, jwt.MapClaims{})
	require.NoError(t, err)

	claims, ok := token.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.Equal(t, "server-nonce-123", claims["nonce"])
}

func TestCreateDPoPProof_WithAccessToken(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	accessToken := "test-access-token-value"
	proof, err := CreateDPoPProof(key, "GET", "https://pds.example.com/xrpc/foo", "", accessToken)
	require.NoError(t, err)

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(proof, jwt.MapClaims{})
	require.NoError(t, err)

	claims, ok := token.Claims.(jwt.MapClaims)
	require.True(t, ok)

	hash := sha256.Sum256([]byte(accessToken))
	expectedATH := base64.RawURLEncoding.EncodeToString(hash[:])
	assert.Equal(t, expectedATH, claims["ath"])
}

func TestCreateDPoPProof_HTUNormalization(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	proof, err := CreateDPoPProof(key, "GET", "https://example.com/path?query=1#frag", "", "")
	require.NoError(t, err)

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(proof, jwt.MapClaims{})
	require.NoError(t, err)

	claims, ok := token.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.Equal(t, "https://example.com/path", claims["htu"])
}

func TestCreateDPoPProof_UniqueJTI(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	proof1, err := CreateDPoPProof(key, "POST", "https://example.com/token", "", "")
	require.NoError(t, err)
	proof2, err := CreateDPoPProof(key, "POST", "https://example.com/token", "", "")
	require.NoError(t, err)

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())

	t1, _, _ := parser.ParseUnverified(proof1, jwt.MapClaims{})
	t2, _, _ := parser.ParseUnverified(proof2, jwt.MapClaims{})

	c1, ok := t1.Claims.(jwt.MapClaims)
	require.True(t, ok)
	c2, ok := t2.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.NotEqual(t, c1["jti"], c2["jti"])
}

// --- Nonce Store ---

func TestNonceStore(t *testing.T) {
	t.Parallel()

	store := NewNonceStore()
	assert.Equal(t, "", store.Get("https://example.com"))

	store.Set("https://example.com", "nonce1")
	assert.Equal(t, "nonce1", store.Get("https://example.com"))

	store.Set("https://example.com", "nonce2")
	assert.Equal(t, "nonce2", store.Get("https://example.com"))

	assert.Equal(t, "", store.Get("https://other.com"))
}

func TestNonceStore_Concurrent(t *testing.T) {
	t.Parallel()

	store := NewNonceStore()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Set("https://example.com", strings.Repeat("x", i))
			store.Get("https://example.com")
		}()
	}
	wg.Wait()
}

// --- URL Helpers ---

func TestOriginFromURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"https://example.com/path", "https://example.com"},
		{"https://example.com:8080/path", "https://example.com:8080"},
		{"https://example.com", "https://example.com"},
		{"https://example.com/", "https://example.com"},
		{"http://localhost:3000/foo", "http://localhost:3000"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, originFromURL(tt.input), "input: %s", tt.input)
	}
}

func TestNormalizeHTU(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"https://example.com/path?query=1", "https://example.com/path"},
		{"https://example.com/path#frag", "https://example.com/path"},
		{"https://example.com/path?q=1#f", "https://example.com/path"},
		{"https://example.com/path", "https://example.com/path"},
		{"https://example.com/path#frag?notquery", "https://example.com/path"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, normalizeHTU(tt.input), "input: %s", tt.input)
	}
}

// --- Client Auth ---

func TestPublicClientAuth(t *testing.T) {
	t.Parallel()

	auth := &PublicClientAuth{ClientID: "https://app.example.com/meta.json"}
	params := make(map[string][]string)
	require.NoError(t, auth.Apply(params, "https://as.example.com"))

	assert.Equal(t, "https://app.example.com/meta.json", params["client_id"][0])
	assert.Empty(t, params["client_assertion"])
}

func TestConfidentialClientAuth(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	auth := &ConfidentialClientAuth{
		ClientID: "https://app.example.com/meta.json",
		Key:      key,
		KeyID:    "key-1",
	}

	params := make(map[string][]string)
	require.NoError(t, auth.Apply(params, "https://as.example.com"))

	assert.Equal(t, "https://app.example.com/meta.json", params["client_id"][0])
	assert.Equal(t, clientAssertionTypeJWTBearer, params["client_assertion_type"][0])
	assert.NotEmpty(t, params["client_assertion"][0])

	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(params["client_assertion"][0], jwt.MapClaims{})
	require.NoError(t, err)

	assert.Equal(t, "ES256", token.Header["alg"])
	assert.Equal(t, "key-1", token.Header["kid"])

	claims, ok := token.Claims.(jwt.MapClaims)
	require.True(t, ok)
	assert.Equal(t, "https://app.example.com/meta.json", claims["iss"])
	assert.Equal(t, "https://app.example.com/meta.json", claims["sub"])

	aud, ok := claims["aud"].([]any)
	require.True(t, ok)
	assert.Equal(t, "https://as.example.com", aud[0])

	assert.NotEmpty(t, claims["jti"])
	assert.NotNil(t, claims["exp"])
}

// --- Metadata Discovery ---

func TestFetchAuthServerMetadata(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/oauth-authorization-server" {
			http.NotFound(w, r)
			return
		}
		meta := map[string]any{
			"issuer":                                srvURL,
			"authorization_endpoint":                srvURL + "/oauth/authorize",
			"token_endpoint":                        srvURL + "/oauth/token",
			"pushed_authorization_request_endpoint": srvURL + "/oauth/par",
			"dpop_signing_alg_values_supported":     []string{"ES256"},
			"scopes_supported":                      []string{"atproto"},
			"client_id_metadata_document_supported": true,
			"require_pushed_authorization_requests": true,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	ctx := context.Background()
	meta, err := FetchAuthServerMetadata(ctx, srv.Client(), srv.URL)
	require.NoError(t, err)

	assert.Equal(t, srv.URL, meta.Issuer)
	assert.True(t, meta.ClientIDMetadataDocumentSupported)
	assert.True(t, meta.RequirePushedAuthorizationRequests)
	assert.Equal(t, srv.URL+"/oauth/token", meta.TokenEndpoint)
}

func TestFetchAuthServerMetadata_IssuerMismatch(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"issuer":                                "https://wrong.example.com",
			"authorization_endpoint":                "https://wrong.example.com/auth",
			"token_endpoint":                        "https://wrong.example.com/token",
			"pushed_authorization_request_endpoint": "https://wrong.example.com/par",
			"client_id_metadata_document_supported": true,
			"require_pushed_authorization_requests": true,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()

	_, err := FetchAuthServerMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestFetchAuthServerMetadata_MissingPAR(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"issuer":                                srvURL,
			"authorization_endpoint":                srvURL + "/auth",
			"token_endpoint":                        srvURL + "/token",
			"client_id_metadata_document_supported": true,
			"require_pushed_authorization_requests": true,
			// Missing pushed_authorization_request_endpoint
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	_, err := FetchAuthServerMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing pushed_authorization_request_endpoint")
}

func TestFetchAuthServerMetadata_NoClientIDDocument(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"issuer":                                srvURL,
			"authorization_endpoint":                srvURL + "/auth",
			"token_endpoint":                        srvURL + "/token",
			"pushed_authorization_request_endpoint": srvURL + "/par",
			"client_id_metadata_document_supported": false,
			"require_pushed_authorization_requests": true,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	_, err := FetchAuthServerMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support client_id_metadata_document")
}

func TestFetchAuthServerMetadata_NoPAR(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"issuer":                                srvURL,
			"authorization_endpoint":                srvURL + "/auth",
			"token_endpoint":                        srvURL + "/token",
			"pushed_authorization_request_endpoint": srvURL + "/par",
			"client_id_metadata_document_supported": true,
			"require_pushed_authorization_requests": false,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	_, err := FetchAuthServerMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not require PAR")
}

func TestFetchAuthServerMetadata_MissingTokenEndpoint(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"issuer":                                srvURL,
			"authorization_endpoint":                srvURL + "/auth",
			"pushed_authorization_request_endpoint": srvURL + "/par",
			"client_id_metadata_document_supported": true,
			"require_pushed_authorization_requests": true,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	_, err := FetchAuthServerMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing token_endpoint")
}

func TestFetchAuthServerMetadata_NoRedirect(t *testing.T) {
	t.Parallel()

	// Server that redirects — should fail since we don't follow redirects.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.com", http.StatusFound)
	}))
	defer srv.Close()

	_, err := FetchAuthServerMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
}

func TestFetchProtectedResourceMetadata(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/oauth-protected-resource" {
			http.NotFound(w, r)
			return
		}
		meta := map[string]any{
			"resource":              srvURL,
			"authorization_servers": []string{"https://auth.example.com"},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	meta, err := FetchProtectedResourceMetadata(context.Background(), srv.Client(), srv.URL)
	require.NoError(t, err)
	assert.Equal(t, []string{"https://auth.example.com"}, meta.AuthorizationServers)
}

func TestFetchProtectedResourceMetadata_ResourceMismatch(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"resource":              "https://evil.com",
			"authorization_servers": []string{"https://auth.example.com"},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()

	_, err := FetchProtectedResourceMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestFetchProtectedResourceMetadata_NoAuthServers(t *testing.T) {
	t.Parallel()

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		meta := map[string]any{
			"resource":              srvURL,
			"authorization_servers": []string{},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	}))
	defer srv.Close()
	srvURL = srv.URL

	_, err := FetchProtectedResourceMetadata(context.Background(), srv.Client(), srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no authorization_servers")
}

// --- Token Exchange ---

func TestExchangeCode(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.NotEmpty(t, r.Header.Get("DPoP"))

		err := r.ParseForm()
		require.NoError(t, err)
		assert.Equal(t, "authorization_code", r.Form.Get("grant_type"))
		assert.Equal(t, "test-code", r.Form.Get("code"))
		assert.Equal(t, "test-verifier", r.Form.Get("code_verifier"))

		w.Header().Set("DPoP-Nonce", "nonce1")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "at_test",
			"token_type":    "DPoP",
			"expires_in":    300,
			"refresh_token": "rt_test",
			"scope":         "atproto transition:generic",
			"sub":           "did:plc:test123",
		})
	}))
	defer srv.Close()

	nonces := NewNonceStore()
	ts, err := ExchangeCode(context.Background(), &ExchangeCodeConfig{
		TokenEndpoint:      srv.URL + "/oauth/token",
		RevocationEndpoint: srv.URL + "/oauth/revoke",
		Code:               "test-code",
		CodeVerifier:       "test-verifier",
		RedirectURI:        "https://app.example.com/callback",
		ClientAuth:         &PublicClientAuth{ClientID: "https://app.example.com/meta"},
		DPoPKey:            key,
		Nonces:             nonces,
		HTTPClient:         srv.Client(),
	})
	require.NoError(t, err)

	assert.Equal(t, "at_test", ts.AccessToken)
	assert.Equal(t, "rt_test", ts.RefreshToken)
	assert.Equal(t, "did:plc:test123", ts.Sub)
	assert.Equal(t, "atproto transition:generic", ts.Scope)
	assert.Equal(t, srv.URL+"/oauth/token", ts.TokenEndpoint)
	assert.Equal(t, srv.URL+"/oauth/revoke", ts.RevocationEndpoint)
	assert.False(t, ts.ExpiresAt.IsZero())
}

func TestExchangeCode_MissingScope(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at_test",
			"token_type":   "DPoP",
			"scope":        "openid", // Missing "atproto"
			"sub":          "did:plc:test123",
		})
	}))
	defer srv.Close()

	_, err = ExchangeCode(context.Background(), &ExchangeCodeConfig{
		TokenEndpoint: srv.URL + "/token",
		Code:          "code",
		ClientAuth:    &PublicClientAuth{ClientID: "c"},
		DPoPKey:       key,
		Nonces:        NewNonceStore(),
		HTTPClient:    srv.Client(),
	})
	assert.ErrorIs(t, err, ErrMissingScope)
}

func TestExchangeCode_MissingSub(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at_test",
			"token_type":   "DPoP",
			"scope":        "atproto",
			// Missing "sub"
		})
	}))
	defer srv.Close()

	_, err = ExchangeCode(context.Background(), &ExchangeCodeConfig{
		TokenEndpoint: srv.URL + "/token",
		Code:          "code",
		ClientAuth:    &PublicClientAuth{ClientID: "c"},
		DPoPKey:       key,
		Nonces:        NewNonceStore(),
		HTTPClient:    srv.Client(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing sub")
}

func TestExchangeCode_NonceRetry(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	var attempt atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempt.Add(1)
		if n == 1 {
			w.Header().Set("DPoP-Nonce", "fresh-nonce")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "use_dpop_nonce"})
			return
		}
		w.Header().Set("DPoP-Nonce", "fresh-nonce")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at_test",
			"token_type":   "DPoP",
			"scope":        "atproto",
			"sub":          "did:plc:test123",
		})
	}))
	defer srv.Close()

	nonces := NewNonceStore()
	ts, err := ExchangeCode(context.Background(), &ExchangeCodeConfig{
		TokenEndpoint: srv.URL + "/token",
		Code:          "code",
		ClientAuth:    &PublicClientAuth{ClientID: "c"},
		DPoPKey:       key,
		Nonces:        nonces,
		HTTPClient:    srv.Client(),
	})
	require.NoError(t, err)
	assert.Equal(t, "at_test", ts.AccessToken)
	assert.Equal(t, int32(2), attempt.Load())
	assert.Equal(t, "fresh-nonce", nonces.Get(originFromURL(srv.URL)))
}

func TestExchangeCode_OAuthError(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "code expired",
		})
	}))
	defer srv.Close()

	_, err = ExchangeCode(context.Background(), &ExchangeCodeConfig{
		TokenEndpoint: srv.URL + "/token",
		Code:          "expired-code",
		ClientAuth:    &PublicClientAuth{ClientID: "c"},
		DPoPKey:       key,
		Nonces:        NewNonceStore(),
		HTTPClient:    srv.Client(),
	})
	require.Error(t, err)
	var oauthErr *OAuthError
	require.ErrorAs(t, err, &oauthErr)
	assert.Equal(t, "invalid_grant", oauthErr.Code)
	assert.Equal(t, "code expired", oauthErr.Description)
}

func TestRefreshToken(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseForm()
		require.NoError(t, err)
		assert.Equal(t, "refresh_token", r.Form.Get("grant_type"))
		assert.Equal(t, "rt_old", r.Form.Get("refresh_token"))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "at_new",
			"token_type":    "DPoP",
			"expires_in":    300,
			"refresh_token": "rt_new",
			"scope":         "atproto",
			"sub":           "did:plc:test123",
		})
	}))
	defer srv.Close()

	ts, err := RefreshToken(context.Background(), &RefreshTokenConfig{
		TokenEndpoint:      srv.URL + "/token",
		RevocationEndpoint: srv.URL + "/revoke",
		RefreshToken:       "rt_old",
		ClientAuth:         &PublicClientAuth{ClientID: "c"},
		DPoPKey:            key,
		Nonces:             NewNonceStore(),
		HTTPClient:         srv.Client(),
	})
	require.NoError(t, err)
	assert.Equal(t, "at_new", ts.AccessToken)
	assert.Equal(t, "rt_new", ts.RefreshToken)
	assert.Equal(t, srv.URL+"/token", ts.TokenEndpoint)
	assert.Equal(t, srv.URL+"/revoke", ts.RevocationEndpoint)
}

// --- Token Staleness ---

func TestTokenSet_IsStale_Empty(t *testing.T) {
	t.Parallel()

	ts := &TokenSet{AccessToken: ""}
	assert.True(t, ts.IsStale(), "empty access token should be stale")
}

func TestTokenSet_IsStale_ZeroExpiry(t *testing.T) {
	t.Parallel()

	ts := &TokenSet{AccessToken: "at_test"}
	assert.False(t, ts.IsStale(), "zero expiry should not be stale")
}

func TestTokenSet_IsStale_FarFuture(t *testing.T) {
	t.Parallel()

	ts := &TokenSet{AccessToken: "at_test", ExpiresAt: time.Now().Add(1 * time.Hour)}
	assert.False(t, ts.IsStale(), "token expiring in 1h should not be stale")
}

func TestTokenSet_IsStale_Expired(t *testing.T) {
	t.Parallel()

	ts := &TokenSet{AccessToken: "at_test", ExpiresAt: time.Now().Add(-1 * time.Second)}
	assert.True(t, ts.IsStale(), "expired token should be stale")
}

// --- Session JSON ---

func TestSessionJSON_RoundTrip(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	original := &Session{
		DPoPKey: key,
		TokenSet: TokenSet{
			Issuer:             "https://bsky.social",
			Sub:                "did:plc:test123",
			AccessToken:        "at_test",
			TokenType:          "DPoP",
			Scope:              "atproto",
			TokenEndpoint:      "https://bsky.social/oauth/token",
			RevocationEndpoint: "https://bsky.social/oauth/revoke",
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored Session
	require.NoError(t, json.Unmarshal(data, &restored))

	assert.Equal(t, original.TokenSet, restored.TokenSet)
	assert.Equal(t, key.PublicKey().DIDKey(), restored.DPoPKey.PublicKey().DIDKey())
}

func TestAuthStateJSON_RoundTrip(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	original := &AuthState{
		Issuer:             "https://bsky.social",
		DPoPKey:            key,
		AuthMethod:         "none",
		Verifier:           "test-verifier",
		RedirectURI:        "https://app.example.com/callback",
		TokenEndpoint:      "https://bsky.social/oauth/token",
		RevocationEndpoint: "https://bsky.social/oauth/revoke",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored AuthState
	require.NoError(t, json.Unmarshal(data, &restored))

	assert.Equal(t, original.Issuer, restored.Issuer)
	assert.Equal(t, original.AuthMethod, restored.AuthMethod)
	assert.Equal(t, original.Verifier, restored.Verifier)
	assert.Equal(t, original.RevocationEndpoint, restored.RevocationEndpoint)
	assert.Equal(t, key.PublicKey().DIDKey(), restored.DPoPKey.PublicKey().DIDKey())
}

// --- Memory Stores ---

func TestMemorySessionStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemorySessionStore()

	_, err := store.GetSession(ctx, "did:plc:test")
	assert.ErrorIs(t, err, ErrNoSession)

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	session := &Session{DPoPKey: key, TokenSet: TokenSet{Sub: "did:plc:test", AccessToken: "at"}}
	require.NoError(t, store.SetSession(ctx, "did:plc:test", session))

	got, err := store.GetSession(ctx, "did:plc:test")
	require.NoError(t, err)
	assert.Equal(t, "at", got.TokenSet.AccessToken)

	require.NoError(t, store.DeleteSession(ctx, "did:plc:test"))
	_, err = store.GetSession(ctx, "did:plc:test")
	assert.ErrorIs(t, err, ErrNoSession)
}

func TestMemoryStateStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryStateStore()

	_, err := store.GetState(ctx, "state123")
	assert.ErrorIs(t, err, ErrInvalidState)

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	state := &AuthState{Issuer: "https://bsky.social", DPoPKey: key, Verifier: "v"}
	require.NoError(t, store.SetState(ctx, "state123", state))

	got, err := store.GetState(ctx, "state123")
	require.NoError(t, err)
	assert.Equal(t, "https://bsky.social", got.Issuer)

	require.NoError(t, store.DeleteState(ctx, "state123"))
	_, err = store.GetState(ctx, "state123")
	assert.ErrorIs(t, err, ErrInvalidState)
}

// --- DPoP Transport ---

func TestDPoPTransport_NonceRetry_401(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	var attempt atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempt.Add(1)
		assert.NotEmpty(t, r.Header.Get("DPoP"))

		if n == 1 {
			w.Header().Set("DPoP-Nonce", "server-nonce-abc")
			w.Header().Set("WWW-Authenticate", `DPoP error="use_dpop_nonce"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Verify nonce was included in retry.
		parser := jwt.NewParser(jwt.WithoutClaimsValidation())
		token, _, parseErr := parser.ParseUnverified(r.Header.Get("DPoP"), jwt.MapClaims{})
		require.NoError(t, parseErr)
		claims, ok := token.Claims.(jwt.MapClaims)
		require.True(t, ok)
		assert.Equal(t, "server-nonce-abc", claims["nonce"])

		w.Header().Set("DPoP-Nonce", "server-nonce-abc")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	nonces := NewNonceStore()
	transport := &Transport{
		Base:   srv.Client().Transport,
		Nonces: nonces,
		Source: &StaticTokenSource{AccessToken: "test-token", Key: key},
	}

	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/resource", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), attempt.Load())
	assert.Equal(t, "server-nonce-abc", nonces.Get(originFromURL(srv.URL)))
}

func TestDPoPTransport_NonceRetry_400_JSON(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	var attempt atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempt.Add(1)
		if n == 1 {
			w.Header().Set("DPoP-Nonce", "nonce-from-as")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "use_dpop_nonce"})
			return
		}
		w.Header().Set("DPoP-Nonce", "nonce-from-as")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	nonces := NewNonceStore()
	transport := &Transport{
		Base:   srv.Client().Transport,
		Nonces: nonces,
		Source: &StaticTokenSource{AccessToken: "test-token", Key: key},
	}

	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/token", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), attempt.Load())
}

func TestDPoPTransport_NoRetryOnSameNonce(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	nonces := NewNonceStore()
	nonces.Set("https://example.com", "already-known")

	var attempt atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt.Add(1)
		// Return same nonce with a 401 — should NOT retry since nonce didn't change.
		w.Header().Set("DPoP-Nonce", "already-known")
		w.Header().Set("WWW-Authenticate", `DPoP error="use_dpop_nonce"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	// Override the origin to match what the nonce store has.
	nonces.Set(originFromURL(srv.URL), "already-known")

	transport := &Transport{
		Base:   srv.Client().Transport,
		Nonces: nonces,
		Source: &StaticTokenSource{AccessToken: "test-token", Key: key},
	}

	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/resource", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, int32(1), attempt.Load(), "should not retry when nonce is the same")
}

func TestDPoPTransport_SetsAuthorizationHeader(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		assert.Equal(t, "DPoP my-token", auth)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	transport := &Transport{
		Base:   srv.Client().Transport,
		Nonces: NewNonceStore(),
		Source: &StaticTokenSource{AccessToken: "my-token", Key: key},
	}

	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/api", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
}

func TestDPoPTransport_NoAuthHeaderWithoutToken(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		assert.Empty(t, auth)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	transport := &Transport{
		Base:   srv.Client().Transport,
		Nonces: NewNonceStore(),
		Source: &StaticTokenSource{AccessToken: "", Key: key},
	}

	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/api", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
}

// --- Callback Validation ---

func TestCallback_MissingIss(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	stateStore := NewMemoryStateStore()
	_ = stateStore.SetState(context.Background(), "s1", &AuthState{
		Issuer:  "https://as.example.com",
		DPoPKey: key,
	})

	c := &Client{
		StateStore: stateStore,
	}

	_, err = c.Callback(context.Background(), CallbackParams{
		Code:  "code",
		State: "s1",
		Iss:   "", // Missing
	})
	assert.ErrorIs(t, err, ErrMissingIssuer)
}

func TestCallback_IssuerMismatch(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	stateStore := NewMemoryStateStore()
	_ = stateStore.SetState(context.Background(), "s1", &AuthState{
		Issuer:  "https://as.example.com",
		DPoPKey: key,
	})

	c := &Client{
		StateStore: stateStore,
	}

	_, err = c.Callback(context.Background(), CallbackParams{
		Code:  "code",
		State: "s1",
		Iss:   "https://evil.example.com",
	})
	assert.ErrorIs(t, err, ErrIssuerMismatch)
}

func TestCallback_InvalidState(t *testing.T) {
	t.Parallel()

	c := &Client{
		StateStore: NewMemoryStateStore(),
	}

	_, err := c.Callback(context.Background(), CallbackParams{
		Code:  "code",
		State: "nonexistent",
		Iss:   "https://as.example.com",
	})
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestCallback_StateConsumedOnUse(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	stateStore := NewMemoryStateStore()
	_ = stateStore.SetState(context.Background(), "s1", &AuthState{
		Issuer:  "https://as.example.com",
		DPoPKey: key,
	})

	c := &Client{
		StateStore: stateStore,
	}

	// First call consumes the state (will fail on token exchange, but state is deleted).
	_, _ = c.Callback(context.Background(), CallbackParams{Code: "code", State: "s1", Iss: "https://as.example.com"})

	// Second call should get ErrInvalidState.
	_, err = c.Callback(context.Background(), CallbackParams{Code: "code", State: "s1", Iss: "https://as.example.com"})
	assert.ErrorIs(t, err, ErrInvalidState)
}

// --- OAuthError ---

func TestOAuthError_Format(t *testing.T) {
	t.Parallel()

	err := &OAuthError{Code: "invalid_grant", Description: "code expired"}
	assert.Equal(t, "oauth: invalid_grant: code expired", err.Error())

	err2 := &OAuthError{Code: "invalid_request"}
	assert.Equal(t, "oauth: invalid_request", err2.Error())
}

// --- isUseDPoPNonceError ---

func TestIsUseDPoPNonceError_400JSON(t *testing.T) {
	t.Parallel()

	body := `{"error":"use_dpop_nonce","error_description":"nonce required"}`
	resp := &http.Response{
		StatusCode: 400,
		Header:     http.Header{"DPoP-Nonce": {"nonce1"}},
		Body:       nopCloser(body),
	}

	assert.True(t, isUseDPoPNonceError(resp))
}

func TestIsUseDPoPNonceError_400NotNonce(t *testing.T) {
	t.Parallel()

	body := `{"error":"invalid_grant"}`
	resp := &http.Response{
		StatusCode: 400,
		Header:     http.Header{},
		Body:       nopCloser(body),
	}

	assert.False(t, isUseDPoPNonceError(resp))
}

func TestIsUseDPoPNonceError_401WWWAuth(t *testing.T) {
	t.Parallel()

	resp := &http.Response{
		StatusCode: 401,
		Header:     http.Header{"Www-Authenticate": {`DPoP error="use_dpop_nonce"`}},
		Body:       nopCloser(""),
	}

	assert.True(t, isUseDPoPNonceError(resp))
}

func TestIsUseDPoPNonceError_401WrongError(t *testing.T) {
	t.Parallel()

	resp := &http.Response{
		StatusCode: 401,
		Header:     http.Header{"Www-Authenticate": {`DPoP error="invalid_token"`}},
		Body:       nopCloser(""),
	}

	assert.False(t, isUseDPoPNonceError(resp))
}

func TestIsUseDPoPNonceError_401NotDPoP(t *testing.T) {
	t.Parallel()

	resp := &http.Response{
		StatusCode: 401,
		Header:     http.Header{"Www-Authenticate": {`Bearer error="use_dpop_nonce"`}},
		Body:       nopCloser(""),
	}

	assert.False(t, isUseDPoPNonceError(resp))
}

func TestIsUseDPoPNonceError_200(t *testing.T) {
	t.Parallel()

	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       nopCloser(""),
	}

	assert.False(t, isUseDPoPNonceError(resp))
}

func nopCloser(s string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(s))
}

// --- Fuzz Tests ---

func FuzzNormalizeHTU(f *testing.F) {
	f.Add("https://example.com/path?query=1#frag")
	f.Add("https://example.com")
	f.Add("")
	f.Add("?")
	f.Add("#")
	f.Add("https://example.com/path#frag?notquery")

	f.Fuzz(func(_ *testing.T, input string) {
		result := normalizeHTU(input)
		// Result must not contain ? or #
		if strings.ContainsAny(result, "?#") {
			panic("normalizeHTU result contains ? or #")
		}
		// Result must be a prefix of input
		if !strings.HasPrefix(input, result) {
			panic("normalizeHTU result is not a prefix of input")
		}
	})
}

func FuzzOriginFromURL(f *testing.F) {
	f.Add("https://example.com/path")
	f.Add("https://example.com:8080/path")
	f.Add("http://localhost:3000/foo/bar")
	f.Add("")
	f.Add("noscheme")

	f.Fuzz(func(_ *testing.T, input string) {
		result := originFromURL(input)
		// Result must not be longer than input
		if len(result) > len(input) {
			panic("origin longer than input")
		}
		// Result must be a prefix of input
		if !strings.HasPrefix(input, result) {
			panic("origin is not a prefix of input")
		}
		// Result must not contain a path separator after the authority
		if idx := strings.Index(result, "://"); idx >= 0 {
			rest := result[idx+3:]
			if strings.Contains(rest, "/") {
				panic("origin contains path")
			}
		}
	})
}

func FuzzParseTokenError(f *testing.F) {
	f.Add(400, []byte(`{"error":"invalid_grant","error_description":"bad code"}`))
	f.Add(500, []byte(`internal server error`))
	f.Add(400, []byte(`{}`))
	f.Add(400, []byte(`not json`))
	f.Add(200, []byte(`{"error":"should_not_happen"}`))

	f.Fuzz(func(t *testing.T, statusCode int, body []byte) {
		err := parseTokenError(statusCode, body)
		if err == nil {
			t.Fatal("parseTokenError should always return an error")
		}
	})
}

func FuzzTokenResponseParsing(f *testing.F) {
	f.Add([]byte(`{"access_token":"at","token_type":"DPoP","scope":"atproto","sub":"did:plc:test"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"access_token":"","scope":"atproto","sub":"did:plc:x"}`))

	f.Fuzz(func(t *testing.T, body []byte) {
		var resp tokenResponse
		if json.Unmarshal(body, &resp) != nil {
			return // invalid JSON, skip
		}
		// Just verify we don't panic when checking fields
		_ = resp.AccessToken == ""
		_ = resp.Sub == ""
		_ = strings.Contains(resp.Scope, "atproto")
	})
}

func FuzzMetadataParsing(f *testing.F) {
	f.Add([]byte(`{"issuer":"https://example.com","authorization_endpoint":"https://example.com/auth","token_endpoint":"https://example.com/token","pushed_authorization_request_endpoint":"https://example.com/par","client_id_metadata_document_supported":true,"require_pushed_authorization_requests":true}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))

	f.Fuzz(func(t *testing.T, body []byte) {
		var meta AuthServerMetadata
		_ = json.Unmarshal(body, &meta)
		// Verify validation doesn't panic
		_ = meta.Issuer
		_ = meta.TokenEndpoint
		_ = meta.PushedAuthorizationRequestEndpoint
	})
}

func FuzzCallbackParams(f *testing.F) {
	f.Add("code123", "state456", "https://as.example.com")
	f.Add("", "", "")
	f.Add("code", "state", "")

	f.Fuzz(func(t *testing.T, code, state, iss string) {
		params := CallbackParams{Code: code, State: state, Iss: iss}
		// Should never panic, even with garbage input.
		c := &Client{StateStore: NewMemoryStateStore()}
		_, err := c.Callback(context.Background(), params)
		// We always expect an error since there's no matching state.
		if err == nil {
			t.Fatal("expected error from Callback with empty state store")
		}
	})
}

func FuzzPKCEVerifierChallenge(f *testing.F) {
	f.Fuzz(func(t *testing.T, _ []byte) {
		pkce, err := GeneratePKCE()
		if err != nil {
			return
		}
		// Verify challenge is always SHA-256 of verifier
		hash := sha256.Sum256([]byte(pkce.Verifier))
		expected := base64.RawURLEncoding.EncodeToString(hash[:])
		if pkce.Challenge != expected {
			t.Fatalf("PKCE challenge mismatch")
		}
		// Verify verifier is valid base64url
		_, err = base64.RawURLEncoding.DecodeString(pkce.Verifier)
		if err != nil {
			t.Fatalf("PKCE verifier is not valid base64url: %v", err)
		}
	})
}

func FuzzSessionJSON(f *testing.F) {
	key, _ := crypto.GenerateP256()
	sess := &Session{
		DPoPKey:  key,
		TokenSet: TokenSet{Issuer: "https://x.com", Sub: "did:plc:x", AccessToken: "at", Scope: "atproto"},
	}
	data, _ := json.Marshal(sess)
	f.Add(data)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"dpop_key":"","token_set":{}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var s Session
		err := json.Unmarshal(data, &s)
		if err != nil {
			return
		}
		// If it parsed, re-marshal should not panic
		_, _ = json.Marshal(&s)
	})
}
