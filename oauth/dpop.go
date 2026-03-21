package oauth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atmos/crypto"

	// Import serviceauth to register ES256/ES256K signing methods with golang-jwt.
	_ "github.com/jcalabro/atmos/serviceauth"
)

// dpopClaims are the JWT claims for a DPoP proof.
type dpopClaims struct {
	jwt.RegisteredClaims
	HTM   string `json:"htm"`             // HTTP method
	HTU   string `json:"htu"`             // HTTP target URI (no query/fragment)
	Nonce string `json:"nonce,omitempty"` // Server-provided nonce
	ATH   string `json:"ath,omitempty"`   // Access token hash (resource server only)
}

// CreateDPoPProof creates a signed DPoP proof JWT per RFC 9449.
func CreateDPoPProof(key *crypto.P256PrivateKey, method, targetURL, nonce, accessToken string) (string, error) {
	// Generate cryptographically random jti.
	var jti [16]byte
	if _, err := rand.Read(jti[:]); err != nil {
		return "", fmt.Errorf("oauth: generate DPoP jti: %w", err)
	}

	claims := dpopClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ID:       base64.RawURLEncoding.EncodeToString(jti[:]),
			IssuedAt: jwt.NewNumericDate(time.Now()),
		},
		HTM:   method,
		HTU:   normalizeHTU(targetURL),
		Nonce: nonce,
	}

	// Compute access token hash if present (for resource server requests).
	if accessToken != "" {
		hash := sha256.Sum256([]byte(accessToken))
		claims.ATH = base64.RawURLEncoding.EncodeToString(hash[:])
	}

	pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	if !ok {
		return "", fmt.Errorf("oauth: DPoP key must be P-256")
	}
	jwk := PublicJWK(pub)

	token := jwt.NewWithClaims(jwt.GetSigningMethod("ES256"), claims)
	token.Header["typ"] = "dpop+jwt"
	token.Header["jwk"] = jwk

	return token.SignedString(key)
}

// normalizeHTU strips query string and fragment from a URL per RFC 9449.
func normalizeHTU(u string) string {
	// Find earliest '?' or '#' and truncate.
	minIdx := len(u)
	if i := strings.IndexByte(u, '?'); i >= 0 && i < minIdx {
		minIdx = i
	}
	if i := strings.IndexByte(u, '#'); i >= 0 && i < minIdx {
		minIdx = i
	}
	return u[:minIdx]
}

// NonceStore stores DPoP nonces per server origin.
// Safe for concurrent use.
type NonceStore struct {
	mu     sync.RWMutex
	nonces map[string]string
}

// NewNonceStore creates a new empty nonce store.
func NewNonceStore() *NonceStore {
	return &NonceStore{nonces: make(map[string]string)}
}

// Get returns the stored nonce for the given origin, or "".
func (s *NonceStore) Get(origin string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nonces[origin]
}

// Set stores a nonce for the given origin.
func (s *NonceStore) Set(origin string, nonce string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nonces[origin] = nonce
}

// originFromURL extracts the scheme+host portion of a URL.
func originFromURL(u string) string {
	// Find "://" then find the next "/" after it.
	idx := strings.Index(u, "://")
	if idx < 0 {
		return u
	}
	rest := u[idx+3:]
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return u[:idx+3+i]
	}
	return u
}

// TokenSource provides the current access token for DPoP-authenticated requests.
// Implementations must be safe for concurrent use.
type TokenSource interface {
	// Token returns the current access token and DPoP key.
	// It may refresh the token if stale.
	Token(ctx context.Context) (accessToken string, key *crypto.P256PrivateKey, err error)
}

// Transport is an http.RoundTripper that adds DPoP proof headers
// and handles nonce retry transparently. It uses a TokenSource to
// get the current (possibly refreshed) access token on each request.
type Transport struct {
	// Base is the underlying transport. If nil, http.DefaultTransport is used.
	Base http.RoundTripper

	// Source provides the current access token and DPoP key.
	Source TokenSource

	// Nonces stores per-origin DPoP nonces.
	Nonces *NonceStore
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	accessToken, dpopKey, err := t.Source.Token(req.Context())
	if err != nil {
		return nil, fmt.Errorf("oauth: get token: %w", err)
	}

	reqURL := req.URL.String()
	origin := originFromURL(reqURL)
	nonce := t.Nonces.Get(origin)

	// Create DPoP proof.
	proof, err := CreateDPoPProof(dpopKey, req.Method, reqURL, nonce, accessToken)
	if err != nil {
		return nil, err
	}

	req = req.Clone(req.Context())
	req.Header.Set("DPoP", proof)
	if accessToken != "" {
		req.Header.Set("Authorization", "DPoP "+accessToken)
	}

	resp, err := t.base().RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Always store the nonce from the response.
	newNonce := resp.Header.Get("DPoP-Nonce")
	if newNonce != "" {
		t.Nonces.Set(origin, newNonce)
	}

	// Check if this is a use_dpop_nonce error requiring retry.
	if newNonce != "" && newNonce != nonce && isUseDPoPNonceError(resp) {
		// Drain and close the original response body.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		// Retry with the new nonce.
		proof2, err := CreateDPoPProof(dpopKey, req.Method, reqURL, newNonce, accessToken)
		if err != nil {
			return nil, err
		}

		retryReq := req.Clone(req.Context())
		retryReq.Header.Set("DPoP", proof2)

		resp2, err := t.base().RoundTrip(retryReq)
		if err != nil {
			return nil, err
		}

		// Store any nonce from the retry response too.
		if retryNonce := resp2.Header.Get("DPoP-Nonce"); retryNonce != "" {
			t.Nonces.Set(origin, retryNonce)
		}
		return resp2, nil
	}

	return resp, nil
}

// StaticTokenSource is a TokenSource that always returns the same token.
// Useful for tests and short-lived operations.
type StaticTokenSource struct {
	AccessToken string
	Key         *crypto.P256PrivateKey
}

func (s *StaticTokenSource) Token(_ context.Context) (string, *crypto.P256PrivateKey, error) {
	return s.AccessToken, s.Key, nil
}

// isUseDPoPNonceError checks if a response is a use_dpop_nonce error.
// For authorization servers: HTTP 400 with {"error":"use_dpop_nonce"}.
// For resource servers: HTTP 401 with WWW-Authenticate containing error="use_dpop_nonce".
//
// For the 400 path, the body is read and replaced so it can be re-read by the caller.
func isUseDPoPNonceError(resp *http.Response) bool {
	if resp.StatusCode == http.StatusUnauthorized {
		wwwAuth := resp.Header.Get("WWW-Authenticate")
		return strings.HasPrefix(wwwAuth, "DPoP") && strings.Contains(wwwAuth, `error="use_dpop_nonce"`)
	}

	if resp.StatusCode == http.StatusBadRequest {
		// Read body to check JSON error, then replace it for the caller.
		body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024))
		if err != nil {
			return false
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		var oauthErr OAuthError
		return json.Unmarshal(body, &oauthErr) == nil && oauthErr.Code == "use_dpop_nonce"
	}

	return false
}
