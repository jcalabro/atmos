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
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
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
	if key == nil {
		return "", fmt.Errorf("oauth: DPoP key is required")
	}
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

// RefreshingTokenSource is an optional capability for a TokenSource: forcing a
// token refresh after the resource server rejects the current access token with
// a 401 invalid_token (the token was revoked or expired earlier than the local
// staleness estimate). staleToken is the token that was rejected; if it no
// longer matches the current token (another goroutine already refreshed), the
// implementation returns the current token without a redundant refresh.
type RefreshingTokenSource interface {
	TokenSource
	Refresh(ctx context.Context, staleToken string) (accessToken string, key *crypto.P256PrivateKey, err error)
}

// Transport is an http.RoundTripper that adds DPoP proof headers
// and handles nonce retry transparently. It uses a TokenSource to
// get the current (possibly refreshed) access token on each request.
// A fresh proof is created for every wire send, including the standard
// library's transparent HTTP/1.1 and HTTP/2 connection-failure retries,
// so a proof JTI is never reused on the wire.
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

// roundTripFreshProofs sends one logical request whose DPoP header has already
// been set for the first wire attempt, and re-signs before every
// transport-selected replacement wire send: Go's transparent HTTP/1.1
// reused-connection replays and HTTP/2 REFUSED_STREAM/GOAWAY retries bypass
// RoundTrip, so without this hook they would carry the previous proof. A
// failed re-sign removes the credential headers and cancels the attempt
// rather than letting the old proof replay.
func (t *Transport) roundTripFreshProofs(req *http.Request, resign func() (string, error)) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(req.Context())
	var connections atomic.Uint32
	var signMu sync.Mutex
	var signErr error
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) {
		if connections.Add(1) == 1 {
			return
		}
		proof, err := resign()
		if err != nil {
			req.Header.Del("Authorization")
			req.Header.Del("DPoP")
			signMu.Lock()
			if signErr == nil {
				signErr = err
			}
			signMu.Unlock()
			cancel(err)
			return
		}
		req.Header.Set("DPoP", proof)
	}}
	traced := req.WithContext(httptrace.WithClientTrace(ctx, trace))
	resp, err := t.base().RoundTrip(traced)
	signMu.Lock()
	deferredSignErr := signErr
	signMu.Unlock()
	if deferredSignErr != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel(deferredSignErr)
		return nil, fmt.Errorf("oauth: sign transparent retry: %w", deferredSignErr)
	}
	if err != nil || resp == nil || resp.Body == nil {
		cancel(err)
		return resp, err
	}
	// The body keeps the traced context alive through close/EOF so the
	// connection remains poolable.
	resp.Body = &cancelContextBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelContextBody struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
	once   sync.Once
}

func (b *cancelContextBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(func() { b.cancel(nil) })
	}
	return n, err
}

func (b *cancelContextBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.cancel(nil) })
	return err
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.Source == nil {
		return nil, fmt.Errorf("oauth: DPoP token source is required")
	}
	if t.Nonces == nil {
		return nil, fmt.Errorf("oauth: DPoP nonce store is required")
	}
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

	resp, err := t.roundTripFreshProofs(req, func() (string, error) {
		return CreateDPoPProof(dpopKey, req.Method, reqURL, nonce, accessToken)
	})
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
		retryReq, err := cloneRequestForRetry(req)
		if err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		// Drain and close the original response body.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		// Retry with the new nonce.
		proof2, err := CreateDPoPProof(dpopKey, req.Method, reqURL, newNonce, accessToken)
		if err != nil {
			if retryReq.Body != nil {
				_ = retryReq.Body.Close()
			}
			return nil, err
		}

		retryReq.Header.Set("DPoP", proof2)

		resp2, err := t.roundTripFreshProofs(retryReq, func() (string, error) {
			return CreateDPoPProof(dpopKey, req.Method, reqURL, newNonce, accessToken)
		})
		if err != nil {
			return nil, err
		}

		// Store any nonce from the retry response too.
		if retryNonce := resp2.Header.Get("DPoP-Nonce"); retryNonce != "" {
			t.Nonces.Set(origin, retryNonce)
		}
		resp = resp2
	}

	// Reactive refresh: the resource server rejected the access token with a 401
	// invalid_token (revoked/expired before the local staleness estimate). If the
	// source can refresh, do so once and retry. Without this the caller would see
	// a spurious 401 even though a valid refresh token exists.
	if rs, ok := t.Source.(RefreshingTokenSource); ok && isInvalidTokenError(resp) {
		newToken, newKey, err := rs.Refresh(req.Context(), accessToken)
		if err != nil {
			return nil, fmt.Errorf("oauth: refresh after invalid_token: %w", err)
		}
		// Only retry if the token actually changed; otherwise return the original
		// 401 (its body is still intact) rather than hot-looping on a token the
		// server keeps rejecting.
		if newToken == "" || newToken == accessToken {
			return resp, nil
		}
		retryReq, err := cloneRequestForRetry(req)
		if err != nil {
			_ = resp.Body.Close()
			return nil, err
		}

		// Commit to the retry: drain and close the original 401 response.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		retryNonce := t.Nonces.Get(origin)
		proof2, err := CreateDPoPProof(newKey, req.Method, reqURL, retryNonce, newToken)
		if err != nil {
			if retryReq.Body != nil {
				_ = retryReq.Body.Close()
			}
			return nil, err
		}
		retryReq.Header.Set("DPoP", proof2)
		retryReq.Header.Set("Authorization", "DPoP "+newToken)

		resp2, err := t.roundTripFreshProofs(retryReq, func() (string, error) {
			return CreateDPoPProof(newKey, req.Method, reqURL, retryNonce, newToken)
		})
		if err != nil {
			return nil, err
		}
		if n := resp2.Header.Get("DPoP-Nonce"); n != "" {
			t.Nonces.Set(origin, n)
		}
		return resp2, nil
	}

	return resp, nil
}

func cloneRequestForRetry(req *http.Request) (*http.Request, error) {
	retry := req.Clone(req.Context())
	if req.Body == nil || req.Body == http.NoBody {
		return retry, nil
	}
	if req.GetBody == nil {
		return nil, ErrNonReplayableRequest
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNonReplayableRequest, err)
	}
	retry.Body = body
	return retry, nil
}

// isInvalidTokenError reports whether resp is a 401 with a DPoP/Bearer
// WWW-Authenticate challenge carrying error="invalid_token" — the resource
// server signaling that the access token is no longer valid.
func isInvalidTokenError(resp *http.Response) bool {
	if resp.StatusCode != http.StatusUnauthorized {
		return false
	}
	wwwAuth := resp.Header.Get("WWW-Authenticate")
	return strings.Contains(wwwAuth, `error="invalid_token"`)
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
