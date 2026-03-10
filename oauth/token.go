package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jcalabro/atmos/crypto"
)

// TokenSet holds the tokens from an OAuth token response.
type TokenSet struct {
	Issuer             string    `json:"iss"`
	Sub                string    `json:"sub"`
	Aud                string    `json:"aud"`
	Scope              string    `json:"scope"`
	AccessToken        string    `json:"access_token"`
	TokenType          string    `json:"token_type"`
	ExpiresAt          time.Time `json:"expires_at"`
	RefreshDeadline    time.Time `json:"refresh_deadline"` // ExpiresAt minus random jitter; refresh when Now > this
	RefreshToken       string    `json:"refresh_token,omitempty"`
	TokenEndpoint      string    `json:"token_endpoint"`
	RevocationEndpoint string    `json:"revocation_endpoint,omitempty"`
}

// IsStale returns true if the token should be refreshed.
// The refresh deadline is precomputed with random jitter when the token
// is received, so this check is a simple time comparison with no syscalls.
func (t *TokenSet) IsStale() bool {
	if t.AccessToken == "" {
		return true
	}
	if t.RefreshDeadline.IsZero() {
		// No deadline set — fall back to expiry check.
		if t.ExpiresAt.IsZero() {
			return false
		}
		return time.Until(t.ExpiresAt) < 10*time.Second
	}
	return time.Now().After(t.RefreshDeadline)
}

// tokenResponse is the raw JSON response from the token endpoint.
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope"`
	Sub          string `json:"sub"`
}

// ExchangeCodeConfig configures a token exchange request.
type ExchangeCodeConfig struct {
	TokenEndpoint      string
	RevocationEndpoint string
	Code               string
	CodeVerifier       string
	RedirectURI        string
	ClientAuth         ClientAuth
	DPoPKey            *crypto.P256PrivateKey
	Nonces             *NonceStore
	HTTPClient         *http.Client
}

// ExchangeCode exchanges an authorization code for tokens.
func ExchangeCode(ctx context.Context, cfg *ExchangeCodeConfig) (*TokenSet, error) {
	params := url.Values{}
	params.Set("grant_type", "authorization_code")
	params.Set("code", cfg.Code)
	params.Set("code_verifier", cfg.CodeVerifier)
	params.Set("redirect_uri", cfg.RedirectURI)

	ts, err := doTokenRequest(ctx, cfg.TokenEndpoint, params, cfg.ClientAuth, cfg.DPoPKey, cfg.Nonces, cfg.HTTPClient)
	if err != nil {
		return nil, err
	}
	ts.TokenEndpoint = cfg.TokenEndpoint
	ts.RevocationEndpoint = cfg.RevocationEndpoint
	return ts, nil
}

// RefreshTokenConfig configures a token refresh request.
type RefreshTokenConfig struct {
	TokenEndpoint      string
	RevocationEndpoint string
	RefreshToken       string
	ClientAuth         ClientAuth
	DPoPKey            *crypto.P256PrivateKey
	Nonces             *NonceStore
	HTTPClient         *http.Client
}

// RefreshToken exchanges a refresh token for new tokens.
func RefreshToken(ctx context.Context, cfg *RefreshTokenConfig) (*TokenSet, error) {
	params := url.Values{}
	params.Set("grant_type", "refresh_token")
	params.Set("refresh_token", cfg.RefreshToken)

	ts, err := doTokenRequest(ctx, cfg.TokenEndpoint, params, cfg.ClientAuth, cfg.DPoPKey, cfg.Nonces, cfg.HTTPClient)
	if err != nil {
		return nil, err
	}
	ts.TokenEndpoint = cfg.TokenEndpoint
	ts.RevocationEndpoint = cfg.RevocationEndpoint
	return ts, nil
}

// RevokeToken revokes a token. Errors are silently ignored per spec.
func RevokeToken(ctx context.Context, endpoint, token string, auth ClientAuth, dpopKey *crypto.P256PrivateKey, nonces *NonceStore, httpClient *http.Client) {
	if endpoint == "" || token == "" {
		return
	}

	params := url.Values{}
	params.Set("token", token)

	origin := originFromURL(endpoint)
	if err := auth.Apply(params, origin); err != nil {
		return
	}

	nonce := nonces.Get(origin)
	proof, err := CreateDPoPProof(dpopKey, "POST", endpoint, nonce, "")
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(params.Encode()))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("DPoP", proof)

	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func doTokenRequest(ctx context.Context, endpoint string, params url.Values, auth ClientAuth, dpopKey *crypto.P256PrivateKey, nonces *NonceStore, httpClient *http.Client) (*TokenSet, error) {
	origin := originFromURL(endpoint)

	if err := auth.Apply(params, origin); err != nil {
		return nil, err
	}

	nonce := nonces.Get(origin)

	proof, err := CreateDPoPProof(dpopKey, "POST", endpoint, nonce, "")
	if err != nil {
		return nil, err
	}

	body, respHeaders, err := doTokenHTTPWithNonceRetry(ctx, httpClient, endpoint, params.Encode(), proof, dpopKey, nonces)
	if err != nil {
		return nil, err
	}
	_ = respHeaders

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("oauth: parse token response: %w", err)
	}

	// Validate required fields.
	if tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("oauth: token response missing access_token")
	}
	if tokenResp.Sub == "" {
		return nil, fmt.Errorf("oauth: token response missing sub")
	}
	if !strings.Contains(tokenResp.Scope, "atproto") {
		return nil, ErrMissingScope
	}
	if tokenResp.TokenType != "" && !strings.EqualFold(tokenResp.TokenType, "DPoP") {
		return nil, fmt.Errorf("oauth: unexpected token_type %q, expected DPoP", tokenResp.TokenType)
	}

	var expiresAt, refreshDeadline time.Time
	if tokenResp.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
		// Precompute refresh deadline with jitter: refresh 10-40s before expiry.
		// Use a simple hash-based jitter to avoid crypto/rand on every check.
		jitter := time.Duration(10+tokenResp.ExpiresIn%30) * time.Second
		refreshDeadline = expiresAt.Add(-jitter)
	}

	return &TokenSet{
		Issuer:          origin,
		Sub:             tokenResp.Sub,
		Scope:           tokenResp.Scope,
		AccessToken:     tokenResp.AccessToken,
		TokenType:       tokenResp.TokenType,
		ExpiresAt:       expiresAt,
		RefreshDeadline: refreshDeadline,
		RefreshToken:    tokenResp.RefreshToken,
	}, nil
}

// doTokenHTTPWithNonceRetry performs the token HTTP request with one nonce retry.
func doTokenHTTPWithNonceRetry(ctx context.Context, client *http.Client, endpoint, formBody, proof string, dpopKey *crypto.P256PrivateKey, nonces *NonceStore) ([]byte, http.Header, error) {
	origin := originFromURL(endpoint)

	body, headers, statusCode, err := doTokenHTTPOnce(ctx, client, endpoint, formBody, proof)
	if err != nil {
		return nil, nil, err
	}

	// Store nonce from response.
	if newNonce := headers.Get("DPoP-Nonce"); newNonce != "" {
		nonces.Set(origin, newNonce)

		// Retry on use_dpop_nonce error.
		if statusCode == http.StatusBadRequest {
			var oauthErr OAuthError
			if json.Unmarshal(body, &oauthErr) == nil && oauthErr.Code == "use_dpop_nonce" {
				proof2, err := CreateDPoPProof(dpopKey, "POST", endpoint, newNonce, "")
				if err != nil {
					return nil, nil, err
				}
				body2, headers2, statusCode2, err := doTokenHTTPOnce(ctx, client, endpoint, formBody, proof2)
				if err != nil {
					return nil, nil, err
				}
				if retryNonce := headers2.Get("DPoP-Nonce"); retryNonce != "" {
					nonces.Set(origin, retryNonce)
				}
				if statusCode2 != http.StatusOK {
					return nil, nil, parseTokenError(statusCode2, body2)
				}
				return body2, headers2, nil
			}
			return nil, nil, parseTokenError(statusCode, body)
		}
	}

	if statusCode != http.StatusOK {
		return nil, nil, parseTokenError(statusCode, body)
	}

	return body, headers, nil
}

// doTokenHTTPOnce performs a single token HTTP request, reads the full body, and closes it.
func doTokenHTTPOnce(ctx context.Context, client *http.Client, endpoint, formBody, dpopProof string) (body []byte, headers http.Header, statusCode int, err error) {
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(formBody))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("oauth: create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("DPoP", dpopProof)

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("oauth: read token response: %w", err)
	}

	return data, resp.Header, resp.StatusCode, nil
}

func parseTokenError(statusCode int, body []byte) error {
	var oauthErr OAuthError
	if json.Unmarshal(body, &oauthErr) == nil && oauthErr.Code != "" {
		return &oauthErr
	}
	return fmt.Errorf("oauth: token endpoint returned HTTP %d", statusCode)
}
