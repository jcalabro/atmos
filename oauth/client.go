package oauth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jcalabro/gt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/xrpc"
)

// Client is an ATProto OAuth 2.0 client that handles the complete
// authorization flow including PKCE, PAR, and DPoP.
type Client struct {
	// ClientMetadata is this client's metadata document.
	ClientMetadata ClientMetadata

	// Identity resolves DIDs and handles.
	Identity *identity.Directory

	// HTTPClient is the HTTP client for OAuth requests.
	// None = default 30s timeout client.
	HTTPClient gt.Option[*http.Client]

	// SessionStore persists authenticated sessions. Required.
	//
	// Implementors are responsible for encrypting tokens at rest.
	// The Session struct contains access and refresh tokens in plaintext.
	SessionStore SessionStore

	// StateStore stores pending authorization flow state. Required.
	StateStore StateStore

	// Key is the signing key for confidential clients.
	// Nil for public clients.
	Key *crypto.P256PrivateKey

	// KeyID is the key identifier for confidential client assertions.
	KeyID string

	noncesOnce sync.Once
	nonces     *NonceStore
	httpOnce   sync.Once
	httpCached *http.Client
}

func (c *Client) getNonces() *NonceStore {
	c.noncesOnce.Do(func() {
		c.nonces = NewNonceStore()
	})
	return c.nonces
}

// AuthorizeOptions configures an authorization request.
type AuthorizeOptions struct {
	// Input is the user's handle or DID.
	Input string

	// RedirectURI is the callback URL. Must be in ClientMetadata.RedirectURIs.
	RedirectURI string

	// Scope overrides the scope from ClientMetadata. Optional.
	Scope string

	// State is opaque application state passed through the flow. Optional.
	State string
}

// AuthorizeResult is returned by Authorize with the URL to redirect the user to.
type AuthorizeResult struct {
	URL   string
	State string
}

// Authorize initiates the OAuth authorization flow.
// Returns the URL to redirect the user's browser to.
func (c *Client) Authorize(ctx context.Context, opts AuthorizeOptions) (*AuthorizeResult, error) {
	httpClient := c.httpClient()

	// 1. Resolve identity → PDS → AS metadata.
	id, err := atmos.ParseATIdentifier(opts.Input)
	if err != nil {
		return nil, fmt.Errorf("oauth: invalid identifier %q: %w", opts.Input, err)
	}

	ident, err := c.Identity.Lookup(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("oauth: resolve identity: %w", err)
	}

	pds := ident.PDSEndpoint()
	if pds == "" {
		return nil, fmt.Errorf("oauth: no PDS endpoint for %s", ident.DID)
	}

	prMeta, err := FetchProtectedResourceMetadata(ctx, httpClient, pds)
	if err != nil {
		return nil, err
	}

	issuer := prMeta.AuthorizationServers[0]
	asMeta, err := FetchAuthServerMetadata(ctx, httpClient, issuer)
	if err != nil {
		return nil, err
	}

	// 2. Generate cryptographic material.
	dpopKey, err := crypto.GenerateP256()
	if err != nil {
		return nil, fmt.Errorf("oauth: generate DPoP key: %w", err)
	}

	pkce, err := GeneratePKCE()
	if err != nil {
		return nil, err
	}

	var stateBuf [16]byte
	if _, err := rand.Read(stateBuf[:]); err != nil {
		return nil, fmt.Errorf("oauth: generate state: %w", err)
	}
	state := base64.RawURLEncoding.EncodeToString(stateBuf[:])

	// 3. Determine and validate redirect URI.
	redirectURI := opts.RedirectURI
	if redirectURI == "" && len(c.ClientMetadata.RedirectURIs) > 0 {
		redirectURI = c.ClientMetadata.RedirectURIs[0]
	}
	if !slices.Contains(c.ClientMetadata.RedirectURIs, redirectURI) {
		return nil, fmt.Errorf("oauth: redirect_uri %q not in registered redirect_uris", redirectURI)
	}

	// 4. Determine scope.
	scope := opts.Scope
	if scope == "" {
		scope = c.ClientMetadata.Scope
	}

	// 5. Build client auth.
	authMethod := c.ClientMetadata.TokenEndpointAuthMethod
	clientAuth := c.clientAuth()

	// 6. Store state with discovered endpoints.
	authState := &AuthState{
		Issuer:             issuer,
		DPoPKey:            dpopKey,
		AuthMethod:         authMethod,
		Verifier:           pkce.Verifier,
		RedirectURI:        redirectURI,
		AppState:           opts.State,
		TokenEndpoint:      asMeta.TokenEndpoint,
		RevocationEndpoint: asMeta.RevocationEndpoint,
	}
	if err := c.StateStore.SetState(ctx, state, authState); err != nil {
		return nil, fmt.Errorf("oauth: store state: %w", err)
	}

	// 7. PAR request.
	parParams := url.Values{}
	parParams.Set("response_type", "code")
	parParams.Set("code_challenge", pkce.Challenge)
	parParams.Set("code_challenge_method", pkce.Method)
	parParams.Set("state", state)
	parParams.Set("redirect_uri", redirectURI)
	parParams.Set("scope", scope)
	parParams.Set("login_hint", opts.Input)

	if err := clientAuth.Apply(parParams, issuer); err != nil {
		return nil, err
	}

	nonces := c.getNonces()
	parEndpoint := asMeta.PushedAuthorizationRequestEndpoint

	requestURI, err := doPAR(ctx, httpClient, parEndpoint, parParams, dpopKey, nonces)
	if err != nil {
		return nil, err
	}

	// 8. Build authorization URL.
	authURL := asMeta.AuthorizationEndpoint + "?" + url.Values{
		"client_id":   {c.ClientMetadata.ClientID},
		"request_uri": {requestURI},
	}.Encode()

	return &AuthorizeResult{
		URL:   authURL,
		State: state,
	}, nil
}

// CallbackParams are the parameters received from the OAuth callback.
type CallbackParams struct {
	Code  string
	State string
	Iss   string // Authorization server issuer (for mix-up prevention)
}

// Callback handles the OAuth redirect callback and completes token exchange.
func (c *Client) Callback(ctx context.Context, params CallbackParams) (*Session, error) {
	httpClient := c.httpClient()

	// 1. Validate and consume state (prevent replay).
	authState, err := c.StateStore.GetState(ctx, params.State)
	if err != nil {
		return nil, ErrInvalidState
	}
	_ = c.StateStore.DeleteState(ctx, params.State)

	// 2. Validate issuer (RFC 9207 mix-up prevention).
	if params.Iss != "" {
		if params.Iss != authState.Issuer {
			return nil, ErrIssuerMismatch
		}
	} else {
		// ATProto mandates authorization_response_iss_parameter_supported: true.
		return nil, ErrMissingIssuer
	}

	// 3. Exchange code for tokens.
	nonces := c.getNonces()
	tokenSet, err := ExchangeCode(ctx, &ExchangeCodeConfig{
		TokenEndpoint:      authState.TokenEndpoint,
		RevocationEndpoint: authState.RevocationEndpoint,
		Code:               params.Code,
		CodeVerifier:       authState.Verifier,
		RedirectURI:        authState.RedirectURI,
		ClientAuth:         c.clientAuth(),
		DPoPKey:            authState.DPoPKey,
		Nonces:             nonces,
		HTTPClient:         httpClient,
	})
	if err != nil {
		return nil, err
	}

	// 4. Critical: verify issuer by resolving sub DID → PDS → AS chain.
	// verifyIssuer returns the verified PDS URL so we don't need to resolve again.
	pds, err := c.verifyIssuer(ctx, httpClient, tokenSet.Sub, authState.Issuer)
	if err != nil {
		RevokeToken(ctx, authState.RevocationEndpoint, tokenSet.AccessToken, c.clientAuth(), authState.DPoPKey, nonces, httpClient)
		return nil, err
	}
	tokenSet.Aud = pds

	// 5. Revoke any existing session for this user before storing the new one.
	if oldSession, oldErr := c.SessionStore.GetSession(ctx, tokenSet.Sub); oldErr == nil {
		RevokeToken(ctx, oldSession.TokenSet.RevocationEndpoint, oldSession.TokenSet.RefreshToken, c.clientAuth(), oldSession.DPoPKey, nonces, httpClient)
	}

	// 6. Store session.
	session := &Session{
		DPoPKey:  authState.DPoPKey,
		TokenSet: *tokenSet,
	}
	if err := c.SessionStore.SetSession(ctx, tokenSet.Sub, session); err != nil {
		RevokeToken(ctx, tokenSet.RevocationEndpoint, tokenSet.AccessToken, c.clientAuth(), authState.DPoPKey, nonces, httpClient)
		return nil, fmt.Errorf("oauth: store session: %w", err)
	}

	return session, nil
}

// AuthenticatedClient returns an *xrpc.Client configured with DPoP authentication
// for the given user DID. The client is long-lived: it automatically refreshes
// stale tokens on each request, protected by a mutex so concurrent requests
// coalesce on a single refresh.
func (c *Client) AuthenticatedClient(ctx context.Context, did string) (*xrpc.Client, error) {
	session, err := c.SessionStore.GetSession(ctx, did)
	if err != nil {
		return nil, err
	}

	pds := session.TokenSet.Aud
	if pds == "" {
		pds, err = c.resolvedPDS(ctx, did)
		if err != nil {
			return nil, fmt.Errorf("oauth: resolve PDS: %w", err)
		}
	}

	source := &sessionTokenSource{
		client:  c,
		did:     did,
		session: session,
	}

	transport := &Transport{
		Base:   xrpc.NewTransport(),
		Source: source,
		Nonces: c.getNonces(),
	}

	return &xrpc.Client{
		Host:       pds,
		HTTPClient: gt.Some(&http.Client{Transport: transport, Timeout: 30 * time.Second}),
	}, nil
}

// sessionTokenSource implements TokenSource backed by a live session.
// It refreshes the token when stale, under a mutex to prevent concurrent
// refresh of single-use refresh tokens.
type sessionTokenSource struct {
	client  *Client
	did     string
	session *Session
	mu      sync.Mutex
}

func (s *sessionTokenSource) Token(ctx context.Context) (string, *crypto.P256PrivateKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session.TokenSet.IsStale() {
		if err := s.client.refreshSession(ctx, s.did, s.session); err != nil {
			return "", nil, err
		}
	}

	return s.session.TokenSet.AccessToken, s.session.DPoPKey, nil
}

// SignOut revokes tokens and deletes the session for the given DID.
func (c *Client) SignOut(ctx context.Context, did string) error {
	session, err := c.SessionStore.GetSession(ctx, did)
	if err != nil {
		return err
	}

	httpClient := c.httpClient()
	nonces := c.getNonces()

	if session.TokenSet.RefreshToken != "" {
		RevokeToken(ctx, session.TokenSet.RevocationEndpoint, session.TokenSet.RefreshToken, c.clientAuth(), session.DPoPKey, nonces, httpClient)
	} else {
		RevokeToken(ctx, session.TokenSet.RevocationEndpoint, session.TokenSet.AccessToken, c.clientAuth(), session.DPoPKey, nonces, httpClient)
	}

	return c.SessionStore.DeleteSession(ctx, did)
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient.HasVal() {
		return c.HTTPClient.Val()
	}
	c.httpOnce.Do(func() {
		c.httpCached = xrpc.NewHTTPClient(30 * time.Second)
	})
	return c.httpCached
}

func (c *Client) clientAuth() ClientAuth {
	if c.Key != nil {
		return &ConfidentialClientAuth{
			ClientID: c.ClientMetadata.ClientID,
			Key:      c.Key,
			KeyID:    c.KeyID,
		}
	}
	return &PublicClientAuth{ClientID: c.ClientMetadata.ClientID}
}

func (c *Client) refreshSession(ctx context.Context, did string, session *Session) error {
	if session.TokenSet.RefreshToken == "" {
		return ErrNoRefreshToken
	}

	httpClient := c.httpClient()
	nonces := c.getNonces()

	// Verify issuer before refreshing.
	if _, err := c.verifyIssuer(ctx, httpClient, did, session.TokenSet.Issuer); err != nil {
		return err
	}

	newTokens, err := RefreshToken(ctx, &RefreshTokenConfig{
		TokenEndpoint:      session.TokenSet.TokenEndpoint,
		RevocationEndpoint: session.TokenSet.RevocationEndpoint,
		RefreshToken:       session.TokenSet.RefreshToken,
		ClientAuth:         c.clientAuth(),
		DPoPKey:            session.DPoPKey,
		Nonces:             nonces,
		HTTPClient:         httpClient,
	})
	if err != nil {
		return fmt.Errorf("oauth: refresh token: %w", err)
	}

	// Validate sub didn't change after refresh.
	if newTokens.Sub != did {
		RevokeToken(ctx, newTokens.RevocationEndpoint, newTokens.AccessToken, c.clientAuth(), session.DPoPKey, nonces, httpClient)
		return fmt.Errorf("oauth: sub mismatch after refresh: expected %s, got %s", did, newTokens.Sub)
	}

	session.TokenSet = *newTokens
	return c.SessionStore.SetSession(ctx, did, session)
}

// verifyIssuer resolves the sub DID and verifies that its PDS points to
// the expected authorization server. Returns the verified PDS URL.
func (c *Client) verifyIssuer(ctx context.Context, httpClient *http.Client, sub, expectedIssuer string) (pdsURL string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	did, err := atmos.ParseDID(sub)
	if err != nil {
		return "", fmt.Errorf("oauth: invalid sub DID %q: %w", sub, err)
	}

	// Purge cache to force fresh resolution (prevent stale cache attacks).
	c.Identity.Purge(ctx, did)

	ident, err := c.Identity.LookupDID(ctx, did)
	if err != nil {
		return "", fmt.Errorf("oauth: resolve sub DID: %w", err)
	}

	pds := ident.PDSEndpoint()
	if pds == "" {
		return "", fmt.Errorf("oauth: sub DID %s has no PDS endpoint", sub)
	}

	prMeta, err := FetchProtectedResourceMetadata(ctx, httpClient, pds)
	if err != nil {
		return "", fmt.Errorf("oauth: fetch PDS metadata for verification: %w", err)
	}

	if len(prMeta.AuthorizationServers) != 1 {
		return "", fmt.Errorf("%w: expected exactly 1 authorization server, got %d", ErrIssuerVerification, len(prMeta.AuthorizationServers))
	}

	resolvedIssuer := prMeta.AuthorizationServers[0]
	if resolvedIssuer != expectedIssuer {
		return "", fmt.Errorf("%w: expected %s, got %s", ErrIssuerVerification, expectedIssuer, resolvedIssuer)
	}

	// RFC 9728 §4: if the AS declares protected_resources, verify the PDS is listed.
	asMeta, err := FetchAuthServerMetadata(ctx, httpClient, resolvedIssuer)
	if err != nil {
		return "", fmt.Errorf("oauth: fetch AS metadata for verification: %w", err)
	}
	if len(asMeta.ProtectedResources) > 0 && !slices.Contains(asMeta.ProtectedResources, prMeta.Resource) {
		return "", fmt.Errorf("%w: PDS %q not in issuer's protected_resources", ErrIssuerVerification, prMeta.Resource)
	}

	return pds, nil
}

func (c *Client) resolvedPDS(ctx context.Context, sub string) (string, error) {
	did, err := atmos.ParseDID(sub)
	if err != nil {
		return "", fmt.Errorf("oauth: invalid sub DID %q: %w", sub, err)
	}
	ident, err := c.Identity.LookupDID(ctx, did)
	if err != nil {
		return "", fmt.Errorf("oauth: resolve sub DID: %w", err)
	}
	pds := ident.PDSEndpoint()
	if pds == "" {
		return "", fmt.Errorf("oauth: sub DID %s has no PDS endpoint", sub)
	}
	return pds, nil
}

// doPAR sends a Pushed Authorization Request with DPoP nonce retry.
func doPAR(ctx context.Context, httpClient *http.Client, endpoint string, params url.Values, dpopKey *crypto.P256PrivateKey, nonces *NonceStore) (string, error) {
	origin := originFromURL(endpoint)
	nonce := nonces.Get(origin)

	proof, err := CreateDPoPProof(dpopKey, "POST", endpoint, nonce, "")
	if err != nil {
		return "", err
	}

	body, headers, statusCode, err := doPARHTTPOnce(ctx, httpClient, endpoint, params.Encode(), proof)
	if err != nil {
		return "", err
	}

	dpopNonce := headers.Get("DPoP-Nonce")
	if dpopNonce != "" {
		nonces.Set(origin, dpopNonce)
	}

	// Handle use_dpop_nonce retry.
	if statusCode == http.StatusBadRequest && dpopNonce != "" && dpopNonce != nonce {
		var oauthErr OAuthError
		if json.Unmarshal(body, &oauthErr) == nil && oauthErr.Code == "use_dpop_nonce" {
			proof2, err := CreateDPoPProof(dpopKey, "POST", endpoint, dpopNonce, "")
			if err != nil {
				return "", err
			}
			body2, headers2, statusCode2, err := doPARHTTPOnce(ctx, httpClient, endpoint, params.Encode(), proof2)
			if err != nil {
				return "", err
			}
			if retryNonce := headers2.Get("DPoP-Nonce"); retryNonce != "" {
				nonces.Set(origin, retryNonce)
			}
			if statusCode2 != http.StatusOK && statusCode2 != http.StatusCreated {
				return "", parsePARError(statusCode2, body2)
			}
			return extractRequestURI(body2)
		}
		return "", parsePARError(statusCode, body)
	}

	if statusCode != http.StatusOK && statusCode != http.StatusCreated {
		return "", parsePARError(statusCode, body)
	}

	return extractRequestURI(body)
}

// doPARHTTPOnce performs a single PAR HTTP request, reads the full body, and closes it.
func doPARHTTPOnce(ctx context.Context, client *http.Client, endpoint, formBody, dpopProof string) (body []byte, headers http.Header, statusCode int, err error) {
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(formBody))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("oauth: create PAR request: %w", err)
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
		return nil, nil, 0, fmt.Errorf("oauth: read PAR response: %w", err)
	}

	return data, resp.Header, resp.StatusCode, nil
}

func extractRequestURI(body []byte) (string, error) {
	var result struct {
		RequestURI string `json:"request_uri"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("oauth: parse PAR response: %w", err)
	}
	if result.RequestURI == "" {
		return "", fmt.Errorf("oauth: PAR response missing request_uri")
	}
	return result.RequestURI, nil
}

func parsePARError(statusCode int, body []byte) error {
	var oauthErr OAuthError
	if json.Unmarshal(body, &oauthErr) == nil && oauthErr.Code != "" {
		return &oauthErr
	}
	return fmt.Errorf("oauth: PAR endpoint returned HTTP %d", statusCode)
}
