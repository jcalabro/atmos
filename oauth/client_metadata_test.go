package oauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestNewLoopbackClientMetadata(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, redirectURI, scope, clientID string
	}{
		{
			name:        "leadsheet IPv4 callback and permission set",
			redirectURI: "http://127.0.0.1:8120/oauth/callback",
			scope:       "atproto include:fm.leadsheet.authFull",
			clientID:    "http://localhost?redirect_uri=http%3A%2F%2F127.0.0.1%3A8120%2Foauth%2Fcallback&scope=atproto+include%3Afm.leadsheet.authFull",
		},
		{
			name:        "IPv6 callback",
			redirectURI: "http://[::1]:8120/oauth/callback",
			scope:       "atproto",
			clientID:    "http://localhost?redirect_uri=http%3A%2F%2F%5B%3A%3A1%5D%3A8120%2Foauth%2Fcallback&scope=atproto",
		},
		{
			name:        "callback with query",
			redirectURI: "http://127.0.0.1/callback?return=%2Fapp",
			scope:       "atproto repo:fm.leadsheet.sheet?action=create",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			meta, err := NewLoopbackClientMetadata(tc.redirectURI, tc.scope)
			require.NoError(t, err)
			if tc.clientID != "" {
				require.Equal(t, tc.clientID, meta.ClientID)
			}
			parsed, err := url.Parse(meta.ClientID)
			require.NoError(t, err)
			require.Equal(t, "http", parsed.Scheme)
			require.Equal(t, "localhost", parsed.Host)
			require.Empty(t, parsed.Path)
			require.Equal(t, tc.redirectURI, parsed.Query().Get("redirect_uri"))
			require.Equal(t, tc.scope, parsed.Query().Get("scope"))
			require.Equal(t, "native", meta.ApplicationType)
			require.Equal(t, []string{"authorization_code", "refresh_token"}, meta.GrantTypes)
			require.Equal(t, []string{"code"}, meta.ResponseTypes)
			require.Equal(t, []string{tc.redirectURI}, meta.RedirectURIs)
			require.Equal(t, tc.scope, meta.Scope)
			require.Equal(t, "none", meta.TokenEndpointAuthMethod)
			require.True(t, meta.DPoPBoundAccessTokens)
			require.Nil(t, meta.JWKS)
			require.Empty(t, meta.TokenEndpointAuthSigningAlg)

			encoded, err := json.Marshal(meta)
			require.NoError(t, err)
			var public map[string]any
			require.NoError(t, json.Unmarshal(encoded, &public))
			require.NotContains(t, public, "jwks")
			require.NotContains(t, public, "token_endpoint_auth_signing_alg")
		})
	}
}

func TestNewLoopbackClientMetadata_InvalidRedirectURI(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"",
		"http://localhost:8120/callback",
		"https://127.0.0.1:8120/callback",
		"http://10.0.0.1:8120/callback",
		"http://127.0.0.2:8120/callback",
		"http://127.0.0.1.evil.example/callback",
		"http://[::2]:8120/callback",
		"http://user@127.0.0.1:8120/callback",
		"http://127.0.0.1:8120/callback#fragment",
		"http://127.0.0.1:0/callback",
		"http://127.0.0.1:65536/callback",
		"http://127.0.0.1:abc/callback",
		"http://127.0.0.1:/callback",
		"http://127.0.0.1/callback?bad=%ZZ",
		"http://127.0.0.1/\\evil.example",
		"127.0.0.1:8120/callback",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			meta, err := NewLoopbackClientMetadata(raw, "atproto")
			require.Error(t, err)
			require.Equal(t, ClientMetadata{}, meta)
		})
	}
}

func TestNewLoopbackClientMetadata_InvalidScope(t *testing.T) {
	t.Parallel()

	for _, scope := range []string{
		"", "openid", "notatproto", "atprotoish",
		"atproto ", " atproto", "atproto  include:fm.leadsheet.authFull",
		"atproto\nopenid", "atproto \"quoted\"", "atproto \\escaped", "atproto café",
	} {
		t.Run(scope, func(t *testing.T) {
			t.Parallel()
			meta, err := NewLoopbackClientMetadata("http://127.0.0.1:8120/callback", scope)
			require.Error(t, err)
			require.Equal(t, ClientMetadata{}, meta)
		})
	}
}

func TestLoopbackClientMetadata_AuthorizeAndCallback(t *testing.T) {
	t.Parallel()

	const did = "did:plc:testuser1234567890abcde"
	const redirectURI = "http://127.0.0.1:8120/oauth/callback"
	const scope = "atproto include:fm.leadsheet.authFull"
	meta, err := NewLoopbackClientMetadata(redirectURI, scope)
	require.NoError(t, err)

	var pdsURL, asURL string
	parForms := make(chan url.Values, 1)
	tokenForms := make(chan url.Values, 1)
	pdsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/oauth-protected-resource" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"resource":              pdsURL,
			"authorization_servers": []string{asURL},
		})
	})
	asHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/oauth-authorization-server":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer":                                asURL,
				"authorization_endpoint":                asURL + "/oauth/authorize",
				"token_endpoint":                        asURL + "/oauth/token",
				"pushed_authorization_request_endpoint": asURL + "/oauth/par",
				"client_id_metadata_document_supported": true,
				"require_pushed_authorization_requests": true,
			})
		case "/oauth/par":
			if err := r.ParseForm(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			parForms <- r.Form
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"request_uri": "urn:ietf:params:oauth:request_uri:loopback-test"})
		case "/oauth/token":
			if err := r.ParseForm(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			tokenForms <- r.Form
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "access-token", "refresh_token": "refresh-token",
				"token_type": "DPoP", "expires_in": 300, "scope": scope, "sub": did,
			})
		default:
			http.NotFound(w, r)
		}
	})
	doc := &identity.DIDDocument{
		ID: did,
		Service: []identity.Service{
			{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer"},
		},
	}
	env := setupVerifyIssuer(t, pdsHandler, asHandler, &fakeResolver{doc: doc})
	pdsURL, asURL = env.pdsURL, env.asURL
	c := env.client
	c.ClientMetadata = meta
	// Test servers are on loopback; real OAuth clients use the protected default.
	c.HTTPClient = gt.Some(env.httpClient)

	result, err := c.Authorize(context.Background(), AuthorizeOptions{Input: did})
	require.NoError(t, err)
	require.NotEmpty(t, result.State)
	par := <-parForms
	require.Equal(t, meta.ClientID, par.Get("client_id"))
	require.Equal(t, redirectURI, par.Get("redirect_uri"))
	require.Equal(t, scope, par.Get("scope"))
	require.Equal(t, "code", par.Get("response_type"))
	require.Equal(t, "S256", par.Get("code_challenge_method"))
	require.Equal(t, result.State, par.Get("state"))

	authorizeURL, err := url.Parse(result.URL)
	require.NoError(t, err)
	require.Equal(t, asURL+"/oauth/authorize", authorizeURL.Scheme+"://"+authorizeURL.Host+authorizeURL.Path)
	require.Equal(t, meta.ClientID, authorizeURL.Query().Get("client_id"))
	require.Equal(t, "urn:ietf:params:oauth:request_uri:loopback-test", authorizeURL.Query().Get("request_uri"))

	session, err := c.Callback(context.Background(), CallbackParams{Code: "code", State: result.State, Iss: asURL})
	require.NoError(t, err)
	require.Equal(t, did, session.TokenSet.Sub)
	require.NotEmpty(t, session.SessionID)
	token := <-tokenForms
	require.Equal(t, meta.ClientID, token.Get("client_id"))
	require.Equal(t, redirectURI, token.Get("redirect_uri"))
	require.Equal(t, "authorization_code", token.Get("grant_type"))
	require.Equal(t, "code", token.Get("code"))
	require.Empty(t, token.Get("client_assertion"))
	_, err = c.SessionStore.GetSession(context.Background(), did, session.SessionID)
	require.NoError(t, err)
}

func TestLoopbackClientMetadata_DoesNotRelaxSSRFProtection(t *testing.T) {
	t.Parallel()

	const did = "did:plc:testuser1234567890abcde"
	meta, err := NewLoopbackClientMetadata("http://127.0.0.1:8120/callback", "atproto")
	require.NoError(t, err)
	var reached atomic.Bool
	pds := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		reached.Store(true)
	}))
	t.Cleanup(pds.Close)
	c := &Client{
		ClientMetadata: meta,
		Identity: &identity.Directory{Resolver: &fakeResolver{doc: &identity.DIDDocument{
			ID: did,
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: pds.URL},
			},
		}}},
	}
	_, err = c.Authorize(context.Background(), AuthorizeOptions{Input: did})
	require.ErrorContains(t, err, "fetch protected resource metadata")
	require.False(t, reached.Load(), "a loopback client must not fetch metadata from a private PDS")
}
