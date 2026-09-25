package oauth

import (
	"context"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atmos/crypto"
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

func TestConfidentialClientMetadata_JWKSURIAndKeyedJWKS(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	const clientID = "https://leadsheet.fm/oauth/client-metadata.json"
	const keyID = "leadsheet-key-1"
	jwksURI := "https://leadsheet.fm/oauth/jwks.json"
	c := &Client{
		ClientMetadata: ClientMetadata{
			ClientID:                    clientID,
			ApplicationType:             "web",
			GrantTypes:                  []string{"authorization_code", "refresh_token"},
			Scope:                       "atproto include:fm.leadsheet.authFull",
			ResponseTypes:               []string{"code"},
			RedirectURIs:                []string{"https://leadsheet.fm/oauth/callback"},
			DPoPBoundAccessTokens:       true,
			TokenEndpointAuthMethod:     "private_key_jwt",
			TokenEndpointAuthSigningAlg: "ES256",
			JWKSURI:                     &jwksURI,
		},
		Key:   key,
		KeyID: keyID,
	}
	require.NoError(t, c.ClientMetadata.ValidateClientAuth())
	require.NoError(t, c.ValidateClientAuth())

	metadataJSON, err := json.Marshal(c.ClientMetadata)
	require.NoError(t, err)
	var metadata map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadataJSON, &metadata))
	require.JSONEq(t, `"https://leadsheet.fm/oauth/jwks.json"`, string(metadata["jwks_uri"]))
	require.NotContains(t, metadata, "jwks")
	require.JSONEq(t, `"private_key_jwt"`, string(metadata["token_endpoint_auth_method"]))
	require.JSONEq(t, `"ES256"`, string(metadata["token_endpoint_auth_signing_alg"]))

	set, err := c.PublicJWKS()
	require.NoError(t, err)
	require.Len(t, set.Keys, 1)
	require.Equal(t, keyID, set.Keys[0].KeyID)
	setJSON, err := json.Marshal(set)
	require.NoError(t, err)
	var public struct {
		Keys []map[string]any `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(setJSON, &public))
	require.Len(t, public.Keys, 1)
	require.Equal(t, keyID, public.Keys[0]["kid"])
	require.NotContains(t, public.Keys[0], "d", "a public JWKS must never contain private key material")

	params := url.Values{}
	require.NoError(t, c.clientAuth().Apply(params, "https://auth.example.com"))
	x, err := base64.RawURLEncoding.DecodeString(set.Keys[0].X)
	require.NoError(t, err)
	y, err := base64.RawURLEncoding.DecodeString(set.Keys[0].Y)
	require.NoError(t, err)
	verificationKey, err := crypto.ParsePublicBytesP256(elliptic.MarshalCompressed(
		elliptic.P256(), new(big.Int).SetBytes(x), new(big.Int).SetBytes(y),
	))
	require.NoError(t, err)
	token, err := jwt.Parse(params.Get("client_assertion"), func(token *jwt.Token) (any, error) {
		return verificationKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}), jwt.WithIssuer(clientID), jwt.WithAudience("https://auth.example.com"))
	require.NoError(t, err)
	require.True(t, token.Valid)
	require.Equal(t, keyID, token.Header["kid"])
}

func TestClientMetadata_ValidateClientAuth(t *testing.T) {
	t.Parallel()

	uri := "https://leadsheet.fm/oauth/jwks.json"
	loopbackURI := "http://127.0.0.1:8120/oauth/jwks.json"
	localhostURI := "http://localhost:8120/oauth/jwks.json"
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	jwk := PublicJWK(pub)
	jwk.KeyID = "k1"
	otherKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	otherPub, ok := otherKey.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	otherJWK := PublicJWK(otherPub)
	inline := &JWKSet{Keys: []ECPublicJWK{jwk}}
	base := ClientMetadata{
		TokenEndpointAuthMethod:     "private_key_jwt",
		TokenEndpointAuthSigningAlg: "ES256",
		JWKSURI:                     &uri,
	}
	for _, tc := range []struct {
		name string
		meta ClientMetadata
		want string
	}{
		{name: "jwks_uri", meta: base},
		{name: "loopback jwks_uri", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKSURI: &loopbackURI}},
		{name: "localhost jwks_uri", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKSURI: &localhostURI}},
		{name: "inline jwks", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: inline}},
		{name: "public client", meta: ClientMetadata{TokenEndpointAuthMethod: "none"}},
		{name: "both key sources", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: inline, JWKSURI: &uri}, want: "mutually exclusive"},
		{name: "missing key source", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256"}, want: "requires jwks or jwks_uri"},
		{name: "missing signing algorithm", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", JWKSURI: &uri}, want: "requires token_endpoint_auth_signing_alg ES256"},
		{name: "empty inline jwks", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{}}, want: "at least one public key"},
		{name: "inline key missing kid", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{Keys: []ECPublicJWK{{}}}}, want: "requires kid"},
		{name: "inline key malformed", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{Keys: []ECPublicJWK{{KTY: "EC", CRV: "P-256", X: "not-base64", Y: jwk.Y, KeyID: "k1"}}}}, want: "invalid x coordinate"},
		{name: "inline key off curve", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{Keys: []ECPublicJWK{{KTY: "EC", CRV: "P-256", X: jwk.X, Y: otherJWK.Y, KeyID: "k1"}}}}, want: "not a P-256 point"},
		{name: "public signing algorithm", meta: ClientMetadata{TokenEndpointAuthMethod: "none", TokenEndpointAuthSigningAlg: "ES256"}, want: "must not declare"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.meta.ValidateClientAuth()
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestClientMetadata_InvalidJWKSURI(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"", "oauth/jwks.json", "ftp://leadsheet.fm/jwks.json",
		"http://leadsheet.fm/jwks.json", "http://127.0.0.2/jwks.json",
		"https://localhost/jwks.json", "https://127.0.0.1/jwks.json",
		"https://127.1/jwks.json", "https://0x7f.1/jwks.json", "https://127.0.0.1./jwks.json",
		"https://internal.local/jwks.json", "https://intranet/jwks.json",
		"https://user@leadsheet.fm/jwks.json", "https://leadsheet.fm/jwks.json#fragment",
		"https://leadsheet.fm/\\evil.example/jwks.json",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			meta := ClientMetadata{
				TokenEndpointAuthMethod:     "private_key_jwt",
				TokenEndpointAuthSigningAlg: "ES256",
				JWKSURI:                     &raw,
			}
			require.ErrorContains(t, meta.ValidateClientAuth(), "invalid jwks_uri")
		})
	}
}

func TestClientValidateClientAuth_MatchesSigningKey(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	otherKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	public, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	otherPublic, ok := otherKey.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	keyJWK := PublicJWK(public)
	keyJWK.KeyID = "k1"
	otherJWK := PublicJWK(otherPublic)
	otherJWK.KeyID = "k1"
	uri := "https://leadsheet.fm/oauth/jwks.json"
	base := ClientMetadata{
		TokenEndpointAuthMethod:     "private_key_jwt",
		TokenEndpointAuthSigningAlg: "ES256",
		JWKSURI:                     &uri,
	}
	for _, tc := range []struct {
		name string
		meta ClientMetadata
		key  *crypto.P256PrivateKey
		kid  string
		want string
	}{
		{name: "remote JWKS", meta: base, key: key, kid: "k1"},
		{name: "matching inline JWKS", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{Keys: []ECPublicJWK{keyJWK}}}, key: key, kid: "k1"},
		{name: "different inline key", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{Keys: []ECPublicJWK{otherJWK}}}, key: key, kid: "k1", want: "does not match"},
		{name: "missing inline key", meta: ClientMetadata{TokenEndpointAuthMethod: "private_key_jwt", TokenEndpointAuthSigningAlg: "ES256", JWKS: &JWKSet{Keys: []ECPublicJWK{otherJWK}}}, key: key, kid: "k2", want: "must appear exactly once"},
		{name: "missing signing key", meta: base, kid: "k1", want: "requires a signing key and kid"},
		{name: "missing kid", meta: base, key: key, want: "requires a signing key and kid"},
		{name: "public client with key", meta: ClientMetadata{TokenEndpointAuthMethod: "none"}, key: key, kid: "k1", want: "public client must not configure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &Client{ClientMetadata: tc.meta, Key: tc.key, KeyID: tc.kid}
			err := client.ValidateClientAuth()
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestAuthorize_RejectsInvalidClientAuthBeforeNetwork(t *testing.T) {
	t.Parallel()

	uri := "https://leadsheet.fm/oauth/jwks.json"
	c := &Client{ClientMetadata: ClientMetadata{
		TokenEndpointAuthMethod:     "private_key_jwt",
		TokenEndpointAuthSigningAlg: "ES256",
		JWKSURI:                     &uri,
	}}
	_, err := c.Authorize(context.Background(), AuthorizeOptions{Input: "did:plc:testuser1234567890abcde"})
	require.ErrorContains(t, err, "requires a signing key and kid")
}

func TestClientPublicJWKS(t *testing.T) {
	t.Parallel()

	set, err := (&Client{}).PublicJWKS()
	require.NoError(t, err)
	require.Empty(t, set.Keys)
	encoded, err := json.Marshal(set)
	require.NoError(t, err)
	require.JSONEq(t, `{"keys":[]}`, string(encoded))

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	_, err = (&Client{Key: key}).PublicJWKS()
	require.ErrorContains(t, err, "requires a kid")
}

func TestConfidentialClientAuth_RequiresKeyAndKeyID(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		key  *crypto.P256PrivateKey
		kid  string
	}{
		{name: "missing key", kid: "k1"},
		{name: "missing kid", key: key},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := ConfidentialClientAuth{ClientID: "https://leadsheet.fm/oauth/client-metadata.json", Key: tc.key, KeyID: tc.kid}
			params := url.Values{}
			require.ErrorContains(t, auth.Apply(params, "https://auth.example.com"), "requires a key and kid")
			require.Empty(t, params)
		})
	}
}
