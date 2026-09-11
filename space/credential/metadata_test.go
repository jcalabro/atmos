package credential

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

func TestClientMetadataResolverInlineJWKSAndCacheCopies(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key-1")
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{key}}})
	}))
	t.Cleanup(server.Close)

	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	first, err := resolver.ResolveKey(t.Context(), server.URL, "key-1")
	require.NoError(t, err)
	second, err := resolver.ResolveKey(t.Context(), server.URL, "key-1")
	require.NoError(t, err)
	require.Equal(t, int32(1), requests.Load())
	require.NotSame(t, first, second)
	require.NotSame(t, first.Key, second.Key)
	require.True(t, first.Key.Equal(second.Key))
	require.Equal(t, "key-1", first.KeyID)
}

func TestClientMetadataResolverRemoteJWKS(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "remote")
	var metadataRequests, jwksRequests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/client":
			metadataRequests.Add(1)
			writeJSON(t, w, map[string]any{"client_id": server.URL + "/client", "jwks_uri": server.URL + "/keys?version=1"})
		case "/keys":
			jwksRequests.Add(1)
			w.Header().Set("Content-Type", "application/jwk-set+json; charset=utf-8")
			writeJSON(t, w, map[string]any{"keys": []any{key}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	resolved, err := resolver.ResolveKey(t.Context(), server.URL+"/client", "remote")
	require.NoError(t, err)
	require.NotNil(t, resolved.Key)
	require.Equal(t, int32(1), metadataRequests.Load())
	require.Equal(t, int32(1), jwksRequests.Load())
}

func TestClientMetadataResolverRejectsMetadataAmbiguityAndMismatch(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key")
	validSet := map[string]any{"keys": []any{key}}
	tests := map[string]func(string) string{
		"missing key source": func(clientID string) string {
			return marshalString(t, map[string]any{"client_id": clientID})
		},
		"both key sources": func(clientID string) string {
			return marshalString(t, map[string]any{"client_id": clientID, "jwks": validSet, "jwks_uri": clientID + "/keys"})
		},
		"null inline": func(clientID string) string {
			return `{"client_id":` + quoteJSON(t, clientID) + `,"jwks":null}`
		},
		"null URI": func(clientID string) string {
			return `{"client_id":` + quoteJSON(t, clientID) + `,"jwks_uri":null}`
		},
		"client ID mismatch": func(string) string {
			return marshalString(t, map[string]any{"client_id": "http://other.invalid", "jwks": validSet})
		},
		"duplicate client ID": func(clientID string) string {
			return `{"client_id":` + quoteJSON(t, clientID) + `,"client_id":` + quoteJSON(t, clientID) + `,"jwks":{"keys":[]}}`
		},
	}
	for name, body := range tests {
		body := body
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, body(server.URL))
			}))
			t.Cleanup(server.Close)
			resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
			_, err := resolver.ResolveKey(t.Context(), server.URL, "key")
			require.Error(t, err)
		})
	}
}

func TestValidateMetadataJWKSStrictSelection(t *testing.T) {
	t.Parallel()
	valid := testMetadataJWK(t, "key")
	tests := map[string]func(map[string]any){
		"missing kid":      func(key map[string]any) { delete(key, "kid") },
		"empty kid":        func(key map[string]any) { key["kid"] = "" },
		"wrong kty":        func(key map[string]any) { key["kty"] = "RSA" },
		"wrong curve":      func(key map[string]any) { key["crv"] = "secp256k1" },
		"wrong algorithm":  func(key map[string]any) { key["alg"] = "ES384" },
		"null algorithm":   func(key map[string]any) { key["alg"] = nil },
		"empty algorithm":  func(key map[string]any) { key["alg"] = "" },
		"encryption use":   func(key map[string]any) { key["use"] = "enc" },
		"null use":         func(key map[string]any) { key["use"] = nil },
		"sign operation":   func(key map[string]any) { key["key_ops"] = []string{"sign"} },
		"null operations":  func(key map[string]any) { key["key_ops"] = nil },
		"empty operations": func(key map[string]any) { key["key_ops"] = []string{} },
		"mixed operations": func(key map[string]any) { key["key_ops"] = []string{"verify", "sign"} },
		"duplicate op":     func(key map[string]any) { key["key_ops"] = []string{"verify", "verify"} },
		"private EC":       func(key map[string]any) { key["d"] = "secret" },
		"private RSA":      func(key map[string]any) { key["p"] = "secret" },
		"symmetric secret": func(key map[string]any) { key["k"] = "secret" },
		"padded x":         func(key map[string]any) { key["x"] = key["x"].(string) + "=" }, //nolint:errcheck // fixture has a string x
		"short y":          func(key map[string]any) { key["y"] = "AA" },
		"off curve": func(key map[string]any) {
			key["x"] = strings.Repeat("A", 43)
			key["y"] = strings.Repeat("A", 43)
		},
	}
	for name, mutate := range tests {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			key := cloneMap(valid)
			mutate(key)
			_, err := validateJWKS([]byte(marshalString(t, map[string]any{"keys": []any{key}})))
			require.Error(t, err)
		})
	}

	t.Run("valid optional constraints", func(t *testing.T) {
		t.Parallel()
		key := cloneMap(valid)
		key["alg"] = "ES256"
		key["use"] = "sig"
		key["key_ops"] = []string{"verify"}
		keys, err := validateJWKS([]byte(marshalString(t, map[string]any{"keys": []any{key}})))
		require.NoError(t, err)
		require.Len(t, keys, 1)
	})

	t.Run("duplicate kid", func(t *testing.T) {
		t.Parallel()
		_, err := validateJWKS([]byte(marshalString(t, map[string]any{"keys": []any{valid, valid}})))
		require.ErrorContains(t, err, "duplicate JWKS kid")
	})

	t.Run("duplicate key field", func(t *testing.T) {
		t.Parallel()
		x := valid["x"].(string) //nolint:errcheck // helper always emits strings
		y := valid["y"].(string) //nolint:errcheck // helper always emits strings
		raw := `{"keys":[{"kty":"EC","kty":"EC","crv":"P-256","kid":"key","x":` + quoteJSON(t, x) + `,"y":` + quoteJSON(t, y) + `}]}`
		_, err := validateJWKS([]byte(raw))
		require.ErrorContains(t, err, "duplicate JSON name")
	})
}

func TestClientMetadataResolverRequiresExactKid(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "present")
	server := metadataServer(t, func(clientID string) any {
		return map[string]any{"client_id": clientID, "jwks": map[string]any{"keys": []any{key}}}
	})
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	_, err := resolver.ResolveKey(t.Context(), server.URL, "absent")
	require.ErrorContains(t, err, `kid "absent"`)
}

func TestClientMetadataResolverRejectsRedirectWithoutFollowing(t *testing.T) {
	t.Parallel()
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		writeJSON(t, w, map[string]any{})
	}))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	t.Cleanup(source.Close)

	resolver := testMetadataResolver(t, source.Client(), ClientMetadataResolverOptions{})
	_, err := resolver.ResolveKey(t.Context(), source.URL, "key")
	require.ErrorContains(t, err, "HTTP status 302")
	require.Zero(t, targetRequests.Load())
}

func TestClientMetadataResolverResponseValidationAndBounds(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key")
	t.Run("metadata limit plus one", func(t *testing.T) {
		t.Parallel()
		server := metadataServer(t, func(string) any { return strings.Repeat("x", 33) })
		resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{MaxMetadataBytes: 32})
		_, err := resolver.ResolveKey(t.Context(), server.URL, "key")
		require.ErrorContains(t, err, "exceeds 32-byte limit")
	})
	t.Run("remote JWKS independent limit", func(t *testing.T) {
		t.Parallel()
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/keys" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, strings.Repeat("x", 65))
				return
			}
			writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks_uri": server.URL + "/keys"})
		}))
		t.Cleanup(server.Close)
		resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{MaxMetadataBytes: 1024, MaxJWKSBytes: 64})
		_, err := resolver.ResolveKey(t.Context(), server.URL, "key")
		require.ErrorContains(t, err, "JWKS exceeds 64-byte limit")
	})
	t.Run("inline JWKS independent limit", func(t *testing.T) {
		t.Parallel()
		server := metadataServer(t, func(clientID string) any {
			return map[string]any{"client_id": clientID, "jwks": map[string]any{"keys": []any{key}}}
		})
		resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{MaxMetadataBytes: 4096, MaxJWKSBytes: 16})
		_, err := resolver.ResolveKey(t.Context(), server.URL, "key")
		require.ErrorContains(t, err, "inline JWKS exceeds 16-byte limit")
	})
	t.Run("wrong content type", func(t *testing.T) {
		t.Parallel()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, "{}")
		}))
		t.Cleanup(server.Close)
		resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
		_, err := resolver.ResolveKey(t.Context(), server.URL, "key")
		require.ErrorContains(t, err, "unexpected Content-Type")
	})
}

func TestClientMetadataResolverIndependentTimeouts(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key")
	tests := []struct {
		name string
		path string
		opts ClientMetadataResolverOptions
	}{
		{name: "metadata", path: "/client", opts: ClientMetadataResolverOptions{MetadataTimeout: 20 * time.Millisecond, JWKSTimeout: time.Second}},
		{name: "JWKS", path: "/keys", opts: ClientMetadataResolverOptions{MetadataTimeout: time.Second, JWKSTimeout: 20 * time.Millisecond}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == tc.path {
					<-r.Context().Done()
					return
				}
				if r.URL.Path == "/client" {
					writeJSON(t, w, map[string]any{"client_id": server.URL + "/client", "jwks_uri": server.URL + "/keys"})
					return
				}
				writeJSON(t, w, map[string]any{"keys": []any{key}})
			}))
			t.Cleanup(server.Close)
			resolver := testMetadataResolver(t, server.Client(), tc.opts)
			_, err := resolver.ResolveKey(t.Context(), server.URL+"/client", "key")
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}

func TestClientMetadataResolverSSRFPolicy(t *testing.T) {
	t.Parallel()
	_, err := NewClientMetadataResolver(ClientMetadataResolverOptions{HTTPClient: http.DefaultClient})
	require.ErrorContains(t, err, "requires explicit private-network")

	resolver, err := NewClientMetadataResolver(ClientMetadataResolverOptions{})
	require.NoError(t, err)
	for _, endpoint := range []string{
		"http://example.com/client", "https://user@example.com/client", "https://example.com/client#fragment",
		"https://localhost/client", "https://127.0.0.1/client", "https://[::1]/client",
		"https://100.64.0.1/client", "https://198.18.0.1/client",
	} {
		_, err := resolver.ResolveKey(t.Context(), endpoint, "key")
		require.Error(t, err, endpoint)
	}

	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(listener.Close)
	_, err = resolver.ResolveKey(t.Context(), strings.Replace(listener.URL, "http://", "https://", 1), "key")
	require.ErrorContains(t, err, "not public")
}

func TestUnsafeMetadataIPRejectsProviderAndMappedRanges(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"0.0.0.1", "100.64.0.1", "100.127.255.254", "198.18.0.1", "198.19.255.254",
		"192.0.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1", "2001:db8::1",
		"64:ff9b::7f00:1", "64:ff9b:1::1", "::ffff:100.64.0.1", "::ffff:198.18.0.1", "::ffff:192.0.2.1",
	} {
		require.True(t, unsafeMetadataIP(netip.MustParseAddr(raw)), raw)
	}
	for _, raw := range []string{"100.63.255.255", "100.128.0.0", "198.17.255.255", "198.20.0.0", "8.8.8.8", "2606:4700:4700::1111"} {
		require.False(t, unsafeMetadataIP(netip.MustParseAddr(raw)), raw)
	}
}

func TestJSONContentTypeIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"application/json", "Application/JSON", "APPLICATION/JWK-SET+JSON", "Application/Problem+JSON; Charset=UTF-8"} {
		require.True(t, isJSONContentType(value), value)
	}
	require.False(t, isJSONContentType("text/json"))
}

func TestClientMetadataResolverCoalescesConcurrentFetches(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key")
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		once.Do(func() { close(entered) })
		<-release
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{key}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			_, err := resolver.ResolveKey(context.Background(), server.URL, "key")
			errs <- err
		}()
	}
	close(start)
	<-entered
	require.Eventually(t, func() bool {
		resolver.mu.Lock()
		defer resolver.mu.Unlock()
		return resolver.flights[server.URL] != nil && resolver.flights[server.URL].waiters == callers
	}, time.Second, time.Millisecond)
	close(release)
	for range callers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), requests.Load())
}

func TestClientMetadataResolverRefreshRotationAndCoalescing(t *testing.T) {
	t.Parallel()
	oldJWK := testMetadataJWK(t, "key")
	newJWK := testMetadataJWK(t, "key")
	// Generate another key if the helper happened to be changed to deterministic.
	for oldJWK["x"] == newJWK["x"] {
		newJWK = testMetadataJWK(t, "key")
	}
	var version atomic.Int32
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		key := oldJWK
		if version.Load() != 0 {
			key = newJWK
		}
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{key}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	oldKey, err := resolver.ResolveKey(t.Context(), server.URL, "key")
	require.NoError(t, err)
	version.Store(1)
	stillOld, err := resolver.ResolveKey(t.Context(), server.URL, "key")
	require.NoError(t, err)
	require.True(t, oldKey.Key.Equal(stillOld.Key))

	const callers = 16
	start := make(chan struct{})
	results := make(chan *ClientSigningKey, callers)
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			key, err := resolver.RefreshKey(context.Background(), server.URL, "key")
			results <- key
			errs <- err
		}()
	}
	close(start)
	for range callers {
		require.NoError(t, <-errs)
		rotated := <-results
		require.False(t, oldKey.Key.Equal(rotated.Key))
	}
	// Scheduling can allow a refresh to finish before late goroutines enter; the
	// cache remains bypassed for those genuinely subsequent explicit refreshes.
	// The important invariant is no more requests than refresh callers plus seed.
	require.LessOrEqual(t, requests.Load(), int32(callers+1))
}

func TestClientMetadataResolverDoesNotServeStaleOnError(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key")
	var fail atomic.Bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{key}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{TTL: 5 * time.Millisecond})
	_, err := resolver.ResolveKey(t.Context(), server.URL, "key")
	require.NoError(t, err)
	fail.Store(true)
	time.Sleep(10 * time.Millisecond)
	_, err = resolver.ResolveKey(t.Context(), server.URL, "key")
	require.ErrorContains(t, err, "HTTP status 503")
	_, err = resolver.ResolveKey(t.Context(), server.URL, "key")
	require.ErrorContains(t, err, "HTTP status 503")
}

func TestClientMetadataResolverCacheBoundsAndEviction(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "key")
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		writeJSON(t, w, map[string]any{"client_id": server.URL + r.URL.Path, "jwks": map[string]any{"keys": []any{key}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{MaxCacheEntries: 1})
	for _, path := range []string{"/one", "/two", "/one"} {
		_, err := resolver.ResolveKey(t.Context(), server.URL+path, "key")
		require.NoError(t, err)
	}
	require.Equal(t, int32(3), requests.Load())

	tiny := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{MaxCacheBytes: 1})
	_, err := tiny.ResolveKey(t.Context(), server.URL+"/tiny", "key")
	require.ErrorContains(t, err, "cache bound")
}

func TestClientMetadataResolverCanceledFlightCannotRepopulate(t *testing.T) {
	t.Parallel()
	oldJWK := testMetadataJWK(t, "key")
	newJWK := testMetadataJWK(t, "key")
	oldBody := func(clientID string) []byte {
		return []byte(marshalString(t, map[string]any{"client_id": clientID, "jwks": map[string]any{"keys": []any{oldJWK}}}))
	}
	newBody := func(clientID string) []byte {
		return []byte(marshalString(t, map[string]any{"client_id": clientID, "jwks": map[string]any{"keys": []any{newJWK}}}))
	}
	entered := make(chan struct{})
	releaseOld := make(chan struct{})
	var request atomic.Int32
	clientID := "http://metadata.test/client"
	client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		if request.Add(1) == 1 {
			close(entered)
			<-releaseOld // deliberately ignore request cancellation
			return jsonResponse(oldBody(clientID)), nil
		}
		return jsonResponse(newBody(clientID)), nil
	})}
	resolver := testMetadataResolver(t, client, ClientMetadataResolverOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := resolver.ResolveKey(ctx, clientID, "key")
		firstDone <- err
	}()
	<-entered
	cancel()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	current, err := resolver.ResolveKey(t.Context(), clientID, "key")
	require.NoError(t, err)
	close(releaseOld)
	require.Eventually(t, func() bool { return request.Load() == 2 }, time.Second, time.Millisecond)
	time.Sleep(time.Millisecond)
	cached, err := resolver.ResolveKey(t.Context(), clientID, "key")
	require.NoError(t, err)
	require.True(t, current.Key.Equal(cached.Key))
	require.False(t, metadataJWKPublicKey(t, oldJWK).Equal(cached.Key))
}

func TestVerifyResolvedClientAttestationRefreshesOnceBeforeReplay(t *testing.T) {
	t.Parallel()
	oldKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	newKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	oldJWK := metadataJWKFromKey(t, oldKey, "key")
	newJWK := metadataJWKFromKey(t, newKey, "key")
	var rotated atomic.Bool
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		jwk := oldJWK
		if rotated.Load() {
			jwk = newJWK
		}
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{jwk}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	_, err = resolver.ResolveKey(t.Context(), server.URL, "key")
	require.NoError(t, err)
	rotated.Store(true)

	now := time.Now().Truncate(time.Second)
	audience := "did:plc:ewvi7nxzyoun6zhxrhs64oiz#atproto_space_host"
	token, err := CreateClientAttestationToken(ClientAttestationTokenParams{
		ClientID: server.URL, Audience: audience, KeyID: "key", Now: now,
	}, newKey)
	require.NoError(t, err)
	store, err := NewMemoryReplayStore(4)
	require.NoError(t, err)
	replay := &countingReplayStore{delegate: store}
	verified, err := VerifyResolvedClientAttestation(t.Context(), token, VerifyResolvedClientAttestationOptions{
		Resolver: resolver, Audience: audience, Now: now, Replay: replay,
	})
	require.NoError(t, err)
	require.Equal(t, server.URL, verified.Issuer)
	require.Equal(t, int32(2), requests.Load())
	require.Equal(t, int32(1), replay.calls.Load())

	_, err = VerifyResolvedClientAttestation(t.Context(), token, VerifyResolvedClientAttestationOptions{
		Resolver: resolver, Audience: audience, Now: now, Replay: replay,
	})
	require.ErrorIs(t, err, ErrReplay)
	require.Equal(t, int32(2), replay.calls.Load())

	unknownKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	badToken, err := CreateClientAttestationToken(ClientAttestationTokenParams{
		ClientID: server.URL, Audience: audience, KeyID: "key", Now: now,
	}, unknownKey)
	require.NoError(t, err)
	before := replay.calls.Load()
	_, err = VerifyResolvedClientAttestation(t.Context(), badToken, VerifyResolvedClientAttestationOptions{
		Resolver: resolver, Audience: audience, Now: now, Replay: replay,
	})
	require.ErrorContains(t, err, "invalid JWT signature")
	require.Equal(t, before, replay.calls.Load(), "failed signatures must not consume replay state")
	require.Equal(t, int32(3), requests.Load(), "a failed signature receives exactly one forced refresh")
}

func TestVerifyResolvedClientAttestationRefreshesNewKidFromCachedJWKS(t *testing.T) {
	t.Parallel()
	oldKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	newKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	var rotated atomic.Bool
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		jwk := metadataJWKFromKey(t, oldKey, "old-kid")
		if rotated.Load() {
			jwk = metadataJWKFromKey(t, newKey, "new-kid")
		}
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{jwk}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	_, err = resolver.ResolveKey(t.Context(), server.URL, "old-kid")
	require.NoError(t, err)
	rotated.Store(true)

	now := time.Now().Truncate(time.Second)
	audience := "did:plc:ewvi7nxzyoun6zhxrhs64oiz#atproto_space_host"
	token, err := CreateClientAttestationToken(ClientAttestationTokenParams{
		ClientID: server.URL, Audience: audience, KeyID: "new-kid", Now: now,
	}, newKey)
	require.NoError(t, err)
	replay, err := NewMemoryReplayStore(1)
	require.NoError(t, err)
	_, err = VerifyResolvedClientAttestation(t.Context(), token, VerifyResolvedClientAttestationOptions{
		Resolver: resolver, Audience: audience, Now: now, Replay: replay,
	})
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load())
}

func TestVerifyResolvedClientAttestationDoesNotRefetchFreshMissingKid(t *testing.T) {
	t.Parallel()
	key := testMetadataJWK(t, "published")
	var requests atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeJSON(t, w, map[string]any{"client_id": server.URL, "jwks": map[string]any{"keys": []any{key}}})
	}))
	t.Cleanup(server.Close)
	resolver := testMetadataResolver(t, server.Client(), ClientMetadataResolverOptions{})
	privateKey, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	now := time.Now().Truncate(time.Second)
	audience := "did:plc:ewvi7nxzyoun6zhxrhs64oiz#atproto_space_host"
	token, err := CreateClientAttestationToken(ClientAttestationTokenParams{
		ClientID: server.URL, Audience: audience, KeyID: "missing", Now: now,
	}, privateKey)
	require.NoError(t, err)
	replay, err := NewMemoryReplayStore(1)
	require.NoError(t, err)
	_, err = VerifyResolvedClientAttestation(t.Context(), token, VerifyResolvedClientAttestationOptions{
		Resolver: resolver, Audience: audience, Now: now, Replay: replay,
	})
	require.Error(t, err)
	require.Equal(t, int32(1), requests.Load())
}

func FuzzValidateMetadataJWKS(f *testing.F) {
	key, err := atmoscrypto.GenerateP256()
	require.NoError(f, err)
	jwk, err := PublicJWK(key.PublicKey())
	require.NoError(f, err)
	valid := marshalString(f, map[string]any{"keys": []any{map[string]any{
		"kty": jwk.KTY, "crv": jwk.CRV, "x": jwk.X, "y": jwk.Y, "kid": "key",
	}}})
	f.Add([]byte(valid))
	f.Add([]byte(`{"keys":[{"kty":"EC","crv":"P-256","kid":"x","x":"","y":"","d":"secret"}]}`))
	f.Add([]byte(`{"keys":[]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		keys, err := validateJWKS(data)
		if err != nil {
			return
		}
		require.NotEmpty(t, keys)
		seen := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			require.NotEmpty(t, key.kid)
			require.NotNil(t, key.key)
			_, duplicate := seen[key.kid]
			require.False(t, duplicate)
			seen[key.kid] = struct{}{}
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

type countingReplayStore struct {
	delegate ReplayStore
	calls    atomic.Int32
}

func (s *countingReplayStore) Consume(ctx context.Context, namespace ReplayNamespace, id string, expiresAt time.Time) error {
	s.calls.Add(1)
	return s.delegate.Consume(ctx, namespace, id, expiresAt)
}

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func jsonResponse(body []byte) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
}

func testMetadataResolver(t testing.TB, client *http.Client, override ClientMetadataResolverOptions) *ClientMetadataResolver {
	t.Helper()
	override.HTTPClient = client
	override.EndpointPolicy = MetadataEndpointPolicy{AllowHTTP: true, AllowPrivateNetworks: true}
	resolver, err := NewClientMetadataResolver(override)
	require.NoError(t, err)
	return resolver
}

func metadataServer(t *testing.T, body func(clientID string) any) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		value := body(server.URL)
		switch value := value.(type) {
		case string:
			_, _ = io.WriteString(w, value)
		default:
			writeJSON(t, w, value)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func testMetadataJWK(t testing.TB, kid string) map[string]any {
	t.Helper()
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	return metadataJWKFromKey(t, key, kid)
}

func metadataJWKFromKey(t testing.TB, key *atmoscrypto.P256PrivateKey, kid string) map[string]any {
	t.Helper()
	jwk, err := PublicJWK(key.PublicKey())
	require.NoError(t, err)
	return map[string]any{"kty": jwk.KTY, "crv": jwk.CRV, "x": jwk.X, "y": jwk.Y, "kid": kid}
}

func metadataJWKPublicKey(t testing.TB, raw map[string]any) *atmoscrypto.P256PublicKey {
	t.Helper()
	keys, err := validateJWKS([]byte(marshalString(t, map[string]any{"keys": []any{raw}})))
	require.NoError(t, err)
	return keys[0].key
}

func writeJSON(t testing.TB, writer io.Writer, value any) {
	t.Helper()
	if response, ok := writer.(http.ResponseWriter); ok {
		response.Header().Set("Content-Type", "application/json")
	}
	require.NoError(t, json.NewEncoder(writer).Encode(value))
}

func marshalString(t testing.TB, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}

func quoteJSON(t testing.TB, value string) string {
	t.Helper()
	return marshalString(t, value)
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

var _ = errors.Is
