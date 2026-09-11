package host

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/serviceauth"
	"github.com/jcalabro/atmos/space/simplespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPManagingAppChecker_RequestAndAuthentication(t *testing.T) {
	t.Parallel()

	const managingApp = "did:plc:defghijklmnopqrstuvwxyza#manager"
	space := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
	user := atmos.DID("did:plc:bcdefghijklmnopqrstuvwxy")
	authorityKey := mustP256(t)
	now := time.Now().UTC().Truncate(time.Second)
	var resolver *integrationResolver
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, "/base/xrpc/com.atproto.simplespace.checkUserAccess", req.URL.Path)
		require.Equal(t, space.String(), req.URL.Query().Get("space"))
		require.Equal(t, user.String(), req.URL.Query().Get("user"))
		require.Equal(t, "write", req.URL.Query().Get("access"))
		require.Equal(t, "https://client.example/metadata.json", req.URL.Query().Get("clientId"))
		raw := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		require.NotEmpty(t, raw)
		claims, err := serviceauth.VerifyToken(req.Context(), raw, serviceauth.VerifyOptions{
			Audience:  managingApp,
			Identity:  &identity.Directory{Resolver: resolver},
			LexMethod: "com.atproto.simplespace.checkUserAccess",
		})
		require.NoError(t, err)
		require.Equal(t, space.Authority(), claims.Issuer)
		require.True(t, now.Equal(claims.IssuedAt))
		require.True(t, now.Add(time.Minute).Equal(claims.ExpiresAt))
		_, _ = io.WriteString(w, `{"authorized":true}`)
	}))
	defer server.Close()
	resolver = &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		space.Authority(): didDoc(space.Authority(), authorityKey, nil),
		"did:plc:defghijklmnopqrstuvwxyza": didDoc("did:plc:defghijklmnopqrstuvwxyza", mustP256(t), &identity.Service{
			ID: "#manager", Type: "AtprotoManagingApp", ServiceEndpoint: server.URL + "/base/",
		}),
	}}
	checker, err := NewHTTPManagingAppChecker(HTTPManagingAppOptions{
		Resolver: resolver, Signer: &integrationSigner{authority: space.Authority(), key: authorityKey}, Clock: ClockFunc(func() time.Time { return now }),
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		ServiceType: func(_ context.Context, identifier string) (string, error) {
			require.Equal(t, managingApp, identifier)
			return "AtprotoManagingApp", nil
		},
		Timeout: time.Second, MaxResponse: 1024,
	})
	require.NoError(t, err)

	allowed, err := checker.CheckUserAccess(t.Context(), managingApp, space, user, simplespace.AccessWrite, "https://client.example/metadata.json")
	require.NoError(t, err)
	assert.True(t, allowed)
}

func TestHTTPManagingAppChecker_FailsClosed(t *testing.T) {
	t.Parallel()

	const managingApp = "did:plc:defghijklmnopqrstuvwxyza#manager"
	space := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
	user := atmos.DID("did:plc:bcdefghijklmnopqrstuvwxy")
	authorityKey := mustP256(t)

	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{name: "denial is a decision", handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"authorized":false}`) }},
		{name: "HTTP error", handler: func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusServiceUnavailable) }},
		{name: "malformed JSON", handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{`) }},
		{name: "oversized response", handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 65)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
				"did:plc:defghijklmnopqrstuvwxyza": didDoc("did:plc:defghijklmnopqrstuvwxyza", mustP256(t), &identity.Service{
					ID: "#manager", Type: "AtprotoManagingApp", ServiceEndpoint: server.URL,
				}),
			}}
			checker, err := NewHTTPManagingAppChecker(HTTPManagingAppOptions{
				Resolver: resolver, Signer: &integrationSigner{authority: space.Authority(), key: authorityKey}, Clock: ClockFunc(time.Now),
				EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
				ServiceType:    func(context.Context, string) (string, error) { return "AtprotoManagingApp", nil },
				Timeout:        time.Second, MaxResponse: 64,
			})
			require.NoError(t, err)

			allowed, err := checker.CheckUserAccess(t.Context(), managingApp, space, user, simplespace.AccessRead, "")
			if tt.name == "denial is a decision" {
				require.NoError(t, err)
				assert.False(t, allowed)
				return
			}
			assert.False(t, allowed)
			assert.ErrorIs(t, err, ErrManagingAppUnavailable)
		})
	}
}

func TestHTTPManagingAppChecker_RejectsRedirectsAndInvalidPolicy(t *testing.T) {
	t.Parallel()

	const managingApp = "did:plc:defghijklmnopqrstuvwxyza#manager"
	space := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
	key := mustP256(t)
	var followed atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed.Add(1)
		_, _ = io.WriteString(w, `{"authorized":true}`)
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		"did:plc:defghijklmnopqrstuvwxyza": didDoc("did:plc:defghijklmnopqrstuvwxyza", mustP256(t), &identity.Service{
			ID: "#manager", Type: "AtprotoManagingApp", ServiceEndpoint: redirect.URL,
		}),
	}}
	checker, err := NewHTTPManagingAppChecker(HTTPManagingAppOptions{
		Resolver: resolver, Signer: &integrationSigner{authority: space.Authority(), key: key}, Clock: ClockFunc(time.Now),
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		ServiceType:    func(context.Context, string) (string, error) { return "AtprotoManagingApp", nil },
		Timeout:        time.Second, MaxResponse: 1024,
	})
	require.NoError(t, err)
	_, err = checker.CheckUserAccess(t.Context(), managingApp, space, integrationAuthorOne, simplespace.AccessRead, "")
	assert.ErrorIs(t, err, ErrManagingAppUnavailable)
	assert.Zero(t, followed.Load())

	_, err = NewHTTPManagingAppChecker(HTTPManagingAppOptions{})
	require.Error(t, err)
	_, err = NewHTTPManagingAppChecker(HTTPManagingAppOptions{
		Resolver: resolver, Signer: &integrationSigner{authority: space.Authority(), key: key}, Clock: ClockFunc(time.Now),
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		ServiceType:    func(context.Context, string) (string, error) { return "AtprotoManagingApp", nil },
		Timeout:        time.Second, MaxResponse: math.MaxInt64,
	})
	require.Error(t, err)
	checker.serviceType = func(context.Context, string) (string, error) { return "", errors.New("policy unavailable") }
	_, err = checker.CheckUserAccess(t.Context(), managingApp, space, integrationAuthorOne, simplespace.AccessRead, "")
	assert.ErrorIs(t, err, ErrManagingAppUnavailable)
}

func TestHTTPManagingAppChecker_TimeoutBoundsResolution(t *testing.T) {
	t.Parallel()

	space := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
	checker, err := NewHTTPManagingAppChecker(HTTPManagingAppOptions{
		Resolver: &blockingManagingResolver{}, Signer: &integrationSigner{authority: space.Authority(), key: mustP256(t)},
		Clock: ClockFunc(time.Now), EndpointPolicy: identity.EndpointPolicy{},
		ServiceType: func(context.Context, string) (string, error) { return "AtprotoManagingApp", nil },
		Timeout:     20 * time.Millisecond, MaxResponse: 1024,
	})
	require.NoError(t, err)
	started := time.Now()
	_, err = checker.CheckUserAccess(t.Context(), "did:plc:defghijklmnopqrstuvwxyza#manager", space, integrationAuthorOne, simplespace.AccessRead, "")
	assert.ErrorIs(t, err, ErrManagingAppUnavailable)
	assert.Less(t, time.Since(started), time.Second)
}

type blockingManagingResolver struct{}

func (*blockingManagingResolver) ResolveDID(ctx context.Context, _ atmos.DID) (*identity.DIDDocument, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingManagingResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", errors.New("not implemented")
}

func TestHTTPDeliveryTransport_BoundsAndSanitizesRequest(t *testing.T) {
	t.Parallel()

	method := atmos.NSID("com.atproto.space.notifyWrite")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, "/base/xrpc/"+method.String(), req.URL.Path)
		require.Empty(t, req.URL.RawQuery)
		require.Equal(t, "Bearer service-token", req.Header.Get("Authorization"))
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"space":"value"}`, string(body))
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	transport, err := NewHTTPDeliveryTransport(identity.EndpointPolicy{AllowPrivateLiteral: true}, time.Second, 16)
	require.NoError(t, err)
	endpoint, err := url.Parse(server.URL + "/base/?discard=yes#fragment")
	require.NoError(t, err)
	require.NoError(t, transport.Deliver(t.Context(), endpoint, method, "service-token", map[string]string{"space": "value"}))

	_, err = NewHTTPDeliveryTransport(identity.EndpointPolicy{}, 0, 1)
	require.Error(t, err)
	_, err = NewHTTPDeliveryTransport(identity.EndpointPolicy{}, time.Second, math.MaxInt64)
	require.Error(t, err)
	require.Error(t, (*HTTPDeliveryTransport)(nil).Deliver(t.Context(), endpoint, method, "token", nil))
}

func TestHTTPDeliveryTransport_RejectsRedirectStatusAndOversize(t *testing.T) {
	t.Parallel()

	var followed atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	defer destination.Close()

	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{name: "redirect", handler: func(w http.ResponseWriter, req *http.Request) {
			http.Redirect(w, req, destination.URL, http.StatusTemporaryRedirect)
		}},
		{name: "error status", handler: func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusServiceUnavailable) }},
		{name: "oversize", handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 17)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			transport, err := NewHTTPDeliveryTransport(identity.EndpointPolicy{AllowPrivateLiteral: true}, time.Second, 16)
			require.NoError(t, err)
			endpoint, err := url.Parse(server.URL)
			require.NoError(t, err)
			require.Error(t, transport.Deliver(context.Background(), endpoint, "com.atproto.space.notifyWrite", "token", struct{}{}))
		})
	}
	assert.Zero(t, followed.Load())
}
