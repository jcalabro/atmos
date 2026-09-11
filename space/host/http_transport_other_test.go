//go:build !js

package host

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPDeliveryTransport_PrivateLiteralPolicyExcludesHostnames(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	literal, err := url.Parse(server.URL)
	require.NoError(t, err)
	transport, err := NewHTTPDeliveryTransport(identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true}, time.Second, 16)
	require.NoError(t, err)

	require.NoError(t, transport.Deliver(t.Context(), literal, "com.atproto.space.notifyWrite", "token", struct{}{}))
	require.Equal(t, int32(1), calls.Load())

	hostname := *literal
	hostname.Host = net.JoinHostPort("localhost", literal.Port())
	err = transport.Deliver(t.Context(), &hostname, "com.atproto.space.notifyWrite", "token", struct{}{})
	require.ErrorContains(t, err, "not public", "AllowPrivateLiteral must not admit private DNS answers for hostnames")
	assert.Equal(t, int32(1), calls.Load())
}

func TestHTTPDeliveryTransport_RejectsResolvedPrivateAddress(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	transport, err := NewHTTPDeliveryTransport(identity.EndpointPolicy{}, time.Second, 16)
	require.NoError(t, err)
	require.Error(t, transport.Deliver(t.Context(), endpoint, "com.atproto.space.notifyWrite", "token", struct{}{}))
	assert.Zero(t, calls.Load())
}
