//go:build !js

package host

import (
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
