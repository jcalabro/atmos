//go:build js

package identity

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type jsRoundTripFunc func(*http.Request) (*http.Response, error)

func (f jsRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNoRedirectFetchTransportSetsBrowserPolicy(t *testing.T) {
	t.Parallel()

	var gotRedirectMode string
	transport := noRedirectFetchTransport{
		useFetchRedirect: true,
		next: jsRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotRedirectMode = req.Header.Get(jsFetchRedirectHeader)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    req,
			}, nil
		}),
	}
	req, err := http.NewRequest(http.MethodGet, "https://plc.directory/did:plc:test", nil)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "error", gotRedirectMode)
	assert.Empty(t, req.Header.Get(jsFetchRedirectHeader), "transport mutated caller request")
}
