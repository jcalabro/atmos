//go:build !js

package identity

import (
	"net/http"
	"time"

	"github.com/jcalabro/atmos/xrpc"
)

// newDefaultHTTPClient returns an HTTP client with SSRF protection for
// server-side identity resolution.
func newDefaultHTTPClient() *http.Client {
	t := xrpc.NewTransport()
	t.DialContext = ssrfSafeDialContext(10 * time.Second)
	return &http.Client{
		Transport: xrpc.WrapGzip(t),
		Timeout:   10 * time.Second,
	}
}
