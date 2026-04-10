//go:build !js

package identity

import (
	"net/http"
	"time"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/jttp"
)

// newDefaultHTTPClient returns an HTTP client with SSRF protection for
// server-side identity resolution. Uses jttp for automatic retries with
// exponential backoff on transient failures.
func newDefaultHTTPClient() *http.Client {
	t := xrpc.NewTransport()
	t.DialContext = ssrfSafeDialContext(10 * time.Second)
	return jttp.New(
		jttp.WithTransport(t),
		jttp.WithTimeout(10*time.Second),
		jttp.WithAdditionalRetryableStatusCodes(http.StatusInternalServerError),
	)
}
