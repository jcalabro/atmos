//go:build js

package identity

import "net/http"

// newDefaultHTTPClient returns a plain HTTP client for browser environments.
// In WASM, Go's http.Transport.RoundTrip uses the Fetch API, but only when
// no custom DialContext is set. We therefore use the default transport to
// ensure requests go through fetch() rather than attempting raw sockets.
func newDefaultHTTPClient() *http.Client {
	return &http.Client{}
}
