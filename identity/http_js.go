//go:build js

package identity

import (
	"net/http"
	"strings"
	"syscall/js"
	"time"
)

const jsFetchRedirectHeader = "js.fetch:redirect"

type noRedirectFetchTransport struct {
	next             http.RoundTripper
	useFetchRedirect bool
}

func (t noRedirectFetchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.useFetchRedirect {
		return t.next.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set(jsFetchRedirectHeader, "error")
	return t.next.RoundTrip(clone)
}

// browserFetchEnabled mirrors net/http's js/wasm selection: Node disables the
// Fetch transport and uses its socket-backed test implementation instead.
func browserFetchEnabled() bool {
	global := js.Global()
	if global.Get("fetch").IsUndefined() {
		return false
	}
	process := global.Get("process")
	return process.Type() != js.TypeObject || !strings.HasPrefix(process.Get("argv0").String(), "node")
}

// newDefaultHTTPClient returns a plain HTTP client for browser environments.
// In WASM, Go's http.Transport.RoundTrip uses the Fetch API, but only when
// no custom DialContext is set. We therefore use the default transport to
// ensure requests go through fetch() rather than attempting raw sockets.
func newDefaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}

// newDefaultPLCHTTPClient remains Fetch-compatible while refusing redirects
// at both the browser Fetch layer and the Go HTTP client layer.
func newDefaultPLCHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: noRedirectFetchTransport{
			next:             http.DefaultTransport,
			useFetchRedirect: browserFetchEnabled(),
		},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
