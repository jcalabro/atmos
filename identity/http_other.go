//go:build !js

package identity

import (
	"net/http"
	"time"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/jttp"
)

// newDefaultHTTPClient returns the HTTP client used by [DefaultResolver] for
// attacker-controlled did:web and handle well-known fetches.
//
// Configuration:
//   - jttp's strict SSRF protection rejects loopback / private /
//     link-local / IMDS targets at dial time. Identity resolution
//     follows attacker-controlled URLs (any handle's domain), so the
//     guard MUST cover the initial request URL, not just redirects.
//   - The shared [xrpc.ATProtoOpts] baseline supplies HTTP/2,
//     connection pooling sized for fan-out (50 idle conns / host,
//     100 max), TLS 1.2 minimum, and retry semantics for transient
//     failures (including 500s, which misconfigured well-known endpoints
//     commonly serve during deploys).
//
// Each [DefaultResolver] instance lazily builds and reuses one client so its
// connection pool remains warm.
func newDefaultHTTPClient() *http.Client {
	opts := append(
		xrpc.ATProtoOpts(10*time.Second),
		jttp.WithStrictSSRFProtection(),
		jttp.WithNoProxy(),
	)
	return jttp.New(opts...)
}

// newDefaultPLCHTTPClient returns a pooled client for the fixed,
// operator-controlled PLC endpoint. It deliberately avoids strict SSRF DNS
// checks on every request; redirects remain disabled so that trust cannot move
// to a different origin.
func newDefaultPLCHTTPClient() *http.Client {
	opts := append(
		xrpc.ATProtoOpts(10*time.Second),
		jttp.WithNoRedirects(),
	)
	return jttp.New(opts...)
}
