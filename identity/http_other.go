//go:build !js

package identity

import (
	"net/http"
	"time"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/jttp"
)

// newDefaultHTTPClient returns the HTTP client used by [DefaultResolver]
// for PLC and well-known fetches.
//
// Configuration:
//   - jttp's strict SSRF protection rejects loopback / private /
//     link-local / IMDS targets at dial time. Identity resolution
//     follows attacker-controlled URLs (any handle's domain), so the
//     guard MUST cover the initial request URL, not just redirects.
//   - The shared [xrpc.ATProtoOpts] baseline supplies HTTP/2,
//     connection pooling sized for fan-out (50 idle conns / host,
//     100 max), TLS 1.2 minimum, and retry semantics for transient
//     failures (including 500s, which misconfigured well-known
//     endpoints commonly serve during deploys).
//
// Each [DefaultResolver] instance lazily builds and reuses one client,
// so a single resolver running for the lifetime of the process keeps
// the connection pool warm — every cache miss after the first reuses
// keep-alive connections to plc.directory.
func newDefaultHTTPClient() *http.Client {
	opts := append(
		xrpc.ATProtoOpts(10*time.Second),
		jttp.WithStrictSSRFProtection(),
	)
	return jttp.New(opts...)
}
