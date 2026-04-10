package xrpc

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/jcalabro/jttp"
)

// NewTransport returns an [*http.Transport] with defaults tuned for ATProto
// production workloads: HTTP/2, TLS 1.2 minimum, and robust connection pooling.
//
// Use this when a raw transport is needed (e.g. as a base for custom
// RoundTripper chains). For a complete [*http.Client] prefer [NewHTTPClient].
//
// Each call returns a new, independent transport with its own connection pool.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
}

// NewHTTPClient returns an [*http.Client] backed by jttp with robust defaults
// including automatic retries with exponential backoff, connection pooling, HTTP/2,
// TLS 1.2 minimum, and timeouts tuned for ATProto production workloads.
//
// Retries cover GET/HEAD/OPTIONS on transient failures (429, 500, 502, 503, 504)
// and connection-level errors (resets, refused, timeouts). POST and other methods
// are not retried at this level to avoid non-idempotent side effects.
//
// Callers that implement their own retry loop (e.g. [Client]) should use jttp
// directly with [jttp.WithNoRetries] to avoid compounding retries.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return jttp.New(atprotoOpts(timeout)...)
}

// atprotoOpts returns jttp options tuned for ATProto production workloads.
func atprotoOpts(timeout time.Duration) []jttp.Option {
	return []jttp.Option{
		jttp.WithTimeout(timeout),
		jttp.WithDialTimeout(10 * time.Second),
		jttp.WithResponseHeaderTimeout(15 * time.Second),
		jttp.WithMaxIdleConns(100),
		jttp.WithMaxIdleConnsPerHost(50),
		jttp.WithMaxConnsPerHost(100),
		jttp.WithAdditionalRetryableStatusCodes(http.StatusInternalServerError),
	}
}
