//go:build js

package client

import (
	"net/http"
	"time"
)

// NetworkPolicy is retained on WebAssembly, where the browser owns DNS and
// socket policy. A successful WASM build does not imply native SSRF guarantees.
type NetworkPolicy struct {
	AllowPrivateNetworks     bool
	AllowPrivateLiteralHosts bool
}

// NewCorrectnessHTTPClient returns a redirect-rejecting browser HTTP client.
// Browser fetch controls connection reuse, proxying, and DNS; callers must
// enforce equivalent origin policy in their embedding environment.
func NewCorrectnessHTTPClient(_ NetworkPolicy) *http.Client {
	return &http.Client{Timeout: 30 * time.Minute, CheckRedirect: rejectRedirect}
}

// NewPooledHTTPClient returns a redirect-rejecting browser HTTP client. Browser
// fetch owns connection pooling, wire retries, proxying, and DNS; callers must
// enforce equivalent transport policy in their embedding environment.
func NewPooledHTTPClient(_ NetworkPolicy) *http.Client {
	return &http.Client{Timeout: 30 * time.Minute, CheckRedirect: rejectRedirect}
}

func hardenTransportNetwork(_ *http.Transport, _ NetworkPolicy) {}
