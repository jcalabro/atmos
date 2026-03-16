package xrpc

import (
	"net"
	"net/http"
	"time"
)

// NewTransport returns an [*http.Transport] with defaults tuned for ATProto
// production workloads.
//
// Each call returns a new, independent transport with its own connection pool.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// NewHTTPClient returns an [*http.Client] backed by [NewTransport] with the
// given overall request timeout. Use this as the default when no
// caller-provided client is set.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: NewTransport(),
		Timeout:   timeout,
	}
}
