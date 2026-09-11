//go:build !js

package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"time"

	"github.com/jcalabro/atmos/xrpc"
)

var blockedNetworkPrefixes = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001:db8::/32"),
}

// NetworkPolicy controls explicit local-test exceptions in the correctness
// transport. Production callers should use the zero value.
type NetworkPolicy struct {
	AllowPrivateNetworks bool
}

// NewCorrectnessHTTPClient returns the explicit HTTP/1, no-connection-reuse
// correctness transport used until a pooled proof-per-wire-send transport is
// available. It has no proxy, rejects redirects, validates every resolved IP
// at dial time, and suppresses the standard library's reproduced hidden retry
// paths by never reusing a connection and disabling HTTP/2.
//
// This mode is intentionally not described as production-performance ready.
func NewCorrectnessHTTPClient(policy NetworkPolicy) *http.Client {
	transport := &http.Transport{
		Proxy:                  nil,
		ForceAttemptHTTP2:      false,
		DisableKeepAlives:      true,
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: xrpc.MaxResponseHeaderBytes,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
	}
	hardenTransportProtocol(transport)
	hardenTransportNetwork(transport, policy)
	return &http.Client{Transport: transport, Timeout: 30 * time.Minute, CheckRedirect: rejectRedirect}
}

func hardenTransportNetwork(transport *http.Transport, policy NetworkPolicy) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	transport.DialTLSContext = nil
	transport.DialTLS = nil //nolint:staticcheck // Clear the deprecated caller hook so it cannot bypass the hardened dialer.
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("space client: split dial address: %w", err)
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("space client: resolve dial host: %w", err)
		}
		var lastErr error
		for _, ip := range ips {
			if !policy.AllowPrivateNetworks && unsafeIP(ip) {
				lastErr = fmt.Errorf("space client: resolved address %s is not public", ip)
				continue
			}
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("space client: host resolved to no addresses")
		}
		return nil, lastErr
	}
}

func unsafeIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	for _, prefix := range blockedNetworkPrefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}
