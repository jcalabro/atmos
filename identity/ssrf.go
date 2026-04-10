package identity

import (
	"context"
	"fmt"
	"net"
	"time"
)

// ssrfSafeDialContext returns a DialContext function that refuses to connect to
// private, loopback, or link-local IP addresses. This prevents SSRF attacks when
// resolving user-supplied handles and DIDs.
//
// If any resolved IP for a hostname is non-routable, the connection is refused
// entirely. This prevents DNS rebinding attacks where a hostname has both public
// and private A records.
func ssrfSafeDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}

		for _, ip := range ips {
			if isNonRoutableIP(ip.IP) {
				return nil, fmt.Errorf("identity: %q resolved to non-routable address %s", host, ip.IP)
			}
		}

		// Dial the resolved IP directly to prevent TOCTOU races from DNS rebinding.
		resolved := net.JoinHostPort(ips[0].IP.String(), port)
		return dialer.DialContext(ctx, network, resolved)
	}
}

// isNonRoutableIP returns true for IPs that should not be contacted during
// identity resolution: loopback, private, link-local, and unspecified addresses.
func isNonRoutableIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified()
}
