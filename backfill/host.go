package backfill

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode"

	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

// ValidateHostname validates an untrusted listHosts hostname for use by the
// default direct-PDS client builder. It intentionally accepts DNS hostnames
// only: no URL syntax, ports, IP literals, local names, or private naming
// suffixes.
func ValidateHostname(hostname string) error {
	if hostname == "" || len(hostname) > 253 {
		return fmt.Errorf("invalid hostname length")
	}
	if hostname != strings.TrimSpace(hostname) || strings.HasSuffix(hostname, ".") {
		return fmt.Errorf("hostname must be canonical DNS text")
	}
	lower := strings.ToLower(hostname)
	if net.ParseIP(lower) != nil {
		return fmt.Errorf("IP literals are not allowed")
	}
	if lower == "localhost" || !strings.Contains(lower, ".") {
		return fmt.Errorf("local or single-label hostname is not allowed")
	}
	for _, suffix := range []string{".local", ".internal", ".lan"} {
		if strings.HasSuffix(lower, suffix) {
			return fmt.Errorf("private hostname suffix %q is not allowed", suffix)
		}
	}
	for _, label := range strings.Split(lower, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("invalid DNS label")
		}
		for _, r := range label {
			if r > unicode.MaxASCII {
				return fmt.Errorf("invalid DNS label character")
			}
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return fmt.Errorf("invalid DNS label character")
			}
		}
	}
	return nil
}

// blockedDialIP is the dial-path IP policy: jttp's redirect/initial-request
// blocklist (loopback, link-local incl. cloud metadata, private + ULA,
// multicast, unspecified, 0.0.0.0/8, limited broadcast) plus two ranges that
// are commonly routed inside provider networks but not covered by
// net.IP.IsPrivate: 100.64.0.0/10 (CGNAT shared space) and 198.18.0.0/15
// (benchmarking). The dialer is the last gate, so it is deliberately
// stricter than the policy-time check. Nil fails closed.
func blockedDialIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsMulticast() ||
		ip.IsUnspecified() || ip.IsPrivate() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		if ip4[0] == 0 || ip4.Equal(net.IPv4bcast) {
			return true
		}
		if ip4[0] == 100 && ip4[1]&0xc0 == 0x40 { // 100.64.0.0/10
			return true
		}
		if ip4[0] == 198 && ip4[1]&0xfe == 18 { // 198.18.0.0/15
			return true
		}
	}
	return false
}

// guardedDialContext resolves the hostname once, rejects any blocked
// address, and then dials only the exact addresses that passed the check.
// This closes the DNS-rebinding TOCTOU in resolve-then-check schemes where
// the policy lookup and the dialer's lookup are separate queries.
func guardedDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	// One deadline covers resolution and every pinned-address attempt: a
	// hostname resolving to many blackholed addresses must not hold a fleet
	// slot for len(addrs) * timeout.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	var addrs []net.IPAddr
	if ip := net.ParseIP(host); ip != nil {
		addrs = []net.IPAddr{{IP: ip}}
	} else {
		addrs, err = net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("backfill: resolve %s: no addresses", host)
	}
	for _, addr := range addrs {
		if blockedDialIP(addr.IP) {
			return nil, fmt.Errorf("backfill: refusing to dial %s: resolves to blocked address %s", host, addr.IP)
		}
	}
	dialer := net.Dialer{KeepAlive: 30 * time.Second} // deadline comes from ctx above
	var lastErr error
	for _, addr := range addrs {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(addr.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// NewDefaultHostClientBuilder returns the engine's default direct-PDS client
// builder: HTTPS-only against validated hostnames, one shared fleet
// transport, strict SSRF protection with a pinned-address guarded dialer,
// and XRPC retries disabled (the engine owns retries).
//
// Exported for consumers whose own builders must wrap the default (e.g. to
// special-case a loopback dev relay) without losing the hardening — an
// injected NewHostClient bypasses the engine's default entirely, so a
// wrapper that doesn't delegate here silently downgrades to whatever
// transport it supplies.
func NewDefaultHostClientBuilder() func(string) (*atmossync.Client, error) {
	return defaultHostClientBuilder()
}

func defaultHostClientBuilder() func(string) (*atmossync.Client, error) {
	// One transport/pool is shared by the entire fleet. The engine owns all
	// retries; the HTTP and XRPC layers each perform one attempt.
	//
	// listHosts names originate from arbitrary requestCrawl calls, so syntax
	// validation alone is not enough (a public-looking name can resolve
	// anywhere). Three layers: strict SSRF protection rejects hostnames whose
	// policy-time resolution is blocked (and jttp applies the same policy to
	// redirect hops by default); the guarded dialer re-checks and pins the
	// exact addresses it dials, closing the DNS-rebinding TOCTOU between the
	// policy lookup and the dial; and no proxy, so nothing bypasses the
	// dialer. The dialer's 5s/30s timeouts intentionally match
	// BulkDownloadOpts' dial settings, which a custom DialContext replaces.
	opts := append(xrpc.BulkDownloadOpts(),
		jttp.WithStrictSSRFProtection(),
		jttp.WithNoProxy(),
		jttp.WithDialContext(guardedDialContext),
	)
	httpClient := jttp.New(opts...)
	return func(hostname string) (*atmossync.Client, error) {
		if err := ValidateHostname(hostname); err != nil {
			return nil, err
		}
		xc := &xrpc.Client{
			Host:       "https://" + strings.ToLower(hostname),
			HTTPClient: gt.Some[*http.Client](httpClient),
			Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}
		return atmossync.NewClient(atmossync.Options{Client: xc}), nil
	}
}
