package xrpc

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/jcalabro/jttp"
)

// MaxResponseHeaderBytes caps the total size of response headers we'll read
// from a single server. ATProto responses are small and header-light;
// anything approaching this limit is a misconfigured or hostile server trying
// to exhaust client memory. Set low because we fan out to thousands of
// unknown PDSes.
const MaxResponseHeaderBytes = 64 << 10 // 64 KiB

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
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    50,
		MaxConnsPerHost:        100,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  15 * time.Second,
		ExpectContinueTimeout:  1 * time.Second,
		MaxResponseHeaderBytes: MaxResponseHeaderBytes,
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
	return jttp.New(ATProtoOpts(timeout)...)
}

// ATProtoOpts returns the canonical jttp option set used across the
// codebase for ATProto production workloads. Callers wanting to layer
// additional options (e.g. [jttp.WithStrictSSRFProtection] for code
// paths that follow attacker-controlled URLs) should pass these as
// the prefix of their own option list.
//
// These options assume we are fanning out to thousands of independently
// operated, potentially slow or adversarial PDSes (think: a web scraper at
// Google's scale). Defaults err on the side of protecting the client's
// memory, connection pool, and time budget rather than maximising per-host
// success.
//
// Use [BulkDownloadOpts] for streaming bulk endpoints (notably
// com.atproto.sync.getRepo) where a wall-clock total-request timeout
// would prematurely kill a slow-but-progressing transfer.
func ATProtoOpts(timeout time.Duration) []jttp.Option {
	return []jttp.Option{
		jttp.WithTimeout(timeout),
		jttp.WithDialTimeout(5 * time.Second),
		jttp.WithTLSHandshakeTimeout(5 * time.Second),
		jttp.WithResponseHeaderTimeout(15 * time.Second),
		jttp.WithExpectContinueTimeout(1 * time.Second),
		jttp.WithMaxIdleConns(100),
		jttp.WithMaxIdleConnsPerHost(50),
		jttp.WithMaxConnsPerHost(100),
		jttp.WithIdleConnTimeout(90 * time.Second),
		jttp.WithDialKeepAlive(30 * time.Second),
		jttp.WithMaxResponseHeaderBytes(MaxResponseHeaderBytes),
		jttp.WithRedirectPolicy(5),
		jttp.WithUserAgent(defaultUserAgent),
		jttp.WithAdditionalRetryableStatusCodes(http.StatusInternalServerError),
		jttp.WithMaxRetryAfter(30 * time.Second),
	}
}

// Bulk-download tuning constants. Exposed so callers (e.g. operators
// running a verifier against unusually slow PDSes) can reason about
// the defaults; override by constructing a custom option list.
const (
	// BulkResponseHeaderTimeout is the time-to-first-byte (TTFB) cap.
	// PDS has this long to begin responding once the request has been
	// dispatched; exceeding it kills the connection. Independent of
	// body size — a slow upstream that hasn't started streaming is
	// indistinguishable from a hung one.
	BulkResponseHeaderTimeout = 30 * time.Second

	// BulkIdleTimeout is the maximum no-progress window during the
	// body transfer. If neither end has produced bytes for this long,
	// the connection is killed. Catches "TCP keep-alive thinks it's
	// alive but the upstream stopped sending" cases.
	BulkIdleTimeout = 30 * time.Second

	// BulkMinTransferBytes is the minimum throughput floor; if the
	// rolling-window average rate drops below this for
	// BulkMinTransferWindow, the connection is killed. Tolerates brief
	// pauses but kills sustained throttling. Any healthy PDS — even a
	// modest self-hosted one — sustains far more than this; the floor
	// exists to evict hosts that trickle bytes just fast enough to
	// dodge BulkIdleTimeout while parking a worker for hours. At
	// 512 KiB/s a 1 GiB CAR completes in ~35 minutes even at the floor.
	BulkMinTransferBytes = 512 * 1024

	// BulkMinTransferWindow is the rolling-window over which
	// BulkMinTransferBytes is averaged. 60s smooths out transient
	// network blips that 10s windows would catch as failures.
	BulkMinTransferWindow = 60 * time.Second

	// BulkMaxRequestTimeout is an absolute wall-clock backstop on a
	// single getRepo (dial + TLS + headers + body). The streaming-aware
	// guards above catch hung and trickling connections; this exists
	// purely so no single download can park a worker indefinitely
	// regardless of how a host games the rate floor. At the 512 KiB/s
	// floor a 1 GiB CAR finishes in ~35 min, so 30 min only ever bites
	// pathologically large repos or sustained-floor adversaries — and
	// such a DID is simply retried on the next resumable pass.
	BulkMaxRequestTimeout = 30 * time.Minute
)

// BulkDownloadOpts returns a jttp option set tuned for streaming
// bulk responses — specifically com.atproto.sync.getRepo, which can
// take minutes for large repositories (the largest accounts on Bluesky
// today carry ~2.5M records and ~1 GiB of CAR data).
//
// Unlike [ATProtoOpts]'s short wall-clock timeout, the body transfer
// is governed primarily by three streaming-aware guards, with a
// generous absolute backstop on top:
//
//   - Time-to-first-byte cap of [BulkResponseHeaderTimeout] (30s).
//   - Idle-progress cap of [BulkIdleTimeout] (30s no bytes -> kill).
//   - Minimum sustained transfer rate of [BulkMinTransferBytes]
//     averaged over [BulkMinTransferWindow] (512 KiB/s over 60s).
//   - Absolute wall-clock backstop of [BulkMaxRequestTimeout] (30m).
//
// The first three mirror libcurl's --connect-timeout / --low-speed-limit
// / --low-speed-time triad: short transfers complete quickly, slow
// upstreams either keep up the floor or get killed, but a steady
// trickle of bytes from a faraway PDS won't be terminated just because
// the total elapsed time exceeded an arbitrary cap. The backstop is the
// last line of defense so no single download can park a worker forever.
//
// Connection pool and SSRF settings match [ATProtoOpts] — same fan-out
// posture against unknown PDSes.
func BulkDownloadOpts() []jttp.Option {
	return []jttp.Option{
		// Absolute wall-clock backstop. jttp's default is 30s, far too
		// short for a multi-hundred-MiB CAR; the streaming guards below
		// do the real work, but this bounds the pathological tail.
		jttp.WithTimeout(BulkMaxRequestTimeout),
		jttp.WithDialTimeout(5 * time.Second),
		jttp.WithTLSHandshakeTimeout(5 * time.Second),
		jttp.WithResponseHeaderTimeout(BulkResponseHeaderTimeout),
		jttp.WithExpectContinueTimeout(1 * time.Second),
		jttp.WithIdleTimeout(BulkIdleTimeout),
		jttp.WithMinTransferRate(BulkMinTransferBytes, BulkMinTransferWindow),
		jttp.WithMaxIdleConns(100),
		jttp.WithMaxIdleConnsPerHost(50),
		jttp.WithMaxConnsPerHost(100),
		jttp.WithIdleConnTimeout(90 * time.Second),
		jttp.WithDialKeepAlive(30 * time.Second),
		jttp.WithMaxResponseHeaderBytes(MaxResponseHeaderBytes),
		jttp.WithRedirectPolicy(5),
		jttp.WithUserAgent(defaultUserAgent),
		jttp.WithAdditionalRetryableStatusCodes(http.StatusInternalServerError),
		jttp.WithMaxRetryAfter(30 * time.Second),
		// Caller (the verifier worker) implements its own retry/resync
		// strategy; jttp-level retries would compound that.
		jttp.WithNoRetries(),
	}
}
