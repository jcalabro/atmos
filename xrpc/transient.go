package xrpc

import (
	"errors"
	"io"
	"net"
	"syscall"
	"time"
)

// IsTransient reports whether err represents a transient failure that
// may succeed on retry. It recognizes xrpc.Error with retryable HTTP
// status codes (429, 500, 502, 503, 504), network timeouts, connection
// resets, and unexpected EOF (connection dropped mid-stream).
func IsTransient(err error) bool {
	if err == nil {
		return false
	}

	// Check specific sentinel errors first — syscall.Errno satisfies
	// net.Error, so these must come before the net.Error interface check.
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var xerr *Error
	if errors.As(err, &xerr) {
		return isRetryable(xerr.StatusCode)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

// RetryAfter extracts the Retry-After time from an xrpc rate limit
// error. Returns the zero time if err is not an xrpc.Error, is not
// rate-limited, or has no Retry-After information.
func RetryAfter(err error) time.Time {
	var xerr *Error
	if !errors.As(err, &xerr) {
		return time.Time{}
	}
	if xerr.RateLimit == nil || xerr.RateLimit.Reset.IsZero() {
		return time.Time{}
	}
	return xerr.RateLimit.Reset
}
