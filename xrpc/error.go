// Package xrpc implements a lexicon-agnostic ATProto XRPC HTTP client.
package xrpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ResponseTooLargeError reports that a buffered XRPC response exceeded its
// configured safety limit. ContentLength is -1 when the server did not declare
// a length and the limit was detected while reading.
type ResponseTooLargeError struct {
	Limit         int64
	ContentLength int64
}

// ResponseReadError reports that an HTTP response was received but its body
// could not be read safely. StatusCode remains authoritative; Err carries
// the truncation, transfer, or size-limit failure.
type ResponseReadError struct {
	StatusCode int
	Err        error
}

func (e *ResponseReadError) Error() string {
	return fmt.Sprintf("xrpc: read HTTP %d response body: %v", e.StatusCode, e.Err)
}

// Unwrap returns the response-body failure.
func (e *ResponseReadError) Unwrap() error { return e.Err }

// ResponseDecodeError reports that a successful HTTP response body could not
// be decoded. StatusCode remains authoritative; Err carries the decoder error.
type ResponseDecodeError struct {
	StatusCode int
	Err        error
}

func (e *ResponseDecodeError) Error() string {
	return fmt.Sprintf("xrpc: decode HTTP %d response body: %v", e.StatusCode, e.Err)
}

// Unwrap returns the response-body decoder failure.
func (e *ResponseDecodeError) Unwrap() error { return e.Err }

func (e *ResponseTooLargeError) Error() string {
	if e.ContentLength >= 0 {
		return fmt.Sprintf("xrpc: response Content-Length %d exceeds limit %d", e.ContentLength, e.Limit)
	}
	return fmt.Sprintf("xrpc: response body exceeds limit %d", e.Limit)
}

// AmbiguousResultError reports a failure after a non-idempotent request may
// have reached its server. Callers must reconcile state rather than replaying
// the operation blindly.
type AmbiguousResultError struct {
	Method string
	URL    string
	Err    error
}

func (e *AmbiguousResultError) Error() string {
	return fmt.Sprintf("xrpc: %s %s may have been applied: %v", e.Method, e.URL, e.Err)
}

// Unwrap returns the transport or response-read failure that made the result
// ambiguous.
func (e *AmbiguousResultError) Unwrap() error { return e.Err }

// Error represents an XRPC error response.
type Error struct {
	StatusCode int
	Name       string     // "error" field from JSON body
	Message    string     // "message" field from JSON body
	RateLimit  *RateLimit // non-nil if rate limit headers present

	// Host is the hostname of the server that produced this error,
	// taken from the final request URL after any redirects. For a
	// getRepo issued against a relay that 302-redirects to the
	// account's PDS, this is the PDS host, not the relay — i.e. the
	// host that actually rate-limited or failed us. Empty when the
	// error occurred before a response was received (e.g. a dial
	// failure) or when the response carried no usable request URL.
	//
	// Exposed so bulk callers (notably the backfill engine) can record
	// per-host failure attribution without string-parsing Error text.
	Host string
}

// RateLimit contains rate limit information from response headers.
type RateLimit struct {
	Limit        int
	Remaining    int
	RemainingSet bool // true only when RateLimit-Remaining was present and valid
	Reset        time.Time
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("xrpc %d %s: %s", e.StatusCode, e.Name, e.Message)
	}
	if e.Name != "" {
		return fmt.Sprintf("xrpc %d %s", e.StatusCode, e.Name)
	}
	return fmt.Sprintf("xrpc %d", e.StatusCode)
}

// IsRateLimited reports whether this error is a 429 Too Many Requests.
func (e *Error) IsRateLimited() bool {
	return e.StatusCode == http.StatusTooManyRequests
}

// errorBody is the JSON structure of XRPC error responses.
type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// parseError parses an XRPC error from an HTTP response.
// body must already be read. resp is used for status code, headers, and
// the final request URL (post-redirect) for host attribution.
func parseError(resp *http.Response, body []byte) *Error {
	xErr := &Error{StatusCode: resp.StatusCode}

	var eb errorBody
	if json.Unmarshal(body, &eb) == nil {
		xErr.Name = eb.Error
		xErr.Message = eb.Message
	}

	xErr.RateLimit = parseRateLimit(resp.Header)
	// resp.Request is the request that produced this response; after a
	// redirect chain the net/http client rewrites it to the final hop,
	// so URL.Host is the server that actually answered (the PDS, not
	// the relay that 302'd us there).
	if resp.Request != nil && resp.Request.URL != nil {
		xErr.Host = resp.Request.URL.Host
	}

	return xErr
}

// parseRateLimit extracts rate limit info from response headers.
// Returns nil if no rate limit headers are present. It recognizes both the
// atproto-style RateLimit-* headers and the standard HTTP Retry-After header
// (delta-seconds or an HTTP-date), folding the latter into Reset so the retry
// loop honors it. When both are present, RateLimit-Reset wins.
func parseRateLimit(h http.Header) *RateLimit {
	return parseRateLimitAt(h, time.Now)
}

func parseRateLimitAt(h http.Header, now func() time.Time) *RateLimit {
	limitStr := h.Get("RateLimit-Limit")
	remainStr := h.Get("RateLimit-Remaining")
	resetStr := h.Get("RateLimit-Reset")
	retryAfter := h.Get("Retry-After")

	if limitStr == "" && remainStr == "" && resetStr == "" && retryAfter == "" {
		return nil
	}

	rl := &RateLimit{}
	if limitStr != "" {
		if limit, err := strconv.Atoi(strings.TrimSpace(limitStr)); err == nil && limit >= 0 {
			rl.Limit = limit
		}
	}
	if remainStr != "" {
		if remaining, err := strconv.Atoi(strings.TrimSpace(remainStr)); err == nil && remaining >= 0 {
			rl.Remaining = remaining
			rl.RemainingSet = true
		}
	}
	if resetStr != "" {
		if unix, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
			rl.Reset = time.Unix(unix, 0)
		}
	}
	// Fall back to the standard Retry-After header when RateLimit-Reset is
	// absent. Retry-After is either an integer number of seconds or an
	// IMF-fixdate (RFC 7231 §7.1.3).
	if rl.Reset.IsZero() && retryAfter != "" {
		const maxDurationSeconds = int64(^uint64(0)>>1) / int64(time.Second)
		if secs, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && secs >= 0 && secs <= maxDurationSeconds {
			rl.Reset = now().Add(time.Duration(secs) * time.Second)
		} else if t, err := http.ParseTime(retryAfter); err == nil {
			rl.Reset = t
		}
	}
	return rl
}

// isRetryable reports whether an HTTP status code should be retried.
func isRetryable(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}
