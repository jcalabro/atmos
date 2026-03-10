// Package xrpc implements a lexicon-agnostic ATProto XRPC HTTP client.
package xrpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Error represents an XRPC error response.
type Error struct {
	StatusCode int
	Name       string     // "error" field from JSON body
	Message    string     // "message" field from JSON body
	RateLimit  *RateLimit // non-nil if rate limit headers present
}

// RateLimit contains rate limit information from response headers.
type RateLimit struct {
	Limit     int
	Remaining int
	Reset     time.Time
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
// body must already be read. resp is used for status code and headers.
func parseError(resp *http.Response, body []byte) *Error {
	xErr := &Error{StatusCode: resp.StatusCode}

	var eb errorBody
	if json.Unmarshal(body, &eb) == nil {
		xErr.Name = eb.Error
		xErr.Message = eb.Message
	}

	xErr.RateLimit = parseRateLimit(resp.Header)
	return xErr
}

// parseRateLimit extracts rate limit info from response headers.
// Returns nil if no rate limit headers are present.
func parseRateLimit(h http.Header) *RateLimit {
	limitStr := h.Get("RateLimit-Limit")
	remainStr := h.Get("RateLimit-Remaining")
	resetStr := h.Get("RateLimit-Reset")

	if limitStr == "" && remainStr == "" && resetStr == "" {
		return nil
	}

	rl := &RateLimit{}
	if limitStr != "" {
		rl.Limit, _ = strconv.Atoi(limitStr)
	}
	if remainStr != "" {
		rl.Remaining, _ = strconv.Atoi(remainStr)
	}
	if resetStr != "" {
		if unix, err := strconv.ParseInt(resetStr, 10, 64); err == nil {
			rl.Reset = time.Unix(unix, 0)
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
