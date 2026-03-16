package xrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jcalabro/gt"
)

const (
	defaultUserAgent   = "jcalabro/mono-atproto"
	maxResponseBody    = 5 << 20   // 5 MB (JSON responses)
	maxRawResponseBody = 512 << 20 // 512 MB (binary: blobs, CAR files)
)

// Client is a low-level ATProto XRPC client. Safe for concurrent use.
type Client struct {
	Host       string                  // base URL, e.g. "https://bsky.social" (required)
	HTTPClient gt.Option[*http.Client] // None = default 30s timeout (lazy init)
	UserAgent  gt.Option[string]       // None = defaultUserAgent
	Retry      gt.Option[RetryPolicy]  // None = DefaultRetryPolicy

	session    sessionState
	rl         rateLimitState
	clientOnce sync.Once
	httpClient *http.Client
}

// client returns the HTTP client, initializing it once if needed.
func (c *Client) client() *http.Client {
	c.clientOnce.Do(func() {
		if c.HTTPClient.HasVal() {
			c.httpClient = c.HTTPClient.Val()
		} else {
			c.httpClient = NewHTTPClient(30 * time.Second)
		}
	})
	return c.httpClient
}

// retryPolicy returns the retry policy, falling back to default.
func (c *Client) retryPolicy() *RetryPolicy {
	if c.Retry.HasVal() {
		r := c.Retry.Val()
		return &r
	}
	return &DefaultRetryPolicy
}

// userAgent returns the user agent string.
func (c *Client) userAgent() string {
	return c.UserAgent.ValOr(defaultUserAgent)
}

// Query executes an XRPC query (GET).
// params values may be string, int64, bool, or []string.
func (c *Client) Query(ctx context.Context, nsid string, params map[string]any, out any) error {
	return c.Do(ctx, "GET", nsid, "", params, nil, out)
}

// QueryRaw executes an XRPC query that returns raw bytes (not JSON).
// Used for endpoints that return binary data (blobs, CAR files, etc.).
func (c *Client) QueryRaw(ctx context.Context, nsid string, params map[string]any) ([]byte, error) {
	var raw rawResponse
	if err := c.doInternal(ctx, "GET", nsid, "", params, nil, &raw, ""); err != nil {
		return nil, err
	}
	return raw.data, nil
}

// rawResponse is a sentinel type recognized by doInternal to skip JSON decoding.
type rawResponse struct {
	data []byte
}

// Procedure executes an XRPC procedure (POST). in is JSON-encoded as body.
func (c *Client) Procedure(ctx context.Context, nsid string, in any, out any) error {
	if in == nil {
		return c.Do(ctx, "POST", nsid, "", nil, nil, out)
	}
	bodyBytes, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("xrpc: marshal request body: %w", err)
	}
	return c.Do(ctx, "POST", nsid, "application/json", nil, bytes.NewReader(bodyBytes), out)
}

// Do is the low-level entry point.
// method is "GET" or "POST". contentType is empty for GET.
func (c *Client) Do(ctx context.Context, method, nsid, contentType string, params map[string]any, body io.Reader, out any) error {
	return c.doInternal(ctx, method, nsid, contentType, params, body, out, "")
}

// doInternal implements the retry loop. bearerOverride, if non-empty, is used
// instead of the stored session's access JWT for the Authorization header.
func (c *Client) doInternal(ctx context.Context, method, nsid, contentType string, params map[string]any, body io.Reader, out any, bearerOverride string) error {
	policy := c.retryPolicy()
	maxAttempts := max(policy.MaxAttempts.Val(), 1)

	_, isRaw := out.(*rawResponse)

	// Pre-read body bytes for seekable retry.
	// Only *bytes.Reader (from Procedure) is seekable; arbitrary io.Readers
	// (e.g. blob uploads) get a single attempt.
	var bodyBytes []byte
	if body != nil {
		if rs, ok := body.(*bytes.Reader); ok {
			bodyBytes = make([]byte, rs.Len())
			if _, err := io.ReadFull(rs, bodyBytes); err != nil {
				return fmt.Errorf("xrpc: read request body: %w", err)
			}
		}
	}

	var lastErr error
	for attempt := range maxAttempts {
		// Proactive rate limiting: if we know quota is exhausted, wait
		// before sending the next request to avoid a 429.
		if err := c.rl.wait(ctx); err != nil {
			return err
		}

		if attempt > 0 {
			d := policy.delay(attempt - 1)

			// For 429 with RateLimit-Reset, sleep until that timestamp.
			if e, ok := errors.AsType[*Error](lastErr); ok && e.IsRateLimited() && e.RateLimit != nil && !e.RateLimit.Reset.IsZero() {
				until := time.Until(e.RateLimit.Reset)
				if until > 0 && until < policy.MaxDelay.Val() {
					d = until
				}
			}

			if err := sleep(ctx, d); err != nil {
				return err
			}
		}

		// Build URL.
		u := c.Host + "/xrpc/" + nsid
		if len(params) > 0 {
			u += "?" + encodeParams(params)
		}

		// Build request body.
		var reqBody io.Reader
		if bodyBytes != nil {
			reqBody = bytes.NewReader(bodyBytes)
		} else if body != nil && attempt == 0 {
			// Non-seekable body: only usable on first attempt.
			reqBody = body
		}

		req, err := http.NewRequestWithContext(ctx, method, u, reqBody)
		if err != nil {
			return fmt.Errorf("xrpc: build request: %w", err)
		}

		req.Header.Set("User-Agent", c.userAgent())
		if isRaw {
			req.Header.Set("Accept", "*/*")
		} else {
			req.Header.Set("Accept", "application/json")
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		// Auth: use explicit override (for refresh/delete), suppress with noAuth
		// sentinel, or fall through to stored session.
		if bearerOverride == noAuth {
			// Explicitly no auth (e.g. createSession).
		} else if bearerOverride != "" {
			req.Header.Set("Authorization", "Bearer "+bearerOverride)
		} else if auth := c.Auth(); auth != nil && auth.AccessJwt != "" {
			req.Header.Set("Authorization", "Bearer "+auth.AccessJwt)
		}

		// Execute.
		resp, err := c.client().Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Network error — retry if attempts remain.
			continue
		}

		respLimit := int64(maxResponseBody)
		if isRaw {
			respLimit = maxRawResponseBody
		}
		respBody, err := io.ReadAll(io.LimitReader(resp.Body, respLimit))
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		// Track rate limit headers on every response.
		if rl := parseRateLimit(resp.Header); rl != nil {
			c.rl.update(rl)
		}

		// Success.
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(respBody) > 0 {
				if raw, ok := out.(*rawResponse); ok {
					raw.data = respBody
				} else if err := json.Unmarshal(respBody, out); err != nil {
					return fmt.Errorf("xrpc: decode response: %w", err)
				}
			}
			return nil
		}

		// Error response.
		xErr := parseError(resp, respBody)
		lastErr = xErr

		if isRetryable(resp.StatusCode) && attempt < maxAttempts-1 {
			continue
		}
		return xErr
	}
	return lastErr
}

// QueryStream executes an XRPC query and returns the raw response body as a
// stream. The caller MUST close the returned ReadCloser.
//
// Unlike QueryRaw, the response is not buffered in memory. Retries are not
// performed because the response body is not seekable.
func (c *Client) QueryStream(ctx context.Context, nsid string, params map[string]any) (io.ReadCloser, error) {
	u := c.Host + "/xrpc/" + nsid
	if len(params) > 0 {
		u += "?" + encodeParams(params)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, fmt.Errorf("xrpc: build request: %w", err)
	}

	req.Header.Set("User-Agent", c.userAgent())
	req.Header.Set("Accept", "*/*")

	if auth := c.Auth(); auth != nil && auth.AccessJwt != "" {
		req.Header.Set("Authorization", "Bearer "+auth.AccessJwt)
	}

	resp, err := c.client().Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.Body, nil
	}

	// Error: read small body for error message, then close.
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10)) // 64KB for error JSON
	return nil, parseError(resp, body)
}

// encodeParams encodes query parameters. Values: string, int64, bool, []string.
func encodeParams(params map[string]any) string {
	vals := url.Values{}
	for k, v := range params {
		switch val := v.(type) {
		case string:
			vals.Set(k, val)
		case int64:
			vals.Set(k, strconv.FormatInt(val, 10))
		case bool:
			vals.Set(k, strconv.FormatBool(val))
		case []string:
			for _, s := range val {
				vals.Add(k, s)
			}
		}
	}
	return vals.Encode()
}
