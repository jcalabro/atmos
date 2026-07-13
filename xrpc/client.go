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
	"syscall"
	"time"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

const (
	defaultUserAgent   = "go/atmos"
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
// Uses jttp with retries disabled because Client.doInternal implements its
// own XRPC-aware retry loop with rate-limit tracking and session refresh.
func (c *Client) client() *http.Client {
	c.clientOnce.Do(func() {
		if c.HTTPClient.HasVal() {
			c.httpClient = c.HTTPClient.Val()
		} else {
			c.httpClient = jttp.New(append(ATProtoOpts(30*time.Second), jttp.WithNoRetries())...)
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

// readResponseBody reads a response body into a single right-sized buffer.
// contentLength is the response's declared Content-Length (-1 when unknown); the
// caller owns closing the body.
//
// When the server advertises a usable Content-Length (0 <= n <= limit), the
// buffer is allocated exactly once and filled with io.ReadFull, avoiding the
// geometric grow-and-recopy of io.ReadAll — which, on a ~280 MB sealed segment,
// spends ~half its CPU in memclr (zeroing each doubled buffer) + memmove
// (copying the old contents forward) and churns ~5x the body size in throwaway
// allocations. Pre-sizing makes the single-goroutine prefetcher that streams the
// archive substantially faster (it is a serial stage that gates the decode pool).
//
// The advertised length is treated as a contract, not a hint: a body that ends
// short of (io.ErrUnexpectedEOF) or runs past (one extra byte reads) the declared
// Content-Length is a truncated/over-long transfer and is returned as a hard
// error rather than a silently short or silently capped buffer — a wrong segment
// is worse than a failed fetch (it would feed corrupt frames to decode).
//
// When Content-Length is absent or negative (chunked / unknown), it falls back
// to the bounded io.ReadAll path unchanged, so non-segment endpoints and servers
// that don't set the header keep working exactly as before.
func readResponseBody(body io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength < 0 || contentLength > limit {
		// Unknown length, or a declared length over our safety cap: read bounded.
		// (contentLength > limit is surfaced as the same truncation the LimitReader
		// would produce; the caller's limit is the zstd-bomb / oversized-body guard.)
		return io.ReadAll(io.LimitReader(body, limit))
	}
	buf := make([]byte, contentLength)
	if _, err := io.ReadFull(body, buf); err != nil {
		// ErrUnexpectedEOF here means the body ended before Content-Length bytes:
		// a truncated transfer. Fail loud rather than return a short buffer.
		return nil, fmt.Errorf("xrpc: read response body (Content-Length %d): %w", contentLength, err)
	}
	// Verify the body does not exceed the declared length. A single extra byte
	// read succeeding means the server sent more than it advertised — refuse it
	// rather than silently truncating to Content-Length.
	var extra [1]byte
	if m, _ := io.ReadFull(body, extra[:]); m > 0 {
		return nil, fmt.Errorf("xrpc: response body exceeds Content-Length %d", contentLength)
	}
	return buf, nil
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

	// Idempotent methods (GET/HEAD/PUT/DELETE) are safe to retry on any
	// transient failure. A POST procedure is NOT idempotent: a 5xx or a network
	// error after the request was sent may mean the server already applied the
	// write (e.g. createRecord), so a blind retry risks a duplicate. For POST we
	// therefore only retry on failures that guarantee the request never reached
	// the server (connection refused) or that the server rejects before
	// processing (429 rate limit).
	idempotent := method != http.MethodPost

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

	// Proactive rate limiting is keyed by host. At request time only the
	// host we dial is known (c.Host); a redirecting front door (relay ->
	// PDS) resolves to the responding host only after the fact, so the
	// wait below catches direct-to-host clients while redirected requests
	// rely on the reactive 429 handling in the retry loop.
	reqHost := hostOfURL(c.Host)

	var lastErr error
	for attempt := range maxAttempts {
		// Proactive rate limiting: if we know this host's quota is
		// exhausted, wait before sending the next request to avoid a 429.
		if err := c.rl.wait(ctx, reqHost); err != nil {
			return err
		}

		if attempt > 0 {
			d := policy.delay(attempt - 1)

			// Honor a server-supplied retry time (RateLimit-Reset or the
			// standard Retry-After header, both folded into RateLimit.Reset) for
			// any retryable status — 429 throttles and 503 backpressure alike —
			// in preference to the fixed exponential backoff, clamped to MaxDelay.
			if e, ok := errors.AsType[*Error](lastErr); ok && e.RateLimit != nil && !e.RateLimit.Reset.IsZero() {
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
			enc, err := encodeParams(params)
			if err != nil {
				return err
			}
			u += "?" + enc
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
			// Network error — retry idempotent methods freely. For a
			// non-idempotent POST, retry only when the connection was refused
			// (the request provably never reached the server); any other
			// transport error is ambiguous and could have committed the write.
			if idempotent || errors.Is(err, syscall.ECONNREFUSED) {
				continue
			}
			return lastErr
		}

		respLimit := int64(maxResponseBody)
		if isRaw {
			respLimit = maxRawResponseBody
		}
		respBody, err := readResponseBody(resp.Body, resp.ContentLength, respLimit)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		// Track rate limit headers on every response, attributed to the
		// host that actually answered (post-redirect), which for a
		// relay-fronted request is the PDS rather than the relay.
		if rl := parseRateLimit(resp.Header); rl != nil {
			c.rl.update(respHost(resp), rl)
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

		// For a non-idempotent POST, a 5xx may mean the server already applied
		// the write; only a 429 (rejected before processing) is safe to retry.
		retryStatus := isRetryable(resp.StatusCode)
		if !idempotent && resp.StatusCode != http.StatusTooManyRequests {
			retryStatus = false
		}
		if retryStatus && attempt < maxAttempts-1 {
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
	body, _, err := c.QueryStreamHost(ctx, nsid, params)
	return body, err
}

// QueryStreamHost is [QueryStream] that also reports the host that
// served the response — the final request URL's host after any
// redirects. For a request against a relay that 302-redirects to a PDS,
// host is the PDS, not the relay.
//
// On the success path host comes from the response's (post-redirect)
// request URL. On the error path the host is also carried on the
// returned [*Error] (Error.Host), so callers that only inspect the
// error still get attribution; the explicit return exists for the
// success path, where there is no error to carry it.
//
// host may be empty if the request failed before any response was
// received (e.g. a dial error); in that case err is the transport
// error, which is not an [*Error] and carries no host.
func (c *Client) QueryStreamHost(ctx context.Context, nsid string, params map[string]any) (body io.ReadCloser, host string, err error) {
	u := c.Host + "/xrpc/" + nsid
	if len(params) > 0 {
		enc, err := encodeParams(params)
		if err != nil {
			return nil, "", err
		}
		u += "?" + enc
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, "", fmt.Errorf("xrpc: build request: %w", err)
	}

	req.Header.Set("User-Agent", c.userAgent())
	req.Header.Set("Accept", "*/*")

	if auth := c.Auth(); auth != nil && auth.AccessJwt != "" {
		req.Header.Set("Authorization", "Bearer "+auth.AccessJwt)
	}

	resp, err := c.client().Do(req)
	if err != nil {
		return nil, "", err
	}

	// resp.Request is the final request after redirects, so its
	// URL.Host is the server that actually answered.
	if resp.Request != nil && resp.Request.URL != nil {
		host = resp.Request.URL.Host
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.Body, host, nil
	}

	// Error: read small body for error message, then close.
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10)) // 64KB for error JSON
	return nil, host, parseError(resp, respBody)
}

// hostOfURL extracts the host (host:port) from a base URL string, or ""
// if the URL does not parse. Used to key proactive rate-limit state.
func hostOfURL(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return u.Host
}

// respHost returns the host that served resp: the final request URL's host
// after any redirects (the PDS, not the relay that 302'd us there), or ""
// when the response carries no usable request URL.
func respHost(resp *http.Response) string {
	if resp.Request != nil && resp.Request.URL != nil {
		return resp.Request.URL.Host
	}
	return ""
}

// encodeParams encodes query parameters. Values: string, int, int64, bool,
// []string. An unsupported value type is an error rather than being silently
// dropped (which would send a request missing a parameter the caller intended).
func encodeParams(params map[string]any) (string, error) {
	vals := url.Values{}
	for k, v := range params {
		switch val := v.(type) {
		case string:
			vals.Set(k, val)
		case int:
			vals.Set(k, strconv.Itoa(val))
		case int64:
			vals.Set(k, strconv.FormatInt(val, 10))
		case bool:
			vals.Set(k, strconv.FormatBool(val))
		case []string:
			for _, s := range val {
				vals.Add(k, s)
			}
		default:
			return "", fmt.Errorf("xrpc: unsupported query param type for %q: %T", k, v)
		}
	}
	return vals.Encode(), nil
}
