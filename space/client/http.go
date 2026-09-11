package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/bluesky-social/gttp"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/xrpc"
)

const (
	defaultJSONLimit   = int64(5 << 20)
	defaultJSONTimeout = 30 * time.Second
	defaultUserAgent   = "go/atmos spaces"
	maxReadAttempts    = 3
)

var (
	// ErrRemote identifies a non-success response from a remote XRPC service.
	ErrRemote = errors.New("space client: remote error")
	// ErrResponseTooLarge identifies a response that exceeded its configured bound.
	ErrResponseTooLarge = errors.New("space client: response too large")
	// ErrBlobCIDMismatch identifies a streamed blob whose SHA-256 multihash does not match its CID.
	ErrBlobCIDMismatch = errors.New("space client: blob CID mismatch")
	// ErrAmbiguousResult means a non-idempotent request may have reached the server.
	ErrAmbiguousResult = errors.New("space client: ambiguous non-idempotent result")
)

// RequestSigner supplies authentication headers after the exact operation and
// destination URL have been constructed and checked. Implementations must
// return a fresh DPoP proof on every call.
type RequestSigner interface {
	SignRequest(ctx context.Context, method, targetURL string) (http.Header, error)
}

// RequestSignerFunc adapts a function to [RequestSigner].
type RequestSignerFunc func(ctx context.Context, method, targetURL string) (http.Header, error)

// SignRequest implements [RequestSigner].
func (f RequestSignerFunc) SignRequest(ctx context.Context, method, targetURL string) (http.Header, error) {
	return f(ctx, method, targetURL)
}

// HTTPError is a bounded representation of an unsuccessful XRPC response.
// The response body is intentionally omitted so credentials or sensitive
// record data cannot accidentally be retained in an error.
type HTTPError struct {
	StatusCode int
	XRPCError  string
}

func (e *HTTPError) Error() string {
	if e.XRPCError == "" {
		return fmt.Sprintf("%v: HTTP %d", ErrRemote, e.StatusCode)
	}
	return fmt.Sprintf("%v: HTTP %d (%s)", ErrRemote, e.StatusCode, e.XRPCError)
}

// Unwrap makes errors.Is(err, ErrRemote) useful without exposing bodies.
func (e *HTTPError) Unwrap() error { return ErrRemote }

type engineOptions struct {
	HTTPClient      *http.Client
	Signer          RequestSigner
	JSONLimit       int64
	MaxReadAttempts int
	NetworkPolicy   NetworkPolicy
}

type engine struct {
	client          *http.Client
	signer          RequestSigner
	jsonLimit       int64
	maxReadAttempts int
}

func newEngine(opts engineOptions) (*engine, error) {
	if opts.JSONLimit < 0 {
		return nil, fmt.Errorf("space client: JSON response limit cannot be negative")
	}
	if opts.MaxReadAttempts < 0 || opts.MaxReadAttempts > maxReadAttempts {
		return nil, fmt.Errorf("space client: read attempts must be in 0..%d", maxReadAttempts)
	}
	client := opts.HTTPClient
	if client == nil {
		client = NewCorrectnessHTTPClient(NetworkPolicy{})
	}
	clientCopy, err := hardenHTTPClient(client, opts.NetworkPolicy)
	if err != nil {
		return nil, err
	}
	limit := opts.JSONLimit
	if limit == 0 {
		limit = defaultJSONLimit
	}
	attempts := opts.MaxReadAttempts
	if attempts == 0 {
		attempts = 1
	}
	return &engine{client: clientCopy, signer: opts.Signer, jsonLimit: limit, maxReadAttempts: attempts}, nil
}

func hardenHTTPClient(input *http.Client, networkPolicy NetworkPolicy) (*http.Client, error) {
	if input == nil {
		return nil, errors.New("space client: HTTP client is required")
	}
	base := input.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	transport, ok := base.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("space client: HTTP transport %T cannot be hardened for exact-send DPoP", base)
	}
	transport = transport.Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	transport.ForceAttemptHTTP2 = false
	transport.ResponseHeaderTimeout = xrpc.BulkResponseHeaderTimeout
	if transport.MaxResponseHeaderBytes <= 0 || transport.MaxResponseHeaderBytes > xrpc.MaxResponseHeaderBytes {
		transport.MaxResponseHeaderBytes = xrpc.MaxResponseHeaderBytes
	}
	hardenTransportProtocol(transport)
	hardenTransportNetwork(transport, networkPolicy)

	timeout := input.Timeout
	if timeout <= 0 || timeout > xrpc.BulkMaxRequestTimeout {
		timeout = xrpc.BulkMaxRequestTimeout
	}
	client := gttp.New(
		gttp.WithTransport(transport),
		gttp.WithNoRetries(),
		gttp.WithRedirectPolicy(0),
		gttp.WithTimeout(timeout),
		gttp.WithIdleTimeout(xrpc.BulkIdleTimeout),
		gttp.WithMinTransferRate(xrpc.BulkMinTransferBytes, xrpc.BulkMinTransferWindow),
	)
	client.CheckRedirect = rejectRedirect
	client.Jar = input.Jar
	return client, nil
}

func hardenTransportProtocol(transport *http.Transport) {
	config := transport.TLSClientConfig
	if config == nil {
		config = &tls.Config{}
	} else {
		config = config.Clone()
	}
	config.InsecureSkipVerify = false
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	config.NextProtos = []string{"http/1.1"}
	transport.TLSClientConfig = config

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport.Protocols = protocols
	// Retain the older explicit opt-out as defense in depth for standard-library
	// versions and paths that consult TLSNextProto before Protocols.
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
}

func (e *engine) json(ctx context.Context, method, target string, input, output any) error {
	return e.jsonWithRetries(ctx, method, target, input, output, method == http.MethodGet)
}

func (e *engine) jsonOnce(ctx context.Context, method, target string, input, output any) error {
	return e.jsonWithRetries(ctx, method, target, input, output, false)
}

func (e *engine) jsonWithRetries(ctx context.Context, method, target string, input, output any, retryReads bool) error {
	ctx, cancel := context.WithTimeout(ctx, defaultJSONTimeout)
	defer cancel()
	var encoded []byte
	var err error
	if input != nil {
		encoded, err = json.Marshal(input)
		if err != nil {
			return fmt.Errorf("space client: encode request: %w", err)
		}
	}
	attempts := 1
	if retryReads {
		attempts = e.maxReadAttempts
	}
	for attempt := 0; attempt < attempts; attempt++ {
		err = e.jsonAttempt(ctx, method, target, encoded, output)
		if err == nil || ctx.Err() != nil || !retryableReadError(err) {
			return err
		}
	}
	return err
}

func (e *engine) jsonAttempt(ctx context.Context, method, target string, encoded []byte, output any) error {
	var body io.Reader
	if encoded != nil {
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return fmt.Errorf("space client: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	if encoded != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if e.signer != nil {
		headers, err := e.signer.SignRequest(ctx, method, target)
		if err != nil {
			return fmt.Errorf("space client: sign request: %w", err)
		}
		for name, values := range headers {
			for _, value := range values {
				req.Header.Add(name, value)
			}
		}
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return requestFailure(method, "send request", err)
	}
	data, readErr := readBounded(resp.Body, resp.ContentLength, e.jsonLimit)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return requestFailure(method, "read response", readErr)
	}
	if closeErr != nil {
		return requestFailure(method, "close response", closeErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var wire struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &wire)
		return &HTTPError{StatusCode: resp.StatusCode, XRPCError: wire.Error}
	}
	if output != nil && len(data) != 0 {
		if _, err := cbor.FromJSON(data); err != nil {
			return requestFailure(method, "validate successful response data model", err)
		}
		if err := json.Unmarshal(data, output); err != nil {
			return requestFailure(method, "decode successful response", err)
		}
	}
	return nil
}

func requestFailure(method, operation string, err error) error {
	if method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions {
		return fmt.Errorf("%w: %s: %w", ErrAmbiguousResult, operation, err)
	}
	return fmt.Errorf("space client: %s: %w", operation, err)
}

func (e *engine) stream(ctx context.Context, target string, maxBytes int64, expectedHash *[32]byte) (io.ReadCloser, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("space client: stream limit must be positive")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("space client: build stream request: %w", err)
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", defaultUserAgent)
	if e.signer != nil {
		headers, err := e.signer.SignRequest(ctx, http.MethodGet, target)
		if err != nil {
			return nil, fmt.Errorf("space client: sign stream request: %w", err)
		}
		for name, values := range headers {
			for _, value := range values {
				req.Header.Add(name, value)
			}
		}
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("space client: send stream request: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, readErr := readBounded(resp.Body, resp.ContentLength, min(maxBytes, 64<<10))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, fmt.Errorf("space client: close error response: %w", closeErr)
		}
		var wire struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &wire)
		return nil, &HTTPError{StatusCode: resp.StatusCode, XRPCError: wire.Error}
	}
	if resp.ContentLength > maxBytes {
		_ = resp.Body.Close()
		return nil, ErrResponseTooLarge
	}
	return newBoundedStream(resp.Body, maxBytes, expectedHash), nil
}

func (e *engine) upload(ctx context.Context, target, contentType string, body io.Reader, output any) error {
	if body == nil || contentType == "" {
		return fmt.Errorf("space client: upload body and content type are required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return fmt.Errorf("space client: build upload request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", defaultUserAgent)
	if e.signer != nil {
		headers, err := e.signer.SignRequest(ctx, http.MethodPost, target)
		if err != nil {
			return fmt.Errorf("space client: sign upload request: %w", err)
		}
		for name, values := range headers {
			for _, value := range values {
				req.Header.Add(name, value)
			}
		}
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAmbiguousResult, err)
	}
	data, readErr := readBounded(resp.Body, resp.ContentLength, e.jsonLimit)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return fmt.Errorf("%w: %w", ErrAmbiguousResult, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("%w: %w", ErrAmbiguousResult, closeErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var wire struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &wire)
		return &HTTPError{StatusCode: resp.StatusCode, XRPCError: wire.Error}
	}
	if output != nil {
		if err := json.Unmarshal(data, output); err != nil {
			return fmt.Errorf("%w: decode upload response: %w", ErrAmbiguousResult, err)
		}
	}
	return nil
}

func readBounded(r io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength > limit {
		return nil, ErrResponseTooLarge
	}
	probeLimit := limit
	if probeLimit < math.MaxInt64 {
		probeLimit++
	}
	data, err := io.ReadAll(io.LimitReader(r, probeLimit))
	if err != nil {
		return nil, fmt.Errorf("space client: read response: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, ErrResponseTooLarge
	}
	if contentLength >= 0 && int64(len(data)) != contentLength {
		return nil, fmt.Errorf("space client: response length mismatch: declared %d, read %d", contentLength, len(data))
	}
	return data, nil
}

func retryableReadError(err error) bool {
	var remote *HTTPError
	if errors.As(err, &remote) {
		return remote.StatusCode == http.StatusTooManyRequests || remote.StatusCode == http.StatusInternalServerError ||
			remote.StatusCode == http.StatusBadGateway || remote.StatusCode == http.StatusServiceUnavailable ||
			remote.StatusCode == http.StatusGatewayTimeout
	}
	return !errors.Is(err, ErrResponseTooLarge)
}

func rejectRedirect(_ *http.Request, _ []*http.Request) error {
	return errors.New("space client: redirects are forbidden")
}

type boundedStream struct {
	inner        io.ReadCloser
	remaining    int64
	expectedHash *[32]byte
	hasher       hash.Hash
	done         bool
}

func newBoundedStream(inner io.ReadCloser, limit int64, expectedHash *[32]byte) *boundedStream {
	var hasher hash.Hash
	if expectedHash != nil {
		hasher = sha256.New()
	}
	return &boundedStream{inner: inner, remaining: limit, expectedHash: expectedHash, hasher: hasher}
}

func (s *boundedStream) Read(p []byte) (int, error) {
	if s.done {
		return 0, io.EOF
	}
	// Compute the one-byte oversize probe only after proving remaining fits in
	// int and cannot overflow: it is strictly smaller than len(p) in this branch.
	if s.remaining < int64(len(p)) {
		p = p[:int(s.remaining)+1]
	}
	n, err := s.inner.Read(p)
	if int64(n) > s.remaining {
		accepted := int(s.remaining)
		if s.hasher != nil && accepted > 0 {
			_, _ = s.hasher.Write(p[:accepted])
		}
		s.remaining = 0
		s.done = true
		return accepted, ErrResponseTooLarge
	}
	s.remaining -= int64(n)
	if s.hasher != nil && n > 0 {
		_, _ = s.hasher.Write(p[:n])
	}
	if err == io.EOF {
		s.done = true
		if s.expectedHash != nil {
			actual := s.hasher.Sum(nil)
			if !bytes.Equal(actual, s.expectedHash[:]) {
				return n, ErrBlobCIDMismatch
			}
		}
	}
	return n, err
}

func (s *boundedStream) Close() error { return s.inner.Close() }

func xrpcURL(endpoint *url.URL, nsid string, params url.Values) string {
	u := *endpoint
	u.Path = stringsTrimRightSlash(u.Path) + "/xrpc/" + nsid
	u.RawQuery = params.Encode()
	return u.String()
}

func stringsTrimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
