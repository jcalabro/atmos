package credential

import (
	"container/list"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/xrpc"
)

const (
	defaultMetadataBytes = 64 << 10
	defaultJWKSBytes     = 256 << 10
	defaultFetchTimeout  = 5 * time.Second
	defaultMetadataTTL   = 5 * time.Minute
	defaultCacheEntries  = 256
	defaultCacheBytes    = 2 << 20
)

var (
	errClientSigningKeyNotFound       = errors.New("credential: client authentication key not found")
	errCachedClientSigningKeyNotFound = errors.New("credential: cached client authentication key not found")
	metadataCGNATPrefix               = netip.MustParsePrefix("100.64.0.0/10")
	metadataBenchmarkPrefix           = netip.MustParsePrefix("198.18.0.0/15")
	metadataSpecialPrefixes           = [...]netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),
		netip.MustParsePrefix("192.0.0.0/24"),
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("64:ff9b::/96"),
		netip.MustParsePrefix("64:ff9b:1::/48"),
		netip.MustParsePrefix("2001:db8::/32"),
	}
)

// MetadataEndpointPolicy contains explicit exceptions intended for local tests.
// Its zero value requires public HTTPS destinations and the resolver-owned,
// DNS-rebinding-safe transport.
type MetadataEndpointPolicy struct {
	AllowHTTP            bool
	AllowPrivateNetworks bool
}

// ClientMetadataResolverOptions configures client metadata and JWKS resolution.
// Metadata and JWKS have independent response and time limits. A custom HTTP
// client is accepted only when private networks are explicitly enabled; the
// production path must retain the resolver's dial-time address validation.
type ClientMetadataResolverOptions struct {
	HTTPClient       *http.Client
	EndpointPolicy   MetadataEndpointPolicy
	MaxMetadataBytes int
	MaxJWKSBytes     int
	MetadataTimeout  time.Duration
	JWKSTimeout      time.Duration
	TTL              time.Duration
	MaxCacheEntries  int
	MaxCacheBytes    int
}

// ClientSigningKey is a copy of a strictly validated client authentication key.
type ClientSigningKey struct {
	KeyID string
	Key   *atmoscrypto.P256PublicKey
}

// VerifyResolvedClientAttestationOptions configures end-to-end attestation
// verification through validated client metadata.
type VerifyResolvedClientAttestationOptions struct {
	Resolver  *ClientMetadataResolver
	Audience  string
	Now       time.Time
	MaxAge    time.Duration
	ClockSkew time.Duration
	Replay    ReplayStore
}

type metadataKey struct {
	kid string
	x   string
	y   string
	key *atmoscrypto.P256PublicKey
}

type validatedClientMetadata struct {
	clientID string
	jwksURI  string
	keys     []metadataKey
}

type metadataCacheEntry struct {
	clientID string
	value    *validatedClientMetadata
	size     int
	expires  time.Time
}

type metadataFlight struct {
	done       chan struct{}
	result     *validatedClientMetadata
	err        error
	generation uint64
	forced     bool
	cancel     context.CancelFunc
	waiters    int
	complete   bool
}

// ClientMetadataResolver fetches, validates, coalesces, and caches client
// metadata and its inline or remote JWKS. It never serves stale data after a
// fetch error.
type ClientMetadataResolver struct {
	httpClient       *http.Client
	policy           MetadataEndpointPolicy
	maxMetadataBytes int
	maxJWKSBytes     int
	metadataTimeout  time.Duration
	jwksTimeout      time.Duration
	ttl              time.Duration
	maxCacheEntries  int
	maxCacheBytes    int

	mu          sync.Mutex
	cache       map[string]*list.Element
	lru         *list.List
	cacheBytes  int
	flights     map[string]*metadataFlight
	generations map[string]uint64
}

// NewClientMetadataResolver constructs a bounded metadata resolver. Nonzero
// limits must be positive; zero selects conservative defaults.
func NewClientMetadataResolver(opts ClientMetadataResolverOptions) (*ClientMetadataResolver, error) {
	if err := validateMetadataOptions(opts); err != nil {
		return nil, err
	}
	client := opts.HTTPClient
	if client != nil && !opts.EndpointPolicy.AllowPrivateNetworks {
		return nil, errors.New("credential: custom metadata HTTP client requires explicit private-network test policy")
	}
	if client == nil {
		client = newMetadataHTTPClient(opts.EndpointPolicy)
	}
	return &ClientMetadataResolver{
		httpClient: client, policy: opts.EndpointPolicy,
		maxMetadataBytes: positiveOr(opts.MaxMetadataBytes, defaultMetadataBytes),
		maxJWKSBytes:     positiveOr(opts.MaxJWKSBytes, defaultJWKSBytes),
		metadataTimeout:  durationOr(opts.MetadataTimeout, defaultFetchTimeout),
		jwksTimeout:      durationOr(opts.JWKSTimeout, defaultFetchTimeout),
		ttl:              durationOr(opts.TTL, defaultMetadataTTL),
		maxCacheEntries:  positiveOr(opts.MaxCacheEntries, defaultCacheEntries),
		maxCacheBytes:    positiveOr(opts.MaxCacheBytes, defaultCacheBytes),
		cache:            make(map[string]*list.Element), lru: list.New(),
		flights: make(map[string]*metadataFlight), generations: make(map[string]uint64),
	}, nil
}

func validateMetadataOptions(opts ClientMetadataResolverOptions) error {
	for name, value := range map[string]int{
		"metadata bytes": opts.MaxMetadataBytes, "JWKS bytes": opts.MaxJWKSBytes,
		"cache entries": opts.MaxCacheEntries, "cache bytes": opts.MaxCacheBytes,
	} {
		if value < 0 {
			return fmt.Errorf("credential: %s bound must not be negative", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"metadata timeout": opts.MetadataTimeout, "JWKS timeout": opts.JWKSTimeout, "cache TTL": opts.TTL,
	} {
		if value < 0 {
			return fmt.Errorf("credential: %s must not be negative", name)
		}
	}
	return nil
}

// ResolveKey returns the exact key named by kid. A cache miss is coalesced with
// concurrent callers for the same client ID.
func (r *ClientMetadataResolver) ResolveKey(ctx context.Context, clientID, kid string) (*ClientSigningKey, error) {
	return r.resolveKey(ctx, clientID, kid, false)
}

// RefreshKey bypasses and invalidates the cached document. Concurrent refreshes
// coalesce, and older in-flight fetches cannot repopulate the cache. Callers use
// this once after signature failure to accommodate key rotation.
func (r *ClientMetadataResolver) RefreshKey(ctx context.Context, clientID, kid string) (*ClientSigningKey, error) {
	return r.resolveKey(ctx, clientID, kid, true)
}

// VerifyResolvedClientAttestation parses an attestation, resolves its exact
// client_id and kid through validated metadata, and verifies all bindings. A
// signature failure triggers at most one coalesced forced refresh for rotation.
// Failed signatures are rejected before replay state is consumed.
func VerifyResolvedClientAttestation(ctx context.Context, raw string, opts VerifyResolvedClientAttestationOptions) (*VerifiedToken, error) {
	if opts.Resolver == nil {
		return nil, errors.New("credential: client metadata resolver is required")
	}
	token, err := ParseClientAttestationToken(raw)
	if err != nil {
		return nil, err
	}
	resolved, err := opts.Resolver.ResolveKey(ctx, token.Issuer, token.Header.KeyID)
	if err != nil {
		if !errors.Is(err, errCachedClientSigningKeyNotFound) {
			return nil, fmt.Errorf("credential: resolve client attestation key: %w", err)
		}
		// A rotation can publish a new kid while the old JWKS is still cached.
		// Only a cache-originated miss retries: a freshly fetched JWKS missing
		// the kid is authoritative for this attempt and must not be fetched twice.
		resolved, err = opts.Resolver.RefreshKey(ctx, token.Issuer, token.Header.KeyID)
		if err != nil {
			return nil, fmt.Errorf("credential: refresh client attestation key: %w", err)
		}
	}
	if err := verifySignature(token, resolved.Key); err != nil {
		resolved, err = opts.Resolver.RefreshKey(ctx, token.Issuer, token.Header.KeyID)
		if err != nil {
			return nil, fmt.Errorf("credential: refresh client attestation key: %w", err)
		}
		if err := verifySignature(token, resolved.Key); err != nil {
			return nil, err
		}
	}
	return VerifyClientAttestationToken(ctx, token, VerifyClientAttestationOptions{
		Key: resolved.Key, ClientID: token.Issuer, KeyID: token.Header.KeyID,
		Audience: opts.Audience, Now: opts.Now, MaxAge: opts.MaxAge,
		ClockSkew: opts.ClockSkew, Replay: opts.Replay,
	})
}

// Purge invalidates one client and fences off any older in-flight cache write.
func (r *ClientMetadataResolver) Purge(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeCacheLocked(clientID)
	r.generations[clientID]++
}

func (r *ClientMetadataResolver) resolveKey(ctx context.Context, clientID, kid string, force bool) (*ClientSigningKey, error) {
	if r == nil {
		return nil, errors.New("credential: client metadata resolver is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if kid == "" {
		return nil, errors.New("credential: client authentication kid is required")
	}
	if _, err := validateMetadataURL(clientID, r.policy); err != nil {
		return nil, fmt.Errorf("credential: invalid client_id metadata URL: %w", err)
	}

	r.mu.Lock()
	if !force {
		if value := r.cachedLocked(clientID); value != nil {
			r.mu.Unlock()
			selected, err := selectMetadataKey(value, kid)
			if errors.Is(err, errClientSigningKeyNotFound) {
				err = errors.Join(errCachedClientSigningKeyNotFound, err)
			}
			return selected, err
		}
		if current := r.flights[clientID]; current != nil && current.generation == r.generations[clientID] {
			current.waiters++
			r.mu.Unlock()
			return r.waitForMetadata(ctx, current, kid)
		}
	} else {
		if current := r.flights[clientID]; current != nil && current.forced && current.generation == r.generations[clientID] {
			current.waiters++
			r.mu.Unlock()
			return r.waitForMetadata(ctx, current, kid)
		}
		r.removeCacheLocked(clientID)
		r.generations[clientID]++
	}
	generation := r.generations[clientID]
	flightCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	flight := &metadataFlight{
		done: make(chan struct{}), generation: generation, forced: force,
		cancel: cancel, waiters: 1,
	}
	r.flights[clientID] = flight
	r.mu.Unlock()

	go r.runMetadataFlight(flightCtx, clientID, flight)
	return r.waitForMetadata(ctx, flight, kid)
}

func (r *ClientMetadataResolver) runMetadataFlight(ctx context.Context, clientID string, flight *metadataFlight) {
	result, err := r.fetchAndValidate(ctx, clientID)
	flight.cancel()
	r.mu.Lock()
	if err == nil && r.generations[clientID] == flight.generation {
		err = r.storeCacheLocked(result)
	}
	flight.result = result
	flight.err = err
	flight.complete = true
	if r.flights[clientID] == flight {
		delete(r.flights, clientID)
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *ClientMetadataResolver) waitForMetadata(ctx context.Context, flight *metadataFlight, kid string) (*ClientSigningKey, error) {
	defer r.releaseMetadataWaiter(flight)
	select {
	case <-flight.done:
		if flight.err != nil {
			return nil, flight.err
		}
		return selectMetadataKey(flight.result, kid)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *ClientMetadataResolver) releaseMetadataWaiter(flight *metadataFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if flight.waiters < 1 {
		panic("credential: corrupt metadata flight waiter count")
	}
	flight.waiters--
	if flight.waiters == 0 && !flight.complete {
		flight.cancel()
		for clientID, current := range r.flights {
			if current == flight {
				delete(r.flights, clientID)
				r.generations[clientID]++
				break
			}
		}
	}
}

func (r *ClientMetadataResolver) fetchAndValidate(ctx context.Context, clientID string) (*validatedClientMetadata, error) {
	metadataBytes, err := r.fetchJSON(ctx, clientID, r.maxMetadataBytes, r.metadataTimeout, "client metadata")
	if err != nil {
		return nil, err
	}
	var envelope struct {
		ClientID string          `json:"client_id"`
		JWKS     json.RawMessage `json:"jwks"`
		JWKSURI  json.RawMessage `json:"jwks_uri"`
	}
	if err := decodeJSONObject(metadataBytes, &envelope, false); err != nil {
		return nil, fmt.Errorf("credential: decode client metadata: %w", err)
	}
	if envelope.ClientID != clientID {
		return nil, fmt.Errorf("credential: metadata client_id %q does not exactly match %q", envelope.ClientID, clientID)
	}
	hasInline := len(envelope.JWKS) != 0
	hasURI := len(envelope.JWKSURI) != 0
	if hasInline == hasURI {
		return nil, errors.New("credential: client metadata must contain exactly one of jwks or jwks_uri")
	}

	var jwks []byte
	var jwksURI string
	if hasInline {
		if string(envelope.JWKS) == "null" {
			return nil, errors.New("credential: inline jwks must be a JSON object")
		}
		jwks = append([]byte(nil), envelope.JWKS...)
		if len(jwks) > r.maxJWKSBytes {
			return nil, fmt.Errorf("credential: inline JWKS exceeds %d-byte limit", r.maxJWKSBytes)
		}
	} else {
		if err := json.Unmarshal(envelope.JWKSURI, &jwksURI); err != nil || jwksURI == "" {
			return nil, errors.New("credential: jwks_uri must be a nonempty string")
		}
		if _, err := validateMetadataURL(jwksURI, r.policy); err != nil {
			return nil, fmt.Errorf("credential: invalid jwks_uri: %w", err)
		}
		jwks, err = r.fetchJSON(ctx, jwksURI, r.maxJWKSBytes, r.jwksTimeout, "JWKS")
		if err != nil {
			return nil, err
		}
	}
	keys, err := validateJWKS(jwks)
	if err != nil {
		return nil, err
	}
	return &validatedClientMetadata{clientID: clientID, jwksURI: jwksURI, keys: keys}, nil
}

func (r *ClientMetadataResolver) fetchJSON(parent context.Context, endpoint string, maxBytes int, timeout time.Duration, kind string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("credential: construct %s request: %w", kind, err)
	}
	req.Header.Set("Accept", "application/json, application/jwk-set+json")
	req.Header.Set("Accept-Encoding", "identity")
	client := *r.httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	client.Timeout = 0 // the independent request context is the sole wall-clock bound
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("credential: fetch %s: %w", kind, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("credential: fetch %s: HTTP status %d", kind, resp.StatusCode)
	}
	if contentType := resp.Header.Get("Content-Type"); contentType != "" && !isJSONContentType(contentType) {
		return nil, fmt.Errorf("credential: fetch %s: unexpected Content-Type %q", kind, contentType)
	}
	probeLimit := int64(maxBytes)
	if probeLimit < math.MaxInt64 {
		probeLimit++
	}
	limited := io.LimitReader(resp.Body, probeLimit)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("credential: read %s: %w", kind, err)
	}
	if len(body) > maxBytes {
		return nil, fmt.Errorf("credential: %s exceeds %d-byte limit", kind, maxBytes)
	}
	return body, nil
}

func validateJWKS(data []byte) ([]metadataKey, error) {
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := decodeJSONObject(data, &set, true); err != nil {
		return nil, fmt.Errorf("credential: decode JWKS: %w", err)
	}
	if len(set.Keys) == 0 {
		return nil, errors.New("credential: JWKS must contain at least one key")
	}
	keys := make([]metadataKey, 0, len(set.Keys))
	seen := make(map[string]struct{}, len(set.Keys))
	for i, raw := range set.Keys {
		key, err := validateMetadataJWK(raw)
		if err != nil {
			return nil, fmt.Errorf("credential: invalid JWKS key %d: %w", i, err)
		}
		if _, exists := seen[key.kid]; exists {
			return nil, fmt.Errorf("credential: duplicate JWKS kid %q", key.kid)
		}
		seen[key.kid] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
}

func validateMetadataJWK(raw []byte) (metadataKey, error) {
	if err := rejectDuplicateNames(raw); err != nil {
		return metadataKey{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return metadataKey{}, err
	}
	for _, privateName := range []string{"d", "k", "p", "q", "dp", "dq", "qi", "oth"} {
		if _, exists := fields[privateName]; exists {
			return metadataKey{}, fmt.Errorf("private JWK parameter %q is forbidden", privateName)
		}
	}
	var jwk struct {
		KTY    string   `json:"kty"`
		CRV    string   `json:"crv"`
		X      string   `json:"x"`
		Y      string   `json:"y"`
		KID    string   `json:"kid"`
		ALG    string   `json:"alg"`
		Use    string   `json:"use"`
		KeyOps []string `json:"key_ops"`
	}
	if err := json.Unmarshal(raw, &jwk); err != nil {
		return metadataKey{}, err
	}
	if jwk.KTY != "EC" || jwk.CRV != "P-256" {
		return metadataKey{}, errors.New("client authentication keys must be EC P-256")
	}
	if jwk.KID == "" {
		return metadataKey{}, errors.New("kid is required")
	}
	if _, present := fields["alg"]; present && jwk.ALG != "ES256" {
		return metadataKey{}, errors.New("alg, when present, must be ES256")
	}
	if _, present := fields["use"]; present && jwk.Use != "sig" {
		return metadataKey{}, errors.New("use, when present, must be sig")
	}
	if _, present := fields["key_ops"]; present && (len(jwk.KeyOps) != 1 || jwk.KeyOps[0] != "verify") {
		return metadataKey{}, errors.New("key_ops, when present, must contain only verify")
	}
	x, err := decodeCoordinate(jwk.X, "x")
	if err != nil {
		return metadataKey{}, err
	}
	y, err := decodeCoordinate(jwk.Y, "y")
	if err != nil {
		return metadataKey{}, err
	}
	uncompressed := make([]byte, 65)
	uncompressed[0] = 4
	copy(uncompressed[1:33], x)
	copy(uncompressed[33:], y)
	compressed := make([]byte, 33)
	compressed[0] = 2 + (y[31] & 1)
	copy(compressed[1:], x)
	key, err := atmoscrypto.ParsePublicBytesP256(compressed)
	if err != nil {
		return metadataKey{}, fmt.Errorf("invalid P-256 point: %w", err)
	}
	if got := key.UncompressedBytes(); !equalBytes(got, uncompressed) {
		return metadataKey{}, errors.New("x and y do not identify the same P-256 point")
	}
	return metadataKey{kid: jwk.KID, x: jwk.X, y: jwk.Y, key: key}, nil
}

func validateMetadataURL(raw string, policy MetadataEndpointPolicy) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if !parsed.IsAbs() || parsed.Host == "" || parsed.Opaque != "" {
		return nil, errors.New("URL must be absolute")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("URL must not contain userinfo or fragment")
	}
	if parsed.Scheme != "https" && (!policy.AllowHTTP || parsed.Scheme != "http") {
		return nil, errors.New("URL must use HTTPS")
	}
	host := parsed.Hostname()
	if host == "" || strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		if !policy.AllowPrivateNetworks {
			return nil, errors.New("localhost metadata destination is forbidden")
		}
	}
	if ip, err := netip.ParseAddr(host); err == nil && unsafeMetadataIP(ip) && !policy.AllowPrivateNetworks {
		return nil, fmt.Errorf("literal address %s is not public", ip)
	}
	return parsed, nil
}

func newMetadataHTTPClient(policy MetadataEndpointPolicy) *http.Client {
	dialer := &net.Dialer{Timeout: defaultFetchTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil, ForceAttemptHTTP2: true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, fmt.Errorf("credential: split metadata dial address: %w", err)
			}
			ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("credential: resolve metadata host: %w", err)
			}
			var lastErr error
			for _, resolved := range ips {
				ip := resolved.Unmap()
				if !policy.AllowPrivateNetworks && unsafeMetadataIP(ip) {
					lastErr = fmt.Errorf("credential: resolved metadata address %s is not public", ip)
					continue
				}
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = errors.New("credential: metadata host resolved to no addresses")
			}
			return nil, lastErr
		},
		TLSHandshakeTimeout: defaultFetchTimeout, ResponseHeaderTimeout: defaultFetchTimeout,
		ExpectContinueTimeout: time.Second, MaxResponseHeaderBytes: xrpc.MaxResponseHeaderBytes,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{Transport: transport}
}

func unsafeMetadataIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	for _, prefix := range metadataSpecialPrefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		metadataCGNATPrefix.Contains(ip) || metadataBenchmarkPrefix.Contains(ip)
}

func isJSONContentType(value string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(value, ";")[0]))
	return mediaType == "application/json" || mediaType == "application/jwk-set+json" || strings.HasSuffix(mediaType, "+json")
}

func selectMetadataKey(metadata *validatedClientMetadata, kid string) (*ClientSigningKey, error) {
	if metadata == nil {
		return nil, errors.New("credential: empty validated client metadata")
	}
	for _, candidate := range metadata.keys {
		if candidate.kid == kid {
			key, err := cloneP256(candidate.key)
			if err != nil {
				return nil, err
			}
			return &ClientSigningKey{KeyID: kid, Key: key}, nil
		}
	}
	return nil, fmt.Errorf("%w: kid %q", errClientSigningKeyNotFound, kid)
}

func cloneMetadata(in *validatedClientMetadata) *validatedClientMetadata {
	if in == nil {
		return nil
	}
	out := &validatedClientMetadata{clientID: in.clientID, jwksURI: in.jwksURI, keys: make([]metadataKey, len(in.keys))}
	for i, key := range in.keys {
		out.keys[i] = metadataKey{kid: key.kid, x: key.x, y: key.y, key: key.key}
	}
	return out
}

func cloneP256(key *atmoscrypto.P256PublicKey) (*atmoscrypto.P256PublicKey, error) {
	if key == nil {
		return nil, errors.New("credential: nil client signing key")
	}
	clone, err := atmoscrypto.ParsePublicBytesP256(key.Bytes())
	if err != nil {
		return nil, fmt.Errorf("credential: clone client signing key: %w", err)
	}
	return clone, nil
}

func (r *ClientMetadataResolver) cachedLocked(clientID string) *validatedClientMetadata {
	element := r.cache[clientID]
	if element == nil {
		return nil
	}
	entry, ok := element.Value.(*metadataCacheEntry)
	if !ok {
		panic("credential: corrupt metadata cache entry")
	}
	if !time.Now().Before(entry.expires) {
		r.removeElementLocked(element)
		return nil
	}
	r.lru.MoveToFront(element)
	return cloneMetadata(entry.value)
}

func (r *ClientMetadataResolver) storeCacheLocked(value *validatedClientMetadata) error {
	if value == nil || value.clientID == "" {
		return errors.New("credential: invalid metadata cache value")
	}
	size := metadataSize(value)
	if size > r.maxCacheBytes {
		return fmt.Errorf("credential: validated metadata exceeds %d-byte cache bound", r.maxCacheBytes)
	}
	r.removeCacheLocked(value.clientID)
	entry := &metadataCacheEntry{
		clientID: value.clientID, value: cloneMetadata(value), size: size, expires: time.Now().Add(r.ttl),
	}
	element := r.lru.PushFront(entry)
	r.cache[value.clientID] = element
	r.cacheBytes += size
	for len(r.cache) > r.maxCacheEntries || r.cacheBytes > r.maxCacheBytes {
		r.removeElementLocked(r.lru.Back())
	}
	return nil
}

func (r *ClientMetadataResolver) removeCacheLocked(clientID string) {
	if element := r.cache[clientID]; element != nil {
		r.removeElementLocked(element)
	}
}

func (r *ClientMetadataResolver) removeElementLocked(element *list.Element) {
	if element == nil {
		return
	}
	entry, ok := element.Value.(*metadataCacheEntry)
	if !ok {
		panic("credential: corrupt metadata cache element")
	}
	delete(r.cache, entry.clientID)
	r.cacheBytes -= entry.size
	r.lru.Remove(element)
}

func metadataSize(value *validatedClientMetadata) int {
	size := len(value.clientID) + len(value.jwksURI)
	for _, key := range value.keys {
		size += len(key.kid) + len(key.x) + len(key.y) + 33
	}
	return size
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for i := range left {
		different |= left[i] ^ right[i]
	}
	return different == 0
}
