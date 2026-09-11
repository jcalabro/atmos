package notificationauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/serviceauth"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/jcalabro/gt"
)

const (
	// NotifyWriteMethod is the method binding for both notifyWrite hops.
	NotifyWriteMethod atmos.NSID = "com.atproto.space.notifyWrite"
	// NotifySpaceDeletedMethod is the method binding for deletion notifications.
	NotifySpaceDeletedMethod atmos.NSID = "com.atproto.space.notifySpaceDeleted"

	// DefaultMaxAge is both the default and largest accepted notification JWT
	// age. A caller may select a smaller window.
	DefaultMaxAge = 5 * time.Minute
	// DefaultClockSkew is both the default and largest accepted clock skew.
	DefaultClockSkew = 5 * time.Second

	maxTokenBytes = 16 << 10
	maxJTIBytes   = 256
)

// ReplayStore atomically consumes service-auth JWT identifiers. Its method
// shape intentionally matches credential.ReplayStore so applications can use
// the same durable implementation without an adapter.
type ReplayStore interface {
	Consume(ctx context.Context, namespace credential.ReplayNamespace, id string, expiresAt time.Time) error
}

// Options contains verification dependencies and lifetime policy shared by
// every notification profile. Identity and Replay are mandatory.
type Options struct {
	// Resolver returns raw DID documents. Verification deliberately does not
	// accept identity.Directory because its normalized maps cannot preserve
	// duplicate fragments, full verification-method IDs, or controllers.
	Resolver identity.Resolver
	Replay   ReplayStore

	// MaxAge defaults to DefaultMaxAge and cannot exceed it.
	MaxAge time.Duration
	// ClockSkew defaults to DefaultClockSkew and cannot exceed it. The zero
	// value requests the default; negative values are invalid.
	ClockSkew time.Duration
}

// WriterOptions configures verification of the writer-to-authority hop.
type WriterOptions struct {
	Options

	// Authority is the bare DID of the authority service receiving the call.
	// It must also be the authority named by the payload's Space.
	Authority atmos.DID
}

// SubscriberOptions configures verification of an authority-to-subscriber
// notification hop.
type SubscriberOptions struct {
	Options

	// Subscriber is the configured receiving service identifier: a DID with
	// an optional verification/service fragment. It is matched byte-for-byte
	// against the JWT audience.
	Subscriber string
}

// Claims is returned only after cryptographic, role, payload, lifetime, and
// replay verification all succeed.
type Claims struct {
	Issuer    atmos.DID
	Audience  string
	LexMethod atmos.NSID
	IssuedAt  time.Time
	ExpiresAt time.Time
	JTI       string
	Space     atmos.SpaceRef
	Repo      atmos.DID
}

// CreateWriterNotifyWriteToken creates a writer-to-authority notifyWrite JWT.
func CreateWriterNotifyWriteToken(repo, authority atmos.DID, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	return CreateWriterNotifyWriteTokenAt(repo, authority, time.Now(), expiresAt, key)
}

// CreateWriterNotifyWriteTokenAt creates a writer-to-authority notifyWrite JWT
// using the explicit issuance time.
func CreateWriterNotifyWriteTokenAt(repo, authority atmos.DID, issuedAt, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	if err := validateBareDID("repo", repo); err != nil {
		return "", err
	}
	if err := validateBareDID("authority", authority); err != nil {
		return "", err
	}
	return createToken(repo, string(authority), NotifyWriteMethod, issuedAt, expiresAt, key)
}

// CreateAuthorityNotifyWriteToken creates an authority-to-subscriber
// notifyWrite JWT.
func CreateAuthorityNotifyWriteToken(authority atmos.DID, subscriber string, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	return CreateAuthorityNotifyWriteTokenAt(authority, subscriber, time.Now(), expiresAt, key)
}

// CreateAuthorityNotifyWriteTokenAt creates an authority-to-subscriber
// notifyWrite JWT using the explicit issuance time.
func CreateAuthorityNotifyWriteTokenAt(authority atmos.DID, subscriber string, issuedAt, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	if err := validateBareDID("authority", authority); err != nil {
		return "", err
	}
	if err := validateServiceIdentifier(subscriber); err != nil {
		return "", fmt.Errorf("notificationauth: subscriber: %w", err)
	}
	return createToken(authority, subscriber, NotifyWriteMethod, issuedAt, expiresAt, key)
}

// CreateAuthorityNotifySpaceDeletedToken creates an authority-to-subscriber
// notifySpaceDeleted JWT.
func CreateAuthorityNotifySpaceDeletedToken(authority atmos.DID, subscriber string, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	return CreateAuthorityNotifySpaceDeletedTokenAt(authority, subscriber, time.Now(), expiresAt, key)
}

// CreateAuthorityNotifySpaceDeletedTokenAt creates an authority-to-subscriber
// notifySpaceDeleted JWT using the explicit issuance time.
func CreateAuthorityNotifySpaceDeletedTokenAt(authority atmos.DID, subscriber string, issuedAt, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	if err := validateBareDID("authority", authority); err != nil {
		return "", err
	}
	if err := validateServiceIdentifier(subscriber); err != nil {
		return "", fmt.Errorf("notificationauth: subscriber: %w", err)
	}
	return createToken(authority, subscriber, NotifySpaceDeletedMethod, issuedAt, expiresAt, key)
}

func createToken(issuer atmos.DID, audience string, method atmos.NSID, issuedAt, expiresAt time.Time, key crypto.PrivateKey) (string, error) {
	if issuedAt.IsZero() {
		return "", errors.New("notificationauth: issuance time is required")
	}
	if !expiresAt.After(issuedAt) {
		return "", errors.New("notificationauth: expiration must be in the future")
	}
	if expiresAt.After(issuedAt.Add(DefaultMaxAge)) {
		return "", fmt.Errorf("notificationauth: expiration exceeds maximum lifetime of %s", DefaultMaxAge)
	}
	return serviceauth.CreateToken(serviceauth.TokenParams{
		Issuer: issuer, Audience: audience, Exp: expiresAt, IssuedAt: issuedAt, LexMethod: method,
	}, key)
}

// VerifyWriterNotifyWrite verifies a writer-to-authority notifyWrite JWT. It
// requires issuer == repo, audience == hosted authority, exact lxm, and the
// payload space's authority == hosted authority.
func VerifyWriterNotifyWrite(ctx context.Context, token string, space atmos.SpaceRef, repo atmos.DID, opts WriterOptions) (*Claims, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	if err := validateBareDID("repo", repo); err != nil {
		return nil, err
	}
	if err := validateBareDID("authority", opts.Authority); err != nil {
		return nil, err
	}
	if space.Authority() != opts.Authority {
		return nil, fmt.Errorf("notificationauth: payload space authority %q does not match hosted authority %q", space.Authority(), opts.Authority)
	}

	claims, maxAge, skew, err := verify(ctx, token, string(opts.Authority), NotifyWriteMethod, opts.Options)
	if err != nil {
		return nil, err
	}
	if claims.Issuer != repo {
		return nil, fmt.Errorf("notificationauth: issuer %q does not match payload repo %q", claims.Issuer, repo)
	}
	return consume(ctx, claims, space, repo, opts.Replay, maxAge, skew)
}

// VerifySubscriberNotifyWrite verifies an authority-to-subscriber notifyWrite
// JWT. The issuer must be the authority named by space; repo is validated but
// is not the issuer on this hop.
func VerifySubscriberNotifyWrite(ctx context.Context, token string, space atmos.SpaceRef, repo atmos.DID, opts SubscriberOptions) (*Claims, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	if err := validateBareDID("repo", repo); err != nil {
		return nil, err
	}
	if err := validateServiceIdentifier(opts.Subscriber); err != nil {
		return nil, fmt.Errorf("notificationauth: subscriber: %w", err)
	}

	claims, maxAge, skew, err := verify(ctx, token, opts.Subscriber, NotifyWriteMethod, opts.Options)
	if err != nil {
		return nil, err
	}
	if claims.Issuer != space.Authority() {
		return nil, fmt.Errorf("notificationauth: issuer %q does not match payload space authority %q", claims.Issuer, space.Authority())
	}
	return consume(ctx, claims, space, repo, opts.Replay, maxAge, skew)
}

// VerifySubscriberNotifySpaceDeleted verifies an authority-to-subscriber
// notifySpaceDeleted JWT. Successful verification authorizes deletion only of
// the exact validated space returned in Claims.
func VerifySubscriberNotifySpaceDeleted(ctx context.Context, token string, space atmos.SpaceRef, opts SubscriberOptions) (*Claims, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	if err := validateServiceIdentifier(opts.Subscriber); err != nil {
		return nil, fmt.Errorf("notificationauth: subscriber: %w", err)
	}

	claims, maxAge, skew, err := verify(ctx, token, opts.Subscriber, NotifySpaceDeletedMethod, opts.Options)
	if err != nil {
		return nil, err
	}
	if claims.Issuer != space.Authority() {
		return nil, fmt.Errorf("notificationauth: issuer %q does not match payload space authority %q", claims.Issuer, space.Authority())
	}
	return consume(ctx, claims, space, "", opts.Replay, maxAge, skew)
}

func verify(ctx context.Context, token, audience string, method atmos.NSID, opts Options) (*serviceauth.TokenClaims, time.Duration, time.Duration, error) {
	maxAge, skew, err := validateOptions(opts)
	if err != nil {
		return nil, 0, 0, err
	}
	algorithm, err := validateTokenEnvelope(token, audience)
	if err != nil {
		return nil, 0, 0, err
	}
	directory := &identity.Directory{
		Resolver:               strictSigningResolver{resolver: opts.Resolver, algorithm: algorithm},
		SkipHandleVerification: true,
	}
	claims, err := serviceauth.VerifyToken(ctx, token, serviceauth.VerifyOptions{
		Audience:  audience,
		Identity:  directory,
		MaxAge:    gt.Some(maxAge),
		Leeway:    gt.Some(skew),
		LexMethod: method,
	})
	if err != nil {
		return nil, 0, 0, fmt.Errorf("notificationauth: verify service JWT: %w", err)
	}
	// All notification roles use bare DIDs. A fragment selects another
	// verification method and is not interchangeable with the payload actor.
	if err := validateBareDID("issuer", claims.Issuer); err != nil {
		return nil, 0, 0, err
	}
	if claims.Audience != audience {
		return nil, 0, 0, fmt.Errorf("notificationauth: audience %q does not exactly match %q", claims.Audience, audience)
	}
	return claims, maxAge, skew, nil
}

func consume(ctx context.Context, verified *serviceauth.TokenClaims, space atmos.SpaceRef, repo atmos.DID, replay ReplayStore, maxAge, skew time.Duration) (*Claims, error) {
	// Retain through the complete acceptance window required by serviceauth's
	// replay contract, not merely until exp. Consume is deliberately last.
	retainUntil := verified.ExpiresAt.Add(maxAge + skew)
	if err := replay.Consume(ctx, credential.ReplayServiceAuth, verified.JTI, retainUntil); err != nil {
		return nil, fmt.Errorf("notificationauth: consume service JWT replay identifier: %w", err)
	}
	return &Claims{
		Issuer: verified.Issuer, Audience: verified.Audience,
		LexMethod: verified.LexMethod, IssuedAt: verified.IssuedAt,
		ExpiresAt: verified.ExpiresAt, JTI: verified.JTI,
		Space: space, Repo: repo,
	}, nil
}

func validateOptions(opts Options) (time.Duration, time.Duration, error) {
	if opts.Resolver == nil {
		return 0, 0, errors.New("notificationauth: identity resolver is required")
	}
	if opts.Replay == nil {
		return 0, 0, errors.New("notificationauth: replay store is required")
	}
	maxAge := opts.MaxAge
	if maxAge == 0 {
		maxAge = DefaultMaxAge
	}
	if maxAge <= 0 || maxAge > DefaultMaxAge {
		return 0, 0, fmt.Errorf("notificationauth: max age must be in (0, %s]", DefaultMaxAge)
	}
	skew := opts.ClockSkew
	if skew == 0 {
		skew = DefaultClockSkew
	}
	if skew < 0 || skew > DefaultClockSkew {
		return 0, 0, fmt.Errorf("notificationauth: clock skew must be in [0, %s]", DefaultClockSkew)
	}
	return maxAge, skew, nil
}

// strictSigningResolver checks the complete raw document and returns only the
// uniquely selected, correctly controlled #atproto Multikey. serviceauth then
// performs its existing algorithm allowlist and signature verification over
// this security-preserving projection.
type strictSigningResolver struct {
	resolver  identity.Resolver
	algorithm string
}

func (r strictSigningResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc, err := r.resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, err
	}
	method, key, err := identity.SelectVerificationMethod(doc, did, "atproto")
	if err != nil {
		return nil, fmt.Errorf("notificationauth: select strict atproto signing key: %w", err)
	}
	expectedAlgorithm := "ES256K"
	if _, ok := key.(*crypto.P256PublicKey); ok {
		expectedAlgorithm = "ES256"
	}
	if r.algorithm != expectedAlgorithm {
		return nil, fmt.Errorf("notificationauth: JWT algorithm %q does not match selected signing key algorithm %q", r.algorithm, expectedAlgorithm)
	}
	return &identity.DIDDocument{
		ID:                 string(did),
		VerificationMethod: []identity.VerificationMethod{*method},
	}, nil
}

func (r strictSigningResolver) ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	return r.resolver.ResolveHandle(ctx, handle)
}

func validateSpace(space atmos.SpaceRef) error {
	if err := space.Validate(); err != nil {
		return fmt.Errorf("notificationauth: invalid payload space: %w", err)
	}
	return nil
}

func validateBareDID(name string, did atmos.DID) error {
	if strings.Contains(string(did), "#") {
		return fmt.Errorf("notificationauth: %s must be a bare DID", name)
	}
	if err := did.Validate(); err != nil {
		return fmt.Errorf("notificationauth: invalid %s: %w", name, err)
	}
	return nil
}

func validateServiceIdentifier(identifier string) error {
	did, fragment, hasFragment := strings.Cut(identifier, "#")
	if hasFragment && (fragment == "" || strings.Contains(fragment, "#")) {
		return errors.New("invalid service fragment")
	}
	if _, err := atmos.ParseDID(did); err != nil {
		return fmt.Errorf("invalid service DID: %w", err)
	}
	return nil
}

func validateTokenEnvelope(token, expectedAudience string) (string, error) {
	if len(token) == 0 || len(token) > maxTokenBytes {
		return "", fmt.Errorf("notificationauth: service JWT must contain 1..%d bytes", maxTokenBytes)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("notificationauth: malformed service JWT")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("notificationauth: decode service JWT header: %w", err)
	}
	headerFields, err := decodeUniqueClaims(header)
	if err != nil {
		return "", fmt.Errorf("notificationauth: decode service JWT header: %w", err)
	}
	var algorithm string
	if err := json.Unmarshal(headerFields["alg"], &algorithm); err != nil {
		return "", errors.New("notificationauth: JWT alg must be a string")
	}
	if algorithm != "ES256" && algorithm != "ES256K" {
		return "", fmt.Errorf("notificationauth: unsupported JWT algorithm %q", algorithm)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("notificationauth: decode service JWT payload: %w", err)
	}
	claims, err := decodeUniqueClaims(payload)
	if err != nil {
		return "", fmt.Errorf("notificationauth: decode service JWT claims: %w", err)
	}
	var jti string
	if err := json.Unmarshal(claims["jti"], &jti); err != nil || jti == "" || len(jti) > maxJTIBytes {
		return "", fmt.Errorf("notificationauth: service JWT jti must contain 1..%d bytes", maxJTIBytes)
	}
	audience := claims["aud"]
	if len(audience) == 0 {
		return "", errors.New("notificationauth: missing audience")
	}
	var single string
	if err := json.Unmarshal(audience, &single); err == nil {
		if single != expectedAudience {
			return "", fmt.Errorf("notificationauth: audience %q does not exactly match %q", single, expectedAudience)
		}
		return algorithm, nil
	}
	var list []string
	if err := json.Unmarshal(audience, &list); err != nil {
		return "", errors.New("notificationauth: audience must be a string or one-element string array")
	}
	if len(list) != 1 || list[0] != expectedAudience {
		return "", fmt.Errorf("notificationauth: audience must contain exactly %q", expectedAudience)
	}
	return algorithm, nil
}

func decodeUniqueClaims(payload []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := start.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("JWT claims must be an object")
	}
	claims := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("JWT claim name must be a string")
		}
		if _, duplicate := claims[name]; duplicate {
			return nil, fmt.Errorf("duplicate JWT claim %q", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		claims[name] = value
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != '}' {
		return nil, errors.New("unterminated JWT claims object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after JWT claims object")
	}
	return claims, nil
}
