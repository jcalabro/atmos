package credential

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/jcalabro/atmos"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
)

const (
	// DelegationTokenType is the media type of a space delegation JWT.
	DelegationTokenType = "atproto-space-delegation+jwt"
	// ClientAttestationTokenType is the media type of a client attestation JWT.
	ClientAttestationTokenType = "atproto-client-attestation+jwt"
	// SpaceCredentialTokenType is the media type of a reusable space credential JWT.
	SpaceCredentialTokenType = "atproto-space-credential+jwt"

	defaultSingleUseLifetime  = 60 * time.Second
	defaultCredentialLifetime = 2 * time.Hour
	defaultClockSkew          = 5 * time.Second
	maxTokenBytes             = 16 << 10
)

const (
	// MaxSingleUseTokenLifetime is the issuance and verification ceiling for
	// delegations and attestations.
	MaxSingleUseTokenLifetime = defaultSingleUseLifetime
	// MaxSpaceCredentialLifetime is the issuance and verification ceiling for
	// reusable credentials.
	MaxSpaceCredentialLifetime = defaultCredentialLifetime
	// MaxClockSkew is the protocol's maximum accepted clock skew.
	MaxClockSkew = defaultClockSkew
)

// TokenProfile identifies one of the three non-interchangeable space JWT profiles.
type TokenProfile uint8

const (
	// DelegationProfile identifies account-to-authority delegation tokens.
	DelegationProfile TokenProfile = iota + 1
	// ClientAttestationProfile identifies application attestations.
	ClientAttestationProfile
	// SpaceCredentialProfile identifies reusable space credentials.
	SpaceCredentialProfile
)

// TokenHeader is a structurally validated JWT protected header.
type TokenHeader struct {
	Algorithm string
	Type      string
	KeyID     string
}

// UnverifiedToken is a structurally validated but cryptographically unverified
// token. Its exported claim values remain attacker controlled.
type UnverifiedToken struct {
	Profile         TokenProfile
	Header          TokenHeader
	Issuer          string
	Subject         string
	Audience        string
	IssuedAt        time.Time
	ExpiresAt       time.Time
	JTI             string
	ConfirmationJKT string

	raw          string
	signingInput []byte
	signature    []byte
}

// Raw returns the original compact JWT. It can contain credentials and should
// not be logged.
func (t *UnverifiedToken) Raw() string {
	if t == nil {
		return ""
	}
	return t.raw
}

// VerifiedToken is a signature-, binding-, and time-verified space token.
// Single-use profiles are returned only after replay consumption succeeds.
type VerifiedToken struct {
	Profile         TokenProfile
	Header          TokenHeader
	Issuer          string
	Subject         string
	Audience        string
	IssuedAt        time.Time
	ExpiresAt       time.Time
	JTI             string
	ConfirmationJKT string
}

// DelegationTokenParams configures creation of a delegation token.
type DelegationTokenParams struct {
	Issuer   atmos.DID
	Subject  atmos.SpaceRef
	Audience string
	Now      time.Time
	Lifetime time.Duration
}

// ClientAttestationTokenParams configures creation of an attestation token.
type ClientAttestationTokenParams struct {
	ClientID string
	Audience string
	KeyID    string
	Now      time.Time
	Lifetime time.Duration
}

// SpaceCredentialTokenParams configures creation of a space credential.
type SpaceCredentialTokenParams struct {
	Issuer         atmos.DID
	Subject        atmos.SpaceRef
	DPoPThumbprint string
	KeyID          string
	Now            time.Time
	Lifetime       time.Duration
}

type wireHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid,omitempty"`
}

type wireClaims struct {
	Issuer       string            `json:"iss"`
	Subject      string            `json:"sub"`
	Audience     string            `json:"aud,omitempty"`
	IssuedAt     int64             `json:"iat"`
	ExpiresAt    int64             `json:"exp"`
	JTI          string            `json:"jti"`
	Confirmation *wireConfirmation `json:"cnf,omitempty"`
}

type wireConfirmation struct {
	JKT string `json:"jkt"`
}

// SpaceHostAudience returns the JWT audience identifier for a space authority.
// It is an identifier, not an HTTP endpoint.
func SpaceHostAudience(authority atmos.DID) string {
	return authority.String() + "#atproto_space_host"
}

// CreateDelegationToken creates a short-lived, single-use delegation JWT.
func CreateDelegationToken(params DelegationTokenParams, key atmoscrypto.PrivateKey) (string, error) {
	if err := params.Issuer.Validate(); err != nil {
		return "", fmt.Errorf("credential: invalid delegation issuer: %w", err)
	}
	if err := params.Subject.Validate(); err != nil {
		return "", fmt.Errorf("credential: invalid delegation subject: %w", err)
	}
	if params.Audience != SpaceHostAudience(params.Subject.Authority()) {
		return "", errors.New("credential: delegation audience must identify the subject authority's space host")
	}
	return createToken(DelegationProfile, params.Issuer.String(), params.Subject.String(), params.Audience, "", "#atproto", params.Now, params.Lifetime, key)
}

// CreateClientAttestationToken creates a short-lived, single-use application JWT.
func CreateClientAttestationToken(params ClientAttestationTokenParams, key atmoscrypto.PrivateKey) (string, error) {
	if params.ClientID == "" || params.KeyID == "" {
		return "", errors.New("credential: attestation client_id, audience, and key id are required")
	}
	if err := validateSpaceHostAudience(params.Audience); err != nil {
		return "", err
	}
	return createToken(ClientAttestationProfile, params.ClientID, params.ClientID, params.Audience, "", params.KeyID, params.Now, params.Lifetime, key)
}

// CreateSpaceCredentialToken creates a reusable DPoP-bound space credential JWT.
func CreateSpaceCredentialToken(params SpaceCredentialTokenParams, key atmoscrypto.PrivateKey) (string, error) {
	if err := params.Issuer.Validate(); err != nil {
		return "", fmt.Errorf("credential: invalid credential issuer: %w", err)
	}
	if err := params.Subject.Validate(); err != nil {
		return "", fmt.Errorf("credential: invalid credential subject: %w", err)
	}
	if params.Issuer != params.Subject.Authority() {
		return "", errors.New("credential: credential issuer must equal the space authority")
	}
	if err := validateThumbprint(params.DPoPThumbprint); err != nil {
		return "", err
	}
	if params.KeyID == "" {
		params.KeyID = "#atproto"
	}
	if params.KeyID != "#atproto" && params.KeyID != "#atproto_space" {
		return "", errors.New("credential: credential key id must be #atproto or #atproto_space")
	}
	return createToken(SpaceCredentialProfile, params.Issuer.String(), params.Subject.String(), "", params.DPoPThumbprint, params.KeyID, params.Now, params.Lifetime, key)
}

func createToken(profile TokenProfile, issuer, subject, audience, jkt, kid string, now time.Time, lifetime time.Duration, key atmoscrypto.PrivateKey) (string, error) {
	algorithm, err := algorithmForPrivateKey(key)
	if err != nil {
		return "", err
	}
	if profile == ClientAttestationProfile && algorithm != "ES256" {
		return "", errors.New("credential: client attestations require ES256 with a P-256 key")
	}
	if now.IsZero() {
		now = time.Now()
	}
	now = now.Truncate(time.Second)
	maxLifetime := profileLifetime(profile)
	if lifetime == 0 {
		lifetime = maxLifetime
	}
	if lifetime <= 0 || lifetime > maxLifetime || lifetime%time.Second != 0 {
		return "", fmt.Errorf("credential: token lifetime must be a whole number of seconds in (0, %s]", maxLifetime)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("credential: generate token jti: %w", err)
	}
	header := wireHeader{Algorithm: algorithm, Type: profileType(profile), KeyID: kid}
	claims := wireClaims{
		Issuer: issuer, Subject: subject, Audience: audience,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(lifetime).Unix(),
		JTI: base64.RawURLEncoding.EncodeToString(nonce[:]),
	}
	if jkt != "" {
		claims.Confirmation = &wireConfirmation{JKT: jkt}
	}
	return signCompact(header, claims, key)
}

func signCompact(header, claims any, key atmoscrypto.PrivateKey) (string, error) {
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("credential: marshal JWT header: %w", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("credential: marshal JWT claims: %w", err)
	}
	input := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	signature, err := key.HashAndSign([]byte(input))
	if err != nil {
		return "", fmt.Errorf("credential: sign JWT: %w", err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// ParseDelegationToken performs structural validation without signature verification.
func ParseDelegationToken(raw string) (*UnverifiedToken, error) {
	return parseProfileToken(DelegationProfile, raw)
}

// ParseClientAttestationToken performs structural validation without signature verification.
func ParseClientAttestationToken(raw string) (*UnverifiedToken, error) {
	return parseProfileToken(ClientAttestationProfile, raw)
}

// ParseSpaceCredentialToken performs structural validation without signature verification.
func ParseSpaceCredentialToken(raw string) (*UnverifiedToken, error) {
	return parseProfileToken(SpaceCredentialProfile, raw)
}

func parseProfileToken(profile TokenProfile, raw string) (*UnverifiedToken, error) {
	token, err := parseToken(profile, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	return token, nil
}

type rawClaims struct {
	Issuer       string            `json:"iss"`
	Subject      string            `json:"sub"`
	Audience     *string           `json:"aud,omitempty"`
	IssuedAt     json.Number       `json:"iat"`
	ExpiresAt    json.Number       `json:"exp"`
	JTI          string            `json:"jti"`
	Confirmation *wireConfirmation `json:"cnf,omitempty"`
}

func parseToken(profile TokenProfile, raw string) (*UnverifiedToken, error) {
	headerPart, claimsPart, signature, signingInput, err := splitCompact(raw)
	if err != nil {
		return nil, err
	}
	var header wireHeader
	if err := decodeJSONObject(headerPart, &header, true); err != nil {
		return nil, fmt.Errorf("credential: invalid JWT header: %w", err)
	}
	if header.Type != profileType(profile) {
		return nil, fmt.Errorf("credential: wrong JWT type %q", header.Type)
	}
	if header.Algorithm != "ES256" && header.Algorithm != "ES256K" {
		return nil, fmt.Errorf("credential: unsupported JWT algorithm %q", header.Algorithm)
	}
	if profile == ClientAttestationProfile && header.Algorithm != "ES256" {
		return nil, errors.New("credential: client attestations require ES256")
	}
	if err := validateProfileKeyID(profile, header.KeyID); err != nil {
		return nil, err
	}
	var claims rawClaims
	if err := decodeJSONObject(claimsPart, &claims, true); err != nil {
		return nil, fmt.Errorf("credential: invalid JWT claims: %w", err)
	}
	iat, err := exactInteger(claims.IssuedAt, "iat")
	if err != nil {
		return nil, err
	}
	exp, err := exactInteger(claims.ExpiresAt, "exp")
	if err != nil {
		return nil, err
	}
	if claims.Issuer == "" || claims.Subject == "" || claims.JTI == "" {
		return nil, errors.New("credential: JWT iss, sub, and jti are required")
	}
	if exp <= iat {
		return nil, errors.New("credential: JWT exp must be after iat")
	}
	maxSeconds := int64(profileLifetime(profile) / time.Second)
	if iat > math.MaxInt64-maxSeconds || exp > iat+maxSeconds {
		return nil, errors.New("credential: JWT lifetime exceeds profile maximum")
	}
	if len(claims.JTI) > maxReplayIDBytes {
		return nil, errors.New("credential: JWT jti exceeds the replay-store limit")
	}
	jkt := ""
	if claims.Confirmation != nil {
		jkt = claims.Confirmation.JKT
	}
	audience := ""
	if claims.Audience != nil {
		audience = *claims.Audience
	}
	if err := validateProfileClaims(profile, claims.Issuer, claims.Subject, audience, claims.Audience != nil, jkt, claims.Confirmation != nil); err != nil {
		return nil, err
	}
	return &UnverifiedToken{
		Profile: profile, Header: TokenHeader(header),
		Issuer: claims.Issuer, Subject: claims.Subject, Audience: audience,
		IssuedAt: time.Unix(iat, 0), ExpiresAt: time.Unix(exp, 0), JTI: claims.JTI, ConfirmationJKT: jkt,
		raw: raw, signingInput: signingInput, signature: signature,
	}, nil
}

func splitCompact(raw string) ([]byte, []byte, []byte, []byte, error) {
	if len(raw) == 0 || len(raw) > maxTokenBytes {
		return nil, nil, nil, nil, errors.New("credential: compact JWT size is invalid")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, nil, nil, nil, errors.New("credential: compact JWT must contain three nonempty parts")
	}
	decoded := make([][]byte, 3)
	for i := range parts {
		value, err := base64.RawURLEncoding.Strict().DecodeString(parts[i])
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("credential: invalid base64url JWT part %d: %w", i+1, err)
		}
		decoded[i] = value
	}
	if len(decoded[2]) != 64 {
		return nil, nil, nil, nil, errors.New("credential: JWT signature must be 64 bytes")
	}
	return decoded[0], decoded[1], decoded[2], []byte(parts[0] + "." + parts[1]), nil
}

func decodeJSONObject(data []byte, dst any, disallowUnknown bool) error {
	if err := rejectDuplicateNames(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func rejectDuplicateNames(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := nameToken.(string)
			if !ok {
				return errors.New("JSON object name is not a string")
			}
			if _, exists := seen[name]; exists {
				return fmt.Errorf("duplicate JSON name %q", name)
			}
			seen[name] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("unterminated JSON array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func exactInteger(number json.Number, name string) (int64, error) {
	if number == "" || strings.ContainsAny(number.String(), ".eE") {
		return 0, fmt.Errorf("credential: JWT %s must be an integer", name)
	}
	value, err := number.Int64()
	if err != nil {
		return 0, fmt.Errorf("credential: JWT %s must be an int64: %w", name, err)
	}
	return value, nil
}

func validateProfileClaims(profile TokenProfile, issuer, subject, audience string, audiencePresent bool, jkt string, confirmationPresent bool) error {
	switch profile {
	case DelegationProfile:
		_, err := atmos.ParseDID(issuer)
		if err != nil {
			return fmt.Errorf("credential: invalid delegation issuer: %w", err)
		}
		space, err := atmos.ParseSpaceRef(subject)
		if err != nil {
			return fmt.Errorf("credential: invalid delegation subject: %w", err)
		}
		if !audiencePresent || audience != SpaceHostAudience(space.Authority()) {
			return errors.New("credential: invalid delegation audience")
		}
		if confirmationPresent {
			return errors.New("credential: delegation must not contain cnf")
		}
	case ClientAttestationProfile:
		if issuer != subject || !audiencePresent || audience == "" {
			return errors.New("credential: attestation iss and sub must equal client_id and aud is required")
		}
		if err := validateSpaceHostAudience(audience); err != nil {
			return err
		}
		if confirmationPresent {
			return errors.New("credential: attestation must not contain cnf")
		}
	case SpaceCredentialProfile:
		space, err := atmos.ParseSpaceRef(subject)
		if err != nil {
			return fmt.Errorf("credential: invalid credential subject: %w", err)
		}
		if issuer != space.Authority().String() || audiencePresent {
			return errors.New("credential: credential issuer must be its space authority and aud must be absent")
		}
		if !confirmationPresent {
			return errors.New("credential: credential cnf is required")
		}
		if err := validateThumbprint(jkt); err != nil {
			return err
		}
	default:
		return errors.New("credential: unknown token profile")
	}
	return nil
}

func validateProfileKeyID(profile TokenProfile, kid string) error {
	switch profile {
	case DelegationProfile:
		if kid != "#atproto" {
			return errors.New("credential: delegation kid must be #atproto")
		}
	case ClientAttestationProfile:
		if kid == "" {
			return errors.New("credential: attestation kid is required")
		}
	case SpaceCredentialProfile:
		if kid != "#atproto" && kid != "#atproto_space" {
			return errors.New("credential: credential kid must be #atproto or #atproto_space")
		}
	default:
		return errors.New("credential: unknown token profile")
	}
	return nil
}

func profileType(profile TokenProfile) string {
	switch profile {
	case DelegationProfile:
		return DelegationTokenType
	case ClientAttestationProfile:
		return ClientAttestationTokenType
	case SpaceCredentialProfile:
		return SpaceCredentialTokenType
	default:
		return ""
	}
}

func profileLifetime(profile TokenProfile) time.Duration {
	if profile == SpaceCredentialProfile {
		return defaultCredentialLifetime
	}
	return defaultSingleUseLifetime
}

func algorithmForPrivateKey(key atmoscrypto.PrivateKey) (string, error) {
	switch key.(type) {
	case *atmoscrypto.P256PrivateKey:
		return "ES256", nil
	case *atmoscrypto.K256PrivateKey:
		return "ES256K", nil
	default:
		return "", fmt.Errorf("credential: unsupported private key type %T", key)
	}
}

func verifySignature(token *UnverifiedToken, key atmoscrypto.PublicKey) error {
	if token == nil {
		return errors.New("credential: token is required")
	}
	var expectedAlgorithm string
	switch key.(type) {
	case *atmoscrypto.P256PublicKey:
		expectedAlgorithm = "ES256"
	case *atmoscrypto.K256PublicKey:
		expectedAlgorithm = "ES256K"
	default:
		return fmt.Errorf("credential: unsupported public key type %T", key)
	}
	if token.Header.Algorithm != expectedAlgorithm {
		return fmt.Errorf("credential: JWT algorithm %s does not match key algorithm %s", token.Header.Algorithm, expectedAlgorithm)
	}
	if err := key.HashAndVerifyLenient(token.signingInput, token.signature); err != nil {
		return fmt.Errorf("%w: invalid JWT signature: %w", ErrInvalidSignature, err)
	}
	return nil
}

func validateTime(token *UnverifiedToken, now time.Time, maxLifetime, skew time.Duration) error {
	if now.IsZero() {
		now = time.Now()
	}
	if maxLifetime <= 0 || skew < 0 {
		return errors.New("credential: max lifetime must be positive and skew nonnegative")
	}
	if token.ExpiresAt.Sub(token.IssuedAt) > maxLifetime {
		return errors.New("credential: JWT lifetime exceeds verifier maximum")
	}
	if token.IssuedAt.After(now.Add(skew)) {
		return errors.New("credential: JWT is issued in the future")
	}
	if !token.ExpiresAt.After(now.Add(-skew)) {
		return fmt.Errorf("%w: JWT is outside its expiration window", ErrTokenExpired)
	}
	return nil
}

func verifiedCopy(token *UnverifiedToken) *VerifiedToken {
	return &VerifiedToken{
		Profile: token.Profile, Header: token.Header, Issuer: token.Issuer, Subject: token.Subject,
		Audience: token.Audience, IssuedAt: token.IssuedAt, ExpiresAt: token.ExpiresAt,
		JTI: token.JTI, ConfirmationJKT: token.ConfirmationJKT,
	}
}

func validateThumbprint(value string) error {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return errors.New("credential: cnf.jkt must be a canonical base64url SHA-256 value")
	}
	return nil
}

func validateSpaceHostAudience(value string) error {
	didValue, found := strings.CutSuffix(value, "#atproto_space_host")
	if !found {
		return errors.New("credential: audience must be a DID with #atproto_space_host")
	}
	did, err := atmos.ParseDID(didValue)
	if err != nil || SpaceHostAudience(did) != value {
		return errors.New("credential: audience must contain a valid authority DID")
	}
	return nil
}

// VerifyDelegationOptions configures delegation verification.
type VerifyDelegationOptions struct {
	Resolver  identity.Resolver
	Refresh   *KeyRefresh
	Issuer    atmos.DID
	Subject   atmos.SpaceRef
	Audience  string
	Now       time.Time
	MaxAge    time.Duration
	ClockSkew time.Duration
	Replay    ReplayStore
}

// VerifyClientAttestationOptions configures attestation verification after the
// caller has securely obtained the named public key from validated metadata.
type VerifyClientAttestationOptions struct {
	Key       atmoscrypto.PublicKey
	ClientID  string
	KeyID     string
	Audience  string
	Now       time.Time
	MaxAge    time.Duration
	ClockSkew time.Duration
	Replay    ReplayStore
}

// VerifySpaceCredentialOptions configures reusable credential verification.
type VerifySpaceCredentialOptions struct {
	Resolver  identity.Resolver
	Refresh   *KeyRefresh
	Subject   atmos.SpaceRef
	Now       time.Time
	MaxAge    time.Duration
	ClockSkew time.Duration
}

// VerifyDelegationToken verifies all delegation bindings and atomically
// consumes its JTI only after cryptographic verification succeeds.
func VerifyDelegationToken(ctx context.Context, token *UnverifiedToken, opts VerifyDelegationOptions) (*VerifiedToken, error) {
	if token == nil || token.Profile != DelegationProfile {
		return nil, errors.New("credential: unverified delegation token is required")
	}
	canonical, err := parseToken(DelegationProfile, token.raw)
	if err != nil {
		return nil, fmt.Errorf("credential: reparse delegation token: %w", err)
	}
	token = canonical
	if err := opts.Issuer.Validate(); err != nil || token.Issuer != opts.Issuer.String() {
		return nil, fmt.Errorf("%w: delegation issuer", ErrTokenBinding)
	}
	if err := opts.Subject.Validate(); err != nil || token.Subject != opts.Subject.String() || token.Audience != opts.Audience || opts.Audience != SpaceHostAudience(opts.Subject.Authority()) {
		return nil, fmt.Errorf("%w: delegation subject or audience", ErrTokenBinding)
	}
	if err := verifyTimeOptions(token, opts.Now, opts.MaxAge, opts.ClockSkew); err != nil {
		return nil, err
	}
	key, err := ResolveDelegationVerificationKey(ctx, opts.Resolver, opts.Issuer, token.Header.KeyID)
	if err != nil {
		return nil, err
	}
	if err := verifySignatureWithRefresh(ctx, token, key, opts.Refresh, opts.Issuer); err != nil {
		return nil, err
	}
	if opts.Replay == nil {
		return nil, errors.New("credential: delegation replay store is required")
	}
	if err := opts.Replay.Consume(ctx, ReplayDelegation, token.JTI, token.ExpiresAt.Add(effectiveSkew(opts.ClockSkew))); err != nil {
		return nil, fmt.Errorf("credential: consume delegation jti: %w", err)
	}
	return verifiedCopy(token), nil
}

// VerifyClientAttestationToken verifies a client attestation and consumes its
// JTI. Metadata/JWKS retrieval is intentionally a separate, network-hardened layer.
func VerifyClientAttestationToken(ctx context.Context, token *UnverifiedToken, opts VerifyClientAttestationOptions) (*VerifiedToken, error) {
	if token == nil || token.Profile != ClientAttestationProfile {
		return nil, errors.New("credential: unverified client attestation is required")
	}
	canonical, err := parseToken(ClientAttestationProfile, token.raw)
	if err != nil {
		return nil, fmt.Errorf("credential: reparse client attestation: %w", err)
	}
	token = canonical
	if opts.ClientID == "" || token.Issuer != opts.ClientID || token.Subject != opts.ClientID || token.Audience != opts.Audience || token.Header.KeyID != opts.KeyID {
		return nil, fmt.Errorf("%w: client attestation", ErrTokenBinding)
	}
	if err := validateSpaceHostAudience(opts.Audience); err != nil {
		return nil, err
	}
	if err := verifyCommon(token, opts.Key, opts.Now, opts.MaxAge, opts.ClockSkew); err != nil {
		return nil, err
	}
	if opts.Replay == nil {
		return nil, errors.New("credential: attestation replay store is required")
	}
	if err := opts.Replay.Consume(ctx, ReplayAttestation, token.JTI, token.ExpiresAt.Add(effectiveSkew(opts.ClockSkew))); err != nil {
		return nil, fmt.Errorf("credential: consume attestation jti: %w", err)
	}
	return verifiedCopy(token), nil
}

// VerifySpaceCredentialToken verifies a reusable space credential. Its JTI is
// not consumed because this profile is intentionally multi-use.
func VerifySpaceCredentialToken(ctx context.Context, token *UnverifiedToken, opts VerifySpaceCredentialOptions) (*VerifiedToken, error) {
	if token == nil || token.Profile != SpaceCredentialProfile {
		return nil, errors.New("credential: unverified space credential is required")
	}
	canonical, err := parseToken(SpaceCredentialProfile, token.raw)
	if err != nil {
		return nil, fmt.Errorf("credential: reparse space credential: %w", err)
	}
	token = canonical
	if err := opts.Subject.Validate(); err != nil || token.Subject != opts.Subject.String() || token.Issuer != opts.Subject.Authority().String() {
		return nil, fmt.Errorf("%w: space credential subject or issuer", ErrTokenBinding)
	}
	if err := verifyTimeOptions(token, opts.Now, opts.MaxAge, opts.ClockSkew); err != nil {
		return nil, err
	}
	key, err := ResolveCredentialVerificationKey(ctx, opts.Resolver, opts.Subject.Authority(), token.Header.KeyID)
	if err != nil {
		return nil, err
	}
	if err := verifySignatureWithRefresh(ctx, token, key, opts.Refresh, opts.Subject.Authority()); err != nil {
		return nil, err
	}
	return verifiedCopy(token), nil
}

func verifyCommon(token *UnverifiedToken, key atmoscrypto.PublicKey, now time.Time, maxAge, skew time.Duration) error {
	if err := verifyTimeOptions(token, now, maxAge, skew); err != nil {
		return err
	}
	return verifySignature(token, key)
}

func verifySignatureWithRefresh(ctx context.Context, token *UnverifiedToken, key atmoscrypto.PublicKey, refresh *KeyRefresh, issuer atmos.DID) error {
	initialErr := verifySignature(token, key)
	if initialErr == nil || refresh == nil {
		return initialErr
	}
	freshKey, err := refresh.ResolveKey(ctx, issuer, token.Header.KeyID)
	if err != nil {
		return errors.Join(initialErr, fmt.Errorf("credential: refresh signing key: %w", err))
	}
	if err := verifySignature(token, freshKey); err != nil {
		return errors.Join(initialErr, fmt.Errorf("credential: refreshed signing key did not verify: %w", err))
	}
	return nil
}

func verifyTimeOptions(token *UnverifiedToken, now time.Time, maxAge, skew time.Duration) error {
	if maxAge == 0 {
		maxAge = profileLifetime(token.Profile)
	}
	if skew == 0 {
		skew = defaultClockSkew
	}
	if maxAge > profileLifetime(token.Profile) || skew > defaultClockSkew {
		return errors.New("credential: verifier age or skew exceeds profile maximum")
	}
	if err := validateTime(token, now, maxAge, skew); err != nil {
		return err
	}
	return nil
}

func effectiveSkew(value time.Duration) time.Duration {
	if value == 0 {
		return defaultClockSkew
	}
	return value
}
