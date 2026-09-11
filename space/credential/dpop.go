package credential

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	atmoscrypto "github.com/jcalabro/atmos/crypto"
)

const (
	// DPoPProofType is the required protected-header type for DPoP proofs.
	DPoPProofType = "dpop+jwt"
	// DPoPMaxAge is the maximum age of a spaces DPoP proof before clock skew.
	DPoPMaxAge = 60 * time.Second
)

// ECPublicJWK is the public-only RFC 7518 representation of a P-256 public key.
// Strict proof decoding rejects every other member, including private d.
type ECPublicJWK struct {
	KTY string `json:"kty"`
	CRV string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// PublicJWK converts a public key to its public-only P-256 JWK representation.
func PublicJWK(key atmoscrypto.PublicKey) (ECPublicJWK, error) {
	publicKey, ok := key.(*atmoscrypto.P256PublicKey)
	if !ok {
		return ECPublicJWK{}, errors.New("credential: DPoP requires a P-256 public key")
	}
	uncompressed := publicKey.UncompressedBytes()
	return ECPublicJWK{
		KTY: "EC", CRV: "P-256",
		X: base64.RawURLEncoding.EncodeToString(uncompressed[1:33]),
		Y: base64.RawURLEncoding.EncodeToString(uncompressed[33:65]),
	}, nil
}

// JWKThumbprint computes the RFC 7638 SHA-256 thumbprint over the canonical
// public crv,kty,x,y members.
func JWKThumbprint(jwk ECPublicJWK) (string, error) {
	if _, err := publicKeyFromJWK(jwk); err != nil {
		return "", err
	}
	// RFC 7638 requires lexicographic member ordering and no whitespace.
	canonical, err := json.Marshal(struct {
		CRV string `json:"crv"`
		KTY string `json:"kty"`
		X   string `json:"x"`
		Y   string `json:"y"`
	}{CRV: jwk.CRV, KTY: jwk.KTY, X: jwk.X, Y: jwk.Y})
	if err != nil {
		return "", fmt.Errorf("credential: marshal JWK thumbprint input: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

// DPoPProofParams configures creation of a P-256 spaces DPoP proof.
type DPoPProofParams struct {
	Key        atmoscrypto.PrivateKey
	Method     string
	TargetURL  string
	Credential string
	Now        time.Time
}

type dpopHeader struct {
	Algorithm string      `json:"alg"`
	Type      string      `json:"typ"`
	JWK       ECPublicJWK `json:"jwk"`
}

type dpopClaims struct {
	JTI      string `json:"jti"`
	HTM      string `json:"htm"`
	HTU      string `json:"htu"`
	ATH      string `json:"ath,omitempty"`
	IssuedAt int64  `json:"iat"`
}

// CreateDPoPProof creates an endpoint-bound proof. Query and fragment are
// excluded from htu as required by RFC 9449.
func CreateDPoPProof(params DPoPProofParams) (string, error) {
	privateKey, ok := params.Key.(*atmoscrypto.P256PrivateKey)
	if !ok {
		return "", errors.New("credential: DPoP signing key must be P-256")
	}
	method, err := normalizeMethod(params.Method)
	if err != nil {
		return "", err
	}
	htu, err := normalizeHTU(params.TargetURL)
	if err != nil {
		return "", err
	}
	jwk, err := PublicJWK(privateKey.PublicKey())
	if err != nil {
		return "", err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("credential: generate DPoP jti: %w", err)
	}
	now := params.Now
	if now.IsZero() {
		now = time.Now()
	}
	claims := dpopClaims{
		JTI: base64.RawURLEncoding.EncodeToString(nonce[:]),
		HTM: method, HTU: htu, IssuedAt: now.Truncate(time.Second).Unix(),
	}
	if params.Credential != "" {
		claims.ATH = credentialHash(params.Credential)
	}
	return signCompact(dpopHeader{Algorithm: "ES256", Type: DPoPProofType, JWK: jwk}, claims, privateKey)
}

// VerifyDPoPOptions configures proof verification. An empty Credential means
// ath must be absent, as in credential exchange.
type VerifyDPoPOptions struct {
	Method      string
	TargetURL   string
	Credential  string
	ExpectedJKT string
	Now         time.Time
	MaxAge      time.Duration
	ClockSkew   time.Duration
	Replay      ReplayStore
}

// VerifiedDPoPProof contains verified DPoP bindings. It is returned only after
// replay consumption succeeds.
type VerifiedDPoPProof struct {
	JTI      string
	JKT      string
	HTM      string
	HTU      string
	IssuedAt time.Time
}

// rawDPoPClaims tracks ath as a raw value because a *string cannot distinguish
// an absent member from a present JSON null, and the no-credential exchange
// profile requires ath to be absent.
type rawDPoPClaims struct {
	JTI      string          `json:"jti"`
	HTM      string          `json:"htm"`
	HTU      string          `json:"htu"`
	ATH      json.RawMessage `json:"ath,omitempty"`
	IssuedAt json.Number     `json:"iat"`
}

// VerifyDPoPProof verifies the signature, method, URI, credential hash, key
// thumbprint, time window, and finally consumes the proof JTI.
func VerifyDPoPProof(ctx context.Context, proof string, opts VerifyDPoPOptions) (*VerifiedDPoPProof, error) {
	headerPart, claimsPart, signature, signingInput, err := splitCompact(proof)
	if err != nil {
		return nil, fmt.Errorf("credential: invalid DPoP proof: %w", err)
	}
	var header dpopHeader
	if err := decodeJSONObject(headerPart, &header, true); err != nil {
		return nil, fmt.Errorf("credential: invalid DPoP header: %w", err)
	}
	if header.Type != DPoPProofType || header.Algorithm != "ES256" {
		return nil, errors.New("credential: DPoP typ must be dpop+jwt and alg must be ES256")
	}
	publicKey, err := publicKeyFromJWK(header.JWK)
	if err != nil {
		return nil, err
	}
	if err := publicKey.HashAndVerifyLenient(signingInput, signature); err != nil {
		return nil, fmt.Errorf("%w: DPoP verification failed: %w", ErrInvalidSignature, err)
	}
	var claims rawDPoPClaims
	if err := decodeJSONObject(claimsPart, &claims, true); err != nil {
		return nil, fmt.Errorf("credential: invalid DPoP claims: %w", err)
	}
	iat, err := exactInteger(claims.IssuedAt, "iat")
	if err != nil {
		return nil, err
	}
	if claims.JTI == "" || len(claims.JTI) > maxReplayIDBytes {
		return nil, errors.New("credential: DPoP jti is absent or too long")
	}
	method, err := normalizeMethod(opts.Method)
	if err != nil {
		return nil, err
	}
	expectedHTU, err := normalizeHTU(opts.TargetURL)
	if err != nil {
		return nil, err
	}
	// The signed htu itself must already be the query/fragment-free normalized
	// value. Merely normalizing attacker input would accept forbidden components.
	claimHTU, err := normalizeHTU(claims.HTU)
	if err != nil || claimHTU != claims.HTU {
		return nil, errors.New("credential: DPoP htu is not a normalized absolute HTTP URL")
	}
	if claims.HTM != method || claims.HTU != expectedHTU {
		return nil, fmt.Errorf("%w: DPoP method or target", ErrTokenBinding)
	}
	ath, athPresent, err := decodeOptionalString(claims.ATH, "ath")
	if err != nil {
		return nil, err
	}
	if opts.Credential == "" {
		if athPresent {
			return nil, errors.New("credential: DPoP ath must be absent without a credential")
		}
	} else if !athPresent || ath != credentialHash(opts.Credential) {
		return nil, fmt.Errorf("%w: DPoP credential hash", ErrTokenBinding)
	}
	jkt, err := JWKThumbprint(header.JWK)
	if err != nil {
		return nil, err
	}
	if opts.ExpectedJKT != "" && jkt != opts.ExpectedJKT {
		return nil, fmt.Errorf("%w: DPoP key thumbprint", ErrTokenBinding)
	}
	maxAge := opts.MaxAge
	if maxAge == 0 {
		maxAge = DPoPMaxAge
	}
	skew := opts.ClockSkew
	if skew == 0 {
		skew = defaultClockSkew
	}
	if maxAge <= 0 || maxAge > DPoPMaxAge || skew < 0 || skew > defaultClockSkew {
		return nil, errors.New("credential: invalid DPoP age or clock skew")
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	issuedAt := time.Unix(iat, 0)
	if issuedAt.After(now.Add(skew)) {
		return nil, errors.New("credential: DPoP proof issued in the future")
	}
	if now.After(issuedAt.Add(maxAge).Add(skew)) {
		return nil, fmt.Errorf("%w: DPoP proof is too old", ErrTokenExpired)
	}
	if opts.Replay == nil {
		return nil, errors.New("credential: DPoP replay store is required")
	}
	if err := opts.Replay.Consume(ctx, ReplayDPoP, claims.JTI, issuedAt.Add(maxAge).Add(skew)); err != nil {
		return nil, fmt.Errorf("credential: consume DPoP jti: %w", err)
	}
	return &VerifiedDPoPProof{JTI: claims.JTI, JKT: jkt, HTM: claims.HTM, HTU: claims.HTU, IssuedAt: issuedAt}, nil
}

func publicKeyFromJWK(jwk ECPublicJWK) (*atmoscrypto.P256PublicKey, error) {
	if jwk.KTY != "EC" || jwk.CRV != "P-256" {
		return nil, errors.New("credential: embedded DPoP JWK must be EC P-256")
	}
	x, err := decodeCoordinate(jwk.X, "x")
	if err != nil {
		return nil, err
	}
	y, err := decodeCoordinate(jwk.Y, "y")
	if err != nil {
		return nil, err
	}
	compressed := make([]byte, 33)
	compressed[0] = 2 | (y[31] & 1)
	copy(compressed[1:], x)
	key, err := atmoscrypto.ParsePublicBytesP256(compressed)
	if err != nil {
		return nil, fmt.Errorf("credential: invalid P-256 JWK point: %w", err)
	}
	// Parsing compressed form determines y from parity. Compare the complete
	// coordinate to reject a supplied y that is not on the selected point.
	uncompressed := key.UncompressedBytes()
	if !bytes.Equal(uncompressed[33:65], y) {
		return nil, errors.New("credential: invalid P-256 JWK y coordinate")
	}
	return key, nil
}

func decodeCoordinate(encoded, name string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return nil, fmt.Errorf("credential: JWK %s must be a canonical 32-byte base64url coordinate", name)
	}
	return decoded, nil
}

func credentialHash(credential string) string {
	digest := sha256.Sum256([]byte(credential))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func normalizeMethod(method string) (string, error) {
	method = strings.ToUpper(method)
	if method == "" || !validHTTPToken(method) {
		return "", errors.New("credential: DPoP method must be a valid HTTP token")
	}
	return method, nil
}

func validHTTPToken(value string) bool {
	for i := range len(value) {
		character := value[i]
		if character <= 0x20 || character >= 0x7f || strings.ContainsRune("()<>@,;:\\\"/[]?={}", rune(character)) {
			return false
		}
	}
	return true
}

func normalizeHTU(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.Opaque != "" || parsed.User != nil {
		return "", errors.New("credential: DPoP target must be an absolute HTTP URL without userinfo")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", errors.New("credential: DPoP target must use HTTP or HTTPS")
	}
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return parsed.String(), nil
}
