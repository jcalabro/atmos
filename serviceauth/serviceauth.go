// Package serviceauth implements ATProto inter-service JWT authentication.
//
// In ATProto, services authenticate to each other using short-lived JWT
// bearer tokens signed with the caller's DID signing key. The receiver
// resolves the caller's DID document, extracts the public key, and
// verifies the JWT signature.
package serviceauth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/gt"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
)

// TokenParams configures a service auth JWT.
type TokenParams struct {
	// Issuer is the DID of the calling service. Required.
	Issuer atmos.DID

	// Audience is the DID of the target service, optionally with a fragment
	// (e.g. "did:web:api.example.com#appview"). Required.
	Audience string

	// Exp is the token expiration time. Typically now + 60s.
	Exp time.Time

	// LexMethod optionally binds the token to a specific XRPC method NSID.
	// Zero value means no binding.
	LexMethod atmos.NSID
}

// TokenClaims holds the verified claims from a service auth JWT.
type TokenClaims struct {
	Issuer    atmos.DID
	Audience  string
	LexMethod atmos.NSID
	IssuedAt  time.Time
	ExpiresAt time.Time
	JTI       string
}

// VerifyOptions configures token verification.
type VerifyOptions struct {
	// Audience is the expected aud claim. Required.
	Audience string

	// Identity resolves issuer DIDs to public keys. Required.
	Identity *identity.Directory

	// MaxAge is the maximum allowed token age (time since iat).
	// None = 5 minutes.
	MaxAge gt.Option[time.Duration]

	// Leeway is the clock skew tolerance for exp/iat checks.
	// None = 5 seconds.
	Leeway gt.Option[time.Duration]

	// LexMethod, if set, must match the token's lxm claim.
	LexMethod atmos.NSID

	retried bool // internal: prevents infinite retry on key rotation
}

// claims is the JWT claims structure used for signing and parsing.
type claims struct {
	jwt.RegisteredClaims
	LexMethod string `json:"lxm,omitempty"`
}

// signingMethod wraps our crypto.PrivateKey for use with golang-jwt.
type signingMethod struct {
	name string
}

var (
	sigES256  = &signingMethod{name: "ES256"}
	sigES256K = &signingMethod{name: "ES256K"}
)

func (s *signingMethod) Alg() string { return s.name }
func (s *signingMethod) Verify(signingString string, sig []byte, key any) error {
	pub, ok := key.(crypto.PublicKey)
	if !ok {
		return errors.New("serviceauth: key must be crypto.PublicKey")
	}
	return pub.HashAndVerifyLenient([]byte(signingString), sig)
}

func (s *signingMethod) Sign(signingString string, key any) ([]byte, error) {
	priv, ok := key.(crypto.PrivateKey)
	if !ok {
		return nil, errors.New("serviceauth: key must be crypto.PrivateKey")
	}
	return priv.HashAndSign([]byte(signingString))
}

func init() {
	jwt.RegisterSigningMethod(sigES256.Alg(), func() jwt.SigningMethod { return sigES256 })
	jwt.RegisterSigningMethod(sigES256K.Alg(), func() jwt.SigningMethod { return sigES256K })
}

// CreateToken signs a service auth JWT with the given private key.
func CreateToken(params TokenParams, key crypto.PrivateKey) (string, error) {
	// Generate random nonce for jti.
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("serviceauth: generate nonce: %w", err)
	}

	now := time.Now()

	c := claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    string(params.Issuer),
			Audience:  jwt.ClaimStrings{params.Audience},
			ExpiresAt: jwt.NewNumericDate(params.Exp),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        base64.RawURLEncoding.EncodeToString(nonce[:]),
		},
	}
	if params.LexMethod != "" {
		c.LexMethod = string(params.LexMethod)
	}

	// Select signing method based on key type.
	var method jwt.SigningMethod
	switch key.(type) {
	case *crypto.P256PrivateKey:
		method = sigES256
	case *crypto.K256PrivateKey:
		method = sigES256K
	default:
		return "", fmt.Errorf("serviceauth: unsupported key type %T", key)
	}

	token := jwt.NewWithClaims(method, c)
	return token.SignedString(key)
}

// VerifyToken parses and verifies a service auth JWT, resolving the
// issuer's DID to obtain the public key.
func VerifyToken(ctx context.Context, tokenString string, opts VerifyOptions) (*TokenClaims, error) {
	maxAge := opts.MaxAge.ValOr(5 * time.Minute)
	leeway := opts.Leeway.ValOr(5 * time.Second)

	var c claims
	_, err := jwt.ParseWithClaims(tokenString, &c, func(token *jwt.Token) (any, error) {
		// Extract issuer to resolve public key.
		iss, err := c.GetIssuer()
		if err != nil || iss == "" {
			return nil, errors.New("serviceauth: missing issuer")
		}

		did, err := atmos.ParseDID(iss)
		if err != nil {
			return nil, fmt.Errorf("serviceauth: invalid issuer DID: %w", err)
		}

		id, err := opts.Identity.LookupDID(ctx, did)
		if err != nil {
			return nil, fmt.Errorf("serviceauth: resolve issuer: %w", err)
		}

		pub, err := id.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("serviceauth: issuer has no signing key: %w", err)
		}
		return pub, nil
	}, jwt.WithValidMethods([]string{"ES256", "ES256K"}),
		jwt.WithAudience(opts.Audience),
		jwt.WithLeeway(leeway),
		jwt.WithIssuedAt(),
	)

	if err != nil {
		// On signature verification failure, purge the DID cache and retry
		// once in case the issuer rotated their signing key.
		if errors.Is(err, jwt.ErrTokenSignatureInvalid) && opts.Identity != nil && !opts.retried {
			iss, _ := c.GetIssuer()
			if did, parseErr := atmos.ParseDID(iss); parseErr == nil {
				opts.Identity.Purge(ctx, did)
				retry := opts
				retry.retried = true
				return VerifyToken(ctx, tokenString, retry)
			}
		}
		return nil, fmt.Errorf("serviceauth: %w", err)
	}

	// Check token age.
	iat, err := c.GetIssuedAt()
	if err != nil || iat == nil {
		return nil, errors.New("serviceauth: missing iat claim")
	}
	if time.Since(iat.Time) > maxAge+leeway {
		return nil, errors.New("serviceauth: token too old")
	}

	// Check lxm binding.
	if opts.LexMethod != "" {
		if c.LexMethod == "" {
			return nil, errors.New("serviceauth: token missing required lxm claim")
		}
		if atmos.NSID(c.LexMethod) != opts.LexMethod {
			return nil, fmt.Errorf("serviceauth: lxm mismatch: token has %q, expected %q", c.LexMethod, opts.LexMethod)
		}
	}

	iss, err := c.GetIssuer()
	if err != nil {
		return nil, fmt.Errorf("serviceauth: get issuer: %w", err)
	}
	aud, err := c.GetAudience()
	if err != nil {
		return nil, fmt.Errorf("serviceauth: get audience: %w", err)
	}
	exp, err := c.GetExpirationTime()
	if err != nil {
		return nil, fmt.Errorf("serviceauth: get expiration: %w", err)
	}

	result := &TokenClaims{
		Issuer:   atmos.DID(iss),
		IssuedAt: iat.Time,
		JTI:      c.ID,
	}
	if len(aud) > 0 {
		result.Audience = aud[0]
	}
	if exp != nil {
		result.ExpiresAt = exp.Time
	}
	if c.LexMethod != "" {
		result.LexMethod = atmos.NSID(c.LexMethod)
	}
	return result, nil
}
