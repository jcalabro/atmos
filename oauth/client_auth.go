package oauth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/url"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atmos/crypto"
)

const clientAssertionTypeJWTBearer = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

// ClientAuth adds client authentication parameters to token endpoint requests.
type ClientAuth interface {
	// Apply adds authentication parameters to the form values.
	Apply(params url.Values, issuer string) error
}

// PublicClientAuth authenticates public clients (token_endpoint_auth_method: "none").
type PublicClientAuth struct {
	ClientID string
}

func (a *PublicClientAuth) Apply(params url.Values, _ string) error {
	params.Set("client_id", a.ClientID)
	return nil
}

// ConfidentialClientAuth authenticates confidential clients using private_key_jwt.
type ConfidentialClientAuth struct {
	ClientID string
	Key      *crypto.P256PrivateKey
	KeyID    string
}

func (a *ConfidentialClientAuth) Apply(params url.Values, issuer string) error {
	// Generate random jti.
	var jti [16]byte
	if _, err := rand.Read(jti[:]); err != nil {
		return fmt.Errorf("oauth: generate client assertion jti: %w", err)
	}

	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    a.ClientID,
		Subject:   a.ClientID,
		Audience:  jwt.ClaimStrings{issuer},
		ID:        base64.RawURLEncoding.EncodeToString(jti[:]),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(60 * time.Second)),
	}

	token := jwt.NewWithClaims(jwt.GetSigningMethod("ES256"), claims)
	token.Header["kid"] = a.KeyID

	assertion, err := token.SignedString(a.Key)
	if err != nil {
		return fmt.Errorf("oauth: sign client assertion: %w", err)
	}

	params.Set("client_id", a.ClientID)
	params.Set("client_assertion_type", clientAssertionTypeJWTBearer)
	params.Set("client_assertion", assertion)
	return nil
}
