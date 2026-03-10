package oauth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// PKCEChallenge holds PKCE code challenge parameters for an authorization request.
type PKCEChallenge struct {
	Verifier  string // base64url-encoded 32 random bytes (43 chars)
	Challenge string // base64url(SHA-256(Verifier))
	Method    string // "S256"
}

// GeneratePKCE creates a new PKCE challenge with a cryptographically random verifier.
func GeneratePKCE() (*PKCEChallenge, error) {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, fmt.Errorf("oauth: generate PKCE verifier: %w", err)
	}

	verifier := base64.RawURLEncoding.EncodeToString(buf[:])
	hash := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(hash[:])

	return &PKCEChallenge{
		Verifier:  verifier,
		Challenge: challenge,
		Method:    "S256",
	}, nil
}
