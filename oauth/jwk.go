package oauth

import (
	"bytes"
	"encoding/base64"
	"fmt"

	"github.com/jcalabro/atmos/crypto"
)

// ECPublicJWK is the JSON Web Key representation of a P-256 public key,
// used in DPoP proof JWT headers and confidential-client JWKS documents.
type ECPublicJWK struct {
	KTY string `json:"kty"`
	CRV string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	// KeyID identifies the key used for confidential-client assertions.
	KeyID string `json:"kid,omitempty"`
}

// PublicJWK returns the JWK representation of a P-256 public key.
func PublicJWK(pub *crypto.P256PublicKey) ECPublicJWK {
	uncompressed := pub.UncompressedBytes() // 0x04 || X(32) || Y(32)
	return ECPublicJWK{
		KTY: "EC",
		CRV: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(uncompressed[1:33]),
		Y:   base64.RawURLEncoding.EncodeToString(uncompressed[33:65]),
	}
}

func validateClientPublicJWK(key ECPublicJWK) error {
	if key.KeyID == "" {
		return fmt.Errorf("oauth: public JWK in jwks requires kid")
	}
	if key.KTY != "EC" || key.CRV != "P-256" {
		return fmt.Errorf("oauth: public JWK in jwks must be an EC P-256 key")
	}
	x, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil || len(x) != 32 {
		return fmt.Errorf("oauth: public JWK in jwks has invalid x coordinate")
	}
	y, err := base64.RawURLEncoding.DecodeString(key.Y)
	if err != nil || len(y) != 32 {
		return fmt.Errorf("oauth: public JWK in jwks has invalid y coordinate")
	}
	compressed := make([]byte, 33)
	compressed[0] = 2 + (y[31] & 1)
	copy(compressed[1:], x)
	pub, err := crypto.ParsePublicBytesP256(compressed)
	if err != nil || !bytes.Equal(pub.UncompressedBytes()[33:65], y) {
		return fmt.Errorf("oauth: public JWK in jwks is not a P-256 point")
	}
	return nil
}
