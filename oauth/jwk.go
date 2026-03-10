package oauth

import (
	"encoding/base64"

	"github.com/jcalabro/atmos/crypto"
)

// ECPublicJWK is the JSON Web Key representation of a P-256 public key,
// used in DPoP proof JWT headers.
type ECPublicJWK struct {
	KTY string `json:"kty"`
	CRV string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
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
