package crypto

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"math/big"
)

var (
	p256Curve     = elliptic.P256()
	p256Order     = p256Curve.Params().N //nolint:staticcheck
	p256HalfOrder = new(big.Int).Rsh(p256Order, 1)
)

// P256PrivateKey is a P-256 private key.
type P256PrivateKey struct {
	key *ecdsa.PrivateKey
}

// GenerateP256 creates a new random P-256 key pair.
func GenerateP256() (*P256PrivateKey, error) {
	key, err := ecdsa.GenerateKey(p256Curve, rand.Reader)
	if err != nil {
		return nil, err
	}
	return &P256PrivateKey{key: key}, nil
}

// ParsePrivateP256 parses a P-256 private key from a raw 32-byte scalar.
func ParsePrivateP256(raw []byte) (*P256PrivateKey, error) {
	key, err := ecdsa.ParseRawPrivateKey(p256Curve, raw)
	if err != nil {
		return nil, err
	}
	return &P256PrivateKey{key: key}, nil
}

// Bytes returns the raw 32-byte private key scalar.
func (k *P256PrivateKey) Bytes() []byte {
	b, err := k.key.Bytes()
	if err != nil {
		panic("crypto: failed to encode P-256 private key: " + err.Error())
	}
	return b
}

// PublicKey returns the corresponding P-256 public key.
func (k *P256PrivateKey) PublicKey() PublicKey {
	return &P256PublicKey{key: &k.key.PublicKey}
}

// HashAndSign computes SHA-256 of content and signs with low-S normalization.
func (k *P256PrivateKey) HashAndSign(content []byte) ([]byte, error) {
	hash := sha256.Sum256(content)
	r, s, err := ecdsa.Sign(rand.Reader, k.key, hash[:])
	if err != nil {
		return nil, err
	}
	s = normalizeLowS(p256Order, p256HalfOrder, s)
	return encodeCompactSig(r, s), nil
}

// P256PublicKey is a P-256 public key.
type P256PublicKey struct {
	key *ecdsa.PublicKey
}

// ParsePublicBytesP256 parses a compressed SEC1 P-256 public key (33 bytes).
func ParsePublicBytesP256(compressed []byte) (*P256PublicKey, error) {
	if len(compressed) != 33 {
		return nil, errors.New("crypto: P-256 compressed public key must be 33 bytes")
	}
	// Decompress to uncompressed form, then parse via the modern API.
	//nolint:staticcheck // No ParseCompressedPublicKey in stdlib; this is the only way to decompress.
	x, y := elliptic.UnmarshalCompressed(p256Curve, compressed)
	if x == nil {
		return nil, errors.New("crypto: invalid P-256 compressed public key")
	}
	// Build uncompressed encoding: 0x04 || X (32 bytes) || Y (32 bytes).
	uncompressed := make([]byte, 65)
	uncompressed[0] = 0x04
	xBytes := x.Bytes()
	yBytes := y.Bytes()
	copy(uncompressed[1+32-len(xBytes):33], xBytes)
	copy(uncompressed[33+32-len(yBytes):65], yBytes)
	key, err := ecdsa.ParseUncompressedPublicKey(p256Curve, uncompressed)
	if err != nil {
		return nil, err
	}
	return &P256PublicKey{key: key}, nil
}

// Bytes returns the compressed SEC1 public key (33 bytes).
func (k *P256PublicKey) Bytes() []byte {
	// Get uncompressed bytes via the modern API, then compress.
	uncompressed, err := k.key.Bytes()
	if err != nil {
		panic("crypto: failed to encode P-256 public key: " + err.Error())
	}
	return compressP256(uncompressed)
}

// UncompressedBytes returns the uncompressed SEC1 encoding of the public key:
// 0x04 || X (32 bytes) || Y (32 bytes) = 65 bytes total.
// This is useful for extracting the X and Y coordinates for JWK serialization.
func (k *P256PublicKey) UncompressedBytes() []byte {
	b, err := k.key.Bytes()
	if err != nil {
		panic("crypto: failed to encode P-256 public key: " + err.Error())
	}
	return b
}

// HashAndVerify computes SHA-256 and verifies the signature, rejecting high-S.
func (k *P256PublicKey) HashAndVerify(content, sig []byte) error {
	return k.verify(content, sig, true)
}

// HashAndVerifyLenient is like [P256PublicKey.HashAndVerify] but accepts high-S signatures.
func (k *P256PublicKey) HashAndVerifyLenient(content, sig []byte) error {
	return k.verify(content, sig, false)
}

func (k *P256PublicKey) verify(content, sig []byte, strictLowS bool) error {
	r, s, err := decodeCompactSig(sig)
	if err != nil {
		return err
	}
	if strictLowS && !isLowS(p256HalfOrder, s) {
		return errors.New("crypto: P-256 signature has high-S value")
	}
	hash := sha256.Sum256(content)
	if !ecdsa.Verify(k.key, hash[:], r, s) {
		return errors.New("crypto: P-256 signature verification failed")
	}
	return nil
}

// DIDKey returns the did:key string for this P-256 public key.
func (k *P256PublicKey) DIDKey() string {
	return "did:key:" + k.Multibase()
}

// Multibase returns the z-prefixed base58btc multicodec encoding.
func (k *P256PublicKey) Multibase() string {
	return encodeMultibase(multicodecP256Pub, k.Bytes())
}

// Equal reports whether two P-256 public keys are identical.
func (k *P256PublicKey) Equal(other PublicKey) bool {
	o, ok := other.(*P256PublicKey)
	if !ok {
		return false
	}
	return bytes.Equal(k.Bytes(), o.Bytes())
}

// compressP256 compresses an uncompressed SEC1 P-256 point (65 bytes) to compressed (33 bytes).
func compressP256(uncompressed []byte) []byte {
	if len(uncompressed) != 65 || uncompressed[0] != 0x04 {
		panic("crypto: invalid uncompressed P-256 point")
	}
	out := make([]byte, 33)
	if uncompressed[64]&1 == 0 {
		out[0] = 0x02
	} else {
		out[0] = 0x03
	}
	copy(out[1:], uncompressed[1:33])
	return out
}
