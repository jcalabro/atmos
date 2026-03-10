package crypto

import (
	"crypto/sha256"
	"errors"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	decredecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// K256PrivateKey is a secp256k1 (K-256) private key.
type K256PrivateKey struct {
	key *secp256k1.PrivateKey
}

// GenerateK256 creates a new random K-256 key pair.
func GenerateK256() (*K256PrivateKey, error) {
	key, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	return &K256PrivateKey{key: key}, nil
}

// ParsePrivateK256 parses a K-256 private key from a raw 32-byte scalar.
func ParsePrivateK256(raw []byte) (*K256PrivateKey, error) {
	if len(raw) != 32 {
		return nil, errors.New("crypto: K-256 private key must be 32 bytes")
	}
	key := secp256k1.PrivKeyFromBytes(raw)
	return &K256PrivateKey{key: key}, nil
}

// Bytes returns the raw 32-byte scalar of the private key.
func (k *K256PrivateKey) Bytes() []byte {
	return k.key.Serialize()
}

// PublicKey returns the corresponding K-256 public key.
func (k *K256PrivateKey) PublicKey() PublicKey {
	return &K256PublicKey{key: k.key.PubKey()}
}

// HashAndSign computes SHA-256 of content and signs with low-S normalization.
func (k *K256PrivateKey) HashAndSign(content []byte) ([]byte, error) {
	hash := sha256.Sum256(content)
	sig := decredecdsa.Sign(k.key, hash[:])

	r := sig.R()
	s := sig.S()

	// Low-S normalization using ModNScalar directly.
	if s.IsOverHalfOrder() {
		s.Negate()
	}

	out := make([]byte, 64)
	rBytes := r.Bytes()
	sBytes := s.Bytes()
	copy(out[0:32], rBytes[:])
	copy(out[32:64], sBytes[:])
	return out, nil
}

// K256PublicKey is a secp256k1 (K-256) public key.
type K256PublicKey struct {
	key *secp256k1.PublicKey
}

// ParsePublicBytesK256 parses a compressed SEC1 K-256 public key (33 bytes).
func ParsePublicBytesK256(raw []byte) (*K256PublicKey, error) {
	if len(raw) != 33 {
		return nil, errors.New("crypto: K-256 compressed public key must be 33 bytes")
	}
	key, err := secp256k1.ParsePubKey(raw)
	if err != nil {
		return nil, err
	}
	return &K256PublicKey{key: key}, nil
}

// Bytes returns the compressed SEC1 public key (33 bytes).
func (k *K256PublicKey) Bytes() []byte {
	return k.key.SerializeCompressed()
}

// HashAndVerify computes SHA-256 and verifies the signature, rejecting high-S.
func (k *K256PublicKey) HashAndVerify(content, sig []byte) error {
	return k.verify(content, sig, true)
}

// HashAndVerifyLenient is like [K256PublicKey.HashAndVerify] but accepts high-S signatures.
func (k *K256PublicKey) HashAndVerifyLenient(content, sig []byte) error {
	return k.verify(content, sig, false)
}

func (k *K256PublicKey) verify(content, sigBytes []byte, strictLowS bool) error {
	if len(sigBytes) != 64 {
		return errors.New("crypto: signature must be 64 bytes")
	}

	var rField, sField secp256k1.ModNScalar
	if rField.SetByteSlice(sigBytes[:32]) {
		return errors.New("crypto: K-256 signature r value overflow")
	}
	if sField.SetByteSlice(sigBytes[32:]) {
		return errors.New("crypto: K-256 signature s value overflow")
	}

	if strictLowS && sField.IsOverHalfOrder() {
		return errors.New("crypto: K-256 signature has high-S value")
	}

	hash := sha256.Sum256(content)
	sig := decredecdsa.NewSignature(&rField, &sField)
	if !sig.Verify(hash[:], k.key) {
		return errors.New("crypto: K-256 signature verification failed")
	}
	return nil
}

// DIDKey returns the did:key string for this K-256 public key.
func (k *K256PublicKey) DIDKey() string {
	return "did:key:" + k.Multibase()
}

// Multibase returns the z-prefixed base58btc multicodec encoding.
func (k *K256PublicKey) Multibase() string {
	return encodeMultibase(multicodecK256Pub, k.Bytes())
}

// Equal reports whether two K-256 public keys are identical.
func (k *K256PublicKey) Equal(other PublicKey) bool {
	o, ok := other.(*K256PublicKey)
	if !ok {
		return false
	}
	return k.key.IsEqual(o.key)
}
