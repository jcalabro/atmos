package crypto

import (
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"errors"

	secp256k1 "gitlab.com/yawning/secp256k1-voi"
	secp256k1secec "gitlab.com/yawning/secp256k1-voi/secec"
)

var k256StrictOptions = &secp256k1secec.ECDSAOptions{
	Hash:            crypto.SHA256,
	Encoding:        secp256k1secec.EncodingCompact,
	RejectMalleable: true,
}

var k256LenientOptions = &secp256k1secec.ECDSAOptions{
	Hash:            crypto.SHA256,
	Encoding:        secp256k1secec.EncodingCompact,
	RejectMalleable: false,
}

// K256PrivateKey is a secp256k1 (K-256) private key.
type K256PrivateKey struct {
	key *secp256k1secec.PrivateKey
}

// GenerateK256 creates a new random K-256 key pair.
func GenerateK256() (*K256PrivateKey, error) {
	key, err := secp256k1secec.GenerateKey()
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
	key, err := secp256k1secec.NewPrivateKey(raw)
	if err != nil {
		return nil, err
	}
	return &K256PrivateKey{key: key}, nil
}

// Bytes returns the raw 32-byte scalar of the private key.
func (k *K256PrivateKey) Bytes() []byte {
	return k.key.Bytes()
}

// PublicKey returns the corresponding K-256 public key.
func (k *K256PrivateKey) PublicKey() PublicKey {
	return &K256PublicKey{key: k.key.PublicKey()}
}

// HashAndSign computes SHA-256 of content and signs with low-S normalization.
func (k *K256PrivateKey) HashAndSign(content []byte) ([]byte, error) {
	hash := sha256.Sum256(content)
	return k.key.Sign(rand.Reader, hash[:], k256StrictOptions)
}

// K256PublicKey is a secp256k1 (K-256) public key.
type K256PublicKey struct {
	key *secp256k1secec.PublicKey
}

// ParsePublicBytesK256 parses a compressed SEC1 K-256 public key (33 bytes).
func ParsePublicBytesK256(raw []byte) (*K256PublicKey, error) {
	if len(raw) != 33 {
		return nil, errors.New("crypto: K-256 compressed public key must be 33 bytes")
	}
	p, err := secp256k1.NewIdentityPoint().SetCompressedBytes(raw)
	if err != nil {
		return nil, err
	}
	pub, err := secp256k1secec.NewPublicKeyFromPoint(p)
	if err != nil {
		return nil, err
	}
	return &K256PublicKey{key: pub}, nil
}

// Bytes returns the compressed SEC1 public key (33 bytes).
func (k *K256PublicKey) Bytes() []byte {
	return k.key.Point().CompressedBytes()
}

// HashAndVerify computes SHA-256 and verifies the signature, rejecting high-S.
func (k *K256PublicKey) HashAndVerify(content, sig []byte) error {
	hash := sha256.Sum256(content)
	if !k.key.Verify(hash[:], sig, k256StrictOptions) {
		return errors.New("crypto: K-256 signature verification failed")
	}
	return nil
}

// HashAndVerifyLenient is like [K256PublicKey.HashAndVerify] but accepts high-S signatures.
func (k *K256PublicKey) HashAndVerifyLenient(content, sig []byte) error {
	hash := sha256.Sum256(content)
	if !k.key.Verify(hash[:], sig, k256LenientOptions) {
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
	return k.key.Equal(o.key)
}
