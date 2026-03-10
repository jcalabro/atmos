package crypto

// PrivateKey can sign data.
type PrivateKey interface {
	// PublicKey returns the corresponding public key.
	PublicKey() PublicKey
	// HashAndSign computes SHA-256 of content and signs it (low-S normalized).
	// Returns a 64-byte compact [R || S] signature.
	HashAndSign(content []byte) ([]byte, error)
}

// PublicKey can verify signatures and be serialized.
type PublicKey interface {
	// Bytes returns the compressed SEC1 public key (33 bytes).
	Bytes() []byte
	// HashAndVerify computes SHA-256 of content and verifies the signature.
	// Rejects non-low-S signatures.
	HashAndVerify(content, sig []byte) error
	// HashAndVerifyLenient is like HashAndVerify but accepts high-S signatures.
	// Used for JWT verification compatibility.
	HashAndVerifyLenient(content, sig []byte) error
	// DIDKey returns the did:key string representation.
	DIDKey() string
	// Multibase returns the z-prefixed base58btc multicodec encoding.
	Multibase() string
	// Equal returns true if the other key is identical.
	Equal(other PublicKey) bool
}
