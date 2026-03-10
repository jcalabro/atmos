package crypto

import (
	"errors"
	"fmt"
	"strings"
)

// ParsePublicDIDKey parses a did:key string and returns the public key.
func ParsePublicDIDKey(s string) (PublicKey, error) {
	if !strings.HasPrefix(s, "did:key:") {
		return nil, errors.New("crypto: not a did:key string")
	}
	return ParsePublicMultibase(s[8:])
}

// ParsePublicMultibase parses a z-prefixed base58btc multicodec public key.
func ParsePublicMultibase(s string) (PublicKey, error) {
	prefix, keyBytes, err := decodeMultibase(s)
	if err != nil {
		return nil, err
	}

	if matchPrefix(prefix, multicodecP256Pub) {
		return ParsePublicBytesP256(keyBytes)
	}
	if matchPrefix(prefix, multicodecK256Pub) {
		return ParsePublicBytesK256(keyBytes)
	}

	return nil, fmt.Errorf("crypto: unknown multicodec prefix [0x%02x, 0x%02x]", prefix[0], prefix[1])
}
