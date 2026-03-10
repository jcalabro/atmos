package crypto

import (
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
)

// Multicodec varint prefixes for key types.
var (
	// Public key prefixes.
	multicodecP256Pub = []byte{0x80, 0x24} // 0x1200
	multicodecK256Pub = []byte{0xe7, 0x01} // 0xE7

)

// encodeMultibase encodes key bytes with a multicodec prefix as z-prefixed base58btc.
func encodeMultibase(prefix, keyBytes []byte) string {
	buf := make([]byte, len(prefix)+len(keyBytes))
	copy(buf, prefix)
	copy(buf[len(prefix):], keyBytes)
	return "z" + base58.Encode(buf)
}

// decodeMultibase decodes a z-prefixed base58btc multibase string.
// Returns the 2-byte multicodec prefix and the remaining key bytes.
func decodeMultibase(s string) (prefix []byte, keyBytes []byte, err error) {
	if len(s) == 0 {
		return nil, nil, errors.New("crypto: empty multibase string")
	}
	if s[0] != 'z' {
		return nil, nil, fmt.Errorf("crypto: expected 'z' multibase prefix, got %q", s[0])
	}
	raw, err := base58.Decode(s[1:])
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: base58 decode: %w", err)
	}
	if len(raw) < 2 {
		return nil, nil, errors.New("crypto: multibase data too short for multicodec prefix")
	}
	return raw[:2], raw[2:], nil
}

// matchPrefix returns true if the prefix matches the expected bytes.
func matchPrefix(prefix, expected []byte) bool {
	return len(prefix) == len(expected) && prefix[0] == expected[0] && prefix[1] == expected[1]
}
