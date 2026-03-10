package mst

import (
	"crypto/sha256"
	"math/bits"
	"unsafe"
)

// HeightForKey computes the MST height for a given key.
// SHA-256 hash the key, count leading zero 2-bit pairs.
func HeightForKey(key string) uint8 {
	// Avoid string→[]byte allocation using unsafe.
	h := sha256.Sum256(unsafe.Slice(unsafe.StringData(key), len(key)))
	return heightFromHash(&h)
}

// heightFromHash counts leading zero 2-bit pairs in a 32-byte hash.
// Exported via HeightForKey; split out for direct testing with synthetic hashes.
func heightFromHash(h *[32]byte) uint8 {
	// Count leading zero 2-bit pairs using CLZ on 64-bit words.
	// Each byte has 4 two-bit pairs, so after i zero bytes the base count is i*4.
	for i := 0; i < 32; i += 8 {
		word := uint64(h[i])<<56 | uint64(h[i+1])<<48 | uint64(h[i+2])<<40 | uint64(h[i+3])<<32 |
			uint64(h[i+4])<<24 | uint64(h[i+5])<<16 | uint64(h[i+6])<<8 | uint64(h[i+7])
		if word == 0 {
			continue
		}
		return uint8(i*4 + bits.LeadingZeros64(word)/2)
	}
	return 128 // all zeros
}
