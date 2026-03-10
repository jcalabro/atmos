package crypto

import (
	"errors"
	"math/big"
)

// encodeCompactSig encodes r, s as a 64-byte [R || S] signature.
func encodeCompactSig(r, s *big.Int) []byte {
	sig := make([]byte, 64)
	rBytes := r.Bytes()
	sBytes := s.Bytes()
	copy(sig[32-len(rBytes):32], rBytes)
	copy(sig[64-len(sBytes):64], sBytes)
	return sig
}

// decodeCompactSig decodes a 64-byte [R || S] signature.
func decodeCompactSig(sig []byte) (r, s *big.Int, err error) {
	if len(sig) != 64 {
		return nil, nil, errors.New("crypto: signature must be 64 bytes")
	}
	r = new(big.Int).SetBytes(sig[:32])
	s = new(big.Int).SetBytes(sig[32:])
	return r, s, nil
}

// isLowS returns true if s <= n/2.
func isLowS(halfOrder, s *big.Int) bool {
	return s.Cmp(halfOrder) <= 0
}

// normalizeLowS ensures s is in the low-S range. If s > n/2, replace with n - s.
func normalizeLowS(order, halfOrder, s *big.Int) *big.Int {
	if isLowS(halfOrder, s) {
		return s
	}
	return new(big.Int).Sub(order, s)
}
