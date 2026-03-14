package cbor

import (
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
)

// Multicodec constants for CID codec and hash function identifiers.
const (
	CodecDagCBOR uint64 = 0x71 // DAG-CBOR content codec
	CodecRaw     uint64 = 0x55 // Raw binary content codec
	HashSHA256   uint64 = 0x12 // SHA-256 multihash function code
)

var base32Lower = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// CID represents a Content Identifier (CIDv1).
// Only SHA-256 hashes with dag-cbor or raw codecs are supported.
// 33 bytes: 32-byte hash + 1-byte codec.
type CID struct {
	hash  [32]byte // SHA-256 hash
	codec uint8    // 0=undefined, 0x55=raw, 0x71=dag-cbor
}

// Version returns the CID version (always 1 for defined CIDs, 0 for undefined).
func (c CID) Version() uint64 {
	if c.codec == 0 {
		return 0
	}
	return 1
}

// Codec returns the content codec (e.g. CodecDagCBOR, CodecRaw).
func (c CID) Codec() uint64 {
	return uint64(c.codec)
}

// HashCode returns the multihash function code (always HashSHA256 for defined CIDs).
func (c CID) HashCode() uint64 {
	if c.codec == 0 {
		return 0
	}
	return HashSHA256
}

// HashLen returns the hash length (always 32 for defined CIDs).
func (c CID) HashLen() int {
	if c.codec == 0 {
		return 0
	}
	return 32
}

// Hash returns the SHA-256 hash bytes.
func (c CID) Hash() [32]byte {
	return c.hash
}

// ComputeCID computes a CIDv1 from data using SHA-256 and the given codec.
func ComputeCID(codec uint64, data []byte) CID {
	return CID{
		codec: uint8(codec),
		hash:  sha256.Sum256(data),
	}
}

// ParseCIDBytes parses a CID from raw bytes (no multibase prefix).
func ParseCIDBytes(buf []byte) (CID, error) {
	if len(buf) == 0 {
		return CID{}, errors.New("empty CID bytes")
	}

	// CIDv0: starts with 0x12 0x20 (sha2-256 multihash, 32 bytes).
	if buf[0] == 0x12 && len(buf) == 34 && buf[1] == 0x20 {
		return CID{}, errors.New("CIDv0 not supported")
	}

	var n int
	var err error

	version, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, fmt.Errorf("CID version: %w", err)
	}
	buf = buf[n:]
	if version != 1 {
		return CID{}, fmt.Errorf("unsupported CID version %d", version)
	}

	codec, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, fmt.Errorf("CID codec: %w", err)
	}
	buf = buf[n:]
	if codec != CodecDagCBOR && codec != CodecRaw {
		return CID{}, fmt.Errorf("unsupported CID codec 0x%x (only dag-cbor and raw allowed)", codec)
	}

	hashCode, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, fmt.Errorf("CID hash code: %w", err)
	}
	buf = buf[n:]
	if hashCode != HashSHA256 {
		return CID{}, fmt.Errorf("unsupported CID hash 0x%x (only SHA-256 allowed)", hashCode)
	}

	hashLen, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, fmt.Errorf("CID hash length: %w", err)
	}
	buf = buf[n:]
	if hashLen != 32 {
		return CID{}, fmt.Errorf("CID hash length must be 32, got %d", hashLen)
	}

	if len(buf) < 32 {
		return CID{}, fmt.Errorf("CID hash truncated: expected 32 bytes, got %d", len(buf))
	}
	if len(buf) > 32 {
		return CID{}, fmt.Errorf("CID has %d trailing bytes", len(buf)-32)
	}

	var c CID
	c.codec = uint8(codec)
	copy(c.hash[:], buf[:32])

	return c, nil
}

// ParseCIDString parses a CID from a multibase string (expects 'b' prefix for base32lower).
func ParseCIDString(s string) (CID, error) {
	if len(s) == 0 {
		return CID{}, errors.New("empty CID string")
	}
	if s[0] != 'b' {
		return CID{}, fmt.Errorf("unsupported multibase prefix %q, expected 'b'", s[0])
	}
	raw, err := base32Lower.DecodeString(s[1:])
	if err != nil {
		return CID{}, fmt.Errorf("base32 decode: %w", err)
	}
	return ParseCIDBytes(raw)
}

// ParseCIDPrefix parses a CID from the front of buf and returns the CID and the
// number of bytes consumed. Unlike ParseCIDBytes, this does not reject trailing bytes.
func ParseCIDPrefix(buf []byte) (CID, int, error) {
	if len(buf) == 0 {
		return CID{}, 0, errors.New("empty CID bytes")
	}

	// Fast path: CIDv1 with dag-cbor (0x71) + SHA-256 (0x12, 0x20)
	// Prefix bytes: 0x01, 0x71, 0x12, 0x20 followed by 32 hash bytes = 36 total.
	if len(buf) >= 36 && buf[0] == 0x01 && buf[1] == 0x71 && buf[2] == 0x12 && buf[3] == 0x20 {
		var c CID
		c.codec = uint8(CodecDagCBOR)
		copy(c.hash[:], buf[4:36])
		return c, 36, nil
	}

	var n, total int
	var err error

	version, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, 0, fmt.Errorf("CID version: %w", err)
	}
	buf = buf[n:]
	total += n
	if version != 1 {
		return CID{}, 0, fmt.Errorf("unsupported CID version %d", version)
	}

	codec, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, 0, fmt.Errorf("CID codec: %w", err)
	}
	buf = buf[n:]
	total += n
	if codec != CodecDagCBOR && codec != CodecRaw {
		return CID{}, 0, fmt.Errorf("unsupported CID codec 0x%x (only dag-cbor and raw allowed)", codec)
	}

	hashCode, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, 0, fmt.Errorf("CID hash code: %w", err)
	}
	buf = buf[n:]
	total += n
	if hashCode != HashSHA256 {
		return CID{}, 0, fmt.Errorf("unsupported CID hash 0x%x (only SHA-256 allowed)", hashCode)
	}

	hashLen, n, err := ReadUvarint(buf)
	if err != nil {
		return CID{}, 0, fmt.Errorf("CID hash length: %w", err)
	}
	buf = buf[n:]
	total += n
	if hashLen != 32 {
		return CID{}, 0, fmt.Errorf("CID hash length must be 32, got %d", hashLen)
	}

	if len(buf) < 32 {
		return CID{}, 0, fmt.Errorf("CID hash truncated: expected 32 bytes, got %d", len(buf))
	}

	var c CID
	c.codec = uint8(codec)
	copy(c.hash[:], buf[:32])
	total += 32

	return c, total, nil
}

// Bytes returns the raw binary CID (version varint + codec varint + multihash).
func (c CID) Bytes() []byte {
	buf := make([]byte, 0, 36)
	return c.AppendBytes(buf)
}

// AppendBytes appends the raw binary CID to buf and returns the extended buffer.
// All varint values (version=1, codec, hash_code=0x12, hash_len=32) are single-byte,
// so we inline them directly instead of calling AppendUvarint 4 times.
func (c CID) AppendBytes(buf []byte) []byte {
	buf = append(buf, 0x01, c.codec, 0x12, 0x20)
	return append(buf, c.hash[:]...)
}

// String returns the base32lower multibase string representation (b-prefixed).
func (c CID) String() string {
	return "b" + base32Lower.EncodeToString(c.Bytes())
}

// Defined returns true if this CID has been set.
func (c CID) Defined() bool {
	return c.codec != 0
}

// Equal returns true if two CIDs are identical.
func (c CID) Equal(other CID) bool {
	return c == other
}
