package cbor

import "math"

// AppendNull appends a CBOR null (0xf6) to buf.
func AppendNull(buf []byte) []byte {
	return append(buf, 0xf6)
}

// AppendBool appends a CBOR boolean.
func AppendBool(buf []byte, v bool) []byte {
	if v {
		return append(buf, 0xf5) // true
	}
	return append(buf, 0xf4) // false
}

// AppendUint appends a minimal CBOR unsigned integer (major type 0).
func AppendUint(buf []byte, v uint64) []byte {
	return appendHeader(buf, 0, v)
}

// AppendInt appends a CBOR integer (signed). Positive values use major type 0,
// negative values use major type 1.
func AppendInt(buf []byte, v int64) []byte {
	if v >= 0 {
		return appendHeader(buf, 0, uint64(v))
	}
	return appendHeader(buf, 1, uint64(-1-v))
}

// AppendFloat64 appends a CBOR float64 (major type 7, additional info 27).
// Panics on NaN or Infinity, which are not allowed in DAG-CBOR.
func AppendFloat64(buf []byte, v float64) []byte {
	if math.IsNaN(v) {
		panic("cbor: NaN not allowed in DAG-CBOR")
	}
	if math.IsInf(v, 0) {
		panic("cbor: Infinity not allowed in DAG-CBOR")
	}
	bits := math.Float64bits(v)
	return append(buf, 0xfb,
		byte(bits>>56), byte(bits>>48), byte(bits>>40), byte(bits>>32),
		byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))
}

// AppendText appends a CBOR text string (major type 3).
// The fast path (len < 24) is inlinable and covers all ATProto field names
// and most short string values.
func AppendText(buf []byte, s string) []byte {
	if len(s) < 24 {
		buf = append(buf, 0x60|byte(len(s)))
		return append(buf, s...)
	}
	return appendTextLong(buf, s)
}

func appendTextLong(buf []byte, s string) []byte {
	n := uint64(len(s))
	if n <= 0xFF {
		buf = append(buf, 0x78, byte(n))
	} else {
		buf = appendHeaderSlow(buf, 3, n)
	}
	return append(buf, s...)
}

// AppendBytes appends a CBOR byte string (major type 2).
// The fast path (len < 24) is inlinable.
func AppendBytes(buf []byte, data []byte) []byte {
	if len(data) < 24 {
		buf = append(buf, 0x40|byte(len(data)))
		return append(buf, data...)
	}
	return appendBytesLong(buf, data)
}

func appendBytesLong(buf []byte, data []byte) []byte {
	n := uint64(len(data))
	if n <= 0xFF {
		buf = append(buf, 0x58, byte(n))
	} else {
		buf = appendHeaderSlow(buf, 2, n)
	}
	return append(buf, data...)
}

// AppendBytesHeader appends a CBOR byte string header (major type 2).
func AppendBytesHeader(buf []byte, length uint64) []byte {
	return appendHeader(buf, 2, length)
}

// AppendArrayHeader appends a CBOR array header (major type 4).
// Inlines the fast path for arrays with 0-23 elements.
func AppendArrayHeader(buf []byte, length uint64) []byte {
	if length < 24 {
		return append(buf, 0x80|byte(length))
	}
	return appendHeaderSlow(buf, 4, length)
}

// AppendMapHeader appends a CBOR map header (major type 5).
// Inlines the fast path for maps with 0-23 entries.
func AppendMapHeader(buf []byte, length uint64) []byte {
	if length < 24 {
		return append(buf, 0xa0|byte(length))
	}
	return appendHeaderSlow(buf, 5, length)
}

// AppendCIDLink appends a DAG-CBOR CID link (tag 42 + byte string with 0x00 prefix + CID bytes).
// The encoding is always 41 bytes: tag42(2) + bytesHeader(2) + prefix(1) + CID(36).
// All fixed bytes are inlined in a single append to minimize overhead.
func AppendCIDLink(buf []byte, c *CID) []byte {
	// 0xd8, 0x2a = tag 42
	// 0x58, 0x25 = byte string of length 37 (1 prefix + 36 CID)
	// 0x00       = CID multibase prefix
	// 0x01       = CID version 1
	// c.codec    = codec (0x71=dag-cbor, 0x55=raw)
	// 0x12       = SHA-256 hash code
	// 0x20       = hash length 32
	buf = append(buf, 0xd8, 0x2a, 0x58, 0x25, 0x00, 0x01, c.codec, 0x12, 0x20)
	return append(buf, c.hash[:]...)
}

// CIDByteLen returns the byte length of a CID's binary encoding without allocating.
// This is always 36: version(1) + codec(1) + hashCode(1) + hashLen(1) + hash(32).
func CIDByteLen(c *CID) int {
	return 36
}

// AppendTextKey returns the precomputed CBOR bytes for a text string key.
// This is a semantic alias for AppendText, used at init time to build
// precomputed key token variables for struct field names. The alias makes
// the intent clear at call sites (e.g. `cborKey_X = cbor.AppendTextKey(nil, "x")`).
func AppendTextKey(buf []byte, s string) []byte {
	return AppendText(buf, s)
}

// CompareCBORKeys compares two string keys by DAG-CBOR sort order
// (shorter CBOR encoding first, then lexicographic).
func CompareCBORKeys(a, b string) int {
	return compareCBORKeys(a, b)
}

// appendHeader appends a CBOR header with the given major type and argument
// using minimal encoding. The fast path (val < 24) is inlinable.
func appendHeader(buf []byte, major byte, val uint64) []byte {
	if val < 24 {
		return append(buf, (major<<5)|byte(val))
	}
	return appendHeaderSlow(buf, major, val)
}

// appendHeaderSlow handles the multi-byte CBOR header cases.
func appendHeaderSlow(buf []byte, major byte, val uint64) []byte {
	m := major << 5
	if val <= 0xFF {
		return append(buf, m|24, byte(val))
	}
	if val <= 0xFFFF {
		return append(buf, m|25, byte(val>>8), byte(val))
	}
	if val <= 0xFFFFFFFF {
		return append(buf, m|26, byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
	}
	return append(buf, m|27, byte(val>>56), byte(val>>48), byte(val>>40), byte(val>>32),
		byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
}
