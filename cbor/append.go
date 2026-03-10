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
func AppendText(buf []byte, s string) []byte {
	buf = appendHeader(buf, 3, uint64(len(s)))
	return append(buf, s...)
}

// AppendBytes appends a CBOR byte string (major type 2).
func AppendBytes(buf []byte, data []byte) []byte {
	buf = appendHeader(buf, 2, uint64(len(data)))
	return append(buf, data...)
}

// AppendBytesHeader appends a CBOR byte string header (major type 2).
func AppendBytesHeader(buf []byte, length uint64) []byte {
	return appendHeader(buf, 2, length)
}

// AppendArrayHeader appends a CBOR array header (major type 4).
func AppendArrayHeader(buf []byte, length uint64) []byte {
	return appendHeader(buf, 4, length)
}

// AppendMapHeader appends a CBOR map header (major type 5).
func AppendMapHeader(buf []byte, length uint64) []byte {
	return appendHeader(buf, 5, length)
}

// AppendCIDLink appends a DAG-CBOR CID link (tag 42 + byte string with 0x00 prefix + CID bytes).
func AppendCIDLink(buf []byte, c *CID) []byte {
	cidLen := CIDByteLen(c)
	// Tag 42: 0xd8, 0x2a
	buf = append(buf, 0xd8, 0x2a)
	buf = AppendBytesHeader(buf, uint64(1+cidLen))
	buf = append(buf, 0x00) // CID prefix byte
	return c.AppendBytes(buf)
}

// CIDByteLen returns the byte length of a CID's binary encoding without allocating.
func CIDByteLen(c *CID) int {
	return 32 + uvarintLen(1) + uvarintLen(uint64(c.codec)) +
		uvarintLen(HashSHA256) + uvarintLen(32)
}

// uvarintLen returns the number of bytes needed to encode v as an unsigned varint.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		n++
		v >>= 7
	}
	return n
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
