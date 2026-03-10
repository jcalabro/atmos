package cbor

import (
	"fmt"
	"math"
	"unicode/utf8"
	"unsafe"
)

// ReadHeader reads a CBOR item header at position pos, returning the major type,
// the argument value, and the new position. Validates minimal encoding.
// The fast path (info < 24, non-major-7) is inlinable.
func ReadHeader(data []byte, pos int) (major byte, val uint64, newPos int, err error) {
	if pos >= len(data) {
		return 0, 0, 0, errUnexpectedEnd
	}
	b := data[pos]
	if b&0x1f < 24 && b>>5 != 7 {
		return b >> 5, uint64(b & 0x1f), pos + 1, nil
	}
	return readHeaderSlow(data, pos, b)
}

func readHeaderSlow(data []byte, pos int, b byte) (major byte, val uint64, newPos int, err error) {
	major = b >> 5
	info := b & 0x1f

	if major == 7 { // simple/float
		switch {
		case info < 24:
			return major, uint64(info), pos + 1, nil
		case info == 24:
			if pos+1 >= len(data) {
				return 0, 0, 0, errTruncated
			}
			return major, uint64(data[pos+1]), pos + 2, nil
		case info == 27:
			if pos+8 >= len(data) {
				return 0, 0, 0, errTruncated
			}
			v := uint64(data[pos+1])<<56 | uint64(data[pos+2])<<48 |
				uint64(data[pos+3])<<40 | uint64(data[pos+4])<<32 |
				uint64(data[pos+5])<<24 | uint64(data[pos+6])<<16 |
				uint64(data[pos+7])<<8 | uint64(data[pos+8])
			return major, v, pos + 9, nil
		default:
			return 0, 0, 0, fmt.Errorf("cbor: unsupported simple value info %d", info)
		}
	}

	// For all other major types, validate minimal encoding (DAG-CBOR requirement).
	switch info {
	case 24:
		if pos+1 >= len(data) {
			return 0, 0, 0, errTruncated
		}
		v := uint64(data[pos+1])
		if v < 24 {
			return 0, 0, 0, errNonMinimal
		}
		return major, v, pos + 2, nil
	case 25:
		if pos+2 >= len(data) {
			return 0, 0, 0, errTruncated
		}
		v := uint64(data[pos+1])<<8 | uint64(data[pos+2])
		if v <= 0xFF {
			return 0, 0, 0, errNonMinimal
		}
		return major, v, pos + 3, nil
	case 26:
		if pos+4 >= len(data) {
			return 0, 0, 0, errTruncated
		}
		v := uint64(data[pos+1])<<24 | uint64(data[pos+2])<<16 |
			uint64(data[pos+3])<<8 | uint64(data[pos+4])
		if v <= 0xFFFF {
			return 0, 0, 0, errNonMinimal
		}
		return major, v, pos + 5, nil
	case 27:
		if pos+8 >= len(data) {
			return 0, 0, 0, errTruncated
		}
		v := uint64(data[pos+1])<<56 | uint64(data[pos+2])<<48 |
			uint64(data[pos+3])<<40 | uint64(data[pos+4])<<32 |
			uint64(data[pos+5])<<24 | uint64(data[pos+6])<<16 |
			uint64(data[pos+7])<<8 | uint64(data[pos+8])
		if v <= 0xFFFFFFFF {
			return 0, 0, 0, errNonMinimal
		}
		return major, v, pos + 9, nil
	default:
		return 0, 0, 0, fmt.Errorf("cbor: unsupported additional info %d", info)
	}
}

var (
	errUnexpectedEnd = fmt.Errorf("cbor: unexpected end of data")
	errTruncated     = fmt.Errorf("cbor: truncated")
	errNonMinimal    = fmt.Errorf("cbor: non-minimal integer encoding")
)

// ReadMapHeader reads a CBOR map header at position pos, returns count and new position.
func ReadMapHeader(data []byte, pos int) (uint64, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return 0, 0, err
	}
	if major != 5 {
		return 0, 0, fmt.Errorf("cbor: expected map (major 5), got major %d", major)
	}
	return val, newPos, nil
}

// ReadArrayHeader reads a CBOR array header at position pos.
func ReadArrayHeader(data []byte, pos int) (uint64, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return 0, 0, err
	}
	if major != 4 {
		return 0, 0, fmt.Errorf("cbor: expected array (major 4), got major %d", major)
	}
	return val, newPos, nil
}

// ReadText reads a CBOR text string at position pos.
func ReadText(data []byte, pos int) (string, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return "", 0, err
	}
	if major != 3 {
		return "", 0, fmt.Errorf("cbor: expected text (major 3), got major %d", major)
	}
	if val > uint64(len(data)-newPos) {
		return "", 0, fmt.Errorf("cbor: text data truncated")
	}
	n := int(val)
	end := newPos + n
	if n == 0 {
		return "", end, nil
	}
	s := unsafe.String(&data[newPos], n)
	if !utf8.ValidString(s) {
		return "", 0, fmt.Errorf("cbor: text string contains invalid UTF-8")
	}
	return s, end, nil
}

// ReadBytes reads a CBOR byte string at position pos, returning a copy of the bytes.
func ReadBytes(data []byte, pos int) ([]byte, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return nil, 0, err
	}
	if major != 2 {
		return nil, 0, fmt.Errorf("cbor: expected bytes (major 2), got major %d", major)
	}
	if val > uint64(len(data)-newPos) {
		return nil, 0, fmt.Errorf("cbor: bytes data truncated")
	}
	end := newPos + int(val)
	out := make([]byte, val)
	copy(out, data[newPos:end])
	return out, end, nil
}

// ReadBytesNoCopy reads a CBOR byte string at position pos, returning a sub-slice
// of data without copying. The caller must not modify the returned slice.
func ReadBytesNoCopy(data []byte, pos int) ([]byte, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return nil, 0, err
	}
	if major != 2 {
		return nil, 0, fmt.Errorf("cbor: expected bytes (major 2), got major %d", major)
	}
	if val > uint64(len(data)-newPos) {
		return nil, 0, fmt.Errorf("cbor: bytes data truncated")
	}
	end := newPos + int(val)
	return data[newPos:end], end, nil
}

// ReadUint reads a CBOR unsigned integer at position pos.
func ReadUint(data []byte, pos int) (uint64, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return 0, 0, err
	}
	if major != 0 {
		return 0, 0, fmt.Errorf("cbor: expected uint (major 0), got major %d", major)
	}
	return val, newPos, nil
}

// ReadInt reads a CBOR integer (signed) at position pos.
func ReadInt(data []byte, pos int) (int64, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return 0, 0, err
	}
	switch major {
	case 0: // unsigned
		if val > math.MaxInt64 {
			return 0, 0, fmt.Errorf("cbor: uint %d overflows int64", val)
		}
		return int64(val), newPos, nil
	case 1: // negative
		if val > math.MaxInt64 {
			return 0, 0, fmt.Errorf("cbor: negative int overflow")
		}
		return -1 - int64(val), newPos, nil
	default:
		return 0, 0, fmt.Errorf("cbor: expected int (major 0 or 1), got major %d", major)
	}
}

// ReadBool reads a CBOR boolean at position pos.
func ReadBool(data []byte, pos int) (bool, int, error) {
	if pos >= len(data) {
		return false, 0, fmt.Errorf("cbor: unexpected end of data")
	}
	switch data[pos] {
	case 0xf5:
		return true, pos + 1, nil
	case 0xf4:
		return false, pos + 1, nil
	default:
		return false, 0, fmt.Errorf("cbor: expected boolean, got 0x%02x", data[pos])
	}
}

// ReadFloat64 reads a CBOR float64 at position pos.
// Only accepts float64 (additional info 27, 0xfb prefix). Rejects NaN and Infinity
// per DAG-CBOR rules.
func ReadFloat64(data []byte, pos int) (float64, int, error) {
	if pos >= len(data) {
		return 0, 0, fmt.Errorf("cbor: unexpected end of data")
	}
	if data[pos] != 0xfb { // major 7, additional info 27 (float64)
		return 0, 0, fmt.Errorf("cbor: expected float64 (0xfb), got 0x%02x", data[pos])
	}
	if pos+8 >= len(data) {
		return 0, 0, fmt.Errorf("cbor: truncated float64")
	}
	bits := uint64(data[pos+1])<<56 | uint64(data[pos+2])<<48 |
		uint64(data[pos+3])<<40 | uint64(data[pos+4])<<32 |
		uint64(data[pos+5])<<24 | uint64(data[pos+6])<<16 |
		uint64(data[pos+7])<<8 | uint64(data[pos+8])
	f := math.Float64frombits(bits)
	if math.IsNaN(f) {
		return 0, 0, fmt.Errorf("cbor: NaN not allowed in DAG-CBOR")
	}
	if math.IsInf(f, 0) {
		return 0, 0, fmt.Errorf("cbor: Infinity not allowed in DAG-CBOR")
	}
	return f, pos + 9, nil
}

// ReadCIDLink reads a DAG-CBOR CID link (tag 42) at position pos.
func ReadCIDLink(data []byte, pos int) (CID, int, error) {
	// Tag 42: 0xd8 0x2a
	if pos+1 >= len(data) || data[pos] != 0xd8 || data[pos+1] != 0x2a {
		return CID{}, 0, fmt.Errorf("cbor: expected tag 42 at pos %d", pos)
	}
	pos += 2

	// Byte string header.
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return CID{}, 0, err
	}
	if major != 2 {
		return CID{}, 0, fmt.Errorf("cbor: expected bytes in CID link")
	}
	n := int(val)
	end := newPos + n
	if end > len(data) {
		return CID{}, 0, fmt.Errorf("cbor: CID link data truncated")
	}

	// First byte must be 0x00 (CID prefix).
	if n < 2 || data[newPos] != 0x00 {
		return CID{}, 0, fmt.Errorf("cbor: invalid CID link prefix")
	}

	cid, _, err := ParseCIDPrefix(data[newPos+1 : end])
	if err != nil {
		return CID{}, 0, err
	}
	return cid, end, nil
}

// ReadNull checks for a CBOR null at position pos and returns the new position.
func ReadNull(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return 0, fmt.Errorf("cbor: unexpected end of data")
	}
	if data[pos] != 0xf6 {
		return 0, fmt.Errorf("cbor: expected null (0xf6), got 0x%02x", data[pos])
	}
	return pos + 1, nil
}

// IsNull returns true if the byte at pos is CBOR null (0xf6).
func IsNull(data []byte, pos int) bool {
	return pos < len(data) && data[pos] == 0xf6
}

// SkipValue skips one complete CBOR value at position pos, returns new position.
func SkipValue(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return 0, fmt.Errorf("cbor: unexpected end of data")
	}

	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return 0, err
	}

	switch major {
	case 0, 1: // uint, negint
		return newPos, nil
	case 2, 3: // bytes, text
		if val > uint64(len(data)-newPos) {
			return 0, fmt.Errorf("cbor: data truncated")
		}
		return newPos + int(val), nil
	case 4: // array
		p := newPos
		for range val {
			p, err = SkipValue(data, p)
			if err != nil {
				return 0, err
			}
		}
		return p, nil
	case 5: // map
		p := newPos
		for range val {
			// skip key
			p, err = SkipValue(data, p)
			if err != nil {
				return 0, err
			}
			// skip value
			p, err = SkipValue(data, p)
			if err != nil {
				return 0, err
			}
		}
		return p, nil
	case 6: // tag
		return SkipValue(data, newPos)
	case 7: // simple/float
		return newPos, nil
	default:
		return 0, fmt.Errorf("cbor: unknown major type %d", major)
	}
}

// PeekType reads a CBOR map, finds the "$type" key, and returns its string value.
// The map position is not advanced — this is meant for peeking into union data.
// Uses direct byte comparison to avoid allocating strings for non-matching keys.
func PeekType(data []byte) (string, error) {
	return PeekTypeAt(data, 0)
}

// PeekTypeAt reads a CBOR map starting at pos, finds the "$type" key, and
// returns its string value. Like PeekType but works at an arbitrary offset.
func PeekTypeAt(data []byte, pos int) (string, error) {
	count, pos, err := ReadMapHeader(data, pos)
	if err != nil {
		return "", err
	}
	for range count {
		// Read key header to get position and length without allocating a string.
		major, val, newPos, err := ReadHeader(data, pos)
		if err != nil {
			return "", err
		}
		if major != 3 {
			return "", fmt.Errorf("cbor: expected text key (major 3), got major %d", major)
		}
		if val > uint64(len(data)-newPos) {
			return "", fmt.Errorf("cbor: text data truncated")
		}
		keyEnd := newPos + int(val)
		// Compare key bytes directly: "$type" = [0x24, 0x74, 0x79, 0x70, 0x65].
		// Go optimizes string(b) == "literal" to avoid allocation.
		if string(data[newPos:keyEnd]) == "$type" {
			typ, _, err := ReadText(data, keyEnd)
			return typ, err
		}
		pos, err = SkipValue(data, keyEnd)
		if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("cbor: no $type field in map")
}
