package cbor

import "errors"

var (
	errVarintTooLong    = errors.New("varint too long")
	errVarintTruncated  = errors.New("varint truncated")
	errVarintNonMinimal = errors.New("non-minimal varint encoding")
)

// AppendUvarint appends an unsigned varint to buf.
func AppendUvarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// ReadUvarint reads an unsigned varint from buf, returning the value and bytes consumed.
func ReadUvarint(buf []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i >= 10 {
			return 0, 0, errVarintTooLong
		}
		if b < 0x80 {
			x |= uint64(b) << s
			return x, i + 1, nil
		}
		x |= uint64(b&0x7F) << s
		s += 7
	}
	return 0, 0, errVarintTruncated
}

// ReadUvarintMinimal reads an unsigned varint that must be canonically (minimally)
// encoded, as required for the components of a CID. A multi-byte encoding whose
// final group is zero — i.e. the value could have fit in fewer bytes — is
// rejected. It also caps the length at 9 bytes (63 bits) per the multiformats
// unsigned-varint spec.
func ReadUvarintMinimal(buf []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i >= 9 {
			return 0, 0, errVarintTooLong
		}
		if b < 0x80 {
			if i > 0 && b == 0 {
				return 0, 0, errVarintNonMinimal
			}
			x |= uint64(b) << s
			return x, i + 1, nil
		}
		x |= uint64(b&0x7F) << s
		s += 7
	}
	return 0, 0, errVarintTruncated
}
