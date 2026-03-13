package cbor

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeNull(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0xf6})
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestDecodeBool(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0xf5})
	require.NoError(t, err)
	require.Equal(t, true, v)

	v, err = Unmarshal([]byte{0xf4})
	require.NoError(t, err)
	require.Equal(t, false, v)
}

func TestDecodeInt_SmallPositive(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0x00})
	require.NoError(t, err)
	require.Equal(t, int64(0), v)

	v, err = Unmarshal([]byte{0x17})
	require.NoError(t, err)
	require.Equal(t, int64(23), v)
}

func TestDecodeInt_OneByte(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0x18, 0x18})
	require.NoError(t, err)
	require.Equal(t, int64(24), v)
}

func TestDecodeInt_Negative(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0x20})
	require.NoError(t, err)
	require.Equal(t, int64(-1), v)

	v, err = Unmarshal([]byte{0x38, 0x18})
	require.NoError(t, err)
	require.Equal(t, int64(-25), v)
}

func TestDecodeString(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0x60})
	require.NoError(t, err)
	require.Equal(t, "", v)

	v, err = Unmarshal(append([]byte{0x65}, "hello"...))
	require.NoError(t, err)
	require.Equal(t, "hello", v)
}

func TestDecodeBytes(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0x42, 0xDE, 0xAD})
	require.NoError(t, err)
	require.Equal(t, []byte{0xDE, 0xAD}, v)
}

func TestDecodeArray(t *testing.T) {
	t.Parallel()
	v, err := Unmarshal([]byte{0x83, 0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), int64(2), int64(3)}, v)
}

func TestDecodeMap(t *testing.T) {
	t.Parallel()
	data := []byte{
		0xa2,       // map of 2
		0x61, 0x61, // "a"
		0x01,       // 1
		0x61, 0x62, // "b"
		0x02, // 2
	}
	v, err := Unmarshal(data)
	require.NoError(t, err)
	m, ok := v.(map[string]any)
	require.True(t, ok)
	require.Equal(t, int64(1), m["a"])
	require.Equal(t, int64(2), m["b"])
}

func TestDecodeFloat64(t *testing.T) {
	t.Parallel()
	// Encode 1.5 as float64.
	b, err := Marshal(float64(1.5))
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, 1.5, v)
}

func TestDecode_RejectNonMinimalEncoding(t *testing.T) {
	t.Parallel()
	// Integer 0 encoded as 1-byte additional (should be just 0x00).
	_, err := Unmarshal([]byte{0x18, 0x00})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-minimal")
}

func TestDecode_RejectIndefiniteLength(t *testing.T) {
	t.Parallel()
	// Indefinite-length array (0x9f).
	_, err := Unmarshal([]byte{0x9f, 0x01, 0xff})
	require.Error(t, err)
	require.Contains(t, err.Error(), "indefinite")
}

func TestDecode_RejectUnsortedMapKeys(t *testing.T) {
	t.Parallel()
	data := []byte{
		0xa2,       // map of 2
		0x61, 0x62, // "b"
		0x02,
		0x61, 0x61, // "a" — wrong order
		0x01,
	}
	_, err := Unmarshal(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sorted")
}

func TestDecode_RejectDuplicateMapKeys(t *testing.T) {
	t.Parallel()
	data := []byte{
		0xa2,       // map of 2
		0x61, 0x61, // "a"
		0x01,
		0x61, 0x61, // "a" again
		0x02,
	}
	_, err := Unmarshal(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestDecode_RejectNonStringMapKeys(t *testing.T) {
	t.Parallel()
	data := []byte{
		0xa1, // map of 1
		0x01, // integer key
		0x02,
	}
	_, err := Unmarshal(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "text string")
}

func TestDecode_RejectTrailingBytes(t *testing.T) {
	t.Parallel()
	_, err := Unmarshal([]byte{0xf6, 0xf6}) // two nulls
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing")
}

func TestDecode_RejectEmptyInput(t *testing.T) {
	t.Parallel()
	_, err := Unmarshal([]byte{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestDecode_RejectFloat16(t *testing.T) {
	t.Parallel()
	// float16 (major 7, additional 25)
	_, err := Unmarshal([]byte{0xf9, 0x3c, 0x00})
	require.Error(t, err)
	require.Contains(t, err.Error(), "float16")
}

func TestDecode_RejectFloat32(t *testing.T) {
	t.Parallel()
	// float32 (major 7, additional 26)
	_, err := Unmarshal([]byte{0xfa, 0x3f, 0xc0, 0x00, 0x00})
	require.Error(t, err)
	require.Contains(t, err.Error(), "float32")
}

func TestDecode_RejectNaN(t *testing.T) {
	t.Parallel()
	// float64 NaN
	b, _ := Marshal(float64(0)) // get a valid float64 template
	// Replace with NaN bits.
	bits := math.Float64bits(math.NaN())
	b[1] = byte(bits >> 56)
	b[2] = byte(bits >> 48)
	b[3] = byte(bits >> 40)
	b[4] = byte(bits >> 32)
	b[5] = byte(bits >> 24)
	b[6] = byte(bits >> 16)
	b[7] = byte(bits >> 8)
	b[8] = byte(bits)
	_, err := Unmarshal(b)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NaN")
}

func TestDecode_RejectUnsupportedTag(t *testing.T) {
	t.Parallel()
	// Tag 1 (epoch datetime) — not allowed in DAG-CBOR.
	_, err := Unmarshal([]byte{0xc1, 0x00})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported tag")
}

func TestDecode_RejectBreakStopCode(t *testing.T) {
	t.Parallel()
	// Break stop code (0xff) — major 7, additional 31.
	_, err := Unmarshal([]byte{0xff})
	require.Error(t, err)
	require.Contains(t, err.Error(), "break")
}

func TestDecode_RejectHugeArrayAllocation(t *testing.T) {
	t.Parallel()
	// CBOR array header claiming 1 billion items: major 4, additional 27 (8-byte), value 1_000_000_000.
	var buf [9]byte
	buf[0] = (majorArray << 5) | 27
	binary.BigEndian.PutUint64(buf[1:], 0x1_0000_0000) // 4294967296, requires 8-byte encoding
	_, err := Unmarshal(buf[:])
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max size")
}

func TestDecode_RejectDeepNesting(t *testing.T) {
	t.Parallel()
	// Build deeply nested arrays: each byte 0x81 is a 1-element array.
	depth := MaxDepth + 10
	data := make([]byte, depth+1)
	for i := 0; i < depth; i++ {
		data[i] = 0x81 // array of 1
	}
	data[depth] = 0x00 // innermost value: integer 0
	_, err := Unmarshal(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "max nesting depth")
}

func TestDecode_RejectUint64OverMaxInt64(t *testing.T) {
	t.Parallel()
	// Encode a uint that is MaxInt64 + 1.
	var buf [9]byte
	buf[0] = (majorUint << 5) | 27
	binary.BigEndian.PutUint64(buf[1:], math.MaxInt64+1)
	_, err := Unmarshal(buf[:])
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsigned integer exceeds int64 range")
}

func TestDecode_RejectInvalidUTF8MapKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "0xFF byte in map key",
			data: []byte{
				0xa1,                   // map of 1
				0x63, 0x61, 0xFF, 0x62, // text(3) "a\xffb" — invalid UTF-8
				0x01, // value: 1
			},
		},
		{
			name: "truncated multi-byte sequence in map key",
			data: []byte{
				0xa1,             // map of 1
				0x62, 0xC0, 0x41, // text(2) "\xc0A" — overlong encoding
				0x01,
			},
		},
		{
			name: "lone continuation byte in map key",
			data: []byte{
				0xa1,             // map of 1
				0x62, 0x80, 0x41, // text(2) "\x80A" — starts with continuation byte
				0x01,
			},
		},
		{
			name: "surrogate half in map key",
			data: []byte{
				0xa1,                   // map of 1
				0x63, 0xED, 0xA0, 0x80, // text(3) U+D800 — surrogate
				0x01,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Unmarshal(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid UTF-8")
		})
	}
}

func TestDecode_ValidUTF8MapKeyAccepted(t *testing.T) {
	t.Parallel()
	// Map with valid UTF-8 key containing multi-byte characters.
	// Key: "café" = 63 61 66 c3 a9 (5 bytes, valid UTF-8)
	data := []byte{
		0xa1,                               // map of 1
		0x65, 0x63, 0x61, 0x66, 0xc3, 0xa9, // text(5) "café"
		0x01, // value: 1
	}
	v, err := Unmarshal(data)
	require.NoError(t, err)
	m, ok := v.(map[string]any)
	require.True(t, ok)
	require.Equal(t, int64(1), m["café"])
}
