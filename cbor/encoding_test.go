package cbor

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeNull(t *testing.T) {
	t.Parallel()
	b, err := Marshal(nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0xf6}, b)
}

func TestEncodeBool(t *testing.T) {
	t.Parallel()
	b, err := Marshal(true)
	require.NoError(t, err)
	require.Equal(t, []byte{0xf5}, b)

	b, err = Marshal(false)
	require.NoError(t, err)
	require.Equal(t, []byte{0xf4}, b)
}

func TestEncodeInt_SmallPositive(t *testing.T) {
	t.Parallel()
	b, err := Marshal(int64(0))
	require.NoError(t, err)
	require.Equal(t, []byte{0x00}, b)

	b, err = Marshal(int64(23))
	require.NoError(t, err)
	require.Equal(t, []byte{0x17}, b)
}

func TestEncodeInt_OneByte(t *testing.T) {
	t.Parallel()
	b, err := Marshal(int64(24))
	require.NoError(t, err)
	require.Equal(t, []byte{0x18, 0x18}, b)

	b, err = Marshal(int64(255))
	require.NoError(t, err)
	require.Equal(t, []byte{0x18, 0xff}, b)
}

func TestEncodeInt_TwoBytes(t *testing.T) {
	t.Parallel()
	b, err := Marshal(int64(256))
	require.NoError(t, err)
	require.Equal(t, []byte{0x19, 0x01, 0x00}, b)
}

func TestEncodeInt_Negative(t *testing.T) {
	t.Parallel()
	b, err := Marshal(int64(-1))
	require.NoError(t, err)
	require.Equal(t, []byte{0x20}, b)

	b, err = Marshal(int64(-24))
	require.NoError(t, err)
	require.Equal(t, []byte{0x37}, b)

	b, err = Marshal(int64(-25))
	require.NoError(t, err)
	require.Equal(t, []byte{0x38, 0x18}, b)
}

func TestEncodeString(t *testing.T) {
	t.Parallel()
	b, err := Marshal("")
	require.NoError(t, err)
	require.Equal(t, []byte{0x60}, b)

	b, err = Marshal("hello")
	require.NoError(t, err)
	require.Equal(t, append([]byte{0x65}, "hello"...), b)
}

func TestEncodeBytes(t *testing.T) {
	t.Parallel()
	b, err := Marshal([]byte{0xDE, 0xAD})
	require.NoError(t, err)
	require.Equal(t, []byte{0x42, 0xDE, 0xAD}, b)
}

func TestEncodeArray(t *testing.T) {
	t.Parallel()
	b, err := Marshal([]any{int64(1), int64(2), int64(3)})
	require.NoError(t, err)
	require.Equal(t, []byte{0x83, 0x01, 0x02, 0x03}, b)
}

func TestEncodeEmptyArray(t *testing.T) {
	t.Parallel()
	b, err := Marshal([]any{})
	require.NoError(t, err)
	require.Equal(t, []byte{0x80}, b)
}

func TestEncodeMap_SortedByKey(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"b": int64(2),
		"a": int64(1),
	}
	b, err := Marshal(m)
	require.NoError(t, err)
	// "a" (0x61, 0x61) sorts before "b" (0x61, 0x62).
	expected := []byte{
		0xa2,       // map of 2 pairs
		0x61, 0x61, // "a"
		0x01,       // 1
		0x61, 0x62, // "b"
		0x02, // 2
	}
	require.Equal(t, expected, b)
}

func TestEncodeMap_SortByLength(t *testing.T) {
	t.Parallel()
	// DAG-CBOR sorts by encoded key bytes: shorter CBOR key headers sort first.
	m := map[string]any{
		"aa": int64(2),
		"b":  int64(1),
	}
	b, err := Marshal(m)
	require.NoError(t, err)
	// "b" (header 0x61) sorts before "aa" (header 0x62).
	expected := []byte{
		0xa2,       // map of 2 pairs
		0x61, 0x62, // "b"
		0x01,             // 1
		0x62, 0x61, 0x61, // "aa"
		0x02, // 2
	}
	require.Equal(t, expected, b)
}

func TestEncodeFloat64(t *testing.T) {
	t.Parallel()
	b, err := Marshal(float64(1.5))
	require.NoError(t, err)
	require.Len(t, b, 9)
	require.Equal(t, byte(0xfb), b[0]) // major 7, additional 27 (float64)
}

func TestEncodeFloat64_RejectNaN(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	err := enc.WriteFloat64(math.NaN())
	require.Error(t, err)
	require.Contains(t, err.Error(), "NaN")
}

func TestEncodeFloat64_RejectInfinity(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	err := enc.WriteFloat64(math.Inf(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Infinity")
}

func TestEncodeUnsupportedType(t *testing.T) {
	t.Parallel()
	_, err := Marshal(struct{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}
