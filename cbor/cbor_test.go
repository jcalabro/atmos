package cbor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Integration tests: encode then decode round-trips.

func TestRoundTrip_Null(t *testing.T) {
	t.Parallel()
	b, err := Marshal(nil)
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestRoundTrip_Bool(t *testing.T) {
	t.Parallel()
	for _, val := range []bool{true, false} {
		b, err := Marshal(val)
		require.NoError(t, err)
		v, err := Unmarshal(b)
		require.NoError(t, err)
		require.Equal(t, val, v)
	}
}

func TestRoundTrip_Integers(t *testing.T) {
	t.Parallel()
	cases := []int64{0, 1, 23, 24, 255, 256, 65535, 65536, -1, -24, -25, -256, -100000, 1000000}
	for _, val := range cases {
		b, err := Marshal(val)
		require.NoError(t, err)
		v, err := Unmarshal(b)
		require.NoError(t, err)
		require.Equal(t, val, v, "round-trip failed for %d", val)
	}
}

func TestRoundTrip_String(t *testing.T) {
	t.Parallel()
	for _, val := range []string{"", "hello", "hello world with unicode: 日本語"} {
		b, err := Marshal(val)
		require.NoError(t, err)
		v, err := Unmarshal(b)
		require.NoError(t, err)
		require.Equal(t, val, v)
	}
}

func TestRoundTrip_Bytes(t *testing.T) {
	t.Parallel()
	data := []byte{0x00, 0x01, 0xFF, 0xDE, 0xAD}
	b, err := Marshal(data)
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, data, v)
}

func TestRoundTrip_Float64(t *testing.T) {
	t.Parallel()
	for _, val := range []float64{0.0, 1.5, -1.5, 3.14159} {
		b, err := Marshal(val)
		require.NoError(t, err)
		v, err := Unmarshal(b)
		require.NoError(t, err)
		require.Equal(t, val, v)
	}
}

func TestRoundTrip_Array(t *testing.T) {
	t.Parallel()
	arr := []any{int64(1), "hello", true, nil}
	b, err := Marshal(arr)
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, arr, v)
}

func TestRoundTrip_Map(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"name":   "alice",
		"age":    int64(30),
		"active": true,
	}
	b, err := Marshal(m)
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, m, v)
}

func TestRoundTrip_NestedStructure(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"items": []any{
			map[string]any{
				"id":   int64(1),
				"name": "first",
			},
			map[string]any{
				"id":   int64(2),
				"name": "second",
			},
		},
		"count": int64(2),
	}
	b, err := Marshal(data)
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, data, v)
}

func TestRoundTrip_CIDLink(t *testing.T) {
	t.Parallel()
	cid := ComputeCID(CodecDagCBOR, []byte("test data"))
	m := map[string]any{
		"link": cid,
	}
	b, err := Marshal(m)
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)

	result, ok := v.(map[string]any)
	require.True(t, ok)
	resultCID, ok := result["link"].(CID)
	require.True(t, ok)
	require.True(t, cid.Equal(resultCID))
}

func TestRoundTrip_EmptyContainers(t *testing.T) {
	t.Parallel()
	// Empty array.
	b, err := Marshal([]any{})
	require.NoError(t, err)
	v, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, []any{}, v)

	// Empty map.
	b, err = Marshal(map[string]any{})
	require.NoError(t, err)
	v, err = Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, map[string]any{}, v)
}

func TestMapKeySorting_Deterministic(t *testing.T) {
	t.Parallel()
	// Encode the same map multiple times and verify identical bytes.
	m := map[string]any{
		"z":   int64(1),
		"a":   int64(2),
		"m":   int64(3),
		"bb":  int64(4),
		"aaa": int64(5),
	}
	b1, err := Marshal(m)
	require.NoError(t, err)

	for range 10 {
		b2, err := Marshal(m)
		require.NoError(t, err)
		require.Equal(t, b1, b2, "encoding should be deterministic")
	}
}
