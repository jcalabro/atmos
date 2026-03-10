package cbor

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendUint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		val  uint64
		want []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{23, []byte{0x17}},
		{24, []byte{0x18, 0x18}},
		{255, []byte{0x18, 0xff}},
		{256, []byte{0x19, 0x01, 0x00}},
		{65535, []byte{0x19, 0xff, 0xff}},
		{65536, []byte{0x1a, 0x00, 0x01, 0x00, 0x00}},
		{math.MaxUint32, []byte{0x1a, 0xff, 0xff, 0xff, 0xff}},
		{math.MaxUint32 + 1, []byte{0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
	}
	for _, tt := range tests {
		got := AppendUint(nil, tt.val)
		assert.Equal(t, tt.want, got, "AppendUint(%d)", tt.val)
	}
}

func TestAppendInt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		val  int64
		want []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{-1, []byte{0x20}},
		{-10, []byte{0x29}},
		{-100, []byte{0x38, 0x63}},
	}
	for _, tt := range tests {
		got := AppendInt(nil, tt.val)
		assert.Equal(t, tt.want, got, "AppendInt(%d)", tt.val)
	}
}

func TestAppendText(t *testing.T) {
	t.Parallel()
	got := AppendText(nil, "hello")
	assert.Equal(t, []byte{0x65, 'h', 'e', 'l', 'l', 'o'}, got)
}

func TestAppendBytes(t *testing.T) {
	t.Parallel()
	got := AppendBytes(nil, []byte{0x01, 0x02})
	assert.Equal(t, []byte{0x42, 0x01, 0x02}, got)
}

func TestAppendBool(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []byte{0xf5}, AppendBool(nil, true))
	assert.Equal(t, []byte{0xf4}, AppendBool(nil, false))
}

func TestAppendNull(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []byte{0xf6}, AppendNull(nil))
}

func TestAppendMapHeader(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []byte{0xa2}, AppendMapHeader(nil, 2))
	assert.Equal(t, []byte{0xa0}, AppendMapHeader(nil, 0))
}

func TestAppendArrayHeader(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []byte{0x83}, AppendArrayHeader(nil, 3))
}

func TestAppendFloat64(t *testing.T) {
	t.Parallel()
	got := AppendFloat64(nil, 1.5)
	assert.Equal(t, 9, len(got))
	assert.Equal(t, byte(0xfb), got[0])

	// Round-trip.
	f, pos, err := ReadFloat64(got, 0)
	require.NoError(t, err)
	assert.Equal(t, 1.5, f)
	assert.Equal(t, 9, pos)

	// Negative float.
	got = AppendFloat64(nil, -273.15)
	f, _, err = ReadFloat64(got, 0)
	require.NoError(t, err)
	assert.Equal(t, -273.15, f)

	// Zero.
	got = AppendFloat64(nil, 0.0)
	f, _, err = ReadFloat64(got, 0)
	require.NoError(t, err)
	assert.Equal(t, 0.0, f)
}

func TestAppendFloat64_PanicsOnNaN(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { AppendFloat64(nil, math.NaN()) })
}

func TestAppendFloat64_PanicsOnInf(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { AppendFloat64(nil, math.Inf(1)) })
	assert.Panics(t, func() { AppendFloat64(nil, math.Inf(-1)) })
}

func TestAppendCIDLinkRoundTrip(t *testing.T) {
	t.Parallel()
	cid := ComputeCID(0x71, []byte("test data"))
	buf := AppendCIDLink(nil, &cid)
	// Should start with tag 42 (0xd8, 0x2a)
	require.True(t, len(buf) > 2)
	assert.Equal(t, byte(0xd8), buf[0])
	assert.Equal(t, byte(0x2a), buf[1])

	// Round-trip through ReadCIDLink.
	decoded, pos, err := ReadCIDLink(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), pos)
	assert.True(t, cid.Equal(decoded))
}

func TestAppendTextKey(t *testing.T) {
	t.Parallel()
	key := AppendTextKey(nil, "did")
	assert.Equal(t, []byte{0x63, 'd', 'i', 'd'}, key)
}

func TestCompareCBORKeys(t *testing.T) {
	t.Parallel()
	assert.Equal(t, -1, CompareCBORKeys("ab", "abc"))  // shorter first
	assert.Equal(t, 1, CompareCBORKeys("abc", "ab"))   // longer second
	assert.Equal(t, -1, CompareCBORKeys("abc", "abd")) // lex order
	assert.Equal(t, 0, CompareCBORKeys("abc", "abc"))  // equal
}

func TestRoundTripAllTypes(t *testing.T) {
	t.Parallel()

	// Encode various values and decode them back.
	var buf []byte

	// uint
	buf = AppendUint(nil, 42)
	v, pos, err := ReadUint(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), v)
	assert.Equal(t, len(buf), pos)

	// int (positive)
	buf = AppendInt(nil, 100)
	iv, pos, err := ReadInt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(100), iv)
	assert.Equal(t, len(buf), pos)

	// int (negative)
	buf = AppendInt(nil, -50)
	iv, pos, err = ReadInt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(-50), iv)
	assert.Equal(t, len(buf), pos)

	// text
	buf = AppendText(nil, "test")
	s, pos, err := ReadText(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, "test", s)
	assert.Equal(t, len(buf), pos)

	// bytes
	buf = AppendBytes(nil, []byte{1, 2, 3})
	b, pos, err := ReadBytes(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, b)
	assert.Equal(t, len(buf), pos)

	// bool
	buf = AppendBool(nil, true)
	bv, pos, err := ReadBool(buf, 0)
	require.NoError(t, err)
	assert.True(t, bv)
	assert.Equal(t, len(buf), pos)

	// null
	buf = AppendNull(nil)
	assert.True(t, IsNull(buf, 0))
	pos, err = ReadNull(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, pos)

	// map header
	buf = AppendMapHeader(nil, 5)
	mc, pos, err := ReadMapHeader(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), mc)
	assert.Equal(t, len(buf), pos)

	// array header
	buf = AppendArrayHeader(nil, 3)
	ac, pos, err := ReadArrayHeader(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), ac)
	assert.Equal(t, len(buf), pos)
}

func TestSkipValue(t *testing.T) {
	t.Parallel()

	// Build a complex CBOR structure: map with nested array.
	var buf []byte
	buf = AppendMapHeader(buf, 2)
	buf = AppendText(buf, "a")
	buf = AppendArrayHeader(buf, 2)
	buf = AppendUint(buf, 1)
	buf = AppendUint(buf, 2)
	buf = AppendText(buf, "b")
	buf = AppendText(buf, "hello")

	pos, err := SkipValue(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), pos)

	// Skip tag + value (CID link).
	cid := ComputeCID(0x71, []byte("data"))
	cidBuf := AppendCIDLink(nil, &cid)
	pos, err = SkipValue(cidBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(cidBuf), pos)

	// Skip bool, null, float64.
	for _, b := range [][]byte{AppendBool(nil, true), AppendNull(nil), AppendFloat64(nil, 3.14)} {
		pos, err = SkipValue(b, 0)
		require.NoError(t, err)
		assert.Equal(t, len(b), pos)
	}
}

func TestSkipValue_NegativeInt(t *testing.T) {
	t.Parallel()
	buf := AppendInt(nil, -1000)
	pos, err := SkipValue(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, len(buf), pos)
}

func TestPeekType(t *testing.T) {
	t.Parallel()

	// Build a map with $type field.
	var buf []byte
	buf = AppendMapHeader(buf, 2)
	buf = AppendText(buf, "$type")
	buf = AppendText(buf, "app.bsky.feed.post")
	buf = AppendText(buf, "text")
	buf = AppendText(buf, "hello")

	typ, err := PeekType(buf)
	require.NoError(t, err)
	assert.Equal(t, "app.bsky.feed.post", typ)

	// $type not first field — should still find it.
	buf = nil
	buf = AppendMapHeader(buf, 2)
	buf = AppendText(buf, "text")
	buf = AppendText(buf, "hello")
	buf = AppendText(buf, "$type")
	buf = AppendText(buf, "app.bsky.feed.like")

	typ, err = PeekType(buf)
	require.NoError(t, err)
	assert.Equal(t, "app.bsky.feed.like", typ)

	// No $type field.
	buf = nil
	buf = AppendMapHeader(buf, 1)
	buf = AppendText(buf, "text")
	buf = AppendText(buf, "hello")
	_, err = PeekType(buf)
	assert.Error(t, err)
}

func TestAppendUintMatchesEncoder(t *testing.T) {
	t.Parallel()
	// Verify AppendUint produces same bytes as Encoder.writeHeader for major 0.
	for _, v := range []uint64{0, 1, 23, 24, 255, 256, 65535, 65536, math.MaxUint32, math.MaxUint32 + 1} {
		var w bytes.Buffer
		enc := NewEncoder(&w)
		err := enc.writeHeader(0, v)
		require.NoError(t, err)
		got := AppendUint(nil, v)
		assert.Equal(t, w.Bytes(), got, "mismatch for %d", v)
	}
}

// --- Error path tests ---

func TestReadHeader_Truncated(t *testing.T) {
	t.Parallel()
	// Empty data.
	_, _, _, err := ReadHeader(nil, 0)
	assert.Error(t, err)

	// 1-byte additional info but no payload.
	_, _, _, err = ReadHeader([]byte{0x18}, 0)
	assert.Error(t, err)

	// 2-byte additional info but only 1 byte of payload.
	_, _, _, err = ReadHeader([]byte{0x19, 0x01}, 0)
	assert.Error(t, err)
}

func TestReadHeader_NonMinimalEncoding(t *testing.T) {
	t.Parallel()
	// 5 encoded as 1-byte additional (should be inline).
	_, _, _, err := ReadHeader([]byte{0x18, 0x05}, 0)
	assert.ErrorContains(t, err, "non-minimal")

	// 200 encoded as 2-byte additional (should be 1-byte).
	_, _, _, err = ReadHeader([]byte{0x19, 0x00, 0xC8}, 0)
	assert.ErrorContains(t, err, "non-minimal")

	// 1000 encoded as 4-byte additional (should be 2-byte).
	_, _, _, err = ReadHeader([]byte{0x1a, 0x00, 0x00, 0x03, 0xe8}, 0)
	assert.ErrorContains(t, err, "non-minimal")

	// 0xFFFFFFFF encoded as 8-byte additional (should be 4-byte).
	_, _, _, err = ReadHeader([]byte{0x1b, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff}, 0)
	assert.ErrorContains(t, err, "non-minimal")
}

func TestReadText_WrongMajor(t *testing.T) {
	t.Parallel()
	// Send uint where text expected.
	_, _, err := ReadText([]byte{0x05}, 0)
	assert.ErrorContains(t, err, "expected text")
}

func TestReadBytes_WrongMajor(t *testing.T) {
	t.Parallel()
	_, _, err := ReadBytes([]byte{0x65, 'h', 'e', 'l', 'l', 'o'}, 0) // text, not bytes
	assert.ErrorContains(t, err, "expected bytes")
}

func TestReadUint_WrongMajor(t *testing.T) {
	t.Parallel()
	_, _, err := ReadUint([]byte{0x20}, 0) // negative int
	assert.ErrorContains(t, err, "expected uint")
}

func TestReadInt_Overflow(t *testing.T) {
	t.Parallel()
	// MaxUint64 as uint: overflows int64.
	buf := AppendUint(nil, math.MaxUint64)
	_, _, err := ReadInt(buf, 0)
	assert.ErrorContains(t, err, "overflows")
}

func TestReadBool_WrongByte(t *testing.T) {
	t.Parallel()
	_, _, err := ReadBool([]byte{0x00}, 0) // uint 0, not bool
	assert.ErrorContains(t, err, "expected boolean")

	_, _, err = ReadBool(nil, 0)
	assert.Error(t, err)
}

func TestReadFloat64_WrongByte(t *testing.T) {
	t.Parallel()
	_, _, err := ReadFloat64([]byte{0xf5}, 0) // true, not float64
	assert.ErrorContains(t, err, "expected float64")
}

func TestReadFloat64_NaN(t *testing.T) {
	t.Parallel()
	bits := math.Float64bits(math.NaN())
	data := []byte{0xfb,
		byte(bits >> 56), byte(bits >> 48), byte(bits >> 40), byte(bits >> 32),
		byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits)}
	_, _, err := ReadFloat64(data, 0)
	assert.ErrorContains(t, err, "NaN")
}

func TestReadFloat64_Inf(t *testing.T) {
	t.Parallel()
	bits := math.Float64bits(math.Inf(1))
	data := []byte{0xfb,
		byte(bits >> 56), byte(bits >> 48), byte(bits >> 40), byte(bits >> 32),
		byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits)}
	_, _, err := ReadFloat64(data, 0)
	assert.ErrorContains(t, err, "Infinity")
}

func TestReadCIDLink_BadPrefix(t *testing.T) {
	t.Parallel()
	// Not a tag 42.
	_, _, err := ReadCIDLink([]byte{0x00}, 0)
	assert.ErrorContains(t, err, "expected tag 42")

	// Tag 42 but truncated.
	_, _, err = ReadCIDLink([]byte{0xd8}, 0)
	assert.ErrorContains(t, err, "expected tag 42")
}

func TestReadMapHeader_WrongMajor(t *testing.T) {
	t.Parallel()
	_, _, err := ReadMapHeader([]byte{0x83}, 0) // array, not map
	assert.ErrorContains(t, err, "expected map")
}

func TestReadArrayHeader_WrongMajor(t *testing.T) {
	t.Parallel()
	_, _, err := ReadArrayHeader([]byte{0xa2}, 0) // map, not array
	assert.ErrorContains(t, err, "expected array")
}

func TestReadNull_NotNull(t *testing.T) {
	t.Parallel()
	_, err := ReadNull([]byte{0x00}, 0)
	assert.ErrorContains(t, err, "expected null")
}

func TestSkipValue_Truncated(t *testing.T) {
	t.Parallel()
	// Text header says 5 bytes but data has only 2.
	_, err := SkipValue([]byte{0x65, 'h', 'i'}, 0)
	assert.Error(t, err)

	// Empty input.
	_, err = SkipValue(nil, 0)
	assert.Error(t, err)
}

// --- Fuzz tests ---

func FuzzReadHeader(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x18, 0x18})
	f.Add([]byte{0x19, 0x01, 0x00})
	f.Add([]byte{0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _ = ReadHeader(data, 0)
	})
}

func FuzzSkipValue(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x65, 'h', 'e', 'l', 'l', 'o'})
	f.Add(AppendMapHeader(AppendArrayHeader(nil, 0), 0))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = SkipValue(data, 0)
	})
}

func FuzzPeekType(f *testing.F) {
	var buf []byte
	buf = AppendMapHeader(buf, 1)
	buf = AppendText(buf, "$type")
	buf = AppendText(buf, "test")
	f.Add(buf)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = PeekType(data)
	})
}
