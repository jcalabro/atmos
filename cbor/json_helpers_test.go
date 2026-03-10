package cbor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendJSONString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want string
	}{
		{"hello", `"hello"`},
		{"", `""`},
		{`has "quotes"`, `"has \"quotes\""`},
		{"has\\backslash", `"has\\backslash"`},
		{"line\nbreak", `"line\nbreak"`},
		{"tab\there", `"tab\there"`},
		{"\r\n", `"\r\n"`},
		{"\b\f", `"\b\f"`},
		{"\x00\x01\x1f", `"\u0000\u0001\u001f"`},
		// HTML-safe escaping (matches encoding/json).
		{"<script>", `"\u003cscript\u003e"`},
		{"a&b", `"a\u0026b"`},
		// Unicode passes through.
		{"こんにちは", `"こんにちは"`},
		{"emoji 🎉", `"emoji 🎉"`},
	}
	for _, tt := range tests {
		got := string(AppendJSONString(nil, tt.in))
		assert.Equal(t, tt.want, got, "AppendJSONString(%q)", tt.in)
	}
}

func TestReadJSONString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want string
		end  int
	}{
		{`"hello"`, "hello", 7},
		{`""`, "", 2},
		{`"has \"quotes\""`, `has "quotes"`, 16},
		{`"back\\slash"`, `back\slash`, 13},
		{`"line\nbreak"`, "line\nbreak", 13},
		{`"\t\r\n\b\f"`, "\t\r\n\b\f", 12},
		{`"slash\/"`, "slash/", 9},
		{`"\u0041"`, "A", 8},
		{`"\u003c"`, "<", 8},
		{`"hello" rest`, "hello", 7},
	}
	for _, tt := range tests {
		got, pos, err := ReadJSONString([]byte(tt.in), 0)
		require.NoError(t, err, "ReadJSONString(%q)", tt.in)
		assert.Equal(t, tt.want, got)
		assert.Equal(t, tt.end, pos)
	}
}

func TestReadJSONString_Errors(t *testing.T) {
	t.Parallel()
	_, _, err := ReadJSONString([]byte(`"unterminated`), 0)
	assert.Error(t, err)
	_, _, err = ReadJSONString([]byte(`not a string`), 0)
	assert.Error(t, err)
	_, _, err = ReadJSONString([]byte(`"bad \x escape"`), 0)
	assert.Error(t, err)
}

func TestJSONString_RoundTrip(t *testing.T) {
	t.Parallel()
	inputs := []string{
		"hello world",
		`has "quotes" and \backslash`,
		"line\nbreak\ttab",
		"\x00\x01\x1f control",
		"<html>&amp;</html>",
		"こんにちは 🎉",
	}
	for _, s := range inputs {
		encoded := AppendJSONString(nil, s)
		decoded, _, err := ReadJSONString(encoded, 0)
		require.NoError(t, err, "round-trip for %q", s)
		assert.Equal(t, s, decoded, "round-trip for %q", s)
	}
}

func TestAppendJSONInt(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "0", string(AppendJSONInt(nil, 0)))
	assert.Equal(t, "42", string(AppendJSONInt(nil, 42)))
	assert.Equal(t, "-1", string(AppendJSONInt(nil, -1)))
	assert.Equal(t, "9223372036854775807", string(AppendJSONInt(nil, 9223372036854775807)))
}

func TestReadJSONInt(t *testing.T) {
	t.Parallel()
	n, pos, err := ReadJSONInt([]byte("42"), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
	assert.Equal(t, 2, pos)

	n, pos, err = ReadJSONInt([]byte("-100,"), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(-100), n)
	assert.Equal(t, 4, pos)

	n, pos, err = ReadJSONInt([]byte("0"), 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 1, pos)

	_, _, err = ReadJSONInt([]byte("abc"), 0)
	assert.Error(t, err)
}

func TestAppendReadJSONBool(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "true", string(AppendJSONBool(nil, true)))
	assert.Equal(t, "false", string(AppendJSONBool(nil, false)))

	b, pos, err := ReadJSONBool([]byte("true"), 0)
	require.NoError(t, err)
	assert.True(t, b)
	assert.Equal(t, 4, pos)

	b, pos, err = ReadJSONBool([]byte("false,"), 0)
	require.NoError(t, err)
	assert.False(t, b)
	assert.Equal(t, 5, pos)

	_, _, err = ReadJSONBool([]byte("null"), 0)
	assert.Error(t, err)
}

func TestJSONNull(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "null", string(AppendJSONNull(nil)))
	assert.True(t, IsJSONNull([]byte("null"), 0))
	assert.True(t, IsJSONNull([]byte("  null"), 0))
	assert.False(t, IsJSONNull([]byte(`"null"`), 0))
	assert.False(t, IsJSONNull([]byte("true"), 0))

	pos, err := SkipJSONNull([]byte("null,"), 0)
	require.NoError(t, err)
	assert.Equal(t, 4, pos)
}

func TestReadJSONObjectStart(t *testing.T) {
	t.Parallel()
	pos, err := ReadJSONObjectStart([]byte(`{"key":1}`), 0)
	require.NoError(t, err)
	assert.Equal(t, 1, pos)

	pos, err = ReadJSONObjectStart([]byte(`  { "key":1}`), 0)
	require.NoError(t, err)
	assert.Equal(t, 4, pos)

	_, err = ReadJSONObjectStart([]byte(`[1]`), 0)
	assert.Error(t, err)
}

func TestReadJSONObjectEnd(t *testing.T) {
	t.Parallel()
	pos, done := ReadJSONObjectEnd([]byte(`}`), 0)
	assert.True(t, done)
	assert.Equal(t, 1, pos)

	pos, done = ReadJSONObjectEnd([]byte(`  }`), 0)
	assert.True(t, done)
	assert.Equal(t, 3, pos)

	_, done = ReadJSONObjectEnd([]byte(`"key"`), 0)
	assert.False(t, done)
}

func TestReadJSONKey(t *testing.T) {
	t.Parallel()
	key, pos, err := ReadJSONKey([]byte(`"name": "value"`), 0)
	require.NoError(t, err)
	assert.Equal(t, "name", key)
	assert.Equal(t, 8, pos)

	key, pos, err = ReadJSONKey([]byte(`  "$type" : "test"`), 0)
	require.NoError(t, err)
	assert.Equal(t, "$type", key)
	assert.Equal(t, 12, pos)
}

func TestReadJSONArray(t *testing.T) {
	t.Parallel()
	pos, err := ReadJSONArrayStart([]byte(`[1,2]`), 0)
	require.NoError(t, err)
	assert.Equal(t, 1, pos)

	pos, done := ReadJSONArrayEnd([]byte(`]`), 0)
	assert.True(t, done)
	assert.Equal(t, 1, pos)

	_, done = ReadJSONArrayEnd([]byte(`1`), 0)
	assert.False(t, done)
}

func TestSkipJSONComma(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1, SkipJSONComma([]byte(`,`), 0))
	assert.Equal(t, 2, SkipJSONComma([]byte(`, `), 0))
	assert.Equal(t, 0, SkipJSONComma([]byte(`}`), 0))
}

func TestReadJSONFloat(t *testing.T) {
	t.Parallel()
	f, pos, err := ReadJSONFloat([]byte("3.14"), 0)
	require.NoError(t, err)
	assert.InDelta(t, 3.14, f, 0.001)
	assert.Equal(t, 4, pos)

	f, pos, err = ReadJSONFloat([]byte("-1.5e10"), 0)
	require.NoError(t, err)
	assert.InDelta(t, -1.5e10, f, 1.0)
	assert.Equal(t, 7, pos)
}

func TestReadJSONString_SurrogatePair(t *testing.T) {
	t.Parallel()
	// U+1F600 (😀) encoded as surrogate pair.
	s, _, err := ReadJSONString([]byte(`"\uD83D\uDE00"`), 0)
	require.NoError(t, err)
	assert.Equal(t, "😀", s)
}

func TestAppendJSONBytes(t *testing.T) {
	t.Parallel()
	got := string(AppendJSONBytes(nil, []byte{0xDE, 0xAD}))
	assert.Equal(t, `"3q0"`, got) // base64 raw standard encoding
}

// --- Benchmarks ---

func BenchmarkAppendJSONString(b *testing.B) {
	buf := make([]byte, 0, 256)
	for b.Loop() {
		buf = AppendJSONString(buf[:0], "hello world, this is a test string")
	}
}

func BenchmarkAppendJSONString_Escapes(b *testing.B) {
	buf := make([]byte, 0, 256)
	for b.Loop() {
		buf = AppendJSONString(buf[:0], "has \"quotes\" and <html> & stuff\n")
	}
}

func BenchmarkReadJSONString(b *testing.B) {
	data := []byte(`"hello world, this is a test string"`)
	for b.Loop() {
		_, _, _ = ReadJSONString(data, 0)
	}
}

func BenchmarkReadJSONString_Escapes(b *testing.B) {
	data := []byte(`"has \"quotes\" and \u003chtml\u003e \u0026 stuff\n"`)
	for b.Loop() {
		_, _, _ = ReadJSONString(data, 0)
	}
}

func BenchmarkAppendJSONInt(b *testing.B) {
	buf := make([]byte, 0, 32)
	for b.Loop() {
		buf = AppendJSONInt(buf[:0], 1234567890)
	}
}

func BenchmarkReadJSONInt(b *testing.B) {
	data := []byte("1234567890")
	for b.Loop() {
		_, _, _ = ReadJSONInt(data, 0)
	}
}
