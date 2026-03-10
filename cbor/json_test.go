package cbor

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToJSON_Bytes(t *testing.T) {
	t.Parallel()
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	j, err := ToJSON(data)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(j, &m))
	require.Contains(t, m, "$bytes")
}

func TestToJSON_CIDLink(t *testing.T) {
	t.Parallel()
	cid := ComputeCID(CodecDagCBOR, []byte("test"))
	j, err := ToJSON(cid)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(j, &m))
	require.Contains(t, m, "$link")
	require.Equal(t, cid.String(), m["$link"])
}

func TestJSON_RoundTrip_Bytes(t *testing.T) {
	t.Parallel()
	original := []byte{0x01, 0x02, 0x03}
	j, err := ToJSON(original)
	require.NoError(t, err)

	v, err := FromJSON(j)
	require.NoError(t, err)
	require.Equal(t, original, v)
}

func TestJSON_RoundTrip_CID(t *testing.T) {
	t.Parallel()
	original := ComputeCID(CodecRaw, []byte("blob"))
	j, err := ToJSON(original)
	require.NoError(t, err)

	v, err := FromJSON(j)
	require.NoError(t, err)
	cid, ok := v.(CID)
	require.True(t, ok)
	require.True(t, original.Equal(cid))
}

func TestJSON_RoundTrip_Complex(t *testing.T) {
	t.Parallel()
	cid := ComputeCID(CodecDagCBOR, []byte("ref"))
	original := map[string]any{
		"text":  "hello",
		"count": int64(42),
		"data":  []byte{0xFF},
		"ref":   cid,
		"items": []any{int64(1), "two"},
	}
	j, err := ToJSON(original)
	require.NoError(t, err)

	v, err := FromJSON(j)
	require.NoError(t, err)
	m, ok := v.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "hello", m["text"])
	require.Equal(t, int64(42), m["count"])
	require.Equal(t, []byte{0xFF}, m["data"])
	refCID, ok := m["ref"].(CID)
	require.True(t, ok)
	require.True(t, cid.Equal(refCID))
}

func TestFromJSON_IntegerConversion(t *testing.T) {
	t.Parallel()
	// JSON numbers that are whole should become int64.
	v, err := FromJSON([]byte(`42`))
	require.NoError(t, err)
	require.Equal(t, int64(42), v)

	// JSON numbers with fractions stay float64.
	v, err = FromJSON([]byte(`3.14`))
	require.NoError(t, err)
	require.Equal(t, 3.14, v)
}

func TestFromJSON_Null(t *testing.T) {
	t.Parallel()
	v, err := FromJSON([]byte(`null`))
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestFromJSON_Bool(t *testing.T) {
	t.Parallel()
	v, err := FromJSON([]byte(`true`))
	require.NoError(t, err)
	require.Equal(t, true, v)
}

func TestPeekJSONType(t *testing.T) {
	t.Parallel()

	// $type as first key.
	typ, err := PeekJSONType([]byte(`{"$type":"app.bsky.feed.post","text":"hello"}`))
	require.NoError(t, err)
	require.Equal(t, "app.bsky.feed.post", typ)

	// $type not first key.
	typ, err = PeekJSONType([]byte(`{"text":"hello","$type":"app.bsky.feed.like"}`))
	require.NoError(t, err)
	require.Equal(t, "app.bsky.feed.like", typ)

	// No $type field.
	typ, err = PeekJSONType([]byte(`{"text":"hello"}`))
	require.NoError(t, err)
	require.Equal(t, "", typ)

	// Empty object.
	typ, err = PeekJSONType([]byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, "", typ)

	// With whitespace.
	typ, err = PeekJSONType([]byte(`  {  "$type" : "app.bsky.feed.post" , "x": 1 } `))
	require.NoError(t, err)
	require.Equal(t, "app.bsky.feed.post", typ)

	// Nested objects — $type in nested object should NOT be found.
	typ, err = PeekJSONType([]byte(`{"embed":{"$type":"app.bsky.embed.images"},"$type":"app.bsky.feed.post"}`))
	require.NoError(t, err)
	require.Equal(t, "app.bsky.feed.post", typ)

	// Value with escaped quotes in string.
	typ, err = PeekJSONType([]byte(`{"text":"hello \"world\"","$type":"app.bsky.feed.post"}`))
	require.NoError(t, err)
	require.Equal(t, "app.bsky.feed.post", typ)

	// Invalid JSON.
	_, err = PeekJSONType([]byte(`not json`))
	require.Error(t, err)

	// Array values should be skipped.
	typ, err = PeekJSONType([]byte(`{"items":[1,2,3],"$type":"app.bsky.feed.post"}`))
	require.NoError(t, err)
	require.Equal(t, "app.bsky.feed.post", typ)

	// Nested array with objects.
	typ, err = PeekJSONType([]byte(`{"items":[{"$type":"inner"}],"$type":"outer"}`))
	require.NoError(t, err)
	require.Equal(t, "outer", typ)

	// Boolean/null values.
	typ, err = PeekJSONType([]byte(`{"active":true,"deleted":false,"ref":null,"$type":"test"}`))
	require.NoError(t, err)
	require.Equal(t, "test", typ)

	// Number values.
	typ, err = PeekJSONType([]byte(`{"count":42,"rate":3.14,"neg":-1,"$type":"test"}`))
	require.NoError(t, err)
	require.Equal(t, "test", typ)
}

func BenchmarkPeekJSONType(b *testing.B) {
	data := []byte(`{"$type":"app.bsky.feed.post","text":"hello world","createdAt":"2024-01-01T00:00:00Z"}`)
	for b.Loop() {
		_, _ = PeekJSONType(data)
	}
}

func BenchmarkPeekJSONType_NotFirst(b *testing.B) {
	data := []byte(`{"text":"hello world","createdAt":"2024-01-01T00:00:00Z","$type":"app.bsky.feed.post"}`)
	for b.Loop() {
		_, _ = PeekJSONType(data)
	}
}
