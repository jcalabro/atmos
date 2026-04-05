package bsky

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- JSON extra fields ---

func TestExtraFields_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	// JSON with two known fields (text, createdAt) and one unknown field (mood).
	input := `{"$type":"app.bsky.feed.post","text":"hello","createdAt":"2024-01-01T00:00:00Z","mood":"happy"}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	assert.Equal(t, "hello", post.Text)
	assert.Equal(t, "2024-01-01T00:00:00Z", post.CreatedAt)

	// Re-marshal: the unknown "mood" field must still be present.
	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))
	assert.JSONEq(t, input, string(out))
}

func TestExtraFields_JSONMultipleUnknowns(t *testing.T) {
	t.Parallel()

	// Multiple unknown fields should all be preserved.
	input := `{"text":"hi","createdAt":"2024-01-01T00:00:00Z","extra1":"val1","extra2":42,"extra3":true}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))

	// Verify all extras are present in output.
	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "val1", m["extra1"])
	assert.Equal(t, float64(42), m["extra2"])
	assert.Equal(t, true, m["extra3"])
}

func TestExtraFields_JSONNestedValue(t *testing.T) {
	t.Parallel()

	// Unknown field with a complex nested value (object and array).
	input := `{"text":"hi","createdAt":"2024-01-01T00:00:00Z","metadata":{"source":"test","tags":["a","b"]}}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))

	// Verify the nested object is preserved intact.
	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	md, ok := m["metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test", md["source"])
}

func TestExtraFields_JSONNoExtras(t *testing.T) {
	t.Parallel()

	// When there are no unknown fields, round-trip should work as before.
	input := `{"text":"hello","createdAt":"2024-01-01T00:00:00Z"}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))
	assert.JSONEq(t, input, string(out))
}

func TestExtraFields_JSONOrderPreserved(t *testing.T) {
	t.Parallel()

	// Unknown fields should appear in the output in the same order they were
	// encountered during unmarshal.
	input := `{"text":"hi","createdAt":"2024-01-01T00:00:00Z","zzz":"last","aaa":"first","mmm":"middle"}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	out, err := post.MarshalJSON()
	require.NoError(t, err)

	// The extras should appear after known fields, in insertion order (zzz, aaa, mmm).
	outStr := string(out)
	zIdx := indexOf(outStr, `"zzz"`)
	aIdx := indexOf(outStr, `"aaa"`)
	mIdx := indexOf(outStr, `"mmm"`)
	require.True(t, zIdx > 0 && aIdx > 0 && mIdx > 0, "all extras must be in output")
	assert.Less(t, zIdx, aIdx, "zzz should come before aaa (insertion order)")
	assert.Less(t, aIdx, mIdx, "aaa should come before mmm (insertion order)")
}

// --- CBOR extra fields ---

func TestExtraFields_CBORRoundTrip(t *testing.T) {
	t.Parallel()

	// Build CBOR with known fields + one unknown field.
	// FeedPost has required fields: text, createdAt, $type
	var buf []byte
	buf = cbor.AppendMapHeader(buf, 4) // 3 known + 1 extra
	// DAG-CBOR key order: text(4), $type(5), createdAt(9), mood(4)
	// But "mood" sorts after "text" with same length, so:
	// DAG-CBOR: mood(4) < text(4)? No: m < t, so mood before text.
	// Key order by length first, then lexicographic:
	// mood(4), text(4): "mood" < "text" → mood first
	// $type(5)
	// createdAt(9)
	buf = cbor.AppendTextKey(buf, "mood")
	buf = cbor.AppendText(buf, "happy")
	buf = cbor.AppendTextKey(buf, "text")
	buf = cbor.AppendText(buf, "hello")
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "app.bsky.feed.post")
	buf = cbor.AppendTextKey(buf, "createdAt")
	buf = cbor.AppendText(buf, "2024-01-01T00:00:00Z")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf))
	assert.Equal(t, "hello", post.Text)
	assert.Equal(t, "2024-01-01T00:00:00Z", post.CreatedAt)

	// Re-marshal: bytes should be identical (DAG-CBOR is deterministic).
	out, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf, out, "CBOR round-trip must produce identical bytes")
}

func TestExtraFields_CBORMultipleUnknowns(t *testing.T) {
	t.Parallel()

	// Build CBOR with 3 known + 2 unknown fields, all in DAG-CBOR order.
	// Known keys: text(4), $type(5), createdAt(9)
	// Unknown keys: ab(2), mood(4)
	// DAG-CBOR order: ab(2), mood(4), text(4), $type(5), createdAt(9)
	var buf []byte
	buf = cbor.AppendMapHeader(buf, 5)
	buf = cbor.AppendTextKey(buf, "ab")
	buf = cbor.AppendText(buf, "value-ab")
	buf = cbor.AppendTextKey(buf, "mood")
	buf = cbor.AppendText(buf, "happy")
	buf = cbor.AppendTextKey(buf, "text")
	buf = cbor.AppendText(buf, "hello")
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "app.bsky.feed.post")
	buf = cbor.AppendTextKey(buf, "createdAt")
	buf = cbor.AppendText(buf, "2024-01-01T00:00:00Z")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf))

	out, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf, out, "CBOR round-trip with multiple extras must produce identical bytes")
}

func TestExtraFields_CBORNoExtras(t *testing.T) {
	t.Parallel()

	// Standard CBOR with no extra fields.
	var buf []byte
	buf = cbor.AppendMapHeader(buf, 3)
	buf = cbor.AppendTextKey(buf, "text")
	buf = cbor.AppendText(buf, "hello")
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "app.bsky.feed.post")
	buf = cbor.AppendTextKey(buf, "createdAt")
	buf = cbor.AppendText(buf, "2024-01-01T00:00:00Z")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf))

	out, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf, out)
}

func TestExtraFields_CBORExtraAfterAllKnown(t *testing.T) {
	t.Parallel()

	// Extra field with a key that sorts after all known keys.
	// "createdAt" is the last known key at length 9.
	// "zzzzzzzzzz" (length 10) sorts after.
	var buf []byte
	buf = cbor.AppendMapHeader(buf, 4)
	buf = cbor.AppendTextKey(buf, "text")
	buf = cbor.AppendText(buf, "hello")
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "app.bsky.feed.post")
	buf = cbor.AppendTextKey(buf, "createdAt")
	buf = cbor.AppendText(buf, "2024-01-01T00:00:00Z")
	buf = cbor.AppendTextKey(buf, "zzzzzzzzzz")
	buf = cbor.AppendText(buf, "trailing")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf))

	out, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf, out)
}

// --- Cross-format: extras do NOT leak between JSON and CBOR ---

func TestExtraFields_CrossFormatNoLeak(t *testing.T) {
	t.Parallel()

	// Unmarshal from JSON with an extra field.
	jsonInput := `{"text":"hello","createdAt":"2024-01-01T00:00:00Z","mood":"happy"}`
	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(jsonInput)))

	// CBOR marshal should NOT contain the JSON extra — the extraCBOR slice is empty.
	cborOut, err := post.MarshalCBOR()
	require.NoError(t, err)

	// Decode the CBOR to verify "mood" is absent.
	count, pos, err := cbor.ReadMapHeader(cborOut, 0)
	require.NoError(t, err)
	var keys []string
	for i := uint64(0); i < count; i++ {
		keyStart, keyEnd, newPos, err := cbor.ReadTextKey(cborOut, pos)
		require.NoError(t, err)
		keys = append(keys, string(cborOut[keyStart:keyEnd]))
		pos, err = cbor.SkipValue(cborOut, newPos)
		require.NoError(t, err)
	}
	assert.NotContains(t, keys, "mood", "JSON extra must not leak into CBOR output")
}

func TestExtraFields_CrossFormatNoLeakCBORToJSON(t *testing.T) {
	t.Parallel()

	// Unmarshal from CBOR with an extra field.
	var buf []byte
	buf = cbor.AppendMapHeader(buf, 4)
	buf = cbor.AppendTextKey(buf, "mood")
	buf = cbor.AppendText(buf, "happy")
	buf = cbor.AppendTextKey(buf, "text")
	buf = cbor.AppendText(buf, "hello")
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "app.bsky.feed.post")
	buf = cbor.AppendTextKey(buf, "createdAt")
	buf = cbor.AppendText(buf, "2024-01-01T00:00:00Z")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf))

	// JSON marshal should NOT contain the CBOR extra.
	jsonOut, err := post.MarshalJSON()
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(jsonOut, &m))
	assert.NotContains(t, m, "mood", "CBOR extra must not leak into JSON output")
}

// --- LexBlob extra fields ---

func TestExtraFields_LexBlobJSONRoundTrip(t *testing.T) {
	t.Parallel()

	input := `{"$type":"blob","mimeType":"image/png","ref":{"$link":"bafytest"},"size":1234,"extra":"blobdata"}`

	var blob lextypes.LexBlob
	require.NoError(t, blob.UnmarshalJSON([]byte(input)))
	assert.Equal(t, "image/png", blob.MimeType)

	out, err := blob.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))
	assert.JSONEq(t, input, string(out))
}

func TestExtraFields_LexBlobCBORRoundTrip(t *testing.T) {
	t.Parallel()

	// Build CBOR for LexBlob with an extra field.
	// Known keys in DAG-CBOR order: ref(3), size(4), $type(5), mimeType(8)
	// Extra: ab(2) sorts before all known keys.
	cid, err := cbor.ParseCIDString("bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq")
	require.NoError(t, err)

	var buf []byte
	buf = cbor.AppendMapHeader(buf, 5)
	buf = cbor.AppendTextKey(buf, "ab")
	buf = cbor.AppendText(buf, "extra-value")
	buf = cbor.AppendTextKey(buf, "ref")
	buf = cbor.AppendCIDLink(buf, &cid)
	buf = cbor.AppendTextKey(buf, "size")
	buf = cbor.AppendInt(buf, 1234)
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "blob")
	buf = cbor.AppendTextKey(buf, "mimeType")
	buf = cbor.AppendText(buf, "image/png")

	var blob lextypes.LexBlob
	require.NoError(t, blob.UnmarshalCBOR(buf))
	assert.Equal(t, "image/png", blob.MimeType)
	assert.Equal(t, int64(1234), blob.Size)

	out, err := blob.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf, out, "LexBlob CBOR round-trip with extras must produce identical bytes")
}

// --- Nested object extra fields ---

func TestExtraFields_NestedObjectJSON(t *testing.T) {
	t.Parallel()

	// FeedPost with a reply that has an unknown field.
	input := `{
		"text":"hi",
		"createdAt":"2024-01-01T00:00:00Z",
		"reply":{
			"root":{"uri":"at://did:plc:a/app.bsky.feed.post/1","cid":"bafytest"},
			"parent":{"uri":"at://did:plc:a/app.bsky.feed.post/2","cid":"bafytest"},
			"unknownReplyField":"surprise"
		}
	}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))
	require.True(t, post.Reply.HasVal())

	// Re-marshal the whole post.
	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))

	// The unknown field in the reply should be preserved.
	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	reply, ok := m["reply"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "surprise", reply["unknownReplyField"])
}

// --- Double unmarshal: extras must reset ---

func TestExtraFields_JSONDoubleUnmarshal(t *testing.T) {
	t.Parallel()

	// First unmarshal with an extra field.
	input1 := `{"text":"hello","createdAt":"2024-01-01T00:00:00Z","mood":"happy"}`
	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input1)))

	// Second unmarshal into the same struct with a different extra field.
	input2 := `{"text":"world","createdAt":"2024-02-01T00:00:00Z","color":"blue"}`
	require.NoError(t, post.UnmarshalJSON([]byte(input2)))

	// Should only have the second unmarshal's extras, not accumulated.
	out, err := post.MarshalJSON()
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "world", m["text"])
	assert.Equal(t, "blue", m["color"])
	assert.NotContains(t, m, "mood", "extras from first unmarshal must not persist")
}

func TestExtraFields_CBORDoubleUnmarshal(t *testing.T) {
	t.Parallel()

	// First CBOR with extra "ab".
	var buf1 []byte
	buf1 = cbor.AppendMapHeader(buf1, 4)
	buf1 = cbor.AppendTextKey(buf1, "ab")
	buf1 = cbor.AppendText(buf1, "first")
	buf1 = cbor.AppendTextKey(buf1, "text")
	buf1 = cbor.AppendText(buf1, "hello")
	buf1 = cbor.AppendTextKey(buf1, "$type")
	buf1 = cbor.AppendText(buf1, "app.bsky.feed.post")
	buf1 = cbor.AppendTextKey(buf1, "createdAt")
	buf1 = cbor.AppendText(buf1, "2024-01-01T00:00:00Z")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf1))

	// Second CBOR with extra "cd" (no "ab").
	var buf2 []byte
	buf2 = cbor.AppendMapHeader(buf2, 4)
	buf2 = cbor.AppendTextKey(buf2, "cd")
	buf2 = cbor.AppendText(buf2, "second")
	buf2 = cbor.AppendTextKey(buf2, "text")
	buf2 = cbor.AppendText(buf2, "world")
	buf2 = cbor.AppendTextKey(buf2, "$type")
	buf2 = cbor.AppendText(buf2, "app.bsky.feed.post")
	buf2 = cbor.AppendTextKey(buf2, "createdAt")
	buf2 = cbor.AppendText(buf2, "2024-02-01T00:00:00Z")

	require.NoError(t, post.UnmarshalCBOR(buf2))

	out, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf2, out, "second unmarshal must fully replace first")
}

// --- Non-string CBOR extra value types ---

func TestExtraFields_CBORNonStringValues(t *testing.T) {
	t.Parallel()

	// Extra fields with integer, boolean, array, and nested map values.
	var buf []byte
	buf = cbor.AppendMapHeader(buf, 6)
	// DAG-CBOR order by key length then lex: n(1), text(4), $type(5), arr(3), count(5), createdAt(9)
	// Actually: n(1), arr(3), text(4), $type(5), count(5), createdAt(9)
	buf = cbor.AppendTextKey(buf, "n")
	buf = cbor.AppendInt(buf, 42)
	buf = cbor.AppendTextKey(buf, "arr")
	buf = cbor.AppendArrayHeader(buf, 2)
	buf = cbor.AppendText(buf, "a")
	buf = cbor.AppendText(buf, "b")
	buf = cbor.AppendTextKey(buf, "text")
	buf = cbor.AppendText(buf, "hello")
	buf = cbor.AppendTextKey(buf, "$type")
	buf = cbor.AppendText(buf, "app.bsky.feed.post")
	buf = cbor.AppendTextKey(buf, "count")
	buf = cbor.AppendBool(buf, true)
	buf = cbor.AppendTextKey(buf, "createdAt")
	buf = cbor.AppendText(buf, "2024-01-01T00:00:00Z")

	var post FeedPost
	require.NoError(t, post.UnmarshalCBOR(buf))
	assert.Equal(t, "hello", post.Text)

	out, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, buf, out, "CBOR round-trip with non-string extra values must produce identical bytes")
}

// --- Null and empty extra values ---

func TestExtraFields_JSONNullExtraValue(t *testing.T) {
	t.Parallel()

	input := `{"text":"hi","createdAt":"2024-01-01T00:00:00Z","extra":null}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))
	assert.JSONEq(t, input, string(out))
}

func TestExtraFields_JSONEmptyObjectAndArray(t *testing.T) {
	t.Parallel()

	input := `{"text":"hi","createdAt":"2024-01-01T00:00:00Z","obj":{},"arr":[]}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	out, err := post.MarshalJSON()
	require.NoError(t, err)
	assert.True(t, json.Valid(out))
	assert.JSONEq(t, input, string(out))
}

// --- Duplicate key behavior ---

func TestExtraFields_JSONDuplicateKnownKey(t *testing.T) {
	t.Parallel()

	// When a known key appears twice, the last value wins and no extra is captured.
	input := `{"text":"first","createdAt":"2024-01-01T00:00:00Z","text":"second"}`

	var post FeedPost
	require.NoError(t, post.UnmarshalJSON([]byte(input)))

	// Last-write-wins for known fields.
	assert.Equal(t, "second", post.Text)

	// No extra should be captured for the duplicate known key.
	out, err := post.MarshalJSON()
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "second", m["text"])
}

// --- AppendCBORExtrasBefore unit tests ---

func TestAppendCBORExtrasBefore_Empty(t *testing.T) {
	t.Parallel()

	// Empty extras: should return immediately.
	idx, buf := appendCBORExtrasBefore(nil, 0, "anything", nil)
	assert.Equal(t, 0, idx)
	assert.Nil(t, buf)
}

func TestAppendCBORExtrasBefore_AllBefore(t *testing.T) {
	t.Parallel()

	// All extras sort before the next known key.
	extras := []extraField{
		{Key: "a", Value: cborTextValue("v1"), Encoding: extraEncodingCBOR},
		{Key: "b", Value: cborTextValue("v2"), Encoding: extraEncodingCBOR},
	}

	idx, buf := appendCBORExtrasBefore(extras, 0, "zzzz", nil)
	assert.Equal(t, 2, idx, "should emit all extras")
	assert.Greater(t, len(buf), 0)
}

func TestAppendCBORExtrasBefore_NoneMatch(t *testing.T) {
	t.Parallel()

	// All extras sort after the next known key.
	extras := []extraField{
		{Key: "zzz", Value: cborTextValue("v1"), Encoding: extraEncodingCBOR},
	}

	idx, buf := appendCBORExtrasBefore(extras, 0, "a", nil)
	assert.Equal(t, 0, idx, "should emit no extras")
	assert.Nil(t, buf)
}

func TestAppendCBORExtrasBefore_EmptyNextKey(t *testing.T) {
	t.Parallel()

	// Empty nextKey: emit all remaining extras.
	extras := []extraField{
		{Key: "x", Value: cborTextValue("v1"), Encoding: extraEncodingCBOR},
		{Key: "y", Value: cborTextValue("v2"), Encoding: extraEncodingCBOR},
	}

	idx, buf := appendCBORExtrasBefore(extras, 0, "", nil)
	assert.Equal(t, 2, idx, "should emit all extras when nextKey is empty")
	assert.Greater(t, len(buf), 0)
}

func TestAppendCBORExtrasBefore_Interleave(t *testing.T) {
	t.Parallel()

	// Extras: a(1), cc(2), zzz(3)
	// Known keys at positions: bb(2), dd(2)
	// Should emit: a before bb, cc before dd, zzz after dd.
	extras := []extraField{
		{Key: "a", Value: cborTextValue("v1"), Encoding: extraEncodingCBOR},
		{Key: "cc", Value: cborTextValue("v2"), Encoding: extraEncodingCBOR},
		{Key: "zzz", Value: cborTextValue("v3"), Encoding: extraEncodingCBOR},
	}

	// Before "bb": only "a" (length 1 < 2, sorts before "bb")
	idx, _ := appendCBORExtrasBefore(extras, 0, "bb", nil)
	assert.Equal(t, 1, idx)

	// Before "dd": "cc" (length 2, "cc" < "dd")
	idx, _ = appendCBORExtrasBefore(extras, idx, "dd", nil)
	assert.Equal(t, 2, idx)

	// After all known keys (nextKey=""): "zzz"
	idx, _ = appendCBORExtrasBefore(extras, idx, "", nil)
	assert.Equal(t, 3, idx)
}

// --- helper ---

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// cborTextValue encodes a string as CBOR text for use as a test extra value.
func cborTextValue(s string) []byte {
	return cbor.AppendText(nil, s)
}
