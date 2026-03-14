package cbor

import "testing"

var benchRecord = map[string]any{
	"$type":     "app.bsky.feed.post",
	"text":      "Hello, world! This is a test post with some content.",
	"createdAt": "2024-01-15T12:00:00.000Z",
	"langs":     []any{"en"},
	"reply": map[string]any{
		"root": map[string]any{
			"uri": "at://did:plc:abc123/app.bsky.feed.post/tid1",
			"cid": "bafyreib2rxk3rybkornupal",
		},
		"parent": map[string]any{
			"uri": "at://did:plc:abc123/app.bsky.feed.post/tid2",
			"cid": "bafyreib2rxk3rybkornupal",
		},
	},
}

func BenchmarkMarshal(b *testing.B) {
	for b.Loop() {
		_, _ = Marshal(benchRecord)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	data, _ := Marshal(benchRecord)
	b.ResetTimer()
	for b.Loop() {
		_, _ = Unmarshal(data)
	}
}

func BenchmarkMarshalRoundTrip(b *testing.B) {
	for b.Loop() {
		data, _ := Marshal(benchRecord)
		_, _ = Unmarshal(data)
	}
}

func BenchmarkComputeCID(b *testing.B) {
	data, _ := Marshal(benchRecord)
	b.ResetTimer()
	for b.Loop() {
		_ = ComputeCID(CodecDagCBOR, data)
	}
}

// --- Append/Read (generated code pattern) benchmarks ---

// Precomputed key tokens, simulating what generated code produces.
var (
	benchKey_type      = AppendTextKey(nil, "$type")
	benchKey_text      = AppendTextKey(nil, "text")
	benchKey_createdAt = AppendTextKey(nil, "createdAt")
	benchKey_langs     = AppendTextKey(nil, "langs")
	benchKey_reply     = AppendTextKey(nil, "reply")
	benchKey_uri       = AppendTextKey(nil, "uri")
	benchKey_cid       = AppendTextKey(nil, "cid")
	benchKey_root      = AppendTextKey(nil, "root")
	benchKey_parent    = AppendTextKey(nil, "parent")
)

// marshalPostAppend simulates what generated MarshalCBOR looks like for a
// FeedPost-sized struct: buffer-append with precomputed keys.
func marshalPostAppend() []byte {
	// Outer map: $type, text, createdAt, langs, reply — 5 fields.
	buf := make([]byte, 0, 256)
	buf = AppendMapHeader(buf, 5)

	// DAG-CBOR key order: text(4), $type(5), langs(5), reply(5), createdAt(9)
	buf = append(buf, benchKey_text...)
	buf = AppendText(buf, "Hello, world! This is a test post with some content.")

	buf = append(buf, benchKey_type...)
	buf = AppendText(buf, "app.bsky.feed.post")

	buf = append(buf, benchKey_langs...)
	buf = AppendArrayHeader(buf, 1)
	buf = AppendText(buf, "en")

	buf = append(buf, benchKey_reply...)
	// Inline nested struct: reply with root + parent, each with uri + cid.
	buf = AppendMapHeader(buf, 2)
	buf = append(buf, benchKey_parent...)
	buf = marshalStrongRef(buf, "at://did:plc:abc123/app.bsky.feed.post/tid2", "bafyreib2rxk3rybkornupal")
	buf = append(buf, benchKey_root...)
	buf = marshalStrongRef(buf, "at://did:plc:abc123/app.bsky.feed.post/tid1", "bafyreib2rxk3rybkornupal")

	buf = append(buf, benchKey_createdAt...)
	buf = AppendText(buf, "2024-01-15T12:00:00.000Z")

	return buf
}

func marshalStrongRef(buf []byte, uri, cidStr string) []byte {
	buf = AppendMapHeader(buf, 2)
	// DAG-CBOR key order: cid(3), uri(3) — same length, lex order.
	buf = append(buf, benchKey_cid...)
	buf = AppendText(buf, cidStr)
	buf = append(buf, benchKey_uri...)
	buf = AppendText(buf, uri)
	return buf
}

// unmarshalPostRead simulates what generated UnmarshalCBOR looks like:
// ReadTextKey-based key dispatch with length-first branching.
func unmarshalPostRead(data []byte) (typ, text, createdAt string, langs []string, err error) {
	count, pos, err := ReadMapHeader(data, 0)
	if err != nil {
		return "", "", "", nil, err
	}
	for range count {
		keyStart, keyEnd, newPos, err := ReadTextKey(data, pos)
		if err != nil {
			return "", "", "", nil, err
		}
		pos = newPos
		switch keyEnd - keyStart {
		case 4: // "text"
			if string(data[keyStart:keyEnd]) == "text" {
				text, pos, err = ReadText(data, pos)
			} else {
				pos, err = SkipValue(data, pos)
			}
		case 5: // "$type", "langs", "reply"
			if string(data[keyStart:keyEnd]) == "$type" {
				typ, pos, err = ReadText(data, pos)
			} else if string(data[keyStart:keyEnd]) == "langs" {
				var arrLen uint64
				arrLen, pos, err = ReadArrayHeader(data, pos)
				if err != nil {
					return "", "", "", nil, err
				}
				langs = make([]string, arrLen)
				for i := range arrLen {
					langs[i], pos, err = ReadText(data, pos)
					if err != nil {
						return "", "", "", nil, err
					}
				}
			} else {
				pos, err = SkipValue(data, pos)
			}
		case 9: // "createdAt"
			if string(data[keyStart:keyEnd]) == "createdAt" {
				createdAt, pos, err = ReadText(data, pos)
			} else {
				pos, err = SkipValue(data, pos)
			}
		default:
			pos, err = SkipValue(data, pos)
		}
		if err != nil {
			return "", "", "", nil, err
		}
	}
	return typ, text, createdAt, langs, nil
}

// BenchmarkAppendMarshal benchmarks the buffer-append marshal pattern
// (what generated MarshalCBOR does).
func BenchmarkAppendMarshal(b *testing.B) {
	for b.Loop() {
		_ = marshalPostAppend()
	}
}

// BenchmarkAppendUnmarshal benchmarks the position-tracking unmarshal pattern
// (what generated UnmarshalCBOR does).
func BenchmarkAppendUnmarshal(b *testing.B) {
	data := marshalPostAppend()
	b.ResetTimer()
	for b.Loop() {
		_, _, _, _, _ = unmarshalPostRead(data)
	}
}

// BenchmarkAppendRoundTrip benchmarks full marshal+unmarshal with the
// generated code pattern.
func BenchmarkAppendRoundTrip(b *testing.B) {
	for b.Loop() {
		data := marshalPostAppend()
		_, _, _, _, _ = unmarshalPostRead(data)
	}
}

// --- Individual helper benchmarks ---

func BenchmarkAppendText(b *testing.B) {
	s := "Hello, world! This is a test post with some content."
	buf := make([]byte, 0, 64)
	for b.Loop() {
		buf = AppendText(buf[:0], s)
	}
}

func BenchmarkReadText(b *testing.B) {
	data := AppendText(nil, "Hello, world! This is a test post with some content.")
	for b.Loop() {
		_, _, _ = ReadText(data, 0)
	}
}

func BenchmarkAppendUint(b *testing.B) {
	buf := make([]byte, 0, 16)
	for b.Loop() {
		buf = AppendUint(buf[:0], 1234567890)
	}
}

func BenchmarkReadUint(b *testing.B) {
	data := AppendUint(nil, 1234567890)
	for b.Loop() {
		_, _, _ = ReadUint(data, 0)
	}
}

func BenchmarkAppendCIDLink(b *testing.B) {
	cid := ComputeCID(CodecDagCBOR, []byte("benchmark data"))
	buf := make([]byte, 0, 64)
	for b.Loop() {
		buf = AppendCIDLink(buf[:0], &cid)
	}
}

func BenchmarkReadCIDLink(b *testing.B) {
	cid := ComputeCID(CodecDagCBOR, []byte("benchmark data"))
	data := AppendCIDLink(nil, &cid)
	for b.Loop() {
		_, _, _ = ReadCIDLink(data, 0)
	}
}

func BenchmarkSkipValue(b *testing.B) {
	data := marshalPostAppend()
	for b.Loop() {
		_, _ = SkipValue(data, 0)
	}
}

func BenchmarkPeekType(b *testing.B) {
	data := marshalPostAppend()
	for b.Loop() {
		_, _ = PeekType(data)
	}
}
