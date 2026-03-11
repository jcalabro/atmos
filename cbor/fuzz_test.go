package cbor

import (
	"bytes"
	"encoding/json"
	"testing"
	"unicode/utf8"
)

// FuzzUnmarshal tests that the CBOR decoder never panics on arbitrary input
// and that any successfully decoded value can be re-encoded.
func FuzzUnmarshal(f *testing.F) {
	// Seed with valid CBOR values.
	seeds := [][]byte{
		{0xf6},                  // null
		{0xf4},                  // false
		{0xf5},                  // true
		{0x00},                  // 0
		{0x18, 0xff},            // 255
		{0x38, 0x00},            // -1
		{0x60},                  // ""
		{0x63, 'a', 'b', 'c'},   // "abc"
		{0x40},                  // h''
		{0x43, 1, 2, 3},         // h'010203'
		{0x80},                  // []
		{0x82, 0x01, 0x02},      // [1, 2]
		{0xa0},                  // {}
		{0xa1, 0x61, 'a', 0x01}, // {"a": 1}
		{0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}, // 3.141592653589793
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		val, err := Unmarshal(data)
		if err != nil {
			return
		}

		// If it decoded successfully, re-encoding should not panic.
		encoded, err := Marshal(val)
		if err != nil {
			return
		}

		// Re-decode the re-encoded value should produce the same result.
		val2, err := Unmarshal(encoded)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}

		// Re-encode again should be identical (deterministic).
		encoded2, err := Marshal(val2)
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
		if !bytes.Equal(encoded, encoded2) {
			t.Fatalf("non-deterministic encoding")
		}
	})
}

// FuzzParseCIDBytes tests CID parsing never panics and round-trips.
func FuzzParseCIDBytes(f *testing.F) {
	// Valid CIDv1 dag-cbor SHA-256.
	c := ComputeCID(CodecDagCBOR, []byte("test"))
	f.Add(c.Bytes())
	f.Add(ComputeCID(CodecRaw, []byte("raw")).Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		cid, err := ParseCIDBytes(data)
		if err != nil {
			return
		}
		// Round-trip: Bytes() should produce equivalent CID.
		rt, err := ParseCIDBytes(cid.Bytes())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !cid.Equal(rt) {
			t.Fatalf("round-trip mismatch")
		}
	})
}

// FuzzParseCIDPrefix tests prefix CID parsing never panics and that the fast
// path (dag-cbor SHA-256) produces the same result as the slow path.
func FuzzParseCIDPrefix(f *testing.F) {
	c := ComputeCID(CodecDagCBOR, []byte("test"))
	buf := c.Bytes()
	buf = append(buf, []byte("trailing data")...)
	f.Add(buf)
	// Seed that hits the fast path: 0x01 0x71 0x12 0x20 + 32 hash bytes.
	f.Add(ComputeCID(CodecDagCBOR, []byte("fast-path seed")).Bytes())
	// Seed that hits the slow path: raw codec.
	f.Add(ComputeCID(CodecRaw, []byte("slow-path seed")).Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		cid, n, err := ParseCIDPrefix(data)
		if err != nil {
			return
		}
		if n > len(data) {
			t.Fatalf("consumed %d bytes but input only has %d", n, len(data))
		}
		// Round-trip through Bytes and re-parse must match.
		rt, err := ParseCIDBytes(cid.Bytes())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !cid.Equal(rt) {
			t.Fatalf("round-trip mismatch")
		}
	})
}

// FuzzReadText tests that ReadText never panics on arbitrary input and that
// successfully decoded strings can be round-tripped through AppendText.
func FuzzReadText(f *testing.F) {
	f.Add([]byte{0x60})                // ""
	f.Add([]byte{0x63, 'a', 'b', 'c'}) // "abc"
	f.Add([]byte{0x65, 'h', 'e', 'l', 'l', 'o'})
	f.Add(AppendText(nil, "$type"))
	f.Add(AppendText(nil, "app.bsky.feed.post"))

	f.Fuzz(func(t *testing.T, data []byte) {
		s, newPos, err := ReadText(data, 0)
		if err != nil {
			return
		}
		// Round-trip: encode the string back and re-decode.
		encoded := AppendText(nil, s)
		s2, newPos2, err := ReadText(encoded, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if s != s2 {
			t.Fatalf("round-trip mismatch: %q vs %q", s, s2)
		}
		if newPos2 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", newPos2, len(encoded))
		}
		_ = newPos
	})
}

// FuzzReadCIDLink tests that ReadCIDLink never panics on arbitrary input and
// that successfully decoded CIDs round-trip through AppendCIDLink.
func FuzzReadCIDLink(f *testing.F) {
	// Valid CID link.
	c := ComputeCID(CodecDagCBOR, []byte("test"))
	f.Add(AppendCIDLink(nil, &c))
	// Raw codec CID link.
	c2 := ComputeCID(CodecRaw, []byte("raw"))
	f.Add(AppendCIDLink(nil, &c2))

	f.Fuzz(func(t *testing.T, data []byte) {
		cid, end, err := ReadCIDLink(data, 0)
		if err != nil {
			return
		}
		if end > len(data) {
			t.Fatalf("consumed %d bytes but input only has %d", end, len(data))
		}
		// Round-trip: encode back and re-decode.
		encoded := AppendCIDLink(nil, &cid)
		cid2, end2, err := ReadCIDLink(encoded, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if !cid.Equal(cid2) {
			t.Fatalf("round-trip CID mismatch")
		}
		if end2 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", end2, len(encoded))
		}
	})
}

// FuzzReadUvarint tests that ReadUvarint never panics on arbitrary input and
// that successfully decoded values round-trip through AppendUvarint.
func FuzzReadUvarint(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x7f})
	f.Add([]byte{0x80, 0x01})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}) // max uint64

	f.Fuzz(func(t *testing.T, data []byte) {
		val, n, err := ReadUvarint(data)
		if err != nil {
			return
		}
		if n > len(data) {
			t.Fatalf("consumed %d bytes but input only has %d", n, len(data))
		}
		// Round-trip.
		encoded := AppendUvarint(nil, val)
		val2, n2, err := ReadUvarint(encoded)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if val != val2 {
			t.Fatalf("round-trip mismatch: %d vs %d", val, val2)
		}
		if n2 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", n2, len(encoded))
		}
	})
}

// FuzzReadInt tests that ReadInt never panics on arbitrary input and that
// successfully decoded values round-trip through AppendInt.
func FuzzReadInt(f *testing.F) {
	f.Add([]byte{0x00})                                                 // 0
	f.Add([]byte{0x18, 0x18})                                           // 24
	f.Add([]byte{0x38, 0x00})                                           // -1
	f.Add([]byte{0x39, 0x01, 0x00})                                     // -257
	f.Add([]byte{0x1b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // max int64

	f.Fuzz(func(t *testing.T, data []byte) {
		val, newPos, err := ReadInt(data, 0)
		if err != nil {
			return
		}
		// Round-trip.
		encoded := AppendInt(nil, val)
		val2, newPos2, err := ReadInt(encoded, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if val != val2 {
			t.Fatalf("round-trip mismatch: %d vs %d", val, val2)
		}
		if newPos2 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", newPos2, len(encoded))
		}
		_ = newPos
	})
}

// FuzzReadFloat64 tests that ReadFloat64 never panics on arbitrary input and
// rejects NaN/Infinity per DAG-CBOR rules.
func FuzzReadFloat64(f *testing.F) {
	f.Add(AppendFloat64(nil, 0.0))
	f.Add(AppendFloat64(nil, 3.141592653589793))
	f.Add(AppendFloat64(nil, -1.0))
	f.Add([]byte{0xfb, 0x7f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // +Inf (must reject)
	f.Add([]byte{0xfb, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // NaN (must reject)

	f.Fuzz(func(t *testing.T, data []byte) {
		val, newPos, err := ReadFloat64(data, 0)
		if err != nil {
			return
		}
		// Round-trip.
		encoded := AppendFloat64(nil, val)
		val2, newPos2, err := ReadFloat64(encoded, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if val != val2 {
			t.Fatalf("round-trip mismatch: %g vs %g", val, val2)
		}
		if newPos2 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", newPos2, len(encoded))
		}
		_ = newPos
	})
}

// FuzzReadBytes tests that ReadBytes and ReadBytesNoCopy never panic on
// arbitrary input and produce consistent results.
func FuzzReadBytes(f *testing.F) {
	f.Add([]byte{0x40})          // h''
	f.Add([]byte{0x43, 1, 2, 3}) // h'010203'
	f.Add(AppendBytes(nil, []byte("hello")))
	f.Add(AppendBytes(nil, make([]byte, 256)))

	f.Fuzz(func(t *testing.T, data []byte) {
		b1, end1, err1 := ReadBytes(data, 0)
		b2, end2, err2 := ReadBytesNoCopy(data, 0)

		// Both must agree on success/failure.
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("ReadBytes err=%v but ReadBytesNoCopy err=%v", err1, err2)
		}
		if err1 != nil {
			return
		}
		if end1 != end2 {
			t.Fatalf("position mismatch: %d vs %d", end1, end2)
		}
		if !bytes.Equal(b1, b2) {
			t.Fatalf("content mismatch")
		}
		// Round-trip through AppendBytes.
		encoded := AppendBytes(nil, b1)
		b3, end3, err := ReadBytes(encoded, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if !bytes.Equal(b1, b3) {
			t.Fatalf("round-trip content mismatch")
		}
		if end3 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", end3, len(encoded))
		}
	})
}

// FuzzParseCIDString tests that CID string parsing never panics and round-trips.
func FuzzParseCIDString(f *testing.F) {
	cid := ComputeCID(CodecDagCBOR, []byte("test"))
	f.Add(cid.String())
	f.Add("")
	f.Add("bafyreib")
	f.Add("QmTest") // CIDv0

	f.Fuzz(func(t *testing.T, s string) {
		c, err := ParseCIDString(s)
		if err != nil {
			return
		}
		rt, err := ParseCIDString(c.String())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !c.Equal(rt) {
			t.Fatalf("round-trip CID mismatch")
		}
	})
}

// FuzzReadJSONString tests the hand-written JSON string parser never panics
// and round-trips through AppendJSONString. This parser handles escape sequences
// including UTF-16 surrogate pairs and is used by all generated UnmarshalJSON methods.
func FuzzReadJSONString(f *testing.F) {
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`""`))
	f.Add([]byte(`"abc\ndef"`))
	f.Add([]byte(`"escaped\"quote"`))
	f.Add([]byte(`"\u0041"`))                 // \uXXXX escape
	f.Add([]byte(`"\uD83D\uDE00"`))           // surrogate pair (😀)
	f.Add([]byte(`"\uD800"`))                 // lone high surrogate
	f.Add([]byte(`"hello\tworld\n"`))         // tab + newline escapes
	f.Add([]byte(`"\u003c\u003e\u0026"`))     // HTML-unsafe chars
	f.Add([]byte(`""`))                       // empty
	f.Add([]byte{})                           // no data
	f.Add([]byte(`"unterminated`))            // missing close quote
	f.Add([]byte(`"\`))                       // truncated escape

	f.Fuzz(func(t *testing.T, data []byte) {
		s, end, err := ReadJSONString(data, 0)
		if err != nil {
			return
		}
		if end > len(data) {
			t.Fatalf("consumed %d bytes but input only has %d", end, len(data))
		}
		// AppendJSONString replaces invalid UTF-8 with U+FFFD, so direct
		// comparison may fail on the first pass. Instead, verify stability:
		// encode → decode → encode must be identical from the second pass onward.
		encoded1 := AppendJSONString(nil, s)
		s2, _, err := ReadJSONString(encoded1, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		encoded2 := AppendJSONString(nil, s2)
		s3, end3, err := ReadJSONString(encoded2, 0)
		if err != nil {
			t.Fatalf("second round-trip decode failed: %v", err)
		}
		if s2 != s3 {
			t.Fatalf("round-trip not stable: %q vs %q", s2, s3)
		}
		if end3 != len(encoded2) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", end3, len(encoded2))
		}
	})
}

// FuzzReadJSONInt tests the hand-written JSON integer parser never panics
// and round-trips through AppendJSONInt.
func FuzzReadJSONInt(f *testing.F) {
	f.Add([]byte("0"))
	f.Add([]byte("42"))
	f.Add([]byte("-1"))
	f.Add([]byte("9007199254740992"))  // max safe JS integer
	f.Add([]byte("-9007199254740992")) // min safe JS integer
	f.Add([]byte(""))
	f.Add([]byte("-"))
	f.Add([]byte("abc"))

	f.Fuzz(func(t *testing.T, data []byte) {
		val, end, err := ReadJSONInt(data, 0)
		if err != nil {
			return
		}
		if end > len(data) {
			t.Fatalf("consumed %d bytes but input only has %d", end, len(data))
		}
		// Round-trip.
		encoded := AppendJSONInt(nil, val)
		val2, end2, err := ReadJSONInt(encoded, 0)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if val != val2 {
			t.Fatalf("round-trip mismatch: %d vs %d", val, val2)
		}
		if end2 != len(encoded) {
			t.Fatalf("round-trip consumed %d bytes, expected %d", end2, len(encoded))
		}
	})
}

// FuzzReadJSONFloat tests the hand-written JSON float parser never panics.
func FuzzReadJSONFloat(f *testing.F) {
	f.Add([]byte("0.0"))
	f.Add([]byte("3.14"))
	f.Add([]byte("-1.5"))
	f.Add([]byte("1e10"))
	f.Add([]byte("1.5E-3"))
	f.Add([]byte(""))
	f.Add([]byte("."))
	f.Add([]byte("1e"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ReadJSONFloat(data, 0)
	})
}

// FuzzReadJSONBool tests the hand-written JSON boolean parser never panics.
func FuzzReadJSONBool(f *testing.F) {
	f.Add([]byte("true"))
	f.Add([]byte("false"))
	f.Add([]byte(""))
	f.Add([]byte("tru"))
	f.Add([]byte("fals"))
	f.Add([]byte("TRUE"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ReadJSONBool(data, 0)
	})
}

// FuzzPeekJSONType tests the hand-written JSON scanner for $type extraction.
// Compares results against encoding/json for correctness.
func FuzzPeekJSONType(f *testing.F) {
	f.Add([]byte(`{"$type":"app.bsky.feed.post"}`))
	f.Add([]byte(`{"other":"val","$type":"app.bsky.actor.profile"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"$type":123}`)) // non-string $type
	f.Add([]byte(`{"nested":{"$type":"inner"}}`))
	f.Add([]byte(``))
	f.Add([]byte(`{`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`{ "$type" : "with spaces" }`))

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := PeekJSONType(data)
		if err != nil {
			return
		}
		// Cross-check against encoding/json when input is valid JSON.
		// PeekJSONType returns raw bytes without UTF-8 normalization,
		// while encoding/json replaces invalid UTF-8 with U+FFFD, so
		// only compare when the result is valid UTF-8.
		if !utf8.ValidString(got) {
			return
		}
		var m map[string]any
		if json.Unmarshal(data, &m) == nil {
			if typeVal, ok := m["$type"].(string); ok {
				if got != typeVal {
					t.Fatalf("PeekJSONType=%q but encoding/json=$type=%q", got, typeVal)
				}
			}
		}
	})
}

// FuzzFromJSONToJSON tests CBOR↔JSON conversion round-trips. FromJSON recognizes
// {"$bytes": "..."} and {"$link": "..."} sentinels, while ToJSON produces them.
func FuzzFromJSONToJSON(f *testing.F) {
	f.Add([]byte(`null`))
	f.Add([]byte(`true`))
	f.Add([]byte(`42`))
	f.Add([]byte(`3.14`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`{"$bytes":"aGVsbG8"}`))
	cid := ComputeCID(CodecDagCBOR, []byte("test"))
	f.Add([]byte(`{"$link":"` + cid.String() + `"}`))
	f.Add([]byte(`{"nested":{"$bytes":"YWJj"},"arr":[1,"two"]}`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))

	f.Fuzz(func(t *testing.T, data []byte) {
		val, err := FromJSON(data)
		if err != nil {
			return
		}
		// Re-encode to JSON.
		encoded, err := ToJSON(val)
		if err != nil {
			return
		}
		// Re-decode and re-encode should be stable.
		val2, err := FromJSON(encoded)
		if err != nil {
			t.Fatalf("round-trip FromJSON failed: %v", err)
		}
		encoded2, err := ToJSON(val2)
		if err != nil {
			t.Fatalf("round-trip ToJSON failed: %v", err)
		}
		if !bytes.Equal(encoded, encoded2) {
			t.Fatalf("JSON round-trip not stable:\n  first:  %s\n  second: %s", encoded, encoded2)
		}
	})
}

// FuzzUnmarshalReader tests that the io.Reader CBOR decoder agrees with the
// []byte decoder on all inputs. Both paths should accept/reject the same inputs
// and produce identical re-encoded output.
func FuzzUnmarshalReader(f *testing.F) {
	seeds := [][]byte{
		{0xf6},                  // null
		{0xf5},                  // true
		{0x00},                  // 0
		{0x63, 'a', 'b', 'c'},  // "abc"
		{0x82, 0x01, 0x02},     // [1, 2]
		{0xa1, 0x61, 'a', 0x01}, // {"a": 1}
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// UnmarshalReader is a streaming decoder — it reads one value and
		// stops without rejecting trailing bytes. Unmarshal wraps the same
		// decoder but additionally rejects trailing bytes. So we can only
		// compare when Unmarshal succeeds (no trailing bytes).
		val1, err1 := Unmarshal(data)
		if err1 != nil {
			// Unmarshal rejected it. UnmarshalReader must still not panic.
			_, _ = UnmarshalReader(bytes.NewReader(data))
			return
		}

		val2, err2 := UnmarshalReader(bytes.NewReader(data))
		if err2 != nil {
			t.Fatalf("Unmarshal succeeded but UnmarshalReader failed: %v", err2)
		}

		// Both values should re-encode identically.
		enc1, e1 := Marshal(val1)
		enc2, e2 := Marshal(val2)
		if (e1 == nil) != (e2 == nil) {
			t.Fatalf("Marshal after Unmarshal err=%v but after UnmarshalReader err=%v", e1, e2)
		}
		if e1 != nil {
			return
		}
		if !bytes.Equal(enc1, enc2) {
			t.Fatalf("Unmarshal and UnmarshalReader produced different values")
		}
	})
}
