package cbor

import (
	"reflect"
	"testing"
	"unsafe"
)

// fixtures spanning the value space NoCopy must handle identically to Unmarshal:
// nested maps, arrays, text (incl. empty + unicode), byte strings, ints, floats,
// bool, null, and a CID link.
func nocopyFixtures(t *testing.T) [][]byte {
	t.Helper()
	cid := ComputeCID(CodecDagCBOR, []byte("hello"))
	vals := []any{
		map[string]any{
			"$type":     "app.bsky.feed.like",
			"createdAt": "2024-11-20T15:27:04.328Z",
			"subject":   map[string]any{"cid": "bafy", "uri": "at://x/y/z"},
		},
		map[string]any{
			"i": int64(42), "n": int64(-7), "f": 3.5, "b": true, "z": nil,
			"s": "héllo wörld", "empty": "",
		},
		map[string]any{
			"arr": []any{int64(1), "two", false, nil, []any{int64(3)}},
			"bin": []byte{0x00, 0x01, 0xfe, 0xff},
			"lnk": cid,
		},
		"top-level-string",
		int64(123),
	}
	out := make([][]byte, 0, len(vals))
	for _, v := range vals {
		b, err := Marshal(v)
		if err != nil {
			t.Fatalf("marshal fixture: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// TestUnmarshalNoCopyMatchesUnmarshal pins that the zero-copy decode produces a
// value deep-equal to the copying decode across the value space. reflect.DeepEqual
// compares string and []byte contents by value, so aliasing vs. copying is
// invisible to the result — only the backing memory differs (asserted separately).
func TestUnmarshalNoCopyMatchesUnmarshal(t *testing.T) {
	t.Parallel()
	for i, data := range nocopyFixtures(t) {
		want, err := Unmarshal(data)
		if err != nil {
			t.Fatalf("fixture %d: Unmarshal: %v", i, err)
		}
		got, err := UnmarshalNoCopy(data)
		if err != nil {
			t.Fatalf("fixture %d: UnmarshalNoCopy: %v", i, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("fixture %d: NoCopy != Unmarshal\n want=%#v\n  got=%#v", i, want, got)
		}
	}
}

// TestUnmarshalNoCopyAliasesInput proves the contract: a string value decoded by
// UnmarshalNoCopy points INTO data (no copy), whereas Unmarshal's points
// elsewhere. We compare the backing-array address of a decoded string against the
// region of data that holds its bytes.
func TestUnmarshalNoCopyAliasesInput(t *testing.T) {
	t.Parallel()
	const text = "a-distinctive-value-string"
	data, err := Marshal(map[string]any{"k": text})
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalNoCopy(data)
	if err != nil {
		t.Fatal(err)
	}
	s := mustString(t, got, "k")
	if s != text {
		t.Fatalf("value mismatch: %q", s)
	}
	// The decoded string's bytes must lie within data's backing array.
	dataStart := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	dataEnd := dataStart + uintptr(len(data))
	strPtr := uintptr(unsafe.Pointer(unsafe.StringData(s)))
	if strPtr < dataStart || strPtr >= dataEnd {
		t.Fatalf("NoCopy string does not alias data: str=%#x data=[%#x,%#x)", strPtr, dataStart, dataEnd)
	}

	// And the copying Unmarshal must NOT alias data (sanity: proves the test can
	// distinguish the two).
	cp, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	cs := mustString(t, cp, "k")
	csPtr := uintptr(unsafe.Pointer(unsafe.StringData(cs)))
	if csPtr >= dataStart && csPtr < dataEnd {
		t.Fatalf("Unmarshal unexpectedly aliased data; test cannot distinguish copy from alias")
	}
}

// TestUnmarshalNoCopyByteStringAliases verifies a decoded byte string is a
// sub-slice of data (not a copy) under NoCopy.
func TestUnmarshalNoCopyByteStringAliases(t *testing.T) {
	t.Parallel()
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	data, err := Marshal(map[string]any{"b": payload})
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalNoCopy(data)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("decoded value is not a map: %T", got)
	}
	b, ok := m["b"].([]byte)
	if !ok {
		t.Fatalf("key b is not a byte string: %T", m["b"])
	}
	if string(b) != string(payload) {
		t.Fatalf("bytes mismatch: %x", b)
	}
	bPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	dataStart := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	dataEnd := dataStart + uintptr(len(data))
	if bPtr < dataStart || bPtr >= dataEnd {
		t.Fatalf("NoCopy byte string does not alias data")
	}
}

// mustString fetches a string value at key k from a decoded map, failing the
// test (rather than panicking on an unchecked assertion) if the shape is wrong.
func mustString(t *testing.T, decoded any, k string) string {
	t.Helper()
	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded value is not a map: %T", decoded)
	}
	s, ok := m[k].(string)
	if !ok {
		t.Fatalf("key %q is not a string: %T", k, m[k])
	}
	return s
}

// FuzzUnmarshalNoCopyMatches asserts NoCopy and Unmarshal agree on the
// accept/reject decision and (on success) the decoded value, for arbitrary input.
func FuzzUnmarshalNoCopyMatches(f *testing.F) {
	for _, fx := range nocopyFixtures(&testing.T{}) {
		f.Add(fx)
	}
	f.Add([]byte{0xf6})       // null
	f.Add([]byte{0x00})       // 0
	f.Add([]byte{0x60})       // ""
	f.Add([]byte{0x42, 1, 2}) // bytes(2)
	f.Fuzz(func(t *testing.T, data []byte) {
		want, wErr := Unmarshal(data)
		got, gErr := UnmarshalNoCopy(data)
		if (wErr == nil) != (gErr == nil) {
			t.Fatalf("accept/reject mismatch: Unmarshal err=%v, NoCopy err=%v", wErr, gErr)
		}
		if wErr != nil {
			return
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("value mismatch on accepted input\n want=%#v\n  got=%#v", want, got)
		}
	})
}
