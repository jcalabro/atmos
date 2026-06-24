package cbor

import (
	"bytes"
	"testing"
)

// benchLike mirrors an app.bsky.feed.like — the dominant record shape in the
// atproto firehose (a small, string-heavy object), so its decode cost governs
// real backfill throughput for consumers of this package.
var benchLike = map[string]any{
	"$type":     "app.bsky.feed.like",
	"createdAt": "2024-11-20T15:27:04.328Z",
	"subject": map[string]any{
		"cid": "bafyreiangzeq6wkzgdywr6x6mfzue6plymwjcn5yc45kx27migmeahilme",
		"uri": "at://did:plc:onwgs7pxf2cgtm5z4bh5mml3/app.bsky.feed.post/3lbetkiuwwc2a",
	},
}

// The three generic-decode entry points, benchmarked on the same record so the
// costs are directly comparable:
//
//   ReadValue       — streaming (io.Reader): per-string readN allocation + reader
//                     indirection. Slowest; what callers get if they wrap a []byte
//                     in bytes.NewReader.
//   Unmarshal       — slice-based, copying: no reader overhead, but each text/
//                     byte string is copied out so the result owns its memory.
//   UnmarshalNoCopy — slice-based, aliasing: strings/bytes point into the input,
//                     eliminating the per-string copy when the input outlives the
//                     result.

func BenchmarkDecodeReadValue_Like(b *testing.B) {
	data, _ := Marshal(benchLike)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := NewDecoder(bytes.NewReader(data)).ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeUnmarshal_Like(b *testing.B) {
	data, _ := Marshal(benchLike)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := Unmarshal(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeUnmarshalNoCopy_Like(b *testing.B) {
	data, _ := Marshal(benchLike)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := UnmarshalNoCopy(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeReadValue_Post(b *testing.B) {
	data, _ := Marshal(benchRecord)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := NewDecoder(bytes.NewReader(data)).ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeUnmarshal_Post(b *testing.B) {
	data, _ := Marshal(benchRecord)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := Unmarshal(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeUnmarshalNoCopy_Post(b *testing.B) {
	data, _ := Marshal(benchRecord)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := UnmarshalNoCopy(data); err != nil {
			b.Fatal(err)
		}
	}
}
