package streaming

import "testing"

// FuzzDecodeFrame tests that decodeFrame never panics on arbitrary input.
// This is the entry point for all firehose data from untrusted network sources.
func FuzzDecodeFrame(f *testing.F) {
	// Valid frame: header {op:1, t:"#commit"} + empty body map.
	f.Add([]byte{
		0xa2,             // map(2)
		0x62, 0x6f, 0x70, // "op"
		0x01,       // 1
		0x61, 0x74, // "t"
		0x67, 0x23, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, // "#commit"
		0xa0, // map(0) - empty body
	})
	// Error frame: op=-1.
	f.Add([]byte{
		0xa2,
		0x62, 0x6f, 0x70,
		0x20, // -1
		0x61, 0x74,
		0x65, 0x23, 0x69, 0x6e, 0x66, 0x6f, // "#info"
		0xa0,
	})
	f.Add([]byte{})
	f.Add([]byte{0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic.
		_, _ = decodeFrame(data)
	})
}

// FuzzDecodeFrameHeader tests that decodeFrameHeader never panics.
func FuzzDecodeFrameHeader(f *testing.F) {
	f.Add([]byte{0xa2, 0x62, 0x6f, 0x70, 0x01, 0x61, 0x74, 0x67, 0x23, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74})
	f.Add([]byte{})
	f.Add([]byte{0xa0})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeFrameHeader(data)
	})
}
