package cbor

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// allocBytes measures the heap bytes allocated while running fn. TotalAlloc is
// process-global, so concurrently running parallel tests can inflate a single
// measurement; taking the minimum over several runs filters out that additive
// noise and converges on fn's own allocation.
func allocBytes(fn func()) uint64 {
	const runs = 8
	best := ^uint64(0)
	for range runs {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		fn()
		runtime.ReadMemStats(&after)
		if d := after.TotalAlloc - before.TotalAlloc; d < best {
			best = d
		}
	}
	return best
}

// C3: a tiny input declaring a huge array must error without first allocating
// proportional to the declared count.
func TestUnmarshal_HugeArrayHeader_NoAmplification(t *testing.T) {
	t.Parallel()
	// 0x9a = array(uint32 length), 0x00100000 = 1,048,576 elements.
	data := []byte{0x9a, 0x00, 0x10, 0x00, 0x00}

	var err error
	got := allocBytes(func() { _, err = Unmarshal(data) })
	require.Error(t, err)
	// 1M *any (8 bytes) would be ~16 MB. A bounded decoder allocates far less.
	require.Less(t, got, uint64(1<<20), "decoder allocated %d bytes for a 5-byte input", got)
}

// C3: same for maps.
func TestUnmarshal_HugeMapHeader_NoAmplification(t *testing.T) {
	t.Parallel()
	// 0xba = map(uint32 length), 0x00100000 entries.
	data := []byte{0xba, 0x00, 0x10, 0x00, 0x00}

	var err error
	got := allocBytes(func() { _, err = Unmarshal(data) })
	require.Error(t, err)
	require.Less(t, got, uint64(1<<20), "decoder allocated %d bytes for a 5-byte input", got)
}

// C3: the streaming decoder cannot bound against remaining length but must
// still not pre-allocate proportional to an attacker-declared count.
func TestUnmarshalReader_HugeArrayHeader_NoAmplification(t *testing.T) {
	t.Parallel()
	data := []byte{0x9a, 0x00, 0x10, 0x00, 0x00}

	var err error
	got := allocBytes(func() { _, err = UnmarshalReader(bytes.NewReader(data)) })
	require.Error(t, err)
	require.Less(t, got, uint64(1<<20), "reader allocated %d bytes for a 5-byte input", got)
}

func TestUnmarshalReader_HugeMapHeader_NoAmplification(t *testing.T) {
	t.Parallel()
	data := []byte{0xba, 0x00, 0x10, 0x00, 0x00}

	var err error
	got := allocBytes(func() { _, err = UnmarshalReader(bytes.NewReader(data)) })
	require.Error(t, err)
	require.Less(t, got, uint64(1<<20), "reader allocated %d bytes for a 5-byte input", got)
}

// cidLinkBytes builds a tag-42 CID link wrapping the given inner CID bytes.
func cidLinkBytes(inner []byte) []byte {
	payload := append([]byte{0x00}, inner...) // identity multibase prefix
	out := []byte{0xd8, 0x2a}                 // tag 42
	out = AppendBytes(out, payload)
	return out
}

// validInnerCID returns the raw bytes of a valid CIDv1 dag-cbor SHA-256 CID.
func validInnerCID() []byte {
	return ComputeCID(CodecDagCBOR, []byte("x")).Bytes()
}

// H2: a CID link whose byte string carries bytes beyond the multihash is
// non-canonical and must be rejected on BOTH the slice and reader paths.
func TestCIDLink_TrailingBytes_RejectedBothPaths(t *testing.T) {
	t.Parallel()
	inner := append(validInnerCID(), 0xff, 0xff) // 2 trailing bytes
	data := cidLinkBytes(inner)

	_, errSlice := Unmarshal(data)
	require.Error(t, errSlice, "slice path must reject trailing CID bytes")

	_, errReader := UnmarshalReader(bytes.NewReader(data))
	require.Error(t, errReader, "reader path must reject trailing CID bytes")
}

// H2 specialized path: ReadCIDLink is used by fast generated decoders and MST
// loading, so it must reject the same non-canonical CID-link payloads as the
// generic Unmarshal paths.
func TestReadCIDLink_TrailingBytes_Rejected(t *testing.T) {
	t.Parallel()
	inner := append(validInnerCID(), 0xff, 0xff)
	data := cidLinkBytes(inner)

	_, _, err := ReadCIDLink(data, 0)
	require.Error(t, err)
}

// A tag-42 byte string with an impossible declared payload length must be
// rejected before any uint64 -> int conversion or slice bounds arithmetic.
func TestReadCIDLink_HugeByteStringLength_Rejected(t *testing.T) {
	t.Parallel()
	data := []byte{
		0xd8, 0x2a, // tag 42
		0x5b,                                           // bytes(uint64 length)
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // MaxUint64
	}

	_, _, err := ReadCIDLink(data, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated")
}

// M1: a CID link whose internal varints are non-minimally encoded is
// non-canonical and must be rejected on both paths.
func TestCIDLink_NonMinimalVersionVarint_RejectedBothPaths(t *testing.T) {
	t.Parallel()
	// Encode version 1 as a non-minimal 2-byte varint [0x81, 0x00], then a
	// normal dag-cbor codec, sha-256 code, length 32, and 32 hash bytes.
	inner := []byte{0x81, 0x00, 0x71, 0x12, 0x20}
	inner = append(inner, make([]byte, 32)...)
	data := cidLinkBytes(inner)

	_, errSlice := Unmarshal(data)
	require.Error(t, errSlice, "slice path must reject non-minimal CID varint")

	_, errReader := UnmarshalReader(bytes.NewReader(data))
	require.Error(t, errReader, "reader path must reject non-minimal CID varint")
}

// A canonical CID link must still decode on both paths and agree.
func TestCIDLink_Canonical_DecodesBothPaths(t *testing.T) {
	t.Parallel()
	data := cidLinkBytes(validInnerCID())

	vSlice, err := Unmarshal(data)
	require.NoError(t, err)
	vReader, err := UnmarshalReader(bytes.NewReader(data))
	require.NoError(t, err)

	cSlice, ok := vSlice.(CID)
	require.True(t, ok)
	cReader, ok := vReader.(CID)
	require.True(t, ok)
	require.True(t, cSlice.Equal(cReader))
}

// ParseCIDPrefix must reject non-minimal internal varints even though it
// tolerates trailing bytes (it is used to parse a CID from the front of a
// larger buffer, e.g. a CAR block).
func TestParseCIDPrefix_NonMinimalVarint_Rejected(t *testing.T) {
	t.Parallel()
	inner := []byte{0x81, 0x00, 0x71, 0x12, 0x20}
	inner = append(inner, make([]byte, 32)...)
	_, _, err := ParseCIDPrefix(inner)
	require.Error(t, err)
}

// Honest arrays and maps that fit their declared size must still decode.
func TestUnmarshal_HonestArrayAndMap_StillWork(t *testing.T) {
	t.Parallel()
	// [1, 2, 3]
	arr, err := Unmarshal([]byte{0x83, 0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), int64(2), int64(3)}, arr)

	// {"a": 1}
	m, err := Unmarshal([]byte{0xa1, 0x61, 'a', 0x01})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": int64(1)}, m)
}
