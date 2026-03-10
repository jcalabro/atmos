package cbor

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeCID(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	c := ComputeCID(CodecDagCBOR, data)
	require.True(t, c.Defined())
	require.Equal(t, uint64(1), c.Version())
	require.Equal(t, CodecDagCBOR, c.Codec())
	require.Equal(t, HashSHA256, c.HashCode())
	require.Equal(t, 32, c.HashLen())

	// Verify the hash is correct SHA-256.
	expected := sha256.Sum256(data)
	require.Equal(t, expected, c.Hash())
}

func TestCID_BytesRoundTrip(t *testing.T) {
	t.Parallel()
	original := ComputeCID(CodecDagCBOR, []byte("test"))
	b := original.Bytes()

	parsed, err := ParseCIDBytes(b)
	require.NoError(t, err)
	require.True(t, original.Equal(parsed))
}

func TestCID_StringRoundTrip(t *testing.T) {
	t.Parallel()
	original := ComputeCID(CodecRaw, []byte("blob data"))
	s := original.String()

	// Must start with 'b' (base32lower multibase).
	require.True(t, len(s) > 0)
	require.Equal(t, byte('b'), s[0])

	parsed, err := ParseCIDString(s)
	require.NoError(t, err)
	require.True(t, original.Equal(parsed))
}

func TestParseCIDString_RejectCIDv0(t *testing.T) {
	t.Parallel()
	// CIDv0 starts with 0x12 0x20 in binary; can't be represented with 'b' prefix
	// but test the bytes path.
	h := sha256.Sum256([]byte("test"))
	cidv0 := append([]byte{0x12, 0x20}, h[:]...)
	_, err := ParseCIDBytes(cidv0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CIDv0")
}

func TestParseCIDString_InvalidPrefix(t *testing.T) {
	t.Parallel()
	_, err := ParseCIDString("zNotBase32Lower")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported multibase prefix")
}

func TestParseCIDBytes_Empty(t *testing.T) {
	t.Parallel()
	_, err := ParseCIDBytes([]byte{})
	require.Error(t, err)
}

func TestCID_Equal(t *testing.T) {
	t.Parallel()
	a := ComputeCID(CodecDagCBOR, []byte("same"))
	b := ComputeCID(CodecDagCBOR, []byte("same"))
	c := ComputeCID(CodecDagCBOR, []byte("different"))
	require.True(t, a.Equal(b))
	require.False(t, a.Equal(c))
}

func TestCID_DifferentCodecs(t *testing.T) {
	t.Parallel()
	a := ComputeCID(CodecDagCBOR, []byte("data"))
	b := ComputeCID(CodecRaw, []byte("data"))
	// Same hash but different codec.
	require.False(t, a.Equal(b))
	require.Equal(t, a.Hash(), b.Hash())
}

func TestCID_Undefined(t *testing.T) {
	t.Parallel()
	var c CID
	require.False(t, c.Defined())
}

func TestCID_RejectWrongSHA256HashLen(t *testing.T) {
	t.Parallel()
	// Build a CIDv1 with SHA-256 hash code but wrong hash length (16 instead of 32).
	var buf []byte
	buf = AppendUvarint(buf, 1)            // version
	buf = AppendUvarint(buf, CodecDagCBOR) // codec
	buf = AppendUvarint(buf, HashSHA256)   // hash code 0x12
	buf = AppendUvarint(buf, 16)           // wrong hash length
	buf = append(buf, make([]byte, 16)...) // 16 zero bytes
	_, err := ParseCIDBytes(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash length must be 32")
}
