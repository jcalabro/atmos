package cbor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVarint_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1<<32 - 1, 1 << 32, 1<<63 - 1}
	for _, val := range cases {
		buf := AppendUvarint(nil, val)
		decoded, n, err := ReadUvarint(buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, val, decoded, "round-trip failed for %d", val)
	}
}

func TestVarint_Truncated(t *testing.T) {
	t.Parallel()
	_, _, err := ReadUvarint([]byte{0x80}) // continuation bit set but no next byte
	require.Error(t, err)
}

func TestVarint_Empty(t *testing.T) {
	t.Parallel()
	_, _, err := ReadUvarint([]byte{})
	require.Error(t, err)
}
