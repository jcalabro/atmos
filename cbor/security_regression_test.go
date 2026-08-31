package cbor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkipValue_MapCountOverflow(t *testing.T) {
	t.Parallel()

	data := []byte{
		0xbb, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // (1<<63)+1 pairs
		0x61, 'x',
		0x01,
	}

	_, err := SkipValue(data, 0)
	require.Error(t, err, "overflowed map pair count must not skip only the key")
}

func TestSkipValue_RejectsNonDAGValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{"invalid utf-8 text", []byte{0x62, 0xff, 0xfe}},
		{"unsupported simple value", []byte{0xf8, 0x20}},
		{"unsupported tag", []byte{0xc1, 0x01}},
		{"non-text map key", []byte{0xa1, 0x00, 0x01}},
		{"unsorted map keys", []byte{0xa2, 0x61, 'y', 0x01, 0x61, 'x', 0x01}},
		{"duplicate map keys", []byte{0xa2, 0x61, 'x', 0x01, 0x61, 'x', 0x01}},
		{"oversized text length", append([]byte{0x5b, 0x00, 0x10, 0x00, 0x00}, make([]byte, 8)...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := SkipValue(tt.data, 0)
			require.Error(t, err)
		})
	}
}

func TestSkipValue_DuplicateEmptyMapKeys(t *testing.T) {
	t.Parallel()

	// {"": 1, "": 2} — an empty key has keyStart == keyEnd, which must not
	// be misread as "no previous key" when checking the next key.
	data := []byte{0xa2, 0x60, 0x01, 0x60, 0x02}
	_, err := SkipValue(data, 0)
	require.ErrorContains(t, err, "duplicate map key")

	// {"": 1, "a": 2} is canonically sorted (shorter key first) and must
	// still be accepted.
	data = []byte{0xa2, 0x60, 0x01, 0x61, 'a', 0x02}
	_, err = SkipValue(data, 0)
	require.NoError(t, err)
}

func TestReadHelpers_EnforceMaxSize(t *testing.T) {
	t.Parallel()

	// 4-byte length header declaring MaxSize+1 (default 1 MiB + 1) with no
	// payload: the size check must fire before the truncation check so a
	// tiny hostile header is reported as oversized, matching SkipValue.
	oversizedText := []byte{0x7a, 0x00, 0x10, 0x00, 0x01}
	oversizedBytes := []byte{0x5a, 0x00, 0x10, 0x00, 0x01}

	_, _, err := ReadText(oversizedText, 0)
	require.ErrorContains(t, err, "max size")

	_, _, _, err = ReadTextKey(oversizedText, 0)
	require.ErrorContains(t, err, "max size")

	_, _, err = ReadBytes(oversizedBytes, 0)
	require.ErrorContains(t, err, "max size")

	_, _, err = ReadBytesNoCopy(oversizedBytes, 0)
	require.ErrorContains(t, err, "max size")
}

func TestSkipValue_DepthBoundary(t *testing.T) {
	t.Parallel()

	for _, depth := range []int{MaxDepth, MaxDepth + 1} {
		data := make([]byte, 0, depth+1)
		for range depth {
			data = append(data, 0x81)
		}
		data = append(data, 0x00)

		_, err := SkipValue(data, 0)
		if depth <= MaxDepth {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}
