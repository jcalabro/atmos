package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRecordKey_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "recordkey_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseRecordKey(v)
			require.NoError(t, err)
		})
	}
}

func TestParseRecordKey_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "recordkey_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseRecordKey(v)
			require.Error(t, err)
		})
	}
}

func TestRecordKey_DotDotRejected(t *testing.T) {
	t.Parallel()
	_, err := ParseRecordKey(".")
	require.Error(t, err)
	_, err = ParseRecordKey("..")
	require.Error(t, err)
}

func TestRecordKey_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	r, err := ParseRecordKey("abc123")
	require.NoError(t, err)
	b, err := r.MarshalText()
	require.NoError(t, err)
	var r2 RecordKey
	require.NoError(t, r2.UnmarshalText(b))
	require.Equal(t, r, r2)
}
