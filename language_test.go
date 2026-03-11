package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLanguage_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "language_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseLanguage(v)
			require.NoError(t, err)
		})
	}
}

func TestParseLanguage_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "language_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseLanguage(v)
			require.Error(t, err)
		})
	}
}

func TestLanguage_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	raw := "en-US"
	l, err := ParseLanguage(raw)
	require.NoError(t, err)
	b, err := l.MarshalText()
	require.NoError(t, err)
	require.Equal(t, raw, string(b))
	var l2 Language
	require.NoError(t, l2.UnmarshalText(b))
	require.Equal(t, l, l2)

	var bad Language
	require.Error(t, bad.UnmarshalText([]byte("")))
}
