package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseURI_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "uri_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseURI(v)
			require.NoError(t, err)
		})
	}
}

func TestParseURI_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "uri_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseURI(v)
			require.Error(t, err)
		})
	}
}

func TestParseURI_SchemeWithDigitsAndPlus(t *testing.T) {
	t.Parallel()
	valid := []string{
		"coap+tcp://example.com/path",
		"h323://example.com",
		"z39.50r://example.com",
	}
	for _, v := range valid {
		t.Run(v, func(t *testing.T) {
			_, err := ParseURI(v)
			require.NoError(t, err)
		})
	}
}

func TestURI_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	raw := "https://example.com/path"
	u, err := ParseURI(raw)
	require.NoError(t, err)
	b, err := u.MarshalText()
	require.NoError(t, err)
	require.Equal(t, raw, string(b))
	var u2 URI
	require.NoError(t, u2.UnmarshalText(b))
	require.Equal(t, u, u2)

	var bad URI
	require.Error(t, bad.UnmarshalText([]byte("")))
}
