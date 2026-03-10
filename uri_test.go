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
