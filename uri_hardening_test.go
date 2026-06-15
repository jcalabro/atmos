package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Nit: RFC 3986 schemes are case-insensitive; uppercase and mixed-case schemes
// must be accepted.
func TestParseURI_UppercaseScheme_Accepted(t *testing.T) {
	t.Parallel()
	for _, v := range []string{
		"HTTPS://example.com",
		"Http://example.com",
		"AT://did:plc:abc123",
	} {
		t.Run(v, func(t *testing.T) {
			_, err := ParseURI(v)
			require.NoError(t, err)
		})
	}
}

// A scheme must still begin with a letter, not a digit.
func TestParseURI_DigitLeadingScheme_Rejected(t *testing.T) {
	t.Parallel()
	_, err := ParseURI("1https://example.com")
	require.Error(t, err)
}
