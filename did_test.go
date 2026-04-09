package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDID_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "did_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			d, err := ParseDID(v)
			require.NoError(t, err)
			require.Equal(t, v, d.String())
		})
	}
}

func TestParseDID_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "did_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDID(v)
			require.Error(t, err)
		})
	}
}

func TestDID_Methods(t *testing.T) {
	t.Parallel()
	d, err := ParseDID("did:plc:abcde1234")
	require.NoError(t, err)
	require.Equal(t, "plc", d.Method())
	require.Equal(t, "abcde1234", d.Identifier())
	require.Equal(t, ATIdentifier("did:plc:abcde1234"), d.ATIdentifier())
}

func TestDID_ZeroValue(t *testing.T) {
	t.Parallel()
	var d DID
	require.Equal(t, "", d.Method())
	require.Equal(t, "", d.Identifier())
	require.Equal(t, "", d.String())
}

func TestDID_PercentEncodingRejected(t *testing.T) {
	t.Parallel()
	// Percent-encoding is not valid in AT Protocol DIDs.
	for _, s := range []string{
		"did:example:abc%2Fdef",
		"did:example:abc%zzdef",
		"did:example:abc%2",
		"did:method:val%BB",
		"did:web:localhost%3A1234",
	} {
		_, err := ParseDID(s)
		require.Error(t, err, s)
	}
}

func TestDID_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	d, err := ParseDID("did:web:example.com")
	require.NoError(t, err)
	b, err := d.MarshalText()
	require.NoError(t, err)
	var d2 DID
	require.NoError(t, d2.UnmarshalText(b))
	require.Equal(t, d, d2)
}
