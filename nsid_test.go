package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNSID_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "nsid_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseNSID(v)
			require.NoError(t, err)
		})
	}
}

func TestParseNSID_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "nsid_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseNSID(v)
			require.Error(t, err)
		})
	}
}

func TestNSID_Authority(t *testing.T) {
	t.Parallel()
	n, err := ParseNSID("com.example.fooBar")
	require.NoError(t, err)
	require.Equal(t, "example.com", n.Authority())
}

func TestNSID_Name(t *testing.T) {
	t.Parallel()
	n, err := ParseNSID("com.example.fooBar")
	require.NoError(t, err)
	require.Equal(t, "fooBar", n.Name())
}

func TestNSID_Normalize(t *testing.T) {
	t.Parallel()
	n, err := ParseNSID("COM.Example.fooBar")
	require.NoError(t, err)
	require.Equal(t, NSID("com.example.fooBar"), n.Normalize())
}

func TestNSID_ZeroValue(t *testing.T) {
	t.Parallel()
	var n NSID
	require.Equal(t, "", n.Authority())
	require.Equal(t, "", n.Name())
}
