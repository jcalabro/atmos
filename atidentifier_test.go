package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAtIdentifier_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "atidentifier_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseAtIdentifier(v)
			require.NoError(t, err)
		})
	}
}

func TestParseAtIdentifier_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "atidentifier_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseAtIdentifier(v)
			require.Error(t, err)
		})
	}
}

func TestAtIdentifier_DID(t *testing.T) {
	t.Parallel()
	a, err := ParseAtIdentifier("did:plc:abc123")
	require.NoError(t, err)
	require.True(t, a.IsDID())
	require.False(t, a.IsHandle())
	d, err := a.AsDID()
	require.NoError(t, err)
	require.Equal(t, DID("did:plc:abc123"), d)
}

func TestAtIdentifier_Handle(t *testing.T) {
	t.Parallel()
	a, err := ParseAtIdentifier("alice.bsky.social")
	require.NoError(t, err)
	require.False(t, a.IsDID())
	require.True(t, a.IsHandle())
	h, err := a.AsHandle()
	require.NoError(t, err)
	require.Equal(t, Handle("alice.bsky.social"), h)
}

func TestAtIdentifier_Normalize(t *testing.T) {
	t.Parallel()
	a, err := ParseAtIdentifier("Alice.Bsky.Social")
	require.NoError(t, err)
	require.Equal(t, AtIdentifier("alice.bsky.social"), a.Normalize())
}
