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

func TestAtIdentifier_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"did:plc:testuser1234567890abcde", "test.example.com"} {
		id, err := ParseAtIdentifier(raw)
		require.NoError(t, err)
		b, err := id.MarshalText()
		require.NoError(t, err)
		require.Equal(t, raw, string(b))
		var id2 AtIdentifier
		require.NoError(t, id2.UnmarshalText(b))
		require.Equal(t, id, id2)
	}
	var bad AtIdentifier
	require.Error(t, bad.UnmarshalText([]byte("")))
}

func TestAtIdentifier_DIDAndHandle(t *testing.T) {
	t.Parallel()
	did, _ := ParseAtIdentifier("did:plc:testuser1234567890abcde")
	require.Equal(t, DID("did:plc:testuser1234567890abcde"), did.DID())
	require.Equal(t, Handle(""), did.Handle())

	handle, _ := ParseAtIdentifier("test.example.com")
	require.Equal(t, DID(""), handle.DID())
	require.Equal(t, Handle("test.example.com"), handle.Handle())
}
