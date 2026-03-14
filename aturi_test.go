package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseATURI_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "aturi_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseATURI(v)
			require.NoError(t, err)
		})
	}
}

func TestParseATURI_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "aturi_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseATURI(v)
			require.Error(t, err)
		})
	}
}

func TestATURI_Parts(t *testing.T) {
	t.Parallel()
	a, err := ParseATURI("at://did:plc:abc123/app.bsky.feed.post/tid123")
	require.NoError(t, err)
	require.Equal(t, ATIdentifier("did:plc:abc123"), a.Authority())
	require.Equal(t, NSID("app.bsky.feed.post"), a.Collection())
	require.Equal(t, RecordKey("tid123"), a.RecordKey())
	require.Equal(t, "app.bsky.feed.post/tid123", a.Path())
}

func TestATURI_AuthorityOnly(t *testing.T) {
	t.Parallel()
	a, err := ParseATURI("at://did:plc:abc123")
	require.NoError(t, err)
	require.Equal(t, ATIdentifier("did:plc:abc123"), a.Authority())
	require.Equal(t, NSID(""), a.Collection())
	require.Equal(t, RecordKey(""), a.RecordKey())
}

func TestATURI_Normalize(t *testing.T) {
	t.Parallel()
	a, err := ParseATURI("at://Alice.Bsky.Social/COM.Example.fooBar/tid123")
	require.NoError(t, err)
	require.Equal(t, ATURI("at://alice.bsky.social/com.example.fooBar/tid123"), a.Normalize())
}

func TestATURI_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	raw := "at://did:plc:testuser1234567890abcde/app.bsky.feed.post/3jqfcqzm3fo2j"
	u, err := ParseATURI(raw)
	require.NoError(t, err)
	b, err := u.MarshalText()
	require.NoError(t, err)
	require.Equal(t, raw, string(b))
	var u2 ATURI
	require.NoError(t, u2.UnmarshalText(b))
	require.Equal(t, u, u2)

	var bad ATURI
	require.Error(t, bad.UnmarshalText([]byte("not-an-aturi")))
}
