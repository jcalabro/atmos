package atmos

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpaceRef(t *testing.T) {
	t.Parallel()
	raw := "at://did:plc:authority/space/com.example.forum/gardening"
	ref, err := ParseSpaceRef(raw)
	require.NoError(t, err)
	require.Equal(t, DID("did:plc:authority"), ref.Authority())
	require.Equal(t, NSID("com.example.forum"), ref.Type())
	require.Equal(t, RecordKey("gardening"), ref.Key())
	require.Equal(t, raw, ref.String())

	data, err := json.Marshal(ref)
	require.NoError(t, err)
	var decoded SpaceRef
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, ref, decoded)
}

func TestSpaceURI(t *testing.T) {
	t.Parallel()
	raw := "at://did:plc:authority/space/com.example.forum/gardening/did:web:author.example/com.example.post/3abc"
	uri, err := ParseSpaceURI(raw)
	require.NoError(t, err)
	require.Equal(t, SpaceRef("at://did:plc:authority/space/com.example.forum/gardening"), uri.Space())
	require.Equal(t, DID("did:web:author.example"), uri.Author())
	require.Equal(t, NSID("com.example.post"), uri.Collection())
	require.Equal(t, RecordKey("3abc"), uri.RecordKey())
}

func TestSpaceAddressInvalid(t *testing.T) {
	t.Parallel()
	validRef := "at://did:plc:authority/space/com.example.forum/gardening"
	validURI := validRef + "/did:plc:author/com.example.post/3abc"
	tests := []string{
		"", "at://alice.example/space/com.example.forum/gardening",
		"at://did:plc:authority/space", "at://did:plc:authority/space/com.example.forum",
		validRef + "/", validRef + "?x=1", validRef + "#/field",
		validRef + "/did:plc:author", validRef + "/did:plc:author/com.example.post",
		validURI + "/extra", strings.Replace(validURI, "did:plc:author", "author.example", 1),
	}
	for _, raw := range tests {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			_, refErr := ParseSpaceRef(raw)
			_, uriErr := ParseSpaceURI(raw)
			require.Error(t, refErr)
			require.Error(t, uriErr)
		})
	}
	_, err := ParseSpaceRef(validURI)
	require.Error(t, err)
	_, err = ParseSpaceURI(validRef)
	require.Error(t, err)
}

func TestValidateLexiconATURI(t *testing.T) {
	t.Parallel()
	valid := []string{
		"at://did:plc:abc/app.example.post/key",
		"at://did:plc:abc/app.example.post/key#/field/0",
		"at://did:plc:abc/app.example.post/key#/a~0b/a~1b",
		"at://did:plc:abc/space/com.example.forum/default",
		"at://did:plc:abc/space/com.example.forum/default#/field",
		"at://did:plc:abc/space/com.example.forum/default/did:plc:author/com.example.post/key#/field",
	}
	for _, raw := range valid {
		require.NoError(t, ValidateLexiconATURI(raw), raw)
	}
	invalid := []string{
		"at://did:plc:abc/app.example.post/key?x=1",
		"at://did:plc:abc/app.example.post/key#field",
		"at://did:plc:abc/app.example.post/key#/%ff",
		"at://did:plc:abc/app.example.post/key#/a~2b",
		"at://did:plc:abc/app.example.post/key#/a%7E2b",
		"at://alice.example/space/com.example.forum/default",
		"at://did:plc:abc/space/com.example.forum/default/did:plc:author",
	}
	for _, raw := range invalid {
		require.Error(t, ValidateLexiconATURI(raw), raw)
	}
}

func FuzzSpaceAddresses(f *testing.F) {
	f.Add("at://did:plc:abc/space/com.example.forum/default")
	f.Add("at://did:plc:abc/space/com.example.forum/default/did:plc:author/com.example.post/key")
	f.Fuzz(func(t *testing.T, raw string) {
		if ref, err := ParseSpaceRef(raw); err == nil {
			require.Equal(t, raw, ref.String())
			require.NoError(t, ref.Validate())
		}
		if uri, err := ParseSpaceURI(raw); err == nil {
			require.Equal(t, raw, uri.String())
			require.NoError(t, uri.Validate())
			require.NoError(t, uri.Space().Validate())
		}
	})
}

func FuzzLexiconATURI(f *testing.F) {
	f.Add("at://did:plc:abc/app.example.post/key#/field/0")
	f.Add("at://did:plc:abc/space/com.example.forum/default/did:plc:author/com.example.post/key")
	f.Fuzz(func(t *testing.T, raw string) {
		_ = ValidateLexiconATURI(raw)
	})
}
