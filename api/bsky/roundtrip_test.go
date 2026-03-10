package bsky

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/gt"
	comatproto "github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

const testCID = "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq"

func roundTripCBOR[T any](t *testing.T,
	original *T,
	marshal func(*T) ([]byte, error),
	unmarshal func(*T, []byte) error,
) {
	t.Helper()
	data, err := marshal(original)
	require.NoError(t, err)
	var decoded T
	require.NoError(t, unmarshal(&decoded, data))
	assert.Equal(t, *original, decoded)
}

func roundTripJSON[T any](t *testing.T,
	original *T,
	marshal func(*T) ([]byte, error),
	unmarshal func(*T, []byte) error,
) {
	t.Helper()
	data, err := marshal(original)
	require.NoError(t, err)
	assert.True(t, json.Valid(data), "invalid JSON: %s", data)
	var decoded T
	require.NoError(t, unmarshal(&decoded, data))
	assert.Equal(t, *original, decoded)
}

// --- FeedPost (record with unions, nested objects, arrays, optionals) ---

func TestFeedPost_RoundTrip(t *testing.T) {
	t.Parallel()

	post := FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "Hello, world! @alice.bsky.social check this out",
		CreatedAt:     "2024-06-15T12:00:00Z",
		Langs:         []string{"en", "fr"},
		Tags:          []string{"test", "atproto"},
		Reply: gt.Some(FeedPost_ReplyRef{
			Root: comatproto.RepoStrongRef{
				CID: testCID,
				URI: "at://did:plc:abc/app.bsky.feed.post/root",
			},
			Parent: comatproto.RepoStrongRef{
				CID: testCID,
				URI: "at://did:plc:abc/app.bsky.feed.post/parent",
			},
		}),
		Facets: []RichtextFacet{
			{
				Index: RichtextFacet_ByteSlice{ByteStart: 14, ByteEnd: 34},
				Features: []RichtextFacet_Features{
					{RichtextFacet_Mention: gt.SomeRef(RichtextFacet_Mention{
						DID: "did:plc:alice",
					})},
				},
			},
			{
				Index: RichtextFacet_ByteSlice{ByteStart: 35, ByteEnd: 49},
				Features: []RichtextFacet_Features{
					{RichtextFacet_Link: gt.SomeRef(RichtextFacet_Link{
						URI: "https://example.com",
					})},
				},
			},
		},
		Embed: gt.Some(FeedPost_Embed{
			EmbedImages: gt.SomeRef(EmbedImages{
				Images: []EmbedImages_Image{
					{
						Alt: "A test image",
						Image: lextypes.LexBlob{
							Type:     "blob",
							Ref:      lextypes.LexCIDLink{Link: testCID},
							MimeType: "image/jpeg",
							Size:     12345,
						},
						AspectRatio: gt.Some(EmbedDefs_AspectRatio{
							Width:  1920,
							Height: 1080,
						}),
					},
				},
			}),
		}),
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		roundTripCBOR(t, &post,
			func(v *FeedPost) ([]byte, error) { return v.MarshalCBOR() },
			func(v *FeedPost, b []byte) error { return v.UnmarshalCBOR(b) },
		)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		roundTripJSON(t, &post,
			func(v *FeedPost) ([]byte, error) { return v.MarshalJSON() },
			func(v *FeedPost, b []byte) error { return v.UnmarshalJSON(b) },
		)
	})
}

// --- FeedPost_TextSlice (simple object with int64 fields) ---

func TestFeedPost_TextSlice_RoundTrip(t *testing.T) {
	t.Parallel()

	s := FeedPost_TextSlice{Start: 0, End: 42}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		roundTripCBOR(t, &s,
			func(v *FeedPost_TextSlice) ([]byte, error) { return v.MarshalCBOR() },
			func(v *FeedPost_TextSlice, b []byte) error { return v.UnmarshalCBOR(b) },
		)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		roundTripJSON(t, &s,
			func(v *FeedPost_TextSlice) ([]byte, error) { return v.MarshalJSON() },
			func(v *FeedPost_TextSlice, b []byte) error { return v.UnmarshalJSON(b) },
		)
	})
}

// --- EmbedImages (blob, CID link, array of objects) ---

func TestEmbedImages_RoundTrip(t *testing.T) {
	t.Parallel()

	v := EmbedImages{
		LexiconTypeID: "app.bsky.embed.images",
		Images: []EmbedImages_Image{
			{
				Alt: "photo of a cat",
				Image: lextypes.LexBlob{
					Type:     "blob",
					Ref:      lextypes.LexCIDLink{Link: testCID},
					MimeType: "image/png",
					Size:     99999,
				},
			},
			{
				Alt: "photo of a dog",
				Image: lextypes.LexBlob{
					Type:     "blob",
					Ref:      lextypes.LexCIDLink{Link: testCID},
					MimeType: "image/jpeg",
					Size:     54321,
				},
				AspectRatio: gt.Some(EmbedDefs_AspectRatio{Width: 800, Height: 600}),
			},
		},
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		roundTripCBOR(t, &v,
			func(v *EmbedImages) ([]byte, error) { return v.MarshalCBOR() },
			func(v *EmbedImages, b []byte) error { return v.UnmarshalCBOR(b) },
		)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		roundTripJSON(t, &v,
			func(v *EmbedImages) ([]byte, error) { return v.MarshalJSON() },
			func(v *EmbedImages, b []byte) error { return v.UnmarshalJSON(b) },
		)
	})
}

// --- RichtextFacet (nested objects, union array) ---

func TestRichtextFacet_RoundTrip(t *testing.T) {
	t.Parallel()

	v := RichtextFacet{
		Index: RichtextFacet_ByteSlice{ByteStart: 0, ByteEnd: 5},
		Features: []RichtextFacet_Features{
			{RichtextFacet_Tag: gt.SomeRef(RichtextFacet_Tag{Tag: "golang"})},
		},
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		roundTripCBOR(t, &v,
			func(v *RichtextFacet) ([]byte, error) { return v.MarshalCBOR() },
			func(v *RichtextFacet, b []byte) error { return v.UnmarshalCBOR(b) },
		)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		roundTripJSON(t, &v,
			func(v *RichtextFacet) ([]byte, error) { return v.MarshalJSON() },
			func(v *RichtextFacet, b []byte) error { return v.UnmarshalJSON(b) },
		)
	})
}

// --- LexBlob + LexCIDLink (CID tag 42 in CBOR, $link in JSON) ---

func TestLexBlob_RoundTrip(t *testing.T) {
	t.Parallel()

	v := lextypes.LexBlob{
		Type:     "blob",
		Ref:      lextypes.LexCIDLink{Link: testCID},
		MimeType: "application/octet-stream",
		Size:     1048576,
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		roundTripCBOR(t, &v,
			func(v *lextypes.LexBlob) ([]byte, error) { return v.MarshalCBOR() },
			func(v *lextypes.LexBlob, b []byte) error { return v.UnmarshalCBOR(b) },
		)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		roundTripJSON(t, &v,
			func(v *lextypes.LexBlob) ([]byte, error) { return v.MarshalJSON() },
			func(v *lextypes.LexBlob, b []byte) error { return v.UnmarshalJSON(b) },
		)
	})
}

// --- ActorGetPreferences_Output (endpoint with ref-to-array) ---

func TestActorGetPreferences_Output_RoundTrip(t *testing.T) {
	t.Parallel()

	v := ActorGetPreferences_Output{
		Preferences: ActorDefs_Preferences{
			{ActorDefs_AdultContentPref: gt.SomeRef(ActorDefs_AdultContentPref{
				Enabled: true,
			})},
		},
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		roundTripCBOR(t, &v,
			func(v *ActorGetPreferences_Output) ([]byte, error) { return v.MarshalCBOR() },
			func(v *ActorGetPreferences_Output, b []byte) error { return v.UnmarshalCBOR(b) },
		)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		roundTripJSON(t, &v,
			func(v *ActorGetPreferences_Output) ([]byte, error) { return v.MarshalJSON() },
			func(v *ActorGetPreferences_Output, b []byte) error { return v.UnmarshalJSON(b) },
		)
	})
}

// --- Edge cases ---

func TestFeedPost_NilVsEmptyArrays(t *testing.T) {
	t.Parallel()

	t.Run("nil_arrays", func(t *testing.T) {
		t.Parallel()
		post := FeedPost{Text: "hello", CreatedAt: "2024-01-01T00:00:00Z"}
		// Langs and Tags are nil

		data, err := post.MarshalCBOR()
		require.NoError(t, err)
		var decoded FeedPost
		require.NoError(t, decoded.UnmarshalCBOR(data))
		assert.Nil(t, decoded.Langs)
		assert.Nil(t, decoded.Tags)

		jdata, err := post.MarshalJSON()
		require.NoError(t, err)
		var jdecoded FeedPost
		require.NoError(t, jdecoded.UnmarshalJSON(jdata))
		assert.Nil(t, jdecoded.Langs)
		assert.Nil(t, jdecoded.Tags)
	})

	t.Run("empty_arrays", func(t *testing.T) {
		t.Parallel()
		post := FeedPost{
			Text:      "hello",
			CreatedAt: "2024-01-01T00:00:00Z",
			Langs:     []string{},
			Tags:      []string{},
		}

		data, err := post.MarshalCBOR()
		require.NoError(t, err)
		var decoded FeedPost
		require.NoError(t, decoded.UnmarshalCBOR(data))
		// Empty slices may decode as nil — that's OK for omitempty semantics.
		// The important thing is no error.
		assert.Empty(t, decoded.Langs)
		assert.Empty(t, decoded.Tags)
	})
}

func TestFeedPost_ZeroOptionFields(t *testing.T) {
	t.Parallel()

	post := FeedPost{
		Text:      "minimal",
		CreatedAt: "2024-01-01T00:00:00Z",
		// All gt.Option fields left as None
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		data, err := post.MarshalCBOR()
		require.NoError(t, err)
		var decoded FeedPost
		require.NoError(t, decoded.UnmarshalCBOR(data))
		assert.False(t, decoded.Embed.HasVal())
		assert.False(t, decoded.Reply.HasVal())
		assert.False(t, decoded.Labels.HasVal())
	})

	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		data, err := post.MarshalJSON()
		require.NoError(t, err)
		var decoded FeedPost
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.False(t, decoded.Embed.HasVal())
		assert.False(t, decoded.Reply.HasVal())
		assert.False(t, decoded.Labels.HasVal())
	})
}

func TestFeedPost_TypeFieldPreservation(t *testing.T) {
	t.Parallel()

	t.Run("with_type", func(t *testing.T) {
		t.Parallel()
		post := FeedPost{
			LexiconTypeID: "app.bsky.feed.post",
			Text:          "hello",
			CreatedAt:     "2024-01-01T00:00:00Z",
		}
		data, err := post.MarshalJSON()
		require.NoError(t, err)
		var decoded FeedPost
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.Equal(t, "app.bsky.feed.post", decoded.LexiconTypeID)
	})

	t.Run("without_type", func(t *testing.T) {
		t.Parallel()
		post := FeedPost{
			Text:      "hello",
			CreatedAt: "2024-01-01T00:00:00Z",
		}
		data, err := post.MarshalJSON()
		require.NoError(t, err)
		var decoded FeedPost
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.Empty(t, decoded.LexiconTypeID)
	})
}
