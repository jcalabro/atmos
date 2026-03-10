package bsky_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos/api/bsky"
	comatproto "github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readFixture reads a CBOR fixture from testdata/.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

// jsonSemEqual asserts two JSON blobs are semantically equal.
func jsonSemEqual(t *testing.T, expected, actual []byte) {
	t.Helper()
	var a, b any
	require.NoError(t, json.Unmarshal(expected, &a))
	require.NoError(t, json.Unmarshal(actual, &b))
	assert.Equal(t, a, b)
}

// --- Interop: FeedPost with EmbedRecordWithMedia (from indigo) ---

func TestFeedPostInterop_CBOR(t *testing.T) {
	t.Parallel()

	data := readFixture(t, "feedpost_record.cbor")

	var fp bsky.FeedPost
	require.NoError(t, fp.UnmarshalCBOR(data))

	// Field assertions.
	assert.Equal(t, "Who the hell do you think you are", fp.Text)
	assert.Equal(t, "2023-03-29T20:59:19.417Z", fp.CreatedAt)

	// Embed variant: must be EmbedRecordWithMedia.
	require.True(t, fp.Embed.HasVal())
	embed := fp.Embed.Val()
	require.True(t, embed.EmbedRecordWithMedia.HasVal())
	assert.False(t, embed.EmbedImages.HasVal())
	assert.False(t, embed.EmbedExternal.HasVal())
	assert.False(t, embed.EmbedRecord.HasVal())

	rwm := embed.EmbedRecordWithMedia.Val()

	// Media: EmbedImages with 1 image.
	require.True(t, rwm.Media.EmbedImages.HasVal())
	imgs := rwm.Media.EmbedImages.Val()
	require.Len(t, imgs.Images, 1)
	img := imgs.Images[0]
	assert.Equal(t, "", img.Alt)
	assert.Equal(t, "image/jpeg", img.Image.MimeType)
	assert.Equal(t, int64(751473), img.Image.Size)
	assert.Equal(t, "bafkreieqq463374bbcbeq7gpmet5rvrpeqow6t4rtjzrkhnlumdylagaqa", img.Image.Ref.Link)

	// Record ref.
	assert.Equal(t, "bafyreiaku7udekkiijxcuue3sn6esz7qijqj637rigz4xqdw57fk5houji", rwm.Record.Record.CID)
	assert.Equal(t, "at://did:plc:rbtury4cp2sdk4tvnedaqu54/app.bsky.feed.post/3jilislho4s2k", rwm.Record.Record.URI)

	// Re-encode to CBOR: byte-for-byte match.
	fp.LexiconTypeID = "app.bsky.feed.post"
	reencoded, err := fp.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, data, reencoded, "CBOR round-trip must be byte-for-byte identical")

	// CID is computable and non-empty.
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	assert.NotEmpty(t, cid.String())
}

func TestFeedPostInterop_JSON(t *testing.T) {
	t.Parallel()

	data := readFixture(t, "feedpost_record.cbor")

	var fp bsky.FeedPost
	require.NoError(t, fp.UnmarshalCBOR(data))
	fp.LexiconTypeID = "app.bsky.feed.post"

	jsonBytes, err := json.Marshal(fp)
	require.NoError(t, err)
	s := string(jsonBytes)

	// Verify no Go field names leak into JSON.
	assert.NotContains(t, s, `"EmbedImages"`)
	assert.NotContains(t, s, `"EmbedVideo"`)
	assert.NotContains(t, s, `"EmbedExternal"`)
	assert.NotContains(t, s, `"EmbedRecord"`)
	assert.NotContains(t, s, `"EmbedRecordWithMedia"`)
	assert.NotContains(t, s, `"Unknown"`)

	// Must contain $type discriminators for union types.
	assert.Contains(t, s, `"$type":"app.bsky.embed.recordWithMedia"`)
	assert.Contains(t, s, `"$type":"app.bsky.embed.images"`)

	// Verify core fields via semantic comparison.
	var obj map[string]any
	require.NoError(t, json.Unmarshal(jsonBytes, &obj))
	assert.Equal(t, "app.bsky.feed.post", obj["$type"])
	assert.Equal(t, "Who the hell do you think you are", obj["text"])
	assert.Equal(t, "2023-03-29T20:59:19.417Z", obj["createdAt"])

	// Check embed structure.
	embedObj, ok := obj["embed"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "app.bsky.embed.recordWithMedia", embedObj["$type"])
	mediaObj, ok := embedObj["media"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "app.bsky.embed.images", mediaObj["$type"])
}

// --- Interop: FeedPost with EmbedImages (from indigo inline hex) ---

func TestFeedPostEmbedImages_Interop(t *testing.T) {
	t.Parallel()

	// From indigo's TestPostToJson: Japanese text post with single image.
	hexStr := "a464746578747834e38282e38186e38193e381a3e381a1e3818ce69cace5aeb654776974746572e381a7e38184e38184e381aee381a7e381afefbc9f652474797065726170702e62736b792e666565642e706f737465656d626564a2652474797065756170702e62736b792e656d6265642e696d6167657366696d6167657381a263616c746065696d616765a463726566d82a5825000155122071e37fa09ed1814412a06d4dcd4f9462500b2992c267b9dea11884c52f6bacce6473697a6519ef2e65247479706564626c6f62686d696d65547970656a696d6167652f6a706567696372656174656441747818323032332d30342d30335432323a34363a31392e3438375a"

	data, err := hex.DecodeString(hexStr)
	require.NoError(t, err)

	var fp bsky.FeedPost
	require.NoError(t, fp.UnmarshalCBOR(data))

	assert.Equal(t, "2023-04-03T22:46:19.487Z", fp.CreatedAt)
	assert.Contains(t, fp.Text, "Twitter")

	require.True(t, fp.Embed.HasVal())
	embed := fp.Embed.Val()
	require.True(t, embed.EmbedImages.HasVal())
	imgs := embed.EmbedImages.Val()
	require.Len(t, imgs.Images, 1)
	assert.Equal(t, "image/jpeg", imgs.Images[0].Image.MimeType)

	// CBOR round-trip.
	fp.LexiconTypeID = "app.bsky.feed.post"
	reencoded, err := fp.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, data, reencoded, "CBOR round-trip must be byte-for-byte identical")
}

// --- Interop: FeedPost with richtext link facet ---

func TestFeedPostRichtextLink_Interop(t *testing.T) {
	t.Parallel()

	data := readFixture(t, "post_richtext_link.cbor")
	origCID := cbor.ComputeCID(cbor.CodecDagCBOR, data)

	var fp bsky.FeedPost
	require.NoError(t, fp.UnmarshalCBOR(data))

	assert.Contains(t, fp.Text, "https://atproto.com")
	require.True(t, fp.Embed.HasVal())

	// CBOR round-trip: re-encode and check CID matches.
	fp.LexiconTypeID = "app.bsky.feed.post"
	reencoded, err := fp.MarshalCBOR()
	require.NoError(t, err)
	reproCID := cbor.ComputeCID(cbor.CodecDagCBOR, reencoded)
	assert.Equal(t, origCID.String(), reproCID.String())

	// JSON round-trip: marshal → unmarshal → compare.
	jsonBytes, err := json.Marshal(fp)
	require.NoError(t, err)

	var fp2 bsky.FeedPost
	require.NoError(t, json.Unmarshal(jsonBytes, &fp2))
	fp2.LexiconTypeID = fp.LexiconTypeID

	// Re-marshal both and compare semantically.
	b1, err := json.Marshal(fp)
	require.NoError(t, err)
	b2, err := json.Marshal(fp2)
	require.NoError(t, err)
	jsonSemEqual(t, b1, b2)
}

// --- JSON round-trips for constructed structs ---

func TestFeedPost_JSONRoundTrip_EmbedImages(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		Text:      "check this out",
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Embed: gt.Some(bsky.FeedPost_Embed{
			EmbedImages: gt.SomeRef(bsky.EmbedImages{
				Images: []bsky.EmbedImages_Image{
					{
						Alt: "a cat",
						Image: lextypes.LexBlob{
							Type:     "blob",
							Ref:      lextypes.LexCIDLink{Link: "bafkreieqq463374bbcbeq7gpmet5rvrpeqow6t4rtjzrkhnlumdylagaqa"},
							MimeType: "image/png",
							Size:     12345,
						},
					},
				},
			}),
		}),
	}

	assertJSONRoundTrip(t, &post)
	assertNoGoUnionFieldNames(t, &post)
}

func TestFeedPost_JSONRoundTrip_EmbedExternal(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		Text:      "cool link",
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Embed: gt.Some(bsky.FeedPost_Embed{
			EmbedExternal: gt.SomeRef(bsky.EmbedExternal{
				External: bsky.EmbedExternal_External{
					URI:         "https://example.com",
					Title:       "Example",
					Description: "An example site",
				},
			}),
		}),
	}

	assertJSONRoundTrip(t, &post)
	assertNoGoUnionFieldNames(t, &post)
}

func TestFeedPost_JSONRoundTrip_EmbedRecord(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		Text:      "quoting this",
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Embed: gt.Some(bsky.FeedPost_Embed{
			EmbedRecord: gt.SomeRef(bsky.EmbedRecord{
				Record: comatproto.RepoStrongRef{
					CID: "bafyreiaku7udekkiijxcuue3sn6esz7qijqj637rigz4xqdw57fk5houji",
					URI: "at://did:plc:test/app.bsky.feed.post/abc123",
				},
			}),
		}),
	}

	assertJSONRoundTrip(t, &post)
	assertNoGoUnionFieldNames(t, &post)
}

func TestFeedPost_JSONRoundTrip_WithReply(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		Text:      "replying",
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Reply: gt.Some(bsky.FeedPost_ReplyRef{
			Parent: comatproto.RepoStrongRef{
				CID: "bafyreiparent",
				URI: "at://did:plc:test/app.bsky.feed.post/parent",
			},
			Root: comatproto.RepoStrongRef{
				CID: "bafyreiroot",
				URI: "at://did:plc:test/app.bsky.feed.post/root",
			},
		}),
	}

	assertJSONRoundTrip(t, &post)
}

func TestFeedPost_JSONRoundTrip_WithFacets(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		Text:      "hello @alice.bsky.social check https://example.com",
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Facets: []bsky.RichtextFacet{
			{
				Index: bsky.RichtextFacet_ByteSlice{ByteStart: 6, ByteEnd: 25},
				Features: []bsky.RichtextFacet_Features{
					{RichtextFacet_Mention: gt.SomeRef(bsky.RichtextFacet_Mention{
						DID: "did:plc:alice",
					})},
				},
			},
			{
				Index: bsky.RichtextFacet_ByteSlice{ByteStart: 32, ByteEnd: 51},
				Features: []bsky.RichtextFacet_Features{
					{RichtextFacet_Link: gt.SomeRef(bsky.RichtextFacet_Link{
						URI: "https://example.com",
					})},
				},
			},
		},
	}

	assertJSONRoundTrip(t, &post)
}

func TestFeedPost_JSONRoundTrip_NoEmbed(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		Text:      "just text",
		CreatedAt: "2024-01-01T00:00:00.000Z",
	}

	b, err := json.Marshal(post)
	require.NoError(t, err)

	// embed field should be absent.
	assert.NotContains(t, string(b), `"embed"`)

	var post2 bsky.FeedPost
	require.NoError(t, json.Unmarshal(b, &post2))
	assert.Equal(t, post, post2)
}

// --- CBOR round-trips ---

func TestFeedPost_CBORRoundTrip_EmbedImages(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "check this out",
		CreatedAt:     "2024-01-01T00:00:00.000Z",
		Embed: gt.Some(bsky.FeedPost_Embed{
			EmbedImages: gt.SomeRef(bsky.EmbedImages{
				LexiconTypeID: "app.bsky.embed.images",
				Images: []bsky.EmbedImages_Image{
					{
						Alt: "a cat",
						Image: lextypes.LexBlob{
							Type:     "blob",
							Ref:      lextypes.LexCIDLink{Link: "bafkreieqq463374bbcbeq7gpmet5rvrpeqow6t4rtjzrkhnlumdylagaqa"},
							MimeType: "image/png",
							Size:     12345,
						},
					},
				},
			}),
		}),
	}

	assertCBORRoundTrip(t, &post)
	assertCBORDeterministic(t, &post)
}

func TestFeedPost_CBORRoundTrip_EmbedRecord(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "quoting",
		CreatedAt:     "2024-01-01T00:00:00.000Z",
		Embed: gt.Some(bsky.FeedPost_Embed{
			EmbedRecord: gt.SomeRef(bsky.EmbedRecord{
				LexiconTypeID: "app.bsky.embed.record",
				Record: comatproto.RepoStrongRef{
					CID: "bafyreiaku7udekkiijxcuue3sn6esz7qijqj637rigz4xqdw57fk5houji",
					URI: "at://did:plc:test/app.bsky.feed.post/abc123",
				},
			}),
		}),
	}

	assertCBORRoundTrip(t, &post)
	assertCBORDeterministic(t, &post)
}

func TestFeedPost_CBORRoundTrip_WithReply(t *testing.T) {
	t.Parallel()

	post := bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "reply",
		CreatedAt:     "2024-01-01T00:00:00.000Z",
		Reply: gt.Some(bsky.FeedPost_ReplyRef{
			Parent: comatproto.RepoStrongRef{CID: "bafyreiparent", URI: "at://did:plc:test/app.bsky.feed.post/parent"},
			Root:   comatproto.RepoStrongRef{CID: "bafyreiroot", URI: "at://did:plc:test/app.bsky.feed.post/root"},
		}),
	}

	assertCBORRoundTrip(t, &post)
	assertCBORDeterministic(t, &post)
}

// --- Union JSON correctness ---

func TestUnionJSON_NoGoFieldNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		embed bsky.FeedPost_Embed
		want  string // expected $type value
	}{
		{
			name: "images",
			embed: bsky.FeedPost_Embed{
				EmbedImages: gt.SomeRef(bsky.EmbedImages{
					Images: []bsky.EmbedImages_Image{{Alt: "x", Image: lextypes.LexBlob{Type: "blob", Ref: lextypes.LexCIDLink{Link: "baftest"}, MimeType: "image/png", Size: 1}}},
				}),
			},
			want: "app.bsky.embed.images",
		},
		{
			name: "external",
			embed: bsky.FeedPost_Embed{
				EmbedExternal: gt.SomeRef(bsky.EmbedExternal{
					External: bsky.EmbedExternal_External{URI: "https://example.com", Title: "t", Description: "d"},
				}),
			},
			want: "app.bsky.embed.external",
		},
		{
			name: "record",
			embed: bsky.FeedPost_Embed{
				EmbedRecord: gt.SomeRef(bsky.EmbedRecord{
					Record: comatproto.RepoStrongRef{CID: "baftest", URI: "at://did:plc:test/app.bsky.feed.post/x"},
				}),
			},
			want: "app.bsky.embed.record",
		},
	}

	goFieldNames := []string{"EmbedImages", "EmbedVideo", "EmbedExternal", "EmbedRecord", "EmbedRecordWithMedia", "Unknown"}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tc.embed)
			require.NoError(t, err)
			s := string(b)

			for _, name := range goFieldNames {
				assert.NotContains(t, s, `"`+name+`"`, "Go field name %q should not appear in JSON", name)
			}
			assert.Contains(t, s, `"$type":"`+tc.want+`"`)
		})
	}
}

func TestUnionJSON_UnmarshalDispatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		json    string
		checker func(t *testing.T, u bsky.FeedPost_Embed)
	}{
		{
			name: "images",
			json: `{"$type":"app.bsky.embed.images","images":[{"alt":"","image":{"$type":"blob","ref":{"$link":"baftest"},"mimeType":"image/png","size":1}}]}`,
			checker: func(t *testing.T, u bsky.FeedPost_Embed) {
				assert.True(t, u.EmbedImages.HasVal())
				assert.False(t, u.EmbedExternal.HasVal())
				assert.False(t, u.EmbedRecord.HasVal())
			},
		},
		{
			name: "external",
			json: `{"$type":"app.bsky.embed.external","external":{"uri":"https://example.com","title":"t","description":"d"}}`,
			checker: func(t *testing.T, u bsky.FeedPost_Embed) {
				assert.True(t, u.EmbedExternal.HasVal())
				assert.False(t, u.EmbedImages.HasVal())
			},
		},
		{
			name: "record",
			json: `{"$type":"app.bsky.embed.record","record":{"cid":"baftest","uri":"at://did:plc:test/app.bsky.feed.post/x"}}`,
			checker: func(t *testing.T, u bsky.FeedPost_Embed) {
				assert.True(t, u.EmbedRecord.HasVal())
				assert.False(t, u.EmbedImages.HasVal())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var u bsky.FeedPost_Embed
			require.NoError(t, json.Unmarshal([]byte(tc.json), &u))
			tc.checker(t, u)
		})
	}
}

func TestUnionJSON_UnknownVariant(t *testing.T) {
	t.Parallel()

	raw := `{"$type":"com.example.unknown","foo":"bar"}`
	var u bsky.FeedPost_Embed
	require.NoError(t, json.Unmarshal([]byte(raw), &u))

	assert.True(t, u.Unknown.HasVal())
	assert.False(t, u.EmbedImages.HasVal())
	assert.Equal(t, "com.example.unknown", u.Unknown.Val().Type)

	// Round-trip: unknown variant should preserve raw JSON.
	b, err := json.Marshal(u)
	require.NoError(t, err)
	jsonSemEqual(t, []byte(raw), b)
}

// --- Helpers ---

func assertJSONRoundTrip(t *testing.T, post *bsky.FeedPost) {
	t.Helper()

	b, err := json.Marshal(post)
	require.NoError(t, err)

	var post2 bsky.FeedPost
	require.NoError(t, json.Unmarshal(b, &post2))

	// Re-marshal to compare semantically (handles omitempty/omitzero differences).
	b2, err := json.Marshal(post2)
	require.NoError(t, err)
	jsonSemEqual(t, b, b2)
}

func assertNoGoUnionFieldNames(t *testing.T, post *bsky.FeedPost) {
	t.Helper()

	b, err := json.Marshal(post)
	require.NoError(t, err)
	s := string(b)

	for _, name := range []string{"EmbedImages", "EmbedVideo", "EmbedExternal", "EmbedRecord", "EmbedRecordWithMedia", "Unknown"} {
		if strings.Contains(s, `"`+name+`"`) {
			t.Errorf("Go field name %q found in JSON output: %s", name, s)
		}
	}
}

func assertCBORRoundTrip(t *testing.T, post *bsky.FeedPost) {
	t.Helper()

	b, err := post.MarshalCBOR()
	require.NoError(t, err)

	var post2 bsky.FeedPost
	require.NoError(t, post2.UnmarshalCBOR(b))

	// Re-encode and compare bytes.
	post2.LexiconTypeID = post.LexiconTypeID
	b2, err := post2.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, b, b2, "CBOR round-trip must produce identical bytes")
}

func assertCBORDeterministic(t *testing.T, post *bsky.FeedPost) {
	t.Helper()

	b1, err := post.MarshalCBOR()
	require.NoError(t, err)
	b2, err := post.MarshalCBOR()
	require.NoError(t, err)
	assert.Equal(t, b1, b2, "CBOR encoding must be deterministic")
}

// --- Benchmarks ---

func benchPost() bsky.FeedPost {
	return bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "check this out",
		CreatedAt:     "2024-01-01T00:00:00.000Z",
		Langs:         []string{"en"},
		Embed: gt.Some(bsky.FeedPost_Embed{
			EmbedImages: gt.SomeRef(bsky.EmbedImages{
				Images: []bsky.EmbedImages_Image{
					{
						Alt: "a cat",
						Image: lextypes.LexBlob{
							Type:     "blob",
							Ref:      lextypes.LexCIDLink{Link: "bafkreieqq463374bbcbeq7gpmet5rvrpeqow6t4rtjzrkhnlumdylagaqa"},
							MimeType: "image/png",
							Size:     12345,
						},
					},
				},
			}),
		}),
		Reply: gt.Some(bsky.FeedPost_ReplyRef{
			Parent: comatproto.RepoStrongRef{
				CID: "bafyreiaku7udekkiijxcuue3sn6esz7qijqj637rigz4xqdw57fk5houji",
				URI: "at://did:plc:test/app.bsky.feed.post/parent",
			},
			Root: comatproto.RepoStrongRef{
				CID: "bafyreiaku7udekkiijxcuue3sn6esz7qijqj637rigz4xqdw57fk5houji",
				URI: "at://did:plc:test/app.bsky.feed.post/root",
			},
		}),
	}
}

func BenchmarkFeedPost_MarshalCBOR(b *testing.B) {
	post := benchPost()
	for b.Loop() {
		_, _ = post.MarshalCBOR()
	}
}

func BenchmarkFeedPost_UnmarshalCBOR(b *testing.B) {
	post := benchPost()
	data, err := post.MarshalCBOR()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		var p bsky.FeedPost
		_ = p.UnmarshalCBOR(data)
	}
}

func BenchmarkFeedPost_MarshalJSON(b *testing.B) {
	post := benchPost()
	for b.Loop() {
		_, _ = json.Marshal(&post)
	}
}

func BenchmarkFeedPost_UnmarshalJSON(b *testing.B) {
	post := benchPost()
	data, err := json.Marshal(&post)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		var p bsky.FeedPost
		_ = json.Unmarshal(data, &p)
	}
}

func BenchmarkFeedPost_CBORRoundTrip(b *testing.B) {
	post := benchPost()
	for b.Loop() {
		data, _ := post.MarshalCBOR()
		var p bsky.FeedPost
		_ = p.UnmarshalCBOR(data)
	}
}

func BenchmarkFeedPost_JSONRoundTrip(b *testing.B) {
	post := benchPost()
	for b.Loop() {
		data, _ := json.Marshal(&post)
		var p bsky.FeedPost
		_ = json.Unmarshal(data, &p)
	}
}

func BenchmarkFeedPost_AppendJSON(b *testing.B) {
	post := benchPost()
	buf := make([]byte, 0, 1024)
	for b.Loop() {
		buf, _ = post.AppendJSON(buf[:0])
	}
}

func BenchmarkFeedPost_UnmarshalJSONDirect(b *testing.B) {
	post := benchPost()
	data, _ := post.MarshalJSON()
	b.ResetTimer()
	for b.Loop() {
		var p bsky.FeedPost
		_ = p.UnmarshalJSON(data)
	}
}
