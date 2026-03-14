package lexgen

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos/lexicon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerate_RecordCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "record", Key: "tid",
				Record: &lexicon.Object{
					Type: "object", Required: []string{"text", "createdAt"},
					Properties: map[string]*lexicon.Field{
						"text":      {Type: "string"},
						"createdAt": {Type: "string", Format: "datetime"},
						"langs":     {Type: "array", Items: &lexicon.Field{Type: "string"}},
					},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])

	// Has precomputed CBOR key tokens.
	assert.Contains(t, code, `cborKey_FeedPost_text`)
	assert.Contains(t, code, `cborKey_FeedPost_createdAt`)
	assert.Contains(t, code, `cbor.AppendTextKey(nil,`)

	// Has MarshalCBOR and UnmarshalCBOR.
	assert.Contains(t, code, "func (s *FeedPost) MarshalCBOR() ([]byte, error)")
	assert.Contains(t, code, "func (s *FeedPost) UnmarshalCBOR(data []byte) error")

	// Marshal uses AppendMapHeader and AppendText.
	assert.Contains(t, code, "cbor.AppendMapHeader(buf,")
	assert.Contains(t, code, "cbor.AppendText(buf,")

	// Unmarshal reads map header and switches on keys.
	assert.Contains(t, code, "cbor.ReadMapHeader(data, pos)")
	assert.Contains(t, code, `case "text":`)
	assert.Contains(t, code, `case "createdAt":`)

	// Skips unknown fields.
	assert.Contains(t, code, "cbor.SkipValue(data, pos)")
}

func TestGenerate_ObjectCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.defs",
		Defs: map[string]*lexicon.Def{
			"postView": {
				Type: "object", Required: []string{"uri"},
				Properties: map[string]*lexicon.Field{
					"uri":   {Type: "string"},
					"count": {Type: "integer"},
				},
			},
		},
	})

	code := string(files["api/bsky/feeddefs.go"])
	assert.Contains(t, code, "func (s *FeedDefs_PostView) MarshalCBOR()")
	assert.Contains(t, code, "func (s *FeedDefs_PostView) UnmarshalCBOR(data []byte) error")
}

func TestGenerate_UnionCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"embed"},
				Properties: map[string]*lexicon.Field{
					"embed": {Type: "union", Refs: []string{"#typeA", "#typeB"}, Closed: true},
				},
			},
			"typeA": {Type: "object", Properties: map[string]*lexicon.Field{"a": {Type: "string"}}},
			"typeB": {Type: "object", Properties: map[string]*lexicon.Field{"b": {Type: "string"}}},
		},
	})

	code := string(files["api/bsky/feedpost.go"])

	// Union CBOR marshal/unmarshal.
	assert.Contains(t, code, "func (u FeedPost_Embed) MarshalCBOR() ([]byte, error)")
	assert.Contains(t, code, "func (u *FeedPost_Embed) UnmarshalCBOR(data []byte) error")
	assert.Contains(t, code, "cbor.PeekTypeAt(data, pos)")
	assert.Contains(t, code, `case "app.bsky.feed.post#typeA":`)
}

func TestGenerate_SharedTypesCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "record", Key: "tid",
				Record: &lexicon.Object{
					Type: "object", Required: []string{"text"},
					Properties: map[string]*lexicon.Field{
						"text": {Type: "string"},
					},
				},
			},
		},
	})

	typesCode := string(files["api/lextypes/types.go"])
	assert.Contains(t, typesCode, "func (b *LexBlob) MarshalCBOR()")
	assert.Contains(t, typesCode, "func (b *LexBlob) UnmarshalCBOR(data []byte) error")
	assert.Contains(t, typesCode, "func (c *LexCIDLink) MarshalCBOR()")
	assert.Contains(t, typesCode, "func (c *LexCIDLink) UnmarshalCBOR(data []byte) error")
}

func TestGenerate_CIDLinkFieldCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"ref"},
				Properties: map[string]*lexicon.Field{
					"ref": {Type: "cid-link"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "func (s *FeedPost) MarshalCBOR()")
}

func TestGenerate_NestedObjectCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"inner"},
				Properties: map[string]*lexicon.Field{
					"inner": {
						Type:     "object",
						Required: []string{"name"},
						Properties: map[string]*lexicon.Field{
							"name": {Type: "string"},
						},
					},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	// Nested object gets its own CBOR methods.
	assert.Contains(t, code, "func (s *FeedPost_Inner) MarshalCBOR()")
	assert.Contains(t, code, "func (s *FeedPost_Inner) UnmarshalCBOR(data []byte) error")
}

func TestGenerate_NullableRequiredCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"val"}, Nullable: []string{"val"},
				Properties: map[string]*lexicon.Field{
					"val": {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	// Nullable required fields should encode null when absent.
	assert.Contains(t, code, "cbor.AppendNull(buf)")
	assert.Contains(t, code, "!s.Val.HasVal()")
}

func TestGenerate_CrossPackageRefCBOR(t *testing.T) {
	t.Parallel()
	files := genMulti(t,
		&lexicon.Schema{
			Lexicon: 1, ID: "com.atproto.repo.strongRef",
			Defs: map[string]*lexicon.Def{
				"main": {
					Type: "object", Required: []string{"uri", "cid"},
					Properties: map[string]*lexicon.Field{
						"uri": {Type: "string"},
						"cid": {Type: "string"},
					},
				},
			},
		},
		&lexicon.Schema{
			Lexicon: 1, ID: "app.bsky.feed.post",
			Defs: map[string]*lexicon.Def{
				"main": {
					Type: "record", Key: "tid",
					Record: &lexicon.Object{
						Type: "object", Required: []string{"text"},
						Properties: map[string]*lexicon.Field{
							"text":  {Type: "string"},
							"reply": {Type: "ref", Ref: "com.atproto.repo.strongRef"},
						},
					},
				},
			},
		},
	)

	code := string(files["api/bsky/feedpost.go"])
	// Cross-package ref field should have CBOR decode with UnmarshalCBOR.
	assert.Contains(t, code, "MarshalCBOR")
	assert.Contains(t, code, "UnmarshalCBOR")

	// The strongRef type itself should have CBOR methods.
	refCode := string(files["api/comatproto/repostrongref.go"])
	assert.Contains(t, refCode, "func (s *RepoStrongRef) MarshalCBOR()")
}

func TestGenerate_OptionalIntegerCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.defs",
		Defs: map[string]*lexicon.Def{
			"postView": {
				Type: "object", Required: []string{},
				Properties: map[string]*lexicon.Field{
					"count":  {Type: "integer"},
					"active": {Type: "boolean"},
				},
			},
		},
	})

	code := string(files["api/bsky/feeddefs.go"])
	// Optional integer should use gt.Some in decode.
	assert.Contains(t, code, "gt.Some(v)")
	assert.Contains(t, code, "cbor.ReadInt(data, pos)")
	assert.Contains(t, code, "cbor.ReadBool(data, pos)")
}

func TestGenerate_ArrayOfRefsCBOR(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"items"},
				Properties: map[string]*lexicon.Field{
					"items": {Type: "array", Items: &lexicon.Field{Type: "ref", Ref: "#item"}},
				},
			},
			"item": {Type: "object", Properties: map[string]*lexicon.Field{
				"name": {Type: "string"},
			}},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	// Array of refs should use ReadArrayHeader and UnmarshalCBORAt per element.
	assert.Contains(t, code, "cbor.ReadArrayHeader(data, pos)")
	assert.Contains(t, code, "UnmarshalCBORAt(data, pos)")
}

func TestGenerate_AllVendoredLexiconsCBOR(t *testing.T) {
	t.Parallel()
	files, err := generateAllVendored()
	require.NoError(t, err)

	// Spot-check that at least some record types got CBOR methods.
	var hasCBOR int
	for _, data := range files {
		code := string(data)
		if containsAll(code, "MarshalCBOR", "UnmarshalCBOR") {
			hasCBOR++
		}
	}
	// We expect CBOR in many files: all records, objects, unions.
	assert.Greater(t, hasCBOR, 10, "expected CBOR methods in many generated files")
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
