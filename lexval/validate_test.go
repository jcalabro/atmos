package lexval

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/lexicon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lexiconsDir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "..", "lexicons")
}

func loadCatalog(t *testing.T) *lexicon.Catalog {
	t.Helper()
	schemas, err := lexicon.ParseDir(lexiconsDir())
	require.NoError(t, err)
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.AddAll(schemas))
	return cat
}

// --- String validation ---

func TestValidate_String_Format_DID(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "did"}
	assert.NoError(t, ValidateValue(nil, "", f, "did:plc:abcde1234"))
	assert.Error(t, ValidateValue(nil, "", f, "not-a-did"))
}

func TestValidate_String_Format_Handle(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "handle"}
	assert.NoError(t, ValidateValue(nil, "", f, "alice.bsky.social"))
	assert.Error(t, ValidateValue(nil, "", f, "not a handle"))
}

func TestValidate_String_Format_ATURI(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "at-uri"}
	assert.NoError(t, ValidateValue(nil, "", f, "at://did:plc:abc123/app.bsky.feed.post/tid"))
	assert.Error(t, ValidateValue(nil, "", f, "http://example.com"))
}

func TestValidate_String_Format_Datetime(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "datetime"}
	assert.NoError(t, ValidateValue(nil, "", f, "2023-01-01T00:00:00Z"))
	assert.Error(t, ValidateValue(nil, "", f, "not-a-datetime"))
}

func TestValidate_String_Format_NSID(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "nsid"}
	assert.NoError(t, ValidateValue(nil, "", f, "app.bsky.feed.post"))
	assert.Error(t, ValidateValue(nil, "", f, "not-an-nsid"))
}

func TestValidate_String_Format_TID(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "tid"}
	assert.NoError(t, ValidateValue(nil, "", f, "2222222222222"))
	assert.Error(t, ValidateValue(nil, "", f, "short"))
}

func TestValidate_String_Format_RecordKey(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "record-key"}
	assert.NoError(t, ValidateValue(nil, "", f, "self"))
	assert.Error(t, ValidateValue(nil, "", f, "."))
}

func TestValidate_String_Format_URI(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "uri"}
	assert.NoError(t, ValidateValue(nil, "", f, "https://example.com"))
	assert.Error(t, ValidateValue(nil, "", f, ""))
}

func TestValidate_String_Format_Language(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "language"}
	assert.NoError(t, ValidateValue(nil, "", f, "en"))
	assert.Error(t, ValidateValue(nil, "", f, ""))
}

func TestValidate_String_Format_CID(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "cid"}
	// Use a valid base32lower CIDv1.
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	assert.NoError(t, ValidateValue(nil, "", f, cid.String()))
	assert.Error(t, ValidateValue(nil, "", f, "not-a-cid"))
}

func TestValidate_String_Format_AtIdentifier(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Format: "at-identifier"}
	assert.NoError(t, ValidateValue(nil, "", f, "did:plc:abcde1234"))
	assert.NoError(t, ValidateValue(nil, "", f, "alice.bsky.social"))
	assert.Error(t, ValidateValue(nil, "", f, ""))
}

func TestValidate_String_MaxLength(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", MaxLength: 10}
	assert.NoError(t, ValidateValue(nil, "", f, "short"))
	assert.Error(t, ValidateValue(nil, "", f, "this is way too long"))
}

func TestValidate_String_MinLength(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", MinLength: 5}
	assert.NoError(t, ValidateValue(nil, "", f, "hello"))
	assert.Error(t, ValidateValue(nil, "", f, "hi"))
}

func TestValidate_String_MaxGraphemes(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", MaxGraphemes: 1}
	// ZWJ family emoji = 1 grapheme but 7 runes.
	assert.NoError(t, ValidateValue(nil, "", f, "👨‍👩‍👧‍👦"))
	assert.Error(t, ValidateValue(nil, "", f, "ab"))
}

func TestValidate_String_MinGraphemes(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", MinGraphemes: 2}
	assert.NoError(t, ValidateValue(nil, "", f, "ab"))
	assert.Error(t, ValidateValue(nil, "", f, "a"))
}

func TestValidate_String_Enum(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Enum: []string{"a", "b", "c"}}
	assert.NoError(t, ValidateValue(nil, "", f, "a"))
	assert.Error(t, ValidateValue(nil, "", f, "d"))
}

func TestValidate_String_Const(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string", Const: "exact"}
	assert.NoError(t, ValidateValue(nil, "", f, "exact"))
	assert.Error(t, ValidateValue(nil, "", f, "other"))
}

func TestValidate_String_WrongType(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string"}
	assert.Error(t, ValidateValue(nil, "", f, 42))
}

// --- Integer validation ---

func TestValidate_Integer_Bounds(t *testing.T) {
	t.Parallel()
	min := int64(0)
	max := int64(100)
	f := &lexicon.Field{Type: "integer", Minimum: &min, Maximum: &max}
	assert.NoError(t, ValidateValue(nil, "", f, int64(50)))
	assert.Error(t, ValidateValue(nil, "", f, int64(-1)))
	assert.Error(t, ValidateValue(nil, "", f, int64(101)))
}

func TestValidate_Integer_Const(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "integer", Const: float64(42)}
	assert.NoError(t, ValidateValue(nil, "", f, int64(42)))
	assert.Error(t, ValidateValue(nil, "", f, int64(99)))
}

func TestValidate_Integer_Float64(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "integer"}
	assert.NoError(t, ValidateValue(nil, "", f, float64(5.0)))
	assert.Error(t, ValidateValue(nil, "", f, float64(5.5)))
}

func TestValidate_Integer_WrongType(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "integer"}
	assert.Error(t, ValidateValue(nil, "", f, "not an int"))
}

// --- Boolean/Bytes/CIDLink ---

func TestValidate_Boolean(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "boolean"}
	assert.NoError(t, ValidateValue(nil, "", f, true))
	assert.Error(t, ValidateValue(nil, "", f, "true"))
}

func TestValidate_Bytes_Length(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "bytes", MaxLength: 4, MinLength: 2}
	assert.NoError(t, ValidateValue(nil, "", f, []byte{1, 2, 3}))
	assert.Error(t, ValidateValue(nil, "", f, []byte{1}))
	assert.Error(t, ValidateValue(nil, "", f, []byte{1, 2, 3, 4, 5}))
}

func TestValidate_CIDLink(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "cid-link"}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	assert.NoError(t, ValidateValue(nil, "", f, cid))
	// JSON form with valid CID.
	assert.NoError(t, ValidateValue(nil, "", f, map[string]any{"$link": cid.String()}))
	// JSON form with invalid CID.
	assert.Error(t, ValidateValue(nil, "", f, map[string]any{"$link": "not-a-cid"}))
	// Missing $link.
	assert.Error(t, ValidateValue(nil, "", f, map[string]any{}))
	// Wrong type.
	assert.Error(t, ValidateValue(nil, "", f, 42))
}

func TestValidate_Bytes_WrongType(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "bytes"}
	assert.Error(t, ValidateValue(nil, "", f, "not bytes"))
}

// --- Blob ---

func TestValidate_Blob_Valid(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "blob", Accept: []string{"image/png", "image/jpeg"}, MaxSize: 1000000}
	blob := map[string]any{
		"$type":    "blob",
		"ref":      map[string]any{"$link": "bafyreib"},
		"mimeType": "image/png",
		"size":     int64(1234),
	}
	assert.NoError(t, ValidateValue(nil, "", f, blob))
}

func TestValidate_Blob_MissingFields(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "blob"}
	assert.Error(t, ValidateValue(nil, "", f, map[string]any{}))
}

func TestValidate_Blob_MaxSize(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "blob", MaxSize: 100}
	blob := map[string]any{
		"$type":    "blob",
		"ref":      map[string]any{"$link": "bafyreib"},
		"mimeType": "image/png",
		"size":     int64(200),
	}
	assert.Error(t, ValidateValue(nil, "", f, blob))
}

func TestValidate_Blob_AcceptMIME(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "blob", Accept: []string{"image/*"}}
	makeBlob := func(mime string) map[string]any {
		return map[string]any{
			"$type":    "blob",
			"ref":      map[string]any{"$link": "bafyreib"},
			"mimeType": mime,
			"size":     int64(100),
		}
	}
	assert.NoError(t, ValidateValue(nil, "", f, makeBlob("image/png")))
	assert.Error(t, ValidateValue(nil, "", f, makeBlob("video/mp4")))
}

// --- Array ---

func TestValidate_Array_Length(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "array", MaxLength: 3, MinLength: 1}
	assert.NoError(t, ValidateValue(nil, "", f, []any{"a", "b"}))
	assert.Error(t, ValidateValue(nil, "", f, []any{}))
	assert.Error(t, ValidateValue(nil, "", f, []any{"a", "b", "c", "d"}))
}

func TestValidate_Array_ItemValidation(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "array", Items: &lexicon.Field{Type: "string"}}
	assert.NoError(t, ValidateValue(nil, "", f, []any{"a", "b"}))
	assert.Error(t, ValidateValue(nil, "", f, []any{"a", 42}))
}

// --- Object ---

func TestValidate_Object_Required(t *testing.T) {
	t.Parallel()
	obj := &lexicon.Object{
		Properties: map[string]*lexicon.Field{
			"name": {Type: "string"},
		},
		Required: []string{"name"},
	}
	assert.NoError(t, ValidateObject(nil, "", obj, map[string]any{"name": "Alice"}))
	assert.Error(t, ValidateObject(nil, "", obj, map[string]any{}))
}

func TestValidate_Object_Nullable(t *testing.T) {
	t.Parallel()
	obj := &lexicon.Object{
		Properties: map[string]*lexicon.Field{
			"name": {Type: "string"},
		},
		Required: []string{"name"},
		Nullable: []string{"name"},
	}
	// nil is valid when nullable + required: required means key must exist, nullable means value can be nil.
	assert.NoError(t, ValidateObject(nil, "", obj, map[string]any{"name": nil}))
}

func TestValidate_Object_UnknownFields(t *testing.T) {
	t.Parallel()
	obj := &lexicon.Object{
		Properties: map[string]*lexicon.Field{
			"name": {Type: "string"},
		},
	}
	assert.NoError(t, ValidateObject(nil, "", obj, map[string]any{"name": "Alice", "extra": 42}))
}

func TestValidate_Object_NestedPath(t *testing.T) {
	t.Parallel()
	obj := &lexicon.Object{
		Properties: map[string]*lexicon.Field{
			"embed": {
				Type: "object",
				Properties: map[string]*lexicon.Field{
					"images": {
						Type: "array",
						Items: &lexicon.Field{
							Type: "object",
							Properties: map[string]*lexicon.Field{
								"alt": {Type: "string"},
							},
							Required: []string{"alt"},
						},
					},
				},
			},
		},
	}
	data := map[string]any{
		"embed": map[string]any{
			"images": []any{
				map[string]any{}, // missing required "alt"
			},
		},
	}
	err := ValidateObject(nil, "", obj, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "object.embed.images[0].alt")
}

// --- Ref ---

func TestValidate_Ref_Local(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"reply": {Type: "ref", Ref: "#replyRef"},
				},
			}},
			"replyRef": {Type: "object", Properties: map[string]*lexicon.Field{
				"root": {Type: "string"},
			}, Required: []string{"root"}},
		},
	}))

	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{
		"reply": map[string]any{"root": "hello"},
	}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{
		"reply": map[string]any{},
	}))
}

func TestValidate_Ref_CrossSchema(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.atproto.repo.strongRef",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "object", Properties: map[string]*lexicon.Field{
				"uri": {Type: "string"},
				"cid": {Type: "string"},
			}, Required: []string{"uri", "cid"}},
		},
	}))
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"ref": {Type: "ref", Ref: "com.atproto.repo.strongRef"},
				},
			}},
		},
	}))

	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{
		"ref": map[string]any{"uri": "at://x", "cid": "baf"},
	}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{
		"ref": map[string]any{"uri": "at://x"},
	}))
}

func TestValidate_Ref_Unresolved(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"ref": {Type: "ref", Ref: "com.nonexistent.schema"},
				},
			}},
		},
	}))
	err := ValidateRecord(cat, "com.example.test", map[string]any{
		"ref": map[string]any{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestValidate_Ref_StringDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "ref", Ref: "#myStr"},
				},
			}},
			"myStr": {Type: "string", MaxLength: 5},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": "hi"}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": "toolong"}))
}

func TestValidate_Ref_ObjectDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"obj": {Type: "ref", Ref: "#myObj"},
				},
			}},
			"myObj": {Type: "object", Properties: map[string]*lexicon.Field{
				"x": {Type: "integer"},
			}, Required: []string{"x"}},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"obj": map[string]any{"x": int64(1)}}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"obj": map[string]any{}}))
}

// --- Union ---

func TestValidate_Union_Closed_Valid(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.typeA",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "object", Properties: map[string]*lexicon.Field{
				"a": {Type: "string"},
			}},
		},
	}))
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "union", Refs: []string{"com.example.typeA"}, Closed: true},
				},
			}},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{
		"val": map[string]any{"$type": "com.example.typeA", "a": "hi"},
	}))
}

func TestValidate_Union_Closed_Unknown(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "union", Refs: []string{}, Closed: true},
				},
			}},
		},
	}))
	err := ValidateRecord(cat, "com.example.test", map[string]any{
		"val": map[string]any{"$type": "com.unknown.type"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in closed union")
}

func TestValidate_Union_Open_Unknown(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "union", Refs: []string{}, Closed: false},
				},
			}},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{
		"val": map[string]any{"$type": "com.unknown.type"},
	}))
}

func TestValidate_Union_MissingType(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "union", Refs: []string{}}
	err := ValidateValue(nil, "", f, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing $type")
}

func TestValidate_Union_InnerValidation(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.typeA",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "object", Properties: map[string]*lexicon.Field{
				"name": {Type: "string"},
			}, Required: []string{"name"}},
		},
	}))
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "union", Refs: []string{"com.example.typeA"}, Closed: true},
				},
			}},
		},
	}))
	// Missing required field "name" in the matched variant.
	err := ValidateRecord(cat, "com.example.test", map[string]any{
		"val": map[string]any{"$type": "com.example.typeA"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name")
}

// --- Integration Tests with Real Schemas ---

func TestValidate_RealSchema_PostValid(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)
	data := map[string]any{
		"text":      "Hello, world!",
		"createdAt": "2023-01-01T00:00:00Z",
	}
	assert.NoError(t, ValidateRecord(cat, "app.bsky.feed.post", data))
}

func TestValidate_RealSchema_PostTextTooLong(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)
	// 301 graphemes exceeds maxGraphemes 300.
	text := strings.Repeat("a", 301)
	data := map[string]any{
		"text":      text,
		"createdAt": "2023-01-01T00:00:00Z",
	}
	err := ValidateRecord(cat, "app.bsky.feed.post", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxGraphemes")
}

func TestValidate_RealSchema_PostMissingCreatedAt(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)
	data := map[string]any{
		"text": "Hello",
	}
	err := ValidateRecord(cat, "app.bsky.feed.post", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "createdAt")
}

func TestValidate_RealSchema_PostInvalidEmbed(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)
	// The embed union in app.bsky.feed.post is open, so unknown $type is accepted.
	// Instead test that a non-map value for embed fails.
	data := map[string]any{
		"text":      "Hello",
		"createdAt": "2023-01-01T00:00:00Z",
		"embed":     "not a map",
	}
	err := ValidateRecord(cat, "app.bsky.feed.post", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected union object")
}

func TestValidate_RealSchema_ProfileValid(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)
	data := map[string]any{
		"displayName": "Alice",
		"description": "Hello!",
	}
	assert.NoError(t, ValidateRecord(cat, "app.bsky.actor.profile", data))
}

func TestValidate_RealSchema_StrongRef(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)

	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	data := map[string]any{
		"text":      "Hello",
		"createdAt": "2023-01-01T00:00:00Z",
		"reply": map[string]any{
			"root": map[string]any{
				"uri": "at://did:plc:abc123/app.bsky.feed.post/tid123",
				"cid": cid.String(),
			},
			"parent": map[string]any{
				"uri": "at://did:plc:abc123/app.bsky.feed.post/tid123",
				"cid": cid.String(),
			},
		},
	}
	assert.NoError(t, ValidateRecord(cat, "app.bsky.feed.post", data))
}

// --- Null/nil handling ---

func TestValidate_NullRequired(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string"}
	err := ValidateValue(nil, "", f, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

// --- $type enforcement ---

func TestValidate_Record_TypeMismatch(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{},
			}},
		},
	}))
	// Wrong $type.
	err := ValidateRecord(cat, "com.example.test", map[string]any{"$type": "com.other.thing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$type")
	// Missing $type is fine (not required by validator, just checked if present).
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{}))
}

// --- Blob with native CID ref ---

func TestValidate_Blob_NativeCIDRef(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "blob"}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	blob := map[string]any{
		"$type":    "blob",
		"ref":      cid,
		"mimeType": "image/png",
		"size":     int64(1234),
	}
	assert.NoError(t, ValidateValue(nil, "", f, blob))
}

// --- Blob MIME wildcard ---

func TestValidate_Blob_AcceptWildcard(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "blob", Accept: []string{"*/*"}}
	blob := map[string]any{
		"$type":    "blob",
		"ref":      map[string]any{"$link": cbor.ComputeCID(cbor.CodecDagCBOR, []byte("x")).String()},
		"mimeType": "application/octet-stream",
		"size":     int64(1),
	}
	assert.NoError(t, ValidateValue(nil, "", f, blob))
}

// --- Ref to boolean/bytes/cid-link def ---

func TestValidate_Ref_BooleanDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "ref", Ref: "#myBool"},
				},
			}},
			"myBool": {Type: "boolean"},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": true}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": "true"}))
}

func TestValidate_Ref_BytesDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "ref", Ref: "#myBytes"},
				},
			}},
			"myBytes": {Type: "bytes", MaxLength: 4},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": []byte{1, 2}}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": []byte{1, 2, 3, 4, 5}}))
}

func TestValidate_Ref_CIDLinkDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "ref", Ref: "#myCid"},
				},
			}},
			"myCid": {Type: "cid-link"},
		},
	}))
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": cid}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": 42}))
}

func TestValidate_Ref_TokenDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "ref", Ref: "#myToken"},
				},
			}},
			"myToken": {Type: "token"},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": "some-token"}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": 42}))
}

func TestValidate_Ref_ArrayDef(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(&lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.test",
		Defs: map[string]*lexicon.Def{
			"main": {Type: "record", Record: &lexicon.Object{
				Properties: map[string]*lexicon.Field{
					"val": {Type: "ref", Ref: "#myArr"},
				},
			}},
			"myArr": {Type: "array", Items: &lexicon.Field{Type: "string"}, MaxLength: 2},
		},
	}))
	assert.NoError(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": []any{"a"}}))
	assert.Error(t, ValidateRecord(cat, "com.example.test", map[string]any{"val": []any{"a", "b", "c"}}))
}

// --- multiError ---

func TestValidate_MultiError_Format(t *testing.T) {
	t.Parallel()
	errs := multiError{
		{Path: "record.text", Message: "too long"},
		{Path: "record.createdAt", Message: "required"},
	}
	s := errs.Error()
	assert.Contains(t, s, "record.text: too long")
	assert.Contains(t, s, "record.createdAt: required")
	assert.Contains(t, s, "\n")
}

// --- Path ---

func TestValidate_Path_Nil(t *testing.T) {
	t.Parallel()
	var p *path
	assert.Equal(t, "", p.String())
}

// --- Unknown schema ---

func TestValidate_UnknownSchema(t *testing.T) {
	t.Parallel()
	cat := lexicon.NewCatalog()
	err := ValidateRecord(cat, "com.nonexistent.schema", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown schema")
}

// --- Integer const with int type ---

func TestValidate_Integer_Const_IntType(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "integer", Const: int(42)}
	assert.NoError(t, ValidateValue(nil, "", f, int64(42)))
	assert.Error(t, ValidateValue(nil, "", f, int64(99)))
}

// --- errors.As support ---

func TestValidate_ErrorsAs_SingleError(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "string"}
	err := ValidateValue(nil, "", f, 42)
	require.Error(t, err)
	ve, ok := errors.AsType[*ValidationError](err)
	assert.True(t, ok)
	assert.Equal(t, "value", ve.Path)
}

// --- Boolean const ---

func TestValidate_Boolean_Const(t *testing.T) {
	t.Parallel()
	f := &lexicon.Field{Type: "boolean", Const: true}
	assert.NoError(t, ValidateValue(nil, "", f, true))
	assert.Error(t, ValidateValue(nil, "", f, false))
}
