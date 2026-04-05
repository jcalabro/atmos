package lexgen

import (
	"testing"

	"github.com/jcalabro/atmos/lexicon"
	"github.com/stretchr/testify/assert"
)

func TestGenerate_ExtraFieldsOnStruct(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"text"},
				Properties: map[string]*lexicon.Field{
					"text": {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "extra []extraField")
}

func TestGenerate_ExtraFieldsOnRecord(t *testing.T) {
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

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "extra []extraField")
}

func TestGenerate_JSONUnmarshalCapturesExtras(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"text"},
				Properties: map[string]*lexicon.Field{
					"text": {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "s.extra = append(s.extra, extraField{Key: key")
	assert.Contains(t, code, "Encoding: extraEncodingJSON")
}

func TestGenerate_JSONMarshalEmitsExtras(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"text"},
				Properties: map[string]*lexicon.Field{
					"text": {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "for _, ef := range s.extra")
	assert.Contains(t, code, "ef.Encoding != extraEncodingJSON")
	assert.Contains(t, code, "cbor.AppendJSONString(buf, ef.Key)")
}

func TestGenerate_CBORUnmarshalCapturesExtras(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"text"},
				Properties: map[string]*lexicon.Field{
					"text": {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "s.extra = append(s.extra, extraField{Key: string(data[keyStart:keyEnd])")
	assert.Contains(t, code, "Encoding: extraEncodingCBOR")
}

func TestGenerate_CBORMarshalEmitsExtras(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"text"},
				Properties: map[string]*lexicon.Field{
					"text": {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "countExtra(s.extra, extraEncodingCBOR)")
	assert.Contains(t, code, "appendCBORExtrasBefore(s.extra, ei,")
}

func TestGenerate_NestedObjectHasExtraFields(t *testing.T) {
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
	count := countOccurrences(code, "extra []extraField")
	assert.GreaterOrEqual(t, count, 2, "both parent and nested types should have extra")
}

func TestGenerate_PerPackageExtraTypes(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"text"},
				Properties: map[string]*lexicon.Field{
					"text": {Type: "string"},
				},
			},
		},
	})

	// Per-package extra.go has unexported types and helpers.
	code := string(files["api/bsky/extra.go"])
	assert.Contains(t, code, "type extraField struct")
	assert.Contains(t, code, "type extraEncoding int8")
	assert.Contains(t, code, "extraEncodingJSON")
	assert.Contains(t, code, "extraEncodingCBOR")
	assert.Contains(t, code, "func countExtra(")
	assert.Contains(t, code, "func clearExtra(")
	assert.Contains(t, code, "func appendCBORExtrasBefore(")

	// Shared types still have LexBlob with extra field.
	typesCode := string(files["api/lextypes/types.go"])
	assert.Contains(t, typesCode, "extra []extraField")
}

func countOccurrences(s, substr string) int {
	count := 0
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			count++
		}
	}
	return count
}
