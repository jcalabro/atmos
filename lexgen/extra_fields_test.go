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

	// Struct has extra field slices.
	assert.Contains(t, code, "extraJSON []lextypes.ExtraField")
	assert.Contains(t, code, "extraCBOR []lextypes.ExtraField")
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
	assert.Contains(t, code, "extraJSON []lextypes.ExtraField")
	assert.Contains(t, code, "extraCBOR []lextypes.ExtraField")
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

	// JSON unmarshal default case captures extras instead of just skipping.
	assert.Contains(t, code, "s.extraJSON = append(s.extraJSON, lextypes.ExtraField{Key: key")
	assert.Contains(t, code, "data[valueStart:pos]")
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

	// JSON marshal emits extras after known fields.
	assert.Contains(t, code, "for _, ef := range s.extraJSON")
	assert.Contains(t, code, "cbor.AppendJSONString(buf, ef.Key)")
	assert.Contains(t, code, "append(buf, ef.Value...)")
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

	// CBOR unmarshal captures extras in both the else branch and default case.
	assert.Contains(t, code, "s.extraCBOR = append(s.extraCBOR, lextypes.ExtraField{Key: string(data[keyStart:keyEnd])")
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

	// CBOR marshal includes extras in map header count.
	assert.Contains(t, code, "len(s.extraCBOR)")
	// Uses AppendCBORExtrasBefore for DAG-CBOR ordered interleaving.
	assert.Contains(t, code, "lextypes.AppendCBORExtrasBefore(s.extraCBOR, ei,")
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

	// Both the parent and the nested type should have extra fields.
	// Count occurrences — should be at least 2 for each (parent + nested).
	jsonCount := countOccurrences(code, "extraJSON []lextypes.ExtraField")
	cborCount := countOccurrences(code, "extraCBOR []lextypes.ExtraField")
	assert.GreaterOrEqual(t, jsonCount, 2, "both parent and nested types should have extraJSON")
	assert.GreaterOrEqual(t, cborCount, 2, "both parent and nested types should have extraCBOR")
}

func TestGenerate_SharedTypesHaveExtraField(t *testing.T) {
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

	code := string(files["api/lextypes/types.go"])

	// ExtraField type is defined.
	assert.Contains(t, code, "type ExtraField struct")
	assert.Contains(t, code, "Key   string")
	assert.Contains(t, code, "Value []byte")

	// AppendCBORExtrasBefore helper is defined.
	assert.Contains(t, code, "func AppendCBORExtrasBefore(")

	// LexBlob has extra fields.
	assert.Contains(t, code, "extraJSON []ExtraField")
	assert.Contains(t, code, "extraCBOR []ExtraField")
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
