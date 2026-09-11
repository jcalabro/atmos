package declaration

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/lexicon"
	"github.com/stretchr/testify/require"
)

func validSchema() *lexicon.Schema {
	return &lexicon.Schema{
		Lexicon: 1,
		ID:      "com.example.forum",
		Defs: map[string]*lexicon.Def{"main": {
			Type: "space", Key: "literal:self", Name: "Example Forum",
			Names:       map[string]string{"es": "Foro de ejemplo"},
			Collections: []string{"com.example.post", "org.example.reaction"},
		}},
	}
}

func FuzzValidateDeclaration(f *testing.F) {
	seed, err := json.Marshal(validSchema())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte(`{"lexicon":1,"id":"com.example.bad","defs":{"main":{"type":"space"}}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		schema, err := lexicon.Parse(raw)
		if err != nil {
			return
		}
		_, _ = Validate(schema)
	})
}

func TestValidate(t *testing.T) {
	t.Parallel()
	got, err := Validate(validSchema())
	require.NoError(t, err)
	require.Equal(t, atmos.NSID("com.example.forum"), got.Type)
	require.Equal(t, "literal:self", got.Key)
	require.Equal(t, "Foro de ejemplo", got.Names[atmos.Language("es")])
	require.Equal(t, []atmos.NSID{"com.example.post", "org.example.reaction"}, got.Collections)
}

func TestValidateRejectsMalformed(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*lexicon.Schema){
		"wrong lexicon version": func(s *lexicon.Schema) { s.Lexicon = 2 },
		"missing main":          func(s *lexicon.Schema) { delete(s.Defs, "main") },
		"wrong type":            func(s *lexicon.Schema) { s.Defs["main"].Type = "record" },
		"missing key":           func(s *lexicon.Schema) { s.Defs["main"].Key = "" },
		"bad key":               func(s *lexicon.Schema) { s.Defs["main"].Key = "literal:." },
		"missing name":          func(s *lexicon.Schema) { s.Defs["main"].Name = "" },
		"long UTF16 name":       func(s *lexicon.Schema) { s.Defs["main"].Name = strings.Repeat("😀", 33) },
		"bad language":          func(s *lexicon.Schema) { s.Defs["main"].Names = map[string]string{"nope_": "x"} },
		"bad localized name":    func(s *lexicon.Schema) { s.Defs["main"].Names = map[string]string{"en": ""} },
		"duplicate language":    func(s *lexicon.Schema) { s.Defs["main"].Names = map[string]string{"en-US": "One", "EN-us": "Two"} },
		"wildcard collection":   func(s *lexicon.Schema) { s.Defs["main"].Collections = []string{"*"} },
		"duplicate collection":  func(s *lexicon.Schema) { s.Defs["main"].Collections = []string{"com.example.post", "com.example.post"} },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			schema := validSchema()
			mutate(schema)
			_, err := Validate(schema)
			require.Error(t, err)
		})
	}
}

func TestLexiconParsePreservesSpaceFields(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"lexicon":1,"id":"com.example.forum","defs":{"main":{"type":"space","description":"desc","key":"any","name":"Forum","name:lang":{"es":"Foro"},"collections":["com.example.post"]}}}`)
	schema, err := lexicon.Parse(raw)
	require.NoError(t, err)
	decl, err := Validate(schema)
	require.NoError(t, err)
	require.Equal(t, "desc", decl.Description)
	require.Equal(t, "Foro", decl.Names["es"])
}
