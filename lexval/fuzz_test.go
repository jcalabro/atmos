package lexval

import (
	"testing"

	"github.com/jcalabro/atmos/lexicon"
)

var fuzzCat *lexicon.Catalog

func init() {
	schemas, err := lexicon.ParseDir(lexiconsDir())
	if err != nil {
		panic(err)
	}
	fuzzCat = lexicon.NewCatalog()
	if err := fuzzCat.AddAll(schemas); err != nil {
		panic(err)
	}
}

func FuzzValidateRecord(f *testing.F) {
	f.Add("Hello, world!", "2023-01-01T00:00:00Z")

	f.Fuzz(func(t *testing.T, text, createdAt string) {
		data := map[string]any{
			"text":      text,
			"createdAt": createdAt,
		}
		// Must not panic.
		_ = ValidateRecord(fuzzCat, "app.bsky.feed.post", data)
	})
}
