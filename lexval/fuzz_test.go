package lexval

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
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

// fuzzCollections is the set of collections exercised by FuzzValidateRecordFromCBOR.
var fuzzCollections = []string{
	"app.bsky.feed.post",
	"app.bsky.actor.profile",
	"app.bsky.feed.like",
	"app.bsky.graph.follow",
	"app.bsky.feed.repost",
	"app.bsky.graph.block",
	"app.bsky.graph.list",
	"app.bsky.graph.listitem",
	"app.bsky.feed.generator",
	"app.bsky.feed.threadgate",
	"app.bsky.labeler.service",
}

// FuzzValidateRecordFromCBOR exercises the full validation tree with arbitrary
// CBOR-decoded data across multiple schema types. This covers unions, refs,
// nested objects, arrays, and diverse field types that the simpler
// FuzzValidateRecord does not reach.
func FuzzValidateRecordFromCBOR(f *testing.F) {
	// Seed with valid CBOR maps of varying shapes.
	seeds := [][]byte{
		// {"text":"hi","createdAt":"2024-01-01T00:00:00Z"}
		{0xa2, 0x64, 0x74, 0x65, 0x78, 0x74, 0x62, 0x68, 0x69,
			0x69, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74,
			0x74, 0x32, 0x30, 0x32, 0x34, 0x2d, 0x30, 0x31, 0x2d, 0x30,
			0x31, 0x54, 0x30, 0x30, 0x3a, 0x30, 0x30, 0x3a, 0x30, 0x30, 0x5a},
		{0xa0},       // empty map
		{0xa1, 0x61, 'a', 0x01}, // {"a": 1}
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		val, err := cbor.Unmarshal(data)
		if err != nil {
			return
		}
		m, ok := val.(map[string]any)
		if !ok {
			return
		}
		// Validate against each collection — must not panic.
		for _, collection := range fuzzCollections {
			_ = ValidateRecord(fuzzCat, collection, m)
		}
	})
}
