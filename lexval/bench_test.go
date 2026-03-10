package lexval

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/lexicon"
)

func benchCatalog(b *testing.B) *lexicon.Catalog {
	b.Helper()
	schemas, err := lexicon.ParseDir(lexiconsDir())
	if err != nil {
		b.Fatal(err)
	}
	cat := lexicon.NewCatalog()
	if err := cat.AddAll(schemas); err != nil {
		b.Fatal(err)
	}
	return cat
}

func BenchmarkValidatePost(b *testing.B) {
	cat := benchCatalog(b)
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	data := map[string]any{
		"text":      strings.Repeat("a", 300),
		"createdAt": "2023-01-01T00:00:00Z",
		"langs":     []any{"en", "ja"},
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

	b.ResetTimer()
	for b.Loop() {
		_ = ValidateRecord(cat, "app.bsky.feed.post", data)
	}
}

func BenchmarkValidateProfile(b *testing.B) {
	cat := benchCatalog(b)
	data := map[string]any{
		"displayName": "Alice Smith",
		"description": "Hello, I am Alice!",
		"avatar": map[string]any{
			"$type":    "blob",
			"ref":      map[string]any{"$link": "bafyreib"},
			"mimeType": "image/jpeg",
			"size":     int64(50000),
		},
	}

	b.ResetTimer()
	for b.Loop() {
		_ = ValidateRecord(cat, "app.bsky.actor.profile", data)
	}
}
