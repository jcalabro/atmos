package lexicon

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		current string
		ref     string
		nsid    string
		def     string
	}{
		{"local", "app.bsky.feed.post", "#replyRef", "app.bsky.feed.post", "replyRef"},
		{"external with def", "app.bsky.feed.post", "com.atproto.repo.defs#commitMeta", "com.atproto.repo.defs", "commitMeta"},
		{"external main", "app.bsky.feed.post", "com.atproto.repo.strongRef", "com.atproto.repo.strongRef", "main"},
		{"external with hash", "x", "app.bsky.embed.images#view", "app.bsky.embed.images", "view"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			nsid, def := SplitRef(tt.current, tt.ref)
			assert.Equal(t, tt.nsid, nsid)
			assert.Equal(t, tt.def, def)
		})
	}
}

func TestCatalog_Resolve_Success(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()

	s1 := &Schema{
		ID: "app.bsky.feed.post",
		Defs: map[string]*Def{
			"main": {
				Type: "record",
				Record: &Object{
					Properties: map[string]*Field{
						"reply": {Type: "ref", Ref: "#replyRef"},
					},
				},
			},
			"replyRef": {
				Type: "object",
				Properties: map[string]*Field{
					"root":   {Type: "ref", Ref: "com.atproto.repo.strongRef"},
					"parent": {Type: "ref", Ref: "com.atproto.repo.strongRef"},
				},
			},
		},
	}
	s2 := &Schema{
		ID: "com.atproto.repo.strongRef",
		Defs: map[string]*Def{
			"main": {Type: "object"},
		},
	}

	require.NoError(t, cat.Add(s1))
	require.NoError(t, cat.Add(s2))
	require.NoError(t, cat.Resolve())
}

func TestCatalog_Resolve_MissingSchema(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()
	s := &Schema{
		ID: "test",
		Defs: map[string]*Def{
			"main": {
				Type: "object",
				Properties: map[string]*Field{
					"ref": {Type: "ref", Ref: "nonexistent.schema#foo"},
				},
			},
		},
	}
	require.NoError(t, cat.Add(s))
	err := cat.Resolve()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent.schema")
}

func TestCatalog_Resolve_MissingDef(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()
	s1 := &Schema{
		ID: "a",
		Defs: map[string]*Def{
			"main": {
				Type: "object",
				Properties: map[string]*Field{
					"ref": {Type: "ref", Ref: "b#nonexistent"},
				},
			},
		},
	}
	s2 := &Schema{
		ID:   "b",
		Defs: map[string]*Def{"main": {Type: "object"}},
	}
	require.NoError(t, cat.Add(s1))
	require.NoError(t, cat.Add(s2))
	err := cat.Resolve()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestCatalog_Resolve_UnionRefs(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()
	s := &Schema{
		ID: "test",
		Defs: map[string]*Def{
			"main": {
				Type: "object",
				Properties: map[string]*Field{
					"embed": {
						Type: "union",
						Refs: []string{"#typeA", "#typeB"},
					},
				},
			},
			"typeA": {Type: "object"},
			"typeB": {Type: "object"},
		},
	}
	require.NoError(t, cat.Add(s))
	require.NoError(t, cat.Resolve())
}

func TestCatalog_Resolve_ArrayItemRef(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()
	s := &Schema{
		ID: "test",
		Defs: map[string]*Def{
			"main": {
				Type: "object",
				Properties: map[string]*Field{
					"items": {
						Type:  "array",
						Items: &Field{Type: "ref", Ref: "#item"},
					},
				},
			},
			"item": {Type: "object"},
		},
	}
	require.NoError(t, cat.Add(s))
	require.NoError(t, cat.Resolve())
}

func TestCatalog_DuplicateSchema(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()
	s := &Schema{ID: "test", Defs: map[string]*Def{"main": {Type: "object"}}}
	require.NoError(t, cat.Add(s))
	err := cat.Add(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestCatalog_Schemas_Sorted(t *testing.T) {
	t.Parallel()
	cat := NewCatalog()
	require.NoError(t, cat.Add(&Schema{ID: "c", Defs: map[string]*Def{"main": {Type: "object"}}}))
	require.NoError(t, cat.Add(&Schema{ID: "a", Defs: map[string]*Def{"main": {Type: "object"}}}))
	require.NoError(t, cat.Add(&Schema{ID: "b", Defs: map[string]*Def{"main": {Type: "object"}}}))

	schemas := cat.Schemas()
	require.Len(t, schemas, 3)
	assert.Equal(t, "a", schemas[0].ID)
	assert.Equal(t, "b", schemas[1].ID)
	assert.Equal(t, "c", schemas[2].ID)
}

func TestCatalog_ResolveAllVendoredLexicons(t *testing.T) {
	t.Parallel()
	schemas, err := ParseDir("../lexicons")
	require.NoError(t, err)

	cat := NewCatalog()
	require.NoError(t, cat.AddAll(schemas))
	require.NoError(t, cat.Resolve())
}
