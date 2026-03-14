package lexgen

import (
	"sync"
	"testing"

	"github.com/jcalabro/atmos/lexicon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	allVendoredOnce  sync.Once
	allVendoredFiles map[string][]byte
	allVendoredErr   error
)

func generateAllVendored() (map[string][]byte, error) {
	allVendoredOnce.Do(func() {
		schemas, err := lexicon.ParseDir("../lexicons")
		if err != nil {
			allVendoredErr = err
			return
		}
		cat := lexicon.NewCatalog()
		if err := cat.AddAll(schemas); err != nil {
			allVendoredErr = err
			return
		}
		if err := cat.Resolve(); err != nil {
			allVendoredErr = err
			return
		}
		allVendoredFiles, allVendoredErr = Generate(testConfig(), cat)
	})
	return allVendoredFiles, allVendoredErr
}

func testConfig() *Config {
	return &Config{
		Packages: []PackageConfig{
			{Prefix: "app.bsky", Package: "bsky", OutDir: "api/bsky", Import: "github.com/jcalabro/atmos/api/bsky"},
			{Prefix: "com.atproto", Package: "comatproto", OutDir: "api/comatproto", Import: "github.com/jcalabro/atmos/api/comatproto"},
			{Prefix: "chat.bsky", Package: "chatbsky", OutDir: "api/chatbsky", Import: "github.com/jcalabro/atmos/api/chatbsky"},
			{Prefix: "tools.ozone", Package: "ozone", OutDir: "api/ozone", Import: "github.com/jcalabro/atmos/api/ozone"},
		},
		SharedTypesDir:    "api/lextypes",
		SharedTypesPkg:    "lextypes",
		SharedTypesImport: "github.com/jcalabro/atmos/api/lextypes",
	}
}

// helper to generate a single schema and return the file contents.
func genOne(t *testing.T, s *lexicon.Schema) map[string][]byte {
	t.Helper()
	cat := lexicon.NewCatalog()
	require.NoError(t, cat.Add(s))
	require.NoError(t, cat.Resolve())
	files, err := Generate(testConfig(), cat)
	require.NoError(t, err)
	return files
}

// helper to generate multiple schemas and return the file contents.
func genMulti(t *testing.T, schemas ...*lexicon.Schema) map[string][]byte {
	t.Helper()
	cat := lexicon.NewCatalog()
	for _, s := range schemas {
		require.NoError(t, cat.Add(s))
	}
	require.NoError(t, cat.Resolve())
	files, err := Generate(testConfig(), cat)
	require.NoError(t, err)
	return files
}

func TestTypeName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		nsid string
		def  string
		want string
	}{
		{"app.bsky.feed.post", "main", "FeedPost"},
		{"app.bsky.feed.post", "replyRef", "FeedPost_ReplyRef"},
		{"app.bsky.feed.defs", "postView", "FeedDefs_PostView"},
		{"app.bsky.feed.defs", "main", "FeedDefs"},
		{"app.bsky.actor.getProfile", "main", "ActorGetProfile"},
		{"com.atproto.repo.createRecord", "main", "RepoCreateRecord"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, TypeName(tt.nsid, tt.def))
		})
	}
}

func TestSchemaFileName(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "feedpost.go", schemaFileName("app.bsky.feed.post"))
	assert.Equal(t, "feeddefs.go", schemaFileName("app.bsky.feed.defs"))
	assert.Equal(t, "actorgetprofile.go", schemaFileName("app.bsky.actor.getProfile"))
	assert.Equal(t, "repocreaterecord.go", schemaFileName("com.atproto.repo.createRecord"))
}

func TestExportFieldName(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "URI", exportFieldName("uri"))
	assert.Equal(t, "CID", exportFieldName("cid"))
	assert.Equal(t, "DID", exportFieldName("did"))
	assert.Equal(t, "URL", exportFieldName("url"))
	assert.Equal(t, "Text", exportFieldName("text"))
	assert.Equal(t, "CreatedAt", exportFieldName("createdAt"))
}

func TestGenerate_ErrorConstants(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.repo.applyWrites",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "procedure",
				Input: &lexicon.Body{Encoding: "application/json", Schema: &lexicon.Field{
					Type: "object", Required: []string{"repo"},
					Properties: map[string]*lexicon.Field{"repo": {Type: "string"}},
				}},
				Errors: []lexicon.ErrorDef{
					{Name: "InvalidSwap"},
					{Name: "DuplicateCreate", Desc: "A create op for a record that already exists."},
				},
			},
		},
	})

	code := string(files["api/comatproto/repoapplywrites.go"])
	assert.Contains(t, code, `ErrRepoApplyWrites_InvalidSwap`)
	assert.Contains(t, code, `"InvalidSwap"`)
	assert.Contains(t, code, `ErrRepoApplyWrites_DuplicateCreate`)
	assert.Contains(t, code, `"DuplicateCreate"`)
	assert.Contains(t, code, "A create op for a record that already exists.")
}

func TestGenerate_ErrorConstants_NoErrors(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.actor.getProfile",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "query",
				Parameters: &lexicon.Params{
					Type: "params", Required: []string{"actor"},
					Properties: map[string]*lexicon.Field{"actor": {Type: "string"}},
				},
				Output: &lexicon.Body{Encoding: "application/json", Schema: &lexicon.Field{
					Type: "object", Required: []string{"did"},
					Properties: map[string]*lexicon.Field{"did": {Type: "string"}},
				}},
			},
		},
	})

	code := string(files["api/bsky/actorgetprofile.go"])
	assert.NotContains(t, code, "Err")
}

func TestGoParamName(t *testing.T) {
	t.Parallel()
	tests := []struct{ input, want string }{
		{"actor", "actor"},
		{"type", "typ"},
		{"range", "rng"},
		{"default", "def"},
		{"select", "sel"},
		{"case", "cas"},
		{"func", "fn"},
		{"var", "v"},
		{"go", "g"},
		{"chan", "ch"},
		{"map", "mp"},
		{"interface", "iface"},
		{"struct", "st"},
		{"return", "ret"},
		{"break", "brk"},
		{"continue", "cont"},
		{"switch", "sw"},
		{"import", "imp"},
		{"package", "pkg"},
		{"defer", "dfr"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, goParamName(tt.input))
		})
	}
}

func TestFindPackage(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	assert.Equal(t, "bsky", findPackage(cfg, "app.bsky.feed.post").Package)
	assert.Equal(t, "comatproto", findPackage(cfg, "com.atproto.repo.createRecord").Package)
	assert.Nil(t, findPackage(cfg, "unknown.namespace"))
}

func TestGenerate_SimpleObject(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.actor.defs",
		Defs: map[string]*lexicon.Def{
			"viewerState": {
				Type: "object",
				Desc: "viewer state",
				Properties: map[string]*lexicon.Field{
					"muted":   {Type: "boolean"},
					"blocked": {Type: "string", Format: "at-uri"},
				},
			},
		},
	})

	code := string(files["api/bsky/actordefs.go"])
	assert.Contains(t, code, "type ActorDefs_ViewerState struct")
	assert.Contains(t, code, "gt.Option[bool]")
	assert.Contains(t, code, "gt.Option[string]")
	assert.Contains(t, code, `json:"muted,omitzero"`)
	assert.Contains(t, code, `json:"blocked,omitzero"`)
}

func TestGenerate_Token(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.defs",
		Defs: map[string]*lexicon.Def{
			"requestLess": {Type: "token", Desc: "Request less content"},
			"requestMore": {Type: "token", Desc: "Request more content"},
		},
	})

	code := string(files["api/bsky/feeddefs.go"])
	assert.Contains(t, code, `FeedDefs_RequestLess = "app.bsky.feed.defs#requestLess"`)
	assert.Contains(t, code, `FeedDefs_RequestMore = "app.bsky.feed.defs#requestMore"`)
}

func TestGenerate_Query(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.actor.getProfile",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "query",
				Parameters: &lexicon.Params{
					Type: "params", Required: []string{"actor"},
					Properties: map[string]*lexicon.Field{
						"actor": {Type: "string", Format: "at-identifier"},
					},
				},
				Output: &lexicon.Body{
					Encoding: "application/json",
					Schema: &lexicon.Field{
						Type:       "object",
						Required:   []string{"handle", "did"},
						Properties: map[string]*lexicon.Field{"handle": {Type: "string"}, "did": {Type: "string"}},
					},
				},
			},
		},
	})

	code := string(files["api/bsky/actorgetprofile.go"])
	assert.Contains(t, code, "func ActorGetProfile(ctx context.Context, c *xrpc.Client, actor string)")
	assert.Contains(t, code, `c.Query(ctx, "app.bsky.actor.getProfile"`)
	assert.Contains(t, code, "type ActorGetProfile_Output struct")
}

func TestGenerate_Procedure(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.server.createSession",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "procedure",
				Input: &lexicon.Body{Encoding: "application/json", Schema: &lexicon.Field{
					Type: "object", Required: []string{"identifier", "password"},
					Properties: map[string]*lexicon.Field{"identifier": {Type: "string"}, "password": {Type: "string"}},
				}},
				Output: &lexicon.Body{Encoding: "application/json", Schema: &lexicon.Field{
					Type: "object", Required: []string{"accessJwt", "refreshJwt", "did"},
					Properties: map[string]*lexicon.Field{"accessJwt": {Type: "string"}, "refreshJwt": {Type: "string"}, "did": {Type: "string"}},
				}},
			},
		},
	})

	code := string(files["api/comatproto/servercreatesession.go"])
	assert.Contains(t, code, "func ServerCreateSession(ctx context.Context, c *xrpc.Client, input *ServerCreateSession_Input)")
	assert.Contains(t, code, `c.Procedure(ctx, "com.atproto.server.createSession"`)
	assert.Contains(t, code, "type ServerCreateSession_Input struct")
	assert.Contains(t, code, "type ServerCreateSession_Output struct")
}

func TestGenerate_Record(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "record", Key: "tid",
				Record: &lexicon.Object{
					Type: "object", Required: []string{"text", "createdAt"},
					Properties: map[string]*lexicon.Field{
						"text": {Type: "string", MaxLength: 3000}, "createdAt": {Type: "string", Format: "datetime"},
						"langs": {Type: "array", MaxLength: 3, Items: &lexicon.Field{Type: "string"}},
					},
				},
			},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	assert.Contains(t, code, "type FeedPost struct")
	assert.Contains(t, code, `json:"$type,omitempty"`)
	assert.Contains(t, code, `cborgen:"$type,const=app.bsky.feed.post"`)
	// Required fields: value types, no omit.
	assert.Contains(t, code, `json:"text"`)
	assert.Contains(t, code, `json:"createdAt"`)
	// Optional array: omitempty.
	assert.Contains(t, code, `json:"langs,omitempty"`)
}

// --- Union tests ---

func TestGenerate_UnionOpenWithUnknown(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.post",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"embed"},
				Properties: map[string]*lexicon.Field{
					"embed": {Type: "union", Refs: []string{"#typeA", "#typeB"}},
				},
			},
			"typeA": {Type: "object", Properties: map[string]*lexicon.Field{"a": {Type: "string"}}},
			"typeB": {Type: "object", Properties: map[string]*lexicon.Field{"b": {Type: "string"}}},
		},
	})

	code := string(files["api/bsky/feedpost.go"])
	// Required union: value type, no omitzero.
	assert.Contains(t, code, "type FeedPost_Embed struct")
	assert.Contains(t, code, `json:"embed"`)
	assert.NotContains(t, code, `json:"embed,omitzero"`)
	// Open union has Unknown field.
	assert.Contains(t, code, "Unknown")
	assert.Contains(t, code, "gt.Ref[lextypes.UnknownUnionVariant]")
	// Has MarshalJSON and UnmarshalJSON.
	assert.Contains(t, code, "func (u FeedPost_Embed) MarshalJSON()")
	assert.Contains(t, code, "func (u *FeedPost_Embed) UnmarshalJSON(data []byte)")
	// Union variants use gt.Ref.
	assert.Contains(t, code, "gt.Ref[FeedPost_TypeA]")
	assert.Contains(t, code, "gt.Ref[FeedPost_TypeB]")
	// MarshalJSON sets LexiconTypeID.
	assert.Contains(t, code, `LexiconTypeID = "app.bsky.feed.post#typeA"`)
	// UnmarshalJSON uses gt.SomeRef.
	assert.Contains(t, code, "gt.SomeRef(v)")
}

func TestGenerate_UnionClosed(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object",
				Properties: map[string]*lexicon.Field{
					"reason": {Type: "union", Refs: []string{"#typeA"}, Closed: true},
				},
			},
			"typeA": {Type: "object"},
		},
	})

	code := string(files["api/bsky/feeddefs.go"])
	// Closed union: no Unknown field.
	assert.NotContains(t, code, "Unknown gt.Ref[lextypes.UnknownUnionVariant]")
	// UnmarshalJSON returns error for unknown types.
	assert.Contains(t, code, `fmt.Errorf("unknown type`)
}

// --- Cross-package reference tests ---

func TestGenerate_CrossPackageRef(t *testing.T) {
	t.Parallel()
	files := genMulti(t,
		&lexicon.Schema{
			Lexicon: 1, ID: "com.atproto.repo.strongRef",
			Defs: map[string]*lexicon.Def{
				"main": {Type: "object", Required: []string{"uri", "cid"}, Properties: map[string]*lexicon.Field{
					"uri": {Type: "string"}, "cid": {Type: "string"},
				}},
			},
		},
		&lexicon.Schema{
			Lexicon: 1, ID: "app.bsky.feed.post",
			Defs: map[string]*lexicon.Def{
				"main": {
					Type: "object", Required: []string{"root"},
					Properties: map[string]*lexicon.Field{
						"root": {Type: "ref", Ref: "com.atproto.repo.strongRef"},
					},
				},
			},
		},
	)

	code := string(files["api/bsky/feedpost.go"])
	// Cross-package ref: qualified type, correct import.
	assert.Contains(t, code, "comatproto.RepoStrongRef")
	assert.Contains(t, code, `comatproto "github.com/jcalabro/atmos/api/comatproto"`)
	// Required ref: value type, no omitzero.
	assert.Contains(t, code, `json:"root"`)
	assert.NotContains(t, code, `json:"root,omitzero"`)
}

// --- Nullable required field tests ---

func TestGenerate_NullableRequired(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.sync.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"since", "name"}, Nullable: []string{"since"},
				Properties: map[string]*lexicon.Field{
					"since": {Type: "string"},
					"name":  {Type: "string"},
				},
			},
		},
	})

	code := string(files["api/comatproto/syncdefs.go"])
	// Nullable+required: gt.Option, NO omitzero (must always serialize, as null when None).
	assert.Contains(t, code, "gt.Option[string]")
	assert.Contains(t, code, `json:"since"`)
	assert.NotContains(t, code, `json:"since,omitzero"`)
	// Required non-nullable: plain string, no omit.
	assert.Contains(t, code, `json:"name"`)
}

// --- Field type tests ---

func TestGenerate_BlobField(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.embed.images",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"image"},
				Properties: map[string]*lexicon.Field{
					"image":  {Type: "blob", Accept: []string{"image/*"}, MaxSize: 1000000},
					"avatar": {Type: "blob"},
				},
			},
		},
	})

	code := string(files["api/bsky/embedimages.go"])
	// Required blob: value type.
	assert.Contains(t, code, `Image`)
	assert.Contains(t, code, `lextypes.LexBlob`)
	assert.Contains(t, code, `json:"image"`)
	// Optional blob: gt.Option.
	assert.Contains(t, code, "gt.Option[lextypes.LexBlob]")
	assert.Contains(t, code, `json:"avatar,omitzero"`)
}

func TestGenerate_CIDLinkField(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.sync.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"commit"},
				Properties: map[string]*lexicon.Field{
					"commit":   {Type: "cid-link"},
					"prevData": {Type: "cid-link"},
				},
			},
		},
	})

	code := string(files["api/comatproto/syncdefs.go"])
	// Required cid-link: value type.
	assert.Contains(t, code, `json:"commit"`)
	assert.NotContains(t, code, `json:"commit,omitzero"`)
	// Optional cid-link: gt.Option.
	assert.Contains(t, code, "gt.Option[lextypes.LexCIDLink]")
	assert.Contains(t, code, `json:"prevData,omitzero"`)
}

func TestGenerate_BytesField(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.sync.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"blocks"},
				Properties: map[string]*lexicon.Field{
					"blocks": {Type: "bytes", MaxLength: 2000000},
				},
			},
		},
	})

	code := string(files["api/comatproto/syncdefs.go"])
	assert.Contains(t, code, "[]byte")
	assert.Contains(t, code, `json:"blocks"`)
}

func TestGenerate_UnknownField(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.repo.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"record"},
				Properties: map[string]*lexicon.Field{
					"record": {Type: "unknown"},
					"debug":  {Type: "unknown"},
				},
			},
		},
	})

	code := string(files["api/comatproto/repodefs.go"])
	assert.Contains(t, code, "json.RawMessage")
	assert.Contains(t, code, `json:"record"`)
	assert.Contains(t, code, `json:"debug,omitempty"`)
}

// --- Subscription test ---

func TestGenerate_Subscription(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.sync.subscribeRepos",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "subscription",
				Parameters: &lexicon.Params{
					Type:       "params",
					Properties: map[string]*lexicon.Field{"cursor": {Type: "integer"}},
				},
				Message: &lexicon.Message{
					Schema: &lexicon.Field{Type: "union", Refs: []string{"#commit", "#identity"}},
				},
			},
			"commit":   {Type: "object", Required: []string{"seq"}, Properties: map[string]*lexicon.Field{"seq": {Type: "integer"}}},
			"identity": {Type: "object", Required: []string{"did"}, Properties: map[string]*lexicon.Field{"did": {Type: "string"}}},
		},
	})

	code := string(files["api/comatproto/syncsubscriberepos.go"])
	// Generates message union type.
	assert.Contains(t, code, "type SyncSubscribeRepos_Message struct")
	assert.Contains(t, code, "gt.Ref[SyncSubscribeRepos_Commit]")
	assert.Contains(t, code, "gt.Ref[SyncSubscribeRepos_Identity]")
	// Open union has Unknown.
	assert.Contains(t, code, "gt.Ref[lextypes.UnknownUnionVariant]")
	// No client function generated for subscription (WebSocket, not HTTP).
	assert.NotContains(t, code, "func SyncSubscribeRepos(")
}

// --- Blob upload procedure test ---

func TestGenerate_BlobUploadProcedure(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.repo.uploadBlob",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type:  "procedure",
				Input: &lexicon.Body{Encoding: "*/*"},
				Output: &lexicon.Body{Encoding: "application/json", Schema: &lexicon.Field{
					Type: "object", Required: []string{"blob"},
					Properties: map[string]*lexicon.Field{"blob": {Type: "blob"}},
				}},
			},
		},
	})

	code := string(files["api/comatproto/repouploadblob.go"])
	// Blob upload uses io.Reader + contentType, not input struct.
	assert.Contains(t, code, "contentType string, body io.Reader")
	assert.Contains(t, code, `c.Do(ctx, "POST"`)
	assert.Contains(t, code, "type RepoUploadBlob_Output struct")
}

// --- Top-level array def test ---

func TestGenerate_ArrayDef(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.actor.defs",
		Defs: map[string]*lexicon.Def{
			"preferences": {
				Type: "array",
				Items: &lexicon.Field{
					Type: "union", Refs: []string{"#prefA", "#prefB"}, Closed: true,
				},
			},
			"prefA": {Type: "object", Properties: map[string]*lexicon.Field{"a": {Type: "string"}}},
			"prefB": {Type: "object", Properties: map[string]*lexicon.Field{"b": {Type: "string"}}},
		},
	})

	code := string(files["api/bsky/actordefs.go"])
	// Array def generates a type alias for a slice of the union.
	assert.Contains(t, code, "type ActorDefs_Preferences =")
	assert.Contains(t, code, "ActorDefs_Preferences_Elem")
	// The element union type is generated.
	assert.Contains(t, code, "type ActorDefs_Preferences_Elem struct")
}

func TestGenerate_ArrayDef_NonUnion(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.actor.defs",
		Defs: map[string]*lexicon.Def{
			"tags": {
				Type:  "array",
				Items: &lexicon.Field{Type: "string"},
			},
		},
	})

	code := string(files["api/bsky/actordefs.go"])
	// Non-union array def generates a type alias for []elemType.
	assert.Contains(t, code, "type ActorDefs_Tags = []string")
}

func TestGenerate_ArrayDef_NoItems(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.actor.defs",
		Defs: map[string]*lexicon.Def{
			"data": {
				Type: "array",
			},
		},
	})

	code := string(files["api/bsky/actordefs.go"])
	// Array def with no items falls back to []json.RawMessage.
	assert.Contains(t, code, "type ActorDefs_Data = []json.RawMessage")
}

// --- String def with knownValues test ---

func TestGenerate_StringDefKnownValues(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "com.atproto.moderation.defs",
		Defs: map[string]*lexicon.Def{
			"reasonType": {
				Type:        "string",
				KnownValues: []string{"com.atproto.moderation.defs#reasonSpam", "com.atproto.moderation.defs#reasonOther"},
			},
		},
	})

	code := string(files["api/comatproto/moderationdefs.go"])
	// Generates type alias.
	assert.Contains(t, code, "type ModerationDefs_ReasonType = string")
	// Generates constants.
	assert.Contains(t, code, `"com.atproto.moderation.defs#reasonSpam"`)
	assert.Contains(t, code, `"com.atproto.moderation.defs#reasonOther"`)
}

// --- Endpoint output as ref test ---

func TestGenerate_QueryOutputRef(t *testing.T) {
	t.Parallel()
	files := genMulti(t,
		&lexicon.Schema{
			Lexicon: 1, ID: "app.bsky.actor.defs",
			Defs: map[string]*lexicon.Def{
				"profileView": {Type: "object", Required: []string{"did"}, Properties: map[string]*lexicon.Field{
					"did": {Type: "string"},
				}},
			},
		},
		&lexicon.Schema{
			Lexicon: 1, ID: "app.bsky.actor.getProfile",
			Defs: map[string]*lexicon.Def{
				"main": {
					Type: "query",
					Parameters: &lexicon.Params{Type: "params", Required: []string{"actor"}, Properties: map[string]*lexicon.Field{
						"actor": {Type: "string"},
					}},
					Output: &lexicon.Body{Encoding: "application/json", Schema: &lexicon.Field{
						Type: "ref", Ref: "app.bsky.actor.defs#profileView",
					}},
				},
			},
		},
	)

	code := string(files["api/bsky/actorgetprofile.go"])
	// Output is a type alias for the referenced type.
	assert.Contains(t, code, "type ActorGetProfile_Output = ActorDefs_ProfileView")
}

// --- Nested inline object test ---

func TestGenerate_NestedInlineObject(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object",
				Properties: map[string]*lexicon.Field{
					"metadata": {
						Type:     "object",
						Required: []string{"source"},
						Properties: map[string]*lexicon.Field{
							"source": {Type: "string"},
							"lang":   {Type: "string"},
						},
					},
				},
			},
		},
	})

	code := string(files["api/bsky/feeddefs.go"])
	// Nested object generates a separate type.
	assert.Contains(t, code, "type FeedDefs_Metadata struct")
	assert.Contains(t, code, `json:"source"`)
	// Optional nested object in parent.
	assert.Contains(t, code, "gt.Option[FeedDefs_Metadata]")
	assert.Contains(t, code, `json:"metadata,omitzero"`)
}

// --- Array of refs test ---

func TestGenerate_ArrayOfRefs(t *testing.T) {
	t.Parallel()
	files := genOne(t, &lexicon.Schema{
		Lexicon: 1, ID: "app.bsky.feed.defs",
		Defs: map[string]*lexicon.Def{
			"main": {
				Type: "object", Required: []string{"items"},
				Properties: map[string]*lexicon.Field{
					"items": {Type: "array", Items: &lexicon.Field{Type: "ref", Ref: "#itemView"}},
				},
			},
			"itemView": {Type: "object", Properties: map[string]*lexicon.Field{"id": {Type: "string"}}},
		},
	})

	code := string(files["api/bsky/feeddefs.go"])
	// Array of refs: plain types, no Option wrapping on elements.
	assert.Contains(t, code, "[]FeedDefs_ItemView")
	assert.NotContains(t, code, "[]gt.Option[FeedDefs_ItemView]")
}

// --- All vendored lexicons test ---

func TestGenerate_AllVendoredLexicons(t *testing.T) {
	t.Parallel()
	files, err := generateAllVendored()
	require.NoError(t, err)

	assert.Greater(t, len(files), 100)
	assert.Contains(t, files, "api/bsky/feedpost.go")
	assert.Contains(t, files, "api/bsky/feeddefs.go")
	assert.Contains(t, files, "api/comatproto/repocreaterecord.go")
	assert.Contains(t, files, "api/comatproto/servercreatesession.go")
	assert.Contains(t, files, "api/lextypes/types.go")
	// Shared types no longer duplicated per-package.
	assert.NotContains(t, files, "api/bsky/types.go")
	assert.NotContains(t, files, "api/comatproto/types.go")
}
