package lexicon

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_SimpleQuery(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "app.bsky.actor.getProfile",
		"defs": {
			"main": {
				"type": "query",
				"parameters": {
					"type": "params",
					"required": ["actor"],
					"properties": {
						"actor": {"type": "string", "format": "at-identifier"}
					}
				},
				"output": {
					"encoding": "application/json",
					"schema": {"type": "ref", "ref": "app.bsky.actor.defs#profileViewDetailed"}
				}
			}
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, "app.bsky.actor.getProfile", s.ID)
	assert.Equal(t, 1, s.Lexicon)

	main := s.Defs["main"]
	require.NotNil(t, main)
	assert.Equal(t, "query", main.Type)
	require.NotNil(t, main.Parameters)
	assert.Equal(t, []string{"actor"}, main.Parameters.Required)

	actor := main.Parameters.Properties["actor"]
	require.NotNil(t, actor)
	assert.Equal(t, "string", actor.Type)
	assert.Equal(t, "at-identifier", actor.Format)

	require.NotNil(t, main.Output)
	assert.Equal(t, "application/json", main.Output.Encoding)
	assert.Equal(t, "ref", main.Output.Schema.Type)
	assert.Equal(t, "app.bsky.actor.defs#profileViewDetailed", main.Output.Schema.Ref)
}

func TestParse_Record(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "app.bsky.feed.post",
		"defs": {
			"main": {
				"type": "record",
				"key": "tid",
				"record": {
					"type": "object",
					"required": ["text", "createdAt"],
					"properties": {
						"text": {"type": "string", "maxLength": 3000, "maxGraphemes": 300},
						"createdAt": {"type": "string", "format": "datetime"},
						"embed": {
							"type": "union",
							"refs": ["app.bsky.embed.images", "app.bsky.embed.video"]
						}
					}
				}
			},
			"replyRef": {
				"type": "object",
				"required": ["root", "parent"],
				"properties": {
					"root": {"type": "ref", "ref": "com.atproto.repo.strongRef"},
					"parent": {"type": "ref", "ref": "com.atproto.repo.strongRef"}
				}
			}
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, "app.bsky.feed.post", s.ID)

	main := s.Defs["main"]
	require.NotNil(t, main)
	assert.Equal(t, "record", main.Type)
	assert.Equal(t, "tid", main.Key)
	require.NotNil(t, main.Record)
	assert.Equal(t, []string{"text", "createdAt"}, main.Record.Required)

	text := main.Record.Properties["text"]
	require.NotNil(t, text)
	assert.Equal(t, 3000, text.MaxLength)
	assert.Equal(t, 300, text.MaxGraphemes)

	embed := main.Record.Properties["embed"]
	require.NotNil(t, embed)
	assert.Equal(t, "union", embed.Type)
	assert.Equal(t, []string{"app.bsky.embed.images", "app.bsky.embed.video"}, embed.Refs)

	reply := s.Defs["replyRef"]
	require.NotNil(t, reply)
	assert.Equal(t, "object", reply.Type)
}

func TestParse_Procedure(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "com.atproto.repo.createRecord",
		"defs": {
			"main": {
				"type": "procedure",
				"input": {
					"encoding": "application/json",
					"schema": {
						"type": "object",
						"required": ["repo", "collection", "record"],
						"properties": {
							"repo": {"type": "string", "format": "at-identifier"},
							"collection": {"type": "string", "format": "nsid"},
							"record": {"type": "unknown"},
							"validate": {"type": "boolean"}
						}
					}
				},
				"output": {
					"encoding": "application/json",
					"schema": {
						"type": "object",
						"required": ["uri", "cid"],
						"properties": {
							"uri": {"type": "string", "format": "at-uri"},
							"cid": {"type": "string", "format": "cid"}
						}
					}
				},
				"errors": [{"name": "InvalidSwap"}]
			}
		}
	}`))
	require.NoError(t, err)

	main := s.Defs["main"]
	assert.Equal(t, "procedure", main.Type)
	require.NotNil(t, main.Input)
	assert.Equal(t, "application/json", main.Input.Encoding)
	assert.Equal(t, "unknown", main.Input.Schema.Properties["record"].Type)
	require.Len(t, main.Errors, 1)
	assert.Equal(t, "InvalidSwap", main.Errors[0].Name)
}

func TestParse_Subscription(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "com.atproto.sync.subscribeRepos",
		"defs": {
			"main": {
				"type": "subscription",
				"parameters": {
					"type": "params",
					"properties": {
						"cursor": {"type": "integer"}
					}
				},
				"message": {
					"schema": {
						"type": "union",
						"refs": ["#commit", "#identity"]
					}
				}
			},
			"commit": {
				"type": "object",
				"required": ["seq", "repo"],
				"nullable": ["since"],
				"properties": {
					"seq": {"type": "integer"},
					"repo": {"type": "string", "format": "did"},
					"since": {"type": "string", "format": "tid"},
					"blocks": {"type": "bytes", "maxLength": 2000000},
					"commit": {"type": "cid-link"}
				}
			},
			"identity": {
				"type": "object",
				"required": ["seq", "did", "time"],
				"properties": {
					"seq": {"type": "integer"},
					"did": {"type": "string", "format": "did"},
					"time": {"type": "string", "format": "datetime"}
				}
			}
		}
	}`))
	require.NoError(t, err)

	main := s.Defs["main"]
	assert.Equal(t, "subscription", main.Type)
	require.NotNil(t, main.Message)
	assert.Equal(t, "union", main.Message.Schema.Type)
	assert.Equal(t, []string{"#commit", "#identity"}, main.Message.Schema.Refs)

	commit := s.Defs["commit"]
	assert.Equal(t, []string{"since"}, commit.Nullable)
	assert.Equal(t, "cid-link", commit.Properties["commit"].Type)
	assert.Equal(t, "bytes", commit.Properties["blocks"].Type)
	assert.Equal(t, 2000000, commit.Properties["blocks"].MaxLength)
}

func TestParse_Token(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "app.bsky.feed.defs",
		"defs": {
			"requestLess": {
				"type": "token",
				"description": "Request less content"
			}
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, "token", s.Defs["requestLess"].Type)
	assert.Equal(t, "Request less content", s.Defs["requestLess"].Desc)
}

func TestParse_BadVersion(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte(`{"lexicon": 2, "id": "test", "defs": {}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported version")
}

func TestParse_MissingID(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte(`{"lexicon": 1, "defs": {}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing id")
}

func TestParse_MissingDefs(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte(`{"lexicon": 1, "id": "test"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing defs")
}

func TestParse_InvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte(`not json`))
	require.Error(t, err)
}

func TestParse_BlobField(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "test.blob",
		"defs": {
			"main": {
				"type": "object",
				"properties": {
					"avatar": {
						"type": "blob",
						"accept": ["image/png", "image/jpeg"],
						"maxSize": 1000000
					}
				}
			}
		}
	}`))
	require.NoError(t, err)
	avatar := s.Defs["main"].Properties["avatar"]
	assert.Equal(t, "blob", avatar.Type)
	assert.Equal(t, []string{"image/png", "image/jpeg"}, avatar.Accept)
	assert.Equal(t, int64(1000000), avatar.MaxSize)
}

func TestParse_ArrayField(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "test.array",
		"defs": {
			"main": {
				"type": "object",
				"properties": {
					"tags": {
						"type": "array",
						"maxLength": 8,
						"items": {"type": "string", "maxLength": 640}
					}
				}
			}
		}
	}`))
	require.NoError(t, err)
	tags := s.Defs["main"].Properties["tags"]
	assert.Equal(t, "array", tags.Type)
	assert.Equal(t, 8, tags.MaxLength)
	require.NotNil(t, tags.Items)
	assert.Equal(t, "string", tags.Items.Type)
	assert.Equal(t, 640, tags.Items.MaxLength)
}

func TestParse_StringDef(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "test.stringdef",
		"defs": {
			"main": {
				"type": "string",
				"description": "A constrained string type",
				"format": "did",
				"maxLength": 2048,
				"minLength": 7,
				"maxGraphemes": 500,
				"minGraphemes": 1,
				"enum": ["did:plc:abc", "did:web:example.com"],
				"knownValues": ["did:plc:known"],
				"const": "did:plc:abc"
			}
		}
	}`))
	require.NoError(t, err)

	main := s.Defs["main"]
	require.NotNil(t, main)
	assert.Equal(t, "string", main.Type)
	assert.Equal(t, "did", main.Format)
	assert.Equal(t, 2048, main.MaxLength)
	assert.Equal(t, 7, main.MinLength)
	assert.Equal(t, 500, main.MaxGraphemes)
	assert.Equal(t, 1, main.MinGraphemes)
	assert.Equal(t, []string{"did:plc:abc", "did:web:example.com"}, main.Enum)
	assert.Equal(t, []string{"did:plc:known"}, main.KnownValues)
	assert.Equal(t, "did:plc:abc", main.Const)
}

func TestParse_IntegerDef(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "test.integerdef",
		"defs": {
			"main": {
				"type": "object",
				"properties": {
					"count": {
						"type": "integer",
						"minimum": 0,
						"maximum": 1000,
						"default": 10
					}
				}
			}
		}
	}`))
	require.NoError(t, err)

	count := s.Defs["main"].Properties["count"]
	require.NotNil(t, count)
	assert.Equal(t, "integer", count.Type)
	require.NotNil(t, count.Minimum)
	assert.Equal(t, int64(0), *count.Minimum)
	require.NotNil(t, count.Maximum)
	assert.Equal(t, int64(1000), *count.Maximum)
	assert.Equal(t, float64(10), count.Default)
}

func TestParse_NullField(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "test.nullfield",
		"defs": {
			"main": {
				"type": "object",
				"nullable": ["optional"],
				"properties": {
					"optional": {"type": "string"}
				}
			}
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"optional"}, s.Defs["main"].Nullable)
}

func TestParse_NestedObject(t *testing.T) {
	t.Parallel()
	s, err := Parse([]byte(`{
		"lexicon": 1,
		"id": "test.nested",
		"defs": {
			"main": {
				"type": "record",
				"key": "tid",
				"record": {
					"type": "object",
					"required": ["data"],
					"properties": {
						"data": {
							"type": "object",
							"required": ["items"],
							"properties": {
								"items": {
									"type": "array",
									"maxLength": 10,
									"items": {
										"type": "union",
										"refs": ["#itemA", "#itemB"],
										"closed": true
									}
								}
							}
						}
					}
				}
			},
			"itemA": {
				"type": "object",
				"properties": {
					"a": {"type": "string"}
				}
			},
			"itemB": {
				"type": "object",
				"properties": {
					"b": {"type": "integer"}
				}
			}
		}
	}`))
	require.NoError(t, err)

	main := s.Defs["main"]
	require.NotNil(t, main)
	assert.Equal(t, "record", main.Type)

	data := main.Record.Properties["data"]
	require.NotNil(t, data)
	assert.Equal(t, "object", data.Type)

	items := data.Properties["items"]
	require.NotNil(t, items)
	assert.Equal(t, "array", items.Type)
	assert.Equal(t, 10, items.MaxLength)

	require.NotNil(t, items.Items)
	assert.Equal(t, "union", items.Items.Type)
	assert.True(t, items.Items.Closed)
	assert.Equal(t, []string{"#itemA", "#itemB"}, items.Items.Refs)
}

func TestParseDirVendoredLexicons(t *testing.T) {
	t.Parallel()
	schemas, err := ParseDir("../lexicons")
	require.NoError(t, err)
	assert.Greater(t, len(schemas), 300)

	// Verify a known schema was parsed.
	var found bool
	for _, s := range schemas {
		if s.ID == "app.bsky.feed.post" {
			found = true
			assert.Equal(t, "record", s.Defs["main"].Type)
			break
		}
	}
	assert.True(t, found, "expected app.bsky.feed.post in vendored lexicons")
}
