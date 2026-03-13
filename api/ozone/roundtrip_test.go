package ozone

import (
	"encoding/json"
	"testing"

	comatproto "github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ModerationEmitEvent_Input (open unions, optional nested object, array of strings) ---

func TestModerationEmitEvent_Input_RoundTrip(t *testing.T) {
	t.Parallel()

	v := &ModerationEmitEvent_Input{
		CreatedBy: "did:plc:moderator",
		Event: ModerationEmitEvent_Input_Event{
			ModerationDefs_ModEventAcknowledge: gt.SomeRef(ModerationDefs_ModEventAcknowledge{
				Comment: gt.Some("reviewed and acknowledged"),
			}),
		},
		Subject: ModerationEmitEvent_Input_Subject{
			AdminDefs_RepoRef: gt.SomeRef(comatproto.AdminDefs_RepoRef{
				DID: "did:plc:badactor",
			}),
		},
		SubjectBlobCids: []string{"bafyreihash1", "bafyreihash2"},
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalCBOR()
		require.NoError(t, err)
		var decoded ModerationEmitEvent_Input
		require.NoError(t, decoded.UnmarshalCBOR(data))
		reencoded, err := decoded.MarshalCBOR()
		require.NoError(t, err)
		assert.Equal(t, data, reencoded)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.True(t, json.Valid(data))
		var decoded ModerationEmitEvent_Input
		require.NoError(t, decoded.UnmarshalJSON(data))
		reencoded, err := decoded.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, string(data), string(reencoded))
	})
}

// --- Unknown union variant round-trip (open union) ---

func TestModerationEmitEvent_UnknownUnionVariant(t *testing.T) {
	t.Parallel()

	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		// Construct JSON with an unrecognized $type for the event union
		raw := `{
			"createdBy": "did:plc:mod",
			"event": {"$type": "com.example.future#newEvent", "foo": "bar"},
			"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": "did:plc:x"}
		}`
		var v ModerationEmitEvent_Input
		require.NoError(t, v.UnmarshalJSON([]byte(raw)))

		// The unknown variant should be populated
		require.True(t, v.Event.Unknown.HasVal())
		assert.Equal(t, "com.example.future#newEvent", v.Event.Unknown.Val().Type)
		assert.NotEmpty(t, v.Event.Unknown.Val().Raw)

		// Re-marshal and unmarshal again — verify stability
		data2, err := v.MarshalJSON()
		require.NoError(t, err)
		var v2 ModerationEmitEvent_Input
		require.NoError(t, v2.UnmarshalJSON(data2))
		assert.Equal(t, v.Event.Unknown.Val().Type, v2.Event.Unknown.Val().Type)
	})

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		v := &ModerationEmitEvent_Input{
			CreatedBy: "did:plc:mod",
			Event: ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventAcknowledge: gt.SomeRef(ModerationDefs_ModEventAcknowledge{
					Comment: gt.Some("ok"),
				}),
			},
			Subject: ModerationEmitEvent_Input_Subject{
				RepoStrongRef: gt.SomeRef(comatproto.RepoStrongRef{
					CID: "bafyreihash1",
					URI: "at://did:plc:x/app.bsky.feed.post/abc",
				}),
			},
		}
		data, err := v.MarshalCBOR()
		require.NoError(t, err)
		var decoded ModerationEmitEvent_Input
		require.NoError(t, decoded.UnmarshalCBOR(data))
		reencoded, err := decoded.MarshalCBOR()
		require.NoError(t, err)
		assert.Equal(t, data, reencoded)
	})
}
