package comatproto

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ServerCreateSession_Output (gt.Option[string], gt.Option[bool], json.RawMessage) ---

func TestServerCreateSession_Output_RoundTrip(t *testing.T) {
	t.Parallel()

	v := &ServerCreateSession_Output{
		AccessJwt:       "eyJhbGciOiJIUzI1NiJ9.access",
		RefreshJwt:      "eyJhbGciOiJIUzI1NiJ9.refresh",
		Handle:          "alice.bsky.social",
		DID:             "did:plc:abc123",
		Email:           gt.Some("alice@example.com"),
		EmailConfirmed:  gt.Some(true),
		EmailAuthFactor: gt.Some(false),
		Active:          gt.Some(true),
		DidDoc:          json.RawMessage(`{"id":"did:plc:abc123"}`),
		Status:          gt.Some("active"),
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		// DidDoc is json.RawMessage — not preserved in CBOR round-trip.
		// Test CBOR with DidDoc nil.
		vCBOR := *v
		vCBOR.DidDoc = nil
		data, err := vCBOR.MarshalCBOR()
		require.NoError(t, err)
		var decoded ServerCreateSession_Output
		require.NoError(t, decoded.UnmarshalCBOR(data))
		assert.Equal(t, vCBOR, decoded)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.True(t, json.Valid(data))
		var decoded ServerCreateSession_Output
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.Equal(t, *v, decoded)
	})
}

// --- LabelDefs_Label (bytes field, gt.Option[string], gt.Option[int64], gt.Option[bool]) ---

func TestLabelDefs_Label_RoundTrip(t *testing.T) {
	t.Parallel()

	v := &LabelDefs_Label{
		Src: "did:plc:labeler",
		URI: "at://did:plc:abc/app.bsky.feed.post/xyz",
		CID: gt.Some("bafyreihash1"),
		Val: "!warn",
		Neg: gt.Some(false),
		Cts: "2024-06-15T12:00:00Z",
		Exp: gt.Some("2025-06-15T12:00:00Z"),
		Ver: gt.Some(int64(1)),
		Sig: []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04},
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalCBOR()
		require.NoError(t, err)
		var decoded LabelDefs_Label
		require.NoError(t, decoded.UnmarshalCBOR(data))
		assert.Equal(t, *v, decoded)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.True(t, json.Valid(data))
		var decoded LabelDefs_Label
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.Equal(t, *v, decoded)
	})
}

// --- LabelDefs_Label empty Sig (bug 13: nil vs empty []byte) ---

func TestLabelDefs_Label_EmptySig(t *testing.T) {
	t.Parallel()

	v := &LabelDefs_Label{
		Src: "did:plc:labeler",
		URI: "at://did:plc:abc/app.bsky.feed.post/xyz",
		Val: "!warn",
		Cts: "2024-06-15T12:00:00Z",
		Sig: []byte{}, // explicitly set to empty, not nil
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalCBOR()
		require.NoError(t, err)
		var decoded LabelDefs_Label
		require.NoError(t, decoded.UnmarshalCBOR(data))
		assert.NotNil(t, decoded.Sig, "empty Sig should survive CBOR round-trip as non-nil")
		assert.Empty(t, decoded.Sig)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), `"sig"`, "empty Sig should be present in JSON")
		var decoded LabelDefs_Label
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.NotNil(t, decoded.Sig, "empty Sig should survive JSON round-trip as non-nil")
		assert.Empty(t, decoded.Sig)
	})
}

// --- RepoApplyWrites_Input (array of unions, required string) ---

func TestRepoApplyWrites_Input_RoundTrip(t *testing.T) {
	t.Parallel()

	v := &RepoApplyWrites_Input{
		Repo:       "did:plc:abc123",
		SwapCommit: gt.Some("bafyreihash1"),
		Validate:   gt.Some(true),
		Writes: []RepoApplyWrites_Input_Writes{
			{RepoApplyWrites_Create: gt.SomeRef(RepoApplyWrites_Create{
				Collection: "app.bsky.feed.post",
				Rkey:       gt.Some("tid123"),
				Value:      json.RawMessage(`{"text":"hello"}`),
			})},
			{RepoApplyWrites_Delete: gt.SomeRef(RepoApplyWrites_Delete{
				Collection: "app.bsky.feed.like",
				Rkey:       "tid456",
			})},
		},
	}

	t.Run("CBOR", func(t *testing.T) {
		t.Parallel()
		// Value fields are json.RawMessage — not preserved in CBOR.
		// Test CBOR with nil Value fields.
		vCBOR := *v
		vCBOR.Writes = []RepoApplyWrites_Input_Writes{
			{RepoApplyWrites_Create: gt.SomeRef(RepoApplyWrites_Create{
				Collection: "app.bsky.feed.post",
				Rkey:       gt.Some("tid123"),
			})},
			{RepoApplyWrites_Delete: gt.SomeRef(RepoApplyWrites_Delete{
				Collection: "app.bsky.feed.like",
				Rkey:       "tid456",
			})},
		}
		data, err := vCBOR.MarshalCBOR()
		require.NoError(t, err)
		var decoded RepoApplyWrites_Input
		require.NoError(t, decoded.UnmarshalCBOR(data))
		assert.Equal(t, vCBOR, decoded)
	})
	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		data, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.True(t, json.Valid(data))
		var decoded RepoApplyWrites_Input
		require.NoError(t, decoded.UnmarshalJSON(data))
		assert.Equal(t, *v, decoded)
	})
}

// --- SyncListRepos_Output (nil array edge case) ---

func TestSyncListRepos_Output_NilRepos(t *testing.T) {
	t.Parallel()

	t.Run("JSON_null_array", func(t *testing.T) {
		t.Parallel()
		raw := `{"repos":null}`
		var v SyncListRepos_Output
		require.NoError(t, v.UnmarshalJSON([]byte(raw)))
		assert.Nil(t, v.Repos)
	})

	t.Run("JSON_empty_array", func(t *testing.T) {
		t.Parallel()
		raw := `{"repos":[]}`
		var v SyncListRepos_Output
		require.NoError(t, v.UnmarshalJSON([]byte(raw)))
		assert.Empty(t, v.Repos)
	})
}
