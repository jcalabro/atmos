package streaming

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeFrame_CommitWithNullSince(t *testing.T) {
	t.Parallel()

	// Build a commit body where "since" is CBOR null (0xf6).
	// This previously caused "expected text (major 3), got major 7"
	// because optional string fields didn't handle null.
	body := cbor.AppendMapHeader(nil, 8)
	body = cbor.AppendTextKey(body, "ops")
	body = append(body, cbor.AppendArrayHeader(nil, 0)...)
	body = cbor.AppendTextKey(body, "rev")
	body = cbor.AppendText(body, "3abc")
	body = cbor.AppendTextKey(body, "seq")
	body = cbor.AppendInt(body, 42)
	body = cbor.AppendTextKey(body, "repo")
	body = cbor.AppendText(body, "did:plc:test")
	body = cbor.AppendTextKey(body, "time")
	body = cbor.AppendText(body, "2024-01-01T00:00:00Z")
	body = cbor.AppendTextKey(body, "blobs")
	body = append(body, cbor.AppendArrayHeader(nil, 0)...)
	body = cbor.AppendTextKey(body, "since")
	body = cbor.AppendNull(body)
	body = cbor.AppendTextKey(body, "blocks")
	body = cbor.AppendBytes(body, nil)

	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendTextKey(hdr, "t")
	hdr = cbor.AppendText(hdr, "#commit")
	frame := append(hdr, body...)

	evt, err := decodeFrame(frame)
	require.NoError(t, err, "decodeFrame should handle null since field")

	require.NotNil(t, evt.Commit)
	assert.Equal(t, "did:plc:test", evt.Commit.Repo)
	assert.False(t, evt.Commit.Since.HasVal(), "since should be None when null")
}

func TestDecodeFrame_CommitWithNullCID(t *testing.T) {
	t.Parallel()

	// Build a minimal commit body where one op has cid=null (a delete).
	// This previously caused "expected tag 42" because the generated
	// UnmarshalCBOR didn't handle CBOR null for optional CID link fields.

	// Build a RepoOp with cid=null: {action: "delete", cid: null, path: "app.bsky.graph.follow/abc"}
	op := cbor.AppendMapHeader(nil, 3)
	op = cbor.AppendTextKey(op, "cid")
	op = cbor.AppendNull(op)
	op = cbor.AppendTextKey(op, "path")
	op = cbor.AppendText(op, "app.bsky.graph.follow/abc")
	op = cbor.AppendTextKey(op, "action")
	op = cbor.AppendText(op, "delete")

	// Build commit body with minimal required fields.
	// DAG-CBOR key order: shorter first, then lexicographic.
	body := cbor.AppendMapHeader(nil, 8)
	body = cbor.AppendTextKey(body, "ops")
	body = append(body, cbor.AppendArrayHeader(nil, 1)...)
	body = append(body, op...)
	body = cbor.AppendTextKey(body, "rev")
	body = cbor.AppendText(body, "3abc")
	body = cbor.AppendTextKey(body, "seq")
	body = cbor.AppendInt(body, 42)
	body = cbor.AppendTextKey(body, "repo")
	body = cbor.AppendText(body, "did:plc:test")
	body = cbor.AppendTextKey(body, "time")
	body = cbor.AppendText(body, "2024-01-01T00:00:00Z")
	body = cbor.AppendTextKey(body, "blobs")
	body = append(body, cbor.AppendArrayHeader(nil, 0)...)
	body = cbor.AppendTextKey(body, "blocks")
	body = cbor.AppendBytes(body, nil)
	body = cbor.AppendTextKey(body, "$type")
	body = cbor.AppendText(body, "com.atproto.sync.subscribeRepos#commit")

	// Build the frame: header + body.
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendTextKey(hdr, "t")
	hdr = cbor.AppendText(hdr, "#commit")
	frame := append(hdr, body...)

	evt, err := decodeFrame(frame)
	require.NoError(t, err, "decodeFrame should handle null CID in delete ops")

	require.NotNil(t, evt.Commit)
	assert.Equal(t, "did:plc:test", evt.Commit.Repo)
	assert.Equal(t, int64(42), evt.Commit.Seq)
	require.Len(t, evt.Commit.Ops, 1)
	assert.Equal(t, "delete", evt.Commit.Ops[0].Action)
	assert.False(t, evt.Commit.Ops[0].CID.HasVal(), "delete op should have null CID")
}
