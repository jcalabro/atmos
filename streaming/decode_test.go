package streaming

import (
	"errors"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
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

// buildDecodeFrame constructs a frame header + body without the build tag
// constraint that client_test.go's buildFrame has.
func buildDecodeFrame(t string, body []byte) []byte {
	hdr := make([]byte, 0, 32)
	hdr = cbor.AppendMapHeader(hdr, 2)
	hdr = cbor.AppendText(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendText(hdr, "t")
	hdr = cbor.AppendText(hdr, t)
	return append(hdr, body...)
}

func TestDecodeFrame_Sync(t *testing.T) {
	t.Parallel()
	body, err := (&comatproto.SyncSubscribeRepos_Sync{
		DID:  "did:plc:test123",
		Seq:  42,
		Rev:  "3abc",
		Time: "2024-01-01T00:00:00Z",
	}).MarshalCBOR()
	require.NoError(t, err)

	frame := buildDecodeFrame("#sync", body)
	evt, err := decodeFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Sync)
	assert.Equal(t, int64(42), evt.Seq)
	assert.Equal(t, "did:plc:test123", evt.Sync.DID)
}

func TestDecodeFrame_Identity(t *testing.T) {
	t.Parallel()
	body, err := (&comatproto.SyncSubscribeRepos_Identity{
		DID:  "did:plc:test123",
		Seq:  99,
		Time: "2024-01-01T00:00:00Z",
	}).MarshalCBOR()
	require.NoError(t, err)

	frame := buildDecodeFrame("#identity", body)
	evt, err := decodeFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Identity)
	assert.Equal(t, int64(99), evt.Seq)
}

func TestDecodeFrame_Account(t *testing.T) {
	t.Parallel()
	body, err := (&comatproto.SyncSubscribeRepos_Account{
		DID:    "did:plc:test123",
		Seq:    77,
		Active: true,
		Time:   "2024-01-01T00:00:00Z",
	}).MarshalCBOR()
	require.NoError(t, err)

	frame := buildDecodeFrame("#account", body)
	evt, err := decodeFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Account)
	assert.Equal(t, int64(77), evt.Seq)
}

func TestDecodeFrame_Info(t *testing.T) {
	t.Parallel()
	body, err := (&comatproto.SyncSubscribeRepos_Info{
		Name: "OutdatedCursor",
	}).MarshalCBOR()
	require.NoError(t, err)

	frame := buildDecodeFrame("#info", body)
	evt, err := decodeFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Info)
	assert.Equal(t, "OutdatedCursor", evt.Info.Name)
}

// TestDecodeFrame_TrailingData asserts a frame with bytes after the body is
// rejected, so a second frame cannot be smuggled into one message and dropped.
func TestDecodeFrame_TrailingData(t *testing.T) {
	t.Parallel()
	body, err := (&comatproto.SyncSubscribeRepos_Info{Name: "OutdatedCursor"}).MarshalCBOR()
	require.NoError(t, err)

	frame := buildDecodeFrame("#info", body)
	frame = append(frame, 0xa0) // an extra empty-map CBOR value after the body

	_, err = decodeFrame(frame)
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing")
}

func TestDecodeFrame_ErrorFrame(t *testing.T) {
	t.Parallel()
	// op = -1 error frame with the generic {error, message} body. It
	// must decode into a *StreamError carrying the machine-readable
	// code — not be misdecoded as a #info body (which would drop the
	// code by reading the wrong key).
	body := cbor.AppendMapHeader(nil, 2)
	body = cbor.AppendTextKey(body, "error")
	body = cbor.AppendText(body, "FutureCursor")
	body = cbor.AppendTextKey(body, "message")
	body = cbor.AppendText(body, "cursor in the future")

	hdr := cbor.AppendMapHeader(nil, 1)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, -1)
	frame := append(hdr, body...)

	evt, err := decodeFrame(frame)
	se, ok := errors.AsType[*StreamError](err)
	require.True(t, ok, "op=-1 frame must decode to *StreamError, got %v", err)
	assert.Equal(t, "FutureCursor", se.Code)
	assert.Equal(t, "cursor in the future", se.Message)
	assert.Nil(t, evt.Info)
}

func TestDecodeFrame_UnknownType(t *testing.T) {
	t.Parallel()
	// A frame with op=1 and an unrecognized type must surface as a
	// *UnknownFrameError with the header type, a best-effort seq read
	// from the body, and the raw frame attached.
	hdr := make([]byte, 0, 32)
	hdr = cbor.AppendMapHeader(hdr, 2)
	hdr = cbor.AppendText(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendText(hdr, "t")
	hdr = cbor.AppendText(hdr, "#unknown")

	body := make([]byte, 0, 16)
	body = cbor.AppendMapHeader(body, 1)
	body = cbor.AppendTextKey(body, "seq")
	body = cbor.AppendInt(body, 77)

	frame := append(hdr, body...)
	_, err := decodeFrame(frame)
	ue, ok := errors.AsType[*UnknownFrameError](err)
	require.True(t, ok, "unknown type must decode to *UnknownFrameError, got %v", err)
	assert.Equal(t, "#unknown", ue.T)
	assert.Equal(t, int64(1), ue.Op)
	assert.Equal(t, int64(77), ue.Seq)
	assert.Equal(t, frame, ue.Frame)
	assert.Equal(t, frame, ErrorRawFrame(ue))
}

func TestDecodeFrame_UnknownTypeNoSeq(t *testing.T) {
	t.Parallel()
	// An unknown type whose body carries no readable seq reports Seq=0
	// (the caller then lets the next recognized frame's GapError cover
	// the hole).
	hdr := make([]byte, 0, 32)
	hdr = cbor.AppendMapHeader(hdr, 2)
	hdr = cbor.AppendText(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendText(hdr, "t")
	hdr = cbor.AppendText(hdr, "#unknown")

	body := make([]byte, 0, 16)
	body = cbor.AppendMapHeader(body, 0)

	frame := append(hdr, body...)
	_, err := decodeFrame(frame)
	ue, ok := errors.AsType[*UnknownFrameError](err)
	require.True(t, ok, "unknown type must decode to *UnknownFrameError, got %v", err)
	assert.Zero(t, ue.Seq)
}

func TestDecodeFrame_UnknownOp(t *testing.T) {
	t.Parallel()
	hdr := make([]byte, 0, 32)
	hdr = cbor.AppendMapHeader(hdr, 2)
	hdr = cbor.AppendText(hdr, "op")
	hdr = cbor.AppendInt(hdr, 2) // op=2 is unknown
	hdr = cbor.AppendText(hdr, "t")
	hdr = cbor.AppendText(hdr, "#commit")

	body := make([]byte, 0, 16)
	body = cbor.AppendMapHeader(body, 0)

	frame := append(hdr, body...)
	_, err := decodeFrame(frame)
	ue, ok := errors.AsType[*UnknownFrameError](err)
	require.True(t, ok, "unknown op must decode to *UnknownFrameError, got %v", err)
	assert.Equal(t, int64(2), ue.Op)
}

func TestBestEffortSeq(t *testing.T) {
	t.Parallel()

	withSeq := cbor.AppendMapHeader(nil, 2)
	withSeq = cbor.AppendTextKey(withSeq, "did")
	withSeq = cbor.AppendText(withSeq, "did:plc:abc")
	withSeq = cbor.AppendTextKey(withSeq, "seq")
	withSeq = cbor.AppendInt(withSeq, 123)
	assert.Equal(t, int64(123), bestEffortSeq(withSeq))

	noSeq := cbor.AppendMapHeader(nil, 1)
	noSeq = cbor.AppendTextKey(noSeq, "did")
	noSeq = cbor.AppendText(noSeq, "did:plc:abc")
	assert.Zero(t, bestEffortSeq(noSeq))

	// seq of the wrong CBOR type: 0, not a panic or garbage.
	badSeq := cbor.AppendMapHeader(nil, 1)
	badSeq = cbor.AppendTextKey(badSeq, "seq")
	badSeq = cbor.AppendText(badSeq, "not-an-int")
	assert.Zero(t, bestEffortSeq(badSeq))

	// Not a map at all.
	assert.Zero(t, bestEffortSeq(cbor.AppendInt(nil, 7)))
	assert.Zero(t, bestEffortSeq(nil))
	assert.Zero(t, bestEffortSeq([]byte{0xff}))
}
