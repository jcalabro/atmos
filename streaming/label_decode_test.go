package streaming

import (
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildLabelFrame(t string, body []byte) []byte {
	hdr := make([]byte, 0, 32)
	hdr = cbor.AppendMapHeader(hdr, 2)
	hdr = cbor.AppendText(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendText(hdr, "t")
	hdr = cbor.AppendText(hdr, t)
	return append(hdr, body...)
}

func buildLabelErrorFrame(body []byte) []byte {
	hdr := cbor.AppendMapHeader(nil, 1)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, -1)
	return append(hdr, body...)
}

func mustMarshalLabelsBody(labels []comatproto.LabelDefs_Label, seq int64) []byte {
	v := &comatproto.LabelSubscribeLabels_Labels{
		Seq:    seq,
		Labels: labels,
	}
	data, err := v.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func mustMarshalLabelInfoBody(name string, message gt.Option[string]) []byte {
	v := &comatproto.LabelSubscribeLabels_Info{
		Name:    name,
		Message: message,
	}
	data, err := v.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func TestDecodeLabelFrame_Labels(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:alice/app.bsky.feed.post/abc",
			Val: "spam",
			Cts: "2024-01-01T00:00:00Z",
		},
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:bob/app.bsky.feed.post/xyz",
			Val: "nudity",
			Cts: "2024-01-01T00:00:01Z",
		},
	}, 42)

	frame := buildLabelFrame("#labels", body)
	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	assert.Equal(t, int64(42), evt.Seq)
	assert.Nil(t, evt.LabelInfo)
	assert.Nil(t, evt.Commit)

	labels := evt.Labels()
	require.Len(t, labels, 2)
	assert.Equal(t, "spam", labels[0].Val)
	assert.Equal(t, "at://did:plc:alice/app.bsky.feed.post/abc", labels[0].URI)
	assert.Equal(t, "did:plc:labeler", labels[0].Src)
	assert.Equal(t, "nudity", labels[1].Val)
	assert.Equal(t, "at://did:plc:bob/app.bsky.feed.post/xyz", labels[1].URI)
}

func TestDecodeLabelFrame_EmptyBatch(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelsBody(nil, 5)
	frame := buildLabelFrame("#labels", body)

	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)
	assert.Equal(t, int64(5), evt.Seq)
	// Labels() returns the empty slice from the batch, not nil.
	assert.NotNil(t, evt.Labels())
	assert.Empty(t, evt.Labels())
}

func TestDecodeLabelFrame_NegationLabels(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:alice/app.bsky.feed.post/abc",
			Val: "spam",
			Neg: gt.Some(true),
			Cts: "2024-01-01T00:00:05Z",
		},
	}, 99)

	frame := buildLabelFrame("#labels", body)
	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	labels := evt.Labels()
	require.Len(t, labels, 1)
	assert.Equal(t, "spam", labels[0].Val)
	assert.Equal(t, "at://did:plc:alice/app.bsky.feed.post/abc", labels[0].URI)
	assert.True(t, labels[0].Neg.ValOr(false), "negation label should have Neg=true")
}

func TestDecodeLabelFrame_MixedApplyAndNegate(t *testing.T) {
	t.Parallel()

	// A batch containing both a new label application and a negation,
	// mirroring the pattern from the reference implementation where both
	// can appear in the same stream.
	body := mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:alice/app.bsky.feed.post/abc",
			Val: "porn",
			Cts: "2024-01-01T00:00:00Z",
		},
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:bob/app.bsky.feed.post/xyz",
			Val: "spam",
			Neg: gt.Some(true),
			Cts: "2024-01-01T00:00:01Z",
		},
	}, 50)

	frame := buildLabelFrame("#labels", body)
	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	labels := evt.Labels()
	require.Len(t, labels, 2)

	// First: apply label
	assert.Equal(t, "porn", labels[0].Val)
	assert.False(t, labels[0].Neg.ValOr(false), "application label should not be negated")

	// Second: negate label
	assert.Equal(t, "spam", labels[1].Val)
	assert.True(t, labels[1].Neg.ValOr(false), "negation label should have Neg=true")
}

func TestDecodeLabelFrame_LabelWithExpiration(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:alice/app.bsky.feed.post/abc",
			Val: "warn",
			Cts: "2024-01-01T00:00:00Z",
			Exp: gt.Some("2024-02-01T00:00:00Z"),
			Ver: gt.Some(int64(1)),
		},
	}, 10)

	frame := buildLabelFrame("#labels", body)
	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	labels := evt.Labels()
	require.Len(t, labels, 1)
	assert.Equal(t, "warn", labels[0].Val)
	assert.Equal(t, "2024-02-01T00:00:00Z", labels[0].Exp.Val())
	assert.Equal(t, int64(1), labels[0].Ver.Val())
}

func TestDecodeLabelFrame_Info(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelInfoBody("OutdatedCursor", gt.Some("cursor is too old"))
	frame := buildLabelFrame("#info", body)

	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	assert.Nil(t, evt.Labels())
	require.NotNil(t, evt.LabelInfo)
	assert.Equal(t, "OutdatedCursor", evt.LabelInfo.Name)
	assert.Equal(t, "cursor is too old", evt.LabelInfo.Message.Val())
}

func TestDecodeLabelFrame_ErrorFrame(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelInfoBody("FutureCursor", gt.Some("cursor in the future"))
	frame := buildLabelErrorFrame(body)

	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	require.NotNil(t, evt.LabelInfo)
	assert.Equal(t, "FutureCursor", evt.LabelInfo.Name)
	assert.Equal(t, "cursor in the future", evt.LabelInfo.Message.Val())
}

func TestDecodeLabelFrame_UnknownOp(t *testing.T) {
	t.Parallel()

	hdr := make([]byte, 0, 32)
	hdr = cbor.AppendMapHeader(hdr, 2)
	hdr = cbor.AppendText(hdr, "op")
	hdr = cbor.AppendInt(hdr, 2) // op=2 is unknown
	hdr = cbor.AppendText(hdr, "t")
	hdr = cbor.AppendText(hdr, "#labels")

	body := cbor.AppendMapHeader(nil, 0)
	frame := append(hdr, body...)

	_, err := decodeLabelFrame(frame)
	require.ErrorIs(t, err, errUnknownOp)
}

func TestDecodeLabelFrame_UnknownType(t *testing.T) {
	t.Parallel()

	body := cbor.AppendMapHeader(nil, 0)
	frame := buildLabelFrame("#unknown", body)

	_, err := decodeLabelFrame(frame)
	require.ErrorIs(t, err, errUnknownType)
}

func TestDecodeLabelFrame_BadBody(t *testing.T) {
	t.Parallel()

	// Valid header but garbage body — should return a decode error, not panic.
	frame := buildLabelFrame("#labels", []byte{0xff, 0xfe})
	_, err := decodeLabelFrame(frame)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode labels")
}

func TestDecodeLabelFrame_BadInfoBody(t *testing.T) {
	t.Parallel()

	frame := buildLabelFrame("#info", []byte{0xff, 0xfe})
	_, err := decodeLabelFrame(frame)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode label info")
}

func TestDecodeLabelFrame_BadErrorBody(t *testing.T) {
	t.Parallel()

	frame := buildLabelErrorFrame([]byte{0xff, 0xfe})
	_, err := decodeLabelFrame(frame)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode error frame")
}

func TestLabels_NilForZeroEvent(t *testing.T) {
	t.Parallel()
	var evt Event
	assert.Nil(t, evt.Labels())
}

func TestLabels_NilForRepoEvents(t *testing.T) {
	t.Parallel()

	// A repo event should return nil from Labels().
	body, err := (&comatproto.SyncSubscribeRepos_Identity{
		DID:  "did:plc:test123",
		Seq:  42,
		Time: "2024-01-01T00:00:00Z",
	}).MarshalCBOR()
	require.NoError(t, err)

	frame := buildDecodeFrame("#identity", body)
	evt, err := decodeFrame(frame)
	require.NoError(t, err)
	assert.Nil(t, evt.Labels(), "Labels() should return nil for repo events")
}

func TestOperations_NilForLabelEvents(t *testing.T) {
	t.Parallel()

	body := mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
		{
			Src: "did:plc:labeler",
			URI: "at://did:plc:alice/app.bsky.feed.post/abc",
			Val: "spam",
			Cts: "2024-01-01T00:00:00Z",
		},
	}, 1)

	frame := buildLabelFrame("#labels", body)
	evt, err := decodeLabelFrame(frame)
	require.NoError(t, err)

	// Operations() should yield nothing for label events.
	var ops int
	for range evt.Operations() {
		ops++
	}
	assert.Equal(t, 0, ops)
}

func FuzzDecodeLabelFrame(f *testing.F) {
	// Valid #labels frame.
	body := mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
		{Src: "did:plc:x", URI: "at://did:plc:y/z", Val: "spam", Cts: "2024-01-01T00:00:00Z"},
	}, 1)
	f.Add(buildLabelFrame("#labels", body))

	// Valid #info frame.
	infoBody := mustMarshalLabelInfoBody("OutdatedCursor", gt.None[string]())
	f.Add(buildLabelFrame("#info", infoBody))

	// Error frame.
	f.Add(buildLabelErrorFrame(infoBody))

	// Unknown op frame (op=2).
	unknownOp := make([]byte, 0, 32)
	unknownOp = cbor.AppendMapHeader(unknownOp, 2)
	unknownOp = cbor.AppendText(unknownOp, "op")
	unknownOp = cbor.AppendInt(unknownOp, 2)
	unknownOp = cbor.AppendText(unknownOp, "t")
	unknownOp = cbor.AppendText(unknownOp, "#labels")
	unknownOp = append(unknownOp, cbor.AppendMapHeader(nil, 0)...)
	f.Add(unknownOp)

	// Empty / garbage.
	f.Add([]byte{})
	f.Add([]byte{0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic.
		_, _ = decodeLabelFrame(data)
	})
}
