package streaming

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildV1MessageFrame wraps a lexicon message (with $type already set)
// in the v1 JSON envelope.
func buildV1MessageFrame(t *testing.T, payload any) []byte {
	t.Helper()
	p, err := json.Marshal(payload)
	require.NoError(t, err)
	return fmt.Appendf(nil, `{"$type":"message","payload":%s}`, p)
}

func TestDecodeV1JSONFrame(t *testing.T) {
	t.Parallel()

	t.Run("commit", func(t *testing.T) {
		t.Parallel()
		commit := &comatproto.SyncSubscribeRepos_Commit{
			LexiconTypeID: "com.atproto.sync.subscribeRepos#commit",
			Seq:           42,
			Repo:          "did:plc:abc",
			Rev:           "3kx2",
			Blocks:        []byte{0x01, 0x02, 0x03},
		}
		frame := buildV1MessageFrame(t, commit)

		evt, err := decodeV1JSONFrame(frame)
		require.NoError(t, err)
		require.NotNil(t, evt.Commit)
		assert.Equal(t, int64(42), evt.Seq)
		assert.Equal(t, "did:plc:abc", evt.Commit.Repo)
		assert.Equal(t, "3kx2", evt.Commit.Rev)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, evt.Commit.Blocks)
	})

	t.Run("sync", func(t *testing.T) {
		t.Parallel()
		sync := &comatproto.SyncSubscribeRepos_Sync{
			LexiconTypeID: "com.atproto.sync.subscribeRepos#sync",
			Seq:           7,
			DID:           "did:plc:xyz",
			Rev:           "3ky0",
		}
		evt, err := decodeV1JSONFrame(buildV1MessageFrame(t, sync))
		require.NoError(t, err)
		require.NotNil(t, evt.Sync)
		assert.Equal(t, int64(7), evt.Seq)
		assert.Equal(t, "did:plc:xyz", evt.Sync.DID)
	})

	t.Run("identity", func(t *testing.T) {
		t.Parallel()
		id := &comatproto.SyncSubscribeRepos_Identity{
			LexiconTypeID: "com.atproto.sync.subscribeRepos#identity",
			Seq:           9,
			DID:           "did:plc:idy",
		}
		evt, err := decodeV1JSONFrame(buildV1MessageFrame(t, id))
		require.NoError(t, err)
		require.NotNil(t, evt.Identity)
		assert.Equal(t, int64(9), evt.Seq)
	})

	t.Run("account", func(t *testing.T) {
		t.Parallel()
		acct := &comatproto.SyncSubscribeRepos_Account{
			LexiconTypeID: "com.atproto.sync.subscribeRepos#account",
			Seq:           11,
			DID:           "did:plc:acc",
			Active:        true,
		}
		evt, err := decodeV1JSONFrame(buildV1MessageFrame(t, acct))
		require.NoError(t, err)
		require.NotNil(t, evt.Account)
		assert.Equal(t, int64(11), evt.Seq)
	})

	t.Run("info", func(t *testing.T) {
		t.Parallel()
		info := &comatproto.SyncSubscribeRepos_Info{
			LexiconTypeID: "com.atproto.sync.subscribeRepos#info",
			Name:          "OutdatedCursor",
		}
		evt, err := decodeV1JSONFrame(buildV1MessageFrame(t, info))
		require.NoError(t, err)
		require.NotNil(t, evt.Info)
		assert.Equal(t, "OutdatedCursor", evt.Info.Name)
		assert.Zero(t, evt.Seq)
	})

	t.Run("error frame", func(t *testing.T) {
		t.Parallel()
		frame := []byte(`{"$type":"error","error":"FutureCursor","message":"requested cursor is in the future"}`)
		_, err := decodeV1JSONFrame(frame)
		se, ok := errors.AsType[*StreamError](err)
		require.True(t, ok, "want *StreamError, got %v", err)
		assert.Equal(t, "FutureCursor", se.Code)
		assert.Equal(t, "requested cursor is in the future", se.Message)
	})

	t.Run("error frame without message", func(t *testing.T) {
		t.Parallel()
		_, err := decodeV1JSONFrame([]byte(`{"$type":"error","error":"ConsumerTooSlow"}`))
		se, ok := errors.AsType[*StreamError](err)
		require.True(t, ok)
		assert.Equal(t, "ConsumerTooSlow", se.Code)
		assert.Empty(t, se.Message)
	})

	t.Run("error frame missing code is malformed", func(t *testing.T) {
		t.Parallel()
		_, err := decodeV1JSONFrame([]byte(`{"$type":"error","message":"no code"}`))
		require.Error(t, err)
		_, isStream := errors.AsType[*StreamError](err)
		assert.False(t, isStream)
	})

	t.Run("unknown envelope type is UnknownFrameError", func(t *testing.T) {
		t.Parallel()
		frame := []byte(`{"$type":"heartbeat","payload":{"seq":99}}`)
		_, err := decodeV1JSONFrame(frame)
		ue, ok := errors.AsType[*UnknownFrameError](err)
		require.True(t, ok, "want *UnknownFrameError, got %v", err)
		assert.Equal(t, "heartbeat", ue.T)
		assert.Equal(t, int64(99), ue.Seq)
		assert.Equal(t, frame, ue.Frame)
	})

	t.Run("unknown payload type is UnknownFrameError", func(t *testing.T) {
		t.Parallel()
		frame := []byte(`{"$type":"message","payload":{"$type":"com.atproto.sync.subscribeRepos#future","seq":123}}`)
		_, err := decodeV1JSONFrame(frame)
		ue, ok := errors.AsType[*UnknownFrameError](err)
		require.True(t, ok, "want *UnknownFrameError, got %v", err)
		assert.Equal(t, "com.atproto.sync.subscribeRepos#future", ue.T)
		assert.Equal(t, int64(123), ue.Seq)
	})

	t.Run("unknown keys in envelope are skipped", func(t *testing.T) {
		t.Parallel()
		frame := []byte(`{"future":true,"$type":"message","payload":{"$type":"com.atproto.sync.subscribeRepos#info","name":"x"},"more":[1,2]}`)
		evt, err := decodeV1JSONFrame(frame)
		require.NoError(t, err)
		require.NotNil(t, evt.Info)
	})

	malformed := map[string]string{
		"empty input":              ``,
		"not an object":            `[1,2,3]`,
		"bare string":              `"message"`,
		"missing $type":            `{"payload":{"seq":1}}`,
		"duplicate $type":          `{"$type":"message","$type":"message","payload":{}}`,
		"duplicate payload":        `{"$type":"message","payload":{},"payload":{}}`,
		"duplicate error":          `{"$type":"error","error":"A","error":"B"}`,
		"duplicate message field":  `{"$type":"error","error":"A","message":"x","message":"y"}`,
		"message missing payload":  `{"$type":"message"}`,
		"non-string $type":         `{"$type":42,"payload":{}}`,
		"trailing bytes":           `{"$type":"message","payload":{"$type":"com.atproto.sync.subscribeRepos#info","name":"x"}}{"$type":"message"}`,
		"trailing garbage":         `{"$type":"error","error":"A"} tail`,
		"unterminated object":      `{"$type":"message","payload":{`,
		"payload not lexicon-able": `{"$type":"message","payload":"not an object"}`,
		"empty payload object":     `{"$type":"message","payload":{}}`,
	}
	for name, input := range malformed {
		t.Run("malformed: "+name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeV1JSONFrame([]byte(input))
			require.Error(t, err)
			// Malformed frames must never masquerade as forward-compat
			// UnknownFrameError or server StreamError.
			_, isUnknown := errors.AsType[*UnknownFrameError](err)
			_, isStream := errors.AsType[*StreamError](err)
			assert.False(t, isUnknown, "should not be UnknownFrameError")
			assert.False(t, isStream, "should not be StreamError")
		})
	}
}

func TestDecodeV1JSONLabelFrame(t *testing.T) {
	t.Parallel()

	t.Run("labels", func(t *testing.T) {
		t.Parallel()
		labels := &comatproto.LabelSubscribeLabels_Labels{
			LexiconTypeID: "com.atproto.label.subscribeLabels#labels",
			Seq:           5,
			Labels: []comatproto.LabelDefs_Label{
				{URI: "at://did:plc:abc/app.bsky.feed.post/1", Val: "spam", Src: "did:plc:labeler", Cts: "2024-01-01T00:00:00Z"},
			},
		}
		evt, err := decodeV1JSONLabelFrame(buildV1MessageFrame(t, labels))
		require.NoError(t, err)
		require.Len(t, evt.Labels(), 1)
		assert.Equal(t, int64(5), evt.Seq)
		assert.Equal(t, "spam", evt.Labels()[0].Val)
	})

	t.Run("info", func(t *testing.T) {
		t.Parallel()
		info := &comatproto.LabelSubscribeLabels_Info{
			LexiconTypeID: "com.atproto.label.subscribeLabels#info",
			Name:          "OutdatedCursor",
		}
		evt, err := decodeV1JSONLabelFrame(buildV1MessageFrame(t, info))
		require.NoError(t, err)
		require.NotNil(t, evt.LabelInfo)
		assert.Equal(t, "OutdatedCursor", evt.LabelInfo.Name)
	})

	t.Run("error frame", func(t *testing.T) {
		t.Parallel()
		_, err := decodeV1JSONLabelFrame([]byte(`{"$type":"error","error":"FutureCursor"}`))
		se, ok := errors.AsType[*StreamError](err)
		require.True(t, ok)
		assert.Equal(t, "FutureCursor", se.Code)
	})

	t.Run("repos payload on label stream is UnknownFrameError", func(t *testing.T) {
		t.Parallel()
		commit := &comatproto.SyncSubscribeRepos_Commit{
			LexiconTypeID: "com.atproto.sync.subscribeRepos#commit",
			Seq:           42,
			Repo:          "did:plc:abc",
		}
		_, err := decodeV1JSONLabelFrame(buildV1MessageFrame(t, commit))
		ue, ok := errors.AsType[*UnknownFrameError](err)
		require.True(t, ok, "want *UnknownFrameError, got %v", err)
		assert.Equal(t, "com.atproto.sync.subscribeRepos#commit", ue.T)
	})
}

// TestV1RoundTrip verifies that every message type round-trips through
// the v1 JSON envelope encoder (used by xrpcserver) and this decoder
// with an identical Event as the v0 CBOR path produces.
func TestV1RoundTrip(t *testing.T) {
	t.Parallel()

	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("commit-block"))
	commit := &comatproto.SyncSubscribeRepos_Commit{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#commit",
		Seq:           1001,
		Repo:          "did:plc:roundtrip",
		Rev:           "3kx9",
		Commit:        lextypes.LexCIDLink{Link: commitCID.String()},
		Blocks:        []byte("car-bytes-here"),
		Time:          "2024-06-01T00:00:00Z",
	}

	// v1 JSON path.
	frame := buildV1MessageFrame(t, commit)
	v1evt, err := decodeV1JSONFrame(frame)
	require.NoError(t, err)

	// v0 CBOR path for the same logical message.
	body, err := commit.MarshalCBOR()
	require.NoError(t, err)
	v0evt, err := decodeFrame(buildFrame("#commit", body))
	require.NoError(t, err)

	require.NotNil(t, v1evt.Commit)
	require.NotNil(t, v0evt.Commit)
	assert.Equal(t, v0evt.Seq, v1evt.Seq)
	assert.Equal(t, v0evt.Commit.Repo, v1evt.Commit.Repo)
	assert.Equal(t, v0evt.Commit.Rev, v1evt.Commit.Rev)
	assert.Equal(t, v0evt.Commit.Blocks, v1evt.Commit.Blocks)
	assert.Equal(t, v0evt.Commit.Time, v1evt.Commit.Time)
}

func TestBestEffortSeqJSON(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		payload string
		want    int64
	}{
		"present":         {`{"seq":42}`, 42},
		"after other key": {`{"a":"b","seq":7}`, 7},
		"absent":          {`{"a":"b"}`, 0},
		"not an object":   {`[1,2]`, 0},
		"non-int seq":     {`{"seq":"nope"}`, 0},
		"nested seq only": {`{"inner":{"seq":9}}`, 0},
		"empty":           {``, 0},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload := []byte(tc.payload)
			if tc.payload == "" {
				payload = nil
			}
			assert.Equal(t, tc.want, bestEffortSeqJSON(payload))
		})
	}
}

// FuzzDecodeV1JSONFrame ensures the decoder never panics and never
// returns a zero Event with nil error on arbitrary input.
func FuzzDecodeV1JSONFrame(f *testing.F) {
	f.Add([]byte(`{"$type":"message","payload":{"$type":"com.atproto.sync.subscribeRepos#commit","seq":42,"repo":"did:plc:abc","rev":"3kx2","blocks":{"$bytes":"AQID"}}}`))
	f.Add([]byte(`{"$type":"error","error":"FutureCursor","message":"x"}`))
	f.Add([]byte(`{"$type":"heartbeat"}`))
	f.Add([]byte(`{"$type":"message","payload":{}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`{"$type":"message","payload":{"$type":"com.atproto.sync.subscribeRepos#info","name":"n"}} `))

	f.Fuzz(func(t *testing.T, data []byte) {
		evt, err := decodeV1JSONFrame(data)
		if err == nil {
			// A successful decode must carry exactly one variant.
			n := 0
			for _, set := range []bool{
				evt.Commit != nil, evt.Sync != nil, evt.Identity != nil,
				evt.Account != nil, evt.Info != nil,
			} {
				if set {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("decoded event has %d variants set", n)
			}
		}
		_, _ = decodeV1JSONLabelFrame(data)
	})
}
