package streaming

import (
	"errors"
	"fmt"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// This file implements the "xrpc.v1.json" subprotocol framing (atproto
// proposal 0015): each WebSocket text frame contains exactly one
// self-describing JSON object discriminated by its "$type" field.
//
//	{"$type":"message","payload":{"$type":"<nsid>#<fragment>", ...}}
//	{"$type":"error","error":"FutureCursor","message":"..."}
//
// Unlike v0 there is no separate {op, t} header object: the payload is
// the lexicon message itself, with the full "<nsid>#<fragment>" in its
// "$type" field, so it decodes directly through the generated union
// unmarshalers.

// v1 envelope "$type" discriminators. These are not NSIDs; they behave
// like other compound data-model types such as "blob" or "bytes".
const (
	v1FrameTypeMessage = "message"
	v1FrameTypeError   = "error"
)

// v1JSONEnvelope is the parsed top-level v1 frame object.
type v1JSONEnvelope struct {
	typ     string // "$type": "message" or "error" (or unknown)
	payload []byte // raw payload JSON for message frames, nil if absent
	errCode string // "error" field for error frames
	errMsg  string // "message" field for error frames
}

// parseV1JSONEnvelope strictly parses the single top-level JSON object
// of a v1 frame. It rejects trailing data after the object, duplicate
// known keys, and a missing "$type". Unknown keys are skipped for
// forward compatibility, mirroring the generated types' handling of
// unrecognized fields.
func parseV1JSONEnvelope(data []byte) (v1JSONEnvelope, error) {
	var env v1JSONEnvelope
	var sawType, sawPayload, sawError, sawMessage bool

	pos, err := cbor.ReadJSONObjectStart(data, 0)
	if err != nil {
		return env, fmt.Errorf("decode v1 frame: %w", err)
	}

	for {
		if end, ok := cbor.ReadJSONObjectEnd(data, pos); ok {
			pos = end
			break
		}
		var key string
		key, pos, err = cbor.ReadJSONKey(data, pos)
		if err != nil {
			return env, fmt.Errorf("decode v1 frame: %w", err)
		}
		switch key {
		case "$type":
			if sawType {
				return env, errors.New("decode v1 frame: duplicate $type key")
			}
			sawType = true
			env.typ, pos, err = cbor.ReadJSONString(data, pos)
		case "payload":
			if sawPayload {
				return env, errors.New("decode v1 frame: duplicate payload key")
			}
			sawPayload = true
			start := cbor.SkipJSONWS(data, pos)
			pos, err = cbor.SkipJSONValue(data, pos)
			if err == nil {
				env.payload = data[start:pos]
			}
		case "error":
			if sawError {
				return env, errors.New("decode v1 frame: duplicate error key")
			}
			sawError = true
			env.errCode, pos, err = cbor.ReadJSONString(data, pos)
		case "message":
			if sawMessage {
				return env, errors.New("decode v1 frame: duplicate message key")
			}
			sawMessage = true
			env.errMsg, pos, err = cbor.ReadJSONString(data, pos)
		default:
			pos, err = cbor.SkipJSONValue(data, pos)
		}
		if err != nil {
			return env, fmt.Errorf("decode v1 frame: %w", err)
		}
		pos = cbor.SkipJSONComma(data, pos)
	}

	// The frame is exactly one object; trailing bytes mean a second
	// frame was smuggled into one WebSocket message. Same contract as
	// checkFrameBodyComplete on the v0 path.
	if rest := cbor.SkipJSONWS(data, pos); rest != len(data) {
		return env, fmt.Errorf("decode v1 frame: %d trailing bytes after object", len(data)-rest)
	}

	if !sawType {
		return env, errors.New("decode v1 frame: missing $type")
	}
	return env, nil
}

// v1EnvelopeToError maps a parsed non-message envelope to its terminal
// error. Shared by the repos and labels decoders.
//
//   - "error" frames become *StreamError, mirroring v0's op=-1 frames.
//     Per the spec the server closes the stream immediately after; the
//     reconnect loop handles that as a normal connection loss.
//   - Any other "$type" is a well-formed frame from a newer protocol
//     revision: *UnknownFrameError, and the client keeps reading.
func v1EnvelopeToError(env v1JSONEnvelope, data []byte) error {
	if env.typ == v1FrameTypeError {
		if env.errCode == "" {
			return errors.New("decode v1 frame: error frame missing error code")
		}
		return &StreamError{Code: env.errCode, Message: env.errMsg}
	}
	// Op 1 by convention: v1 has no op field, and 1 is the "typed
	// message" op on the v0 wire (same convention as the Jetstream
	// decoder).
	return &UnknownFrameError{T: env.typ, Op: 1, Seq: bestEffortSeqJSON(env.payload), Frame: data}
}

// decodeV1JSONFrame decodes an xrpc.v1.json subscribeRepos frame.
func decodeV1JSONFrame(data []byte) (Event, error) {
	env, err := parseV1JSONEnvelope(data)
	if err != nil {
		return Event{}, err
	}
	if env.typ != v1FrameTypeMessage {
		return Event{}, v1EnvelopeToError(env, data)
	}
	if env.payload == nil {
		return Event{}, errors.New("decode v1 frame: message frame missing payload")
	}

	var msg comatproto.SyncSubscribeRepos_Message
	if err := msg.UnmarshalJSON(env.payload); err != nil {
		return Event{}, fmt.Errorf("decode v1 payload: %w", err)
	}
	switch {
	case msg.SyncSubscribeRepos_Commit.HasVal():
		v := msg.SyncSubscribeRepos_Commit.Val()
		return Event{Seq: v.Seq, Commit: v}, nil
	case msg.SyncSubscribeRepos_Sync.HasVal():
		v := msg.SyncSubscribeRepos_Sync.Val()
		return Event{Seq: v.Seq, Sync: v}, nil
	case msg.SyncSubscribeRepos_Identity.HasVal():
		v := msg.SyncSubscribeRepos_Identity.Val()
		return Event{Seq: v.Seq, Identity: v}, nil
	case msg.SyncSubscribeRepos_Account.HasVal():
		v := msg.SyncSubscribeRepos_Account.Val()
		return Event{Seq: v.Seq, Account: v}, nil
	case msg.SyncSubscribeRepos_Info.HasVal():
		return Event{Info: msg.SyncSubscribeRepos_Info.Val()}, nil
	case msg.Unknown.HasVal():
		u := msg.Unknown.Val()
		if u.Type == "" {
			// A payload with no $type is not a lexicon message at all —
			// malformed, not forward-compat.
			return Event{}, errors.New("decode v1 payload: missing $type")
		}
		return Event{}, &UnknownFrameError{T: u.Type, Op: 1, Seq: bestEffortSeqJSON(env.payload), Frame: data}
	default:
		return Event{}, errors.New("decode v1 payload: empty message union")
	}
}

// decodeV1JSONLabelFrame decodes an xrpc.v1.json subscribeLabels frame.
func decodeV1JSONLabelFrame(data []byte) (Event, error) {
	env, err := parseV1JSONEnvelope(data)
	if err != nil {
		return Event{}, err
	}
	if env.typ != v1FrameTypeMessage {
		return Event{}, v1EnvelopeToError(env, data)
	}
	if env.payload == nil {
		return Event{}, errors.New("decode v1 frame: message frame missing payload")
	}

	var msg comatproto.LabelSubscribeLabels_Message
	if err := msg.UnmarshalJSON(env.payload); err != nil {
		return Event{}, fmt.Errorf("decode v1 payload: %w", err)
	}
	switch {
	case msg.LabelSubscribeLabels_Labels.HasVal():
		v := msg.LabelSubscribeLabels_Labels.Val()
		return Event{Seq: v.Seq, labelBatch: v}, nil
	case msg.LabelSubscribeLabels_Info.HasVal():
		return Event{LabelInfo: msg.LabelSubscribeLabels_Info.Val()}, nil
	case msg.Unknown.HasVal():
		u := msg.Unknown.Val()
		if u.Type == "" {
			// A payload with no $type is not a lexicon message at all —
			// malformed, not forward-compat.
			return Event{}, errors.New("decode v1 payload: missing $type")
		}
		return Event{}, &UnknownFrameError{T: u.Type, Op: 1, Seq: bestEffortSeqJSON(env.payload), Frame: data}
	default:
		return Event{}, errors.New("decode v1 payload: empty message union")
	}
}

// bestEffortSeqJSON extracts an int64 "seq" field from a JSON-object
// payload, returning 0 when the payload is not an object or carries no
// readable seq. The JSON counterpart of bestEffortSeq: it attributes a
// seq to unknown frame types so gap detection can account for them.
func bestEffortSeqJSON(payload []byte) int64 {
	if payload == nil {
		return 0
	}
	pos, err := cbor.ReadJSONObjectStart(payload, 0)
	if err != nil {
		return 0
	}
	for {
		if _, ok := cbor.ReadJSONObjectEnd(payload, pos); ok {
			return 0
		}
		var key string
		key, pos, err = cbor.ReadJSONKey(payload, pos)
		if err != nil {
			return 0
		}
		if key == "seq" {
			v, _, err := cbor.ReadJSONInt(payload, pos)
			if err != nil {
				return 0
			}
			return v
		}
		if pos, err = cbor.SkipJSONValue(payload, pos); err != nil {
			return 0
		}
		pos = cbor.SkipJSONComma(payload, pos)
	}
}
