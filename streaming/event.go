package streaming

import (
	"errors"
	"fmt"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// errUnknownType is returned for unrecognized event types, which are
// silently skipped for forward compatibility.
var errUnknownType = errors.New("unknown event type")

// Event is a single firehose event with its sequence number.
type Event struct {
	Seq      int64
	Commit   *comatproto.SyncSubscribeRepos_Commit
	Sync     *comatproto.SyncSubscribeRepos_Sync
	Identity *comatproto.SyncSubscribeRepos_Identity
	Account  *comatproto.SyncSubscribeRepos_Account
	Info     *comatproto.SyncSubscribeRepos_Info
}

// frameHeader is the CBOR header that precedes each event body on the wire.
// op=1 means a regular message, t is the type suffix (e.g. "#commit").
type frameHeader struct {
	Op int64  // 1 = message, -1 = error
	T  string // type, e.g. "#commit"
}

// decodeFrame decodes an ATProto event stream frame (two concatenated CBOR values:
// header map + body).
func decodeFrame(data []byte) (Event, error) {
	hdr, bodyStart, err := decodeFrameHeader(data)
	if err != nil {
		return Event{}, fmt.Errorf("decode frame header: %w", err)
	}

	body := data[bodyStart:]

	if hdr.Op == -1 {
		// Error frame — decode as Info.
		var info comatproto.SyncSubscribeRepos_Info
		if err := info.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode error frame: %w", err)
		}
		return Event{Info: &info}, nil
	}

	if hdr.Op != 1 {
		return Event{}, fmt.Errorf("unknown frame op: %d", hdr.Op)
	}

	return decodeMessageBody(hdr.T, body)
}

// decodeFrameHeader reads the CBOR map header {op: int, t: string}.
func decodeFrameHeader(data []byte) (frameHeader, int, error) {
	count, pos, err := cbor.ReadMapHeader(data, 0)
	if err != nil {
		return frameHeader{}, 0, err
	}

	var hdr frameHeader
	for i := uint64(0); i < count; i++ {
		key, newPos, err := cbor.ReadText(data, pos)
		if err != nil {
			return frameHeader{}, 0, err
		}
		pos = newPos
		switch key {
		case "op":
			hdr.Op, pos, err = cbor.ReadInt(data, pos)
			if err != nil {
				return frameHeader{}, 0, err
			}
		case "t":
			hdr.T, pos, err = cbor.ReadText(data, pos)
			if err != nil {
				return frameHeader{}, 0, err
			}
		default:
			pos, err = cbor.SkipValue(data, pos)
			if err != nil {
				return frameHeader{}, 0, err
			}
		}
	}
	return hdr, pos, nil
}

// decodeMessageBody decodes the body CBOR into the appropriate Event variant.
func decodeMessageBody(typ string, body []byte) (Event, error) {
	switch typ {
	case "#commit":
		var v comatproto.SyncSubscribeRepos_Commit
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode commit: %w", err)
		}
		return Event{Seq: v.Seq, Commit: &v}, nil
	case "#sync":
		var v comatproto.SyncSubscribeRepos_Sync
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode sync: %w", err)
		}
		return Event{Seq: v.Seq, Sync: &v}, nil
	case "#identity":
		var v comatproto.SyncSubscribeRepos_Identity
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode identity: %w", err)
		}
		return Event{Seq: v.Seq, Identity: &v}, nil
	case "#account":
		var v comatproto.SyncSubscribeRepos_Account
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode account: %w", err)
		}
		return Event{Seq: v.Seq, Account: &v}, nil
	case "#info":
		var v comatproto.SyncSubscribeRepos_Info
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode info: %w", err)
		}
		return Event{Info: &v}, nil
	default:
		return Event{}, errUnknownType
	}
}

// seqOf returns the sequence number for an event, or 0 if none.
func (e *Event) seqOf() int64 {
	return e.Seq
}
