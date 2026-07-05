package streaming

import (
	"context"
	"errors"
	"fmt"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/sync"
)

// errUnknownType is the internal sentinel body decoders return for an
// unrecognized event type; the frame-level decoders (decodeFrame,
// decodeLabelFrame, decodeJetstreamFrame) translate it into a
// *UnknownFrameError carrying the raw frame and best-effort seq.
var errUnknownType = errors.New("unknown event type")

// decodeStreamErrorFrame decodes an op=-1 error-frame body
// ({error, message}) into a *StreamError. Per the event-stream framing
// spec the body is {error: string, message?: string}, where error is a
// machine-readable code such as "FutureCursor", "OutdatedCursor", or
// "ConsumerTooSlow". This is distinct from the subscribeRepos #info
// message (which uses a "name" field); decoding an error frame as #info
// would silently drop the error code.
func decodeStreamErrorFrame(body []byte) (*StreamError, error) {
	count, pos, err := cbor.ReadMapHeader(body, 0)
	if err != nil {
		return nil, fmt.Errorf("decode error frame: %w", err)
	}
	var se StreamError
	for range count {
		key, newPos, err := cbor.ReadText(body, pos)
		if err != nil {
			return nil, fmt.Errorf("decode error frame: %w", err)
		}
		pos = newPos
		switch key {
		case "error":
			se.Code, pos, err = cbor.ReadText(body, pos)
		case "message":
			se.Message, pos, err = cbor.ReadText(body, pos)
		default:
			pos, err = cbor.SkipValue(body, pos)
		}
		if err != nil {
			return nil, fmt.Errorf("decode error frame: %w", err)
		}
	}
	return &se, nil
}

// bestEffortSeq extracts an int64 "seq" field from a CBOR-map frame
// body, returning 0 when the body is not a map or carries no readable
// seq. Used to attribute a seq to unknown frame types so gap detection
// can account for them; every sequenced-stream message type in the
// atproto lexicons carries a top-level "seq", so a future frame type is
// overwhelmingly likely to be readable here even though we cannot
// decode the rest of it.
func bestEffortSeq(body []byte) int64 {
	count, pos, err := cbor.ReadMapHeader(body, 0)
	if err != nil {
		return 0
	}
	for range count {
		key, next, err := cbor.ReadText(body, pos)
		if err != nil {
			return 0
		}
		pos = next
		if key == "seq" {
			v, _, err := cbor.ReadInt(body, pos)
			if err != nil {
				return 0
			}
			return v
		}
		if pos, err = cbor.SkipValue(body, pos); err != nil {
			return 0
		}
	}
	return 0
}

// ResyncKind classifies events whose operations represent an
// authoritative full-repo replacement rather than an incremental commit
// diff.
//
// The zero value, ResyncNone, is used for ordinary firehose events. A
// non-zero value means Event.Operations yields ActionResync operations
// for the repo identified by Event.Sync. The operation set may be empty:
// an empty resync event is still meaningful because it says the
// authoritative repo currently contains no records.
type ResyncKind uint8

const (
	// ResyncNone marks ordinary non-resync events. This is the zero
	// value so existing Event literals keep their previous meaning.
	ResyncNone ResyncKind = iota

	// ResyncSyncEvent marks a resync caused directly by an upstream
	// com.atproto.sync.subscribeRepos#sync frame. The original Sync
	// frame is preserved on Event.Sync, and Event.Seq is the relay seq
	// for that frame.
	ResyncSyncEvent

	// ResyncAsync marks a synthetic Event emitted after the verifier's
	// background repair path completes. These events are not upstream
	// frames, so Event.Seq is zero. Event.Sync carries the repaired DID
	// and new rev as the normal repository-resync envelope.
	ResyncAsync
)

// Event is a single event from a subscribeRepos or subscribeLabels stream.
type Event struct {
	Seq      int64
	Commit   *comatproto.SyncSubscribeRepos_Commit
	Sync     *comatproto.SyncSubscribeRepos_Sync
	Identity *comatproto.SyncSubscribeRepos_Identity
	Account  *comatproto.SyncSubscribeRepos_Account
	Info     *comatproto.SyncSubscribeRepos_Info

	// Resync is non-zero when this event carries authoritative full-repo
	// replacement state. Consumers do not need a separate resync API:
	// call Operations() on the normal Event and process ActionResync ops.
	//
	// For ResyncSyncEvent, Sync is the upstream #sync frame. For
	// ResyncAsync, Sync is a synthetic envelope containing the DID and
	// post-resync rev. In both cases Operations() may yield zero ops for
	// an empty authoritative repo; the Event itself must still be
	// delivered.
	Resync ResyncKind

	// Label stream fields (access via Labels()).
	labelBatch *comatproto.LabelSubscribeLabels_Labels
	LabelInfo  *comatproto.LabelSubscribeLabels_Info

	// Jetstream event. Populated when consuming from a Jetstream server.
	// For account/identity events, the existing Account/Identity fields
	// are also populated.
	Jetstream *JetstreamEvent

	// Set by readLoop for lazy #sync handling. Unexported, single-goroutine.
	ctx        context.Context
	syncClient *sync.Client

	// verifiedOps and verifierRan together encode the result of running
	// a sync.Verifier on this event. When verifierRan is true,
	// Operations() yields verifiedOps directly without re-decoding the
	// CAR. An empty-but-verifierRan-true means "verifier saw zero ops" —
	// distinct from "verifier never ran" (verifierRan=false).
	verifiedOps []Operation
	verifierRan bool

	// strictValidation, when true, makes Operations() validate each
	// op's typed fields (NSID, RecordKey, DID, TID) before yielding
	// and surface a typed atmos syntax error for any that fail.
	// Plumbed from Options.StrictValidation by readLoop.
	strictValidation bool
}

// Labels returns the individual labels from a subscribeLabels event,
// or nil for non-label events.
func (e *Event) Labels() []comatproto.LabelDefs_Label {
	if e.labelBatch == nil {
		return nil
	}
	return e.labelBatch.Labels
}

// frameHeader is the CBOR header that precedes each event body on the wire.
// op=1 means a regular message, t is the type suffix (e.g. "#commit").
type frameHeader struct {
	Op int64  // 1 = message, -1 = error
	T  string // type, e.g. "#commit"
}

// checkFrameBodyComplete verifies the body is exactly one CBOR value with no
// trailing bytes. A frame is exactly two concatenated CBOR values (header +
// body); trailing bytes mean a second frame was smuggled into one message,
// which the TS reference rejects ("too many CBOR data items in frame").
func checkFrameBodyComplete(body []byte) error {
	end, err := cbor.SkipValue(body, 0)
	if err != nil {
		return fmt.Errorf("decode frame body: %w", err)
	}
	if end != len(body) {
		return fmt.Errorf("decode frame: %d trailing bytes after body", len(body)-end)
	}
	return nil
}

// decodeFrame decodes an ATProto event stream frame (two concatenated CBOR values:
// header map + body).
func decodeFrame(data []byte) (Event, error) {
	hdr, bodyStart, err := decodeFrameHeader(data)
	if err != nil {
		return Event{}, fmt.Errorf("decode frame header: %w", err)
	}

	body := data[bodyStart:]
	if err := checkFrameBodyComplete(body); err != nil {
		return Event{}, err
	}

	if hdr.Op == -1 {
		// Error frame: body is {error, message}, NOT the #info {name, message}.
		se, err := decodeStreamErrorFrame(body)
		if err != nil {
			return Event{}, err
		}
		return Event{}, se
	}

	if hdr.Op != 1 {
		return Event{}, &UnknownFrameError{T: hdr.T, Op: hdr.Op, Seq: bestEffortSeq(body), Frame: data}
	}

	evt, err := decodeMessageBody(hdr.T, body)
	if errors.Is(err, errUnknownType) {
		return Event{}, &UnknownFrameError{T: hdr.T, Op: hdr.Op, Seq: bestEffortSeq(body), Frame: data}
	}
	return evt, err
}

// decodeFrameHeader reads the CBOR map header {op: int, t: string}.
func decodeFrameHeader(data []byte) (frameHeader, int, error) {
	count, pos, err := cbor.ReadMapHeader(data, 0)
	if err != nil {
		return frameHeader{}, 0, err
	}

	var hdr frameHeader
	for range count {
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
// For Jetstream commit events (which lack a seq field), time_us is used
// as the cursor value instead.
func (e *Event) seqOf() int64 {
	if e.Jetstream != nil && e.Seq == 0 {
		return e.Jetstream.TimeUS
	}
	return e.Seq
}

// repoOf returns the DID associated with this event, or "" for events
// that have no per-repo binding (#info frames, server-side error frames,
// label streams). Used to key the parallel scheduler.
func (e *Event) repoOf() string {
	switch {
	case e.Commit != nil:
		return e.Commit.Repo
	case e.Sync != nil:
		return e.Sync.DID
	case e.Identity != nil:
		return e.Identity.DID
	case e.Account != nil:
		return e.Account.DID
	}
	return ""
}

// decodeLabelFrame decodes an ATProto subscribeLabels frame (two concatenated
// CBOR values: header map + body).
func decodeLabelFrame(data []byte) (Event, error) {
	hdr, bodyStart, err := decodeFrameHeader(data)
	if err != nil {
		return Event{}, fmt.Errorf("decode frame header: %w", err)
	}

	body := data[bodyStart:]
	if err := checkFrameBodyComplete(body); err != nil {
		return Event{}, err
	}

	if hdr.Op == -1 {
		// Error frame: body is {error, message}, NOT the #info {name, message}.
		se, err := decodeStreamErrorFrame(body)
		if err != nil {
			return Event{}, err
		}
		return Event{}, se
	}

	if hdr.Op != 1 {
		return Event{}, &UnknownFrameError{T: hdr.T, Op: hdr.Op, Seq: bestEffortSeq(body), Frame: data}
	}

	switch hdr.T {
	case "#labels":
		var v comatproto.LabelSubscribeLabels_Labels
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode labels: %w", err)
		}
		return Event{Seq: v.Seq, labelBatch: &v}, nil
	case "#info":
		var v comatproto.LabelSubscribeLabels_Info
		if err := v.UnmarshalCBOR(body); err != nil {
			return Event{}, fmt.Errorf("decode label info: %w", err)
		}
		return Event{LabelInfo: &v}, nil
	default:
		return Event{}, &UnknownFrameError{T: hdr.T, Op: hdr.Op, Seq: bestEffortSeq(body), Frame: data}
	}
}
