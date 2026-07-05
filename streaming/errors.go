package streaming

import (
	"errors"
	"fmt"
)

// GapError indicates missed sequence numbers in the firehose.
type GapError struct {
	Expected int64 // lastSeq + 1
	Got      int64 // actual seq received
}

func (e *GapError) Error() string {
	return fmt.Sprintf("sequence gap: expected %d, got %d", e.Expected, e.Got)
}

// DropError indicates that an event was dropped from the parallel
// scheduler's per-DID queue because the queue was full. Surfaced via
// the streaming client's iterator as a (nil, err) yield. Dropped
// events are silently lost from the perspective of the consumer.
//
// QueueLen is the per-DID queue capacity (currently Options.Parallelism
// * 2). It is fixed for the lifetime of the client; this field exists
// so the consumer can log a single drop and know whether the cap is
// configured too low without consulting Options.
//
// AdditionalDropsSuppressed is non-zero when the internal drop-
// notification channel filled before this DropError was delivered,
// causing N other DropErrors to be coalesced into this one rather
// than reported individually. Consumers that need exact loss accounting
// should sum DropError.AdditionalDropsSuppressed + 1 across all
// DropErrors yielded.
type DropError struct {
	DID                       string
	Seq                       int64
	QueueLen                  int
	AdditionalDropsSuppressed uint64
}

func (e *DropError) Error() string {
	if e.AdditionalDropsSuppressed > 0 {
		return fmt.Sprintf("event dropped: did=%s seq=%d queueLen=%d (+%d additional drops suppressed)",
			e.DID, e.Seq, e.QueueLen, e.AdditionalDropsSuppressed)
	}
	return fmt.Sprintf("event dropped: did=%s seq=%d queueLen=%d", e.DID, e.Seq, e.QueueLen)
}

// DecodeError wraps a decode failure and carries the raw frame bytes.
type DecodeError struct {
	Frame []byte // raw WebSocket message
	Err   error
}

func (e *DecodeError) Error() string { return e.Err.Error() }
func (e *DecodeError) Unwrap() error { return e.Err }

// UnknownFrameError indicates a well-formed frame whose type ("t") or op
// code this build does not recognize — a relay speaking a newer protocol
// revision. Per the atproto event-stream spec the client stays connected
// and keeps reading; the frame itself is never delivered as an Event. It
// is surfaced as an iterator error rather than skipped silently because
// for archival consumers an unrecognized frame IS data loss: the relay
// consumed a seq for it but nothing can be stored, and the remediation
// (upgrade the client) is the opposite of a relay-side gap's (data is
// gone upstream; nothing to do).
//
// When Seq is non-zero it was extracted from the frame body and gap
// detection has accounted for this frame: no spurious GapError fires on
// the next recognized frame. When Seq is 0 the body carried no readable
// seq, and the next recognized frame fires a GapError that covers the
// unknown frame's position. Unknown frames never enter the watermark, so
// the reconnect cursor does not advance past one until a later
// recognized event does — a reconnect may therefore re-deliver and
// re-report the same unknown frame (at-least-once, like events).
type UnknownFrameError struct {
	T     string // frame type from the header, e.g. "#futureThing"; empty for unknown ops
	Op    int64  // frame op code; 1 for typed messages, anything else is an unknown op
	Seq   int64  // best-effort seq from the frame body, or 0 if unavailable
	Frame []byte // raw WebSocket message
}

func (e *UnknownFrameError) Error() string {
	return fmt.Sprintf("unknown frame: op=%d t=%q seq=%d", e.Op, e.T, e.Seq)
}

// StreamError is an event-stream error frame (op = -1) sent by the
// server, e.g. "FutureCursor" or "ConsumerTooSlow", usually immediately
// before it closes the connection. The client yields it on the iterator
// (after flushing events that preceded it) and keeps its normal
// lifecycle: the server's close lands as a read error and the client
// reconnects with backoff from the persisted cursor.
//
// Consumers should watch for repeats. A persistent FutureCursor loop
// means the persisted cursor is ahead of everything the relay has
// (cursor corruption, or a relay restored from an older backup) and will
// never resolve without operator intervention — each reconnect
// re-subscribes with the same cursor and receives the same error frame.
type StreamError struct {
	Code    string // machine-readable error code, e.g. "FutureCursor"
	Message string // optional human-readable detail
}

func (e *StreamError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("stream error frame: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("stream error frame: %s", e.Code)
}

// ErrorRawFrame extracts the raw frame bytes from a DecodeError or
// UnknownFrameError, or nil.
func ErrorRawFrame(err error) []byte {
	if de, ok := errors.AsType[*DecodeError](err); ok {
		return de.Frame
	}
	if ue, ok := errors.AsType[*UnknownFrameError](err); ok {
		return ue.Frame
	}
	return nil
}

// DialError indicates a connection failure that should not be retried,
// such as the server returning a non-WebSocket HTTP response (e.g. 200, 404).
type DialError struct {
	StatusCode int   // HTTP status code, or 0 if unavailable
	Err        error // underlying error
}

func (e *DialError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("dial: HTTP %d: %v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("dial: %v", e.Err)
}

func (e *DialError) Unwrap() error { return e.Err }
