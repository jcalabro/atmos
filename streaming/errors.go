package streaming

import (
	"errors"
	"fmt"
)

// GapError indicates missed sequence numbers in the firehose. Under
// parallel verification (Options.Parallelism > 1), gaps are detected
// per-DID rather than globally; DID identifies the repo whose seq
// sequence skipped. Under strict-order mode (Parallelism = 1), DID is
// empty and Expected/Got reference the global firehose stream.
type GapError struct {
	DID      string // empty under strict-order mode
	Expected int64
	Got      int64
}

func (e *GapError) Error() string {
	if e.DID != "" {
		return fmt.Sprintf("sequence gap on %s: expected %d, got %d", e.DID, e.Expected, e.Got)
	}
	return fmt.Sprintf("sequence gap: expected %d, got %d", e.Expected, e.Got)
}

// DropError indicates that an event was dropped from the parallel
// scheduler's per-DID queue because the queue was full. Surfaced via
// the Verifier's AsyncErrors channel; consumers see it through the
// (nil, err) iterator yield. Dropped events are silently lost from
// the perspective of the consumer.
//
// QueueLen is the per-DID queue capacity at the time of the drop;
// callers can use this to tell whether the cap was set too low.
type DropError struct {
	DID      string
	Seq      int64
	QueueLen int
}

func (e *DropError) Error() string {
	return fmt.Sprintf("event dropped: did=%s seq=%d queueLen=%d", e.DID, e.Seq, e.QueueLen)
}

// DecodeError wraps a decode failure and carries the raw frame bytes.
type DecodeError struct {
	Frame []byte // raw WebSocket message
	Err   error
}

func (e *DecodeError) Error() string { return e.Err.Error() }
func (e *DecodeError) Unwrap() error { return e.Err }

// ErrorRawFrame extracts the raw frame bytes from a DecodeError, or nil.
func ErrorRawFrame(err error) []byte {
	if de, ok := errors.AsType[*DecodeError](err); ok {
		return de.Frame
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
