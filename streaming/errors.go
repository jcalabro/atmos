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
