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
