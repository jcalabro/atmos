package streaming

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGapError(t *testing.T) {
	t.Parallel()
	e := &GapError{Expected: 10, Got: 15}
	assert.Equal(t, "sequence gap: expected 10, got 15", e.Error())
}

func TestDecodeError(t *testing.T) {
	t.Parallel()
	inner := errors.New("bad frame")
	e := &DecodeError{Frame: []byte{1, 2, 3}, Err: inner}
	assert.Equal(t, "bad frame", e.Error())
	assert.Equal(t, inner, e.Unwrap())
	assert.True(t, errors.Is(e, inner))
}

func TestErrorRawFrame(t *testing.T) {
	t.Parallel()

	// Direct DecodeError.
	frame := []byte{0xAB, 0xCD}
	de := &DecodeError{Frame: frame, Err: errors.New("x")}
	assert.Equal(t, frame, ErrorRawFrame(de))

	// Wrapped DecodeError.
	wrapped := fmt.Errorf("outer: %w", de)
	assert.Equal(t, frame, ErrorRawFrame(wrapped))

	// Non-DecodeError.
	assert.Nil(t, ErrorRawFrame(errors.New("plain")))

	// Nil error.
	assert.Nil(t, ErrorRawFrame(nil))
}

func TestGapError_DifferentValues(t *testing.T) {
	t.Parallel()
	e := &GapError{Expected: 1, Got: 100}
	require.Contains(t, e.Error(), "expected 1")
	require.Contains(t, e.Error(), "got 100")
}
