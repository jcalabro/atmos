package xrpc

import (
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsTransient_Nil(t *testing.T) {
	t.Parallel()
	assert.False(t, IsTransient(nil))
}

func TestIsTransient_XRPCRetryable(t *testing.T) {
	t.Parallel()
	for _, code := range []int{429, 500, 502, 503, 504} {
		assert.True(t, IsTransient(&Error{StatusCode: code}), "status %d", code)
	}
}

func TestIsTransient_XRPCPermanent(t *testing.T) {
	t.Parallel()
	for _, code := range []int{400, 401, 403, 404, 409} {
		assert.False(t, IsTransient(&Error{StatusCode: code}), "status %d", code)
	}
}

func TestIsTransient_WrappedXRPC(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("sync failed: %w", &Error{StatusCode: 503})
	assert.True(t, IsTransient(err))
}

type timeoutErr struct{ timeout bool }

func (e *timeoutErr) Error() string   { return "timeout" }
func (e *timeoutErr) Timeout() bool   { return e.timeout }
func (e *timeoutErr) Temporary() bool { return false }

// Verify timeoutErr satisfies net.Error.
var _ net.Error = (*timeoutErr)(nil)

func TestIsTransient_NetTimeout(t *testing.T) {
	t.Parallel()
	assert.True(t, IsTransient(&timeoutErr{timeout: true}))
	assert.False(t, IsTransient(&timeoutErr{timeout: false}))
}

func TestIsTransient_ConnReset(t *testing.T) {
	t.Parallel()
	assert.True(t, IsTransient(syscall.ECONNRESET))
	assert.True(t, IsTransient(fmt.Errorf("read: %w", syscall.ECONNRESET)))
}

func TestIsTransient_ConnRefused(t *testing.T) {
	t.Parallel()
	assert.True(t, IsTransient(syscall.ECONNREFUSED))
}

func TestIsTransient_UnexpectedEOF(t *testing.T) {
	t.Parallel()
	assert.True(t, IsTransient(io.ErrUnexpectedEOF))
	assert.True(t, IsTransient(fmt.Errorf("read body: %w", io.ErrUnexpectedEOF)))
}

func TestIsTransient_RegularError(t *testing.T) {
	t.Parallel()
	assert.False(t, IsTransient(fmt.Errorf("bad cbor data")))
	assert.False(t, IsTransient(io.EOF))
}

func TestRetryAfter_WithRateLimit(t *testing.T) {
	t.Parallel()
	reset := time.Now().Add(30 * time.Second)
	err := &Error{
		StatusCode: 429,
		RateLimit:  &RateLimit{Reset: reset},
	}
	assert.Equal(t, reset, RetryAfter(err))
}

func TestRetryAfter_Wrapped(t *testing.T) {
	t.Parallel()
	reset := time.Now().Add(10 * time.Second)
	err := fmt.Errorf("failed: %w", &Error{
		StatusCode: 429,
		RateLimit:  &RateLimit{Reset: reset},
	})
	assert.Equal(t, reset, RetryAfter(err))
}

func TestRetryAfter_NoRateLimit(t *testing.T) {
	t.Parallel()
	assert.True(t, RetryAfter(&Error{StatusCode: 500}).IsZero())
}

func TestRetryAfter_ZeroResetTime(t *testing.T) {
	t.Parallel()
	err := &Error{
		StatusCode: 429,
		RateLimit:  &RateLimit{Limit: 100, Remaining: 0},
	}
	assert.True(t, RetryAfter(err).IsZero())
}

func TestRetryAfter_NilError(t *testing.T) {
	t.Parallel()
	assert.True(t, RetryAfter(nil).IsZero())
}

func TestRetryAfter_NonXRPCError(t *testing.T) {
	t.Parallel()
	assert.True(t, RetryAfter(fmt.Errorf("something")).IsZero())
}
