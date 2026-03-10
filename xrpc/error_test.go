package xrpc

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  Error
		want string
	}{
		{"with message", Error{StatusCode: 400, Name: "InvalidRequest", Message: "bad param"}, "xrpc 400 InvalidRequest: bad param"},
		{"without message", Error{StatusCode: 401, Name: "AuthRequired"}, "xrpc 401 AuthRequired"},
		{"status only", Error{StatusCode: 500}, "xrpc 500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.err.Error())
		})
	}
}

func TestError_IsRateLimited(t *testing.T) {
	t.Parallel()
	assert.True(t, (&Error{StatusCode: 429}).IsRateLimited())
	assert.False(t, (&Error{StatusCode: 400}).IsRateLimited())
}

func TestParseError_WithJSON(t *testing.T) {
	t.Parallel()
	resp := &http.Response{
		StatusCode: 400,
		Header:     http.Header{},
	}
	body := []byte(`{"error":"InvalidRequest","message":"bad cursor"}`)
	e := parseError(resp, body)
	assert.Equal(t, 400, e.StatusCode)
	assert.Equal(t, "InvalidRequest", e.Name)
	assert.Equal(t, "bad cursor", e.Message)
	assert.Nil(t, e.RateLimit)
}

func TestParseError_EmptyBody(t *testing.T) {
	t.Parallel()
	resp := &http.Response{
		StatusCode: 502,
		Header:     http.Header{},
	}
	e := parseError(resp, nil)
	assert.Equal(t, 502, e.StatusCode)
	assert.Empty(t, e.Name)
}

func TestParseError_NonJSON(t *testing.T) {
	t.Parallel()
	resp := &http.Response{
		StatusCode: 500,
		Header:     http.Header{},
	}
	e := parseError(resp, []byte("gateway timeout"))
	assert.Equal(t, 500, e.StatusCode)
	assert.Empty(t, e.Name)
}

func TestParseRateLimit_Full(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	h.Set("RateLimit-Limit", "100")
	h.Set("RateLimit-Remaining", "0")
	h.Set("RateLimit-Reset", "1700000000")
	rl := parseRateLimit(h)
	require.NotNil(t, rl)
	assert.Equal(t, 100, rl.Limit)
	assert.Equal(t, 0, rl.Remaining)
	assert.Equal(t, time.Unix(1700000000, 0), rl.Reset)
}

func TestParseRateLimit_Partial(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	h.Set("RateLimit-Remaining", "5")
	rl := parseRateLimit(h)
	require.NotNil(t, rl)
	assert.Equal(t, 0, rl.Limit)
	assert.Equal(t, 5, rl.Remaining)
}

func TestParseRateLimit_None(t *testing.T) {
	t.Parallel()
	assert.Nil(t, parseRateLimit(http.Header{}))
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()
	assert.True(t, isRetryable(429))
	assert.True(t, isRetryable(500))
	assert.True(t, isRetryable(502))
	assert.True(t, isRetryable(503))
	assert.True(t, isRetryable(504))
	assert.False(t, isRetryable(400))
	assert.False(t, isRetryable(401))
	assert.False(t, isRetryable(404))
}
