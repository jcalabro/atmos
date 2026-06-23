package xrpc

import (
	"net/http"
	"net/url"
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

func TestParseError_Host(t *testing.T) {
	t.Parallel()

	t.Run("from request URL", func(t *testing.T) {
		t.Parallel()
		u, err := url.Parse("https://pds.example.com/xrpc/com.atproto.sync.getRepo?did=did:plc:x")
		require.NoError(t, err)
		resp := &http.Response{
			StatusCode: 429,
			Header:     http.Header{},
			Request:    &http.Request{URL: u},
		}
		e := parseError(resp, []byte(`{"error":"RateLimitExceeded"}`))
		assert.Equal(t, "pds.example.com", e.Host)
	})

	t.Run("no request is empty host", func(t *testing.T) {
		t.Parallel()
		resp := &http.Response{StatusCode: 500, Header: http.Header{}}
		e := parseError(resp, nil)
		assert.Empty(t, e.Host)
	})
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

func TestParseRateLimit_RetryAfterSeconds(t *testing.T) {
	// Pin time so the delta-seconds form is deterministic.
	fixed := time.Unix(1_700_000_000, 0)
	old := nowFunc
	nowFunc = func() time.Time { return fixed }
	defer func() { nowFunc = old }()

	h := http.Header{}
	h.Set("Retry-After", "120")
	rl := parseRateLimit(h)
	require.NotNil(t, rl)
	assert.Equal(t, fixed.Add(120*time.Second), rl.Reset)
}

func TestParseRateLimit_RetryAfterHTTPDate(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	h.Set("Retry-After", "Wed, 21 Oct 2026 07:28:00 GMT")
	rl := parseRateLimit(h)
	require.NotNil(t, rl)
	want, _ := http.ParseTime("Wed, 21 Oct 2026 07:28:00 GMT")
	assert.Equal(t, want, rl.Reset)
}

func TestParseRateLimit_RateLimitResetWinsOverRetryAfter(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	h.Set("RateLimit-Reset", "1700000000")
	h.Set("Retry-After", "120")
	rl := parseRateLimit(h)
	require.NotNil(t, rl)
	assert.Equal(t, time.Unix(1700000000, 0), rl.Reset, "RateLimit-Reset takes precedence")
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
