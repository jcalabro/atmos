package xrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

const testNSID = "test.method"

func testKey(host string) rateLimitKey { return rateLimitKey{host: host, nsid: testNSID} }

// A client keeping a reserve parks while that much of the limit remains, and
// not before.
func TestRateLimitState_ReserveParksEarly(t *testing.T) {
	t.Parallel()
	reset := time.Now().Add(time.Minute)
	for _, tc := range []struct {
		remaining int
		reserve   float64
		parked    bool
	}{
		{remaining: 0, reserve: 0, parked: true},
		{remaining: 1, reserve: 0, parked: false},
		{remaining: 300, reserve: 0.05, parked: true},
		{remaining: 301, reserve: 0.05, parked: false},
		{remaining: 6000, reserve: 1, parked: true},
	} {
		t.Run(fmt.Sprintf("remaining=%d/reserve=%v", tc.remaining, tc.reserve), func(t *testing.T) {
			t.Parallel()
			var s rateLimitState
			s.update(testKey("a.example"), &RateLimit{Limit: 6000, Remaining: tc.remaining, RemainingSet: true, Reset: reset}, time.Hour, tc.reserve)
			require.Equal(t, tc.parked, !s.parkedUntil(testKey("a.example")).IsZero())
		})
	}
}

// A reserve needs the limit to mean anything: without RateLimit-Limit the
// client parks only on a spent quota.
func TestRateLimitState_ReserveWithoutLimit(t *testing.T) {
	t.Parallel()
	var s rateLimitState
	s.update(testKey("a.example"), &RateLimit{Remaining: 1, RemainingSet: true, Reset: time.Now().Add(time.Minute)}, time.Hour, 0.5)
	require.True(t, s.parkedUntil(testKey("a.example")).IsZero())
}

// Responses to requests in flight when a quota ran out arrive after the one
// that parked it, still reporting quota from earlier in the same window.
// They must not release the park; a response from a later window does.
func TestRateLimitState_StaleResponseKeepsPark(t *testing.T) {
	t.Parallel()
	var s rateLimitState
	key := testKey("a.example")
	reset := time.Now().Add(time.Minute)
	s.update(key, &RateLimit{Limit: 100, Remaining: 0, RemainingSet: true, Reset: reset}, time.Hour, 0)
	s.update(key, &RateLimit{Limit: 100, Remaining: 7, RemainingSet: true, Reset: reset}, time.Hour, 0)
	require.Equal(t, reset, s.parkedUntil(key), "same-window response released the park")
	s.update(key, &RateLimit{Limit: 100, Remaining: 9, RemainingSet: true, Reset: reset.Add(-time.Second)}, time.Hour, 0)
	require.Equal(t, reset, s.parkedUntil(key), "older-window response released the park")

	s.update(key, &RateLimit{Limit: 100, Remaining: 99, RemainingSet: true, Reset: reset.Add(5 * time.Minute)}, time.Hour, 0)
	require.True(t, s.parkedUntil(key).IsZero(), "a new window releases the park")
}

// A later, shorter park never shortens an earlier, longer one.
func TestRateLimitState_ParkNeverShrinks(t *testing.T) {
	t.Parallel()
	var s rateLimitState
	key := testKey("a.example")
	long := time.Now().Add(time.Minute)
	s.update(key, &RateLimit{Remaining: 0, RemainingSet: true, Reset: long}, time.Hour, 0)
	// A Retry-After with no RateLimit-Remaining parks on its own.
	s.update(key, &RateLimit{Reset: time.Now().Add(time.Second)}, time.Hour, 0)
	require.Equal(t, long, s.parkedUntil(key))
}

// Each method is its own quota: a PDS limits getRepo apart from its global
// read bucket, so a parked getRepo must not block listRepos, nor the reverse.
func TestRateLimitState_KeyedByMethod(t *testing.T) {
	t.Parallel()
	var s rateLimitState
	getRepo := rateLimitKey{host: "a.example", nsid: "com.atproto.sync.getRepo"}
	listRepos := rateLimitKey{host: "a.example", nsid: "com.atproto.sync.listRepos"}
	reset := time.Now().Add(time.Minute)
	s.update(getRepo, &RateLimit{Limit: 6000, Remaining: 0, RemainingSet: true, Reset: reset}, time.Hour, 0)
	s.update(listRepos, &RateLimit{Limit: 3000, Remaining: 2900, RemainingSet: true, Reset: reset.Add(time.Second)}, time.Hour, 0)
	require.Equal(t, reset, s.parkedUntil(getRepo), "listRepos quota released getRepo")
	require.True(t, s.parkedUntil(listRepos).IsZero())
}

// wait reports the time it slept, and nothing when there was no park.
func TestRateLimitState_WaitReportsDuration(t *testing.T) {
	t.Parallel()
	var s rateLimitState
	key := testKey("a.example")
	waited, err := s.wait(t.Context(), key)
	require.NoError(t, err)
	require.Zero(t, waited)

	s.update(key, &RateLimit{Remaining: 0, RemainingSet: true, Reset: time.Now().Add(30 * time.Millisecond)}, time.Second, 0)
	waited, err = s.wait(t.Context(), key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, waited, 20*time.Millisecond)
}

// rateLimitServer answers every request with the given rate-limit headers
// and status, and counts the requests it served.
func rateLimitServer(t *testing.T, status int, limit, remaining int, reset time.Time) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("RateLimit-Limit", fmt.Sprint(limit))
		w.Header().Set("RateLimit-Remaining", fmt.Sprint(remaining))
		w.Header().Set("RateLimit-Reset", fmt.Sprint(reset.Unix()))
		w.WriteHeader(status)
		if status != http.StatusOK {
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RateLimitExceeded"})
			return
		}
		_, _ = w.Write([]byte("car bytes"))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

// A streamed query tracks the quota it reports, on success and on a 429, and
// the next one waits for the park rather than spend a request on it.
func TestQueryStream_TracksRateLimit(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusOK, http.StatusTooManyRequests} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			reset := time.Now().Add(time.Hour)
			srv, calls := rateLimitServer(t, status, 6000, 0, reset)
			c := &Client{Host: srv.URL, HTTPClient: gt.Some(srv.Client()), MaxRateLimitWait: gt.Some(2 * time.Hour)}

			body, err := c.QueryStream(t.Context(), testNSID, nil)
			if status == http.StatusOK {
				require.NoError(t, err)
				_, _ = io.Copy(io.Discard, body)
				require.NoError(t, body.Close())
			} else {
				require.True(t, IsRateLimited(err))
			}
			require.Equal(t, time.Unix(reset.Unix()+1, 0), c.RateLimitedUntil(testNSID))
			require.True(t, c.RateLimitedUntil("other.method").IsZero())

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			_, err = c.QueryStream(ctx, testNSID, nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, int32(1), calls.Load(), "a parked query reached the server")
		})
	}
}

// MaxRateLimitWait lets a client wait out a server's whole window; without
// it a park stays bounded by MaxServerDirectedDelay.
func TestClient_MaxRateLimitWait(t *testing.T) {
	t.Parallel()
	reset := time.Now().Add(5 * time.Minute)
	for _, tc := range []struct {
		name    string
		maxWait gt.Option[time.Duration]
		want    time.Duration
	}{
		{name: "default", want: MaxServerDirectedDelay},
		{name: "raised", maxWait: gt.Some(10 * time.Minute), want: time.Until(reset)},
		{name: "zero", maxWait: gt.Some(time.Duration(0)), want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := rateLimitServer(t, http.StatusOK, 6000, 0, reset)
			c := &Client{Host: srv.URL, HTTPClient: gt.Some(srv.Client()), MaxRateLimitWait: tc.maxWait}
			body, err := c.QueryStream(t.Context(), testNSID, nil)
			require.NoError(t, err)
			require.NoError(t, body.Close())
			until := c.RateLimitedUntil(testNSID)
			if tc.want == 0 {
				require.True(t, until.IsZero())
				return
			}
			require.WithinDuration(t, time.Now().Add(tc.want), until, 2*time.Second)
		})
	}
}

// RateLimitReserve parks a client while the reserve remains, through a real
// response.
func TestClient_RateLimitReserve(t *testing.T) {
	t.Parallel()
	reset := time.Now().Add(time.Minute)
	for _, tc := range []struct {
		remaining int
		parked    bool
	}{{remaining: 5, parked: true}, {remaining: 6, parked: false}} {
		t.Run(fmt.Sprint(tc.remaining), func(t *testing.T) {
			t.Parallel()
			srv, _ := rateLimitServer(t, http.StatusOK, 100, tc.remaining, reset)
			c := &Client{Host: srv.URL, HTTPClient: gt.Some(srv.Client()), Retry: gt.Some(noRetry()), RateLimitReserve: gt.Some(0.05)}
			_, err := c.QueryRaw(t.Context(), testNSID, nil)
			require.NoError(t, err)
			require.Equal(t, tc.parked, !c.RateLimitedUntil(testNSID).IsZero())
		})
	}
}

// WaitRateLimit waits out a park without sending anything, and honors ctx.
func TestClient_WaitRateLimit(t *testing.T) {
	t.Parallel()
	c := &Client{Host: "https://a.example"}
	key := rateLimitKey{host: "a.example", nsid: testNSID}
	c.rl.update(key, &RateLimit{Remaining: 0, RemainingSet: true, Reset: time.Now().Add(30 * time.Millisecond)}, time.Second, 0)
	waited, err := c.WaitRateLimit(t.Context(), testNSID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, waited, 20*time.Millisecond)

	c.rl.update(key, &RateLimit{Remaining: 0, RemainingSet: true, Reset: time.Now().Add(time.Hour)}, time.Hour, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.WaitRateLimit(ctx, testNSID)
	require.ErrorIs(t, err, context.Canceled)
}
