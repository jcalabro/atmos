package streaming

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memConn is an in-memory Conn: Read yields queued frames in order, then
// blocks until Close. It lets a test drive the client without a socket.
type memConn struct {
	frames  chan memFrame
	closed  chan struct{}
	once    sync.Once
	subprot string
}

// memFrame is a queued message with its websocket message type.
type memFrame struct {
	msgType websocket.MessageType
	data    []byte
}

func newMemConn(frames ...[]byte) *memConn {
	c := &memConn{frames: make(chan memFrame, len(frames)), closed: make(chan struct{})}
	for _, f := range frames {
		c.frames <- memFrame{websocket.MessageBinary, f}
	}
	return c
}

// newMemConnTyped builds a memConn with per-frame message types and a
// negotiated subprotocol, for driving the v1.json client path.
func newMemConnTyped(subprotocol string, frames ...memFrame) *memConn {
	c := &memConn{
		frames:  make(chan memFrame, len(frames)),
		closed:  make(chan struct{}),
		subprot: subprotocol,
	}
	for _, f := range frames {
		c.frames <- f
	}
	return c
}

func (c *memConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case f := <-c.frames:
		return f.msgType, f.data, nil
	case <-c.closed:
		return 0, nil, io.EOF
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (c *memConn) Close(websocket.StatusCode, string) error { c.closeOnce(); return nil }
func (c *memConn) CloseNow() error                          { c.closeOnce(); return nil }
func (c *memConn) SetReadLimit(int64)                       {}
func (c *memConn) Subprotocol() string                      { return c.subprot }
func (c *memConn) closeOnce()                               { c.once.Do(func() { close(c.closed) }) }

// TestDialInjection drives the client over an injected in-memory Conn and
// asserts events decode through the normal pipeline with no socket.
func TestDialInjection(t *testing.T) {
	t.Parallel()

	conn := newMemConn(
		buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
		buildFrame("#account", buildAccountBody(2, "did:plc:bob", true)),
	)

	var dialedURL string
	client := mustNewClient(t, Options{
		URL:         "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
		Parallelism: gt.Some(1),
		Dial: gt.Some(DialFunc(func(_ context.Context, url string, _ DialConfig) (Conn, *http.Response, error) {
			dialedURL = url
			return conn, nil, nil
		})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var events []Event
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, batch...)
		if len(events) >= 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, int64(1), events[0].Seq)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
	assert.Equal(t, int64(2), events[1].Seq)
	assert.Equal(t, "did:plc:bob", events[1].Account.DID)
	assert.Equal(t, int64(2), client.Cursor())
	assert.Equal(t, "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos", dialedURL)
}

// TestDialInjectionCursorInURL asserts the injected dialer receives the URL
// with the resume cursor appended, matching the real dial path.
func TestDialInjectionCursorInURL(t *testing.T) {
	t.Parallel()

	conn := newMemConn(buildFrame("#identity", buildIdentityBody(6, "did:plc:alice")))

	var dialedURL string
	client := mustNewClient(t, Options{
		URL:         "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
		Cursor:      gt.Some(int64(5)),
		Parallelism: gt.Some(1),
		Dial: gt.Some(DialFunc(func(_ context.Context, url string, _ DialConfig) (Conn, *http.Response, error) {
			dialedURL = url
			return conn, nil, nil
		})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		if len(batch) > 0 {
			cancel()
		}
	}

	assert.Contains(t, dialedURL, "cursor=5")
}

// TestDialTransientStatusRetries pins the dial status split: an
// overloaded server (or its load balancer) answering 5xx/429/408/425 is
// redialed with backoff and the stream resumes, while any other non-101
// status stays a terminal *DialError. Treating a 503 as terminal ended
// a relay consumer's stream permanently during a relay DDoS.
func TestDialTransientStatusRetries(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		status    int
		transient bool
	}{
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusGatewayTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusRequestTimeout, true},
		{http.StatusTooEarly, true},
		{http.StatusOK, false},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusNotFound, false},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			t.Parallel()

			const rejections = 3
			conn := newMemConn(buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
			var (
				mu         sync.Mutex
				dials      int
				reconnects int
			)
			client := mustNewClient(t, Options{
				URL:         "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
				Parallelism: gt.Some(1),
				Backoff: gt.Some(BackoffPolicy{
					InitialDelay: gt.Some(time.Millisecond),
					MaxDelay:     gt.Some(time.Millisecond),
				}),
				OnReconnect: gt.Some(func(int, time.Duration) {
					mu.Lock()
					reconnects++
					mu.Unlock()
				}),
				Dial: gt.Some(DialFunc(func(context.Context, string, DialConfig) (Conn, *http.Response, error) {
					mu.Lock()
					defer mu.Unlock()
					dials++
					if dials <= rejections {
						return nil, &http.Response{StatusCode: tc.status}, errors.New("expected handshake response status code 101")
					}
					return conn, nil, nil
				})),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var (
				events  []Event
				errs    []error
				dialErr *DialError
			)
			for batch, err := range client.Events(ctx) {
				if err != nil {
					errs = append(errs, err)
					if de, ok := errors.AsType[*DialError](err); ok {
						dialErr = de
					}
					continue
				}
				events = append(events, batch...)
				if len(events) > 0 {
					cancel()
				}
			}

			mu.Lock()
			defer mu.Unlock()
			if tc.transient {
				require.Empty(t, errs, "a transient status must not surface to the consumer")
				require.Len(t, events, 1, "the stream must resume after transient rejections")
				require.Equal(t, rejections+1, dials)
				require.Equal(t, rejections, reconnects, "each transient rejection must back off via OnReconnect")
				return
			}
			require.Len(t, errs, 1)
			require.NotNil(t, dialErr, "want *DialError, got %v", errs[0])
			require.Equal(t, tc.status, dialErr.StatusCode)
			require.Empty(t, events, "a deterministic rejection ends the iterator")
			require.Equal(t, 1, dials, "a deterministic rejection must not be redialed")
		})
	}
}
