package streaming

import (
	"context"
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
	frames chan []byte
	closed chan struct{}
	once   sync.Once
}

func newMemConn(frames ...[]byte) *memConn {
	c := &memConn{frames: make(chan []byte, len(frames)), closed: make(chan struct{})}
	for _, f := range frames {
		c.frames <- f
	}
	return c
}

func (c *memConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case f := <-c.frames:
		return websocket.MessageBinary, f, nil
	case <-c.closed:
		return 0, nil, io.EOF
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (c *memConn) Close(websocket.StatusCode, string) error { c.closeOnce(); return nil }
func (c *memConn) CloseNow() error                          { c.closeOnce(); return nil }
func (c *memConn) SetReadLimit(int64)                       {}
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
		Dial: gt.Some(DialFunc(func(_ context.Context, url string) (Conn, *http.Response, error) {
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
		Dial: gt.Some(DialFunc(func(_ context.Context, url string) (Conn, *http.Response, error) {
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
