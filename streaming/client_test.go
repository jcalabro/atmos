// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildFrame constructs an ATProto event stream frame: CBOR header + CBOR body.
func buildFrame(t string, body []byte) []byte {
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendTextKey(hdr, "t")
	hdr = cbor.AppendText(hdr, t)
	return append(hdr, body...)
}

// buildErrorFrame constructs an error frame (op=-1).
func buildErrorFrame(body []byte) []byte {
	hdr := cbor.AppendMapHeader(nil, 1)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, -1)
	return append(hdr, body...)
}

func buildIdentityBody(seq int64, did string) []byte {
	evt := &comatproto.SyncSubscribeRepos_Identity{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#identity",
		DID:           did,
		Seq:           seq,
		Time:          "2024-01-01T00:00:00Z",
	}
	data, err := evt.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func buildAccountBody(seq int64, did string, active bool) []byte {
	evt := &comatproto.SyncSubscribeRepos_Account{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#account",
		DID:           did,
		Seq:           seq,
		Active:        active,
		Time:          "2024-01-01T00:00:00Z",
	}
	data, err := evt.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func buildInfoBody(name string) []byte {
	evt := &comatproto.SyncSubscribeRepos_Info{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#info",
		Name:          name,
	}
	data, err := evt.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func mustNewClient(t *testing.T, opts Options) *Client {
	t.Helper()
	c, err := NewClient(opts)
	require.NoError(t, err)
	return c
}

func startMockRelay(t *testing.T, handler func(conn *websocket.Conn, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("accept error: %v", err)
			return
		}
		handler(conn, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// writeFrames sends a list of frames to the connection and closes it.
func writeFrames(conn *websocket.Conn, frames ...[]byte) {
	ctx := context.Background()
	for _, f := range frames {
		_ = conn.Write(ctx, websocket.MessageBinary, f)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "done")
}

func TestHappyPath(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
			buildFrame("#account", buildAccountBody(3, "did:plc:carol", true)),
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, evt)
		if len(events) == 3 {
			cancel()
		}
	}

	require.Len(t, events, 3)
	assert.Equal(t, int64(1), events[0].Seq)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
	assert.Equal(t, int64(2), events[1].Seq)
	assert.Equal(t, "did:plc:bob", events[1].Identity.DID)
	assert.Equal(t, int64(3), events[2].Seq)
	assert.Equal(t, "did:plc:carol", events[2].Account.DID)
	assert.True(t, events[2].Account.Active)
	assert.Equal(t, int64(3), client.Cursor())
}

func TestSequenceGap(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(5, "did:plc:bob")),
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	var (
		events []Event
		gaps   []*GapError
	)
	for evt, err := range client.Events(ctx) {
		if err != nil {
			if ge, ok := errors.AsType[*GapError](err); ok {
				gaps = append(gaps, ge)
				continue
			}
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, gaps, 1)
	assert.Equal(t, int64(2), gaps[0].Expected)
	assert.Equal(t, int64(5), gaps[0].Got)
	require.Len(t, events, 2)
}

func TestReconnection(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		n := connCount.Add(1)
		ctx := context.Background()
		if n == 1 {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
			_ = conn.CloseNow()
			return
		}
		assert.Contains(t, r.URL.RawQuery, "cursor=1")
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var reconnects atomic.Int32
	client := mustNewClient(t, Options{
		URL: wsURL(srv),
		Backoff: gt.Some(BackoffPolicy{
			InitialDelay: gt.Some(10 * time.Millisecond),
			MaxDelay:     gt.Some(50 * time.Millisecond),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(false),
		}),
		OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
			reconnects.Add(1)
		}),
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
	assert.Equal(t, "did:plc:bob", events[1].Identity.DID)
	assert.GreaterOrEqual(t, reconnects.Load(), int32(1))
}

func TestConsumerTooSlow(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		n := connCount.Add(1)
		ctx := context.Background()
		if n == 1 {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
			_ = conn.Close(websocket.StatusPolicyViolation, "ConsumerTooSlow")
			return
		}
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv),
		Backoff: gt.Some(BackoffPolicy{
			InitialDelay: gt.Some(10 * time.Millisecond),
			MaxDelay:     gt.Some(50 * time.Millisecond),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(false),
		}),
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, int32(2), connCount.Load())
}

func TestContextCancellation(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		<-time.After(10 * time.Second)
	})

	ctx, cancel := context.WithCancel(context.Background())
	client := mustNewClient(t, Options{URL: wsURL(srv)})

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var count int
	for range client.Events(ctx) {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestBadCBOR(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{0xff, 0xfe, 0xfd})
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	var (
		events   []Event
		errCount int
	)
	for evt, err := range client.Events(ctx) {
		if err != nil {
			errCount++
			continue
		}
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, 1, errCount)
}

func TestConcurrentCursorRead(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= 100; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_ = client.Cursor()
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	var count int
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		_ = evt
		count++
		if count >= 100 {
			cancel()
		}
	}
	<-done

	assert.Equal(t, int64(100), client.Cursor())
}

func TestGracefulClose(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv),
		Backoff: gt.Some(BackoffPolicy{
			InitialDelay: gt.Some(10 * time.Millisecond),
			MaxDelay:     gt.Some(50 * time.Millisecond),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(false),
		}),
	})

	var count int
	for _, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		count++
		if count >= 3 {
			close(done)
			cancel()
			break
		}
	}

	assert.GreaterOrEqual(t, count, 3)
}

func TestErrorFrame(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildErrorFrame(buildInfoBody("OutdatedCursor")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) == 1 {
			cancel()
		}
	}

	require.Len(t, events, 1)
	require.NotNil(t, events[0].Info)
	assert.Equal(t, "OutdatedCursor", events[0].Info.Name)
}

func TestInfoEvent(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#info", buildInfoBody("OutdatedCursor")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) == 1 {
			cancel()
		}
	}

	require.Len(t, events, 1)
	require.NotNil(t, events[0].Info)
	assert.Equal(t, "OutdatedCursor", events[0].Info.Name)
}
