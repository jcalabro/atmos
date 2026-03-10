// Integration tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_MixedEventTypes(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#account", buildAccountBody(2, "did:plc:bob", true)),
			buildFrame("#info", buildInfoBody("OutdatedCursor")),
			buildFrame("#identity", buildIdentityBody(3, "did:plc:carol")),
			buildFrame("#account", buildAccountBody(4, "did:plc:dave", false)),
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
		if len(events) == 5 {
			cancel()
		}
	}

	require.Len(t, events, 5)

	assert.NotNil(t, events[0].Identity)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)

	assert.NotNil(t, events[1].Account)
	assert.Equal(t, "did:plc:bob", events[1].Account.DID)
	assert.True(t, events[1].Account.Active)

	assert.NotNil(t, events[2].Info)
	assert.Equal(t, "OutdatedCursor", events[2].Info.Name)

	assert.NotNil(t, events[3].Identity)
	assert.Equal(t, "did:plc:carol", events[3].Identity.DID)

	assert.NotNil(t, events[4].Account)
	assert.False(t, events[4].Account.Active)

	assert.Equal(t, int64(4), client.Cursor())
}

func TestIntegration_ReconnectWithCursor(t *testing.T) {
	t.Parallel()

	type connLog struct {
		query string
	}
	conns := make(chan connLog, 10)

	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		conns <- connLog{query: r.URL.RawQuery}
		ctx := context.Background()

		cursor := r.URL.Query().Get("cursor")
		if cursor == "" {
			for i := int64(1); i <= 3; i++ {
				_ = conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
			}
			_ = conn.CloseNow()
			return
		}

		for i := int64(4); i <= 5; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
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
		if len(events) == 5 {
			cancel()
		}
	}

	require.Len(t, events, 5)
	for i, evt := range events {
		assert.Equal(t, int64(i+1), evt.Seq)
	}

	<-conns // first connection
	c2 := <-conns
	assert.Contains(t, c2.query, "cursor=3")
}

func TestIntegration_MaxMessageSize(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		big := make([]byte, 1024)
		for i := range big {
			big[i] = 0x61
		}
		_ = conn.Write(ctx, websocket.MessageBinary, big)
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:            wsURL(srv),
		MaxMessageSize: gt.Some(int64(512)),
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) == 1 {
			time.AfterFunc(50*time.Millisecond, cancel)
		}
	}

	require.GreaterOrEqual(t, len(events), 1)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
}

func TestIntegration_FuzzFrameDecoder(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		nil,
		{},
		{0x00},
		{0xa2, 0x62, 0x6f, 0x70, 0x01},
		{0xff, 0xff, 0xff},
		make([]byte, 100),
	}
	for _, input := range inputs {
		_, _ = decodeFrame(input)
	}
}
