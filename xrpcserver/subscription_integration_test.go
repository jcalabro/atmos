// Tests require a real WebSocket dial, unavailable in Node/WASI.
//go:build !js && !wasip1

package xrpcserver_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests wire the xrpcserver subscription support to the streaming
// client — each end validates the other's framing, on both
// subprotocols.

const labelsNSID = "com.atproto.label.subscribeLabels"

func labelsUnion(seq int64, uri, val string) comatproto.LabelSubscribeLabels_Message {
	return comatproto.LabelSubscribeLabels_Message{
		LabelSubscribeLabels_Labels: gt.SomeRef(comatproto.LabelSubscribeLabels_Labels{
			Seq: seq,
			Labels: []comatproto.LabelDefs_Label{
				{URI: uri, Val: val, Src: "did:plc:labeler", Cts: "2024-01-01T00:00:00Z"},
			},
		}),
	}
}

// startLabelServer serves a label subscription emitting the given
// messages then blocking until client disconnect (so the client doesn't
// see a spurious close-triggered reconnect mid-read).
func startLabelServer(t *testing.T, cfg xrpcserver.SubscriptionConfig, msgs ...comatproto.LabelSubscribeLabels_Message) *httptest.Server {
	t.Helper()
	var s xrpcserver.Server
	err := s.HandleSubscription(labelsNSID, cfg,
		func(ctx context.Context, _ xrpcserver.Params, stream *xrpcserver.Stream) error {
			for _, m := range msgs {
				if err := stream.Send(ctx, "#labels", m); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return err
				}
			}
			<-ctx.Done()
			return nil
		})
	require.NoError(t, err)
	srv := httptest.NewServer(&s)
	t.Cleanup(srv.Close)
	return srv
}

func consumeLabels(t *testing.T, url string, subs gt.Option[[]xrpc.Subprotocol], want int) []streaming.Event {
	t.Helper()
	client, err := streaming.NewClient(streaming.Options{
		URL:          url,
		Subprotocols: subs,
		Parallelism:  gt.Some(1),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var events []streaming.Event
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, batch...)
		if len(events) >= want {
			cancel()
		}
	}
	require.Len(t, events, want)
	return events
}

func TestServerClientRoundTrip(t *testing.T) {
	t.Parallel()

	msgs := []comatproto.LabelSubscribeLabels_Message{
		labelsUnion(1, "at://did:plc:a/app.bsky.feed.post/1", "spam"),
		labelsUnion(2, "at://did:plc:b/app.bsky.feed.post/2", "gore"),
	}

	check := func(t *testing.T, events []streaming.Event) {
		t.Helper()
		require.Len(t, events[0].Labels(), 1)
		assert.Equal(t, int64(1), events[0].Seq)
		assert.Equal(t, "spam", events[0].Labels()[0].Val)
		assert.Equal(t, int64(2), events[1].Seq)
		assert.Equal(t, "gore", events[1].Labels()[0].Val)
	}

	t.Run("v0 cbor unnegotiated", func(t *testing.T) {
		t.Parallel()
		srv := startLabelServer(t, xrpcserver.SubscriptionConfig{}, msgs...)
		url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + labelsNSID
		check(t, consumeLabels(t, url, gt.None[[]xrpc.Subprotocol](), 2))
	})

	t.Run("v1 json negotiated", func(t *testing.T) {
		t.Parallel()
		srv := startLabelServer(t, xrpcserver.SubscriptionConfig{
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, xrpc.SubprotocolV1JSON}),
		}, msgs...)
		url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + labelsNSID
		check(t, consumeLabels(t, url, gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}), 2))
	})

	t.Run("client offers v1 to v0-only server", func(t *testing.T) {
		t.Parallel()
		srv := startLabelServer(t, xrpcserver.SubscriptionConfig{}, msgs...)
		url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + labelsNSID
		// Server supports only v0; the offer is not echoed and the
		// client falls back to CBOR cleanly.
		check(t, consumeLabels(t, url, gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV0CBOR}), 2))
	})
}

// TestServerClientErrorFrame asserts a server SendError surfaces as a
// typed StreamError on the client iterator, for both subprotocols.
func TestServerClientErrorFrame(t *testing.T) {
	t.Parallel()

	for _, sub := range []gt.Option[[]xrpc.Subprotocol]{
		gt.None[[]xrpc.Subprotocol](),
		gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
	} {
		var s xrpcserver.Server
		err := s.HandleSubscription(labelsNSID, xrpcserver.SubscriptionConfig{
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, xrpc.SubprotocolV1JSON}),
		},
			func(ctx context.Context, _ xrpcserver.Params, stream *xrpcserver.Stream) error {
				return stream.SendError(ctx, "FutureCursor", "cursor is ahead of stream head")
			})
		require.NoError(t, err)
		srv := httptest.NewServer(&s)
		t.Cleanup(srv.Close)

		client, err := streaming.NewClient(streaming.Options{
			URL:          "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + labelsNSID,
			Subprotocols: sub,
			Parallelism:  gt.Some(1),
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		var streamErr *streaming.StreamError
		for _, err := range client.Events(ctx) {
			if err != nil {
				if se, ok := errors.AsType[*streaming.StreamError](err); ok {
					streamErr = se
					cancel()
				}
			}
		}
		cancel()
		require.NotNil(t, streamErr)
		assert.Equal(t, "FutureCursor", streamErr.Code)
		assert.Equal(t, "cursor is ahead of stream head", streamErr.Message)
	}
}

// TestServerClientCursorParam asserts the client's resume cursor lands
// in the server handler's Params.
func TestServerClientCursorParam(t *testing.T) {
	t.Parallel()

	gotCursor := make(chan int64, 1)
	var s xrpcserver.Server
	err := s.HandleSubscription(labelsNSID, xrpcserver.SubscriptionConfig{},
		func(ctx context.Context, p xrpcserver.Params, stream *xrpcserver.Stream) error {
			gotCursor <- p.Int64Or("cursor", -1)
			if err := stream.Send(ctx, "#labels", labelsUnion(43, "at://x", "ok")); err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		})
	require.NoError(t, err)
	srv := httptest.NewServer(&s)
	t.Cleanup(srv.Close)

	client, err := streaming.NewClient(streaming.Options{
		URL:         "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + labelsNSID,
		Cursor:      gt.Some(int64(42)),
		Parallelism: gt.Some(1),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		if len(batch) > 0 {
			cancel()
		}
	}
	assert.Equal(t, int64(42), <-gotCursor)
}

// TestServerClientCompression asserts opt-in permessage-deflate
// round-trips on a v1.json stream (context takeover on both ends).
func TestServerClientCompression(t *testing.T) {
	t.Parallel()

	srv := startLabelServer(t, xrpcserver.SubscriptionConfig{
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, xrpc.SubprotocolV1JSON}),
		Compression:  gt.Some(websocket.CompressionContextTakeover),
	},
		labelsUnion(1, "at://did:plc:a/app.bsky.feed.post/1", "spam"),
		labelsUnion(2, "at://did:plc:a/app.bsky.feed.post/2", "spam"),
	)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + labelsNSID

	client, err := streaming.NewClient(streaming.Options{
		URL:          url,
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
		Compression:  gt.Some(websocket.CompressionContextTakeover),
		Parallelism:  gt.Some(1),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var events []streaming.Event
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, batch...)
		if len(events) >= 2 {
			cancel()
		}
	}
	require.Len(t, events, 2)
	assert.Equal(t, int64(2), events[1].Seq)
}
