// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelClient_HappyPath(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()

		// Batch 1: two labels applied
		frame1 := buildLabelFrame("#labels", mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
			{
				Src: "did:plc:labeler",
				URI: "at://did:plc:alice/app.bsky.feed.post/abc",
				Val: "spam",
				Cts: "2024-01-01T00:00:00Z",
			},
			{
				Src: "did:plc:labeler",
				URI: "at://did:plc:bob/app.bsky.feed.post/xyz",
				Val: "nudity",
				Cts: "2024-01-01T00:00:01Z",
			},
		}, 1))

		// Batch 2: one negation (removing "spam" from alice's post)
		frame2 := buildLabelFrame("#labels", mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
			{
				Src: "did:plc:labeler",
				URI: "at://did:plc:alice/app.bsky.feed.post/abc",
				Val: "spam",
				Neg: gt.Some(true),
				Cts: "2024-01-01T00:00:05Z",
			},
		}, 2))

		_ = conn.Write(ctx, websocket.MessageBinary, frame1)
		_ = conn.Write(ctx, websocket.MessageBinary, frame2)
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// URL contains "subscribeLabels" to trigger label decoder.
	client := mustNewClient(t, Options{
		URL: wsURL(srv) + "/xrpc/com.atproto.label.subscribeLabels",
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)

	// First event: two labels applied.
	labels1 := events[0].Labels()
	require.Len(t, labels1, 2)
	assert.Equal(t, "spam", labels1[0].Val)
	assert.False(t, labels1[0].Neg.ValOr(false))
	assert.Equal(t, "nudity", labels1[1].Val)
	assert.Equal(t, int64(1), events[0].Seq)

	// Second event: negation label.
	labels2 := events[1].Labels()
	require.Len(t, labels2, 1)
	assert.Equal(t, "spam", labels2[0].Val)
	assert.True(t, labels2[0].Neg.ValOr(false), "should be a negation label")
	assert.Equal(t, "at://did:plc:alice/app.bsky.feed.post/abc", labels2[0].URI)
	assert.Equal(t, int64(2), events[1].Seq)

	// Operations() yields nothing for label events.
	for range events[0].Operations() {
		t.Fatal("Operations() should yield nothing for label events")
	}

	assert.Equal(t, int64(2), client.Cursor())
}

func TestLabelClient_ApplyAndRemoveLabels(t *testing.T) {
	t.Parallel()

	// Simulates the lifecycle: apply labels, then negate some, then apply new
	// ones. Consumers should track effective label state.
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()

		// Step 1: Apply "spam" and "misleading" to a post.
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
			mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
				{Src: "did:plc:mod", URI: "at://did:plc:alice/app.bsky.feed.post/1", Val: "spam", Cts: "2024-01-01T00:00:00Z"},
				{Src: "did:plc:mod", URI: "at://did:plc:alice/app.bsky.feed.post/1", Val: "misleading", Cts: "2024-01-01T00:00:00Z"},
			}, 1)))

		// Step 2: Negate "spam" (false positive), keep "misleading".
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
			mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
				{Src: "did:plc:mod", URI: "at://did:plc:alice/app.bsky.feed.post/1", Val: "spam", Neg: gt.Some(true), Cts: "2024-01-01T00:01:00Z"},
			}, 2)))

		// Step 3: Apply "porn" to a different post with an expiration.
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
			mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
				{Src: "did:plc:mod", URI: "at://did:plc:bob/app.bsky.feed.post/2", Val: "porn", Cts: "2024-01-01T00:02:00Z", Exp: gt.Some("2024-02-01T00:00:00Z"), Ver: gt.Some(int64(1))},
			}, 3)))

		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv) + "/xrpc/com.atproto.label.subscribeLabels",
	})

	// Track effective labels per URI as a consumer would.
	type labelKey struct{ URI, Val string }
	effective := make(map[labelKey]bool) // true = applied, false/missing = removed

	var eventCount int
	for evt, err := range client.Events(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, lbl := range evt.Labels() {
			key := labelKey{URI: lbl.URI, Val: lbl.Val}
			if lbl.Neg.ValOr(false) {
				delete(effective, key)
			} else {
				effective[key] = true
			}
		}
		eventCount++
		if eventCount == 3 {
			cancel()
		}
	}

	require.Equal(t, 3, eventCount)

	// After processing all events:
	// - "spam" on post/1 was applied then negated → removed
	// - "misleading" on post/1 was applied → still active
	// - "porn" on post/2 was applied → still active
	assert.False(t, effective[labelKey{URI: "at://did:plc:alice/app.bsky.feed.post/1", Val: "spam"}],
		"spam should have been removed by negation")
	assert.True(t, effective[labelKey{URI: "at://did:plc:alice/app.bsky.feed.post/1", Val: "misleading"}],
		"misleading should still be active")
	assert.True(t, effective[labelKey{URI: "at://did:plc:bob/app.bsky.feed.post/2", Val: "porn"}],
		"porn should be active")
}

func TestLabelClient_InfoEvent(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		body := mustMarshalLabelInfoBody("OutdatedCursor", gt.Some("please reconnect"))
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#info", body))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv) + "/xrpc/com.atproto.label.subscribeLabels",
	})

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
	require.NotNil(t, events[0].LabelInfo)
	assert.Equal(t, "OutdatedCursor", events[0].LabelInfo.Name)
	assert.Nil(t, events[0].Labels())
}

func TestLabelClient_Reconnect(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		n := connCount.Add(1)
		ctx := context.Background()
		if n == 1 {
			// First connection: send one batch then drop.
			_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
				mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
					{Src: "did:plc:mod", URI: "at://did:plc:alice/post/1", Val: "spam", Cts: "2024-01-01T00:00:00Z"},
				}, 1)))
			_ = conn.CloseNow()
			return
		}
		// Second connection: verify cursor was sent.
		assert.Contains(t, r.URL.RawQuery, "cursor=1")
		// Send a negation as the next event.
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
			mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
				{Src: "did:plc:mod", URI: "at://did:plc:alice/post/1", Val: "spam", Neg: gt.Some(true), Cts: "2024-01-01T00:01:00Z"},
			}, 2)))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv) + "/xrpc/com.atproto.label.subscribeLabels",
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

	// First event: application.
	labels1 := events[0].Labels()
	require.Len(t, labels1, 1)
	assert.Equal(t, "spam", labels1[0].Val)
	assert.False(t, labels1[0].Neg.ValOr(false))

	// Second event (after reconnect): negation.
	labels2 := events[1].Labels()
	require.Len(t, labels2, 1)
	assert.Equal(t, "spam", labels2[0].Val)
	assert.True(t, labels2[0].Neg.ValOr(false))

	assert.GreaterOrEqual(t, connCount.Load(), int32(2))
}

func TestLabelClient_BadCBOR(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		// Valid, then garbage, then valid again.
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
			mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
				{Src: "did:plc:mod", URI: "at://u", Val: "spam", Cts: "2024-01-01T00:00:00Z"},
			}, 1)))
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{0xff, 0xfe, 0xfd})
		_ = conn.Write(ctx, websocket.MessageBinary, buildLabelFrame("#labels",
			mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
				{Src: "did:plc:mod", URI: "at://u", Val: "nudity", Cts: "2024-01-01T00:00:01Z"},
			}, 2)))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv) + "/xrpc/com.atproto.label.subscribeLabels",
	})

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
	assert.Equal(t, 1, errCount, "should have received exactly one decode error")
	assert.Equal(t, "spam", events[0].Labels()[0].Val)
	assert.Equal(t, "nudity", events[1].Labels()[0].Val)
}

func TestLabelClient_UnknownFrameSkipped(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		// Send an unknown frame type (forward compat), then a valid labels frame.
		unknown := buildLabelFrame("#newtype", cbor.AppendMapHeader(nil, 0))
		valid := buildLabelFrame("#labels", mustMarshalLabelsBody([]comatproto.LabelDefs_Label{
			{Src: "did:plc:mod", URI: "at://u", Val: "spam", Cts: "2024-01-01T00:00:00Z"},
		}, 1))
		_ = conn.Write(ctx, websocket.MessageBinary, unknown)
		_ = conn.Write(ctx, websocket.MessageBinary, valid)
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: wsURL(srv) + "/xrpc/com.atproto.label.subscribeLabels",
	})

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

	// Unknown frame should have been silently skipped.
	require.Len(t, events, 1)
	assert.Equal(t, "spam", events[0].Labels()[0].Val)
}
