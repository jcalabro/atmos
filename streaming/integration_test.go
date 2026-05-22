// Integration tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
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

	// Parallelism=1: this test asserts strict cross-DID ordering by
	// index (events[0] is alice, [1] is bob, etc.).
	client := mustNewClient(t, Options{URL: wsURL(srv), Parallelism: gt.Some(1)})

	var events []Event
	for batch, err := range client.Events(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, batch...)
		if len(events) >= 5 {
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
	for batch, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, batch...)
		if len(events) >= 5 {
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
	for batch, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, batch...)
		if len(events) >= 1 {
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

// TestIntegration_SyncEvent_EndToEnd tests the full pipeline: WebSocket sends
// a #sync frame → readLoop stamps ctx/syncClient → consumer calls Operations()
// → gets ActionResync operations from the HTTP-fetched repo.
func TestIntegration_SyncEvent_EndToEnd(t *testing.T) {
	t.Parallel()

	// Build a test repo and serve it over HTTP.
	carData := buildResyncRepo(t, 3)
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carData)
	}))
	t.Cleanup(httpSrv.Close)

	xc := &xrpc.Client{Host: httpSrv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	// Build a #sync CBOR frame.
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#sync",
		DID:           "did:plc:test123",
		Rev:           "3abc",
		Seq:           1,
		Time:          "2024-01-01T00:00:00Z",
	}
	syncBody, err := syncEvt.MarshalCBOR()
	require.NoError(t, err)

	// WebSocket relay that sends the #sync frame.
	wsSrv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, buildFrame("#sync", syncBody))
	})

	client := mustNewClient(t, Options{
		URL:        wsURL(wsSrv),
		BatchSize:  gt.Some(1),
		SyncClient: gt.Some(sc),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ops []Operation
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		for _, evt := range batch {
			for op, err := range evt.Operations() {
				require.NoError(t, err)
				ops = append(ops, op)
			}
		}
		cancel() // only process one event
	}

	require.Len(t, ops, 3)
	for _, op := range ops {
		assert.Equal(t, ActionResync, op.Action)
		assert.Equal(t, "app.bsky.feed.post", string(op.Collection))
		assert.Equal(t, "did:plc:test123", string(op.Repo))
		assert.Equal(t, "3abc", string(op.Rev))
		assert.True(t, op.CID.Defined(), "resync op CID must be defined")
	}
}

func TestReadLoopParallel_DeliversAllEvents(t *testing.T) {
	t.Parallel()

	const N = 20
	const Workers = 4

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		frames := make([][]byte, 0, N)
		// Use a unique DID per event to avoid per-DID gaps (each DID
		// only sees one seq, so no per-DID gap can fire).
		for i := int64(1); i <= N; i++ {
			did := fmt.Sprintf("did:plc:u%d", i)
			frames = append(frames, buildFrame("#identity", buildIdentityBody(i, did)))
		}
		writeFrames(conn, frames...)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Parallelism: gt.Some(Workers),
	})

	seen := make(map[int64]bool)
	for batch, err := range client.Events(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, evt := range batch {
			if evt.Seq > 0 {
				seen[evt.Seq] = true
			}
		}
		if len(seen) >= N {
			cancel()
		}
	}
	require.Len(t, seen, N)
}

// TestReadLoop_StrictOrderingAtN1 pins the cross-DID strict-ordering
// guarantee at Parallelism=1: events arrive in the exact order the
// relay sent them, regardless of DID. Under Parallelism > 1 this
// invariant relaxes (per-DID order only), so this test guards the N=1
// escape hatch from accidentally inheriting the parallel ordering
// relaxation.
func TestReadLoop_StrictOrderingAtN1(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
			buildFrame("#identity", buildIdentityBody(3, "did:plc:carol")),
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Parallelism: gt.Some(1),
	})

	var seqs []int64
	for batch, err := range client.Events(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, evt := range batch {
			if evt.Seq > 0 {
				seqs = append(seqs, evt.Seq)
			}
		}
		if len(seqs) >= 3 {
			cancel()
		}
	}
	require.Equal(t, []int64{1, 2, 3}, seqs)
}

func TestReadLoopParallel_GapError(t *testing.T) {
	t.Parallel()

	// Skip seq 3 to create a global gap.
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:a")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:b")),
			buildFrame("#identity", buildIdentityBody(4, "did:plc:c")), // gap
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Parallelism: gt.Some(2),
	})

	var gapErr *GapError
	count := 0
	for batch, err := range client.Events(ctx) {
		if err != nil {
			var ge *GapError
			if stderrors.As(err, &ge) {
				gapErr = ge
			}
			continue
		}
		count += len(batch)
		if gapErr != nil && count >= 3 {
			cancel()
		}
	}
	require.NotNil(t, gapErr, "expected GapError")
	require.Equal(t, int64(3), gapErr.Expected)
	require.Equal(t, int64(4), gapErr.Got)
}

// TestReadLoop_PreErrorEventsDeliveredFirst pins down a contract that
// the unified readLoop must honor: events that arrived BEFORE a
// DecodeError or GapError must reach the consumer before the error.
// Pre-unification this fell out trivially at N=1 (synchronous decode,
// no scheduler). Post-unification all events route through the
// scheduler, so the dispatch goroutine must drain in-flight results
// before yielding errors. This test exercises the N>1 path explicitly
// to cover the contract for both single and multi-worker configs;
// TestBatch_GapBreaksBatch covers N=1.
func TestReadLoop_PreErrorEventsDeliveredFirst(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:a")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:b")),
			buildFrame("#identity", buildIdentityBody(5, "did:plc:c")), // gap (expected 3)
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		Parallelism:  gt.Some(4),
		BatchSize:    gt.Some(100),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	// Track every yielded thing in order: events under "evt-{seq}" tags,
	// errors as "gap" or "decode". The contract is that "evt-1" and
	// "evt-2" must appear before "gap" in the timeline.
	var timeline []string
	for batch, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			var ge *GapError
			if stderrors.As(iterErr, &ge) {
				timeline = append(timeline, "gap")
			} else {
				timeline = append(timeline, "err")
			}
			continue
		}
		for _, evt := range batch {
			timeline = append(timeline, fmt.Sprintf("evt-%d", evt.Seq))
		}
		if hasAll(timeline, "evt-1", "evt-2", "gap") {
			cancel()
		}
	}

	gapIdx := indexOf(timeline, "gap")
	require.GreaterOrEqual(t, gapIdx, 0, "must have observed a GapError; timeline=%v", timeline)
	evt1 := indexOf(timeline, "evt-1")
	evt2 := indexOf(timeline, "evt-2")
	require.GreaterOrEqual(t, evt1, 0, "evt-1 missing; timeline=%v", timeline)
	require.GreaterOrEqual(t, evt2, 0, "evt-2 missing; timeline=%v", timeline)
	require.Less(t, evt1, gapIdx, "evt-1 must precede GapError; timeline=%v", timeline)
	require.Less(t, evt2, gapIdx, "evt-2 must precede GapError; timeline=%v", timeline)
}

func indexOf(s []string, target string) int {
	for i, v := range s {
		if v == target {
			return i
		}
	}
	return -1
}

func hasAll(s []string, targets ...string) bool {
	for _, t := range targets {
		if indexOf(s, t) < 0 {
			return false
		}
	}
	return true
}
