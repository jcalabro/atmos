// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch_InvalidBatchSize(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Options{
		URL:       "wss://example.com",
		BatchSize: gt.Some(0),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BatchSize")

	_, err = NewClient(Options{
		URL:       "wss://example.com",
		BatchSize: gt.Some(-1),
	})
	require.Error(t, err)
}

func TestBatch_InvalidBatchTimeout(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Options{
		URL:          "wss://example.com",
		BatchTimeout: gt.Some(time.Duration(0)),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BatchTimeout")

	_, err = NewClient(Options{
		URL:          "wss://example.com",
		BatchTimeout: gt.Some(-time.Second),
	})
	require.Error(t, err)
}

func TestBatch_FullBatch(t *testing.T) {
	t.Parallel()
	const batchSize = 5
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= batchSize; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		// Keep connection open so the batch flushes via size, not connection close.
		time.Sleep(100 * time.Millisecond)
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(99, "did:plc:test")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(batchSize),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	var firstBatchLen int
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		firstBatchLen = len(batch)
		cancel()
		break
	}
	assert.Equal(t, batchSize, firstBatchLen)
}

func TestBatch_TimeoutFlush(t *testing.T) {
	t.Parallel()
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		<-ctx.Done()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(100),
		BatchTimeout: gt.Some(50 * time.Millisecond),
	})

	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		assert.Len(t, batch, 2)
		cancel()
		break
	}
}

func TestBatch_ErrorBreaksBatch(t *testing.T) {
	t.Parallel()
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{0xff, 0xfe, 0xfd})
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(3, "did:plc:carol")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(100),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	var (
		batches [][]Event
		errs    []error
	)
	for batch, err := range client.Events(ctx) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		batches = append(batches, batch)
		totalEvents := 0
		for _, b := range batches {
			totalEvents += len(b)
		}
		if totalEvents >= 3 {
			cancel()
		}
	}

	require.GreaterOrEqual(t, len(batches), 1)
	assert.Equal(t, int64(1), batches[0][0].Seq)
	require.Len(t, errs, 1)
	var decErr *DecodeError
	require.True(t, errors.As(errs[0], &decErr))
}

func TestBatch_GapBreaksBatch(t *testing.T) {
	t.Parallel()
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(5, "did:plc:carol")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(100),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	var (
		batches [][]Event
		gaps    []*GapError
	)
	for batch, err := range client.Events(ctx) {
		if err != nil {
			if ge, ok := errors.AsType[*GapError](err); ok {
				gaps = append(gaps, ge)
			}
			continue
		}
		batches = append(batches, batch)
		totalEvents := 0
		for _, b := range batches {
			totalEvents += len(b)
		}
		if totalEvents >= 3 {
			cancel()
		}
	}

	require.GreaterOrEqual(t, len(batches), 1)
	assert.Len(t, batches[0], 2, "first batch should contain events before gap")
	assert.Equal(t, int64(1), batches[0][0].Seq)
	assert.Equal(t, int64(2), batches[0][1].Seq)
	require.Len(t, gaps, 1)
	assert.Equal(t, int64(3), gaps[0].Expected)
	assert.Equal(t, int64(5), gaps[0].Got)
}

func TestBatch_ConnectionLossFlushes(t *testing.T) {
	t.Parallel()
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")))
		_ = conn.CloseNow()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(100),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	var events []Event
	for batch, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, batch...)
		if len(events) >= 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, int64(1), events[0].Seq)
	assert.Equal(t, int64(2), events[1].Seq)
}

func TestBatch_SingleEvent(t *testing.T) {
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
		URL:       wsURL(srv),
		BatchSize: gt.Some(1),
	})

	var batchSizes []int
	var events []Event
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		batchSizes = append(batchSizes, len(batch))
		events = append(events, batch...)
		if len(events) >= 3 {
			cancel()
		}
	}

	require.Len(t, events, 3)
	for _, size := range batchSizes {
		assert.Equal(t, 1, size, "every batch should contain exactly one event")
	}
}

func TestBatch_EmptyBatchNotYielded(t *testing.T) {
	t.Parallel()
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(100),
		BatchTimeout: gt.Some(50 * time.Millisecond),
	})

	var yields int
	for batch, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		yields++
		assert.NotEmpty(t, batch, "should never yield an empty batch")
		cancel()
	}
	assert.Equal(t, 1, yields)
}

func TestBatch_CursorAfterBatch(t *testing.T) {
	t.Parallel()
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= 10; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		time.Sleep(100 * time.Millisecond)
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(99, "did:plc:test")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(5),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	assert.Equal(t, int64(0), client.Cursor())

	// The cursor is updated *after* yield returns, so we check the cursor
	// from the previous batch at the start of each iteration.
	var batchCount int
	var cursorsAfterYield []int64
	for _, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		batchCount++
		// Record the cursor value visible at the start of each yield.
		// Because flushBatch stores the cursor after yield returns, the
		// cursor observed here reflects the *previous* batch.
		cursorsAfterYield = append(cursorsAfterYield, client.Cursor())
		if batchCount >= 2 {
			cancel()
		}
	}
	assert.GreaterOrEqual(t, batchCount, 2)
	// After the loop completes, the cursor should reflect the last batch.
	// First yield sees cursor 0 (nothing stored yet), second yield sees 5
	// (first batch's cursor stored after first yield returned).
	assert.Equal(t, int64(0), cursorsAfterYield[0], "cursor during first yield")
	assert.Equal(t, int64(5), cursorsAfterYield[1], "cursor during second yield")
	assert.Equal(t, int64(10), client.Cursor(), "cursor after loop")
}

func TestBatch_MultipleBatches(t *testing.T) {
	t.Parallel()
	const batchSize = 3
	const totalEvents = 8

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= totalEvents; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(batchSize),
		BatchTimeout: gt.Some(5 * time.Second),
	})

	var batches [][]Event
	var events []Event
	for batch, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		batches = append(batches, batch)
		events = append(events, batch...)
		if len(events) >= totalEvents {
			cancel()
		}
	}

	require.Len(t, events, totalEvents)
	assert.GreaterOrEqual(t, len(batches), 2)
	for i, evt := range events {
		assert.Equal(t, int64(i+1), evt.Seq)
	}
}

func TestBatch_CursorPersistenceWithBatches(t *testing.T) {
	t.Parallel()

	store := &mockCursorStore{}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= 12; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// BatchSize=3, CursorInterval=5, 12 events.
	// Batches: [1,2,3], [4,5,6], [7,8,9], [10,11,12]
	// cursorCount after each batch: 3, 6, 9, 12
	// Saves when cursorCount/5 > prevCount/5:
	//   batch 1: 3/5=0 > 0/5=0 → false
	//   batch 2: 6/5=1 > 3/5=0 → true (save at cursor=6)
	//   batch 3: 9/5=1 > 6/5=1 → false
	//   batch 4: 12/5=2 > 9/5=1 → true (save at cursor=12)
	// Total saves: 2
	client, err := NewClient(Options{
		URL:            wsURL(srv),
		CursorStore:    gt.Some[CursorStore](store),
		CursorInterval: gt.Some(int64(5)),
		BatchSize:      gt.Some(3),
		BatchTimeout:   gt.Some(5 * time.Second),
	})
	require.NoError(t, err)

	var events []Event
	for batch, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, batch...)
		if len(events) >= 12 {
			cancel()
		}
	}

	require.Len(t, events, 12)

	store.mu.Lock()
	saveCount := store.saveCount
	savedCursor := store.cursor
	store.mu.Unlock()

	assert.Equal(t, 2, saveCount, "should save cursor twice (at events 6 and 12)")
	assert.Equal(t, int64(12), savedCursor)
}
