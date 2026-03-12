// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Simulated clocks for deterministic lock operation timing
// ---------------------------------------------------------------------------

// yieldSleep returns after a tiny real sleep (100µs), preventing CPU
// starvation while keeping tests fast.
func yieldSleep(ctx context.Context, _ time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	time.Sleep(100 * time.Microsecond)
	return ctx.Err()
}

// ---------------------------------------------------------------------------
// Mock lock infrastructure
// ---------------------------------------------------------------------------

// mockLockState is shared state simulating a distributed lock store. Multiple
// mockLock instances point to the same state, enabling multi-node simulation.
type mockLockState struct {
	mu     sync.Mutex
	holder string // identity of current holder, "" if none
}

// mockLock implements DistributedLocker for testing.
type mockLock struct {
	state    *mockLockState
	identity string

	mu         sync.Mutex
	acquireErr error // if non-nil, Acquire returns this instead of normal logic
	renewErr   error // if non-nil, Renew returns this instead of normal logic
	releaseErr error // if non-nil, Release returns this additionally

	acquireCount atomic.Int32
	renewCount   atomic.Int32
	releaseCount atomic.Int32

	// failAcquireN causes the first N acquires to fail with ErrLockHeld.
	failAcquireN atomic.Int32
}

func newMockLockState() *mockLockState {
	return &mockLockState{}
}

func newMockLock(state *mockLockState, identity string) *mockLock {
	return &mockLock{state: state, identity: identity}
}

func (m *mockLock) Acquire(_ context.Context, _ time.Duration) error {
	m.acquireCount.Add(1)

	m.mu.Lock()
	injectedErr := m.acquireErr
	m.mu.Unlock()
	if injectedErr != nil {
		return injectedErr
	}

	if n := m.failAcquireN.Load(); n > 0 {
		m.failAcquireN.Add(-1)
		return ErrLockHeld
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.state.holder != "" && m.state.holder != m.identity {
		return ErrLockHeld
	}

	m.state.holder = m.identity
	return nil
}

func (m *mockLock) Renew(_ context.Context, _ time.Duration) error {
	m.renewCount.Add(1)

	m.mu.Lock()
	injectedErr := m.renewErr
	m.mu.Unlock()
	if injectedErr != nil {
		// Simulate losing the lock (as would happen when a real lease expires).
		m.state.mu.Lock()
		if m.state.holder == m.identity {
			m.state.holder = ""
		}
		m.state.mu.Unlock()
		return injectedErr
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.state.holder != m.identity {
		return ErrNotHolder
	}
	return nil
}

func (m *mockLock) Release(_ context.Context) error {
	m.releaseCount.Add(1)

	m.mu.Lock()
	injectedErr := m.releaseErr
	m.mu.Unlock()

	m.state.mu.Lock()
	if m.state.holder == m.identity {
		m.state.holder = ""
	}
	m.state.mu.Unlock()

	return injectedErr
}

func (m *mockLock) setAcquireErr(err error) {
	m.mu.Lock()
	m.acquireErr = err
	m.mu.Unlock()
}

func (m *mockLock) setRenewErr(err error) {
	m.mu.Lock()
	m.renewErr = err
	m.mu.Unlock()
}

func (m *mockLock) setReleaseErr(err error) {
	m.mu.Lock()
	m.releaseErr = err
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// fastLockOpts returns DistributedLockerOptions with fast timings for tests.
func fastLockOpts(locker DistributedLocker) DistributedLockerOptions {
	return DistributedLockerOptions{
		Locker:              locker,
		LeaseDuration:       500 * time.Millisecond,
		RenewalInterval:     50 * time.Millisecond,
		AcquisitionInterval: 20 * time.Millisecond,
	}
}

func fastBackoff() BackoffPolicy {
	return BackoffPolicy{
		InitialDelay: gt.Some(5 * time.Millisecond),
		MaxDelay:     gt.Some(10 * time.Millisecond),
		Multiplier:   gt.Some(1.0),
		Jitter:       gt.Some(false),
	}
}

// mustNewLeaderClient creates a client with a lock and yieldSleep for tests.
func mustNewLeaderClient(t *testing.T, opts Options) *Client {
	t.Helper()
	c, err := NewClient(opts)
	require.NoError(t, err)
	c.lockSleep = yieldSleep
	return c
}

// ---------------------------------------------------------------------------
// Single-node tests (NoopLock)
// ---------------------------------------------------------------------------

func TestNoopLock(t *testing.T) {
	t.Parallel()
	var lk NoopLock
	ctx := context.Background()
	assert.NoError(t, lk.Acquire(ctx, time.Second))
	assert.NoError(t, lk.Renew(ctx, time.Second))
	assert.NoError(t, lk.Release(ctx))
}

func TestLeaderClient_NoopLock(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: wsURL(srv)})
	assert.True(t, client.IsLeader())

	var events []Event
	for evt, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, int64(1), events[0].Seq)
	assert.Equal(t, int64(2), events[1].Seq)
}

// ---------------------------------------------------------------------------
// Single-node with mock lock
// ---------------------------------------------------------------------------

func TestLeaderClient_AcquiresBeforeConsuming(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
		)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")
	ml.failAcquireN.Store(3)

	var becameLeader atomic.Bool
	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() { becameLeader.Store(true) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, evt)
		if len(events) == 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.True(t, becameLeader.Load())
	assert.GreaterOrEqual(t, ml.acquireCount.Load(), int32(4))
}

func TestLeaderClient_StopsOnLockLoss(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var lostCount atomic.Int32
	lockOpts := fastLockOpts(ml)
	lockOpts.OnLostLeadership = func() {
		lostCount.Add(1)
		cancel() // stop the iterator immediately on lock loss
	}

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var count int
	for range client.Events(ctx) {
		count++
		if count == 3 {
			ml.setRenewErr(ErrNotHolder)
		}
	}

	assert.Equal(t, int32(1), lostCount.Load(), "OnLostLeadership should be called exactly once")
	assert.GreaterOrEqual(t, count, 3)
}

func TestLeaderClient_ReacquiresAfterLoss(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		n := connCount.Add(1)
		ctx := context.Background()
		if n == 1 {
			for i := int64(1); i <= 3; i++ {
				_ = conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
			}
			// Keep alive until disconnected.
			for {
				if err := conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(50, "did:plc:filler"))); err != nil {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}
		// Reconnection: send more events.
		for i := int64(100); i <= 102; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	var becameCount atomic.Int32
	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() { becameCount.Add(1) }
	lockOpts.OnLostLeadership = func() {
		// Allow reacquisition after loss.
		ml.setRenewErr(nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var gotFirst3 bool
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		if evt.Seq == 3 && !gotFirst3 {
			gotFirst3 = true
			ml.setRenewErr(ErrNotHolder) // lose lock after first batch
		}
		if evt.Seq >= 100 {
			cancel()
			break
		}
	}

	assert.GreaterOrEqual(t, becameCount.Load(), int32(2))
}

func TestLeaderClient_GracefulShutdown(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	var lostLeadership atomic.Bool
	lockOpts := fastLockOpts(ml)
	lockOpts.OnLostLeadership = func() { lostLeadership.Store(true) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var count int
	for range client.Events(ctx) {
		count++
		if count >= 3 {
			cancel()
		}
	}

	assert.GreaterOrEqual(t, count, 3)
	assert.True(t, lostLeadership.Load())
	assert.GreaterOrEqual(t, ml.releaseCount.Load(), int32(1))
}

func TestLeaderClient_ContextCancelDuringAcquisition(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		<-time.After(10 * time.Second)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")
	ml.setAcquireErr(ErrLockHeld)

	ctx, cancel := context.WithCancel(context.Background())

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(fastLockOpts(ml)),
	})

	// Cancel after a few acquire attempts via goroutine, since the
	// for-range blocks until yield is called (which never happens here).
	go func() {
		for ml.acquireCount.Load() < 3 {
			runtime.Gosched()
		}
		cancel()
	}()

	var count int
	for range client.Events(ctx) {
		count++
	}

	assert.Equal(t, 0, count)
	assert.GreaterOrEqual(t, ml.acquireCount.Load(), int32(3))
}

func TestLeaderClient_PanicReleasesLock(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
		)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(fastLockOpts(ml)),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	assert.Panics(t, func() {
		for range client.Events(ctx) {
			panic("boom")
		}
	})

	assert.GreaterOrEqual(t, ml.releaseCount.Load(), int32(1))
}

func TestLeaderClient_CursorPreservedAcrossLockCycles(t *testing.T) {
	t.Parallel()

	var conns []string
	var connsMu sync.Mutex

	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		connsMu.Lock()
		conns = append(conns, r.URL.RawQuery)
		connsMu.Unlock()

		ctx := context.Background()
		cursor := r.URL.Query().Get("cursor")
		if cursor == "" {
			for i := int64(1); i <= 3; i++ {
				_ = conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
			}
			for {
				if err := conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(50, "did:plc:filler"))); err != nil {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}
		for i := int64(100); i <= 102; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	lockOpts := fastLockOpts(ml)
	lockOpts.OnLostLeadership = func() { ml.setRenewErr(nil) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var gotFirst3 bool
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		if evt.Seq == 3 && !gotFirst3 {
			gotFirst3 = true
			ml.setRenewErr(ErrNotHolder)
		}
		if evt.Seq >= 100 {
			cancel()
			break
		}
	}

	connsMu.Lock()
	defer connsMu.Unlock()
	require.GreaterOrEqual(t, len(conns), 2)
	found := false
	for _, q := range conns[1:] {
		if q != "" {
			found = true
			assert.Contains(t, q, "cursor=")
		}
	}
	assert.True(t, found, "reconnection should include cursor parameter")
}

func TestLeaderClient_IsLeader(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	var becameLeader atomic.Bool
	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() { becameLeader.Store(true) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	assert.False(t, client.IsLeader())

	sawLeaderTrue := false
	for range client.Events(ctx) {
		if becameLeader.Load() && client.IsLeader() {
			sawLeaderTrue = true
			cancel()
			break
		}
	}
	assert.True(t, sawLeaderTrue)
}

func TestLeaderClient_Close(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
		)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(fastLockOpts(ml)),
	})

	// Breaking out of the range loop terminates the iterator, which runs
	// the deferred releaseOnShutdown.
	for range client.Events(ctx) {
		break
	}

	assert.GreaterOrEqual(t, ml.releaseCount.Load(), int32(1),
		"lock should be released when Events iterator terminates")

	_ = client.Close()
}

func TestLeaderClient_NilLockerReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Options{
		URL:    "wss://example.com",
		Locker: gt.Some(DistributedLockerOptions{}),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Locker must not be nil")
}

func TestLeaderClient_SequentialEventsCalls(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		n := connCount.Add(1)
		if n == 1 {
			writeFrames(conn,
				buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
				buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
			)
			return
		}
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(3, "did:plc:carol")),
			buildFrame("#identity", buildIdentityBody(4, "did:plc:dave")),
		)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	var becameCount atomic.Int32
	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() { becameCount.Add(1) }

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	// First Events() call — consume events 1-2.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	var firstBatch []Event
	for evt, err := range client.Events(ctx1) {
		require.NoError(t, err)
		firstBatch = append(firstBatch, evt)
		if len(firstBatch) == 2 {
			cancel1()
		}
	}
	require.Len(t, firstBatch, 2)

	// Second Events() call — should reacquire and consume events 3-4.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	var secondBatch []Event
	for evt, err := range client.Events(ctx2) {
		require.NoError(t, err)
		secondBatch = append(secondBatch, evt)
		if len(secondBatch) == 2 {
			cancel2()
		}
	}
	require.Len(t, secondBatch, 2)

	// Lock was acquired twice (once per Events call).
	assert.Equal(t, int32(2), becameCount.Load())
}

// ---------------------------------------------------------------------------
// Multi-node simulation
// ---------------------------------------------------------------------------

func TestLeaderClient_TwoNodes_Handoff(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	mlA := newMockLock(state, "node-A")
	mlB := newMockLock(state, "node-B")

	var (
		nodeABecame atomic.Bool
		nodeALost   atomic.Bool
		nodeBBecame atomic.Bool
	)

	lockOptsA := fastLockOpts(mlA)
	lockOptsA.OnBecameLeader = func() { nodeABecame.Store(true) }
	lockOptsA.OnLostLeadership = func() { nodeALost.Store(true) }

	lockOptsB := fastLockOpts(mlB)
	lockOptsB.OnBecameLeader = func() { nodeBBecame.Store(true) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clientA := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOptsA),
	})
	clientB := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOptsB),
	})

	nodeAEvents := make(chan Event, 100)
	nodeBEvents := make(chan Event, 100)

	// Start node A first and wait for events before starting B to avoid
	// racing for initial lock acquisition. Use non-blocking sends so a
	// fast relay never deadlocks the iterator.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for evt, err := range clientA.Events(ctx) {
			if err == nil {
				select {
				case nodeAEvents <- evt:
				default:
				}
			}
		}
	}()

	// Wait for node A to become leader and consume some events.
	for range 3 {
		select {
		case <-nodeAEvents:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for node A events")
		}
	}
	require.True(t, nodeABecame.Load())

	// Now start node B (it will block in waitForLock since A holds the lock).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for evt, err := range clientB.Events(ctx) {
			if err == nil {
				select {
				case nodeBEvents <- evt:
				default:
				}
			}
		}
	}()

	// Make node A lose the lock and prevent it from re-acquiring.
	mlA.setAcquireErr(ErrLockHeld)
	mlA.setRenewErr(ErrNotHolder)

	// Wait for B to become leader.
	select {
	case <-nodeBEvents:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for node B events")
	}

	cancel()
	wg.Wait()

	assert.True(t, nodeABecame.Load())
	assert.True(t, nodeALost.Load())
	assert.True(t, nodeBBecame.Load())
}

func TestLeaderClient_MultiNode_OnlyOneConsumes(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	const numNodes = 3

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var (
		wg     sync.WaitGroup
		events [numNodes]atomic.Int32
	)

	for i := range numNodes {
		ml := newMockLock(state, fmt.Sprintf("node-%d", i))
		client := mustNewLeaderClient(t, Options{
			URL:     wsURL(srv),
			Backoff: gt.Some(fastBackoff()),
			Locker:  gt.Some(fastLockOpts(ml)),
		})

		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range client.Events(ctx) {
				events[idx].Add(1)
			}
		}()
	}

	// Wait for some events to be consumed, then stop.
	for {
		total := int32(0)
		for i := range numNodes {
			total += events[i].Load()
		}
		if total >= 10 {
			break
		}
		runtime.Gosched()
	}
	cancel()
	wg.Wait()

	consumingNodes := 0
	for i := range numNodes {
		if events[i].Load() > 0 {
			consumingNodes++
		}
	}
	assert.Equal(t, 1, consumingNodes)
}

// ---------------------------------------------------------------------------
// Edge case / liveness tests
// ---------------------------------------------------------------------------

func TestLeaderClient_AcquireFailsThenSucceeds(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
		)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")
	ml.setAcquireErr(errors.New("connection refused"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(fastLockOpts(ml)),
	})

	// Clear error after a few attempts from a goroutine (since the
	// for-range loop blocks while waitForLock is polling).
	go func() {
		for ml.acquireCount.Load() < 5 {
			runtime.Gosched()
		}
		ml.setAcquireErr(nil)
	}()

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
	assert.GreaterOrEqual(t, ml.acquireCount.Load(), int32(5))
}

func TestLeaderClient_RenewFailsOnce_Reacquires(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		n := connCount.Add(1)
		ctx := context.Background()
		if n == 1 {
			for i := int64(1); i <= 5; i++ {
				_ = conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
			}
			for {
				if err := conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(50, "did:plc:filler"))); err != nil {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}
		for i := int64(100); i <= 105; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	lockOpts := fastLockOpts(ml)
	lockOpts.OnLostLeadership = func() { ml.setRenewErr(nil) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	seenSeqs := make(map[int64]bool)
	var gotFirst5 bool
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		seenSeqs[evt.Seq] = true
		if evt.Seq >= 5 && !gotFirst5 {
			gotFirst5 = true
			ml.setRenewErr(ErrNotHolder) // lose lock
		}
		if evt.Seq >= 100 {
			cancel()
			break
		}
	}

	for i := int64(1); i <= 5; i++ {
		assert.True(t, seenSeqs[i], "event %d should have been delivered", i)
	}
	assert.True(t, seenSeqs[100])
}

func TestLeaderClient_RapidLockCycling(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	var becameCount, lostCount atomic.Int32

	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() { becameCount.Add(1) }
	lockOpts.OnLostLeadership = func() {
		lostCount.Add(1)
		// Allow reacquisition immediately.
		ml.setRenewErr(nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var eventCount int
	for range client.Events(ctx) {
		eventCount++
		// Trigger lock loss every 5 events to create rapid cycling.
		if eventCount%5 == 0 {
			ml.setRenewErr(ErrNotHolder)
		}
		if becameCount.Load() >= 4 {
			cancel()
			break
		}
	}

	assert.GreaterOrEqual(t, becameCount.Load(), int32(4))
	assert.GreaterOrEqual(t, lostCount.Load(), int32(3))
	// 4 cycles × 5 events per cycle = at least 15 events expected (some
	// extra events may arrive during lock transitions).
	assert.GreaterOrEqual(t, eventCount, 15)
}

func TestLeaderClient_ReleaseFails_StillReacquires(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); ; i++ {
			if err := conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test"))); err != nil {
				return
			}
		}
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")
	ml.setReleaseErr(errors.New("release failed"))

	var becameCount atomic.Int32
	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() { becameCount.Add(1) }
	lockOpts.OnLostLeadership = func() { ml.setRenewErr(nil) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var eventCount int
	for range client.Events(ctx) {
		eventCount++
		if eventCount == 3 {
			ml.setRenewErr(ErrNotHolder) // lose lock (release will also fail)
		}
		if becameCount.Load() >= 2 && eventCount >= 5 {
			cancel()
			break
		}
	}

	assert.GreaterOrEqual(t, becameCount.Load(), int32(2))
	assert.Greater(t, eventCount, 0)
}

func TestLeaderClient_AllEventsDelivered(t *testing.T) {
	t.Parallel()

	const totalEvents = 50

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
		n := connCount.Add(1)
		ctx := context.Background()

		start := int64(1)
		if cursor := r.URL.Query().Get("cursor"); cursor != "" {
			var c int64
			if _, err := fmt.Sscanf(cursor, "%d", &c); err == nil {
				start = c + 1
			}
		}

		if n == 1 {
			for i := start; i <= totalEvents; i++ {
				_ = conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
			}
			for {
				if err := conn.Write(ctx, websocket.MessageBinary,
					buildFrame("#identity", buildIdentityBody(totalEvents+99, "did:plc:filler"))); err != nil {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}
		for i := start; i <= totalEvents; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:test")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	lockOpts := fastLockOpts(ml)
	lockOpts.OnLostLeadership = func() { ml.setRenewErr(nil) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	seenSeqs := make(map[int64]bool)
	triggerLoss := false
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		if evt.Seq > 0 && evt.Seq <= totalEvents {
			seenSeqs[evt.Seq] = true
		}
		// Trigger one lock loss mid-stream.
		if evt.Seq == 25 && !triggerLoss {
			triggerLoss = true
			ml.setRenewErr(ErrNotHolder)
		}
		if int64(len(seenSeqs)) >= totalEvents {
			cancel()
			break
		}
	}

	for i := int64(1); i <= totalEvents; i++ {
		assert.True(t, seenSeqs[i], "event %d should have been delivered (at-least-once)", i)
	}
}

func TestLeaderClient_LockLossDuringBackoff(t *testing.T) {
	t.Parallel()

	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		n := connCount.Add(1)
		if n == 1 {
			_ = conn.CloseNow() // close immediately to trigger backoff
			return
		}
		writeFrames(conn,
			buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")),
			buildFrame("#identity", buildIdentityBody(2, "did:plc:bob")),
		)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")
	ml.setRenewErr(ErrNotHolder) // lose lock immediately (during backoff)

	var becameCount atomic.Int32
	lockOpts := fastLockOpts(ml)
	lockOpts.OnBecameLeader = func() {
		if becameCount.Add(1) >= 2 {
			ml.setRenewErr(nil) // let second acquisition hold
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(lockOpts),
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) >= 2 {
			cancel()
		}
	}

	assert.GreaterOrEqual(t, len(events), 2)
	assert.GreaterOrEqual(t, becameCount.Load(), int32(2))
}

func TestLeaderClient_ImmediateContextCancel(t *testing.T) {
	t.Parallel()

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		<-time.After(10 * time.Second)
	})

	state := newMockLockState()
	ml := newMockLock(state, "node-1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	client := mustNewLeaderClient(t, Options{
		URL:     wsURL(srv),
		Backoff: gt.Some(fastBackoff()),
		Locker:  gt.Some(fastLockOpts(ml)),
	})

	var count int
	for range client.Events(ctx) {
		count++
	}

	assert.Equal(t, 0, count)
}
