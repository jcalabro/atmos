// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"net/http"
	gosync "sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// gatedStateStore wraps a MemStateStore and blocks every SaveHosting
// until released, wedging #account verifications inside their workers
// so their results stay in flight (pendingResults > 0) until the test
// chooses to let them land.
type gatedStateStore struct {
	*sync.MemStateStore
	gate   chan struct{}
	mu     gosync.Mutex
	wedged int
	notify chan struct{}
}

func (s *gatedStateStore) SaveHosting(ctx context.Context, did atmos.DID, state sync.HostingState) error {
	s.mu.Lock()
	s.wedged++
	select {
	case s.notify <- struct{}{}:
	default:
	}
	s.mu.Unlock()
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.MemStateStore.SaveHosting(ctx, did, state)
}

func (s *gatedStateStore) wedgedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wedged
}

// TestCancelDrain_ConsumerStopMidDrainDoesNotYieldAfterFalse pins the
// range-over-func contract on the graceful-cancel drain path: once the
// consumer's loop body returns false (yield dead), the drain must stop
// DELIVERING straggler verification results. The unfixed drain
// discarded handleVerifyResult's stopped return and called yield again,
// and the Go runtime panics with "range function continued iteration
// after function for loop body returned false" — the exact crash a
// jetstream CI race run hit (bluesky-social/jetstream PR #235).
//
// Shape (the panic needs the FIRST yield-false to happen INSIDE the
// drain, with more results still pending):
//
//  1. Two #account events for distinct DIDs; both verifications wedge
//     in the gated SaveHosting, so nothing is delivered and both
//     results are in flight (pendingResults == 2).
//  2. The test cancels ctx. readLoop enters drainCancelDoneResults
//     with both results pending, then the gate opens.
//  3. The first drained result reaches the consumer, whose body
//     returns on ANY delivery — yield returns false inside the drain.
//  4. The second drained result is the trap: the unfixed drain calls
//     yield again (runtime panic); the fixed drain receives it (to
//     unblock the worker) without delivering.
func TestCancelDrain_ConsumerStopMidDrainDoesNotYieldAfterFalse(t *testing.T) {
	t.Parallel()

	didA := atmos.DID("did:plc:draindaaaa")
	didB := atmos.DID("did:plc:draindbbbb")

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#account", buildAccountBody(1, string(didA), true)))
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#account", buildAccountBody(2, string(didB), true)))
		// Keep the socket open; the test drives shutdown via ctx.
		<-ctx.Done()
	})

	store := &gatedStateStore{
		MemStateStore: sync.NewMemStateStore(),
		gate:          make(chan struct{}),
		notify:        make(chan struct{}, 2),
	}
	resolver := testutil.NewTrackingResolver()
	dir := &identity.Directory{Resolver: resolver}
	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: store,
		Policy:     gt.Some(sync.PolicyError),
		SyncClient: gt.Some(sync.NewClient(sync.Options{
			Client: &xrpc.Client{Host: "https://nope.invalid"},
		})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := mustNewClient(t, Options{
		URL:      wsURL(srv),
		Verifier: gt.Some(verifier),
		// One event per batch so each drained result is its own yield:
		// the first yield kills the loop, the second is the trap.
		BatchSize:    gt.Some(1),
		BatchTimeout: gt.Some(50 * time.Millisecond),
	})

	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for _, err := range client.Events(ctx) {
			if err != nil {
				continue
			}
			// Stop on the FIRST delivered batch. Nothing is delivered
			// until the drain releases the wedged verifications, so
			// this yield-false happens inside drainCancelDoneResults
			// with the second result still pending.
			return
		}
	}()

	// Wait until BOTH verifications are wedged inside SaveHosting, so
	// pendingResults == 2 when the drain starts.
	deadline := time.After(5 * time.Second)
	for store.wedgedCount() < 2 {
		select {
		case <-store.notify:
		case <-deadline:
			t.Fatalf("verifications wedged: %d, want 2", store.wedgedCount())
		}
	}

	// Enter the graceful-cancel drain, then let both results land
	// inside its grace window.
	cancel()
	close(store.gate)

	select {
	case <-consumed:
		// The unfixed drain panics ("range function continued iteration
		// after function for loop body returned false") before reaching
		// here; reaching here green IS the assertion.
	case <-time.After(5 * time.Second):
		t.Fatal("consumer goroutine did not return after stop mid-drain")
	}
}
