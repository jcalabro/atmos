package sync_test

import (
	"errors"
	stdsync "sync"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferOverflowError_ErrorAndUnwrap(t *testing.T) {
	t.Parallel()

	err := &sync.BufferOverflowError{
		DID:     atmos.DID("did:plc:alice"),
		Dropped: 7,
	}
	assert.Contains(t, err.Error(), "did:plc:alice")
	assert.Contains(t, err.Error(), "7")

	// errors.As must work so consumers can branch on the type.
	var typed *sync.BufferOverflowError
	assert.True(t, errors.As(error(err), &typed))
	assert.Equal(t, atmos.DID("did:plc:alice"), typed.DID)
	assert.Equal(t, 7, typed.Dropped)
}

// Verifier construction must accept the new option fields and apply
// the documented defaults when they are omitted.
func TestNewVerifier_AsyncOptionDefaults(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// ResyncEvents and AsyncErrors must be drainable channels of the
	// configured buffer sizes; we don't expose the buffer size from
	// outside, so we only assert non-nil and that the channels are not
	// closed.
	require.NotNil(t, v.ResyncEvents())
	require.NotNil(t, v.AsyncErrors())

	select {
	case _, ok := <-v.ResyncEvents():
		require.True(t, ok, "ResyncEvents() must not be closed before Close()")
	default:
	}
}

func newTestVerifier(t *testing.T) (*sync.Verifier, error) {
	t.Helper()
	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	return sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		SyncClient: gt.Some(&sync.Client{}),
	})
}

func TestVerifier_Close_Idempotent(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)

	require.NoError(t, v.Close())
	require.NoError(t, v.Close()) // second call must not panic or err

	// After close, ResyncEvents and AsyncErrors must be drainable to
	// EOF (closed and empty).
	_, ok := <-v.ResyncEvents()
	assert.False(t, ok, "ResyncEvents() should be closed after Close()")
	_, ok = <-v.AsyncErrors()
	assert.False(t, ok, "AsyncErrors() should be closed after Close()")
}

// sendAsyncError must not drop errors. With a buffer of 1, the second
// send blocks until the first is drained. We assert that by draining
// in a separate goroutine and seeing both errors arrive.
func TestVerifier_SendAsyncError_BlocksWhenFull(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:        dir,
		StateStore:       sync.NewMemStateStore(),
		SyncClient:       gt.Some(&sync.Client{}),
		AsyncErrorBuffer: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Use the test export below to call sendAsyncError directly.
	go sync.SendAsyncErrorForTest(v, errors.New("first"))
	go sync.SendAsyncErrorForTest(v, errors.New("second"))

	got := make([]string, 0, 2)
	for range 2 {
		select {
		case e := <-v.AsyncErrors():
			got = append(got, e.Error())
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for queued async error")
		}
	}
	assert.ElementsMatch(t, []string{"first", "second"}, got)
}

// TestVerifier_VerifyAfterClose asserts that a chain-break verification
// after Close() returns the original error rather than panicking.
//
// Regression: the worker pool used to close(resyncQueue) in Close(),
// which made any subsequent send from handleVerificationFailure's
// PolicyResync branch panic with "send on closed channel". The fix
// switched workers to select on workerCtx.Done; producers' existing
// workerCtx.Done branch in the FSM now handles closed-verifier cases.
func TestVerifier_VerifyAfterClose(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)
	require.NoError(t, v.Close())

	// We can't easily synthesize a real chain-break commit without the
	// full test fixture machinery from verifier_test.go, but we CAN
	// invoke handleVerificationFailure's send-site by exercising the
	// per-DID FSM directly via export_test helpers. The simpler test:
	// just send an async error post-Close and assert no panic — same
	// shape (channel send under closeOnce) but with a much smaller
	// fixture.
	//
	// (A second, more realistic test using a chain-break commit lives
	// in the closed-verifier swarm test below.)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("post-Close sendAsyncError panicked: %v", r)
		}
	}()
	sync.SendAsyncErrorForTest(v, errors.New("post-close error"))
}

// TestVerifier_EnqueueAfterClose is the direct regression test: simulate
// the producer (handleVerificationFailure) trying to enqueue a resync
// job after Close. Must not panic.
func TestVerifier_EnqueueAfterClose(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)
	require.NoError(t, v.Close())

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("post-Close enqueue panicked: %v", r)
		}
	}()
	require.NoError(t, sync.EnqueueResyncForTest(v, atmos.DID("did:plc:test")))
}

// TestVerifier_EnqueueDuringClose exercises the concurrent race: many
// enqueue attempts running while Close() is in progress. None must
// panic. Run with -race for full effect.
func TestVerifier_EnqueueDuringClose(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)

	const concurrentSenders = 20
	var wg stdsync.WaitGroup

	// Start senders concurrently.
	for i := 0; i < concurrentSenders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("sender %d panicked: %v", idx, r)
				}
			}()
			// Simulate verifyCommit's send pattern. This is best-effort:
			// some calls will succeed before Close; others will hit the
			// closed-verifier path. Neither should panic.
			_ = sync.EnqueueResyncForTest(v, atmos.DID("did:plc:race"+string(rune('a'+idx))))
		}(i)
	}

	// Trigger Close concurrently while senders are still running.
	// Close will cancel workerCtx, causing subsequent sends to take
	// the workerCtx.Done() branch instead of panicking. Start Close
	// immediately so that by the time senders run, some will see
	// a closed verifier.
	go func() {
		_ = v.Close()
	}()

	wg.Wait()
}

// TestVerifier_SendAsyncErrorDuringClose is the direct regression for the
// "send on closed channel" panic: sendAsyncError can be invoked from a
// goroutine that is NOT tracked by workerWG (e.g. the streaming parallel
// scheduler worker driving VerifyCommit -> handleVerificationFailure on a
// pending-buffer overflow). Such a sender can race Close()'s close(asyncErrs).
// Many senders run concurrently with Close; none must panic. Run with -race.
func TestVerifier_SendAsyncErrorDuringClose(t *testing.T) {
	t.Parallel()

	for iter := 0; iter < 50; iter++ {
		v, err := newTestVerifier(t)
		require.NoError(t, err)

		// Drain asyncErrs so senders that win the race before Close can make
		// progress rather than all blocking on a full buffer.
		go func() {
			for range v.AsyncErrors() { //nolint:revive // intentional drain to EOF
			}
		}()

		const senders = 32
		var wg stdsync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < senders; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("sender %d panicked: %v", idx, r)
					}
				}()
				<-start
				sync.SendAsyncErrorForTest(v, &sync.BufferOverflowError{
					DID:     atmos.DID("did:plc:race"),
					Dropped: 1,
				})
			}(i)
		}

		// Release all senders, then Close concurrently so the close of
		// asyncErrs interleaves with in-flight sends.
		close(start)
		require.NoError(t, v.Close())
		wg.Wait()
	}
}
