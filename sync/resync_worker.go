package sync

import (
	"github.com/jcalabro/atmos"
)

// ResyncEvents returns the channel of completed async resync events.
// Drain promptly: workers block on send when this channel is full
// (buffer size from VerifierOptions.ResyncEventBuffer; default
// DefaultResyncEventBuffer).
//
// Closed by Verifier.Close() after all in-flight workers exit.
func (v *Verifier) ResyncEvents() <-chan ResyncEvent {
	return v.resyncDone
}

// AsyncErrors returns the channel of errors produced by background
// resync workers: *ResyncFailedError, *ResyncRateLimitedError,
// *BufferOverflowError, and wrapped infra errors. Drain promptly;
// workers block on send when this channel is full.
//
// Closed by Verifier.Close() after all in-flight workers exit.
func (v *Verifier) AsyncErrors() <-chan error {
	return v.asyncErrs
}

// Close stops the async-resync worker pool and closes ResyncEvents
// and AsyncErrors. Safe to call multiple times. Outstanding jobs in
// the queue are abandoned; in-flight workers' contexts are cancelled
// (so a stuck getRepo unblocks).
//
// After Close, calls to verifyCommit that would have triggered a
// resync return the original verification error directly, as if the
// verifier were configured with PolicyError.
func (v *Verifier) Close() error {
	v.closeOnce.Do(func() {
		v.workerCancel()
		close(v.resyncQueue)
		v.workerWG.Wait()
		close(v.resyncDone)
		close(v.asyncErrs)
	})
	return nil
}

// startWorkers spawns n goroutines pulling from resyncQueue. Called
// once by NewVerifier; the workers exit when resyncQueue is closed
// (by Close()).
func (v *Verifier) startWorkers(n int) {
	for range n {
		v.workerWG.Add(1)
		go v.worker()
	}
}

// worker is the worker-pool goroutine body. Pulls jobs from
// resyncQueue until the channel is closed, then exits.
func (v *Verifier) worker() {
	defer v.workerWG.Done()
	for job := range v.resyncQueue {
		// runResyncJob is implemented in Task 6.
		v.runResyncJob(job)
	}
}

// runResyncJob is the per-job worker body. Stub — implemented in Task 6.
// Defined here so worker() compiles before Task 6 lands.
func (v *Verifier) runResyncJob(job resyncJob) {
	// Placeholder — overwritten in Task 6.
}

// lookupOrCreateResyncState returns the per-DID async-resync state
// for did, creating a fresh one if absent. Atomic via xsync.Map.
func (v *Verifier) lookupOrCreateResyncState(did atmos.DID) *didResyncState {
	if existing, ok := v.resyncStates.Load(did); ok {
		return existing
	}
	fresh := &didResyncState{}
	actual, _ := v.resyncStates.LoadOrStore(did, fresh)
	return actual
}

// lookupResyncState returns the per-DID state if present, or nil.
// Workers use this; the entry is guaranteed present because verifyCommit
// created it before enqueueing the job.
func (v *Verifier) lookupResyncState(did atmos.DID) *didResyncState {
	s, _ := v.resyncStates.Load(did)
	return s
}

// markIdleAndCleanup transitions the per-DID state back to Idle and,
// if pending is empty, removes the entry from the map. Worker calls
// this after sending the ResyncEvent (success) or after surfacing the
// error (failure).
//
// Pending should be empty here in normal operation: the worker drained
// it under the per-DID lock before this call. The robustness check
// for non-empty pending exists so a future code path that adds to
// pending after drain does not silently leave entries stuck.
func (v *Verifier) markIdleAndCleanup(state *didResyncState, did atmos.DID) {
	state.mu.Lock()
	state.status = statusIdle
	empty := len(state.pending) == 0
	state.mu.Unlock()
	if empty {
		v.resyncStates.Delete(did)
	}
}

// sendAsyncError delivers err on the asyncErrs channel. Tries
// non-blocking first to avoid select overhead; on full channel falls
// back to a blocking send, with workerCtx cancellation as the only
// escape (so Close() does not deadlock a worker that's mid-send).
//
// Workers may also block on a full resyncDone channel, but its larger
// buffer (DefaultResyncEventBuffer = 2048) makes that rare.
func (v *Verifier) sendAsyncError(err error) {
	if err == nil {
		return
	}
	select {
	case v.asyncErrs <- err:
		return
	default:
	}
	select {
	case v.asyncErrs <- err:
	case <-v.workerCtx.Done():
	}
}
