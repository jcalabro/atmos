package sync

import (
	"context"
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
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

// runResyncJob is the per-job worker body. Holds the per-DID
// serialization lock for the duration of the resync HTTP call AND
// the pending drain, so subsequent verifyCommit calls for the same
// DID continue to land in the pending buffer (FSM still in
// statusResyncing).
func (v *Verifier) runResyncJob(job resyncJob) {
	state := v.lookupResyncState(job.did)
	if state == nil {
		// Defensive: verifyCommit creates the state before enqueueing,
		// so this is unreachable except after a programmer error.
		v.sendAsyncError(fmt.Errorf("verifier: missing resync state for %s", job.did))
		return
	}

	unlock := v.lockDID(job.did)
	defer unlock()

	// Honor both the readLoop's context AND the verifier's worker
	// context. Close() cancels workerCtx; either being cancelled
	// should abort an in-flight getRepo or replay.
	ctx, cancelCtx := mergeCtx(job.ctx, v.workerCtx)
	defer cancelCtx()

	// Capture pre-resync rev for OnResync / ResyncEvent.OldRev.
	oldRev := ""
	if old, _ := v.opts.StateStore.LoadChain(ctx, job.did); old != nil {
		oldRev = old.Rev
	}

	ops, err := v.resync(ctx, job.did, job.reason)
	if err != nil {
		v.sendAsyncError(err)
		v.markIdleAndCleanup(state, job.did)
		return
	}

	// Capture post-resync rev. resync() advanced state to
	// (newRev, newData) before returning; reload to get newRev.
	newRev := ""
	if newState, _ := v.opts.StateStore.LoadChain(ctx, job.did); newState != nil {
		newRev = newState.Rev
	}

	// Drain pending: replay each commit through verifyCommitInline.
	// Replays at or below newRev drop silently via the existing
	// rev-replay logic (rev <= state.Rev returns nil, nil).
	state.mu.Lock()
	pending := state.pending
	state.pending = nil
	state.mu.Unlock()

	for _, c := range pending {
		replayOps, rerr := v.verifyCommitInline(ctx, job.did, c)
		if rerr != nil {
			v.sendAsyncError(rerr)
			continue
		}
		ops = append(ops, replayOps...)
	}

	// Send the combined event. Block on full resyncDone is acceptable
	// back-pressure; workerCtx cancellation breaks us out so Close()
	// doesn't deadlock.
	select {
	case v.resyncDone <- ResyncEvent{
		DID:    job.did,
		OldRev: oldRev,
		NewRev: newRev,
		Reason: job.reason,
		Ops:    ops,
	}:
	case <-v.workerCtx.Done():
	}

	v.markIdleAndCleanup(state, job.did)
}

// verifyCommitInline is verifyCommit's body without the lockDID
// acquisition (workers hold it across the resync + replay cycle) and
// without async resync enqueue (replay errors surface as typed errors,
// not new resync jobs).
//
// Future-rev / size checks are duplicated here from verifyCommit. They
// cost effectively nothing and protect against a malicious or buggy
// commit landing in pending under one set of gates and being replayed
// after gates have changed (e.g. FutureRevTolerance reduced).
//
// Returns the typed error directly. OnVerificationFailure is NOT fired
// during replay; consumers monitor AsyncErrors instead. This is a
// documented simplification — replay failures are rare.
func (v *Verifier) verifyCommitInline(
	ctx context.Context,
	did atmos.DID,
	commit *comatproto.SyncSubscribeRepos_Commit,
) ([]VerifierOp, error) {
	if frErr := v.checkFutureRev(did, commit.Rev); frErr != nil {
		v.futureRevsRejected.Add(1)
		return nil, frErr
	}
	if tlErr := checkCommitSize(did, commit); tlErr != nil {
		v.oversizedCommits.Add(1)
		return nil, tlErr
	}
	ops, _, err := v.verifyCommitLocked(ctx, did, commit, false)
	return ops, err
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

// mergeCtx returns a context that is cancelled when EITHER a or b is
// cancelled. Used by resync workers to honor both the readLoop's
// context (from job.ctx) and the verifier's workerCtx (cancelled by
// Close()).
func mergeCtx(a, b context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(a)
	stop := make(chan struct{})
	go func() {
		select {
		case <-b.Done():
			cancel()
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }
}
