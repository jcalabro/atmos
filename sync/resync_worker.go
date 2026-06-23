package sync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/puzpuzpuz/xsync/v4"
)

// ResyncEvents returns the channel of completed async resync events.
// Drain promptly: workers block on send when this channel is full
// (buffer size from VerifierOptions.ResyncEventBuffer; default
// DefaultResyncEventBuffer).
//
// Closed by Verifier.Close() after all in-flight workers exit.
//
// After EnableOrderedResyncDelivery, completed resyncs are registered
// in the per-DID outbox (claimed via TakeCompletedResyncs) instead of
// being sent here, and this channel goes quiet. Package streaming
// enables ordered delivery automatically; only direct Verifier
// consumers that bypass streaming read this channel.
func (v *Verifier) ResyncEvents() <-chan ResyncEvent {
	return v.resyncDone
}

// EnableOrderedResyncDelivery switches completed async resyncs from
// channel delivery (ResyncEvents) to the per-DID outbox, where they
// are claimed via TakeCompletedResyncs. Sticky and idempotent; safe
// to call concurrently.
//
// The outbox is what makes "resync delivered before any post-resync
// event for the same DID" a happens-before guarantee rather than a
// channel-select coin flip: the worker registers the completed resync
// while still holding the per-DID verification lock, so any event for
// that DID verified afterwards observes it via TakeCompletedResyncs
// and the caller can emit the resync first. A racing select over a
// separate completion channel cannot provide that ordering — a
// post-resync commit's result can win the select and be delivered
// first, which downstream archival consumers (whose compaction treats
// the resync as a whole-account tombstone) must never observe.
//
// Intended for a single streaming integration per verifier. Unclaimed
// outbox entries are dropped at Close, exactly like undrained channel
// buffer contents.
func (v *Verifier) EnableOrderedResyncDelivery() {
	v.orderedResyncDelivery.Store(true)
}

// TakeCompletedResyncs atomically claims and returns the completed,
// not-yet-delivered async resyncs for did, in completion order.
// Returns nil when there are none. Only meaningful after
// EnableOrderedResyncDelivery.
//
// Callers must emit every claimed event (a claimed-but-unemitted
// resync is lost until the DID's next divergence re-triggers one).
func (v *Verifier) TakeCompletedResyncs(did atmos.DID) []ResyncEvent {
	evs, ok := v.resyncOutbox.LoadAndDelete(did)
	if !ok {
		return nil
	}
	v.resyncOutboxSize.Add(-int64(len(evs)))
	return evs
}

// CompletedResyncDIDs returns the DIDs that currently have completed,
// unclaimed resyncs in the outbox. Order is unspecified. Used by the
// streaming collector's periodic sweep to deliver resyncs for DIDs
// with no follow-on events.
func (v *Verifier) CompletedResyncDIDs() []atmos.DID {
	var dids []atmos.DID
	v.resyncOutbox.Range(func(did atmos.DID, _ []ResyncEvent) bool {
		dids = append(dids, did)
		return true
	})
	return dids
}

// CompletedResyncCount reports the number of completed, unclaimed
// resync events in the outbox. Cheap (one atomic load); intended as
// the fast-path check before CompletedResyncDIDs. May transiently
// undercount during a concurrent registration; callers polling on a
// timer observe the final value on the next tick.
func (v *Verifier) CompletedResyncCount() int64 {
	return v.resyncOutboxSize.Load()
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
// and AsyncErrors. Safe to call multiple times. Outstanding jobs are
// abandoned; in-flight workers' contexts are cancelled (so a stuck
// getRepo unblocks).
//
// After Close, calls to verifyCommit that would have triggered a
// resync return the original verification error directly, as if the
// verifier were configured with PolicyError.
func (v *Verifier) Close() error {
	v.closeOnce.Do(func() {
		v.workerCancel()
		v.workerWG.Wait()
		// Take the write lock so any concurrent sendAsyncError (which may run on
		// a goroutine outside workerWG, e.g. a streaming scheduler worker)
		// observes closed==true and bails out instead of sending on a closed
		// channel.
		v.closeMu.Lock()
		v.closed = true
		close(v.resyncDone)
		close(v.asyncErrs)
		v.closeMu.Unlock()
	})
	return nil
}

// startWorkers spawns n goroutines pulling from resyncQueue. Called
// once by NewVerifier; the workers exit when workerCtx is cancelled
// (by Close()).
func (v *Verifier) startWorkers(n int) {
	for range n {
		v.workerWG.Add(1)
		go v.worker()
	}
}

// worker is the worker-pool goroutine body. Pulls jobs from
// resyncQueue until workerCtx is cancelled, then exits.
func (v *Verifier) worker() {
	defer v.workerWG.Done()
	for {
		select {
		case job := <-v.resyncQueue:
			v.runResyncJob(job)
		case <-v.workerCtx.Done():
			return
		}
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
	newState, err := v.opts.StateStore.LoadChain(ctx, job.did)
	if err != nil {
		v.sendAsyncError(fmt.Errorf("verifier: load post-resync chain state for %s: %w", job.did, err))
		v.markIdleAndCleanup(state, job.did)
		return
	}
	if newState != nil {
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

	ev := ResyncEvent{
		DID:    job.did,
		OldRev: oldRev,
		NewRev: newRev,
		Reason: job.reason,
		Ops:    ops,
	}
	if v.orderedResyncDelivery.Load() {
		// Ordered mode: register in the outbox while still holding the
		// per-DID lock. Any event for this DID verified after this
		// point observes the entry via TakeCompletedResyncs, which is
		// what gives consumers the "resync before any post-resync
		// event" happens-before guarantee. Idle DIDs are picked up by
		// the streaming collector's periodic sweep.
		v.resyncOutbox.Compute(job.did, func(old []ResyncEvent, _ bool) ([]ResyncEvent, xsync.ComputeOp) {
			return append(old, ev), xsync.UpdateOp
		})
		v.resyncOutboxSize.Add(1)
	} else {
		// Channel mode (direct Verifier consumers). Block on full
		// resyncDone is acceptable back-pressure; workerCtx
		// cancellation breaks us out so Close() doesn't deadlock.
		select {
		case v.resyncDone <- ev:
		case <-v.workerCtx.Done():
		}
	}

	v.markIdleAndCleanup(state, job.did)
}

// queueAsyncResync transitions did to statusResyncing and enqueues an
// async resync job if the DID was idle; a no-op when a resync is
// already in flight (it will heal the DID). Used by VerifySync to
// schedule one deferred retry after a failed synchronous resync —
// without it the #sync directive is consumed by the failure and an
// idle DID stays stale until its next commit chain-breaks.
//
// Mirrors handleVerificationFailure's statusIdle arm; that path stays
// inline because its statusResyncing arm must also buffer the
// triggering commit, which a #sync retry does not have.
//
// Returns false when the verifier closed mid-enqueue (job not queued).
func (v *Verifier) queueAsyncResync(ctx context.Context, did atmos.DID, reason ResyncReason) bool {
	state := v.lookupOrCreateResyncState(did)
	state.mu.Lock()
	if state.status != statusIdle {
		state.mu.Unlock()
		return true
	}
	state.status = statusResyncing
	state.mu.Unlock()
	select {
	case v.resyncQueue <- resyncJob{ctx: ctx, did: did, reason: reason}:
		return true
	case <-v.workerCtx.Done():
		v.markIdleAndCleanup(state, did)
		return false
	}
}

// resyncRetryable reports whether a failed synchronous resync is worth
// one deferred async retry: rate limiting and transport/infra failures
// are; deterministic rejections (inactive account, signature mismatch,
// rev regression) are not — retrying re-fetches the same answer.
func resyncRetryable(err error) bool {
	if _, ok := errors.AsType[*ResyncRateLimitedError](err); ok {
		return true
	}
	rf, ok := errors.AsType[*ResyncFailedError](err)
	if !ok {
		return false
	}
	if rf.Reason == ReasonAccountInactive {
		return false
	}
	if _, ok := errors.AsType[*SignatureError](rf.Cause); ok {
		return false
	}
	_, isRegression := errors.AsType[*RevRegressionError](rf.Cause)
	return !isRegression
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
//
// After Close(), asyncErrs is closed and sends would panic. The
// workerCtx.Done() check protects against post-Close calls.
func (v *Verifier) sendAsyncError(err error) {
	if err == nil {
		return
	}
	// Hold the read lock for the whole check-and-send so Close() (which takes the
	// write lock before closing asyncErrs) cannot close the channel between our
	// closed check and our send. Multiple senders proceed concurrently under the
	// shared read lock; only Close contends. This is what makes a send from a
	// non-workerWG goroutine (e.g. the streaming scheduler) safe.
	v.closeMu.RLock()
	defer v.closeMu.RUnlock()
	if v.closed {
		return
	}
	// Fast check: if the verifier is shutting down, don't try to send.
	select {
	case <-v.workerCtx.Done():
		return
	default:
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
