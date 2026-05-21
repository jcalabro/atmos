package sync

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
