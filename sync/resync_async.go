package sync

import (
	"context"
	"fmt"
	stdsync "sync"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
)

// Defaults for the async-resync subsystem. Each is overridable on
// VerifierOptions; see the field doc comments there for sizing
// rationale.
const (
	DefaultAsyncResyncWorkers = 32
	DefaultPendingCap         = 2048
	DefaultResyncEventBuffer  = 2048
	DefaultAsyncErrorBuffer   = 256
)

// ResyncEvent is a worker output: the result of one chain-break-driven
// resync, including the ops produced by replaying any commits buffered
// during the resync. The streaming layer drains the channel returned
// by Verifier.ResyncEvents and yields the ops through synthetic Events
// to the consumer's iterator. Pure consumers of sync.Verifier (without
// the streaming layer) may also drain ResyncEvents directly.
type ResyncEvent struct {
	DID    atmos.DID
	OldRev string
	NewRev string
	Reason ResyncReason
	Ops    []VerifierOp
}

// BufferOverflowError signals that the per-DID pending buffer (capacity
// VerifierOptions.PendingCap; default DefaultPendingCap) overflowed
// while a resync was in flight for that DID, dropping commits.
//
// Dropped commits are permanently lost. The verifier does NOT
// auto-trigger a follow-up resync — auto-retrying invites loops on
// pathologically slow PDSes, and the next firehose commit for the DID
// will surface a fresh chain break that triggers another resync via
// the normal path. Consumers should log/alert on this error.
type BufferOverflowError struct {
	DID     atmos.DID
	Dropped int
}

func (e *BufferOverflowError) Error() string {
	return fmt.Sprintf("verifier: pending buffer overflow for %s (dropped %d commits)",
		e.DID, e.Dropped)
}

// resyncStatus is the per-DID FSM the async path tracks under
// didResyncState.mu.
type resyncStatus uint8

const (
	statusIdle resyncStatus = iota
	statusResyncing
)

// didResyncState is the verifier's per-DID async-resync state. Lives
// in resyncStates keyed by DID, created lazily by verifyCommit on the
// first chain break, deleted by the worker after pending is drained
// and ops are sent.
//
// mu protects status and pending. The verifier's separate per-DID
// serialization mutex (lockDID) is still acquired for the duration of
// any actual verification work; mu is a smaller-scope lock that
// guards the FSM.
type didResyncState struct {
	mu      stdsync.Mutex
	status  resyncStatus
	pending []*comatproto.SyncSubscribeRepos_Commit
}

// resyncJob is what verifyCommit pushes onto the worker queue.
//
// ctx is the readLoop's context; workers respect cancellation so
// streaming.Client.Close() interrupts a stuck getRepo.
type resyncJob struct {
	ctx     context.Context
	did     atmos.DID
	trigger *comatproto.SyncSubscribeRepos_Commit
	reason  ResyncReason
}
