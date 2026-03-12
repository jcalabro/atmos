package streaming

import (
	"context"
	"errors"
	"time"
)

const (
	defaultLeaseDuration       = 3 * time.Second
	defaultRenewalInterval     = 1 * time.Second
	defaultAcquisitionInterval = 500 * time.Millisecond
	shutdownTimeout            = 5 * time.Second
)

var (
	// ErrNotHolder is returned when a Renew or Release is attempted by a
	// node that does not currently hold the lock.
	ErrNotHolder = errors.New("not the lock holder")

	// ErrLockHeld is returned by Acquire when another node holds the lock
	// and the lease has not expired.
	ErrLockHeld = errors.New("lock held by another node")
)

// DistributedLocker is a lease-based distributed lock. The client serializes
// all calls: Acquire, Renew, and Release are never called concurrently.
// Implementations do not need to be safe for concurrent use.
//
// The semantics are:
//   - Acquire claims the lock with a lease duration. If the lock is held by
//     another node whose lease has not expired, it returns [ErrLockHeld].
//   - Renew extends the lease. Returns [ErrNotHolder] if this node no longer
//     holds the lock (e.g. the lease expired or was taken by another node).
//   - Release explicitly yields the lock. Best-effort; crash-safety comes
//     from lease expiration rather than explicit release.
//
// Users implement this interface on top of their distributed store (e.g.
// Redis SETNX+TTL, Postgres advisory locks, etcd leases, DynamoDB
// conditional puts, FoundationDB transactions, etc.).
type DistributedLocker interface {
	// Acquire attempts to claim the lock with the given lease duration.
	// Returns nil on success, [ErrLockHeld] if another holder's lease has
	// not expired, or another error on infrastructure failure.
	Acquire(ctx context.Context, lease time.Duration) error

	// Renew extends the current lease by the given duration from now.
	// Returns [ErrNotHolder] if this node no longer holds the lock.
	Renew(ctx context.Context, lease time.Duration) error

	// Release explicitly yields the lock. Returns [ErrNotHolder] if this
	// node does not hold the lock (which is not necessarily an error
	// condition — it may have already expired).
	Release(ctx context.Context) error
}

// DistributedLockerOptions configures distributed lock coordination for high
// availability deployments. When provided to [Options], the client gates event
// consumption behind lock acquisition: only the lock holder consumes from the
// firehose, while other nodes wait idle and attempt to acquire on a periodic
// interval.
//
// Events are delivered at least once. In rare cases during leader failover,
// the same event may be emitted more than once. Consumers must handle events
// idempotently.
type DistributedLockerOptions struct {
	// Locker is the distributed lock implementation. Required.
	Locker DistributedLocker

	// LeaseDuration controls how long a lock lease is valid before it
	// expires. Zero means 3s.
	LeaseDuration time.Duration

	// RenewalInterval controls how often the lock holder renews the lease.
	// Must be significantly less than LeaseDuration to avoid accidental
	// expiration. Zero means 1s.
	RenewalInterval time.Duration

	// AcquisitionInterval controls how often a non-holder polls to acquire
	// the lock. Zero means 500ms.
	AcquisitionInterval time.Duration

	// OnBecameLeader is called when this node acquires the lock and becomes
	// the active consumer. May be nil.
	OnBecameLeader func()

	// OnLostLeadership is called when this node loses the lock (either by
	// renewal failure, explicit release, or shutdown). May be nil.
	OnLostLeadership func()
}

// NoopLock always succeeds. Used for single-node deployments where no
// distributed coordination is needed. This is the default when
// [Options].Locker is not set.
type NoopLock struct{}

func (NoopLock) Acquire(_ context.Context, _ time.Duration) error { return nil }
func (NoopLock) Renew(_ context.Context, _ time.Duration) error   { return nil }
func (NoopLock) Release(_ context.Context) error                  { return nil }
