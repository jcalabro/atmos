package sync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
)

// RepoKey uniquely identifies one author's repository in one space.
type RepoKey struct {
	Space  atmos.SpaceRef
	Author atmos.DID
}

// Validate validates both coordinates.
func (k RepoKey) Validate() error {
	if err := k.Space.Validate(); err != nil {
		return fmt.Errorf("space sync: invalid space: %w", err)
	}
	if err := k.Author.Validate(); err != nil {
		return fmt.Errorf("space sync: invalid author: %w", err)
	}
	return nil
}

// Provenance records the direct-host and author-key context used to verify a
// checkpoint. Values are evidence, not authorization for a later request.
type Provenance struct {
	HostURL      string
	KeyMultibase string
	ResolvedAt   time.Time
}

// Checkpoint is the durable verification boundary for a repo generation.
type Checkpoint struct {
	Revision       atmos.TID
	Hash           [spaces.CommitHashSize]byte
	State          [spaces.LtHashStateSize]byte
	IndexComplete  bool
	ValuesComplete bool
	Provenance     Provenance
}

// Record is one verified final record value.
type Record struct {
	CID  cbor.CID
	Data []byte
}

// Clone returns an independent record copy.
func (r Record) Clone() Record {
	r.Data = append([]byte(nil), r.Data...)
	return r
}

// Repo is an immutable copy of a published repository generation. Version is
// an opaque compare-and-swap token; zero means no generation has been published.
type Repo struct {
	Key                 RepoKey
	Version             uint64
	LifecycleGeneration uint64
	AccountGeneration   uint64
	Checkpoint          Checkpoint
	Index               spaces.RepoIndex
	Records             map[spaces.RecordPath]Record
}

// Clone returns a deep copy.
func (r Repo) Clone() Repo {
	r.Index = r.Index.Clone()
	r.Records = cloneRecords(r.Records)
	return r
}

func cloneRecords(records map[spaces.RecordPath]Record) map[spaces.RecordPath]Record {
	out := make(map[spaces.RecordPath]Record, len(records))
	for path, record := range records {
		out[path] = record.Clone()
	}
	return out
}

// StageID identifies an invisible staged generation.
type StageID uint64

// Stage is the durable working generation returned by Store.Begin. Mutations
// are invisible until Store.Promote succeeds.
type Stage struct {
	ID                  StageID
	Key                 RepoKey
	ExpectedVersion     uint64
	LifecycleGeneration uint64
	AccountGeneration   uint64
	Index               spaces.RepoIndex
	Records             map[spaces.RecordPath]Record
	State               [spaces.LtHashStateSize]byte
}

// Clone returns a deep copy.
func (s Stage) Clone() Stage {
	s.Index = s.Index.Clone()
	s.Records = cloneRecords(s.Records)
	return s
}

// LifecycleState describes whether network work and serving may proceed.
type LifecycleState uint8

const (
	// LifecycleActive permits normal sync and serving.
	LifecycleActive LifecycleState = iota + 1
	// LifecycleStale permits reconciliation work but requires explicit
	// application authorization before serving the retained checkpoint.
	LifecycleStale
	// LifecycleSuspended retains verified data but stops network work. Serving
	// requires explicit application authorization.
	LifecycleSuspended
	// LifecycleDeleted permanently tombstones this local space incarnation and
	// requires cleanup of replicated data and credential material.
	LifecycleDeleted
)

// Lifecycle is the durable fencing state for a space.
type Lifecycle struct {
	State          LifecycleState
	Generation     uint64
	Reason         string
	CleanupPending bool
	UpdatedAt      time.Time
}

// Lease is a persisted callback registration renewal deadline.
type Lease struct {
	ServiceID string
	ExpiresAt time.Time
}

// EventKind identifies bounded, non-sensitive durable outbox records.
type EventKind string

const (
	// EventRepoPromoted announces an atomically published checkpoint.
	EventRepoPromoted EventKind = "repo_promoted"
	// EventSpaceLifecycle announces a durable lifecycle transition.
	EventSpaceLifecycle EventKind = "space_lifecycle"
)

// Event is delivered at least once. Consumers must deduplicate by ID.
type Event struct {
	ID                  uint64
	Kind                EventKind
	Key                 RepoKey
	Revision            atmos.TID
	RepoVersion         uint64
	LifecycleGeneration uint64
	LifecycleState      LifecycleState
	CreatedAt           time.Time
}

// Store is the durable replication contract. Begin creates an invisible clone
// of the expected generation. SaveStage may be called repeatedly. Promote must
// atomically compare both fences, publish the stage, and append its outbox event.
type Store interface {
	Lifecycle(context.Context, atmos.SpaceRef) (Lifecycle, error)
	TransitionLifecycle(context.Context, atmos.SpaceRef, uint64, LifecycleState, string) (Lifecycle, error)
	RepoLifecycle(context.Context, RepoKey) (Lifecycle, error)
	TransitionRepoLifecycle(context.Context, RepoKey, uint64, LifecycleState, string) (Lifecycle, error)
	LoadRepo(context.Context, RepoKey) (Repo, error)
	ListRepos(context.Context, atmos.SpaceRef) ([]RepoKey, error)
	// ListRepoLifecycles returns every key in the space with a durable per-author
	// lifecycle row, including keys that have no published generation. Restart
	// cleanup depends on it: a deletion whose purge failed before anything was
	// published must remain discoverable, so implementations must retain deletion
	// tombstone rows until PurgeSpace.
	ListRepoLifecycles(context.Context, atmos.SpaceRef) ([]RepoKey, error)
	Begin(context.Context, RepoKey, uint64, uint64) (Stage, error)
	SaveStage(context.Context, Stage) error
	Promote(context.Context, StageID, Checkpoint) (Repo, error)
	DiscardStage(context.Context, StageID) error
	DiscardUnfinished(context.Context, atmos.SpaceRef) error
	// PurgeSpace removes repo copies, derived indexes, blob access references,
	// and stages after a durable deletion tombstone.
	PurgeSpace(context.Context, atmos.SpaceRef) error
	PurgeRepo(context.Context, RepoKey) error
	GetLease(context.Context, atmos.SpaceRef) (Lease, error)
	SetLease(context.Context, atmos.SpaceRef, Lease) error
	PeekEvents(context.Context, int) ([]Event, error)
	AckEvent(context.Context, uint64) error
}

// FailureInjector is implemented by test stores that can fail named atomic
// boundaries. Store conformance tests use it without weakening production APIs.
type FailureInjector interface {
	FailNext(operation string, err error)
}

var (
	// ErrNotFound reports an absent repository, lease, or event.
	ErrNotFound = errors.New("space sync: not found")
	// ErrConflict reports a failed repo-version or lifecycle-generation CAS.
	ErrConflict = errors.New("space sync: compare-and-swap conflict")
	// ErrDeleted reports a tombstoned space.
	ErrDeleted = errors.New("space sync: space deleted")
	// ErrSuspended reports that network work is fenced by access/account state.
	ErrSuspended = errors.New("space sync: space suspended")
	// ErrRepoAbsent distinguishes an unwritten repository from a verified empty
	// repository, which has a real signed empty-state commit.
	ErrRepoAbsent = errors.New("space sync: repository has never been written")
	// ErrCredentialDenied reports an authenticated reader credential that can no
	// longer access the space. The Syncer transitions to suspended retention.
	ErrCredentialDenied = errors.New("space sync: credential denied")
	// ErrAccountSuspended reports a direct repo-host account status response.
	ErrAccountSuspended = errors.New("space sync: author account suspended")
)

// IncompleteReason classifies a bounded pass that cannot be published.
type IncompleteReason string

const (
	IncompleteHistory       IncompleteReason = "history"
	IncompletePagination    IncompleteReason = "pagination"
	IncompleteMissingCommit IncompleteReason = "missing_commit"
	IncompleteFinalValue    IncompleteReason = "final_value"
	IncompleteLimits        IncompleteReason = "limits"
	IncompleteSnapshot      IncompleteReason = "snapshot"
)

// IncompleteError indicates that no staged work was published and recovery or
// a later pass may converge. Cause is retained through errors.Is/errors.As.
type IncompleteError struct {
	Reason IncompleteReason
	Cause  error
}

func (e *IncompleteError) Error() string {
	if e.Cause == nil {
		return "space sync: incomplete " + string(e.Reason)
	}
	return fmt.Sprintf("space sync: incomplete %s: %v", e.Reason, e.Cause)
}

// Unwrap returns the contributing failure.
func (e *IncompleteError) Unwrap() error { return e.Cause }

// IntegrityError reports rollback, equivocation, invalid prev, or a verified
// commit that does not match staged state. It must never trigger blind retry.
type IntegrityError struct{ Cause error }

func (e *IntegrityError) Error() string { return "space sync: integrity conflict: " + e.Cause.Error() }

// Unwrap returns the contributing failure.
func (e *IntegrityError) Unwrap() error { return e.Cause }

// ServeAuthorization is required to expose retained data while suspended.
// Implementations should apply their own current viewer authorization policy.
type ServeAuthorization func(context.Context, RepoKey, Lifecycle) error
