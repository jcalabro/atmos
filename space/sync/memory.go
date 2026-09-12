package sync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
)

// MemoryStore is a bounded, concurrency-safe Store for tests and examples. It
// is deliberately not an implicit production fallback and does not survive a
// process restart.
type MemoryStore struct {
	mu         sync.Mutex
	maxRepos   int
	maxStages  int
	maxEvents  int
	maxBytes   uint64
	nextStage  StageID
	nextEvent  uint64
	lifecycles map[atmos.SpaceRef]Lifecycle
	repoStates map[RepoKey]Lifecycle
	repos      map[RepoKey]Repo
	stages     map[StageID]Stage
	leases     map[atmos.SpaceRef]Lease
	events     []Event
	failures   map[string][]error
	now        func() time.Time
}

// NewMemoryStore constructs an explicitly bounded store.
func NewMemoryStore(maxRepos, maxStages, maxEvents int, maxBytes int64) (*MemoryStore, error) {
	if maxRepos <= 0 || maxStages <= 0 || maxEvents <= 0 || maxBytes <= 0 {
		return nil, errors.New("space sync: memory-store limits must be positive")
	}
	return &MemoryStore{
		maxRepos: maxRepos, maxStages: maxStages, maxEvents: maxEvents, maxBytes: uint64(maxBytes),
		lifecycles: make(map[atmos.SpaceRef]Lifecycle), repoStates: make(map[RepoKey]Lifecycle), repos: make(map[RepoKey]Repo),
		stages: make(map[StageID]Stage), leases: make(map[atmos.SpaceRef]Lease),
		failures: make(map[string][]error), now: time.Now,
	}, nil
}

func (s *MemoryStore) repoLifecycle(key RepoKey) (Lifecycle, error) {
	lifecycle, ok := s.repoStates[key]
	if !ok {
		if len(s.repoStates) >= s.maxRepos {
			return Lifecycle{}, errors.New("space sync: repo-state capacity exhausted")
		}
		lifecycle = Lifecycle{State: LifecycleActive, Generation: 1, UpdatedAt: s.now()}
		s.repoStates[key] = lifecycle
	}
	return lifecycle, nil
}

// FailNext injects one failure at operation. Supported boundaries are the
// Store method names in lower snake case (for example "promote").
func (s *MemoryStore) FailNext(operation string, err error) {
	if err == nil {
		panic("space sync: cannot inject a nil failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[operation] = append(s.failures[operation], err)
}

func (s *MemoryStore) fail(operation string) error {
	queue := s.failures[operation]
	if len(queue) == 0 {
		return nil
	}
	err := queue[0]
	s.failures[operation] = queue[1:]
	return err
}

func (s *MemoryStore) lifecycle(space atmos.SpaceRef) (Lifecycle, error) {
	lifecycle, ok := s.lifecycles[space]
	if !ok {
		if len(s.lifecycles) >= s.maxRepos {
			return Lifecycle{}, errors.New("space sync: space-state capacity exhausted")
		}
		lifecycle = Lifecycle{State: LifecycleActive, Generation: 1, UpdatedAt: s.now()}
		s.lifecycles[space] = lifecycle
	}
	return lifecycle, nil
}

// Lifecycle returns the current fence, creating the initial active fence.
func (s *MemoryStore) Lifecycle(ctx context.Context, space atmos.SpaceRef) (Lifecycle, error) {
	if err := ctx.Err(); err != nil {
		return Lifecycle{}, err
	}
	if err := space.Validate(); err != nil {
		return Lifecycle{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("lifecycle"); err != nil {
		return Lifecycle{}, err
	}
	return s.lifecycle(space)
}

// TransitionLifecycle atomically changes lifecycle state and enqueues an event.
func (s *MemoryStore) TransitionLifecycle(ctx context.Context, space atmos.SpaceRef, expected uint64, state LifecycleState, reason string) (Lifecycle, error) {
	if err := ctx.Err(); err != nil {
		return Lifecycle{}, err
	}
	if err := space.Validate(); err != nil {
		return Lifecycle{}, err
	}
	if state < LifecycleActive || state > LifecycleDeleted {
		return Lifecycle{}, errors.New("space sync: invalid lifecycle state")
	}
	if len(reason) > 1024 {
		return Lifecycle{}, errors.New("space sync: lifecycle reason exceeds 1024 bytes")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("transition_lifecycle"); err != nil {
		return Lifecycle{}, err
	}
	current, err := s.lifecycle(space)
	if err != nil {
		return Lifecycle{}, err
	}
	if current.Generation != expected {
		return Lifecycle{}, ErrConflict
	}
	if current.State == LifecycleDeleted && state != LifecycleDeleted {
		return Lifecycle{}, ErrDeleted
	}
	if len(s.events) >= s.maxEvents {
		return Lifecycle{}, errors.New("space sync: outbox capacity exhausted")
	}
	current.Generation++
	current.State, current.Reason, current.UpdatedAt = state, reason, s.now()
	current.CleanupPending = state == LifecycleDeleted
	s.lifecycles[space] = current
	s.nextEvent++
	s.events = append(s.events, Event{ID: s.nextEvent, Kind: EventSpaceLifecycle, Key: RepoKey{Space: space}, LifecycleGeneration: current.Generation, LifecycleState: state, CreatedAt: current.UpdatedAt})
	return current, nil
}

// RepoLifecycle returns the author-account fence for one replicated repo.
func (s *MemoryStore) RepoLifecycle(ctx context.Context, key RepoKey) (Lifecycle, error) {
	if err := ctx.Err(); err != nil {
		return Lifecycle{}, err
	}
	if err := key.Validate(); err != nil {
		return Lifecycle{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("repo_lifecycle"); err != nil {
		return Lifecycle{}, err
	}
	return s.repoLifecycle(key)
}

// TransitionRepoLifecycle fences work for one author without suspending the
// rest of the space.
func (s *MemoryStore) TransitionRepoLifecycle(ctx context.Context, key RepoKey, expected uint64, state LifecycleState, reason string) (Lifecycle, error) {
	if err := ctx.Err(); err != nil {
		return Lifecycle{}, err
	}
	if err := key.Validate(); err != nil {
		return Lifecycle{}, err
	}
	if state < LifecycleActive || state > LifecycleDeleted {
		return Lifecycle{}, errors.New("space sync: invalid lifecycle state")
	}
	if len(reason) > 1024 {
		return Lifecycle{}, errors.New("space sync: lifecycle reason exceeds 1024 bytes")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("transition_repo_lifecycle"); err != nil {
		return Lifecycle{}, err
	}
	current, err := s.repoLifecycle(key)
	if err != nil {
		return Lifecycle{}, err
	}
	if current.Generation != expected {
		return Lifecycle{}, ErrConflict
	}
	if current.State == LifecycleDeleted && state != LifecycleDeleted {
		return Lifecycle{}, ErrDeleted
	}
	if len(s.events) >= s.maxEvents {
		return Lifecycle{}, errors.New("space sync: outbox capacity exhausted")
	}
	current.Generation++
	current.State, current.Reason, current.UpdatedAt = state, reason, s.now()
	current.CleanupPending = state == LifecycleDeleted
	s.repoStates[key] = current
	s.nextEvent++
	s.events = append(s.events, Event{ID: s.nextEvent, Kind: EventSpaceLifecycle, Key: key, LifecycleGeneration: current.Generation, LifecycleState: state, CreatedAt: current.UpdatedAt})
	return current, nil
}

// LoadRepo returns a deep copy of a published generation.
func (s *MemoryStore) LoadRepo(ctx context.Context, key RepoKey) (Repo, error) {
	if err := ctx.Err(); err != nil {
		return Repo{}, err
	}
	if err := key.Validate(); err != nil {
		return Repo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("load_repo"); err != nil {
		return Repo{}, err
	}
	repo, ok := s.repos[key]
	if !ok {
		return Repo{}, ErrNotFound
	}
	return repo.Clone(), nil
}

// ListRepos returns stable sorted keys for published generations.
func (s *MemoryStore) ListRepos(ctx context.Context, space atmos.SpaceRef) ([]RepoKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := space.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("list_repos"); err != nil {
		return nil, err
	}
	keys := make([]RepoKey, 0)
	for key := range s.repos {
		if key.Space == space {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Author < keys[j].Author })
	return keys, nil
}

// ListRepoLifecycles returns stable sorted keys for every durable per-author
// lifecycle row, including tombstoned keys with no published generation.
func (s *MemoryStore) ListRepoLifecycles(ctx context.Context, space atmos.SpaceRef) ([]RepoKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := space.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("list_repo_lifecycles"); err != nil {
		return nil, err
	}
	keys := make([]RepoKey, 0)
	for key := range s.repoStates {
		if key.Space == space {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Author < keys[j].Author })
	return keys, nil
}

// Begin creates an invisible clone from the expected published generation.
func (s *MemoryStore) Begin(ctx context.Context, key RepoKey, expectedVersion, generation uint64) (Stage, error) {
	if err := ctx.Err(); err != nil {
		return Stage{}, err
	}
	if err := key.Validate(); err != nil {
		return Stage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("begin"); err != nil {
		return Stage{}, err
	}
	lifecycle, err := s.lifecycle(key.Space)
	if err != nil {
		return Stage{}, err
	}
	if lifecycle.State == LifecycleDeleted {
		return Stage{}, ErrDeleted
	}
	if lifecycle.State != LifecycleActive && lifecycle.State != LifecycleStale {
		return Stage{}, ErrSuspended
	}
	if lifecycle.Generation != generation {
		return Stage{}, ErrConflict
	}
	repoLifecycle, err := s.repoLifecycle(key)
	if err != nil {
		return Stage{}, err
	}
	if repoLifecycle.State == LifecycleDeleted {
		return Stage{}, ErrDeleted
	}
	if repoLifecycle.State != LifecycleActive && repoLifecycle.State != LifecycleStale {
		return Stage{}, ErrSuspended
	}
	current, exists := s.repos[key]
	if (!exists && expectedVersion != 0) || (exists && current.Version != expectedVersion) {
		return Stage{}, ErrConflict
	}
	if len(s.stages) >= s.maxStages {
		return Stage{}, errors.New("space sync: stage capacity exhausted")
	}
	if !exists && len(s.repos) >= s.maxRepos {
		return Stage{}, errors.New("space sync: repo capacity exhausted")
	}
	s.nextStage++
	stage := Stage{ID: s.nextStage, Key: key, ExpectedVersion: expectedVersion, LifecycleGeneration: generation, AccountGeneration: repoLifecycle.Generation, Index: make(map[spaces.RecordPath]cbor.CID), Records: make(map[spaces.RecordPath]Record)}
	if exists {
		stage.Index, stage.Records, stage.State = current.Index.Clone(), cloneRecords(current.Records), current.Checkpoint.State
	}
	if saturatingAdd(s.ownedBytes(0, nil), stageMemoryBytes(stage)) > s.maxBytes {
		return Stage{}, errors.New("space sync: memory-store byte capacity exhausted")
	}
	s.stages[stage.ID] = stage.Clone()
	return stage, nil
}

// SaveStage durably replaces one invisible stage.
func (s *MemoryStore) SaveStage(ctx context.Context, stage Stage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("save_stage"); err != nil {
		return err
	}
	stored, ok := s.stages[stage.ID]
	if !ok {
		return ErrNotFound
	}
	if stored.Key != stage.Key || stored.ExpectedVersion != stage.ExpectedVersion || stored.LifecycleGeneration != stage.LifecycleGeneration || stored.AccountGeneration != stage.AccountGeneration {
		return ErrConflict
	}
	if saturatingAdd(s.ownedBytes(stage.ID, nil), stageMemoryBytes(stage)) > s.maxBytes {
		return errors.New("space sync: memory-store byte capacity exhausted")
	}
	s.stages[stage.ID] = stage.Clone()
	return nil
}

// Promote atomically publishes a stage and its outbox event.
func (s *MemoryStore) Promote(ctx context.Context, id StageID, checkpoint Checkpoint) (Repo, error) {
	if err := ctx.Err(); err != nil {
		return Repo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("promote"); err != nil {
		return Repo{}, err
	}
	stage, ok := s.stages[id]
	if !ok {
		return Repo{}, ErrNotFound
	}
	lifecycle, err := s.lifecycle(stage.Key.Space)
	if err != nil {
		return Repo{}, err
	}
	if lifecycle.State == LifecycleDeleted {
		return Repo{}, ErrDeleted
	}
	if lifecycle.State != LifecycleActive && lifecycle.State != LifecycleStale {
		return Repo{}, ErrSuspended
	}
	if lifecycle.Generation != stage.LifecycleGeneration {
		return Repo{}, ErrConflict
	}
	repoLifecycle, err := s.repoLifecycle(stage.Key)
	if err != nil {
		return Repo{}, err
	}
	if repoLifecycle.State == LifecycleDeleted {
		return Repo{}, ErrDeleted
	}
	if repoLifecycle.State != LifecycleActive && repoLifecycle.State != LifecycleStale {
		return Repo{}, ErrSuspended
	}
	if repoLifecycle.Generation != stage.AccountGeneration {
		return Repo{}, ErrConflict
	}
	current, exists := s.repos[stage.Key]
	if (!exists && stage.ExpectedVersion != 0) || (exists && current.Version != stage.ExpectedVersion) {
		return Repo{}, ErrConflict
	}
	if !checkpoint.IndexComplete || !checkpoint.ValuesComplete || len(stage.Index) != len(stage.Records) {
		return Repo{}, errors.New("space sync: refusing to publish an incomplete generation")
	}
	if err := checkpoint.Revision.Validate(); err != nil {
		return Repo{}, fmt.Errorf("space sync: invalid checkpoint revision: %w", err)
	}
	if checkpoint.Provenance.HostURL == "" || checkpoint.Provenance.KeyMultibase == "" || checkpoint.Provenance.ResolvedAt.IsZero() {
		return Repo{}, errors.New("space sync: checkpoint verification provenance is required")
	}
	if len(checkpoint.Provenance.HostURL) > 2048 || len(checkpoint.Provenance.KeyMultibase) > 512 {
		return Repo{}, errors.New("space sync: checkpoint provenance exceeds memory-store limits")
	}
	repoCommit, err := spaces.NewRepoCommitFromIndex(stage.Index)
	if err != nil {
		return Repo{}, err
	}
	if repoCommit.State() != stage.State || repoCommit.State() != checkpoint.State || repoCommit.Digest() != checkpoint.Hash {
		return Repo{}, errors.New("space sync: checkpoint does not match staged index state")
	}
	for path, cid := range stage.Index {
		record, present := stage.Records[path]
		if !present || !record.CID.Equal(cid) {
			return Repo{}, fmt.Errorf("space sync: missing final value at %s", path)
		}
		if err := validateBody(cid, record.Data); err != nil {
			return Repo{}, fmt.Errorf("space sync: invalid final value at %s: %w", path, err)
		}
	}
	if len(s.events) >= s.maxEvents {
		return Repo{}, errors.New("space sync: outbox capacity exhausted")
	}
	version := uint64(1)
	if exists {
		version = current.Version + 1
	}
	repo := Repo{Key: stage.Key, Version: version, LifecycleGeneration: lifecycle.Generation, AccountGeneration: repoLifecycle.Generation, Checkpoint: checkpoint, Index: stage.Index.Clone(), Records: cloneRecords(stage.Records)}
	if saturatingAdd(s.ownedBytes(stage.ID, &stage.Key), repoMemoryBytes(repo)) > s.maxBytes {
		return Repo{}, errors.New("space sync: memory-store byte capacity exhausted")
	}
	s.repos[stage.Key] = repo
	delete(s.stages, id)
	s.nextEvent++
	s.events = append(s.events, Event{ID: s.nextEvent, Kind: EventRepoPromoted, Key: stage.Key, Revision: checkpoint.Revision, RepoVersion: version, LifecycleGeneration: lifecycle.Generation, CreatedAt: s.now()})
	return repo.Clone(), nil
}

// DiscardStage removes one invisible generation.
func (s *MemoryStore) DiscardStage(ctx context.Context, id StageID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("discard_stage"); err != nil {
		return err
	}
	delete(s.stages, id)
	return nil
}

// DiscardUnfinished removes all interrupted stages for a space.
func (s *MemoryStore) DiscardUnfinished(ctx context.Context, space atmos.SpaceRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := space.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("discard_unfinished"); err != nil {
		return err
	}
	for id, stage := range s.stages {
		if stage.Key.Space == space {
			delete(s.stages, id)
		}
	}
	return nil
}

// PurgeSpace removes published and staged repo data after a durable tombstone.
func (s *MemoryStore) PurgeSpace(ctx context.Context, space atmos.SpaceRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := space.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("purge_space"); err != nil {
		return err
	}
	lifecycle, err := s.lifecycle(space)
	if err != nil {
		return err
	}
	if lifecycle.State != LifecycleDeleted {
		return errors.New("space sync: purge requires a deletion tombstone")
	}
	for key := range s.repos {
		if key.Space == space {
			delete(s.repos, key)
		}
	}
	for key := range s.repoStates {
		if key.Space == space {
			delete(s.repoStates, key)
		}
	}
	for id, stage := range s.stages {
		if stage.Key.Space == space {
			delete(s.stages, id)
		}
	}
	delete(s.leases, space)
	lifecycle.CleanupPending = false
	s.lifecycles[space] = lifecycle
	return nil
}

// PurgeRepo removes one account's replicated data after a durable tombstone.
func (s *MemoryStore) PurgeRepo(ctx context.Context, key RepoKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("purge_repo"); err != nil {
		return err
	}
	lifecycle, err := s.repoLifecycle(key)
	if err != nil {
		return err
	}
	if lifecycle.State != LifecycleDeleted {
		return errors.New("space sync: repo purge requires a deletion tombstone")
	}
	delete(s.repos, key)
	for id, stage := range s.stages {
		if stage.Key == key {
			delete(s.stages, id)
		}
	}
	lifecycle.CleanupPending = false
	s.repoStates[key] = lifecycle
	return nil
}

// GetLease loads callback-registration state.
func (s *MemoryStore) GetLease(ctx context.Context, space atmos.SpaceRef) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if err := space.Validate(); err != nil {
		return Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("get_lease"); err != nil {
		return Lease{}, err
	}
	lease, ok := s.leases[space]
	if !ok {
		return Lease{}, ErrNotFound
	}
	return lease, nil
}

// SetLease persists callback-registration state.
func (s *MemoryStore) SetLease(ctx context.Context, space atmos.SpaceRef, lease Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := space.Validate(); err != nil {
		return err
	}
	if lease.ServiceID == "" || len(lease.ServiceID) > 512 || lease.ExpiresAt.IsZero() {
		return errors.New("space sync: invalid callback lease")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("set_lease"); err != nil {
		return err
	}
	if _, exists := s.leases[space]; !exists && len(s.leases) >= s.maxRepos {
		return errors.New("space sync: lease capacity exhausted")
	}
	s.leases[space] = lease
	return nil
}

func (s *MemoryStore) ownedBytes(excludeStage StageID, excludeRepo *RepoKey) uint64 {
	var total uint64
	for id, stage := range s.stages {
		if id != excludeStage {
			total = saturatingAdd(total, stageMemoryBytes(stage))
		}
	}
	for key, repo := range s.repos {
		if excludeRepo == nil || key != *excludeRepo {
			total = saturatingAdd(total, repoMemoryBytes(repo))
		}
	}
	return total
}

func stageMemoryBytes(stage Stage) uint64 {
	return generationMemoryBytes(stage.Index, stage.Records) + spaces.LtHashStateSize + 128
}

func repoMemoryBytes(repo Repo) uint64 {
	return generationMemoryBytes(repo.Index, repo.Records) + spaces.LtHashStateSize + spaces.CommitHashSize + uint64(len(repo.Checkpoint.Provenance.HostURL)+len(repo.Checkpoint.Provenance.KeyMultibase)) + 256
}

func generationMemoryBytes(index spaces.RepoIndex, records map[spaces.RecordPath]Record) uint64 {
	var total uint64
	for path := range index {
		total = saturatingAdd(total, uint64(len(path))+128)
	}
	for _, record := range records {
		total = saturatingAdd(total, uint64(len(record.Data))+128)
	}
	return total
}

func saturatingAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

// PeekEvents returns, without claiming, the oldest outbox events.
func (s *MemoryStore) PeekEvents(ctx context.Context, limit int) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("space sync: event limit must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("peek_events"); err != nil {
		return nil, err
	}
	limit = min(limit, len(s.events))
	return append([]Event(nil), s.events[:limit]...), nil
}

// AckEvent idempotently removes an event after successful downstream handling.
func (s *MemoryStore) AckEvent(ctx context.Context, id uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fail("ack_event"); err != nil {
		return err
	}
	for i := range s.events {
		if s.events[i].ID == id {
			s.events = append(s.events[:i], s.events[i+1:]...)
			return nil
		}
	}
	return nil
}
