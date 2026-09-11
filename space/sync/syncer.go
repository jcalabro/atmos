package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
)

// RecoveryMode selects the bounded snapshot recovery strategy.
type RecoveryMode uint8

const (
	// RecoveryFull always downloads a complete CAR.
	RecoveryFull RecoveryMode = iota + 1
	// RecoveryIndexOnly downloads an index CAR, reuses matching verified values,
	// and fetches remaining final records individually.
	RecoveryIndexOnly
)

// Limits bounds every replication pass. No production operating envelope is
// silently selected by this package.
type Limits struct {
	PageSize            int
	DirectoryPageSize   int
	MaxPages            int
	MaxDirectoryPages   int
	MaxAuthors          int
	MaxOperations       int
	MaxIncrementalBytes int64
	MaxFinalFetches     int
	MaxRecoveryAttempts int
	MaxRepoBytes        int64
	PassTimeout         time.Duration
	CleanupTimeout      time.Duration
	CAR                 spaces.CARLimits
}

// Validate rejects absent or inconsistent limits.
func (l Limits) Validate() error {
	if l.PageSize <= 0 || l.PageSize > 1000 || l.DirectoryPageSize <= 0 || l.DirectoryPageSize > 1000 {
		return errors.New("space sync: page sizes must be within 1..1000")
	}
	if l.MaxPages <= 0 || l.MaxDirectoryPages <= 0 || l.MaxAuthors <= 0 || l.MaxOperations <= 0 || l.MaxFinalFetches <= 0 || l.MaxRecoveryAttempts <= 0 {
		return errors.New("space sync: count limits must be positive")
	}
	if l.MaxRepoBytes <= 0 || l.MaxIncrementalBytes <= 0 || l.PassTimeout <= 0 || l.CleanupTimeout <= 0 {
		return errors.New("space sync: byte and time limits must be positive")
	}
	if err := l.CAR.Validate(); err != nil {
		return err
	}
	if uint64(l.MaxRepoBytes) > l.CAR.MaxTotalSize {
		return errors.New("space sync: stream byte limit exceeds CAR total-size limit")
	}
	return nil
}

// EventCode is a bounded operational event category.
type EventCode string

const (
	EventSyncStarted      EventCode = "sync_started"
	EventSyncPromoted     EventCode = "sync_promoted"
	EventRecoveryStarted  EventCode = "recovery_started"
	EventSyncIncomplete   EventCode = "sync_incomplete"
	EventQueueSaturated   EventCode = "queue_saturated"
	EventLifecycleChanged EventCode = "lifecycle_changed"
	EventCleanupFailed    EventCode = "cleanup_failed"
	EventLeaseRenewed     EventCode = "lease_renewed"
)

// OperationalEvent contains bounded state only. Error messages, record bodies,
// tokens, proofs, and keys are deliberately excluded.
type OperationalEvent struct {
	Code     EventCode
	Author   atmos.DID
	Revision atmos.TID
	Reason   IncompleteReason
}

// Reporter receives synchronous structured events. Implementations must return
// quickly and must not call back into the Syncer.
type Reporter interface{ Report(OperationalEvent) }

// ReporterFunc adapts a function to Reporter.
type ReporterFunc func(OperationalEvent)

// Report implements Reporter.
func (f ReporterFunc) Report(event OperationalEvent) { f(event) }

// Options configures a Syncer bound to exactly one space.
type Options struct {
	Space      atmos.SpaceRef
	Store      Store
	Source     Source
	Limits     Limits
	Recovery   RecoveryMode
	Reporter   Reporter
	Credential CredentialPurger
}

// Syncer publishes complete verified generations for one exact space.
type Syncer struct {
	space      atmos.SpaceRef
	store      Store
	source     Source
	limits     Limits
	recovery   RecoveryMode
	reporter   Reporter
	credential CredentialPurger
	mu         sync.Mutex
	flights    map[atmos.DID]*syncFlight
}

type syncFlight struct {
	done   chan struct{}
	cancel context.CancelFunc
	repo   Repo
	err    error
}

// New constructs a Syncer without performing network I/O.
func New(opts Options) (*Syncer, error) {
	if err := opts.Space.Validate(); err != nil {
		return nil, fmt.Errorf("space sync: invalid space: %w", err)
	}
	if opts.Store == nil || opts.Source == nil {
		return nil, errors.New("space sync: store and source are required")
	}
	if opts.Credential == nil {
		if purger, ok := opts.Source.(CredentialPurger); ok {
			opts.Credential = purger
		} else {
			return nil, errors.New("space sync: credential purger is required for deletion cleanup")
		}
	}
	if err := opts.Limits.Validate(); err != nil {
		return nil, err
	}
	if opts.Recovery != RecoveryFull && opts.Recovery != RecoveryIndexOnly {
		return nil, errors.New("space sync: invalid recovery mode")
	}
	return &Syncer{space: opts.Space, store: opts.Store, source: opts.Source, limits: opts.Limits, recovery: opts.Recovery, reporter: opts.Reporter, credential: opts.Credential, flights: make(map[atmos.DID]*syncFlight)}, nil
}

// Space returns the immutable space identity.
func (s *Syncer) Space() atmos.SpaceRef { return s.space }

func (s *Syncer) report(event OperationalEvent) {
	if s.reporter != nil {
		s.reporter.Report(event)
	}
}

// SyncRepo performs one bounded direct-host pass for author. It recovers from a
// missing base, pruned/inconsistent history, or a snapshot mismatch, but never
// publishes a partially verified generation.
func (s *Syncer) SyncRepo(ctx context.Context, author atmos.DID) (Repo, error) {
	if err := author.Validate(); err != nil {
		return Repo{}, err
	}
	s.mu.Lock()
	if flight := s.flights[author]; flight != nil {
		s.mu.Unlock()
		select {
		case <-flight.done:
			return flight.repo.Clone(), flight.err
		case <-ctx.Done():
			return Repo{}, ctx.Err()
		}
	}
	flightCtx, flightCancel := context.WithCancel(ctx)
	flight := &syncFlight{done: make(chan struct{}), cancel: flightCancel}
	s.flights[author] = flight
	s.mu.Unlock()
	flight.repo, flight.err = s.syncRepo(flightCtx, author)
	if errors.Is(flight.err, ErrCredentialDenied) {
		maintenance, cancel := s.maintenanceContext(ctx)
		flight.err = errors.Join(flight.err, s.Suspend(maintenance, "credential denied"))
		cancel()
	} else if errors.Is(flight.err, ErrAccountSuspended) {
		maintenance, cancel := s.maintenanceContext(ctx)
		flight.err = errors.Join(flight.err, s.suspendAuthor(maintenance, author, "repo host account status"))
		cancel()
	} else if flight.err != nil && !errors.Is(flight.err, context.Canceled) && !errors.Is(flight.err, context.DeadlineExceeded) && !errors.Is(flight.err, ErrConflict) && !errors.Is(flight.err, ErrDeleted) && !errors.Is(flight.err, ErrSuspended) && !errors.Is(flight.err, ErrRepoAbsent) {
		maintenance, cancel := s.maintenanceContext(ctx)
		flight.err = errors.Join(flight.err, s.markAuthorStale(maintenance, author, "sync pass failed"))
		cancel()
	} else if flight.err == nil {
		maintenance, cancel := s.maintenanceContext(ctx)
		flight.err = s.activateAuthor(maintenance, author, "direct verification succeeded")
		cancel()
	}
	flightCancel()
	s.mu.Lock()
	delete(s.flights, author)
	close(flight.done)
	s.mu.Unlock()
	return flight.repo.Clone(), flight.err
}

func (s *Syncer) syncRepo(ctx context.Context, author atmos.DID) (Repo, error) {
	key := RepoKey{Space: s.space, Author: author}
	if err := key.Validate(); err != nil {
		return Repo{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.limits.PassTimeout)
	defer cancel()
	lifecycle, err := s.store.Lifecycle(ctx, s.space)
	if err != nil {
		return Repo{}, err
	}
	if lifecycle.State == LifecycleDeleted {
		return Repo{}, ErrDeleted
	}
	if lifecycle.State != LifecycleActive && lifecycle.State != LifecycleStale {
		return Repo{}, ErrSuspended
	}
	account, err := s.store.RepoLifecycle(ctx, key)
	if err != nil {
		return Repo{}, err
	}
	if account.State == LifecycleDeleted {
		return Repo{}, ErrDeleted
	}
	if account.State != LifecycleActive && account.State != LifecycleStale {
		return Repo{}, ErrSuspended
	}
	authorSource, err := s.source.OpenAuthor(ctx, author)
	if err != nil {
		return Repo{}, err
	}
	s.report(OperationalEvent{Code: EventSyncStarted, Author: author})

	base, err := s.store.LoadRepo(ctx, key)
	if errors.Is(err, ErrNotFound) {
		base = Repo{Key: key}
	} else if err != nil {
		return Repo{}, err
	}
	stage, err := s.store.Begin(ctx, key, base.Version, lifecycle.Generation)
	if err != nil {
		return Repo{}, err
	}
	keep := false
	defer func() {
		if !keep {
			maintenance, cancel := s.maintenanceContext(ctx)
			_ = s.store.DiscardStage(maintenance, stage.ID)
			cancel()
		}
	}()

	if base.Version == 0 {
		repo, err := s.recover(ctx, authorSource, base, &stage)
		if err == nil {
			keep = true
		}
		return repo, err
	}
	checkpoint, unchanged, err := s.incremental(ctx, authorSource, base, &stage)
	if err != nil {
		var recoverable *recoveryRequiredError
		if !errors.As(err, &recoverable) {
			s.reportIncomplete(err, author)
			return Repo{}, err
		}
		if discardErr := s.store.DiscardStage(ctx, stage.ID); discardErr != nil {
			return Repo{}, errors.Join(err, discardErr)
		}
		stage, err = s.store.Begin(ctx, key, base.Version, lifecycle.Generation)
		if err != nil {
			return Repo{}, err
		}
		repo, err := s.recover(ctx, authorSource, base, &stage)
		if err == nil {
			keep = true
		}
		return repo, err
	}
	if unchanged {
		return base.Clone(), nil
	}
	repo, err := s.store.Promote(ctx, stage.ID, checkpoint)
	if err != nil {
		return Repo{}, err
	}
	keep = true
	s.report(OperationalEvent{Code: EventSyncPromoted, Author: author, Revision: repo.Checkpoint.Revision})
	return repo, nil
}

func (s *Syncer) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, flight := range s.flights {
		flight.cancel()
	}
}

func (s *Syncer) cancelAuthor(author atmos.DID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if flight := s.flights[author]; flight != nil {
		flight.cancel()
	}
}

func (s *Syncer) maintenanceContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), s.limits.CleanupTimeout)
}

func (s *Syncer) incremental(ctx context.Context, source AuthorSource, base Repo, stage *Stage) (Checkpoint, bool, error) {
	since := base.Checkpoint.Revision
	cursor := ""
	seenCursors := make(map[string]struct{})
	operations := 0
	var incrementalBytes int64
	var terminal *spaces.SignedCommit
	lastRevision := since
	for pageNumber := 0; pageNumber < s.limits.MaxPages; pageNumber++ {
		page, err := source.ListRepoOps(ctx, since, s.limits.PageSize, cursor, false)
		if err != nil {
			return Checkpoint{}, false, err
		}
		if operations > s.limits.MaxOperations-len(page.Operations) {
			return Checkpoint{}, false, &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("operation limit exceeded")}
		}
		for i := range page.Operations {
			op := page.Operations[i]
			if int64(len(op.Body)) > s.limits.MaxIncrementalBytes-incrementalBytes {
				return Checkpoint{}, false, &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("incremental byte limit exceeded")}
			}
			incrementalBytes += int64(len(op.Body))
			if op.Revision.Integer() <= since.Integer() || op.Revision.Integer() < lastRevision.Integer() {
				return Checkpoint{}, false, &IntegrityError{Cause: errors.New("oplog revisions are not ordered after since")}
			}
			if err := applyOperation(stage, op); err != nil {
				return Checkpoint{}, false, &recoveryRequiredError{cause: err}
			}
			if len(stage.Index) > s.limits.CAR.MaxRecords {
				return Checkpoint{}, false, &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("record limit exceeded")}
			}
			if uint64(len(op.Body)) > s.limits.CAR.MaxRecordSize {
				return Checkpoint{}, false, &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("record body limit exceeded")}
			}
			lastRevision = op.Revision
			operations++
		}
		stage.State = mustRepoState(stage.Index)
		if err := s.validateStageBounds(*stage); err != nil {
			return Checkpoint{}, false, err
		}
		if err := s.store.SaveStage(ctx, *stage); err != nil {
			return Checkpoint{}, false, err
		}
		if page.Cursor != "" {
			if page.Commit != nil {
				return Checkpoint{}, false, &IntegrityError{Cause: errors.New("nonterminal page included commit")}
			}
			if page.Cursor == cursor {
				return Checkpoint{}, false, &IncompleteError{Reason: IncompletePagination, Cause: errors.New("cursor made no progress")}
			}
			if _, exists := seenCursors[page.Cursor]; exists {
				return Checkpoint{}, false, &IncompleteError{Reason: IncompletePagination, Cause: errors.New("cursor cycle")}
			}
			seenCursors[page.Cursor] = struct{}{}
			cursor = page.Cursor
			continue
		}
		if page.Commit == nil {
			return Checkpoint{}, false, &IncompleteError{Reason: IncompleteMissingCommit}
		}
		terminal = page.Commit
		break
	}
	if terminal == nil {
		return Checkpoint{}, false, &IncompleteError{Reason: IncompletePagination, Cause: errors.New("page limit reached")}
	}
	if terminal.Rev.Integer() < base.Checkpoint.Revision.Integer() || terminal.Rev.Integer() < lastRevision.Integer() {
		return Checkpoint{}, false, &IntegrityError{Cause: errors.New("terminal commit revision rolled back")}
	}
	if terminal.Rev == base.Checkpoint.Revision {
		if !bytes.Equal(terminal.Hash[:], base.Checkpoint.Hash[:]) {
			return Checkpoint{}, false, &IntegrityError{Cause: errors.New("equal revision has a different hash")}
		}
		if operations != 0 {
			return Checkpoint{}, false, &IntegrityError{Cause: errors.New("operations present at unchanged revision")}
		}
		return Checkpoint{}, true, nil
	}
	if operations == 0 {
		return Checkpoint{}, false, &recoveryRequiredError{cause: errors.New("new head without retained operations")}
	}
	resolved := source.Resolved()
	if _, err := spaces.VerifyCommit(*terminal, spaces.CommitContext{Space: s.space, Author: base.Key.Author, Rev: terminal.Rev}, resolved.Key); err != nil {
		return Checkpoint{}, false, err
	}
	repoCommit, err := spaces.NewRepoCommitFromState(stage.State[:])
	if err != nil {
		return Checkpoint{}, false, err
	}
	if !repoCommit.Matches(*terminal) {
		return Checkpoint{}, false, &recoveryRequiredError{cause: errors.New("staged LtHash does not match terminal commit")}
	}
	if err := s.completeFinalValues(ctx, source, stage); err != nil {
		return Checkpoint{}, false, err
	}
	return checkpointFrom(*terminal, stage.State, resolved.Provenance), false, nil
}

type recoveryRequiredError struct{ cause error }

func (e *recoveryRequiredError) Error() string {
	return "space sync: snapshot recovery required: " + e.cause.Error()
}
func (e *recoveryRequiredError) Unwrap() error { return e.cause }

func applyOperation(stage *Stage, op Operation) error {
	if err := op.Revision.Validate(); err != nil {
		return err
	}
	if err := op.Path.Validate(); err != nil {
		return err
	}
	current, exists := stage.Index[op.Path]
	switch {
	case op.Prev == nil && op.CID != nil:
		if exists {
			return fmt.Errorf("create path %s already exists", op.Path)
		}
	case op.Prev != nil && op.CID != nil:
		if !exists || !current.Equal(*op.Prev) {
			return fmt.Errorf("update prev mismatch at %s", op.Path)
		}
	case op.Prev != nil && op.CID == nil:
		if !exists || !current.Equal(*op.Prev) {
			return fmt.Errorf("delete prev mismatch at %s", op.Path)
		}
	default:
		return errors.New("operation has neither prev nor CID")
	}
	if op.CID != nil && len(op.Body) != 0 {
		if err := validateBody(*op.CID, op.Body); err != nil {
			return err
		}
	}
	repoCommit, err := spaces.NewRepoCommitFromState(stage.State[:])
	if err != nil {
		return err
	}
	if err := repoCommit.Apply(spaces.RepoOp{Collection: op.Path.Collection().String(), RKey: op.Path.RecordKey().String(), CID: op.CID, Prev: op.Prev}); err != nil {
		return err
	}
	stage.State = repoCommit.State()
	if op.CID == nil {
		delete(stage.Index, op.Path)
		delete(stage.Records, op.Path)
		return nil
	}
	stage.Index[op.Path] = *op.CID
	if len(op.Body) == 0 {
		if record, ok := stage.Records[op.Path]; !ok || !record.CID.Equal(*op.CID) {
			delete(stage.Records, op.Path)
		}
		return nil
	}
	stage.Records[op.Path] = Record{CID: *op.CID, Data: append([]byte(nil), op.Body...)}
	return nil
}

func (s *Syncer) completeFinalValues(ctx context.Context, source AuthorSource, stage *Stage) error {
	missing := 0
	for _, path := range stage.Index.Paths() {
		expected := stage.Index[path]
		if record, ok := stage.Records[path]; ok && record.CID.Equal(expected) {
			continue
		}
		missing++
		if missing > s.limits.MaxFinalFetches {
			return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("final-value fetch limit exceeded")}
		}
		record, err := source.GetRecord(ctx, path)
		if err != nil {
			return &IncompleteError{Reason: IncompleteFinalValue, Cause: err}
		}
		if !record.CID.Equal(expected) {
			return &IncompleteError{Reason: IncompleteFinalValue, Cause: errors.New("concurrent final record change")}
		}
		if uint64(len(record.Data)) > s.limits.CAR.MaxRecordSize {
			return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("final record body limit exceeded")}
		}
		if err := validateBody(expected, record.Data); err != nil {
			return &IntegrityError{Cause: err}
		}
		stage.Records[path] = record.Clone()
	}
	if err := s.validateStageBounds(*stage); err != nil {
		return err
	}
	if err := s.store.SaveStage(ctx, *stage); err != nil {
		return err
	}
	return nil
}

func (s *Syncer) validateStageBounds(stage Stage) error {
	if len(stage.Index) > s.limits.CAR.MaxRecords {
		return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("record limit exceeded")}
	}
	var total uint64
	for _, record := range stage.Records {
		if uint64(len(record.Data)) > s.limits.CAR.MaxRecordSize {
			return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("record body limit exceeded")}
		}
		if uint64(len(record.Data)) > uint64(s.limits.MaxRepoBytes)-total {
			return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("repository value bytes exceed limit")}
		}
		total += uint64(len(record.Data))
	}
	return nil
}

func (s *Syncer) recover(ctx context.Context, source AuthorSource, base Repo, stage *Stage) (Repo, error) {
	s.report(OperationalEvent{Code: EventRecoveryStarted, Author: stage.Key.Author})
	var last error
	for attempt := 0; attempt < s.limits.MaxRecoveryAttempts; attempt++ {
		commit, err := source.GetLatestCommit(ctx)
		if err != nil {
			return Repo{}, err
		}
		if base.Version != 0 {
			if commit.Rev.Integer() < base.Checkpoint.Revision.Integer() {
				return Repo{}, &IntegrityError{Cause: errors.New("snapshot head rolled back")}
			}
			if commit.Rev == base.Checkpoint.Revision && !bytes.Equal(commit.Hash[:], base.Checkpoint.Hash[:]) {
				return Repo{}, &IntegrityError{Cause: errors.New("snapshot head equivocates at equal revision")}
			}
		}
		resolved := source.Resolved()
		mode := spaces.CARFull
		if s.recovery == RecoveryIndexOnly && base.Version != 0 {
			mode = spaces.CARIndexOnly
		}
		body, err := source.GetRepo(ctx, mode == spaces.CARIndexOnly, s.limits.MaxRepoBytes)
		if err != nil {
			last = err
			continue
		}
		stage.Index = make(spaces.RepoIndex)
		stage.Records = make(map[spaces.RecordPath]Record)
		verified, verifyErr := spaces.VerifyRepoCAR(ctx, body, spaces.VerifyRepoOptions{Context: spaces.CommitContext{Space: s.space, Author: stage.Key.Author, Rev: commit.Rev}, Key: resolved.Key, Mode: mode, Limits: s.limits.CAR}, func(record spaces.VerifiedRecord) error {
			stage.Records[record.Path] = Record{CID: record.CID, Data: append([]byte(nil), record.Data...)}
			return nil
		})
		closeErr := body.Close()
		if verifyErr != nil || closeErr != nil {
			last = errors.Join(verifyErr, closeErr)
			continue
		}
		if verified.Commit.SignedCommit.Rev != commit.Rev || !bytes.Equal(verified.Commit.SignedCommit.Hash[:], commit.Hash[:]) {
			last = errors.New("CAR commit changed after latest-commit read")
			continue
		}
		stage.Index, stage.State = verified.Index.Clone(), verified.State
		if mode == spaces.CARIndexOnly {
			for path, cid := range stage.Index {
				if old, ok := base.Records[path]; ok && old.CID.Equal(cid) {
					stage.Records[path] = old.Clone()
				}
			}
			if err := s.completeFinalValues(ctx, source, stage); err != nil {
				last = err
				continue
			}
		}
		if err := s.validateStageBounds(*stage); err != nil {
			last = err
			continue
		}
		if err := s.store.SaveStage(ctx, *stage); err != nil {
			return Repo{}, err
		}
		checkpoint := checkpointFrom(verified.Commit.SignedCommit, verified.State, resolved.Provenance)
		repo, err := s.store.Promote(ctx, stage.ID, checkpoint)
		if err != nil {
			return Repo{}, err
		}
		s.report(OperationalEvent{Code: EventSyncPromoted, Author: stage.Key.Author, Revision: repo.Checkpoint.Revision})
		return repo, nil
	}
	err := &IncompleteError{Reason: IncompleteSnapshot, Cause: last}
	s.reportIncomplete(err, stage.Key.Author)
	return Repo{}, err
}

func checkpointFrom(commit spaces.SignedCommit, state [spaces.LtHashStateSize]byte, provenance Provenance) Checkpoint {
	return Checkpoint{Revision: commit.Rev, Hash: commit.Hash, State: state, IndexComplete: true, ValuesComplete: true, Provenance: provenance}
}

func validateBody(expected cbor.CID, data []byte) error {
	value, err := cbor.Unmarshal(data)
	if err != nil {
		return fmt.Errorf("invalid DAG-CBOR record: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return errors.New("record body is not a DAG-CBOR map")
	}
	canonical, err := cbor.Marshal(value)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return errors.New("record body is not canonical DAG-CBOR")
	}
	actual := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	if !actual.Equal(expected) {
		return fmt.Errorf("record CID mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

func mustRepoState(index spaces.RepoIndex) [spaces.LtHashStateSize]byte {
	repo, err := spaces.NewRepoCommitFromIndex(index)
	if err != nil {
		panic("space sync: validated staged index became invalid: " + err.Error())
	}
	return repo.State()
}

func (s *Syncer) reportIncomplete(err error, author atmos.DID) {
	var incomplete *IncompleteError
	if errors.As(err, &incomplete) {
		s.report(OperationalEvent{Code: EventSyncIncomplete, Author: author, Reason: incomplete.Reason})
	}
}

// ReadRepo applies the explicit Q4 serving contract. Active verified data may
// be returned directly. Suspended data requires application authorization.
// Deleted data is never returned.
func (s *Syncer) ReadRepo(ctx context.Context, author atmos.DID, authorize ServeAuthorization) (Repo, error) {
	key := RepoKey{Space: s.space, Author: author}
	lifecycle, err := s.store.Lifecycle(ctx, s.space)
	if err != nil {
		return Repo{}, err
	}
	account, err := s.store.RepoLifecycle(ctx, key)
	if err != nil {
		return Repo{}, err
	}
	if lifecycle.State == LifecycleDeleted || account.State == LifecycleDeleted {
		return Repo{}, ErrDeleted
	}
	if lifecycle.State != LifecycleActive || account.State != LifecycleActive {
		if authorize == nil {
			return Repo{}, ErrSuspended
		}
		blocked := lifecycle
		if account.State != LifecycleActive {
			blocked = account
		}
		if err := authorize(ctx, key, blocked); err != nil {
			return Repo{}, fmt.Errorf("space sync: offline serving denied: %w", err)
		}
	}
	repo, err := s.store.LoadRepo(ctx, key)
	if err != nil {
		return Repo{}, err
	}
	if !repo.Checkpoint.IndexComplete || !repo.Checkpoint.ValuesComplete {
		return Repo{}, errors.New("space sync: store returned an incomplete published repo")
	}
	return repo, nil
}

// OutboxConsumer handles idempotent publication events.
type OutboxConsumer interface {
	Consume(context.Context, Event) error
}

// DeliverOutbox delivers up to limit events in order, acknowledging each only
// after success. A crash after Consume and before Ack causes safe redelivery.
func (s *Syncer) DeliverOutbox(ctx context.Context, consumer OutboxConsumer, limit int) (int, error) {
	if consumer == nil {
		return 0, errors.New("space sync: outbox consumer is required")
	}
	events, err := s.store.PeekEvents(ctx, limit)
	if err != nil {
		return 0, err
	}
	for i := range events {
		if err := consumer.Consume(ctx, events[i]); err != nil {
			return i, err
		}
		if err := s.store.AckEvent(ctx, events[i].ID); err != nil {
			return i, err
		}
	}
	return len(events), nil
}
