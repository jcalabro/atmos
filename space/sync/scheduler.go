package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
)

// JobResult makes every asynchronous sync outcome visible to the embedding
// application without logging sensitive error text in structured events.
type JobResult struct {
	Author atmos.DID
	Repo   Repo
	Err    error
}

// SchedulerOptions configures bounded discovery, direct polling, callback
// renewal, and worker concurrency.
type SchedulerOptions struct {
	Workers                   int
	QueueCapacity             int
	SweepInterval             time.Duration
	RegistrationCheckInterval time.Duration
	RegistrationRenewBefore   time.Duration
	CallbackServiceID         string
	CallbackServiceType       string
	OnResult                  func(JobResult)
}

// AlphaSchedulerOptions returns bounded first-release scheduling settings with
// the required result handler installed. Callback registration remains opt-in;
// callers set both callback fields together after constructing the profile.
func AlphaSchedulerOptions(onResult func(JobResult)) SchedulerOptions {
	return SchedulerOptions{
		Workers:                   32,
		QueueCapacity:             10_000,
		SweepInterval:             5 * time.Minute,
		RegistrationCheckInterval: time.Hour,
		RegistrationRenewBefore:   10 * time.Minute,
		OnResult:                  onResult,
	}
}

// Validate checks scheduler bounds and makes background failures observable.
func (o SchedulerOptions) Validate() error {
	if o.Workers <= 0 || o.QueueCapacity <= 0 || o.SweepInterval <= 0 || o.RegistrationCheckInterval <= 0 || o.RegistrationRenewBefore < 0 {
		return errors.New("space sync: scheduler counts and intervals must be positive")
	}
	if o.OnResult == nil {
		return errors.New("space sync: scheduler result handler is required")
	}
	if (o.CallbackServiceID == "") != (o.CallbackServiceType == "") {
		return errors.New("space sync: callback service ID and type must be provided together")
	}
	return nil
}

// QueueFullError reports visible backpressure. The next periodic sweep remains
// the reconciliation mechanism.
type QueueFullError struct{ Author atmos.DID }

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("space sync: work queue full for %s", e.Author)
}

// Scheduler owns bounded dirty-repo work for a Syncer.
type Scheduler struct {
	syncer *Syncer
	opts   SchedulerOptions
	queue  chan atmos.DID
	mu     sync.Mutex
	dirty  map[atmos.DID]bool
}

// NewScheduler constructs a scheduler. Run owns its goroutines and waits for
// all of them before returning.
func NewScheduler(syncer *Syncer, opts SchedulerOptions) (*Scheduler, error) {
	if syncer == nil {
		return nil, errors.New("space sync: syncer is required")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &Scheduler{syncer: syncer, opts: opts, queue: make(chan atmos.DID, opts.QueueCapacity), dirty: make(map[atmos.DID]bool)}, nil
}

// Hint coalesces a notification into bounded work. Revision/hash are hints and
// deliberately never mutate the durable checkpoint.
func (s *Scheduler) Hint(author atmos.DID) error {
	if err := author.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, present := s.dirty[author]; present {
		s.dirty[author] = true
		return nil
	}
	select {
	case s.queue <- author:
		s.dirty[author] = false
		return nil
	default:
		s.syncer.report(OperationalEvent{Code: EventQueueSaturated, Author: author})
		return &QueueFullError{Author: author}
	}
}

// Sweep enumerates the mutable authority directory and independently schedules
// every already-known repo. An omission never deletes local data.
func (s *Scheduler) Sweep(ctx context.Context) error {
	known, err := s.syncer.store.ListRepos(ctx, s.syncer.space)
	if err != nil {
		return err
	}
	authors := make(map[atmos.DID]struct{}, len(known))
	if len(known) > s.syncer.limits.MaxAuthors {
		return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("known-author limit exceeded")}
	}
	knownAuthors := make(map[atmos.DID]struct{}, len(known))
	var joined error
	for _, key := range known {
		authors[key.Author] = struct{}{}
		knownAuthors[key.Author] = struct{}{}
		joined = errors.Join(joined, s.Hint(key.Author))
	}
	cursor := ""
	seen := make(map[string]struct{})
	for pageNumber := 0; pageNumber < s.syncer.limits.MaxDirectoryPages; pageNumber++ {
		page, err := s.syncer.source.ListRepos(ctx, s.syncer.limits.DirectoryPageSize, cursor)
		if err != nil {
			return errors.Join(joined, err)
		}
		for _, hint := range page.Repos {
			if err := hint.Author.Validate(); err != nil {
				return &IntegrityError{Cause: fmt.Errorf("invalid directory author: %w", err)}
			}
			if err := hint.Revision.Validate(); err != nil {
				return &IntegrityError{Cause: fmt.Errorf("invalid directory revision: %w", err)}
			}
			authors[hint.Author] = struct{}{}
			if len(authors) > s.syncer.limits.MaxAuthors {
				return &IncompleteError{Reason: IncompleteLimits, Cause: errors.New("author limit exceeded")}
			}
		}
		if page.Cursor == "" {
			break
		}
		if page.Cursor == cursor {
			return &IncompleteError{Reason: IncompletePagination, Cause: errors.New("directory cursor made no progress")}
		}
		if _, exists := seen[page.Cursor]; exists {
			return &IncompleteError{Reason: IncompletePagination, Cause: errors.New("directory cursor cycle")}
		}
		seen[page.Cursor] = struct{}{}
		cursor = page.Cursor
		if pageNumber == s.syncer.limits.MaxDirectoryPages-1 {
			return &IncompleteError{Reason: IncompletePagination, Cause: errors.New("directory page limit reached")}
		}
	}
	for author := range authors {
		if _, alreadyKnown := knownAuthors[author]; !alreadyKnown {
			joined = errors.Join(joined, s.Hint(author))
		}
	}
	return joined
}

// Run performs restart cleanup, immediate bootstrap, periodic directory/direct
// sweeps, independent callback renewal, and bounded worker execution.
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.syncer.store.DiscardUnfinished(ctx, s.syncer.space); err != nil {
		return err
	}
	if err := s.syncer.ResumeCleanup(ctx); err != nil {
		return err
	}
	var workers sync.WaitGroup
	for range s.opts.Workers {
		workers.Add(1)
		go func() { defer workers.Done(); s.worker(ctx) }()
	}
	defer workers.Wait()
	if err := s.Sweep(ctx); err != nil {
		s.opts.OnResult(JobResult{Err: err})
	}
	if err := s.renewRegistration(ctx); err != nil {
		s.opts.OnResult(JobResult{Err: err})
	}
	sweep := time.NewTicker(s.opts.SweepInterval)
	defer sweep.Stop()
	registration := time.NewTicker(s.opts.RegistrationCheckInterval)
	defer registration.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sweep.C:
			if err := s.Sweep(ctx); err != nil {
				s.opts.OnResult(JobResult{Err: err})
			}
		case <-registration.C:
			if err := s.renewRegistration(ctx); err != nil {
				s.opts.OnResult(JobResult{Err: err})
			}
		}
	}
}

func (s *Scheduler) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case author := <-s.queue:
			// Coalesced reruns execute in this worker rather than re-entering the
			// queue: a blocking re-send from the only goroutines that drain the
			// queue can deadlock every worker once the queue is full. The dirty
			// entry stays present (value false) across the rerun so concurrent
			// Hint calls keep coalescing instead of enqueueing a duplicate.
			for {
				repo, err := s.syncer.SyncRepo(ctx, author)
				s.opts.OnResult(JobResult{Author: author, Repo: repo, Err: err})
				s.mu.Lock()
				rerun := s.dirty[author]
				if rerun {
					s.dirty[author] = false
				} else {
					delete(s.dirty, author)
				}
				s.mu.Unlock()
				if !rerun {
					break
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}
}

func (s *Scheduler) renewRegistration(ctx context.Context) error {
	if s.opts.CallbackServiceID == "" {
		return nil
	}
	registrar, ok := s.syncer.source.(CallbackRegistrar)
	if !ok {
		return errors.New("space sync: source does not support callback registration")
	}
	lease, err := s.syncer.store.GetLease(ctx, s.syncer.space)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil && lease.ServiceID == s.opts.CallbackServiceID && time.Until(lease.ExpiresAt) > s.opts.RegistrationRenewBefore {
		return nil
	}
	expires, err := registrar.RegisterNotify(ctx, s.opts.CallbackServiceID, s.opts.CallbackServiceType)
	if err != nil {
		return err
	}
	if !expires.After(time.Now()) {
		return errors.New("space sync: callback registration already expired")
	}
	if err := s.syncer.store.SetLease(ctx, s.syncer.space, Lease{ServiceID: s.opts.CallbackServiceID, ExpiresAt: expires}); err != nil {
		return err
	}
	s.syncer.report(OperationalEvent{Code: EventLeaseRenewed})
	return nil
}
