package sync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jcalabro/atmos"
)

// AccountEventKind identifies a trusted lifecycle event supplied by the
// embedding application's account/identity integration.
type AccountEventKind uint8

const (
	AccountActivated AccountEventKind = iota + 1
	AccountDeactivated
	AccountTakenDown
	AccountDeleted
	AccountIdentityChanged
)

// ApplyVerifiedSpaceDeletion consumes an authority-authenticated deletion
// signal. The caller must first verify the expected authority issuer, audience,
// exact method, and named space. The name makes that trust boundary explicit.
func (s *Syncer) ApplyVerifiedSpaceDeletion(ctx context.Context, reason string) error {
	lifecycle, err := s.store.Lifecycle(ctx, s.space)
	if err != nil {
		return err
	}
	if lifecycle.State != LifecycleDeleted {
		lifecycle, err = s.store.TransitionLifecycle(ctx, s.space, lifecycle.Generation, LifecycleDeleted, reason)
		if err != nil {
			return err
		}
	}
	s.cancelAll()
	s.report(OperationalEvent{Code: EventLifecycleChanged})
	return s.finishSpaceCleanup(ctx, lifecycle)
}

// Suspend stops new network work while retaining the last verified copy. It is
// suitable for credential expiry/denial, policy denial, deactivation, or a
// caller-selected transient-outage policy. It does not authorize offline serve.
func (s *Syncer) Suspend(ctx context.Context, reason string) error {
	lifecycle, err := s.store.Lifecycle(ctx, s.space)
	if err != nil {
		return err
	}
	if lifecycle.State == LifecycleDeleted {
		return ErrDeleted
	}
	if lifecycle.State == LifecycleSuspended {
		return nil
	}
	_, err = s.store.TransitionLifecycle(ctx, s.space, lifecycle.Generation, LifecycleSuspended, reason)
	if err == nil {
		s.cancelAll()
		s.report(OperationalEvent{Code: EventLifecycleChanged})
	}
	return err
}

// Resume re-enables network work after the embedding application has restored
// access. A deletion tombstone cannot be resumed or reused.
func (s *Syncer) Resume(ctx context.Context, reason string) error {
	lifecycle, err := s.store.Lifecycle(ctx, s.space)
	if err != nil {
		return err
	}
	if lifecycle.State == LifecycleDeleted {
		return ErrDeleted
	}
	if lifecycle.State == LifecycleActive {
		return nil
	}
	_, err = s.store.TransitionLifecycle(ctx, s.space, lifecycle.Generation, LifecycleActive, reason)
	if err == nil {
		s.report(OperationalEvent{Code: EventLifecycleChanged})
	}
	return err
}

// ApplyTrustedAccountEvent fences one author's repository. These events must
// come from the embedding application's trusted public account/identity event
// integration; arbitrary repo-host error strings are not lifecycle authority.
func (s *Syncer) ApplyTrustedAccountEvent(ctx context.Context, key RepoKey, kind AccountEventKind, reason string) error {
	if key.Space != s.space {
		return errors.New("space sync: account event targets another space")
	}
	if err := key.Validate(); err != nil {
		return err
	}
	current, err := s.store.RepoLifecycle(ctx, key)
	if err != nil {
		return err
	}
	var target LifecycleState
	switch kind {
	case AccountActivated, AccountIdentityChanged:
		target = LifecycleActive
	case AccountDeactivated, AccountTakenDown:
		target = LifecycleSuspended
	case AccountDeleted:
		target = LifecycleDeleted
	default:
		return fmt.Errorf("space sync: invalid account event kind %d", kind)
	}
	if current.State == LifecycleDeleted && target != LifecycleDeleted {
		return ErrDeleted
	}
	_, err = s.store.TransitionRepoLifecycle(ctx, key, current.Generation, target, reason)
	if err != nil {
		return err
	}
	s.cancelAuthor(key.Author)
	s.report(OperationalEvent{Code: EventLifecycleChanged, Author: key.Author})
	if target == LifecycleDeleted {
		if err := s.store.PurgeRepo(ctx, key); err != nil {
			s.report(OperationalEvent{Code: EventCleanupFailed, Author: key.Author})
			return err
		}
	}
	return nil
}

func (s *Syncer) suspendAuthor(ctx context.Context, author atmos.DID, reason string) error {
	key := RepoKey{Space: s.space, Author: author}
	current, err := s.store.RepoLifecycle(ctx, key)
	if err != nil {
		return err
	}
	if current.State == LifecycleDeleted {
		return ErrDeleted
	}
	if current.State == LifecycleSuspended {
		return nil
	}
	_, err = s.store.TransitionRepoLifecycle(ctx, key, current.Generation, LifecycleSuspended, reason)
	if err == nil {
		s.cancelAuthor(author)
		s.report(OperationalEvent{Code: EventLifecycleChanged, Author: author})
	}
	return err
}

func (s *Syncer) markAuthorStale(ctx context.Context, author atmos.DID, reason string) error {
	key := RepoKey{Space: s.space, Author: author}
	current, err := s.store.RepoLifecycle(ctx, key)
	if err != nil {
		return err
	}
	if current.State == LifecycleDeleted {
		return ErrDeleted
	}
	if current.State == LifecycleSuspended || current.State == LifecycleStale {
		return nil
	}
	_, err = s.store.TransitionRepoLifecycle(ctx, key, current.Generation, LifecycleStale, reason)
	if err == nil {
		s.report(OperationalEvent{Code: EventLifecycleChanged, Author: author})
	}
	return err
}

func (s *Syncer) activateAuthor(ctx context.Context, author atmos.DID, reason string) error {
	key := RepoKey{Space: s.space, Author: author}
	current, err := s.store.RepoLifecycle(ctx, key)
	if err != nil {
		return err
	}
	if current.State == LifecycleDeleted {
		return ErrDeleted
	}
	if current.State == LifecycleSuspended {
		return nil
	}
	if current.State == LifecycleActive {
		return nil
	}
	_, err = s.store.TransitionRepoLifecycle(ctx, key, current.Generation, LifecycleActive, reason)
	if err == nil {
		s.report(OperationalEvent{Code: EventLifecycleChanged, Author: author})
	}
	return err
}

// ResumeCleanup retries durable deletion work left visible by a prior failure
// or process crash. It never interprets ordinary HTTP errors as deletion.
func (s *Syncer) ResumeCleanup(ctx context.Context) error {
	lifecycle, err := s.store.Lifecycle(ctx, s.space)
	if err != nil {
		return err
	}
	if lifecycle.State == LifecycleDeleted && lifecycle.CleanupPending {
		return s.finishSpaceCleanup(ctx, lifecycle)
	}
	keys, err := s.store.ListRepos(ctx, s.space)
	if err != nil {
		return err
	}
	var joined error
	for _, key := range keys {
		state, stateErr := s.store.RepoLifecycle(ctx, key)
		if stateErr != nil {
			joined = errors.Join(joined, stateErr)
			continue
		}
		if state.State == LifecycleDeleted && state.CleanupPending {
			joined = errors.Join(joined, s.store.PurgeRepo(ctx, key))
		}
	}
	return joined
}

func (s *Syncer) finishSpaceCleanup(ctx context.Context, lifecycle Lifecycle) error {
	if lifecycle.State != LifecycleDeleted {
		return errors.New("space sync: cleanup requires deletion tombstone")
	}
	if s.credential != nil {
		if err := s.credential.PurgeCredential(ctx, s.space); err != nil {
			s.report(OperationalEvent{Code: EventCleanupFailed})
			return fmt.Errorf("space sync: purge credential material: %w", err)
		}
	}
	if err := s.store.PurgeSpace(ctx, s.space); err != nil {
		s.report(OperationalEvent{Code: EventCleanupFailed})
		return err
	}
	return nil
}
