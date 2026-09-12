package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/stretchr/testify/require"
)

func TestSchedulerDirectlyPollsKnownRepoWhenDirectoryOmitsIt(t *testing.T) {
	t.Parallel()
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	source.directory = map[string]RepoPage{"": {}}
	commit := source.commit
	source.pages = map[string]OperationPage{"": {Commit: &commit}}
	results := make(chan JobResult, 4)
	scheduler, err := NewScheduler(syncer, SchedulerOptions{Workers: 1, QueueCapacity: 1, SweepInterval: time.Hour, RegistrationCheckInterval: time.Hour, OnResult: func(result JobResult) { results <- result }})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	select {
	case result := <-results:
		require.NoError(t, result.Err)
		require.Equal(t, testAuthor, result.Author)
	case <-time.After(time.Second):
		t.Fatal("known repo was not directly polled")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestSchedulerQueueSaturationIsTypedAndReconciledBySweep(t *testing.T) {
	t.Parallel()
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	scheduler, err := NewScheduler(syncer, SchedulerOptions{Workers: 1, QueueCapacity: 1, SweepInterval: time.Hour, RegistrationCheckInterval: time.Hour, OnResult: func(JobResult) {}})
	require.NoError(t, err)
	require.NoError(t, scheduler.Hint(testAuthor))
	err = scheduler.Hint(atmos.DID("did:plc:other"))
	var full *QueueFullError
	require.ErrorAs(t, err, &full)
}

func TestSchedulerStillQueuesKnownReposDuringDirectoryOutage(t *testing.T) {
	t.Parallel()
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	source.directoryErr = errors.New("authority unavailable")
	scheduler, err := NewScheduler(syncer, SchedulerOptions{Workers: 1, QueueCapacity: 1, SweepInterval: time.Hour, RegistrationCheckInterval: time.Hour, OnResult: func(JobResult) {}})
	require.NoError(t, err)
	err = scheduler.Sweep(context.Background())
	require.ErrorContains(t, err, "authority unavailable")
	select {
	case author := <-scheduler.queue:
		require.Equal(t, testAuthor, author)
	default:
		t.Fatal("known repo was not queued independently")
	}
}

func TestSchedulerCoalescedRerunDoesNotDeadlockOnFullQueue(t *testing.T) {
	t.Parallel()
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 40, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	// The single worker parks in OnResult after each sync and before it reads
	// the dirty bit, making the interleaving below deterministic.
	entered := make(chan atmos.DID)
	proceed := make(chan struct{})
	scheduler, err := NewScheduler(syncer, SchedulerOptions{Workers: 1, QueueCapacity: 1, SweepInterval: time.Hour, RegistrationCheckInterval: time.Hour, OnResult: func(result JobResult) { entered <- result.Author; <-proceed }})
	require.NoError(t, err)
	other := atmos.DID("did:plc:other")
	require.NoError(t, scheduler.Hint(testAuthor))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); scheduler.worker(ctx) }()
	receive := func(want atmos.DID) {
		t.Helper()
		select {
		case author := <-entered:
			require.Equal(t, want, author)
		case <-time.After(5 * time.Second):
			t.Fatal("scheduler made no progress; coalesced rerun deadlocked the worker")
		}
	}
	release := func() {
		t.Helper()
		select {
		case proceed <- struct{}{}:
		case <-time.After(5 * time.Second):
			t.Fatal("worker never resumed from the result handler")
		}
	}
	receive(testAuthor)
	// The worker is parked after syncing testAuthor: coalesce a rerun for it and
	// fill the only queue slot with someone else. A blocking rerun re-enqueue
	// would deadlock the sole worker once released.
	require.NoError(t, scheduler.Hint(testAuthor))
	require.NoError(t, scheduler.Hint(other))
	release()
	receive(testAuthor)
	release()
	receive(other)
	release()
	cancel()
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not exit on cancellation")
	}
}

func TestSchedulerPersistsCallbackLease(t *testing.T) {
	t.Parallel()
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{}, spaces.CARFull)
	source.registerAt = time.Now().Add(time.Hour)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	scheduler, err := NewScheduler(syncer, SchedulerOptions{Workers: 1, QueueCapacity: 1, SweepInterval: time.Hour, RegistrationCheckInterval: time.Hour, RegistrationRenewBefore: time.Minute, CallbackServiceID: "did:plc:callback#space", CallbackServiceType: "SpaceCallback", OnResult: func(JobResult) {}})
	require.NoError(t, err)
	require.NoError(t, scheduler.renewRegistration(context.Background()))
	lease, err := store.GetLease(context.Background(), testSpace)
	require.NoError(t, err)
	require.Equal(t, "did:plc:callback#space", lease.ServiceID)
	require.Equal(t, source.registerAt, lease.ExpiresAt)
}
