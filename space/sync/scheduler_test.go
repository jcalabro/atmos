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
