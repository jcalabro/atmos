package sync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/stretchr/testify/require"
)

const (
	testSpace  = atmos.SpaceRef("at://did:plc:authority/space/com.example.space/main")
	testAuthor = atmos.DID("did:plc:author")
)

type sourceFixture struct {
	mu            sync.Mutex
	key           crypto.PrivateKey
	records       map[spaces.RecordPath]Record
	commit        spaces.SignedCommit
	car           []byte
	pages         map[string]OperationPage
	directory     map[string]RepoPage
	directoryErr  error
	seenCursors   []string
	recordFetches int
	indexRequests int
	latestBlock   chan struct{}
	registerAt    time.Time
}

func (s *sourceFixture) ListRepos(_ context.Context, _ int, cursor string) (RepoPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.directoryErr != nil {
		return RepoPage{}, s.directoryErr
	}
	page, ok := s.directory[cursor]
	if !ok {
		return RepoPage{}, errors.New("unexpected directory cursor")
	}
	return page, nil
}

func (s *sourceFixture) ListRepoOps(_ context.Context, _ atmos.DID, _ atmos.TID, _ int, cursor string, _ bool) (OperationPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seenCursors = append(s.seenCursors, cursor)
	page, ok := s.pages[cursor]
	if !ok {
		return OperationPage{}, errors.New("unexpected op cursor")
	}
	return clonePage(page), nil
}

func (s *sourceFixture) GetLatestCommit(ctx context.Context, _ atmos.DID) (spaces.SignedCommit, error) {
	if s.latestBlock != nil {
		select {
		case <-s.latestBlock:
		case <-ctx.Done():
			return spaces.SignedCommit{}, ctx.Err()
		}
	}
	return s.commit, nil
}

func (s *sourceFixture) GetRecord(_ context.Context, _ atmos.DID, path spaces.RecordPath) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordFetches++
	record, ok := s.records[path]
	if !ok {
		return Record{}, ErrNotFound
	}
	return record.Clone(), nil
}

func (s *sourceFixture) GetRepo(_ context.Context, _ atmos.DID, indexOnly bool, _ int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if indexOnly {
		s.indexRequests++
	}
	return io.NopCloser(bytes.NewReader(s.car)), nil
}

func (s *sourceFixture) ResolveAuthor(context.Context, atmos.DID) (ResolvedAuthor, error) {
	return ResolvedAuthor{Key: s.key.PublicKey(), Provenance: Provenance{HostURL: "https://author.example", KeyMultibase: s.key.PublicKey().Multibase(), ResolvedAt: time.Unix(1, 0)}}, nil
}

func (s *sourceFixture) OpenAuthor(_ context.Context, author atmos.DID) (AuthorSource, error) {
	return &fixtureAuthorSource{source: s, author: author}, nil
}

type fixtureAuthorSource struct {
	source *sourceFixture
	author atmos.DID
}

func (s *fixtureAuthorSource) ListRepoOps(ctx context.Context, since atmos.TID, limit int, cursor string, exclude bool) (OperationPage, error) {
	return s.source.ListRepoOps(ctx, s.author, since, limit, cursor, exclude)
}
func (s *fixtureAuthorSource) GetLatestCommit(ctx context.Context) (spaces.SignedCommit, error) {
	return s.source.GetLatestCommit(ctx, s.author)
}
func (s *fixtureAuthorSource) GetRecord(ctx context.Context, path spaces.RecordPath) (Record, error) {
	return s.source.GetRecord(ctx, s.author, path)
}
func (s *fixtureAuthorSource) GetRepo(ctx context.Context, indexOnly bool, limit int64) (io.ReadCloser, error) {
	return s.source.GetRepo(ctx, s.author, indexOnly, limit)
}
func (s *fixtureAuthorSource) Resolved() ResolvedAuthor {
	resolved, _ := s.source.ResolveAuthor(context.Background(), s.author)
	return resolved
}

func (s *sourceFixture) RegisterNotify(context.Context, string, string) (time.Time, error) {
	return s.registerAt, nil
}

func (s *sourceFixture) PurgeCredential(context.Context, atmos.SpaceRef) error { return nil }

func clonePage(page OperationPage) OperationPage {
	out := page
	out.Operations = append([]Operation(nil), page.Operations...)
	if page.Commit != nil {
		commit := *page.Commit
		out.Commit = &commit
	}
	return out
}

type mapSnapshot map[spaces.RecordPath]Record

func (s mapSnapshot) ForEach(ctx context.Context, yield func(spaces.RecordRef) error) error {
	index := make(spaces.RepoIndex, len(s))
	for path, record := range s {
		index[path] = record.CID
	}
	for _, path := range index.Paths() {
		if err := yield(spaces.RecordRef{Path: path, CID: s[path].CID}); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (s mapSnapshot) OpenRecord(_ context.Context, path spaces.RecordPath) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s[path].Data)), nil
}

func testLimits() Limits {
	return Limits{PageSize: 2, DirectoryPageSize: 2, MaxPages: 20, MaxDirectoryPages: 20, MaxAuthors: 20, MaxOperations: 1000, MaxIncrementalBytes: 1 << 20, MaxFinalFetches: 100, MaxRecoveryAttempts: 2, MaxRepoBytes: 1 << 20, PassTimeout: 5 * time.Second, CleanupTimeout: time.Second, CAR: spaces.CARLimits{MaxHeaderSize: 4096, MaxCommitSize: 4096, MaxIndexSize: 1 << 16, MaxRecordSize: 1 << 16, MaxTotalSize: 1 << 20, MaxRecords: 100}}
}

func canonical(t *testing.T, value map[string]any) Record {
	t.Helper()
	data, err := cbor.Marshal(value)
	require.NoError(t, err)
	return Record{CID: cbor.ComputeCID(cbor.CodecDagCBOR, data), Data: data}
}

func makeSource(t *testing.T, rev atmos.TID, records map[spaces.RecordPath]Record, mode spaces.CARMode) *sourceFixture {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	index := make(spaces.RepoIndex, len(records))
	for path, record := range records {
		index[path] = record.CID
	}
	repoCommit, err := spaces.NewRepoCommitFromIndex(index)
	require.NoError(t, err)
	commit, err := repoCommit.Sign(spaces.CommitContext{Space: testSpace, Author: testAuthor, Rev: rev}, key)
	require.NoError(t, err)
	var car bytes.Buffer
	require.NoError(t, spaces.SerializeRepoCAR(context.Background(), &car, commit, mapSnapshot(records), mode, testLimits().CAR))
	return &sourceFixture{key: key, records: cloneRecords(records), commit: commit, car: car.Bytes(), pages: make(map[string]OperationPage), directory: map[string]RepoPage{"": {Repos: []RepoHint{{Author: testAuthor, Revision: rev, Hash: commit.Hash}}}}}
}

func newSyncer(t *testing.T, source Source, store Store, recovery RecoveryMode) *Syncer {
	t.Helper()
	syncer, err := New(Options{Space: testSpace, Source: source, Store: store, Limits: testLimits(), Recovery: recovery})
	require.NoError(t, err)
	return syncer
}

func TestFullBootstrapPublishesCompleteGenerationAndOutbox(t *testing.T) {
	t.Parallel()
	path, err := spaces.ParseRecordPath("com.example.post/one")
	require.NoError(t, err)
	record := canonical(t, map[string]any{"$type": "com.example.post", "text": "one"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	store, err := NewMemoryStore(10, 10, 10, 10<<20)
	require.NoError(t, err)
	repo, err := newSyncer(t, source, store, RecoveryFull).SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.True(t, repo.Checkpoint.IndexComplete)
	require.True(t, repo.Checkpoint.ValuesComplete)
	require.Equal(t, record.Data, repo.Records[path].Data)
	events, err := store.PeekEvents(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, EventRepoPromoted, events[0].Kind)
}

func TestIncrementalOpaqueCursorPrevAndMissingFinalValue(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	old := canonical(t, map[string]any{"$type": "com.example.post", "text": "old"})
	rev1, rev2 := atmos.NewTID(1, 0), atmos.NewTID(2, 0)
	source := makeSource(t, rev1, map[spaces.RecordPath]Record{path: old}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	newRecord := canonical(t, map[string]any{"$type": "com.example.post", "text": "new"})
	repoCommit, err := spaces.NewRepoCommitFromIndex(spaces.RepoIndex{path: newRecord.CID})
	require.NoError(t, err)
	commit, err := repoCommit.Sign(spaces.CommitContext{Space: testSpace, Author: testAuthor, Rev: rev2}, source.key)
	require.NoError(t, err)
	source.mu.Lock()
	source.records[path] = newRecord
	source.commit = commit
	source.pages = map[string]OperationPage{
		"":             {Operations: []Operation{{Revision: rev2, Path: path, Prev: &old.CID, CID: &newRecord.CID}}, Cursor: "opaque:/?x=1"},
		"opaque:/?x=1": {Commit: &commit},
	}
	source.mu.Unlock()
	repo, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.Equal(t, newRecord.Data, repo.Records[path].Data)
	require.Equal(t, []string{"", "opaque:/?x=1"}, source.seenCursors)
	require.Equal(t, 1, source.recordFetches)
}

func TestIncrementalAppliesMultipleSameRevisionSamePathExactlyOnce(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	old := canonical(t, map[string]any{"$type": "com.example.post", "text": "old"})
	middle := canonical(t, map[string]any{"$type": "com.example.post", "text": "middle"})
	final := canonical(t, map[string]any{"$type": "com.example.post", "text": "final"})
	rev1, rev2 := atmos.NewTID(1, 0), atmos.NewTID(2, 0)
	source := makeSource(t, rev1, map[spaces.RecordPath]Record{path: old}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	repoCommit, err := spaces.NewRepoCommitFromIndex(spaces.RepoIndex{path: final.CID})
	require.NoError(t, err)
	commit, err := repoCommit.Sign(spaces.CommitContext{Space: testSpace, Author: testAuthor, Rev: rev2}, source.key)
	require.NoError(t, err)
	source.pages = map[string]OperationPage{"": {Operations: []Operation{{Revision: rev2, Path: path, Prev: &old.CID, CID: &middle.CID}, {Revision: rev2, Path: path, Prev: &middle.CID, CID: &final.CID, Body: final.Data}}, Commit: &commit}}
	repo, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.Equal(t, final.Data, repo.Records[path].Data)
	require.Zero(t, source.recordFetches)
}

func TestFinalValueRaceIsIncompleteAndLeavesBaseVisible(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	old := canonical(t, map[string]any{"$type": "com.example.post", "text": "old"})
	next := canonical(t, map[string]any{"$type": "com.example.post", "text": "next"})
	raced := canonical(t, map[string]any{"$type": "com.example.post", "text": "raced"})
	rev1, rev2 := atmos.NewTID(1, 0), atmos.NewTID(2, 0)
	source := makeSource(t, rev1, map[spaces.RecordPath]Record{path: old}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	base, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	repoCommit, err := spaces.NewRepoCommitFromIndex(spaces.RepoIndex{path: next.CID})
	require.NoError(t, err)
	commit, err := repoCommit.Sign(spaces.CommitContext{Space: testSpace, Author: testAuthor, Rev: rev2}, source.key)
	require.NoError(t, err)
	source.pages = map[string]OperationPage{"": {Operations: []Operation{{Revision: rev2, Path: path, Prev: &old.CID, CID: &next.CID}}, Commit: &commit}}
	source.records[path] = raced
	_, err = syncer.SyncRepo(context.Background(), testAuthor)
	var incomplete *IncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, IncompleteFinalValue, incomplete.Reason)
	stored, err := store.LoadRepo(context.Background(), base.Key)
	require.NoError(t, err)
	require.Equal(t, base.Version, stored.Version)
	require.Equal(t, old.Data, stored.Records[path].Data)
	_, err = syncer.ReadRepo(context.Background(), testAuthor, nil)
	require.ErrorIs(t, err, ErrSuspended)
	retained, err := syncer.ReadRepo(context.Background(), testAuthor, func(context.Context, RepoKey, Lifecycle) error { return nil })
	require.NoError(t, err)
	require.Equal(t, old.Data, retained.Records[path].Data)
	source.records[path] = next
	repo, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.Equal(t, next.Data, repo.Records[path].Data)
	account, err := store.RepoLifecycle(context.Background(), base.Key)
	require.NoError(t, err)
	require.Equal(t, LifecycleActive, account.State)
}

func TestIncrementalPrevMismatchRecoversIndexOnlyAndReusesValues(t *testing.T) {
	t.Parallel()
	path1, _ := spaces.ParseRecordPath("com.example.post/one")
	path2, _ := spaces.ParseRecordPath("com.example.post/two")
	one := canonical(t, map[string]any{"$type": "com.example.post", "text": "one"})
	two := canonical(t, map[string]any{"$type": "com.example.post", "text": "two"})
	rev1, rev2 := atmos.NewTID(1, 0), atmos.NewTID(2, 0)
	source := makeSource(t, rev1, map[spaces.RecordPath]Record{path1: one}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryIndexOnly)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	newSource := makeSourceWithKey(t, source.key, rev2, map[spaces.RecordPath]Record{path1: one, path2: two}, spaces.CARIndexOnly)
	wrong := two.CID
	newSource.pages[""] = OperationPage{Operations: []Operation{{Revision: rev2, Path: path1, Prev: &wrong, CID: &two.CID}}}
	syncer.source = newSource
	repo, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.Len(t, repo.Records, 2)
	require.Equal(t, 1, newSource.indexRequests)
	require.Equal(t, 1, newSource.recordFetches, "matching old value should be reused")
}

func makeSourceWithKey(t *testing.T, key crypto.PrivateKey, rev atmos.TID, records map[spaces.RecordPath]Record, mode spaces.CARMode) *sourceFixture {
	t.Helper()
	index := make(spaces.RepoIndex, len(records))
	for path, record := range records {
		index[path] = record.CID
	}
	repoCommit, err := spaces.NewRepoCommitFromIndex(index)
	require.NoError(t, err)
	commit, err := repoCommit.Sign(spaces.CommitContext{Space: testSpace, Author: testAuthor, Rev: rev}, key)
	require.NoError(t, err)
	var car bytes.Buffer
	require.NoError(t, spaces.SerializeRepoCAR(context.Background(), &car, commit, mapSnapshot(records), mode, testLimits().CAR))
	return &sourceFixture{key: key, records: cloneRecords(records), commit: commit, car: car.Bytes(), pages: make(map[string]OperationPage), directory: map[string]RepoPage{"": {}}}
}

func TestIncompleteCursorCycleNeverPublishes(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	old := canonical(t, map[string]any{"$type": "com.example.post", "v": "old"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: old}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 10, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	base, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	source.pages = map[string]OperationPage{"": {Cursor: "a"}, "a": {Cursor: "a"}}
	_, err = syncer.SyncRepo(context.Background(), testAuthor)
	var incomplete *IncompleteError
	require.ErrorAs(t, err, &incomplete)
	require.Equal(t, IncompletePagination, incomplete.Reason)
	stored, err := store.LoadRepo(context.Background(), RepoKey{Space: testSpace, Author: testAuthor})
	require.NoError(t, err)
	require.Equal(t, base.Version, stored.Version)
}

func TestEqualRevisionEquivocationNeverRecoversOrPublishes(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	record := canonical(t, map[string]any{"$type": "com.example.post"})
	rev := atmos.NewTID(1, 0)
	source := makeSource(t, rev, map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 10, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	base, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	bad := source.commit
	bad.Hash[0] ^= 1
	source.pages[""] = OperationPage{Commit: &bad}
	_, err = syncer.SyncRepo(context.Background(), testAuthor)
	var integrity *IntegrityError
	require.ErrorAs(t, err, &integrity)
	stored, _ := store.LoadRepo(context.Background(), base.Key)
	require.Equal(t, base.Version, stored.Version)
}

func TestPromotionFailureIsInvisibleAndOutboxRedelivers(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	record := canonical(t, map[string]any{"$type": "com.example.post"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 10, 10<<20)
	store.FailNext("promote", errors.New("crash before atomic commit"))
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.ErrorContains(t, err, "crash before")
	_, err = store.LoadRepo(context.Background(), RepoKey{Space: testSpace, Author: testAuthor})
	require.ErrorIs(t, err, ErrNotFound)
	events, _ := store.PeekEvents(context.Background(), 10)
	for _, event := range events {
		require.NotEqual(t, EventRepoPromoted, event.Kind)
		require.NoError(t, store.AckEvent(context.Background(), event.ID))
	}
	repo, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	events, err = store.PeekEvents(context.Background(), 10)
	require.NoError(t, err)
	for _, event := range events {
		if event.Kind != EventRepoPromoted {
			require.NoError(t, store.AckEvent(context.Background(), event.ID))
		}
	}
	consumer := &countConsumer{fail: true}
	_, err = syncer.DeliverOutbox(context.Background(), consumer, 10)
	require.Error(t, err)
	consumer.fail = false
	count, err := syncer.DeliverOutbox(context.Background(), consumer, 10)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, repo.Checkpoint.Revision, consumer.last.Revision)
	require.Equal(t, 2, consumer.calls)
}

type countConsumer struct {
	calls int
	fail  bool
	last  Event
}

func (c *countConsumer) Consume(_ context.Context, event Event) error {
	c.calls++
	c.last = event
	if c.fail {
		return errors.New("downstream")
	}
	return nil
}

func TestDeletionFencesAndCancelsInflightRecovery(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	record := canonical(t, map[string]any{"$type": "com.example.post"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	source.latestBlock = make(chan struct{})
	store, _ := NewMemoryStore(10, 10, 10, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	done := make(chan error, 1)
	go func() { _, err := syncer.SyncRepo(context.Background(), testAuthor); done <- err }()
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return len(store.stages) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, syncer.ApplyVerifiedSpaceDeletion(context.Background(), "verified authority notification"))
	require.ErrorIs(t, <-done, context.Canceled)
	_, err := store.LoadRepo(context.Background(), RepoKey{Space: testSpace, Author: testAuthor})
	require.ErrorIs(t, err, ErrNotFound)
	lifecycle, _ := store.Lifecycle(context.Background(), testSpace)
	require.Equal(t, LifecycleDeleted, lifecycle.State)
	require.False(t, lifecycle.CleanupPending)
}

func TestSuspendedServingRequiresExplicitAuthorization(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	record := canonical(t, map[string]any{"$type": "com.example.post"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.NoError(t, syncer.Suspend(context.Background(), "credential denied"))
	_, err = syncer.ReadRepo(context.Background(), testAuthor, nil)
	require.ErrorIs(t, err, ErrSuspended)
	repo, err := syncer.ReadRepo(context.Background(), testAuthor, func(context.Context, RepoKey, Lifecycle) error { return nil })
	require.NoError(t, err)
	require.Equal(t, record.Data, repo.Records[path].Data)
	_, err = syncer.SyncRepo(context.Background(), testAuthor)
	require.ErrorIs(t, err, ErrSuspended)
}

func TestCleanupFailureRemainsVisibleAndRetryableAfterRestart(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	record := canonical(t, map[string]any{"$type": "com.example.post"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	store.FailNext("purge_space", errors.New("disk unavailable"))
	err = syncer.ApplyVerifiedSpaceDeletion(context.Background(), "verified")
	require.ErrorContains(t, err, "disk unavailable")
	lifecycle, err := store.Lifecycle(context.Background(), testSpace)
	require.NoError(t, err)
	require.True(t, lifecycle.CleanupPending)
	restarted := newSyncer(t, source, store, RecoveryFull)
	require.NoError(t, restarted.ResumeCleanup(context.Background()))
	lifecycle, err = store.Lifecycle(context.Background(), testSpace)
	require.NoError(t, err)
	require.False(t, lifecycle.CleanupPending)
}

func TestAccountLifecycleFencesOnlyNamedAuthor(t *testing.T) {
	t.Parallel()
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{}, spaces.CARFull)
	syncer := newSyncer(t, source, store, RecoveryFull)
	key := RepoKey{Space: testSpace, Author: testAuthor}
	require.NoError(t, syncer.ApplyTrustedAccountEvent(context.Background(), key, AccountDeactivated, "trusted event"))
	_, err := syncer.SyncRepo(context.Background(), testAuthor)
	require.ErrorIs(t, err, ErrSuspended)
	spaceState, err := store.Lifecycle(context.Background(), testSpace)
	require.NoError(t, err)
	require.Equal(t, LifecycleActive, spaceState.State)
	require.NoError(t, syncer.ApplyTrustedAccountEvent(context.Background(), key, AccountActivated, "restored"))
	_, err = syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	require.NoError(t, syncer.ApplyTrustedAccountEvent(context.Background(), key, AccountDeleted, "trusted deletion"))
	_, err = syncer.ReadRepo(context.Background(), testAuthor, func(context.Context, RepoKey, Lifecycle) error { return nil })
	require.ErrorIs(t, err, ErrDeleted)
}

func TestConcurrentSyncCoalescesOneLogicalPass(t *testing.T) {
	t.Parallel()
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	record := canonical(t, map[string]any{"$type": "com.example.post"})
	source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
	block := make(chan struct{})
	source.latestBlock = block
	store, _ := NewMemoryStore(10, 10, 20, 10<<20)
	syncer := newSyncer(t, source, store, RecoveryFull)
	results := make(chan Repo, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() { repo, err := syncer.SyncRepo(context.Background(), testAuthor); results <- repo; errs <- err }()
	}
	require.Eventually(t, func() bool { syncer.mu.Lock(); defer syncer.mu.Unlock(); return len(syncer.flights) == 1 }, time.Second, time.Millisecond)
	close(block)
	first, second := <-results, <-results
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, first.Version, second.Version)
	events, err := store.PeekEvents(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
}

type failingOpsSource struct {
	Source
	err error
}

func (s failingOpsSource) OpenAuthor(ctx context.Context, author atmos.DID) (AuthorSource, error) {
	bound, err := s.Source.OpenAuthor(ctx, author)
	if err != nil {
		return nil, err
	}
	return failingAuthorSource{AuthorSource: bound, err: s.err}, nil
}

type failingAuthorSource struct {
	AuthorSource
	err error
}

func (s failingAuthorSource) ListRepoOps(context.Context, atmos.TID, int, string, bool) (OperationPage, error) {
	return OperationPage{}, s.err
}

func TestCredentialAndAccountDenialSelectExplicitSuspensionState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		sourceErr error
		account   bool
	}{
		{name: "credential", sourceErr: ErrCredentialDenied},
		{name: "account", sourceErr: ErrAccountSuspended, account: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, _ := spaces.ParseRecordPath("com.example.post/one")
			record := canonical(t, map[string]any{"$type": "com.example.post"})
			source := makeSource(t, atmos.NewTID(1, 0), map[spaces.RecordPath]Record{path: record}, spaces.CARFull)
			store, _ := NewMemoryStore(10, 10, 20, 10<<20)
			syncer := newSyncer(t, source, store, RecoveryFull)
			_, err := syncer.SyncRepo(context.Background(), testAuthor)
			require.NoError(t, err)
			syncer.source = failingOpsSource{Source: source, err: test.sourceErr}
			_, err = syncer.SyncRepo(context.Background(), testAuthor)
			require.ErrorIs(t, err, test.sourceErr)
			spaceState, _ := store.Lifecycle(context.Background(), testSpace)
			accountState, _ := store.RepoLifecycle(context.Background(), RepoKey{Space: testSpace, Author: testAuthor})
			if test.account {
				require.Equal(t, LifecycleActive, spaceState.State)
				require.Equal(t, LifecycleSuspended, accountState.State)
			} else {
				require.Equal(t, LifecycleSuspended, spaceState.State)
				require.Equal(t, LifecycleActive, accountState.State)
			}
		})
	}
}

func TestModelRandomIncrementalOperationsConverge(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(7, 11))
	rev := atmos.NewTID(1, 0)
	records := make(map[spaces.RecordPath]Record)
	source := makeSource(t, rev, records, spaces.CARFull)
	store, _ := NewMemoryStore(10, 10, 1000, 10<<20)
	limits := testLimits()
	limits.MaxOperations = 5000
	limits.MaxFinalFetches = 5000
	syncer, err := New(Options{Space: testSpace, Source: source, Store: store, Limits: limits, Recovery: RecoveryFull})
	require.NoError(t, err)
	_, err = syncer.SyncRepo(context.Background(), testAuthor)
	require.NoError(t, err)
	for step := 2; step < 102; step++ {
		path, _ := spaces.ParseRecordPath("com.example.post/" + string(rune('a'+rng.IntN(12))))
		previous, exists := records[path]
		var cid, prev *cbor.CID
		var body []byte
		if exists && rng.IntN(4) == 0 {
			prev = &previous.CID
			delete(records, path)
		} else {
			next := canonical(t, map[string]any{"$type": "com.example.post", "step": int64(step)})
			cid, body = &next.CID, next.Data
			if exists {
				prev = &previous.CID
			}
			records[path] = next
		}
		rev = atmos.NewTID(int64(step), 0)
		index := make(spaces.RepoIndex)
		for p, r := range records {
			index[p] = r.CID
		}
		rc, err := spaces.NewRepoCommitFromIndex(index)
		require.NoError(t, err)
		commit, err := rc.Sign(spaces.CommitContext{Space: testSpace, Author: testAuthor, Rev: rev}, source.key)
		require.NoError(t, err)
		source.pages = map[string]OperationPage{"": {Operations: []Operation{{Revision: rev, Path: path, CID: cid, Prev: prev, Body: body}}, Commit: &commit}}
		source.records = cloneRecords(records)
		source.commit = commit
		repo, err := syncer.SyncRepo(context.Background(), testAuthor)
		require.NoError(t, err)
		require.Equal(t, len(records), len(repo.Records))
		for p, expected := range records {
			require.Equal(t, expected.Data, repo.Records[p].Data)
		}
	}
}
