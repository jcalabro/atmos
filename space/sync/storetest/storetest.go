// Package storetest provides a reusable conformance harness for sync.Store
// implementations.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
	spacesync "github.com/jcalabro/atmos/space/sync"
)

// Factory returns a fresh empty store and its optional failure injector.
type Factory func() (spacesync.Store, spacesync.FailureInjector, error)

// Run exercises atomic visibility, both CAS fences, crash cleanup, deletion
// fencing, outbox creation, and injected pre-promotion failure. It returns the
// first invariant violation so any storage backend can call it from its own
// test suite without importing testing here.
func Run(ctx context.Context, factory Factory) error {
	store, injector, err := factory()
	if err != nil {
		return err
	}
	space := atmos.SpaceRef("at://did:plc:storetest/space/com.example.space/main")
	key := spacesync.RepoKey{Space: space, Author: "did:plc:author"}
	lifecycle, err := store.Lifecycle(ctx, space)
	if err != nil || lifecycle.State != spacesync.LifecycleActive {
		return fmt.Errorf("initial lifecycle: %w", err)
	}
	stage, err := store.Begin(ctx, key, 0, lifecycle.Generation)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	path, err := spaces.ParseRecordPath("com.example.post/one")
	if err != nil {
		return err
	}
	data, err := cbor.Marshal(map[string]any{"$type": "com.example.post"})
	if err != nil {
		return err
	}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	stage.Index[path] = cid
	stage.Records[path] = spacesync.Record{CID: cid, Data: data}
	commit, err := spaces.NewRepoCommitFromIndex(stage.Index)
	if err != nil {
		return err
	}
	stage.State = commit.State()
	if err := store.SaveStage(ctx, stage); err != nil {
		return fmt.Errorf("save stage: %w", err)
	}
	if _, err := store.LoadRepo(ctx, key); !errors.Is(err, spacesync.ErrNotFound) {
		return errors.New("staged generation became visible")
	}
	checkpoint := spacesync.Checkpoint{Revision: atmos.NewTID(1, 0), Hash: commit.Digest(), State: commit.State(), IndexComplete: true, ValuesComplete: true, Provenance: spacesync.Provenance{HostURL: "https://author.example", KeyMultibase: "zTest", ResolvedAt: time.Unix(1, 0)}}
	if injector != nil {
		injected := errors.New("injected promotion failure")
		injector.FailNext("promote", injected)
		if _, err := store.Promote(ctx, stage.ID, checkpoint); !errors.Is(err, injected) {
			return errors.New("promotion injection was not observed")
		}
		if _, err := store.LoadRepo(ctx, key); !errors.Is(err, spacesync.ErrNotFound) {
			return errors.New("failed promotion became visible")
		}
		events, err := store.PeekEvents(ctx, 10)
		if err != nil || len(events) != 0 {
			return errors.New("failed promotion created an outbox event")
		}
	}
	repo, err := store.Promote(ctx, stage.ID, checkpoint)
	if err != nil {
		return fmt.Errorf("promote: %w", err)
	}
	events, err := store.PeekEvents(ctx, 10)
	if err != nil || len(events) != 1 || events[0].RepoVersion != repo.Version {
		return errors.New("promotion and outbox were not atomic")
	}
	stale, err := store.Begin(ctx, key, repo.Version, lifecycle.Generation)
	if err != nil {
		return err
	}
	newer, err := store.Begin(ctx, key, repo.Version, lifecycle.Generation)
	if err != nil {
		return err
	}
	if _, err := store.Promote(ctx, newer.ID, checkpoint); err != nil {
		return err
	}
	if _, err := store.Promote(ctx, stale.ID, checkpoint); !errors.Is(err, spacesync.ErrConflict) {
		return errors.New("stale repo CAS promoted")
	}
	unfinished, err := store.Begin(ctx, key, repo.Version+1, lifecycle.Generation)
	if err != nil {
		return err
	}
	if err := store.DiscardUnfinished(ctx, space); err != nil {
		return err
	}
	if _, err := store.Promote(ctx, unfinished.ID, checkpoint); !errors.Is(err, spacesync.ErrNotFound) {
		return errors.New("restart cleanup retained unfinished stage")
	}
	fenced, err := store.Begin(ctx, key, repo.Version+1, lifecycle.Generation)
	if err != nil {
		return err
	}
	// A per-author deletion tombstone must stay enumerable even when the author
	// never published a generation, or restart cleanup cannot retry its purge.
	unpublished := spacesync.RepoKey{Space: space, Author: "did:plc:unpublished"}
	unpublishedState, err := store.RepoLifecycle(ctx, unpublished)
	if err != nil {
		return fmt.Errorf("unpublished repo lifecycle: %w", err)
	}
	if _, err := store.TransitionRepoLifecycle(ctx, unpublished, unpublishedState.Generation, spacesync.LifecycleDeleted, "test"); err != nil {
		return fmt.Errorf("tombstone unpublished repo: %w", err)
	}
	published, err := store.ListRepos(ctx, space)
	if err != nil {
		return err
	}
	for _, listed := range published {
		if listed == unpublished {
			return errors.New("unpublished tombstone listed as a published repo")
		}
	}
	states, err := store.ListRepoLifecycles(ctx, space)
	if err != nil {
		return fmt.Errorf("list repo lifecycles: %w", err)
	}
	foundUnpublished := false
	for _, listed := range states {
		if listed == unpublished {
			foundUnpublished = true
		}
	}
	if !foundUnpublished {
		return errors.New("lifecycle enumeration omitted an unpublished deletion tombstone")
	}
	lifecycle, err = store.TransitionLifecycle(ctx, space, lifecycle.Generation, spacesync.LifecycleDeleted, "test")
	if err != nil {
		return err
	}
	if _, err := store.Promote(ctx, fenced.ID, checkpoint); !errors.Is(err, spacesync.ErrDeleted) && !errors.Is(err, spacesync.ErrConflict) {
		return errors.New("deletion did not fence an existing stage")
	}
	if _, err := store.Begin(ctx, key, repo.Version+1, lifecycle.Generation); !errors.Is(err, spacesync.ErrDeleted) {
		return errors.New("deletion did not fence begin")
	}
	return nil
}
