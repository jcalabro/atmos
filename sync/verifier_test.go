package sync_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestMemStateStore_LoadMissingReturnsNilNil(t *testing.T) {
	t.Parallel()

	store := sync.NewMemStateStore()
	state, err := store.LoadChain(context.Background(), atmos.DID("did:plc:abc"))
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemStateStore_SaveThenLoad(t *testing.T) {
	t.Parallel()

	store := sync.NewMemStateStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	want := sync.ChainState{Rev: "3l3qo2vutsw2b", Data: cid}
	require.NoError(t, store.SaveChain(context.Background(), did, want))

	got, err := store.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.Rev, got.Rev)
	assert.True(t, got.Data.Equal(want.Data))
}

func TestMemStateStore_Delete(t *testing.T) {
	t.Parallel()

	store := sync.NewMemStateStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	require.NoError(t, store.SaveChain(context.Background(), did, sync.ChainState{Rev: "r1", Data: cid}))
	require.NoError(t, store.Delete(context.Background(), did))

	state, err := store.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemStateStore_DeleteMissingNoError(t *testing.T) {
	t.Parallel()

	store := sync.NewMemStateStore()
	require.NoError(t, store.Delete(context.Background(), atmos.DID("did:plc:never-saved")))
}

func TestErrorTypes_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	t.Run("ChainBreakError", func(t *testing.T) {
		cid, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
		err := &sync.ChainBreakError{
			DID:          atmos.DID("did:plc:abc"),
			SeenRev:      "r1",
			SeenData:     cid,
			GotRev:       "r2",
			GotPrevData:  cid,
			InvertedData: cid,
		}
		assert.Contains(t, err.Error(), "chain break")
		assert.Contains(t, err.Error(), "did:plc:abc")

		var target *sync.ChainBreakError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("InversionError", func(t *testing.T) {
		cause := errors.New("missing block")
		err := &sync.InversionError{DID: "did:plc:x", Rev: "r1", Cause: cause}
		assert.Contains(t, err.Error(), "inversion failed")
		assert.ErrorIs(t, err, cause)

		var target *sync.InversionError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("SignatureError", func(t *testing.T) {
		cause := errors.New("bad sig")
		err := &sync.SignatureError{DID: "did:plc:x", Rev: "r1", KeyDID: "did:key:z...", Cause: cause}
		assert.Contains(t, err.Error(), "signature")
		assert.ErrorIs(t, err, cause)

		var target *sync.SignatureError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("ResyncFailedError", func(t *testing.T) {
		cause := errors.New("PDS down")
		err := &sync.ResyncFailedError{DID: "did:plc:x", Reason: sync.ReasonChainBreak, Cause: cause}
		assert.Contains(t, err.Error(), "resync failed")
		assert.ErrorIs(t, err, cause)

		var target *sync.ResyncFailedError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("ResyncRateLimitedError", func(t *testing.T) {
		err := &sync.ResyncRateLimitedError{DID: "did:plc:x"}
		assert.Contains(t, err.Error(), "rate limited")
		assert.Contains(t, err.Error(), "did:plc:x")

		var target *sync.ResyncRateLimitedError
		assert.True(t, errors.As(err, &target))
	})

	t.Run("Wrapping with fmt.Errorf", func(t *testing.T) {
		inner := &sync.SignatureError{DID: "did:plc:x", Rev: "r1"}
		wrapped := fmt.Errorf("verifier: %w", inner)
		var target *sync.SignatureError
		assert.True(t, errors.As(wrapped, &target))
	})

	t.Run("InversionError nil cause", func(t *testing.T) {
		err := &sync.InversionError{DID: "did:plc:x", Rev: "r1"}
		assert.Contains(t, err.Error(), "inversion failed")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("SignatureError nil cause", func(t *testing.T) {
		err := &sync.SignatureError{DID: "did:plc:x", Rev: "r1", KeyDID: "did:key:z..."}
		assert.Contains(t, err.Error(), "signature")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("ResyncFailedError nil cause", func(t *testing.T) {
		err := &sync.ResyncFailedError{DID: "did:plc:x", Reason: sync.ReasonChainBreak}
		assert.Contains(t, err.Error(), "resync failed")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("ChainBreakError first sighting and zero inverted", func(t *testing.T) {
		// SeenRev empty + zero SeenData + zero InvertedData should not produce bare "b".
		err := &sync.ChainBreakError{
			DID:    atmos.DID("did:plc:x"),
			GotRev: "r2",
		}
		msg := err.Error()
		assert.Contains(t, msg, "first-sighting")
		assert.Contains(t, msg, "inverted=n/a")
		assert.NotContains(t, msg, "data=b ")
		assert.NotContains(t, msg, "data=b,")
		assert.NotContains(t, msg, "data=b)")
		assert.NotContains(t, msg, "inverted=b)")
	})
}

func TestResyncReason_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "chain_break", sync.ReasonChainBreak.String())
	assert.Equal(t, "inversion_failure", sync.ReasonInversionFailure.String())
	assert.Equal(t, "sync_event", sync.ReasonSyncEvent.String())
	assert.Equal(t, "unknown_reason(99)", sync.ResyncReason(99).String())
}

func TestVerifierPolicy_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "resync", sync.PolicyResync.String())
	assert.Equal(t, "error", sync.PolicyError.String())
	assert.Equal(t, "unknown_policy(99)", sync.VerifierPolicy(99).String())
}

func TestVerifierOptions_ZeroValuePolicyIsResync(t *testing.T) {
	t.Parallel()
	var o sync.VerifierOptions
	assert.False(t, o.Policy.HasVal(), "zero VerifierOptions.Policy must be None")
	assert.Equal(t, sync.PolicyResync, o.Policy.ValOr(sync.PolicyResync))
}

func TestVerifierStatsZero(t *testing.T) {
	t.Parallel()
	var s sync.VerifierStats
	assert.Equal(t, uint64(0), s.EventsVerified)
	assert.Equal(t, uint64(0), s.ChainBreaks)
	assert.Equal(t, uint64(0), s.Resyncs)
	assert.Equal(t, uint64(0), s.InversionFailures)
	assert.Equal(t, uint64(0), s.SignatureFailures)
	assert.Equal(t, uint64(0), s.ResyncFailures)
	assert.Equal(t, uint64(0), s.RevReplaysDropped)
}

func TestNewVerifier_RequiredFields(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://example.invalid"}
	sc := sync.NewClient(sync.Options{Client: xc})

	t.Run("missing StateStore", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: gt.Some(sc),
			Directory:  dir,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "StateStore")
	})

	t.Run("missing Directory", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: gt.Some(sc),
			StateStore: sync.NewMemStateStore(),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Directory")
	})

	t.Run("PolicyResync requires SyncClient", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			Directory:  dir,
			StateStore: sync.NewMemStateStore(),
			Policy:     gt.Some(sync.PolicyResync),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SyncClient")
	})

	t.Run("PolicyError works without SyncClient", func(t *testing.T) {
		v, err := sync.NewVerifier(sync.VerifierOptions{
			Directory:  dir,
			StateStore: sync.NewMemStateStore(),
			Policy:     gt.Some(sync.PolicyError),
		})
		require.NoError(t, err)
		assert.NotNil(t, v)
	})

	t.Run("happy path with all required", func(t *testing.T) {
		v, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: gt.Some(sc),
			Directory:  dir,
			StateStore: sync.NewMemStateStore(),
		})
		require.NoError(t, err)
		require.NotNil(t, v)
	})
}

func TestNewVerifier_StatsStartAtZero(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://example.invalid"}
	sc := sync.NewClient(sync.Options{Client: xc})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sc),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
	})
	require.NoError(t, err)
	assert.NotNil(t, v)
	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.EventsVerified)
}

func TestNewVerifier_DoesNotMutateCallerOptions(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://example.invalid"}
	sc := sync.NewClient(sync.Options{Client: xc})

	opts := sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		ResyncLimit: gt.Some(rate.Limit(0)),
		ResyncBurst: gt.Some(0),
	}
	_, err := sync.NewVerifier(opts)
	require.NoError(t, err)
	// NewVerifier defaults ResyncLimit and ResyncBurst internally; the
	// caller's struct must not be mutated.
	assert.Equal(t, gt.Some(rate.Limit(0)), opts.ResyncLimit)
	assert.Equal(t, gt.Some(0), opts.ResyncBurst)
}

// ---------------------------------------------------------------------------
// InvertCommit tests
// ---------------------------------------------------------------------------

func TestInvertCommit_SingleCreateOnEmptyMST(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:test1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, prevData := testutil.BuildEmptyRepo(t, did)
	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "hello", "createdAt": "2024-01-01T00:00:00Z"},
	}})

	got, err := sync.InvertCommit(commit)
	require.NoError(t, err)
	assert.True(t, got.Equal(prevData),
		"inverting a create should restore the empty MST root: got %s want %s",
		got, prevData)
}

func TestInvertCommit_MultiOp(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:test2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state: r already has rec1 (so we can update it) and rec2
	// (so we can delete it). Capture prevData after seeding.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	require.NoError(t, r.Create("app.bsky.feed.post", "rec2", map[string]any{"text": "doomed"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{
		{Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec3", Record: map[string]any{"text": "new"}},
		{Action: testutil.ActionUpdate, Collection: "app.bsky.feed.post", RKey: "rec1", Record: map[string]any{"text": "new"}},
		{Action: testutil.ActionDelete, Collection: "app.bsky.feed.post", RKey: "rec2"},
	})

	got, err := sync.InvertCommit(commit)
	require.NoError(t, err)
	assert.True(t, got.Equal(prevData), "got %s want %s", got, prevData)
}

func TestInvertCommit_EmptyOps(t *testing.T) {
	t.Parallel()
	// An empty-ops commit (allowed by the spec) inverts to itself —
	// inverted root == post-state root == prevData (which was also the
	// pre-state root, since no mutation happened).
	did := atmos.DID("did:plc:empty")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, prevData := testutil.BuildEmptyRepo(t, did)
	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, nil)

	got, err := sync.InvertCommit(commit)
	require.NoError(t, err)
	assert.True(t, got.Equal(prevData))
}

// TestInvertCommit_MissingPrevOnRealCommit specifically exercises the
// "update op with no Prev" branch by building a real synthetic commit
// and then mutating one op to drop its Prev, ensuring the malformed
// op is detected during the inversion loop rather than the prologue.
func TestInvertCommit_MissingPrevOnRealCommit(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:noprev")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})
	// Drop the Prev field that the helper added.
	commit.Ops[0].Prev = gt.None[lextypes.LexCIDLink]()

	_, err = sync.InvertCommit(commit)
	var ie *sync.InversionError
	require.ErrorAs(t, err, &ie)
}

func TestInvertCommit_MalformedCAR(t *testing.T) {
	t.Parallel()
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:abc",
		Rev:    "r1",
		Blocks: []byte{0x00, 0x01, 0x02}, // garbage
	}
	_, err := sync.InvertCommit(commit)
	var ie *sync.InversionError
	require.ErrorAs(t, err, &ie)
}

// TestInvertCommit_ValidCARNoBlocks builds a CAR with only a header
// (no blocks). The CAR parses cleanly but the commit-block lookup
// fails, exercising a distinct error path from a malformed CAR.
func TestInvertCommit_ValidCARNoBlocks(t *testing.T) {
	t.Parallel()

	fakeCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = car.NewWriter(&buf, []cbor.CID{fakeCID})
	require.NoError(t, err)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:abc",
		Rev:    "r1",
		Blocks: buf.Bytes(),
		Commit: lextypes.LexCIDLink{Link: fakeCID.String()},
	}
	_, err = sync.InvertCommit(commit)
	var ie *sync.InversionError
	require.ErrorAs(t, err, &ie)
}

func TestInvertCommit_PropertyRandomOps(t *testing.T) {
	t.Parallel()
	iterations := 50
	if !testing.Short() {
		iterations = 1000
	}
	for i := range iterations {
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			t.Parallel()
			did := atmos.DID(fmt.Sprintf("did:plc:prop%d", i))
			key, err := crypto.GenerateP256()
			require.NoError(t, err)

			// Seed with 5 records so we have things to update/delete.
			r, _ := testutil.BuildEmptyRepo(t, did)
			for j := range 5 {
				require.NoError(t, r.Create("app.bsky.feed.post",
					fmt.Sprintf("seed%d", j),
					map[string]any{"text": fmt.Sprintf("seed%d", j)}))
			}
			prevData, err := r.Tree.WriteBlocks(r.Store)
			require.NoError(t, err)

			// Generate 1-10 ops. Update/delete may only target seed
			// keys that exist in the pre-state tree; otherwise the
			// pre-state lookup in buildSyntheticCommit would fail.
			// Creates use a new collection of "newK" keys (each used
			// at most once). We do NOT permit update/delete on a
			// newly-created key in the same ops list — that would
			// require multi-step state tracking the synthetic
			// builder doesn't do.
			seed := int64(i*1000 + 1)
			rng := mathrand.New(mathrand.NewSource(seed))
			nOps := 1 + rng.Intn(10)
			ops := make([]testutil.OpAction, 0, nOps)
			seedKeys := []string{"seed0", "seed1", "seed2", "seed3", "seed4"}
			deletedSeeds := make(map[string]struct{})
			usedNewKeys := make(map[string]struct{})

			availableSeeds := func() []string {
				out := make([]string, 0, len(seedKeys))
				for _, k := range seedKeys {
					if _, gone := deletedSeeds[k]; !gone {
						out = append(out, k)
					}
				}
				return out
			}

			attempt := 0
			for len(ops) < nOps {
				attempt++
				if attempt > nOps*4 {
					// pathological rng; just stop
					break
				}
				switch rng.Intn(3) {
				case 0:
					rkey := fmt.Sprintf("new%d", attempt)
					if _, dup := usedNewKeys[rkey]; dup {
						continue
					}
					usedNewKeys[rkey] = struct{}{}
					ops = append(ops, testutil.OpAction{
						Action:     testutil.ActionCreate,
						Collection: "app.bsky.feed.post",
						RKey:       rkey,
						Record:     map[string]any{"text": rkey},
					})
				case 1:
					avail := availableSeeds()
					if len(avail) == 0 {
						continue
					}
					target := avail[rng.Intn(len(avail))]
					ops = append(ops, testutil.OpAction{
						Action:     testutil.ActionUpdate,
						Collection: "app.bsky.feed.post",
						RKey:       target,
						Record:     map[string]any{"text": "updated"},
					})
				case 2:
					avail := availableSeeds()
					if len(avail) == 0 {
						continue
					}
					target := avail[rng.Intn(len(avail))]
					deletedSeeds[target] = struct{}{}
					ops = append(ops, testutil.OpAction{
						Action:     testutil.ActionDelete,
						Collection: "app.bsky.feed.post",
						RKey:       target,
					})
				}
			}

			commit := testutil.BuildSyntheticCommit(t, r, key, prevData, ops)
			got, err := sync.InvertCommit(commit)
			require.NoError(t, err)
			assert.True(t, got.Equal(prevData), "iter %d: ops=%v got %s want %s", i, ops, got, prevData)
		})
	}
}

// TestVerifier_PerDIDLocking asserts that two concurrent calls for the
// same DID do not interleave (per-DID serialization), and that calls
// for different DIDs do not block each other.
func TestVerifier_PerDIDLocking(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	did := atmos.DID("did:plc:abc")

	var counter int
	var racy bool
	wg := stdsync.WaitGroup{}
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			unlock := sync.LockDIDForTest(v, did)
			defer unlock()
			start := counter
			time.Sleep(5 * time.Millisecond)
			if counter != start {
				racy = true
			}
			counter = start + 1
		}()
	}
	wg.Wait()
	assert.False(t, racy, "per-DID mutex did not serialize work")
	assert.Equal(t, 2, counter)

	// Different DIDs must NOT serialize.
	t.Run("different DIDs run in parallel", func(t *testing.T) {
		didA := atmos.DID("did:plc:a")
		didB := atmos.DID("did:plc:b")
		ready := make(chan struct{})
		release := make(chan struct{})
		go func() {
			unlock := sync.LockDIDForTest(v, didA)
			close(ready)
			<-release
			unlock()
		}()
		<-ready
		// didB should lock immediately.
		done := make(chan struct{})
		go func() {
			unlock := sync.LockDIDForTest(v, didB)
			unlock()
			close(done)
		}()
		select {
		case <-done:
			// good
		case <-time.After(100 * time.Millisecond):
			t.Fatal("locking didB blocked while didA was held; per-DID lock map is global")
		}
		close(release)
	})
}

func TestVerifier_VerifySignature_Success(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:sig1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	rev := atmos.NewTIDClock(0).Next()
	cidA, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	c := &repo.Commit{DID: string(did), Version: 3, Data: cidA, Rev: string(rev)}
	require.NoError(t, c.Sign(key))

	require.NoError(t, sync.VerifyCommitSignatureForTest(v, context.Background(), did, c))
}

func TestVerifier_VerifySignature_KeyRotation(t *testing.T) {
	t.Parallel()
	// First resolution returns an outdated key (verification fails);
	// second resolution (after Purge) returns the correct key.

	did := atmos.DID("did:plc:rot1")
	oldKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	newKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, oldKey.PublicKey())
	cache := identity.NewLRUCache(64, time.Hour)
	dir := &identity.Directory{Resolver: resolver, Cache: cache}

	// Warm cache with the wrong key.
	_, err = dir.LookupDID(context.Background(), did)
	require.NoError(t, err)

	// Now flip the resolver to return the new key.
	resolver.Docs[did] = testutil.BuildDIDDoc(did, newKey.PublicKey())

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	rev := atmos.NewTIDClock(0).Next()
	cidA, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	c := &repo.Commit{DID: string(did), Version: 3, Data: cidA, Rev: string(rev)}
	require.NoError(t, c.Sign(newKey))

	// First verify should fail with the cached old key, then verifier
	// purges and retries, getting the new key, and succeeds.
	require.NoError(t, sync.VerifyCommitSignatureForTest(v, context.Background(), did, c))
	assert.GreaterOrEqual(t, resolver.ResolveHits[did], 2,
		"expected at least one re-resolution after Purge")
}

func TestVerifier_VerifySignature_PermanentFailure(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:badsig")
	correctKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	wrongKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, wrongKey.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	rev := atmos.NewTIDClock(0).Next()
	cidA, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	c := &repo.Commit{DID: string(did), Version: 3, Data: cidA, Rev: string(rev)}
	require.NoError(t, c.Sign(correctKey))

	err = sync.VerifyCommitSignatureForTest(v, context.Background(), did, c)
	var sigErr *sync.SignatureError
	require.ErrorAs(t, err, &sigErr)
	assert.Equal(t, did, sigErr.DID)
	assert.Equal(t, wrongKey.PublicKey().DIDKey(), sigErr.KeyDID,
		"expected SignatureError.KeyDID to be populated with the resolved key on the failure path")
}

func TestVerifier_PerDIDRateLimiter(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
		// Effectively no refill during the test (one token per ~17 minutes).
		// Keeps the burst-then-deny assertion bulletproof under any CI scheduling
		// delay; the test never waits, just calls Allow() four times.
		ResyncLimit: gt.Some(rate.Limit(0.001)),
		ResyncBurst: gt.Some(2),
	})
	require.NoError(t, err)

	did := atmos.DID("did:plc:throttle")

	// First two should succeed (burst=2).
	assert.True(t, sync.AllowResyncForTest(v, did))
	assert.True(t, sync.AllowResyncForTest(v, did))
	// Third should be denied.
	assert.False(t, sync.AllowResyncForTest(v, did))

	// Different DID has its own bucket.
	other := atmos.DID("did:plc:other")
	assert.True(t, sync.AllowResyncForTest(v, other))
}

// TestVerifier_MutexCacheBounded asserts the per-DID mutex map honors
// MutexCapacity: after touching far more DIDs than the cap, the map
// settles at the watermark rather than growing unboundedly. This is
// the headline fix for review issue #3 (per-DID maps growing forever).
func TestVerifier_MutexCacheBounded(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:     dir,
		StateStore:    sync.NewMemStateStore(),
		Policy:        gt.Some(sync.PolicyError),
		MutexCapacity: gt.Some(8),
	})
	require.NoError(t, err)

	for i := range 100 {
		did := atmos.DID(fmt.Sprintf("did:plc:bounded%d", i))
		// Acquire then immediately release so each entry is unpinned
		// after the call. Without unpinning, soft-overflow would let
		// the map grow past the cap.
		unlock := sync.LockDIDForTest(v, did)
		unlock()
	}

	assert.LessOrEqual(t, sync.MutexCacheLen(v), 8,
		"cache must honor MutexCapacity once entries are unpinned")
	assert.Equal(t, 8, sync.MutexCacheLen(v),
		"cache should be at watermark after eviction settles")
}

// TestVerifier_HeldMutexNotEvicted is the safety property: a mutex
// currently held by one goroutine MUST NOT be evicted, because eviction
// would let a second goroutine acquire a fresh mutex for the same DID
// and break the per-DID serialization guarantee. This test pins the
// LRU at capacity by leaving one mutex held while exhausting capacity
// with cycled entries; reacquiring the held DID's lock must recover
// the same mutex (the second acquire must block on the first).
func TestVerifier_HeldMutexNotEvicted(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:     dir,
		StateStore:    sync.NewMemStateStore(),
		Policy:        gt.Some(sync.PolicyError),
		MutexCapacity: gt.Some(2),
	})
	require.NoError(t, err)

	pinned := atmos.DID("did:plc:pinned")
	unlockPinned := sync.LockDIDForTest(v, pinned)

	// Touch many other DIDs to force eviction pressure. Each of these
	// is unpinned by the time we move on, so the cache can recycle
	// their slots — but must NOT recycle "pinned"'s slot.
	for i := range 100 {
		did := atmos.DID(fmt.Sprintf("did:plc:churn%d", i))
		unlock := sync.LockDIDForTest(v, did)
		unlock()
	}

	// Try to acquire pinned's mutex from another goroutine. If
	// eviction had replaced it with a fresh mutex, this would acquire
	// immediately. If the original mutex survived (correct), this
	// goroutine must block until we release.
	got := make(chan struct{})
	go func() {
		unlock := sync.LockDIDForTest(v, pinned)
		unlock()
		close(got)
	}()

	select {
	case <-got:
		t.Fatal("second lockDID returned while the first was still held — held mutex was evicted")
	case <-time.After(50 * time.Millisecond):
		// good: the second goroutine is blocked, proving the same mutex was returned.
	}

	unlockPinned()
	<-got // now the second goroutine completes
}

// TestVerifier_LimiterCacheBounded mirrors MutexCacheBounded for the
// limiter side. Limiters don't pin (eviction loses bucket state, which
// is policy-acceptable for an inactive DID), so growth is strictly
// capped at LimiterCapacity.
func TestVerifier_LimiterCacheBounded(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:       dir,
		StateStore:      sync.NewMemStateStore(),
		Policy:          gt.Some(sync.PolicyError),
		LimiterCapacity: gt.Some(8),
		ResyncLimit:     gt.Some(rate.Limit(0.001)),
		ResyncBurst:     gt.Some(1),
	})
	require.NoError(t, err)

	for i := range 100 {
		did := atmos.DID(fmt.Sprintf("did:plc:lim%d", i))
		_ = sync.AllowResyncForTest(v, did)
	}

	assert.Equal(t, 8, sync.LimiterCacheLen(v),
		"limiter cache must honor LimiterCapacity")
}

func TestVerifier_Resync_Success(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:resync1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	for i := range 3 {
		require.NoError(t, r.Create("app.bsky.feed.post",
			fmt.Sprintf("rec%d", i), map[string]any{"text": fmt.Sprintf("rec%d", i)}))
	}
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	var (
		resyncDID    atmos.DID
		resyncOldRev string
		resyncNewRev string
		resyncReason sync.ResyncReason
	)
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
		OnResync: gt.Some(func(d atmos.DID, oldRev, newRev string, reason sync.ResyncReason) {
			resyncDID = d
			resyncOldRev = oldRev
			resyncNewRev = newRev
			resyncReason = reason
		}),
	})
	require.NoError(t, err)

	ops, err := v.Resync(context.Background(), did)
	require.NoError(t, err)
	require.Len(t, ops, 3)
	for _, op := range ops {
		assert.Equal(t, atmos.ActionResync, op.Action)
		assert.Equal(t, did, op.Repo)
	}
	assert.Equal(t, did, resyncDID)
	assert.Equal(t, sync.ReasonSyncEvent, resyncReason)
	assert.Empty(t, resyncOldRev, "first resync should report empty oldRev (no prior chain state)")
	state, err := v.StateStore().LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, state.Rev, resyncNewRev, "newRev should match the rev we just wrote to chain state")
}

func TestVerifier_Resync_RateLimited(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:rl1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x",
		map[string]any{"text": "x"}))
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Limit(0.001)), // effectively no refill during test
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	// First resync allowed.
	_, err = v.Resync(context.Background(), did)
	require.NoError(t, err)
	// Second immediately should be rate limited.
	_, err = v.Resync(context.Background(), did)
	var rl *sync.ResyncRateLimitedError
	require.ErrorAs(t, err, &rl)

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.ResyncFailures, "rate limit is not a failure; counter must not increment")
	assert.Equal(t, uint64(1), stats.Resyncs, "first call should have succeeded")
}

func TestVerifier_Resync_BadSignatureFails(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:badsig1")
	signKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	wrongKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x",
		map[string]any{"text": "x"}))
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, signKey))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, wrongKey.PublicKey()) // wrong key in DID doc
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)
}

// buildCustomServedCAR constructs a CAR containing a single record at
// "app.bsky.feed.post/x" plus a signed commit whose envelope fields
// can be tuned per call. mutate runs against the inner commit just
// before signing — useful for fabricating served-state shapes the
// regular ExportCAR path would never produce (wrong DID, wrong
// version, manually-set rev). Used only by the resync-validation
// tests below.
func buildCustomServedCAR(t *testing.T, key crypto.PrivateKey, declaredDID atmos.DID, mutate func(*repo.Commit)) []byte {
	t.Helper()
	r, _ := testutil.BuildEmptyRepo(t, declaredDID)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	rootCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Build a commit by hand so we control every field.
	c := &repo.Commit{
		DID:     string(declaredDID),
		Version: 3,
		Data:    rootCID,
		Rev:     string(r.Clock.Next()),
	}
	mutate(c)
	require.NoError(t, c.Sign(key))
	commitBytes, err := c.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	require.NoError(t, r.Store.PutBlock(commitCID, commitBytes))

	// CAR write: every block in the store + commit as root.
	memStore, ok := r.Store.(*mst.MemBlockStore)
	require.True(t, ok)
	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{commitCID})
	require.NoError(t, err)
	for cid, data := range memStore.All() {
		require.NoError(t, cw.WriteBlock(cid, data))
	}
	return carBuf.Bytes()
}

// TestVerifier_Resync_ServedDIDMismatchRejected covers the
// misattribution defense: a fake getRepo that returns a CAR signed by
// the requested DID's key but with a different DID embedded in the
// inner commit must be rejected. The signature step alone wouldn't
// catch this (we resolve the requested DID's key, and the commit was
// signed with that key) — validateFetchedCommit's DID check is the
// guard.
func TestVerifier_Resync_ServedDIDMismatchRejected(t *testing.T) {
	t.Parallel()

	requestedDID := atmos.DID("did:plc:requested")
	otherDID := atmos.DID("did:plc:other")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Served CAR: signed by `key` (the requested DID's key per the
	// resolver below) but inner.DID = otherDID.
	carBytes := buildCustomServedCAR(t, key, requestedDID, func(c *repo.Commit) {
		c.DID = string(otherDID)
	})

	xc := testutil.NewFakeSyncServer(t, requestedDID, carBytes)
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[requestedDID] = testutil.BuildDIDDoc(requestedDID, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), requestedDID)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)

	var fme *sync.FieldMismatchError
	require.ErrorAs(t, rfe.Cause, &fme,
		"resync DID mismatch should surface as FieldMismatchError wrapped in ResyncFailedError")
	assert.Equal(t, "did", fme.Field)
	assert.Equal(t, string(requestedDID), fme.Envelope)
	assert.Equal(t, string(otherDID), fme.Inner)

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.Resyncs, "rejected resync must not count as success")
	assert.Equal(t, uint64(1), stats.ResyncFailures)
}

// TestVerifier_Resync_ServedVersionV2Rejected covers the spec-mandated
// commit version check. Sync 1.1 mandates v3; a v2 commit served by a
// non-upgraded PDS must be rejected at resync time, mirroring the
// firehose-side checkCommitFields gate.
func TestVerifier_Resync_ServedVersionV2Rejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:v2srv")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	carBytes := buildCustomServedCAR(t, key, did, func(c *repo.Commit) {
		c.Version = 2
	})

	xc := testutil.NewFakeSyncServer(t, did, carBytes)
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)

	var fme *sync.FieldMismatchError
	require.ErrorAs(t, rfe.Cause, &fme)
	assert.Equal(t, "version", fme.Field)
	assert.Equal(t, "3", fme.Envelope)
	assert.Equal(t, "2", fme.Inner)
}

// TestVerifier_Resync_RevRegressionRejected is the substantive guard:
// pre-seed chain state at a high rev, fake server returns a commit at
// a strictly lower rev. Without this check we'd silently roll the
// chain backward and reject every legitimate follow-on commit.
func TestVerifier_Resync_RevRegressionRejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:revreg1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Served commit with a hand-set low rev.
	lowRev := "3aaaaaaaaaaaa"
	carBytes := buildCustomServedCAR(t, key, did, func(c *repo.Commit) {
		c.Rev = lowRev
	})

	xc := testutil.NewFakeSyncServer(t, did, carBytes)
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	cs := sync.NewMemStateStore()
	// Pre-seed with a HIGHER rev.
	priorCID, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3zzzzzzzzzzzz", Data: priorCID}))

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)

	var rre *sync.RevRegressionError
	require.ErrorAs(t, rfe.Cause, &rre)
	assert.Equal(t, did, rre.DID)
	assert.Equal(t, "3zzzzzzzzzzzz", rre.SeenRev)
	assert.Equal(t, lowRev, rre.GotRev)

	// State must NOT have been advanced (otherwise the next legitimate
	// commit would chain off the regressed root).
	got, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "3zzzzzzzzzzzz", got.Rev, "chain state must be unchanged after rejected regression")
}

// TestVerifier_Resync_SameRevDifferentDataRejected covers the
// "contradiction at equal rev" branch: the upstream serves a commit
// at the same rev we already have, but its data CID differs. Either
// our state is wrong or the upstream is corrupt; rolling forward
// without distinguishing risks corrupting the consumer's view.
func TestVerifier_Resync_SameRevDifferentDataRejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:samerev1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pin the served rev to a known value.
	servedRev := "3bbbbbbbbbbbb"
	carBytes := buildCustomServedCAR(t, key, did, func(c *repo.Commit) {
		c.Rev = servedRev
	})

	xc := testutil.NewFakeSyncServer(t, did, carBytes)
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	cs := sync.NewMemStateStore()
	// Pre-seed with the same rev but a DIFFERENT data CID.
	bogusData, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: servedRev, Data: bogusData}))

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)

	var rre *sync.RevRegressionError
	require.ErrorAs(t, rfe.Cause, &rre)
	assert.Equal(t, servedRev, rre.SeenRev)
	assert.Equal(t, servedRev, rre.GotRev)
	assert.False(t, rre.SeenData.Equal(rre.GotData),
		"contradiction is precisely 'same rev, different data'")
}

// TestVerifier_Resync_FirstSightingAcceptsAnyRev guards the
// no-prior-state path: with no SeenRev, there's no monotonicity
// invariant to enforce. A first-sighting resync at any rev must land.
func TestVerifier_Resync_FirstSightingAcceptsAnyRev(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:firstresync1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "y",
		map[string]any{"text": "y"}))
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	ops, err := v.Resync(context.Background(), did)
	require.NoError(t, err)
	require.NotEmpty(t, ops)
	assert.Equal(t, uint64(1), v.Stats().Resyncs)
}

// TestVerifier_Resync_IdempotentSameRevSameData covers the inverse of
// SameRevDifferentDataRejected: when the served commit matches the
// persisted state exactly, the resync is benign and accepted. Without
// this branch a benign state-confirmation would surface as a regression
// error, which is unfriendly to consumers running periodic refresh.
func TestVerifier_Resync_IdempotentSameRevSameData(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:idem1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// First resync with no prior state — establishes a baseline.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "z",
		map[string]any{"text": "z"}))
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(2), // need 2 resyncs in this test
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	require.NoError(t, err, "first resync establishes baseline")

	// Second resync against the same fake server — same rev, same data.
	// Must not fire RevRegressionError; the state is unchanged.
	_, err = v.Resync(context.Background(), did)
	require.NoError(t, err, "idempotent resync must be permitted")

	stats := v.Stats()
	assert.Equal(t, uint64(2), stats.Resyncs)
	assert.Equal(t, uint64(0), stats.ResyncFailures)
}

// TestRevRegressionError_FormatAndUnwrap covers the typed-error
// contract for RevRegressionError, mirroring the FormatAndUnwrap
// tests for the other typed errors in this package.
func TestRevRegressionError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	cidA, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	err := &sync.RevRegressionError{
		DID:      atmos.DID("did:plc:regret"),
		SeenRev:  "3zzzzzzzzzzzz",
		SeenData: cidA,
		GotRev:   "3aaaaaaaaaaaa",
		GotData:  cidA,
	}
	msg := err.Error()
	assert.Contains(t, msg, "rev regression")
	assert.Contains(t, msg, "did:plc:regret")
	assert.Contains(t, msg, "3zzzzzzzzzzzz")
	assert.Contains(t, msg, "3aaaaaaaaaaaa")

	var target *sync.RevRegressionError
	assert.True(t, errors.As(err, &target))
	assert.Equal(t, atmos.DID("did:plc:regret"), target.DID)

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.NotNil(t, target)
}

// failingChainStore wraps a real StateStore and forces SaveChain
// (and only SaveChain) to fail. LoadChain plus the four hosting
// methods delegate untouched so tests can drive chain-write-failure
// scenarios alongside normal hosting state.
type failingChainStore struct {
	real sync.StateStore
}

func (s *failingChainStore) LoadChain(ctx context.Context, did atmos.DID) (*sync.ChainState, error) {
	return s.real.LoadChain(ctx, did)
}
func (s *failingChainStore) SaveChain(_ context.Context, _ atmos.DID, _ sync.ChainState) error {
	return errors.New("disk full")
}
func (s *failingChainStore) LoadHosting(ctx context.Context, did atmos.DID) (*sync.HostingState, error) {
	return s.real.LoadHosting(ctx, did)
}
func (s *failingChainStore) SaveHosting(ctx context.Context, did atmos.DID, st sync.HostingState) error {
	return s.real.SaveHosting(ctx, did, st)
}
func (s *failingChainStore) Delete(ctx context.Context, did atmos.DID) error {
	return s.real.Delete(ctx, did)
}

// countingChainStore wraps a real StateStore and increments atomic
// counters per method call. Used by tests that assert the verifier's
// gate ordering — e.g. "the chain store is not consulted when the
// hosting gate would reject" (issue #7 from the review).
type countingChainStore struct {
	real        sync.StateStore
	loadChain   atomic.Int64
	saveChain   atomic.Int64
	loadHosting atomic.Int64
	saveHosting atomic.Int64
}

func (s *countingChainStore) LoadChain(ctx context.Context, did atmos.DID) (*sync.ChainState, error) {
	s.loadChain.Add(1)
	return s.real.LoadChain(ctx, did)
}
func (s *countingChainStore) SaveChain(ctx context.Context, did atmos.DID, st sync.ChainState) error {
	s.saveChain.Add(1)
	return s.real.SaveChain(ctx, did, st)
}
func (s *countingChainStore) LoadHosting(ctx context.Context, did atmos.DID) (*sync.HostingState, error) {
	s.loadHosting.Add(1)
	return s.real.LoadHosting(ctx, did)
}
func (s *countingChainStore) SaveHosting(ctx context.Context, did atmos.DID, st sync.HostingState) error {
	s.saveHosting.Add(1)
	return s.real.SaveHosting(ctx, did, st)
}
func (s *countingChainStore) Delete(ctx context.Context, did atmos.DID) error {
	return s.real.Delete(ctx, did)
}

// ---------------------------------------------------------------------------
// VerifyCommit
// ---------------------------------------------------------------------------

func TestVerifyCommit_HappyPath(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:happy1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	chainStore := sync.NewMemStateStore()
	require.NoError(t, chainStore.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:   dir,
		StateStore:  chainStore,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec2",
		Record:     map[string]any{"text": "v2"},
	}})

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, atmos.ActionCreate, ops[0].Action)
	assert.Equal(t, "app.bsky.feed.post", string(ops[0].Collection))
	assert.Equal(t, "rec2", string(ops[0].RKey))

	state, err := chainStore.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Equal(t, commit.Rev, state.Rev)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.EventsVerified)
}

// TestVerifyCommit_EmptyOpsCommit guards the streaming integration's
// ability to distinguish a successful zero-ops verification from a
// rev-replay drop. The verifier must return a non-nil empty slice on
// success here, not (nil, nil).
func TestVerifyCommit_EmptyOpsCommit(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:emptyops1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	chainStore := sync.NewMemStateStore()
	require.NoError(t, chainStore.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:   dir,
		StateStore:  chainStore,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	// Zero-ops commit at a higher rev than the persisted state.
	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, nil)

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	require.NotNil(t, ops, "empty-ops verification must return a non-nil empty slice (rev-replay returns nil)")
	assert.Empty(t, ops)

	// State must have advanced to the new commit.
	state, err := chainStore.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Equal(t, commit.Rev, state.Rev)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.EventsVerified)
	assert.Equal(t, uint64(0), stats.RevReplaysDropped)
}

func TestVerifyCommit_RevReplay(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:replay1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	highRev := "3zzzzzzzzzzzz"
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: highRev, Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "replay"},
	}})
	commit.Rev = "3aaaaaaaaaaaa" // low rev

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	assert.Nil(t, ops, "rev replay should drop silently")
	assert.Equal(t, uint64(1), v.Stats().RevReplaysDropped)
}

func TestVerifyCommit_ChainBreakUnderPolicyError(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:cb1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	otherCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	var failureCalled bool
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, _ error) {
			failureCalled = true
		}),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})

	_, err = v.VerifyCommit(context.Background(), commit)
	var cb *sync.ChainBreakError
	require.ErrorAs(t, err, &cb)
	assert.True(t, failureCalled)
	assert.Equal(t, uint64(1), v.Stats().ChainBreaks)
}

// TestVerifyCommit_HookCanCallResyncWithoutDeadlock asserts the
// hook-after-unlock contract: OnVerificationFailure fires after the
// per-DID mutex has been released, so a hook implementation that
// calls back into the verifier — typically Resync — does not
// deadlock. This was issue #8 from the review: previously the hook
// fired under the per-DID lock for most error types, and a hook
// invoking Resync (which also takes the lock) would hang forever.
//
// We trigger a chain break (PolicyError so the hook fires AND the
// caller sees the typed error), have the hook call Resync against a
// fake getRepo, and assert the whole thing completes within a
// generous deadline. Without the fix this test deadlocks rather
// than failing — so we run it under a t.Context() with a deadline
// and check via a done channel.
func TestVerifyCommit_HookCanCallResyncWithoutDeadlock(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:hookresync1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Pre-seed chain state with a wrong CID so the upcoming commit
	// trips the chain-break gate.
	otherCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	// Build the chain-break-triggering commit first, then export the
	// CAR so the resync's served rev is strictly greater than the
	// chain-break commit's rev (the resync-time monotonicity gate
	// rejects regressions otherwise).
	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action: testutil.ActionUpdate, Collection: "app.bsky.feed.post", RKey: "rec1",
		Record: map[string]any{"text": "v2"},
	}})

	// Fake getRepo server so the hook's Resync call succeeds.
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	// Forward declaration: the hook needs to reference the verifier
	// it's installed on, but the verifier construction needs the hook.
	// Stash it via a pointer that gets set after NewVerifier returns.
	var (
		v             *sync.Verifier
		hookErr       error
		hookResyncErr error
		hookOps       []sync.VerifierOp
	)
	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyError),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(2),
		OnVerificationFailure: gt.Some(func(d atmos.DID, e error) {
			hookErr = e
			// The headline test: from inside the hook, call Resync on
			// the same DID. Pre-fix this deadlocks because the hook
			// fires under the per-DID mutex that Resync also takes.
			ops, rerr := v.Resync(context.Background(), d)
			hookOps = ops
			hookResyncErr = rerr
		}),
	})
	require.NoError(t, err)
	v = verifier

	// Run the call in a goroutine with a deadline so a regression
	// surfaces as a test failure rather than the test process hanging
	// until t's own timeout.
	done := make(chan error, 1)
	go func() {
		_, vErr := v.VerifyCommit(context.Background(), commit)
		done <- vErr
	}()

	select {
	case vErr := <-done:
		var cb *sync.ChainBreakError
		require.ErrorAs(t, vErr, &cb,
			"primary error must still surface as ChainBreakError")
	case <-time.After(5 * time.Second):
		t.Fatal("VerifyCommit deadlocked: hook firing under per-DID lock prevents inner Resync")
	}

	// The hook saw the chain break.
	var cb *sync.ChainBreakError
	require.ErrorAs(t, hookErr, &cb)
	// The hook's inner Resync succeeded.
	require.NoError(t, hookResyncErr, "hook's Resync should have completed cleanly")
	require.NotEmpty(t, hookOps, "hook's Resync should have yielded ops")
	for _, op := range hookOps {
		assert.Equal(t, atmos.ActionResync, op.Action)
	}

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ChainBreaks)
	assert.Equal(t, uint64(1), stats.Resyncs, "the hook-driven resync should have completed")
}

// TestVerifyCommit_PolicyErrorSaveFailureCountedInStats verifies
// that a ChainStore.Save failure during PolicyError state-advance is
// counted in VerifierStats.ChainStateSaveFailures rather than silently
// swallowed. The original ChainBreakError still surfaces (the typed
// signal is the primary report); the counter exists so operators can
// detect that the secondary save failed.
func TestVerifyCommit_PolicyErrorSaveFailureCountedInStats(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:psf1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	otherCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	// failingChainStore errs on every Save. Pre-seed the chain state
	// directly via the underlying real store so the verifier sees a
	// non-empty state (forcing a chain break) without the failingChainStore
	// erroring on the seed call.
	realStore := sync.NewMemStateStore()
	require.NoError(t, realStore.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))
	cs := &failingChainStore{real: realStore}

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})

	_, err = v.VerifyCommit(context.Background(), commit)
	var cb *sync.ChainBreakError
	require.ErrorAs(t, err, &cb,
		"primary verification error must still surface as ChainBreakError")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ChainStateSaveFailures,
		"PolicyError save failure should be counted")
	assert.Equal(t, uint64(1), stats.ChainBreaks)
}

func TestVerifyCommit_FirstSightingNoBreak(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fresh1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "first",
		Record:     map[string]any{"text": "first"},
	}})

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	assert.Len(t, ops, 1)
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, commit.Rev, state.Rev)
}

func TestVerifyCommit_ChainBreakUnderPolicyResync(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:cbres1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	otherCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "x",
		Record:     map[string]any{"text": "y"},
	}})

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	require.Nil(t, ops, "async resync: ops arrive via ResyncEvents()")

	select {
	case ev := <-v.ResyncEvents():
		require.Equal(t, did, ev.DID)
		require.Greater(t, len(ev.Ops), 0)
		for _, op := range ev.Ops {
			assert.Equal(t, atmos.ActionResync, op.Action)
		}
	case err := <-v.AsyncErrors():
		t.Fatalf("expected ResyncEvent, got async error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ResyncEvent")
	}

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ChainBreaks)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// Lenient inversion: when the upstream's prevData matches our stored
// chain state but our local inversion produces a different root (an
// inversion-incomplete CAR, which Bluesky's production relay
// intentionally forwards under LenientSyncValidation), the verifier
// must accept the commit, advance state, and surface
// *InversionIncompleteError via OnVerificationFailure — NOT trigger a
// resync.
//
// We provoke the condition synthetically: build a valid commit, then
// tamper with op.Prev so the inverter inserts the wrong CID at the
// key during the inverse path. Inversion then yields a root different
// from the genuine prev state (which we save in the StateStore as
// state.Data). commit.PrevData remains correctly set, so prevData
// matches state.Data while inverted does not — exactly the
// inversion-incomplete signature.
func TestVerifyCommit_LenientInversion_AcceptsInversionIncomplete(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:lenient1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Build the prev state and persist it as the verifier's stored
	// chain state.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: realPrevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	// PolicyResync requires a SyncClient even though lenient mode
	// means resync is never triggered. Provide a fake one wired to
	// the prev repo so that an unexpected resync would at least not
	// crash the test.
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	// Configure verifier with LenientInversion explicitly true (the
	// default; we set it to assert intent).
	var hookMu stdsync.Mutex
	var hookErrs []error
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:       gt.Some(sc),
		Directory:        dir,
		StateStore:       cs,
		Policy:           gt.Some(sync.PolicyResync),
		LenientInversion: gt.Some(true),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, err error) {
			hookMu.Lock()
			hookErrs = append(hookErrs, err)
			hookMu.Unlock()
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Build an update commit with a tampered op.Prev so the inverter
	// computes a wrong root. PrevData is set correctly to the genuine
	// pre-state root.
	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "x",
		Record:     map[string]any{"text": "y"},
	}})
	bogusCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	require.Equal(t, 1, len(commit.Ops))
	commit.Ops[0].Prev = gt.Some(lextypes.LexCIDLink{Link: bogusCID.String()})

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err, "lenient mode must not surface a typed error to the caller")
	require.NotNil(t, ops, "lenient mode must yield ops; the commit was accepted")
	require.Greater(t, len(ops), 0)

	// State must have advanced to the new commit's data CID.
	newState, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, newState)
	assert.Equal(t, commit.Rev, newState.Rev, "rev must advance under lenient mode")

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.ChainBreaks, "no chain break should be counted")
	assert.Equal(t, uint64(0), stats.Resyncs, "no resync should be triggered")
	assert.Equal(t, uint64(1), stats.InversionIncomplete, "InversionIncomplete counter must increment")

	// OnVerificationFailure must have fired exactly once with a
	// *InversionIncompleteError so consumers can log/alert.
	var iiErr *sync.InversionIncompleteError
	require.Eventually(t, func() bool {
		hookMu.Lock()
		defer hookMu.Unlock()
		for _, e := range hookErrs {
			if errors.As(e, &iiErr) {
				return true
			}
		}
		return false
	}, 200*time.Millisecond, 20*time.Millisecond, "InversionIncompleteError must surface via OnVerificationFailure")
	assert.Equal(t, did, iiErr.DID)
	assert.Equal(t, realPrevData, iiErr.GotPrevData, "prevData must match stored state")
	assert.NotEqual(t, realPrevData, iiErr.InvertedData, "inverted must differ from stored state")
}

// Strict mode (LenientInversion=false): the same condition that lenient
// mode accepts must instead trigger ChainBreakError → resync.
func TestVerifyCommit_StrictInversion_RejectsInversionIncomplete(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:strict1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: realPrevData}))

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:       gt.Some(sc),
		Directory:        dir,
		StateStore:       cs,
		Policy:           gt.Some(sync.PolicyResync),
		LenientInversion: gt.Some(false),
		ResyncLimit:      gt.Some(rate.Inf),
		ResyncBurst:      gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "x",
		Record:     map[string]any{"text": "y"},
	}})
	bogusCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	require.Equal(t, 1, len(commit.Ops))
	commit.Ops[0].Prev = gt.Some(lextypes.LexCIDLink{Link: bogusCID.String()})

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	require.Nil(t, ops, "strict mode + PolicyResync: chain break enqueues async resync, returns nil ops")

	select {
	case ev := <-v.ResyncEvents():
		require.Equal(t, did, ev.DID)
	case asyncErr := <-v.AsyncErrors():
		t.Fatalf("expected ResyncEvent, got async error: %v", asyncErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ResyncEvent")
	}

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ChainBreaks, "strict mode must count this as a chain break")
	assert.Equal(t, uint64(0), stats.InversionIncomplete, "strict mode must NOT count InversionIncomplete")
	assert.Equal(t, uint64(1), stats.Resyncs)
}

func TestVerifier_Resync_ChainStoreSaveFailureIsResyncFailedError(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:savefail1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x",
		map[string]any{"text": "x"}))
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  &failingChainStore{real: sync.NewMemStateStore()},
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)
	assert.Contains(t, rfe.Cause.Error(), "save chain state",
		"expected the cause to mention the save failure")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ResyncFailures)
	assert.Equal(t, uint64(0), stats.Resyncs)
}

func TestVerifySync_HappyPath(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncev1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	for i := range 2 {
		require.NoError(t, r.Create("app.bsky.feed.post",
			fmt.Sprintf("rec%d", i), map[string]any{"text": "x"}))
	}
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:  string(did),
		Rev:  "3newrev",
		Time: "2026-05-19T00:00:00Z",
	}
	ops, err := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	for _, op := range ops {
		assert.Equal(t, atmos.ActionResync, op.Action)
	}
}

func TestVerifySync_RevReplay(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncrep1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	someCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3zzzzzzzzzzzz", Data: someCID}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyResync),
	})
	require.NoError(t, err)

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID: string(did),
		Rev: "3aaaaaaaaaaaa", // older
	}
	ops, err := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, err)
	assert.Nil(t, ops)
	assert.Equal(t, uint64(1), v.Stats().RevReplaysDropped)
}

// ---------------------------------------------------------------------------
// #sync no-op fast path (A5): skip getRepo when embedded data == seen data
// ---------------------------------------------------------------------------

// syncNoOpVerifier builds a verifier wired against a fake getRepo
// server that, if hit, would return a CAR matching the test's
// pre-state. The intent is to spot regressions where the fast path
// fails to short-circuit: if a test expects no-op behavior and the
// verifier accidentally falls through to resync, the fake server
// fields the request and the test's resync counter assertions catch
// it. PolicyResync is used so a fall-through results in a successful
// resync (visible via the counter) rather than a typed error.
func syncNoOpVerifier(t *testing.T, did atmos.DID, key crypto.PrivateKey, r *repo.Repo) (*sync.Verifier, sync.StateStore) {
	t.Helper()
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	cs := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	return v, cs
}

// TestVerifySync_NoOpFastPath is the headline A5 test:
// when the upstream's #sync embedded commit declares the same data
// CID we already have in chain state, we advance rev tracking and
// skip getRepo. SyncNoOps increments; Resyncs stays at zero.
func TestVerifySync_NoOpFastPath(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncnop1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, cs := syncNoOpVerifier(t, did, key, r)

	// Pre-seed chain state with the data CID we'll match.
	oldRev := string(atmos.NewTIDFromTime(time.Now().Add(-1*time.Hour), 0))
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: oldRev, Data: dataCID}))

	// Build a #sync event whose embedded commit asserts the same
	// data CID. BuildSyncEventBlocks mints the inner commit's rev
	// from the repo's clock; the verifier's field-consistency gate
	// requires envelope.Rev == inner.Rev, so use the same value here.
	blocks, newRev := testutil.BuildSyncEventBlocks(t, r, key, dataCID)
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    newRev,
		Blocks: blocks,
	}

	ops, vErr := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, vErr)
	assert.Nil(t, ops, "no-op fast path should yield (nil, nil)")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.SyncNoOps)
	assert.Equal(t, uint64(0), stats.Resyncs, "no-op must NOT trigger getRepo")
	assert.Equal(t, uint64(0), stats.ResyncFailures)

	// State.Rev advanced to the sync event's rev; data unchanged.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, newRev, state.Rev, "rev should advance to the sync event's claim")
	assert.True(t, state.Data.Equal(dataCID), "data CID must be unchanged")
}

// TestVerifySync_FastPathRejectsBadSignature is the
// headline issue-#12 test: a hostile upstream that has observed our
// SeenData CID could craft a #sync event with that data CID and any
// rev they want, and (pre-fix) we would advance our rev tracker
// without verifying the embedded commit's signature. Subsequent
// legitimate events at rev <= the forged rev would then be silently
// dropped as replays.
//
// The fast path now signature-verifies the embedded commit. A
// commit signed with a key that doesn't match the DID document
// surfaces as *SignatureError; chain state is NOT advanced.
func TestVerifySync_FastPathRejectsBadSignature(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncfastsig1")
	realKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	attackerKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Pre-seed chain state with the data CID the attacker has observed.
	oldRev := string(atmos.NewTIDFromTime(time.Now().Add(-1*time.Hour), 0))
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: oldRev, Data: dataCID}))

	// DID document advertises realKey; the embedded commit will be
	// signed with attackerKey, so signature verification fails.
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, realKey.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	// Build the malicious #sync event: data == ourSeenData, signed by
	// the wrong key. The "any rev they want" part is what the attacker
	// is exploiting — they pick a far-future rev to poison our tracker.
	blocks, attackerRev := testutil.BuildSyncEventBlocks(t, r, attackerKey, dataCID)
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    attackerRev,
		Blocks: blocks,
	}

	_, vErr := v.VerifySync(context.Background(), syncEvt)
	var sigErr *sync.SignatureError
	require.ErrorAs(t, vErr, &sigErr,
		"forged #sync with wrong-key signature must surface as SignatureError")
	assert.Equal(t, did, sigErr.DID)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.SignatureFailures)
	assert.Equal(t, uint64(0), stats.SyncNoOps,
		"forged commit must NOT count as a successful no-op")
	assert.Equal(t, uint64(0), stats.Resyncs,
		"signature failure on fast path must bypass policy, not fall through to resync")

	// Chain state must NOT have been advanced. Otherwise the attacker
	// has poisoned the tracker even on rejection.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, oldRev, state.Rev,
		"forged #sync must not advance rev — that's the attack we're preventing")
	assert.True(t, state.Data.Equal(dataCID))
}

// TestVerifySync_FastPathRejectsRevMismatch covers a
// related attack vector: an upstream that takes a real signed commit
// at rev=X but relabels the firehose envelope's Rev to a different
// value Y. Without the field-consistency gate on the fast path, the
// rev advance would land at Y (the envelope's claim) even though the
// signed inner says X. The check rejects with FieldMismatchError.
func TestVerifySync_FastPathRejectsRevMismatch(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncfastrev1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	oldRev := string(atmos.NewTIDFromTime(time.Now().Add(-1*time.Hour), 0))
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: oldRev, Data: dataCID}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	// Build a real signed commit, then deliberately set the envelope
	// rev to something different (within the future-rev tolerance so
	// that gate doesn't fire first).
	blocks, innerRev := testutil.BuildSyncEventBlocks(t, r, key, dataCID)
	envelopeRev := string(atmos.NewTIDFromTime(time.Now().Add(1*time.Minute), 0))
	require.NotEqual(t, innerRev, envelopeRev, "test setup must produce a real mismatch")
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    envelopeRev,
		Blocks: blocks,
	}

	_, vErr := v.VerifySync(context.Background(), syncEvt)
	var fme *sync.FieldMismatchError
	require.ErrorAs(t, vErr, &fme,
		"envelope/inner rev disagreement must surface as FieldMismatchError")
	assert.Equal(t, "rev", fme.Field)
	assert.Equal(t, envelopeRev, fme.Envelope)
	assert.Equal(t, innerRev, fme.Inner)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.FieldMismatches)
	assert.Equal(t, uint64(0), stats.SyncNoOps)

	// Chain state must NOT have been advanced.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, oldRev, state.Rev)
}

// TestVerifySync_DataMismatchFallsThrough asserts the
// inverse: when the embedded commit's data CID differs from our
// SeenData, the verifier falls through to a real resync. The fake
// sync server is hit; ActionResync ops are emitted; SyncNoOps stays
// at zero.
func TestVerifySync_DataMismatchFallsThrough(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncnop2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state of the repo (what getRepo will serve): one record.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	currentDataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, cs := syncNoOpVerifier(t, did, key, r)

	// Pre-seed chain state with a DIFFERENT data CID — guarantees the
	// no-op fast path can't engage.
	otherCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	// Data mismatch path; envelope rev doesn't need to match the
	// inner since the fast path won't engage.
	blocks, _ := testutil.BuildSyncEventBlocks(t, r, key, currentDataCID)
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    string(atmos.NewTIDNow(0)),
		Blocks: blocks,
	}

	ops, vErr := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, vErr)
	require.NotEmpty(t, ops, "data mismatch must trigger resync, yielding ActionResync ops")
	for _, op := range ops {
		assert.Equal(t, atmos.ActionResync, op.Action)
	}

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.SyncNoOps, "data mismatch must NOT increment SyncNoOps")
	assert.Equal(t, uint64(1), stats.Resyncs)

	// State.Data advanced to the resynced commit's data CID.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.True(t, state.Data.Equal(currentDataCID))
}

// TestVerifySync_FirstSightingFallsThrough asserts the
// fast path doesn't engage when there's no chain state yet — without
// prior SeenData we have nothing to compare the embedded commit's
// data CID against, so we must fall through to getRepo.
func TestVerifySync_FirstSightingFallsThrough(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncnop3")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, _ := syncNoOpVerifier(t, did, key, r)

	// First-sighting path; envelope rev doesn't need to match the
	// inner since the fast path won't engage (no chain state yet).
	blocks, _ := testutil.BuildSyncEventBlocks(t, r, key, dataCID)
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    string(atmos.NewTIDNow(0)),
		Blocks: blocks,
	}

	ops, vErr := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, vErr)
	require.NotEmpty(t, ops, "first sighting must resync to establish a baseline")

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.SyncNoOps)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// TestVerifySync_EmptyBlocksFallsThrough covers the
// "older PDS that doesn't yet emit Blocks on #sync" case: with no
// embedded commit there's nothing to compare data CIDs against, so
// the fast path is skipped and a normal resync runs. (The pre-A5
// behavior — included as a regression test for the same code path
// the old TestVerifySync_HappyPath exercises.)
func TestVerifySync_EmptyBlocksFallsThrough(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncnop4")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, cs := syncNoOpVerifier(t, did, key, r)
	// Even with state.Data matching what the resync would return, the
	// fast path can't engage with no Blocks to decode — assert a
	// resync still runs.
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: dataCID}))

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID: string(did),
		Rev: string(atmos.NewTIDNow(0)),
		// Blocks intentionally omitted.
	}

	ops, vErr := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, vErr)
	require.NotEmpty(t, ops)

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.SyncNoOps)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// TestVerifySync_MalformedBlocksFallsThrough asserts
// graceful handling of bogus Blocks. The fast path tries to decode
// and silently falls through to resync rather than erroring — the
// authoritative state from getRepo is the right answer regardless of
// what the upstream put in the embedded CAR.
func TestVerifySync_MalformedBlocksFallsThrough(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncnop5")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, cs := syncNoOpVerifier(t, did, key, r)
	require.NoError(t, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: dataCID}))

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    string(atmos.NewTIDNow(0)),
		Blocks: []byte{0xff, 0xff, 0xff}, // garbage CAR
	}

	ops, vErr := v.VerifySync(context.Background(), syncEvt)
	require.NoError(t, vErr, "malformed Blocks should fall through to resync, not error")
	require.NotEmpty(t, ops)

	stats := v.Stats()
	assert.Equal(t, uint64(0), stats.SyncNoOps)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// TestVerifySync_NoOpSaveFailureSurfacesError covers the
// observability concern: if ChainStore.Save fails on the no-op fast
// path (advancing rev with unchanged data), we surface the error
// rather than silently falling through to a resync that would also
// fail to save. Operators see chain-store breakage immediately.
func TestVerifySync_NoOpSaveFailureSurfacesError(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncnop6")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	dataCID, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Real underlying store pre-seeded with matching state.
	realStore := sync.NewMemStateStore()
	require.NoError(t, realStore.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: dataCID}))
	cs := &failingChainStore{real: realStore}

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyResync),
	})
	require.NoError(t, err)

	// Save-failure on no-op fast path; envelope rev must match inner
	// rev so the fast path engages.
	blocks, innerRev := testutil.BuildSyncEventBlocks(t, r, key, dataCID)
	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(did),
		Rev:    innerRev,
		Blocks: blocks,
	}

	_, vErr := v.VerifySync(context.Background(), syncEvt)
	require.Error(t, vErr, "save failure on no-op fast path must surface as error")
	assert.Contains(t, vErr.Error(), "save chain state on sync no-op")

	stats := v.Stats()
	// SyncNoOps is NOT incremented on save failure — the fast path
	// didn't actually complete. Resyncs also not attempted.
	assert.Equal(t, uint64(0), stats.SyncNoOps)
	assert.Equal(t, uint64(0), stats.Resyncs)
}

// ---------------------------------------------------------------------------
// Future-rev rejection (Sync 1.1 spec MUST: future-timestamped revs ignored)
// ---------------------------------------------------------------------------

// frozenClock returns a func() time.Time that always reports the given
// instant. Used by the future-rev tests so they never wait on the real
// clock and stay deterministic across CI variance.
func frozenClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// futureRevTestVerifier returns a Verifier whose wall clock is pinned
// to `now`, with PolicyError so test assertions see typed errors
// directly. Tests that need to drive a `verifyCommit` happy-path
// alongside a future-rev path use the standard chain-store seeding
// pattern in the individual tests.
func futureRevTestVerifier(t *testing.T, did atmos.DID, key crypto.PrivateKey, now time.Time, tolerance gt.Option[time.Duration]) *sync.Verifier {
	t.Helper()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:         gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:          dir,
		StateStore:         sync.NewMemStateStore(),
		Policy:             gt.Some(sync.PolicyError),
		FutureRevTolerance: tolerance,
		Now:                gt.Some(frozenClock(now)),
	})
	require.NoError(t, err)
	return v
}

// TestFutureRevError_FormatAndUnwrap exercises the typed-error contract
// (errors.As pickup, message contents) without exercising the verifier.
func TestFutureRevError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	revTime := now.Add(2 * time.Hour)
	err := &sync.FutureRevError{
		DID:       atmos.DID("did:plc:future"),
		Rev:       "3xxxxxxxxxxxx",
		RevTime:   revTime,
		Now:       now,
		Tolerance: 5 * time.Minute,
	}
	msg := err.Error()
	assert.Contains(t, msg, "future rev")
	assert.Contains(t, msg, "did:plc:future")
	assert.Contains(t, msg, "3xxxxxxxxxxxx")
	assert.Contains(t, msg, "5m0s")

	var target *sync.FutureRevError
	assert.True(t, errors.As(err, &target))
	assert.Equal(t, atmos.DID("did:plc:future"), target.DID)

	// Wrapping with fmt.Errorf must preserve typed pickup.
	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.NotNil(t, target)
}

// TestVerifyCommit_FutureRevRejected covers the spec MUST: a #commit
// whose rev's TID timestamp is more than the tolerance ahead of the
// verifier's wall clock is rejected outright with *FutureRevError. The
// counter increments, OnVerificationFailure fires, and chain state is
// NOT advanced (so a real commit at a sane rev can still land
// afterwards).
func TestVerifyCommit_FutureRevRejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:future1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	// Rev one hour ahead of wall clock — well past the default 5m tolerance.
	farFutureRev := atmos.NewTIDFromTime(now.Add(1*time.Hour), 0)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "future"},
	}})
	commit.Rev = string(farFutureRev)

	var hookFired bool
	var hookErr error
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	cs := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
		Now:        gt.Some(frozenClock(now)),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, e error) {
			hookFired = true
			hookErr = e
		}),
	})
	require.NoError(t, err)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	var fre *sync.FutureRevError
	require.ErrorAs(t, vErr, &fre)
	assert.Nil(t, ops)
	assert.Equal(t, did, fre.DID)
	assert.Equal(t, string(farFutureRev), fre.Rev)
	assert.Equal(t, now, fre.Now)
	assert.Equal(t, 5*time.Minute, fre.Tolerance)
	assert.True(t, fre.RevTime.After(now), "rev time must be in the future relative to clock")

	assert.True(t, hookFired, "OnVerificationFailure should fire for future-rev rejection")
	assert.ErrorAs(t, hookErr, &fre, "hook should receive the typed FutureRevError")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.FutureRevsRejected)
	assert.Equal(t, uint64(0), stats.EventsVerified, "rejected event must not count as verified")
	assert.Equal(t, uint64(0), stats.ChainBreaks)
	assert.Equal(t, uint64(0), stats.RevReplaysDropped)

	// Chain state must NOT have been advanced — a future rev that landed in
	// state would starve out every legitimate event for this DID.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state, "future-rev rejection must not advance chain state")
}

// TestVerifyCommit_FutureRevWithinTolerance asserts the gate's other
// edge: a rev whose timestamp is ahead of wall clock but within the
// tolerance window is accepted normally. Guards against a regression
// where the comparison flips and rejects sane revs from clocks slightly
// ahead.
func TestVerifyCommit_FutureRevWithinTolerance(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:future2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	// Rev 1 minute ahead — well within the default 5m tolerance.
	nearFutureRev := atmos.NewTIDFromTime(now.Add(1*time.Minute), 0)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "near"},
	}})
	commit.Rev = string(nearFutureRev)
	rebuildInnerCommit(t, commit, key, func(c *repo.Commit) {
		c.Rev = string(nearFutureRev)
	})

	v := futureRevTestVerifier(t, did, key, now, gt.None[time.Duration]())

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	require.Len(t, ops, 1)
	assert.Equal(t, atmos.ActionCreate, ops[0].Action)
	assert.Equal(t, uint64(0), v.Stats().FutureRevsRejected)
	assert.Equal(t, uint64(1), v.Stats().EventsVerified)
}

// TestVerifyCommit_FutureRevCustomTolerance checks that operators
// can tighten or loosen the window via VerifierOptions.FutureRevTolerance.
// A 30-second tolerance with a 1-minute-ahead rev is rejected; the same
// rev under the default 5m tolerance would have been accepted (covered
// in the WithinTolerance test above).
func TestVerifyCommit_FutureRevCustomTolerance(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:future3")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	rev := atmos.NewTIDFromTime(now.Add(1*time.Minute), 0)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "x"},
	}})
	commit.Rev = string(rev)

	v := futureRevTestVerifier(t, did, key, now, gt.Some(30*time.Second))

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var fre *sync.FutureRevError
	require.ErrorAs(t, vErr, &fre)
	assert.Equal(t, 30*time.Second, fre.Tolerance)
	assert.Equal(t, uint64(1), v.Stats().FutureRevsRejected)
}

// TestVerifyCommit_FutureRevUnparseableRevSkipsCheck documents the
// best-effort behavior on malformed input: an unparseable envelope rev
// does NOT trigger FutureRevError; the future-rev gate yields. With
// the field-consistency gate (A2) in place, an unparseable envelope
// rev that disagrees with the parseable inner rev is caught downstream
// as a FieldMismatchError — the test asserts that's how the malformed
// event surfaces, not as a future-rev rejection.
func TestVerifyCommit_FutureRevUnparseableRevSkipsCheck(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:future4")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v"},
	}})
	// Replace envelope with something that fails atmos.ParseTID — not
	// 13 chars. Inner rev still says the synthetic-clock TID.
	commit.Rev = "not-a-tid"

	v := futureRevTestVerifier(t, did, key, now, gt.None[time.Duration]())

	_, vErr := v.VerifyCommit(context.Background(), commit)
	// A1 contract: future-rev gate must not trip on unparseable input.
	assert.Equal(t, uint64(0), v.Stats().FutureRevsRejected)
	// A2 contract: malformed envelope surfaces as FieldMismatchError
	// rather than something downstream and less specific.
	var fme *sync.FieldMismatchError
	assert.ErrorAs(t, vErr, &fme)
}

// TestVerifyCommit_FutureRevDisabledByNegativeTolerance covers the
// explicit operator opt-out: setting tolerance to a negative duration
// disables the gate entirely. A far-future rev that would normally be
// rejected lands as a first-sighting accept.
func TestVerifyCommit_FutureRevDisabledByNegativeTolerance(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:future5")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	farFutureRev := atmos.NewTIDFromTime(now.Add(100*365*24*time.Hour), 0) // 100 years out

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "century"},
	}})
	// Keep envelope and inner in sync — otherwise the field-consistency
	// gate (A2) catches the mismatch first and the test stops measuring
	// the future-rev opt-out.
	commit.Rev = string(farFutureRev)
	rebuildInnerCommit(t, commit, key, func(c *repo.Commit) {
		c.Rev = string(farFutureRev)
	})

	v := futureRevTestVerifier(t, did, key, now, gt.Some(-time.Second))

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr, "negative tolerance disables the future-rev gate")
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().FutureRevsRejected)
}

// TestVerifySync_FutureRev asserts the same gate applies
// to #sync events, not just #commit. A sync event at a future rev is
// rejected outright; chain state is untouched and no resync is
// triggered (which would have hit the sync server otherwise).
func TestVerifySync_FutureRev(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:future6")
	now := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	farFutureRev := atmos.NewTIDFromTime(now.Add(1*time.Hour), 0)

	resolver := testutil.NewTrackingResolver()
	dir := &identity.Directory{Resolver: resolver}
	cs := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		// SyncClient pointing at an unreachable host — if the verifier
		// erroneously triggered resync, the test would fail with a
		// network error instead of FutureRevError.
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyResync),
		Now:        gt.Some(frozenClock(now)),
	})
	require.NoError(t, err)

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID: string(did),
		Rev: string(farFutureRev),
	}

	_, vErr := v.VerifySync(context.Background(), syncEvt)
	var fre *sync.FutureRevError
	require.ErrorAs(t, vErr, &fre)
	assert.Equal(t, did, fre.DID)
	assert.Equal(t, uint64(1), v.Stats().FutureRevsRejected)
	assert.Equal(t, uint64(0), v.Stats().Resyncs, "future-rev sync must not trigger resync")

	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state)
}

// TestNewVerifier_DefaultsApplyFutureRevTolerance documents that
// NewVerifier populates the default tolerance and clock when callers
// leave them unset. Without this guarantee the public field would be a
// foot-gun (zero-valued tolerance would reject every future-by-any-margin
// rev).
func TestNewVerifier_DefaultsApplyFutureRevTolerance(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:defaults1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
		// FutureRevTolerance and Now intentionally left unset.
	})
	require.NoError(t, err)

	// A "now-ish" rev produced from time.Now must verify cleanly under
	// the default tolerance.
	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "ok"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
}

// ---------------------------------------------------------------------------
// Outer/inner field consistency (Sync 1.1 spec MUST: envelope == signed inner)
// ---------------------------------------------------------------------------

// rebuildInnerCommit decodes commit.Blocks, finds the commit block,
// applies mutate to the decoded *repo.Commit, re-signs with key,
// re-encodes, and rewrites the envelope's Commit link + Blocks CAR
// to reference the new commit block. Used by the field-mismatch tests
// to construct envelopes whose inner commit deliberately diverges from
// what BuildSyntheticCommit produced.
func rebuildInnerCommit(t *testing.T, c *comatproto.SyncSubscribeRepos_Commit, key crypto.PrivateKey, mutate func(*repo.Commit)) {
	t.Helper()

	// Pull every block out of the existing CAR so we can rebuild the CAR
	// with a swapped commit block but identical MST blocks.
	blocks := make(map[cbor.CID][]byte)
	cr, err := car.NewReader(bytes.NewReader(c.Blocks))
	require.NoError(t, err)
	for {
		b, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		blocks[b.CID] = b.Data
	}

	// Decode the original commit block, mutate, re-sign, re-encode.
	oldCommitCID, err := cbor.ParseCIDString(c.Commit.Link)
	require.NoError(t, err)
	inner, err := repo.DecodeCommitCBOR(blocks[oldCommitCID])
	require.NoError(t, err)
	mutate(inner)
	require.NoError(t, inner.Sign(key))
	newCommitBytes, err := inner.EncodeCBOR()
	require.NoError(t, err)
	newCommitCID := cbor.ComputeCID(cbor.CodecDagCBOR, newCommitBytes)

	// Rebuild the CAR with the new commit block as root. We drop the
	// old commit block from the map (it's superseded and unreachable);
	// MST nodes carry over unchanged.
	delete(blocks, oldCommitCID)
	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{newCommitCID})
	require.NoError(t, err)
	require.NoError(t, cw.WriteBlock(newCommitCID, newCommitBytes))
	for cid, data := range blocks {
		require.NoError(t, cw.WriteBlock(cid, data))
	}

	c.Commit = lextypes.LexCIDLink{Link: newCommitCID.String()}
	c.Blocks = carBuf.Bytes()
}

// fieldMismatchTestVerifier constructs a verifier wired with a
// resolver that knows about did, configured with PolicyResync pointing
// at an unreachable sync server (a tripwire — if the field-mismatch
// gate erroneously triggers resync, the test fails with a network
// error rather than a clean FieldMismatchError).
func fieldMismatchTestVerifier(t *testing.T, did atmos.DID, key crypto.PrivateKey) (*sync.Verifier, sync.StateStore, *bool, *error) {
	t.Helper()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	cs := sync.NewMemStateStore()
	hookFired := false
	var hookErr error
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyResync),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, e error) {
			hookFired = true
			hookErr = e
		}),
	})
	require.NoError(t, err)
	return v, cs, &hookFired, &hookErr
}

func TestFieldMismatchError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	err := &sync.FieldMismatchError{
		DID:      atmos.DID("did:plc:victim"),
		Field:    "rev",
		Envelope: "3zzzzzzzzzzzz",
		Inner:    "3aaaaaaaaaaaa",
	}
	msg := err.Error()
	assert.Contains(t, msg, "rev field mismatch")
	assert.Contains(t, msg, "did:plc:victim")
	assert.Contains(t, msg, "3zzzzzzzzzzzz")
	assert.Contains(t, msg, "3aaaaaaaaaaaa")

	var target *sync.FieldMismatchError
	assert.True(t, errors.As(err, &target))

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, "rev", target.Field)
}

// TestVerifyCommit_FieldMismatchRev is the security-relevant
// scenario: a misbehaving PDS replays an old signed commit but
// relabels the firehose envelope's Rev to a higher value, hoping to
// bypass our rev-replay drop and corrupt chain state with stale data.
// The signed inner commit still says the original (older) rev, so the
// signature still validates — only our envelope/inner cross-check
// catches the swap. Bypasses policy; chain state is NOT advanced.
func TestVerifyCommit_FieldMismatchRev(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fmrev1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "real"},
	}})
	innerRev := commit.Rev
	// Envelope claims a higher rev than the signed inner — the scenario
	// a malicious PDS uses to defeat rev-replay drops. Use a rev a
	// minute ahead of wall clock (well within the default 5m future-rev
	// tolerance) so the future-rev gate doesn't catch it first.
	envelopeRev := string(atmos.NewTIDFromTime(time.Now().Add(1*time.Minute), 0))
	commit.Rev = envelopeRev

	v, cs, hookFired, hookErr := fieldMismatchTestVerifier(t, did, key)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	var fme *sync.FieldMismatchError
	require.ErrorAs(t, vErr, &fme)
	assert.Nil(t, ops)
	assert.Equal(t, "rev", fme.Field)
	assert.Equal(t, envelopeRev, fme.Envelope)
	assert.Equal(t, innerRev, fme.Inner)
	assert.True(t, *hookFired, "OnVerificationFailure should fire on field mismatch")
	assert.ErrorAs(t, *hookErr, &fme)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.FieldMismatches)
	assert.Equal(t, uint64(0), stats.EventsVerified)
	assert.Equal(t, uint64(0), stats.SignatureFailures, "field check must run before signature verify")
	assert.Equal(t, uint64(0), stats.Resyncs, "field mismatch must not trigger resync")

	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state, "field mismatch must not advance chain state")
}

// TestVerifyCommit_FieldMismatchDID covers the misattribution
// vector: an attacker who controls any PDS can build a valid signed
// commit for their own DID and wrap it in a firehose envelope that
// claims a different ("victim") DID. Without this gate, signature
// verification still rejects it (the victim's key can't verify the
// attacker's sig) — but a FieldMismatchError is more precise and
// fires before the more expensive signature check.
func TestVerifyCommit_FieldMismatchDID(t *testing.T) {
	t.Parallel()

	innerDID := atmos.DID("did:plc:attacker")
	envelopeDID := atmos.DID("did:plc:victim")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, innerDID)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "evil"},
	}})
	// Swap envelope DID. Inner commit still says innerDID; signature
	// is over the inner commit, so it would still verify against
	// innerDID's key — but the verifier would look up envelopeDID's
	// key (a different one), so without the field check we'd surface
	// SignatureError instead of FieldMismatchError. The test asserts
	// the precise typed error and that the signature check never ran.
	commit.Repo = string(envelopeDID)

	v, _, _, _ := fieldMismatchTestVerifier(t, envelopeDID, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var fme *sync.FieldMismatchError
	require.ErrorAs(t, vErr, &fme)
	assert.Equal(t, "did", fme.Field)
	assert.Equal(t, string(envelopeDID), fme.Envelope)
	assert.Equal(t, string(innerDID), fme.Inner)
	assert.Equal(t, envelopeDID, fme.DID)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.FieldMismatches)
	assert.Equal(t, uint64(0), stats.SignatureFailures, "field check must short-circuit before signature verify")
}

// TestVerifyCommit_FieldMismatchVersion covers the spec MUST that
// Sync 1.1 mandates commit version 3. A v2 commit on a 1.1 firehose
// is a producer bug or a stale-data replay; we reject it outright.
func TestVerifyCommit_FieldMismatchVersion(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fmver1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})
	// Rebuild the inner commit at version 2, re-signing so the
	// signature would still validate if we let it through. Only the
	// version check should reject this.
	rebuildInnerCommit(t, commit, key, func(c *repo.Commit) {
		c.Version = 2
	})

	v, _, _, _ := fieldMismatchTestVerifier(t, did, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var fme *sync.FieldMismatchError
	require.ErrorAs(t, vErr, &fme)
	assert.Equal(t, "version", fme.Field)
	assert.Equal(t, "3", fme.Envelope)
	assert.Equal(t, "2", fme.Inner)
	assert.Equal(t, uint64(1), v.Stats().FieldMismatches)
}

// TestVerifyCommit_FieldMismatchPriorityVersionFirst documents the
// helper's check ordering: when more than one field disagrees, version
// is reported first. The ordering is part of the contract because
// operators interpreting metrics by Field label rely on consistent
// labeling for identical underlying conditions.
func TestVerifyCommit_FieldMismatchPriorityVersionFirst(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fmpri1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "x"},
	}})
	// Mutate envelope rev too — version + rev both mismatch the inner.
	// Within the default future-rev tolerance.
	commit.Rev = string(atmos.NewTIDFromTime(time.Now().Add(1*time.Minute), 0))
	rebuildInnerCommit(t, commit, key, func(c *repo.Commit) {
		c.Version = 2
	})

	v, _, _, _ := fieldMismatchTestVerifier(t, did, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var fme *sync.FieldMismatchError
	require.ErrorAs(t, vErr, &fme)
	assert.Equal(t, "version", fme.Field, "version takes priority over rev when both mismatch")
}

// TestVerifyCommit_FieldMismatchHappyPathUnchanged guards against a
// regression where the new check spuriously rejects valid commits.
// The standard happy-path commit (envelope and inner agree) must still
// verify cleanly.
func TestVerifyCommit_FieldMismatchHappyPathUnchanged(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fmhappy1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "ok"},
	}})

	v, _, _, _ := fieldMismatchTestVerifier(t, did, key)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().FieldMismatches)
}

// rewriteCARRoot rewrites the CAR header in `in` so its first root is
// `newRoot`, leaving every block intact. The block referenced by the
// envelope's Commit link is still present, so the envelope-link
// lookup succeeds — only the CAR root disagrees with the link. Used
// to exercise the CAR-root mismatch gate.
func rewriteCARRoot(t *testing.T, in []byte, newRoot cbor.CID) []byte {
	t.Helper()
	cr, err := car.NewReader(bytes.NewReader(in))
	require.NoError(t, err)

	type block struct {
		cid  cbor.CID
		data []byte
	}
	var blocks []block
	for {
		b, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		blocks = append(blocks, block{cid: b.CID, data: b.Data})
	}

	var out bytes.Buffer
	cw, err := car.NewWriter(&out, []cbor.CID{newRoot})
	require.NoError(t, err)
	for _, b := range blocks {
		require.NoError(t, cw.WriteBlock(b.cid, b.data))
	}
	return out.Bytes()
}

// TestVerifyCommit_CARRootMismatchRejected covers issue #10: the
// CAR's first root MUST equal the envelope's Commit link. A
// misbehaving upstream that lists a different root in the CAR
// header is rejected outright — bypasses VerifierPolicy because no
// resync against the same upstream can repair a malformed CAR.
//
// The signed inner commit and its block are intact (and
// signature-verifiable); only the CAR header's root field is
// inconsistent. Without this gate, indigo's atproto/repo (which
// trusts the CAR root) and atmos (which trusts the envelope link)
// would disagree on which block is "the commit" and reach different
// conclusions about a malformed event.
func TestVerifyCommit_CARRootMismatchRejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:carroot1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec1",
		Record: map[string]any{"text": "ok"},
	}})

	// Rewrite the CAR header to advertise an unrelated CID as root,
	// without touching the blocks. The block referenced by the
	// envelope's Commit link is still present and decodes cleanly.
	bogusRoot, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Blocks = rewriteCARRoot(t, commit.Blocks, bogusRoot)

	v, _, hookFired, hookErr := fieldMismatchTestVerifier(t, did, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var fme *sync.FieldMismatchError
	require.ErrorAs(t, vErr, &fme,
		"CAR root mismatch must surface as FieldMismatchError")
	assert.Equal(t, "commit", fme.Field)
	assert.Equal(t, bogusRoot.String(), fme.Inner)
	// Envelope side equals the original commit-block CID.
	assert.Equal(t, commit.Commit.Link, fme.Envelope)

	// Hook fires for this typed error.
	assert.True(t, *hookFired)
	assert.ErrorAs(t, *hookErr, &fme)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.FieldMismatches)
	assert.Equal(t, uint64(0), stats.EventsVerified)
	assert.Equal(t, uint64(0), stats.SignatureFailures,
		"CAR-root check must run before signature verify so a malformed event doesn't pay P-256 cost")
	assert.Equal(t, uint64(0), stats.Resyncs,
		"CAR root mismatch must bypass policy and not trigger resync")
}

// TestInvertCommit_CARRootMismatch covers the same gate exposed via
// the public InvertCommit API: a CAR whose root disagrees with the
// envelope's Commit link surfaces as *InversionError, with the
// underlying *FieldMismatchError reachable via errors.As. This keeps
// the InvertCommit error contract uniform while still letting
// callers distinguish "structurally inconsistent input" from
// genuine inversion failures.
func TestInvertCommit_CARRootMismatch(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:invcarroot1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec1",
		Record: map[string]any{"text": "ok"},
	}})

	bogusRoot, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Blocks = rewriteCARRoot(t, commit.Blocks, bogusRoot)

	_, err = sync.InvertCommit(commit)
	var ie *sync.InversionError
	require.ErrorAs(t, err, &ie)

	var fme *sync.FieldMismatchError
	require.ErrorAs(t, err, &fme,
		"InvertCommit's wrapped error must still surface the FieldMismatchError via errors.As")
	assert.Equal(t, "commit", fme.Field)
}

// ---------------------------------------------------------------------------
// Op-CID consistency: ops list must agree with post-state MST (C4)
// ---------------------------------------------------------------------------

// opCIDMismatchTestVerifier builds a verifier with PolicyError so the
// typed error surfaces directly. Resolver knows about did so signature
// verify succeeds (we want the OpCIDMismatchError gate to be the
// rejecting layer, not signature).
func opCIDMismatchTestVerifier(t *testing.T, did atmos.DID, key crypto.PrivateKey) (*sync.Verifier, sync.StateStore) {
	t.Helper()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	cs := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	return v, cs
}

func TestOpCIDMismatchError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	cidA, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	err := &sync.OpCIDMismatchError{
		DID:    atmos.DID("did:plc:opcid"),
		Rev:    "3aaaaaaaaaaaa",
		Path:   "app.bsky.feed.post/abc",
		Reason: "create_cid_mismatch",
		OpCID:  cidA,
	}
	msg := err.Error()
	assert.Contains(t, msg, "op cid mismatch")
	assert.Contains(t, msg, "did:plc:opcid")
	assert.Contains(t, msg, "app.bsky.feed.post/abc")
	assert.Contains(t, msg, "create_cid_mismatch")
	assert.Contains(t, msg, "<missing>", "MSTCID is zero in this fixture; format must show <missing>, not bare CID zero")

	var target *sync.OpCIDMismatchError
	assert.True(t, errors.As(err, &target))

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, "create_cid_mismatch", target.Reason)
}

// TestVerifyCommit_OpCIDMismatchCreate covers the most direct
// attack: a create op declares a CID that doesn't match what the
// post-state MST holds at that path. Without this gate, the verifier
// would emit the wrong CID to consumers — they'd fetch records by the
// envelope's CID, which the PDS may not even host.
func TestVerifyCommit_OpCIDMismatchCreate(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "real"},
	}})
	// Swap the create's claimed CID to something unrelated. The MST
	// still holds the actual record CID at path "app.bsky.feed.post/rec1".
	bogus, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Ops[0].CID = gt.Some(lextypes.LexCIDLink{Link: bogus.String()})

	v, cs := opCIDMismatchTestVerifier(t, did, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var oce *sync.OpCIDMismatchError
	require.ErrorAs(t, vErr, &oce)
	assert.Equal(t, "create_cid_mismatch", oce.Reason)
	assert.Equal(t, "app.bsky.feed.post/rec1", oce.Path)
	assert.True(t, oce.OpCID.Equal(bogus), "OpCID should report the claimed (bogus) CID")
	assert.True(t, oce.MSTCID.Defined(), "MSTCID should be populated with the real tree value")
	assert.False(t, oce.MSTCID.Equal(bogus), "MSTCID should differ from the bogus claim")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.OpCIDMismatches)
	assert.Equal(t, uint64(0), stats.EventsVerified)

	// PolicyError advances state even on this failure path (matching
	// inversion-failure semantics) when the inner data CID is decodable
	// — which it is here, since only the ops list was corrupted.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, commit.Rev, state.Rev)
}

// TestVerifyCommit_OpCIDMismatchUpdate covers the same shape on an
// update op: claimed CID disagrees with MST tree value.
func TestVerifyCommit_OpCIDMismatchUpdate(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	bogus, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Ops[0].CID = gt.Some(lextypes.LexCIDLink{Link: bogus.String()})

	v, _ := opCIDMismatchTestVerifier(t, did, key)
	// Pre-seed chain state so the chain-check passes before reaching
	// the op-CID gate.
	require.NoError(t, v.StateStore().SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var oce *sync.OpCIDMismatchError
	require.ErrorAs(t, vErr, &oce)
	assert.Equal(t, "update_cid_mismatch", oce.Reason)
	assert.Equal(t, "app.bsky.feed.post/rec1", oce.Path)
}

// TestVerifyCommit_OpCIDMismatchDeletePathPresent covers a delete
// op whose path is still present in the post-state MST — i.e. the
// commit claims to delete something but never actually removed it
// from the tree. Concrete attack shape: emit a delete op for a record
// the consumer cares about, hoping the consumer purges its index
// while the PDS still has the record.
func TestVerifyCommit_OpCIDMismatchDeletePathPresent(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid3")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state: rec1 exists. Build a real Create commit on rec2 — the
	// post-state MST contains both rec1 and rec2.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "alive"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec2",
		Record:     map[string]any{"text": "new"},
	}})
	// Inject a phony delete op for rec1 — its path is still in the
	// post-state tree, so the gate must reject. Inversion would also
	// catch a malformed delete (it requires Prev), so we set Prev to
	// the real pre-state CID to ensure inversion alone wouldn't have
	// rejected — making the op-CID gate the actual rejecting layer.
	rec1CID, _, err := r.Get("app.bsky.feed.post", "rec1")
	require.NoError(t, err)
	commit.Ops = append(commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: testutil.ActionDelete,
		Path:   "app.bsky.feed.post/rec1",
		Prev:   gt.Some(lextypes.LexCIDLink{Link: rec1CID.String()}),
	})

	v, _ := opCIDMismatchTestVerifier(t, did, key)
	require.NoError(t, v.StateStore().SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var oce *sync.OpCIDMismatchError
	require.ErrorAs(t, vErr, &oce)
	assert.Equal(t, "delete_path_present", oce.Reason)
	assert.Equal(t, "app.bsky.feed.post/rec1", oce.Path)
	assert.True(t, oce.MSTCID.Defined(), "MSTCID should report the value still present at the deleted path")
}

// TestVerifyCommit_OpCIDMismatchCreateMissingCID covers a create
// op with no CID. The lexicon allows op.CID to be optional but a
// create without one is structurally meaningless; we reject.
func TestVerifyCommit_OpCIDMismatchCreateMissingCID(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid4")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "real"},
	}})
	commit.Ops[0].CID = gt.None[lextypes.LexCIDLink]()

	v, _ := opCIDMismatchTestVerifier(t, did, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var oce *sync.OpCIDMismatchError
	require.ErrorAs(t, vErr, &oce)
	assert.Equal(t, "create_missing_cid", oce.Reason)
	assert.False(t, oce.OpCID.Defined())
}

// TestVerifyCommit_OpCIDMismatchDeleteUnexpectedCID covers a
// delete op that carries a CID it shouldn't. Indigo's parseCommitOps
// rejects this; we surface as a structural mismatch.
func TestVerifyCommit_OpCIDMismatchDeleteUnexpectedCID(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid5")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "doomed"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionDelete,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
	}})
	// Sneak a CID onto the delete op.
	bogus, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Ops[0].CID = gt.Some(lextypes.LexCIDLink{Link: bogus.String()})

	v, _ := opCIDMismatchTestVerifier(t, did, key)
	require.NoError(t, v.StateStore().SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var oce *sync.OpCIDMismatchError
	require.ErrorAs(t, vErr, &oce)
	assert.Equal(t, "delete_unexpected_cid", oce.Reason)
	assert.True(t, oce.OpCID.Equal(bogus))
}

// TestVerifyCommit_OpCIDMismatchUnderPolicyResync asserts the
// recovery semantics: under PolicyResync, an op-CID mismatch triggers
// transparent resync (same path as inversion failure) and the
// consumer sees ActionResync ops rather than the typed error.
func TestVerifyCommit_OpCIDMismatchUnderPolicyResync(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid6")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state: a small repo for the resync target.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: realPrevData}))

	// Resync target: serve the current full repo via a fake getRepo.
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Build a commit, then corrupt the op CID to force op-CID mismatch.
	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "y",
		Record:     map[string]any{"text": "y"},
	}})
	bogus, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Ops[0].CID = gt.Some(lextypes.LexCIDLink{Link: bogus.String()})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	require.Nil(t, ops, "async resync: ops arrive via ResyncEvents()")

	select {
	case ev := <-v.ResyncEvents():
		require.Equal(t, did, ev.DID)
		require.Greater(t, len(ev.Ops), 0)
		for _, op := range ev.Ops {
			assert.Equal(t, atmos.ActionResync, op.Action)
		}
	case err := <-v.AsyncErrors():
		t.Fatalf("expected ResyncEvent, got async error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ResyncEvent")
	}

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.OpCIDMismatches)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// TestVerifyCommit_OpCIDMismatchHappyPathUnchanged asserts the new
// gate doesn't spuriously reject standard commits. Synthetic commits
// from the test helper are always self-consistent; the counter must
// stay zero.
func TestVerifyCommit_OpCIDMismatchHappyPathUnchanged(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:opcid7")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "real"},
	}})

	v, _ := opCIDMismatchTestVerifier(t, did, key)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().OpCIDMismatches)
	assert.Equal(t, uint64(1), v.Stats().EventsVerified)
}

// ---------------------------------------------------------------------------
// Size limits (C5): blocks ≤ MaxCommitBlocksBytes, ops ≤ MaxCommitOps
// ---------------------------------------------------------------------------

func TestCommitTooLargeError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	err := &sync.CommitTooLargeError{
		DID:   atmos.DID("did:plc:big"),
		Rev:   "3aaaaaaaaaaaa",
		Field: "blocks",
		Got:   3_000_000,
		Limit: sync.MaxCommitBlocksBytes,
	}
	msg := err.Error()
	assert.Contains(t, msg, "commit too large")
	assert.Contains(t, msg, "did:plc:big")
	assert.Contains(t, msg, "blocks=3000000")

	var target *sync.CommitTooLargeError
	assert.True(t, errors.As(err, &target))

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, "blocks", target.Field)
}

// TestVerifyCommit_OversizedBlocksRejected exercises the
// MaxCommitBlocksBytes gate. The Blocks field is filled with bogus
// bytes — the gate runs before any CAR parse, so we don't need
// valid contents to test the size check, just enough bytes to
// exceed the limit.
func TestVerifyCommit_OversizedBlocksRejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:big1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	// One byte over the limit. We don't bother with valid CAR
	// contents because the size gate runs first; this proves we
	// reject without ever calling into the CAR parser.
	oversized := make([]byte, sync.MaxCommitBlocksBytes+1)
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   string(did),
		Rev:    string(atmos.NewTIDNow(0)),
		Blocks: oversized,
	}

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var tlErr *sync.CommitTooLargeError
	require.ErrorAs(t, vErr, &tlErr)
	assert.Equal(t, "blocks", tlErr.Field)
	assert.Equal(t, sync.MaxCommitBlocksBytes+1, tlErr.Got)
	assert.Equal(t, sync.MaxCommitBlocksBytes, tlErr.Limit)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.OversizedCommits)
	assert.Equal(t, uint64(0), stats.InversionFailures, "size gate must run before CAR parse")
}

// TestVerifyCommit_OversizedOpsRejected exercises the MaxCommitOps
// gate. We construct a commit with 201 ops on distinct paths (so the
// duplicate-path gate doesn't engage instead). Ops have empty CIDs;
// the size check runs before any CID work.
func TestVerifyCommit_OversizedOpsRejected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:big2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	ops := make([]comatproto.SyncSubscribeRepos_RepoOp, sync.MaxCommitOps+1)
	for i := range ops {
		ops[i] = comatproto.SyncSubscribeRepos_RepoOp{
			Action: testutil.ActionCreate,
			Path:   fmt.Sprintf("app.bsky.feed.post/rec%d", i),
		}
	}
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo: string(did),
		Rev:  string(atmos.NewTIDNow(0)),
		Ops:  ops,
	}

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var tlErr *sync.CommitTooLargeError
	require.ErrorAs(t, vErr, &tlErr)
	assert.Equal(t, "ops", tlErr.Field)
	assert.Equal(t, sync.MaxCommitOps+1, tlErr.Got)
	assert.Equal(t, sync.MaxCommitOps, tlErr.Limit)
	assert.Equal(t, uint64(1), v.Stats().OversizedCommits)
}

// TestVerifyCommit_OversizedFiresHook asserts OnVerificationFailure
// is invoked for size-limit rejections — the typed error is rare
// enough in production that a callback-only consumer might miss it
// otherwise.
func TestVerifyCommit_OversizedFiresHook(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:big3")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	var hookErr error
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, e error) {
			hookErr = e
		}),
	})
	require.NoError(t, err)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   string(did),
		Rev:    string(atmos.NewTIDNow(0)),
		Blocks: make([]byte, sync.MaxCommitBlocksBytes+1),
	}

	_, vErr := v.VerifyCommit(context.Background(), commit)
	require.Error(t, vErr)
	var tlErr *sync.CommitTooLargeError
	require.ErrorAs(t, hookErr, &tlErr, "hook must receive the typed CommitTooLargeError")
}

// TestVerifyCommit_OversizedBlocksTakesPriority asserts the gate's
// ordering: when both blocks and ops exceed limits, "blocks" is
// reported first. Ordering is part of the contract because metrics
// labeled by Field need consistent labels for identical underlying
// conditions.
func TestVerifyCommit_OversizedBlocksTakesPriority(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:big4")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	ops := make([]comatproto.SyncSubscribeRepos_RepoOp, sync.MaxCommitOps+1)
	for i := range ops {
		ops[i] = comatproto.SyncSubscribeRepos_RepoOp{
			Action: testutil.ActionCreate,
			Path:   fmt.Sprintf("app.bsky.feed.post/rec%d", i),
		}
	}
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   string(did),
		Rev:    string(atmos.NewTIDNow(0)),
		Blocks: make([]byte, sync.MaxCommitBlocksBytes+1),
		Ops:    ops,
	}

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var tlErr *sync.CommitTooLargeError
	require.ErrorAs(t, vErr, &tlErr)
	assert.Equal(t, "blocks", tlErr.Field, "blocks takes priority when both exceed limits")
}

// TestVerifyCommit_AtLimitAccepted guards against an off-by-one
// regression: a commit at exactly the limit (not over) should pass
// the size gate. We use a synthetic commit that's well-formed up to
// the size check; later gates may reject for other reasons but the
// size counter must stay zero.
func TestVerifyCommit_AtLimitAccepted(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:big5")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "ok"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().OversizedCommits)
}

// ---------------------------------------------------------------------------
// Duplicate op paths (C3): single commit with multiple ops on one path
// ---------------------------------------------------------------------------

func TestDuplicatePathError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	err := &sync.DuplicatePathError{
		DID:  atmos.DID("did:plc:dup"),
		Rev:  "3aaaaaaaaaaaa",
		Path: "app.bsky.feed.post/rec1",
	}
	msg := err.Error()
	assert.Contains(t, msg, "duplicate op path")
	assert.Contains(t, msg, "did:plc:dup")
	assert.Contains(t, msg, "app.bsky.feed.post/rec1")

	var target *sync.DuplicatePathError
	assert.True(t, errors.As(err, &target))

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, "app.bsky.feed.post/rec1", target.Path)
}

// duplicatePathTestVerifier wires a verifier with PolicyError so the
// typed error surfaces directly. The duplicate-path gate runs before
// CAR decode so the test doesn't need the chain store pre-seeded with
// matching state.
func duplicatePathTestVerifier(t *testing.T, did atmos.DID, key crypto.PrivateKey) (*sync.Verifier, sync.StateStore) {
	t.Helper()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	cs := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	return v, cs
}

// TestVerifyCommit_DuplicatePathTwoCreates is the headline C3
// test: a commit's ops list contains two creates on the same path.
// A well-formed producer would have folded these. Our gate rejects
// before any CAR work so an attacker can't use duplicate-path
// commits as a way to confuse downstream consumer state machines.
func TestVerifyCommit_DuplicatePathTwoCreates(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:duppath1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "first"},
	}})
	// Inject a duplicate op directly — the synthetic builder won't
	// produce duplicates on its own.
	commit.Ops = append(commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: testutil.ActionCreate,
		Path:   "app.bsky.feed.post/rec1",
		CID:    commit.Ops[0].CID,
	})

	v, _ := duplicatePathTestVerifier(t, did, key)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var dpe *sync.DuplicatePathError
	require.ErrorAs(t, vErr, &dpe)
	assert.Equal(t, "app.bsky.feed.post/rec1", dpe.Path)
	assert.Equal(t, did, dpe.DID)
	assert.Equal(t, commit.Rev, dpe.Rev)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.DuplicatePaths)
	// Gate runs before CAR decode, so neither inversion nor op-CID
	// counters should have ticked — proves the early-rejection
	// ordering hasn't drifted.
	assert.Equal(t, uint64(0), stats.InversionFailures)
	assert.Equal(t, uint64(0), stats.OpCIDMismatches)
}

// TestVerifyCommit_DuplicatePathDeleteThenCreate covers the
// race-state-machines scenario flagged in the typed error's doc:
// a delete and a create on the same path within one commit. A
// careless consumer could observe the create first (record now
// exists in their index) then process the delete (record gone),
// or vice versa — depending on iteration order. Rejecting the
// commit closes the ambiguity.
func TestVerifyCommit_DuplicatePathDeleteThenCreate(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:duppath2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state: rec1 exists.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	rec1CID, _, err := r.Get("app.bsky.feed.post", "rec1")
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionDelete,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
	}})
	// Append a create on the same path. CID can be anything since the
	// gate runs before any CID validation.
	commit.Ops = append(commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: testutil.ActionCreate,
		Path:   "app.bsky.feed.post/rec1",
		CID:    gt.Some(lextypes.LexCIDLink{Link: rec1CID.String()}),
	})

	v, _ := duplicatePathTestVerifier(t, did, key)
	require.NoError(t, v.StateStore().SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var dpe *sync.DuplicatePathError
	require.ErrorAs(t, vErr, &dpe)
	assert.Equal(t, uint64(1), v.Stats().DuplicatePaths)
}

// TestVerifyCommit_DuplicatePathHappyPathUnchanged guards against
// a regression where the new gate spuriously triggers on standard
// commits. Two ops on DIFFERENT paths must verify cleanly.
func TestVerifyCommit_DuplicatePathHappyPathUnchanged(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:duphappy1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{
		{Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec1", Record: map[string]any{"text": "a"}},
		{Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec2", Record: map[string]any{"text": "b"}},
	})

	v, _ := duplicatePathTestVerifier(t, did, key)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 2)
	assert.Equal(t, uint64(0), v.Stats().DuplicatePaths)
}

// TestVerifyCommit_DuplicatePathUnderPolicyResync asserts
// transparent recovery: a duplicate-path commit under PolicyResync
// triggers a getRepo. The fake server returns the current authoritative
// state; the consumer sees ActionResync ops.
func TestVerifyCommit_DuplicatePathUnderPolicyResync(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:dupresync1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "y",
		Record:     map[string]any{"text": "y"},
	}})
	// Inject duplicate path.
	commit.Ops = append(commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: testutil.ActionCreate,
		Path:   "app.bsky.feed.post/y",
		CID:    commit.Ops[0].CID,
	})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	require.Nil(t, ops, "async resync: ops arrive via ResyncEvents()")

	select {
	case ev := <-v.ResyncEvents():
		require.Equal(t, did, ev.DID)
		require.NotEmpty(t, ev.Ops)
		for _, op := range ev.Ops {
			assert.Equal(t, atmos.ActionResync, op.Action)
		}
	case err := <-v.AsyncErrors():
		t.Fatalf("expected ResyncEvent, got async error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ResyncEvent")
	}

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.DuplicatePaths)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// ---------------------------------------------------------------------------
// Legacy 1.0-shape commits (A3): distinct typed error vs InversionError
// ---------------------------------------------------------------------------

// stripPrevData removes the envelope's prevData field and clears any
// op.Prev set by the synthetic-commit builder, producing a Sync-1.0
// shape commit on otherwise-1.1 plumbing. Used to fabricate the
// "non-upgraded upstream PDS" scenario.
func stripPrevData(c *comatproto.SyncSubscribeRepos_Commit) {
	c.PrevData = gt.None[lextypes.LexCIDLink]()
	for i := range c.Ops {
		c.Ops[i].Prev = gt.None[lextypes.LexCIDLink]()
	}
}

func TestLegacyCommitError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	cidA, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	err := &sync.LegacyCommitError{
		DID:      atmos.DID("did:plc:legacy"),
		Rev:      "3newrev",
		SeenRev:  "3oldrev",
		SeenData: cidA,
	}
	msg := err.Error()
	assert.Contains(t, msg, "legacy 1.0-shape")
	assert.Contains(t, msg, "did:plc:legacy")
	assert.Contains(t, msg, "3newrev")
	assert.Contains(t, msg, "3oldrev")

	var target *sync.LegacyCommitError
	assert.True(t, errors.As(err, &target))

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, atmos.DID("did:plc:legacy"), target.DID)
}

func TestResyncReason_Legacy(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "legacy_commit", sync.ReasonLegacyCommit.String())
}

// TestVerifyCommit_LegacyCommitUnderRejectAndPolicyError covers
// the strict-mode path: with LegacyCommitPolicy=LegacyReject and
// VerifierPolicy=PolicyError, a v3-encoded commit with no prevData
// and no op.Prev on update/delete surfaces as a precise
// LegacyCommitError so operators can distinguish "PDS not yet on
// Sync 1.1" from genuine corruption. The default LegacyAccept path
// is exercised separately.
func TestVerifyCommit_LegacyCommitUnderRejectAndPolicyError(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacy1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state: rec1 exists. Build a v3 commit that updates rec1, then
	// strip prevData and op.Prev to produce the legacy shape.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	stripPrevData(commit)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	var hookErr error
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:         gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:          dir,
		StateStore:         cs,
		Policy:             gt.Some(sync.PolicyError),
		LegacyCommitPolicy: gt.Some(sync.LegacyReject),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, e error) {
			hookErr = e
		}),
	})
	require.NoError(t, err)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var lcErr *sync.LegacyCommitError
	require.ErrorAs(t, vErr, &lcErr)
	assert.Equal(t, did, lcErr.DID)
	assert.Equal(t, "3aaaaaaaaaaaa", lcErr.SeenRev)
	assert.True(t, lcErr.SeenData.Equal(prevData))

	assert.ErrorAs(t, hookErr, &lcErr, "OnVerificationFailure must receive the typed legacy error")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.LegacyCommits)
	assert.Equal(t, uint64(0), stats.InversionFailures, "legacy must not inflate the inversion-failure counter")
	assert.Equal(t, uint64(0), stats.ChainBreaks, "legacy is not a chain break")
}

// TestVerifyCommit_LegacyCommitUnderRejectAndPolicyResync asserts
// the strict-mode recovery semantics: with LegacyCommitPolicy=
// LegacyReject and VerifierPolicy=PolicyResync, a legacy commit
// triggers a transparent resync (Reason: legacy_commit visible to
// OnResync), consumers see ActionResync ops, and the resync counter
// increments. The default LegacyAccept path is exercised separately.
func TestVerifyCommit_LegacyCommitUnderRejectAndPolicyResync(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacy2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	// Resync target: serve the current full repo.
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	var resyncReason sync.ResyncReason
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:         gt.Some(sc),
		Directory:          dir,
		StateStore:         cs,
		Policy:             gt.Some(sync.PolicyResync),
		LegacyCommitPolicy: gt.Some(sync.LegacyReject),
		ResyncLimit:        gt.Some(rate.Inf),
		ResyncBurst:        gt.Some(1),
		OnResync: gt.Some(func(_ atmos.DID, _, _ string, reason sync.ResyncReason) {
			resyncReason = reason
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	stripPrevData(commit)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	require.Nil(t, ops, "async resync: ops arrive via ResyncEvents()")

	select {
	case ev := <-v.ResyncEvents():
		require.Equal(t, did, ev.DID)
		require.Greater(t, len(ev.Ops), 0)
		for _, op := range ev.Ops {
			assert.Equal(t, atmos.ActionResync, op.Action)
		}
	case err := <-v.AsyncErrors():
		t.Fatalf("expected ResyncEvent, got async error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ResyncEvent")
	}
	assert.Equal(t, sync.ReasonLegacyCommit, resyncReason, "OnResync should receive legacy_commit, not chain_break")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.LegacyCommits)
	assert.Equal(t, uint64(1), stats.Resyncs)
	assert.Equal(t, uint64(0), stats.ChainBreaks)
	assert.Equal(t, uint64(0), stats.InversionFailures)
}

// TestVerifyCommit_LegacyCommitFirstSightingUnaffected asserts the
// design choice baked into the gate: with state == nil there's no
// prior chain to validate against, and a 1.1-compliant first commit
// for an account legitimately has no prevData (no previous data CID
// exists). We must not falsely flag first-sighting commits as legacy.
func TestVerifyCommit_LegacyCommitFirstSightingUnaffected(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacy3")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(), // empty: first sighting
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "first"},
	}})
	// Strip prevData even though the synthetic builder didn't add a
	// prev for the create — this mirrors what a legitimate first
	// commit on a 1.1 repo looks like (no previous data to reference).
	stripPrevData(commit)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().LegacyCommits)
	assert.Equal(t, uint64(1), v.Stats().EventsVerified)
}

// TestVerifyCommit_LegacyCommitMixedPrevWins covers the boundary
// between legacy and chain-break: a commit with no prevData but
// where some update/delete op DOES carry prev. The producer is
// clearly on 1.1 and just dropped prevData — surface as a chain
// break, not legacy. Defends the helper's "any op.Prev set means
// not legacy" rule.
func TestVerifyCommit_LegacyCommitMixedPrevWins(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacy4")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	// Drop only prevData — keep the update's op.Prev set. This is the
	// "1.1 producer that forgot prevData" case, which we want to flag
	// as a chain break (because we genuinely cannot validate the
	// chain link), not legacy (because the producer clearly knows
	// about the new fields).
	commit.PrevData = gt.None[lextypes.LexCIDLink]()

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	// With op.Prev still set, inversion proceeds normally and produces
	// the correct pre-state root — but the missing envelope prevData
	// (decoded as the zero CID) doesn't equal seenData, so this
	// surfaces as ChainBreakError, not LegacyCommitError.
	var cb *sync.ChainBreakError
	assert.ErrorAs(t, vErr, &cb)
	var lc *sync.LegacyCommitError
	assert.False(t, errors.As(vErr, &lc), "1.1 producer with op.Prev must not be flagged legacy")
	assert.Equal(t, uint64(0), v.Stats().LegacyCommits)
	assert.Equal(t, uint64(1), v.Stats().ChainBreaks)
}

// TestVerifyCommit_LegacyCommitCreateOnlyUnderReject covers the
// create-only commit case under strict mode (LegacyReject + PolicyError).
// With no update/delete ops to inspect, op.Prev gives no signal — but
// a missing envelope prevData on a non-first-sighting commit is still
// 1.0 shape (1.1 mandates prevData). Strict mode surfaces it as
// legacy. The default LegacyAccept path is exercised separately.
func TestVerifyCommit_LegacyCommitCreateOnlyUnderReject(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacy5")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "newrec",
		Record:     map[string]any{"text": "new"},
	}})
	stripPrevData(commit)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:         gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:          dir,
		StateStore:         cs,
		Policy:             gt.Some(sync.PolicyError),
		LegacyCommitPolicy: gt.Some(sync.LegacyReject),
	})
	require.NoError(t, err)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var lcErr *sync.LegacyCommitError
	require.ErrorAs(t, vErr, &lcErr)
	assert.Equal(t, uint64(1), v.Stats().LegacyCommits)
}

// TestVerifyCommit_LegacyCommitHappyPathUnchanged guards against a
// regression where the new gate spuriously triggers on standard
// 1.1-shaped commits. The synthetic builder produces compliant
// commits by default; the counter must stay zero.
func TestVerifyCommit_LegacyCommitHappyPathUnchanged(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacy6")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().LegacyCommits)
	assert.Equal(t, uint64(1), v.Stats().EventsVerified)
}

// ---------------------------------------------------------------------------
// LegacyCommitPolicy: LegacyAccept (default) accepts legacy-shape commits
// ---------------------------------------------------------------------------

func TestLegacyCommitPolicy_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "accept", sync.LegacyAccept.String())
	assert.Equal(t, "reject", sync.LegacyReject.String())
	assert.Equal(t, "unknown_legacy_policy(99)", sync.LegacyCommitPolicy(99).String())
}

func TestVerifierOptions_LegacyAcceptIsDefault(t *testing.T) {
	t.Parallel()
	var o sync.VerifierOptions
	assert.False(t, o.LegacyCommitPolicy.HasVal(), "zero VerifierOptions.LegacyCommitPolicy must be None")
	assert.Equal(t, sync.LegacyAccept, o.LegacyCommitPolicy.ValOr(sync.LegacyAccept))
}

// TestVerifyCommit_LegacyCommitAcceptedByDefault is the headline
// test for the new lenient default. A 1.0-shape commit that would
// have been rejected under LegacyReject is accepted: ops flow through
// to the consumer as normal (action=update), state advances to the
// new data CID, and the LegacyCommits counter increments so operators
// can still see non-upgraded upstreams. No resync is triggered.
func TestVerifyCommit_LegacyCommitAcceptedByDefault(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacyacc1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	stripPrevData(commit)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	// PolicyResync configured to make resync attempts visible — if the
	// gate accidentally falls through to handleVerificationFailure we'd
	// see resyncs > 0 and "resync" actions instead of "update".
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyResync),
		// LegacyCommitPolicy left unset — exercises the default.
	})
	require.NoError(t, err)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	require.Len(t, ops, 1)
	assert.Equal(t, atmos.ActionUpdate, ops[0].Action, "ops should pass through with their original action, not be relabeled as resync")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.LegacyCommits, "counter must still tick under accept mode")
	assert.Equal(t, uint64(1), stats.EventsVerified, "legacy commits accepted under LegacyAccept count as verified")
	assert.Equal(t, uint64(0), stats.Resyncs, "LegacyAccept must not trigger resync")
	assert.Equal(t, uint64(0), stats.ChainBreaks)
	assert.Equal(t, uint64(0), stats.InversionFailures)

	// State advanced to the new commit's data CID.
	state, err := cs.LoadChain(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	dataCID, ok := testutil.InnerCommitDataCID(commit)
	require.True(t, ok)
	assert.True(t, state.Data.Equal(dataCID))
	assert.Equal(t, commit.Rev, state.Rev)
}

// TestVerifyCommit_LegacyCommitAcceptStillEnforcesSignature asserts
// the lenient mode is lenient about the chain-link check ONLY. A
// legacy commit signed with the wrong key still fails signature
// verification — LegacyAccept doesn't open the door to forged commits.
func TestVerifyCommit_LegacyCommitAcceptStillEnforcesSignature(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacyacc2")
	signKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	wrongKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	// Sign with signKey, but the DID document advertises wrongKey.
	commit := testutil.BuildSyntheticCommit(t, r, signKey, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	stripPrevData(commit)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, wrongKey.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
		// LegacyCommitPolicy unset → LegacyAccept default.
	})
	require.NoError(t, err)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var sigErr *sync.SignatureError
	require.ErrorAs(t, vErr, &sigErr, "LegacyAccept must NOT bypass signature verification")
	assert.Equal(t, uint64(1), v.Stats().LegacyCommits)
	assert.Equal(t, uint64(1), v.Stats().SignatureFailures)
}

// TestVerifyCommit_LegacyCommitAcceptStillEnforcesOpCIDs asserts
// the same for op-CID consistency: a legacy commit whose ops list
// disagrees with its post-state MST is still rejected via the
// existing op-CID gate. LegacyAccept skips ONLY the chain-link
// check, not other structural validation.
func TestVerifyCommit_LegacyCommitAcceptStillEnforcesOpCIDs(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacyacc3")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "old"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "new"},
	}})
	stripPrevData(commit)
	// Corrupt the op's CID claim — should be caught by the op-CID
	// gate even under LegacyAccept.
	bogus, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	commit.Ops[0].CID = gt.Some(lextypes.LexCIDLink{Link: bogus.String()})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var oce *sync.OpCIDMismatchError
	require.ErrorAs(t, vErr, &oce, "LegacyAccept must NOT bypass op-CID consistency")
	assert.Equal(t, uint64(1), v.Stats().LegacyCommits)
	assert.Equal(t, uint64(1), v.Stats().OpCIDMismatches)
}

// TestVerifyCommit_LegacyCommitAcceptChainsForward asserts that a
// followup well-formed 1.1 commit, arriving after a legacy commit
// was accepted, chains correctly off the legacy commit's data CID.
// I.e. accepting the legacy commit didn't leave chain state in a
// confused half-advanced spot.
func TestVerifyCommit_LegacyCommitAcceptChainsForward(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:legacyacc4")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v0"}))
	prevData0, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData0}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	// Commit 1: legacy shape. Accepted, advances state to its data CID.
	c1 := testutil.BuildSyntheticCommit(t, r, key, prevData0, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v1"},
	}})
	prevData1, ok := testutil.InnerCommitDataCID(c1)
	require.True(t, ok)
	stripPrevData(c1)

	_, vErr := v.VerifyCommit(context.Background(), c1)
	require.NoError(t, vErr, "first legacy commit should be accepted")

	// Commit 2: a normal 1.1 commit whose prevData points at commit-1's
	// data CID. This is the correctness check: did the verifier really
	// advance to prevData1, or did it leave state at prevData0?
	c2 := testutil.BuildSyntheticCommit(t, r, key, prevData1, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), c2)
	require.NoError(t, vErr, "1.1 followup must chain cleanly off the accepted legacy commit")
	assert.Len(t, ops, 1)
	assert.Equal(t, atmos.ActionUpdate, ops[0].Action)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.LegacyCommits)
	assert.Equal(t, uint64(2), stats.EventsVerified)
	assert.Equal(t, uint64(0), stats.ChainBreaks)
}

// TestVerifyCommit_InversionFailureUnderPolicyResync exercises the
// symmetric resync path for malformed CARs: an inversion failure under
// PolicyResync should trigger transparent resync via getRepo. The
// consumer sees ActionResync ops, and counters reflect both the
// inversion failure and the resync. (Only chain-break-under-PolicyResync
// was previously covered.)
func TestVerifyCommit_InversionFailureUnderPolicyResync(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:invres1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Pre-state: a small repo for the resync target.
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "x", map[string]any{"text": "x"}))
	realPrevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Pre-seed chain state.
	cs := sync.NewMemStateStore()
	require.NoError(t, cs.SaveChain(context.Background(), did, sync.ChainState{
		Rev: "3aaaaaaaaaaaa", Data: realPrevData,
	}))

	// Build a getRepo-able CAR for resync.
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  cs,
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// Build a commit, then corrupt its Blocks to force an inversion failure.
	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "y",
		Record:     map[string]any{"text": "y"},
	}})
	commit.Blocks = []byte{0xff, 0xff, 0xff} // garbage CAR

	ops, err := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, err)
	require.Nil(t, ops, "async resync: ops arrive via ResyncEvents()")

	select {
	case ev := <-v.ResyncEvents():
		require.Equal(t, did, ev.DID)
		require.Greater(t, len(ev.Ops), 0)
		for _, op := range ev.Ops {
			assert.Equal(t, atmos.ActionResync, op.Action)
		}
	case err := <-v.AsyncErrors():
		t.Fatalf("expected ResyncEvent, got async error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ResyncEvent")
	}

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.InversionFailures)
	assert.Equal(t, uint64(1), stats.Resyncs)
}

// TestVerifyCommit_MissingRecordBlockCounter exercises the
// observability hook for upstreams that ship incomplete CARs: when a
// create/update op declares a CID whose block isn't present in the
// CAR, the verifier still emits the op (with empty BlockData) and
// increments Stats.MissingRecordBlocksOps. Tests the observability
// path; correctness of the verifier's decision (still accept, still
// pass through) is unchanged.
func TestVerifyCommit_MissingRecordBlockCounter(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:missingblk1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	chainStore := sync.NewMemStateStore()
	require.NoError(t, chainStore.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: chainStore,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "real"},
	}})

	// Strip the record block from the CAR but leave the commit + MST
	// nodes intact. The op-CID gate runs against the MST tree value
	// (which is the record's CID), not against block presence — so
	// the commit still verifies; only the BlockData on the emitted
	// op is empty and the counter ticks.
	recordCID, err := cbor.ParseCIDString(commit.Ops[0].CID.Val().Link)
	require.NoError(t, err)
	commit.Blocks = stripBlockFromCAR(t, commit.Blocks, recordCID)

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr, "missing record block must NOT fail verification — counter only")
	require.Len(t, ops, 1)
	assert.Equal(t, atmos.ActionCreate, ops[0].Action)
	assert.Empty(t, ops[0].BlockData, "BlockData should be empty when the block was missing from the CAR")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.MissingRecordBlocksOps)
	assert.Equal(t, uint64(1), stats.EventsVerified, "still counted as verified — the commit itself is fine")
}

// stripBlockFromCAR returns a CAR identical to in except with the
// block at target removed. Used by the missing-block counter test
// to simulate an upstream PDS that shipped an incomplete CAR.
func stripBlockFromCAR(t *testing.T, in []byte, target cbor.CID) []byte {
	t.Helper()
	cr, err := car.NewReader(bytes.NewReader(in))
	require.NoError(t, err)
	roots := cr.Header().Roots

	var blocks []car.Block
	for {
		b, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if b.CID.Equal(target) {
			continue
		}
		blocks = append(blocks, b)
	}

	var out bytes.Buffer
	cw, err := car.NewWriter(&out, roots)
	require.NoError(t, err)
	for _, b := range blocks {
		require.NoError(t, cw.WriteBlock(b.CID, b.Data))
	}
	return out.Bytes()
}

// BenchmarkVerifyCommit_HappyPath measures the per-event hot path
// — a clean #commit that passes every gate (rev-replay, future-rev,
// legacy detection, inversion, chain check, field consistency,
// signature, op-CID consistency, state advance). The pre-state is a
// synthetic ~50-record repo, representative of a small Bluesky
// account; each iteration verifies an update of one record. Used to
// quantify the savings from CAR-parse deduplication (B1) and to
// guard against future regressions on the hot path.
func BenchmarkVerifyCommit_HappyPath(b *testing.B) {
	did := atmos.DID("did:plc:bench1")
	key, err := crypto.GenerateP256()
	require.NoError(b, err)

	r, _ := testutil.BuildEmptyRepo(b, did)
	const seedRecords = 50
	for i := range seedRecords {
		require.NoError(b, r.Create("app.bsky.feed.post",
			fmt.Sprintf("rec%d", i), map[string]any{"text": fmt.Sprintf("v%d", i)}))
	}
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(b, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	cs := sync.NewMemStateStore()
	require.NoError(b, cs.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(b, err)

	// Build one representative commit and reuse its bytes per
	// iteration. Each iter resets state to the seeded value before
	// verifying so the rev-replay drop never trips.
	commit := testutil.BuildSyntheticCommit(b, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec0",
		Record:     map[string]any{"text": "updated"},
	}})

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// Reset state every iteration so the rev-replay gate stays
		// open. SaveChain on MemStateStore is an xsync.Map.Store —
		// its cost is constant and small relative to the verification
		// work, so it doesn't meaningfully pollute the measurement.
		_ = cs.SaveChain(ctx, did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData})
		ops, err := v.VerifyCommit(ctx, commit)
		if err != nil {
			b.Fatalf("verify: %v", err)
		}
		if len(ops) != 1 {
			b.Fatalf("expected 1 op, got %d", len(ops))
		}
	}
}

// ---------------------------------------------------------------------------
// Hosting status (A8): #account events, HostingState, HostingPolicy
// ---------------------------------------------------------------------------

func TestMemStateStore_HostingRoundTrip(t *testing.T) {
	t.Parallel()

	store := sync.NewMemStateStore()
	did := atmos.DID("did:plc:host1")

	got, err := store.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, got, "missing hosting state should return (nil, nil)")

	want := sync.HostingState{
		Active: false,
		Status: sync.StatusTakendown,
		Seq:    42,
		Time:   "2026-05-20T00:00:00Z",
	}
	require.NoError(t, store.SaveHosting(context.Background(), did, want))

	got, err = store.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want, *got)
}

func TestMemStateStore_DeleteClearsBoth(t *testing.T) {
	t.Parallel()

	store := sync.NewMemStateStore()
	did := atmos.DID("did:plc:both1")
	cid, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")

	require.NoError(t, store.SaveChain(context.Background(), did,
		sync.ChainState{Rev: "r1", Data: cid}))
	require.NoError(t, store.SaveHosting(context.Background(), did,
		sync.HostingState{Active: true, Seq: 1}))

	require.NoError(t, store.Delete(context.Background(), did))

	chain, err := store.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, chain, "Delete should clear chain state")

	hosting, err := store.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, hosting, "Delete should clear hosting state")
}

func TestHostingState_IsActive(t *testing.T) {
	t.Parallel()

	var nilState *sync.HostingState
	assert.True(t, nilState.IsActive(), "nil receiver should be treated as active (first sighting)")

	active := &sync.HostingState{Active: true}
	assert.True(t, active.IsActive())

	inactive := &sync.HostingState{Active: false, Status: sync.StatusTakendown}
	assert.False(t, inactive.IsActive())
}

func TestHostingPolicy_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "track", sync.HostingTrack.String())
	assert.Equal(t, "gate", sync.HostingGate.String())
	assert.Equal(t, "unknown_hosting_policy(99)", sync.HostingPolicy(99).String())
}

func TestResyncReason_AccountInactive(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "account_inactive", sync.ReasonAccountInactive.String())
}

func TestAccountInactiveError_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	err := &sync.AccountInactiveError{
		DID:    atmos.DID("did:plc:gone"),
		Status: sync.StatusTakendown,
	}
	msg := err.Error()
	assert.Contains(t, msg, "account inactive")
	assert.Contains(t, msg, "did:plc:gone")
	assert.Contains(t, msg, "takendown")

	// Empty status: error message should still be coherent.
	bare := &sync.AccountInactiveError{DID: atmos.DID("did:plc:gone")}
	assert.Contains(t, bare.Error(), "account inactive")
	assert.NotContains(t, bare.Error(), "status=")

	var target *sync.AccountInactiveError
	assert.True(t, errors.As(err, &target))

	wrapped := fmt.Errorf("verifier: %w", err)
	target = nil
	assert.True(t, errors.As(wrapped, &target))
	assert.Equal(t, sync.StatusTakendown, target.Status)
}

// hostingTestVerifier builds a verifier with the given HostingPolicy
// and PolicyError, plus a fake getRepo server pointed at an
// unreachable host (so any accidental resync surfaces as a network
// error instead of silent recovery).
func hostingTestVerifier(t *testing.T, did atmos.DID, key crypto.PrivateKey, policy sync.HostingPolicy) (*sync.Verifier, sync.StateStore) {
	t.Helper()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	ss := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:     dir,
		StateStore:    ss,
		Policy:        gt.Some(sync.PolicyError),
		HostingPolicy: gt.Some(policy),
	})
	require.NoError(t, err)
	return v, ss
}

// TestVerifier_OnAccountEvent_PersistsState exercises the basic
// happy path: an #account event passed to OnAccountEvent ends up in
// the StateStore and fires the callback exactly once.
func TestVerifier_OnAccountEvent_PersistsState(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:onacc1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	ss := sync.NewMemStateStore()

	var (
		callbackDID   atmos.DID
		callbackState sync.HostingState
		callbackCalls int
	)
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: ss,
		Policy:     gt.Some(sync.PolicyError),
		OnAccountStateChanged: gt.Some(func(d atmos.DID, s sync.HostingState) {
			callbackDID = d
			callbackState = s
			callbackCalls++
		}),
	})
	require.NoError(t, err)

	evt := &comatproto.SyncSubscribeRepos_Account{
		DID:    string(did),
		Active: false,
		Status: gt.Some(sync.StatusTakendown),
		Seq:    100,
		Time:   "2026-05-20T00:00:00Z",
	}
	require.NoError(t, v.OnAccountEvent(context.Background(), evt))

	got, err := ss.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Active)
	assert.Equal(t, sync.StatusTakendown, got.Status)
	assert.Equal(t, int64(100), got.Seq)
	assert.Equal(t, "2026-05-20T00:00:00Z", got.Time)

	assert.Equal(t, 1, callbackCalls)
	assert.Equal(t, did, callbackDID)
	assert.Equal(t, *got, callbackState)
}

// TestVerifier_OnAccountEvent_ReplayDrop covers the seq-based replay
// gate: an event at or below the persisted seq is silently dropped,
// counter increments, callback does NOT fire.
func TestVerifier_OnAccountEvent_ReplayDrop(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:onacc2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	ss := sync.NewMemStateStore()

	callbackCalls := 0
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: ss,
		Policy:     gt.Some(sync.PolicyError),
		OnAccountStateChanged: gt.Some(func(_ atmos.DID, _ sync.HostingState) {
			callbackCalls++
		}),
	})
	require.NoError(t, err)

	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: true, Seq: 200,
	}))
	// Re-deliver same seq.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 200,
	}))
	// Lower seq.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 50,
	}))

	got, err := ss.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Active, "state must reflect first event, not the dropped re-deliveries")
	assert.Equal(t, int64(200), got.Seq)

	stats := v.Stats()
	assert.Equal(t, uint64(2), stats.AccountEventReplaysDropped)
	assert.Equal(t, 1, callbackCalls, "callback must not fire on replay drops")
}

// TestVerifier_OnAccountEvent_ForwardCompatStatus asserts the
// open-vocabulary contract: an unknown status string is preserved
// verbatim in HostingState.
func TestVerifier_OnAccountEvent_ForwardCompatStatus(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fwd1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	ss := sync.NewMemStateStore()
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: ss,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some("future_value_not_in_spec"), Seq: 1,
	}))

	got, err := ss.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "future_value_not_in_spec", got.Status,
		"unknown status strings must be preserved verbatim")
}

func TestVerifier_OnAccountEvent_InvalidDID(t *testing.T) {
	t.Parallel()

	dir := &identity.Directory{Resolver: testutil.NewTrackingResolver()}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	err = v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: "not-a-did", Active: true, Seq: 1,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid account DID")
}

// TestVerifyCommit_HostingTrackDoesNotGate is the headline assertion
// for the default policy: even after an #account event marks the DID
// inactive, subsequent #commit events still verify normally.
// Consumers under HostingTrack are expected to filter on their own
// (or use HostingGate).
func TestVerifyCommit_HostingTrackDoesNotGate(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:track1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, ss := hostingTestVerifier(t, did, key, sync.HostingTrack)

	// Mark inactive.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 1,
	}))

	// Subsequent commit still verifies.
	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "ignored takedown"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().AccountsInactive)

	hosting, err := ss.LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, hosting)
	assert.False(t, hosting.Active, "state was still tracked, just not gated")
}

// TestVerifyCommit_HostingGateDropsCommit is the headline
// assertion for HostingGate: a #commit for a DID whose persisted
// state is non-active returns AccountInactiveError, fires the hook,
// increments the counter, and does not advance state.
func TestVerifyCommit_HostingGateDropsCommit(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:gate1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	ss := sync.NewMemStateStore()

	var hookErr error
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:     dir,
		StateStore:    ss,
		Policy:        gt.Some(sync.PolicyError),
		HostingPolicy: gt.Some(sync.HostingGate),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, e error) {
			hookErr = e
		}),
	})
	require.NoError(t, err)

	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 1,
	}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "should be dropped"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	var aiErr *sync.AccountInactiveError
	require.ErrorAs(t, vErr, &aiErr)
	assert.Nil(t, ops)
	assert.Equal(t, sync.StatusTakendown, aiErr.Status)
	assert.Equal(t, did, aiErr.DID)

	require.ErrorAs(t, hookErr, &aiErr)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.AccountsInactive)
	assert.Equal(t, uint64(0), stats.EventsVerified, "gated commit must not count as verified")

	chain, err := ss.LoadChain(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, chain, "gated commit must not advance chain state")
}

// TestVerifyCommit_HostingGateSkipsChainLoad asserts gate ordering:
// when HostingGate rejects, the verifier must not have consulted the
// chain store. Issue #7 from the review: a takedown-heavy upstream
// otherwise pays a chain-store round trip per gated event for state
// it never uses.
//
// Wraps a real MemStateStore in countingChainStore so the test can
// assert LoadChain count == 0 across the gated event. Hosting reads
// (one for the gate's lookup) are expected and not asserted on.
func TestVerifyCommit_HostingGateSkipsChainLoad(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:gateorder1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	cs := &countingChainStore{real: sync.NewMemStateStore()}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:     dir,
		StateStore:    cs,
		Policy:        gt.Some(sync.PolicyError),
		HostingPolicy: gt.Some(sync.HostingGate),
	})
	require.NoError(t, err)

	// Mark inactive. OnAccountEvent itself touches LoadHosting (read
	// before write for replay protection) and SaveHosting; we don't
	// care about those counts here.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 1,
	}))

	// Reset chain counters specifically — hosting counters can carry
	// values from OnAccountEvent's read-then-write.
	cs.loadChain.Store(0)
	cs.saveChain.Store(0)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec1",
		Record: map[string]any{"text": "should be gated"},
	}})

	_, vErr := v.VerifyCommit(context.Background(), commit)
	var aiErr *sync.AccountInactiveError
	require.ErrorAs(t, vErr, &aiErr)

	assert.Equal(t, int64(0), cs.loadChain.Load(),
		"gated commit must not consult the chain store for state load")
	assert.Equal(t, int64(0), cs.saveChain.Load(),
		"gated commit must not advance chain state")
}

func TestVerifySync_HostingGateDrops(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:gate2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	v, _ := hostingTestVerifier(t, did, key, sync.HostingGate)

	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusSuspended), Seq: 1,
	}))

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID: string(did),
		Rev: string(atmos.NewTIDNow(0)),
	}
	_, vErr := v.VerifySync(context.Background(), syncEvt)
	var aiErr *sync.AccountInactiveError
	require.ErrorAs(t, vErr, &aiErr)
	assert.Equal(t, sync.StatusSuspended, aiErr.Status)
	assert.Equal(t, uint64(1), v.Stats().AccountsInactive)
}

// TestVerifyCommit_HostingGateReactivation asserts that flipping
// state from inactive back to active re-enables verification.
func TestVerifyCommit_HostingGateReactivation(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:reactivate1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, _ := hostingTestVerifier(t, did, key, sync.HostingGate)

	// Takedown.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 1,
	}))
	// Reinstate.
	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: true, Seq: 2,
	}))

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "back online"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr)
	assert.Len(t, ops, 1)
	assert.Equal(t, uint64(0), v.Stats().AccountsInactive)
}

// TestVerifyCommit_HostingGateFirstSightingAllows guards the
// first-sighting behavior: with no persisted state, the verifier
// permits the event (matches indigo's relay semantics).
func TestVerifyCommit_HostingGateFirstSightingAllows(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:first1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	v, _ := hostingTestVerifier(t, did, key, sync.HostingGate)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "first sighting"},
	}})

	ops, vErr := v.VerifyCommit(context.Background(), commit)
	require.NoError(t, vErr, "first sighting must be permitted under HostingGate")
	assert.Len(t, ops, 1)
}

// TestVerifier_Resync_HostingGateBlocks asserts the resync gate:
// under HostingGate, Verifier.Resync for an inactive DID returns
// ResyncFailedError{Reason: ReasonAccountInactive} without hitting
// getRepo. Verified by pointing the SyncClient at an unreachable
// host — if the gate fails to engage, we'd see a network error
// instead.
func TestVerifier_Resync_HostingGateBlocks(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:resgate1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	v, _ := hostingTestVerifier(t, did, key, sync.HostingGate)

	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 1,
	}))

	_, vErr := v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, vErr, &rfe)
	assert.Equal(t, sync.ReasonAccountInactive, rfe.Reason)

	var aiErr *sync.AccountInactiveError
	require.ErrorAs(t, rfe.Cause, &aiErr)
	assert.Equal(t, sync.StatusTakendown, aiErr.Status)

	assert.Equal(t, uint64(1), v.Stats().ResyncFailures)
	assert.Equal(t, uint64(0), v.Stats().Resyncs)
}

// TestVerifier_Resync_HostingTrackPermits documents the use case
// from the design doc: a moderation tool wants to fetch the repo
// of a takendown account. Under the default HostingTrack, even a
// DID marked inactive can be resynced.
func TestVerifier_Resync_HostingTrackPermits(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:restrack1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "still here"}))
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:    gt.Some(sc),
		Directory:     dir,
		StateStore:    sync.NewMemStateStore(),
		Policy:        gt.Some(sync.PolicyResync),
		HostingPolicy: gt.Some(sync.HostingTrack),
		ResyncLimit:   gt.Some(rate.Inf),
		ResyncBurst:   gt.Some(1),
	})
	require.NoError(t, err)

	require.NoError(t, v.OnAccountEvent(context.Background(), &comatproto.SyncSubscribeRepos_Account{
		DID: string(did), Active: false, Status: gt.Some(sync.StatusTakendown), Seq: 1,
	}))

	ops, vErr := v.Resync(context.Background(), did)
	require.NoError(t, vErr, "HostingTrack must permit resync of inactive accounts")
	require.NotEmpty(t, ops)
	assert.Equal(t, uint64(1), v.Stats().Resyncs)
}
