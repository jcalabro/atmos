package sync_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mathrand "math/rand"
	stdsync "sync"
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
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestMemChainStore_LoadMissingReturnsNilNil(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	state, err := store.Load(context.Background(), atmos.DID("did:plc:abc"))
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemChainStore_SaveThenLoad(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	want := sync.ChainState{Rev: "3l3qo2vutsw2b", Data: cid}
	require.NoError(t, store.Save(context.Background(), did, want))

	got, err := store.Load(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.Rev, got.Rev)
	assert.True(t, got.Data.Equal(want.Data))
}

func TestMemChainStore_Delete(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	require.NoError(t, store.Save(context.Background(), did, sync.ChainState{Rev: "r1", Data: cid}))
	require.NoError(t, store.Delete(context.Background(), did))

	state, err := store.Load(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemChainStore_DeleteMissingNoError(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	require.NoError(t, store.Delete(context.Background(), atmos.DID("did:plc:never-saved")))
}

func TestErrorTypes_FormatAndUnwrap(t *testing.T) {
	t.Parallel()

	t.Run("ChainBreakError", func(t *testing.T) {
		cid, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
		cause := errors.New("cause goes here")
		err := &sync.ChainBreakError{
			DID:          atmos.DID("did:plc:abc"),
			SeenRev:      "r1",
			SeenData:     cid,
			GotRev:       "r2",
			GotPrevData:  cid,
			InvertedData: cid,
			Cause:        cause,
		}
		assert.Contains(t, err.Error(), "chain break")
		assert.Contains(t, err.Error(), "did:plc:abc")
		assert.ErrorIs(t, err, cause)

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
	assert.Equal(t, sync.PolicyResync, o.Policy)
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

	t.Run("missing ChainStore", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: sc,
			Directory:  dir,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ChainStore")
	})

	t.Run("missing Directory", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: sc,
			ChainStore: sync.NewMemChainStore(),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Directory")
	})

	t.Run("PolicyResync requires SyncClient", func(t *testing.T) {
		_, err := sync.NewVerifier(sync.VerifierOptions{
			Directory:  dir,
			ChainStore: sync.NewMemChainStore(),
			Policy:     sync.PolicyResync,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SyncClient")
	})

	t.Run("PolicyError works without SyncClient", func(t *testing.T) {
		v, err := sync.NewVerifier(sync.VerifierOptions{
			Directory:  dir,
			ChainStore: sync.NewMemChainStore(),
			Policy:     sync.PolicyError,
		})
		require.NoError(t, err)
		assert.NotNil(t, v)
	})

	t.Run("happy path with all required", func(t *testing.T) {
		v, err := sync.NewVerifier(sync.VerifierOptions{
			SyncClient: sc,
			Directory:  dir,
			ChainStore: sync.NewMemChainStore(),
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
		SyncClient: sc,
		Directory:  dir,
		ChainStore: sync.NewMemChainStore(),
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
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		ResyncLimit: 0,
		ResyncBurst: 0,
	}
	_, err := sync.NewVerifier(opts)
	require.NoError(t, err)
	// NewVerifier defaults ResyncLimit and ResyncBurst internally; the
	// caller's struct must not be mutated.
	assert.Equal(t, rate.Limit(0), opts.ResyncLimit)
	assert.Equal(t, 0, opts.ResyncBurst)
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
		ChainStore: sync.NewMemChainStore(),
		Policy:     sync.PolicyError,
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
		ChainStore: sync.NewMemChainStore(),
		Policy:     sync.PolicyError,
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
		ChainStore: sync.NewMemChainStore(),
		Policy:     sync.PolicyError,
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
		ChainStore: sync.NewMemChainStore(),
		Policy:     sync.PolicyError,
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
		ChainStore: sync.NewMemChainStore(),
		Policy:     sync.PolicyError,
		// Effectively no refill during the test (one token per ~17 minutes).
		// Keeps the burst-then-deny assertion bulletproof under any CI scheduling
		// delay; the test never waits, just calls Allow() four times.
		ResyncLimit: rate.Limit(0.001),
		ResyncBurst: 2,
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
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
		OnResync: func(d atmos.DID, oldRev, newRev string, reason sync.ResyncReason) {
			resyncDID = d
			resyncOldRev = oldRev
			resyncNewRev = newRev
			resyncReason = reason
		},
	})
	require.NoError(t, err)

	ops, err := v.Resync(context.Background(), did)
	require.NoError(t, err)
	require.Len(t, ops, 3)
	for _, op := range ops {
		assert.Equal(t, "resync", op.Action)
		assert.Equal(t, string(did), op.Repo)
	}
	assert.Equal(t, did, resyncDID)
	assert.Equal(t, sync.ReasonSyncEvent, resyncReason)
	assert.Empty(t, resyncOldRev, "first resync should report empty oldRev (no prior chain state)")
	state, err := v.ChainStore().Load(context.Background(), did)
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
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Limit(0.001), // effectively no refill during test
		ResyncBurst: 1,
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
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
	})
	require.NoError(t, err)

	_, err = v.Resync(context.Background(), did)
	var rfe *sync.ResyncFailedError
	require.ErrorAs(t, err, &rfe)
}

type failingChainStore struct {
	real sync.ChainStore
}

func (s *failingChainStore) Load(ctx context.Context, did atmos.DID) (*sync.ChainState, error) {
	return s.real.Load(ctx, did)
}
func (s *failingChainStore) Save(_ context.Context, _ atmos.DID, _ sync.ChainState) error {
	return errors.New("disk full")
}
func (s *failingChainStore) Delete(ctx context.Context, did atmos.DID) error {
	return s.real.Delete(ctx, did)
}

// ---------------------------------------------------------------------------
// VerifyAndExpand (#commit)
// ---------------------------------------------------------------------------

func TestVerifyAndExpand_HappyPath(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:happy1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	chainStore := sync.NewMemChainStore()
	require.NoError(t, chainStore.Save(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:   dir,
		ChainStore:  chainStore,
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec2",
		Record:     map[string]any{"text": "v2"},
	}})

	ops, err := v.VerifyAndExpand(context.Background(), commit, nil)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, "create", ops[0].Action)
	assert.Equal(t, "app.bsky.feed.post", ops[0].Collection)
	assert.Equal(t, "rec2", ops[0].RKey)

	state, err := chainStore.Load(context.Background(), did)
	require.NoError(t, err)
	assert.Equal(t, commit.Rev, state.Rev)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.EventsVerified)
}

// TestVerifyAndExpand_EmptyOpsCommit guards the streaming integration's
// ability to distinguish a successful zero-ops verification from a
// rev-replay drop. The verifier must return a non-nil empty slice on
// success here, not (nil, nil).
func TestVerifyAndExpand_EmptyOpsCommit(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:emptyops1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v1"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	chainStore := sync.NewMemChainStore()
	require.NoError(t, chainStore.Save(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:   dir,
		ChainStore:  chainStore,
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
	})
	require.NoError(t, err)

	// Zero-ops commit at a higher rev than the persisted state.
	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, nil)

	ops, err := v.VerifyAndExpand(context.Background(), commit, nil)
	require.NoError(t, err)
	require.NotNil(t, ops, "empty-ops verification must return a non-nil empty slice (rev-replay returns nil)")
	assert.Empty(t, ops)

	// State must have advanced to the new commit.
	state, err := chainStore.Load(context.Background(), did)
	require.NoError(t, err)
	assert.Equal(t, commit.Rev, state.Rev)

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.EventsVerified)
	assert.Equal(t, uint64(0), stats.RevReplaysDropped)
}

func TestVerifyAndExpand_RevReplay(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:replay1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "rec1", map[string]any{"text": "v"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemChainStore()
	highRev := "3zzzzzzzzzzzz"
	require.NoError(t, cs.Save(context.Background(), did,
		sync.ChainState{Rev: highRev, Data: prevData}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:  dir,
		ChainStore: cs,
		Policy:     sync.PolicyError,
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "replay"},
	}})
	commit.Rev = "3aaaaaaaaaaaa" // low rev

	ops, err := v.VerifyAndExpand(context.Background(), commit, nil)
	require.NoError(t, err)
	assert.Nil(t, ops, "rev replay should drop silently")
	assert.Equal(t, uint64(1), v.Stats().RevReplaysDropped)
}

func TestVerifyAndExpand_ChainBreakUnderPolicyError(t *testing.T) {
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
	cs := sync.NewMemChainStore()
	require.NoError(t, cs.Save(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	var failureCalled bool
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:  dir,
		ChainStore: cs,
		Policy:     sync.PolicyError,
		OnVerificationFailure: func(_ atmos.DID, _ error) {
			failureCalled = true
		},
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})

	_, err = v.VerifyAndExpand(context.Background(), commit, nil)
	var cb *sync.ChainBreakError
	require.ErrorAs(t, err, &cb)
	assert.True(t, failureCalled)
	assert.Equal(t, uint64(1), v.Stats().ChainBreaks)
}

// TestVerifyAndExpand_PolicyErrorSaveFailureCountedInStats verifies
// that a ChainStore.Save failure during PolicyError state-advance is
// counted in VerifierStats.ChainStateSaveFailures rather than silently
// swallowed. The original ChainBreakError still surfaces (the typed
// signal is the primary report); the counter exists so operators can
// detect that the secondary save failed.
func TestVerifyAndExpand_PolicyErrorSaveFailureCountedInStats(t *testing.T) {
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
	realStore := sync.NewMemChainStore()
	require.NoError(t, realStore.Save(context.Background(), did,
		sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))
	cs := &failingChainStore{real: realStore}

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:  dir,
		ChainStore: cs,
		Policy:     sync.PolicyError,
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "rec1",
		Record:     map[string]any{"text": "v2"},
	}})

	_, err = v.VerifyAndExpand(context.Background(), commit, nil)
	var cb *sync.ChainBreakError
	require.ErrorAs(t, err, &cb,
		"primary verification error must still surface as ChainBreakError")

	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ChainStateSaveFailures,
		"PolicyError save failure should be counted")
	assert.Equal(t, uint64(1), stats.ChainBreaks)
}

func TestVerifyAndExpand_FirstSightingNoBreak(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:fresh1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	cs := sync.NewMemChainStore()
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:  dir,
		ChainStore: cs,
		Policy:     sync.PolicyError,
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action:     testutil.ActionCreate,
		Collection: "app.bsky.feed.post",
		RKey:       "first",
		Record:     map[string]any{"text": "first"},
	}})

	ops, err := v.VerifyAndExpand(context.Background(), commit, nil)
	require.NoError(t, err)
	assert.Len(t, ops, 1)
	state, err := cs.Load(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, commit.Rev, state.Rev)
}

func TestVerifyAndExpand_ChainBreakUnderPolicyResync(t *testing.T) {
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
	cs := sync.NewMemChainStore()
	require.NoError(t, cs.Save(context.Background(), did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: otherCID}))

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))
	xc := testutil.NewFakeSyncServer(t, did, carBuf.Bytes())

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  cs,
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
	})
	require.NoError(t, err)

	commit := testutil.BuildSyntheticCommit(t, r, key, realPrevData, []testutil.OpAction{{
		Action:     testutil.ActionUpdate,
		Collection: "app.bsky.feed.post",
		RKey:       "x",
		Record:     map[string]any{"text": "y"},
	}})

	ops, err := v.VerifyAndExpand(context.Background(), commit, nil)
	require.NoError(t, err)
	require.Greater(t, len(ops), 0)
	for _, op := range ops {
		assert.Equal(t, "resync", op.Action)
	}
	stats := v.Stats()
	assert.Equal(t, uint64(1), stats.ChainBreaks)
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
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  &failingChainStore{real: sync.NewMemChainStore()},
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
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

func TestVerifyAndExpand_SyncEvent(t *testing.T) {
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
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
	})
	require.NoError(t, err)

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID:  string(did),
		Rev:  "3newrev",
		Time: "2026-05-19T00:00:00Z",
	}
	ops, err := v.VerifyAndExpand(context.Background(), nil, syncEvt)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	for _, op := range ops {
		assert.Equal(t, "resync", op.Action)
	}
}

func TestVerifyAndExpand_SyncEvent_RevReplay(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:syncrep1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	someCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	cs := sync.NewMemChainStore()
	require.NoError(t, cs.Save(context.Background(), did,
		sync.ChainState{Rev: "3zzzzzzzzzzzz", Data: someCID}))

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:  dir,
		ChainStore: cs,
		Policy:     sync.PolicyResync,
	})
	require.NoError(t, err)

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID: string(did),
		Rev: "3aaaaaaaaaaaa", // older
	}
	ops, err := v.VerifyAndExpand(context.Background(), nil, syncEvt)
	require.NoError(t, err)
	assert.Nil(t, ops)
	assert.Equal(t, uint64(1), v.Stats().RevReplaysDropped)
}
