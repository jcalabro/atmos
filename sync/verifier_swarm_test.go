//go:build !js && !wasip1

package sync_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mathrand "math/rand"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// TestVerifierSwarm drives the Verifier with random fault injection
// across multiple DIDs in parallel. After each iteration we assert the
// counter invariant:
//
//	emitted == accepted + dropped + chainBreaks + inversionErrs
//
// and confirm each Stats() field matches the locally-tracked count.
//
// The original Sync 1.1 plan called for 1000/100k iterations, but
// atmos has no test-long recipe: we target 50 iters under -short and
// 500 otherwise. Each iteration spawns 4 DIDs, seeds them, and runs
// 50 random fault events — 500 * 4 * (1+50) = 102,000 events on the
// default `just test` invocation, completing in well under a minute
// on a developer laptop.
func TestVerifierSwarm(t *testing.T) {
	t.Parallel()

	iters := 500
	if testing.Short() {
		iters = 50
	}
	for i := 0; i < iters; i++ {
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			t.Parallel()
			runOneSwarmIteration(t, int64(i)+1)
		})
	}
}

// runOneSwarmIteration executes one self-contained swarm iteration with
// its own Verifier, its own ChainStore, and its own set of DIDs. There
// is no shared state across iterations.
func runOneSwarmIteration(t *testing.T, seed int64) {
	t.Helper()
	rng := mathrand.New(mathrand.NewSource(seed))

	const numDIDs = 4
	dids := make([]atmos.DID, numDIDs)
	keys := make([]crypto.PrivateKey, numDIDs)
	repos := make([]*repo.Repo, numDIDs)
	for i := 0; i < numDIDs; i++ {
		dids[i] = atmos.DID(fmt.Sprintf("did:plc:swarm%d-%d", seed, i))
		k, err := crypto.GenerateP256()
		require.NoError(t, err)
		keys[i] = k
		repos[i], _ = testutil.BuildEmptyRepo(t, dids[i])
	}

	cs := sync.NewMemChainStore()
	resolver := testutil.NewTrackingResolver()
	for i := 0; i < numDIDs; i++ {
		resolver.Docs[dids[i]] = testutil.BuildDIDDoc(dids[i], keys[i].PublicKey())
	}
	dir := &identity.Directory{Resolver: resolver}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}}),
		Directory:  dir,
		ChainStore: cs,
		Policy:     sync.PolicyError,
	})
	require.NoError(t, err)

	// lastGood[i] is the data CID of the last commit the verifier
	// accepted for dids[i] — i.e., what the verifier's ChainStore
	// believes for that DID. Equal to repos[i].Tree.RootCID() at all
	// times outside of a single event's processing window.
	lastGood := make([]cbor.CID, numDIDs)
	for i := 0; i < numDIDs; i++ {
		root, err := repos[i].Tree.WriteBlocks(repos[i].Store)
		require.NoError(t, err)
		lastGood[i] = root
	}

	// Counters tracked locally by mirroring the verifier's decisions.
	var (
		emitted       int
		accepted      int
		dropped       int
		chainBreaks   int
		inversionErrs int
	)

	// emit runs one synthetic commit through the verifier, classifying
	// the outcome and reconciling local state with the verifier's.
	emit := func(t *testing.T, didIdx int, commit *comatproto.SyncSubscribeRepos_Commit, expectClean bool) {
		t.Helper()
		emitted++
		ops, vErr := v.VerifyAndExpand(context.Background(), commit, nil)
		switch {
		case vErr == nil && ops == nil:
			// Silent rev-replay drop. State unchanged. Reset r.Tree
			// since BuildSyntheticCommit mutated it but the verifier
			// rejected the result.
			dropped++
			repos[didIdx].Tree = mst.LoadTree(repos[didIdx].Store, lastGood[didIdx])
		case vErr == nil:
			accepted++
			// Verifier saved state to the commit's data CID. Pull it
			// back out of the CAR (the verifier already proved the
			// CAR is well-formed by accepting the commit).
			dataCID := extractDataCID(t, commit)
			lastGood[didIdx] = dataCID
		default:
			var cb *sync.ChainBreakError
			var ie *sync.InversionError
			switch {
			case errors.As(vErr, &cb):
				chainBreaks++
				// PolicyError advances state on chain break (see
				// handleVerificationFailure) when the commit's data
				// CID is decodable, which it is for this fault path
				// — only the prevData claim was tampered with. Mirror
				// that locally.
				lastGood[didIdx] = extractDataCID(t, commit)
			case errors.As(vErr, &ie):
				inversionErrs++
				// On inversion failure the malformed CAR usually
				// prevents data-CID extraction, so the verifier
				// leaves state untouched. Reset r.Tree to lastGood
				// to undo BuildSyntheticCommit's mutation.
				if dataCID := tryExtractDataCID(commit); dataCID.Defined() {
					lastGood[didIdx] = dataCID
				} else {
					repos[didIdx].Tree = mst.LoadTree(repos[didIdx].Store, lastGood[didIdx])
				}
			default:
				t.Fatalf("seed=%d unexpected error type: %T %v", seed, vErr, vErr)
			}
		}
		// Sanity: if we expected a clean accept, complain loudly. This
		// catches drift in the test harness itself.
		if expectClean && (vErr != nil || ops == nil) {
			t.Fatalf("seed=%d expected clean accept, got ops=%v err=%v", seed, ops, vErr)
		}
	}

	// Step 1: seed each DID with one clean commit so that the
	// rev-replay and chain-break gates have meaningful state to
	// compare against. Without this, the very first fault for a DID
	// would be accepted as first-sighting and the counter
	// invariants wouldn't hold.
	for i := 0; i < numDIDs; i++ {
		commit := testutil.BuildSyntheticCommit(t, repos[i], keys[i], lastGood[i], []testutil.OpAction{{
			Action:     testutil.ActionCreate,
			Collection: "app.bsky.feed.post",
			RKey:       fmt.Sprintf("seed%d", i),
			Record:     map[string]any{"text": "seed"},
		}})
		emit(t, i, commit, true)
	}

	// Step 2: 50 events of random fault injection.
	const numEvents = 50
	for k := 0; k < numEvents; k++ {
		didIdx := rng.Intn(numDIDs)
		fault := rng.Intn(10)

		var commit *comatproto.SyncSubscribeRepos_Commit
		switch {
		case fault < 7: // 70% clean
			commit = testutil.BuildSyntheticCommit(t, repos[didIdx], keys[didIdx], lastGood[didIdx], []testutil.OpAction{{
				Action:     testutil.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       fmt.Sprintf("ev%d-%d", k, didIdx),
				Record:     map[string]any{"text": "x"},
			}})
		case fault == 7: // rev-replay
			commit = testutil.BuildSyntheticCommit(t, repos[didIdx], keys[didIdx], lastGood[didIdx], []testutil.OpAction{{
				Action:     testutil.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       fmt.Sprintf("dup%d-%d", k, didIdx),
				Record:     map[string]any{"text": "y"},
			}})
			// Override to a rev that's strictly less than any TID the
			// clock has produced (TIDs start with '3'; '2' sorts below).
			commit.Rev = "2222222222222"
		case fault == 8: // chain break (bogus prevData claim)
			commit = testutil.BuildSyntheticCommit(t, repos[didIdx], keys[didIdx], lastGood[didIdx], []testutil.OpAction{{
				Action:     testutil.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       fmt.Sprintf("brk%d-%d", k, didIdx),
				Record:     map[string]any{"text": "z"},
			}})
			bogusCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
			require.NoError(t, err)
			commit.PrevData = gt.Some(lextypes.LexCIDLink{Link: bogusCID.String()})
		case fault == 9: // malformed CAR
			commit = testutil.BuildSyntheticCommit(t, repos[didIdx], keys[didIdx], lastGood[didIdx], []testutil.OpAction{{
				Action:     testutil.ActionCreate,
				Collection: "app.bsky.feed.post",
				RKey:       fmt.Sprintf("bad%d-%d", k, didIdx),
				Record:     map[string]any{"text": "w"},
			}})
			commit.Blocks = []byte{0xff, 0xff, 0xff}
		}

		emit(t, didIdx, commit, false)
	}

	stats := v.Stats()
	require.Equal(t, emitted, accepted+dropped+chainBreaks+inversionErrs,
		"seed=%d emit=%d acc=%d drop=%d cb=%d ie=%d",
		seed, emitted, accepted, dropped, chainBreaks, inversionErrs)
	require.Equal(t, uint64(accepted), stats.EventsVerified, "seed=%d", seed)
	require.Equal(t, uint64(dropped), stats.RevReplaysDropped, "seed=%d", seed)
	require.Equal(t, uint64(chainBreaks), stats.ChainBreaks, "seed=%d", seed)
	require.Equal(t, uint64(inversionErrs), stats.InversionFailures, "seed=%d", seed)
	// No signature failures: every commit is signed with the matching key.
	require.Equal(t, uint64(0), stats.SignatureFailures, "seed=%d", seed)
	// PolicyError: never resyncs.
	require.Equal(t, uint64(0), stats.Resyncs, "seed=%d", seed)
	require.Equal(t, uint64(0), stats.ResyncFailures, "seed=%d", seed)
	require.Equal(t, uint64(0), stats.ChainStateSaveFailures, "seed=%d MemChainStore should never fail to save", seed)
}

// extractDataCID decodes the inner repo.Commit from a synthetic
// SyncSubscribeRepos_Commit's CAR and returns its Data field. The CAR
// must be well-formed; callers that may have a malformed CAR should
// use tryExtractDataCID instead.
func extractDataCID(t *testing.T, commit *comatproto.SyncSubscribeRepos_Commit) cbor.CID {
	t.Helper()
	cid := tryExtractDataCID(commit)
	require.True(t, cid.Defined(), "couldn't extract data CID from synthetic commit")
	return cid
}

// tryExtractDataCID is the non-fatal variant — returns the zero CID on
// any decode failure, used in the malformed-CAR fault branch.
func tryExtractDataCID(commit *comatproto.SyncSubscribeRepos_Commit) cbor.CID {
	store, _, err := repo.LoadBlocksFromCAR(bytes.NewReader(commit.Blocks))
	if err != nil {
		return cbor.CID{}
	}
	commitCID, err := cbor.ParseCIDString(commit.Commit.Link)
	if err != nil {
		return cbor.CID{}
	}
	data, err := store.GetBlock(commitCID)
	if err != nil {
		return cbor.CID{}
	}
	c, err := repo.DecodeCommitCBOR(data)
	if err != nil {
		return cbor.CID{}
	}
	return c.Data
}
