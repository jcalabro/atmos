//go:build !js && !wasip1

package sync_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mathrand "math/rand"
	stdsync "sync"
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
	"golang.org/x/time/rate"
)

// TestVerifierSwarm drives the Verifier with random fault injection
// across multiple DIDs in parallel. After each iteration we assert the
// counter invariant:
//
//	emitted == accepted + dropped + chainBreaks + inversionErrs
//
// and confirm each Stats() field matches the locally-tracked count.
func TestVerifierSwarm(t *testing.T) {
	t.Parallel()

	iters := 10
	if !testing.Short() {
		iters = 1000
	}

	for i := 0; i < iters; i++ {
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			t.Parallel()
			runOneSwarmIteration(t, int64(i)+1)
		})
	}
}

// runOneSwarmIteration executes one self-contained swarm iteration with
// its own Verifier, its own StateStore, and its own set of DIDs. There
// is no shared state across iterations.
func runOneSwarmIteration(t *testing.T, seed int64) {
	t.Helper()
	rng := mathrand.New(mathrand.NewSource(seed))

	const numDIDs = 4
	dids := make([]atmos.DID, numDIDs)
	keys := make([]crypto.PrivateKey, numDIDs)
	repos := make([]*repo.Repo, numDIDs)
	for i := range numDIDs {
		dids[i] = atmos.DID(fmt.Sprintf("did:plc:swarm%d-%d", seed, i))
		k, err := crypto.GenerateP256()
		require.NoError(t, err)
		keys[i] = k
		repos[i], _ = testutil.BuildEmptyRepo(t, dids[i])
	}

	cs := sync.NewMemStateStore()
	resolver := testutil.NewTrackingResolver()
	for i := range numDIDs {
		resolver.Docs[dids[i]] = testutil.BuildDIDDoc(dids[i], keys[i].PublicKey())
	}
	dir := &identity.Directory{Resolver: resolver}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: cs,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	// lastGood[i] is the data CID of the last commit the verifier
	// accepted for dids[i] — i.e., what the verifier's StateStore
	// believes for that DID. Equal to repos[i].Tree.RootCID() at all
	// times outside of a single event's processing window.
	lastGood := make([]cbor.CID, numDIDs)
	for i := range numDIDs {
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
			dataCID, ok := testutil.InnerCommitDataCID(commit)
			require.True(t, ok, "couldn't extract data CID from accepted commit")
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
				dataCID, ok := testutil.InnerCommitDataCID(commit)
				require.True(t, ok, "couldn't extract data CID from chain-break commit")
				lastGood[didIdx] = dataCID
			case errors.As(vErr, &ie):
				inversionErrs++
				// On inversion failure the malformed CAR usually
				// prevents data-CID extraction, so the verifier
				// leaves state untouched. Reset r.Tree to lastGood
				// to undo BuildSyntheticCommit's mutation.
				if dataCID, ok := testutil.InnerCommitDataCID(commit); ok {
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
	for i := range numDIDs {
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
	for k := range numEvents {
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
	require.Equal(t, uint64(0), stats.ChainStateSaveFailures, "seed=%d MemStateStore should never fail to save", seed)
}

// TestVerifierSwarm_PolicyResync mirrors TestVerifierSwarm but under
// PolicyResync: chain breaks and inversion failures trigger
// transparent resyncs that must yield ActionResync ops to the
// consumer while leaving chain state at the authoritative current
// state. Counter invariants:
//
//	emitted == accepted + dropped + chainBreaks + inversionErrs
//	stats.Resyncs == chainBreaks + inversionErrs   (one resync per failure)
//
// The fake getRepo server is wired up dynamically: each clean accept
// re-exports the repo's current CAR and stashes it in a shared map so
// that any subsequent resync for that DID sees the up-to-date state.
//
// Like the PolicyError swarm, every commit is signed with the
// matching key, so SignatureFailures must stay zero.
func TestVerifierSwarm_PolicyResync(t *testing.T) {
	t.Parallel()

	iters := 10
	if !testing.Short() {
		iters = 1000
	}

	for i := 0; i < iters; i++ {
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			t.Parallel()
			runOneSwarmIterationPolicyResync(t, int64(i)+1)
		})
	}
}

func runOneSwarmIterationPolicyResync(t *testing.T, seed int64) {
	t.Helper()
	rng := mathrand.New(mathrand.NewSource(seed))

	const numDIDs = 4
	dids := make([]atmos.DID, numDIDs)
	keys := make([]crypto.PrivateKey, numDIDs)
	repos := make([]*repo.Repo, numDIDs)
	for i := range numDIDs {
		dids[i] = atmos.DID(fmt.Sprintf("did:plc:swarmrs%d-%d", seed, i))
		k, err := crypto.GenerateP256()
		require.NoError(t, err)
		keys[i] = k
		repos[i], _ = testutil.BuildEmptyRepo(t, dids[i])
	}

	// carForDID is the per-DID CAR the fake getRepo server returns.
	// Updated every time a clean commit advances the verifier's state
	// so a subsequent resync yields the up-to-date authoritative repo.
	var carMu stdsync.Mutex
	cars := make(map[atmos.DID][]byte, numDIDs)

	// Helper: re-export the current repo state and stash the CAR.
	exportCAR := func(didIdx int) {
		var buf bytes.Buffer
		require.NoError(t, repos[didIdx].ExportCAR(&buf, keys[didIdx]))
		carMu.Lock()
		cars[dids[didIdx]] = buf.Bytes()
		carMu.Unlock()
	}

	xc := testutil.NewFakeSyncServerMulti(t, func(did atmos.DID) ([]byte, bool) {
		carMu.Lock()
		defer carMu.Unlock()
		car, ok := cars[did]
		return car, ok
	})

	cs := sync.NewMemStateStore()
	resolver := testutil.NewTrackingResolver()
	for i := range numDIDs {
		resolver.Docs[dids[i]] = testutil.BuildDIDDoc(dids[i], keys[i].PublicKey())
	}
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

	lastGood := make([]cbor.CID, numDIDs)
	for i := range numDIDs {
		root, err := repos[i].Tree.WriteBlocks(repos[i].Store)
		require.NoError(t, err)
		lastGood[i] = root
	}

	var (
		emitted       int
		accepted      int
		dropped       int
		chainBreaks   int
		inversionErrs int
	)

	emit := func(t *testing.T, didIdx int, commit *comatproto.SyncSubscribeRepos_Commit) {
		t.Helper()
		emitted++
		ops, vErr := v.VerifyAndExpand(context.Background(), commit, nil)
		require.NoError(t, vErr,
			"PolicyResync should never surface a typed error for chain/inversion faults — got %T %v",
			vErr, vErr)
		if ops == nil {
			// Silent rev-replay drop. State unchanged. Rebuild from the
			// served CAR (same reason as the resync path: avoid
			// store-pollution carrying over across faults).
			dropped++
			carMu.Lock()
			carBytes := cars[dids[didIdx]]
			carMu.Unlock()
			rebuiltStore, _, err := repo.LoadBlocksFromCAR(bytes.NewReader(carBytes))
			require.NoError(t, err, "rebuild store from served CAR")
			repos[didIdx].Store = rebuiltStore
			repos[didIdx].Tree = mst.LoadTree(rebuiltStore, lastGood[didIdx])
			return
		}

		// Distinguish a clean-accept (op actions inherit from commit:
		// "create", "update", or "delete") from a resync (every op is
		// "resync"). Mixed shouldn't happen — the verifier emits one
		// or the other per event.
		isResync := false
		for _, op := range ops {
			if op.Action == atmos.ActionResync {
				isResync = true
				break
			}
		}

		if isResync {
			// Resync triggered by chain break or inversion failure.
			// Classify by reconstructing what the fault must have been
			// from the commit shape: inversion fault is the exact
			// [0xff, 0xff, 0xff] sentinel the random-fault loop sets
			// on commit.Blocks; everything else is a chain break (the
			// other resync-triggering fault path).
			if bytes.Equal(commit.Blocks, []byte{0xff, 0xff, 0xff}) {
				inversionErrs++
			} else {
				chainBreaks++
			}
			// Both fault classes leave the verifier's state at the
			// served data CID, which equals lastGood. Rebuild our
			// local repo from the served CAR so subsequent
			// BuildSyntheticCommit calls operate on a consistent
			// state — using mst.LoadTree alone left subtle store
			// pollution that broke inversion of follow-on commits.
			carMu.Lock()
			carBytes := cars[dids[didIdx]]
			carMu.Unlock()
			rebuiltStore, _, err := repo.LoadBlocksFromCAR(bytes.NewReader(carBytes))
			require.NoError(t, err, "rebuild store from served CAR")
			repos[didIdx].Store = rebuiltStore
			repos[didIdx].Tree = mst.LoadTree(rebuiltStore, lastGood[didIdx])
			// Every resync op should reference the fetched commit.
			for _, op := range ops {
				require.Equal(t, atmos.ActionResync, op.Action,
					"seed=%d resync batch must contain only ActionResync ops", seed)
				require.Equal(t, string(dids[didIdx]), op.Repo)
			}
			return
		}

		// Clean accept: state advanced to the new commit's data CID.
		accepted++
		dataCID, ok := testutil.InnerCommitDataCID(commit)
		require.True(t, ok, "couldn't extract data CID from accepted commit")
		lastGood[didIdx] = dataCID
		// Refresh the served CAR so a future resync sees this state.
		exportCAR(didIdx)
	}

	// Step 0: prime the fake server with each DID's empty-repo CAR
	// so the first chain break / inversion can resync against
	// well-formed authoritative state.
	for i := range numDIDs {
		exportCAR(i)
	}

	// Step 1: seed each DID with one clean commit.
	for i := range numDIDs {
		commit := testutil.BuildSyntheticCommit(t, repos[i], keys[i], lastGood[i], []testutil.OpAction{{
			Action:     testutil.ActionCreate,
			Collection: "app.bsky.feed.post",
			RKey:       fmt.Sprintf("seed%d", i),
			Record:     map[string]any{"text": "seed"},
		}})
		emit(t, i, commit)
	}

	// Step 2: 50 events of random fault injection.
	const numEvents = 50
	for k := range numEvents {
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

		emit(t, didIdx, commit)
	}

	stats := v.Stats()
	require.Equal(t, emitted, accepted+dropped+chainBreaks+inversionErrs,
		"seed=%d emit=%d acc=%d drop=%d cb=%d ie=%d",
		seed, emitted, accepted, dropped, chainBreaks, inversionErrs)
	require.Equal(t, uint64(accepted), stats.EventsVerified, "seed=%d", seed)
	require.Equal(t, uint64(dropped), stats.RevReplaysDropped, "seed=%d", seed)
	require.Equal(t, uint64(chainBreaks), stats.ChainBreaks, "seed=%d", seed)
	require.Equal(t, uint64(inversionErrs), stats.InversionFailures, "seed=%d", seed)
	// Every chain break and inversion failure under PolicyResync
	// triggers exactly one successful resync.
	require.Equal(t, uint64(chainBreaks+inversionErrs), stats.Resyncs, "seed=%d", seed)
	require.Equal(t, uint64(0), stats.ResyncFailures, "seed=%d", seed)
	require.Equal(t, uint64(0), stats.SignatureFailures, "seed=%d", seed)
	require.Equal(t, uint64(0), stats.ChainStateSaveFailures, "seed=%d", seed)
}
