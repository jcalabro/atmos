// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
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

// buildUpdateChainFrames returns N synthetic commit frames, each an
// Update of a single shared record key. Each commit's Seq is set to
// (i+1). Returns the post-state prevData for the LAST commit.
//
// We use Update (rather than Create) because a chain of Creates would
// expose an unrelated MST.Remove dirtying issue surfaced by the
// verifier's inversion routine on multi-level trees. Update inverts as
// Insert, which correctly propagates dirty markers up the spine. The
// streaming-layer wiring under test here is independent of which op
// type was applied.
func buildUpdateChainFrames(t *testing.T, did atmos.DID, key crypto.PrivateKey, n int) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, n)

	for i := 0; i < n; i++ {
		// Build each commit on a fresh repo seeded with the prior
		// version of "shared" so the pre-state is well-defined.
		r, _ := testutil.BuildEmptyRepo(t, did)
		require.NoError(t, r.Create("app.bsky.feed.post", "shared",
			map[string]any{"text": fmt.Sprintf("v%d", i)}))
		prevData, err := r.Tree.WriteBlocks(r.Store)
		require.NoError(t, err)

		commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
			Action:     string(ActionUpdate),
			Collection: "app.bsky.feed.post",
			RKey:       "shared",
			Record:     map[string]any{"text": fmt.Sprintf("v%d-next", i)},
		}})
		commit.Seq = int64(i + 1)
		body, err := commit.MarshalCBOR()
		require.NoError(t, err)
		frames = append(frames, buildFrame("#commit", body))
	}
	return frames
}

// innerCommitDataCID extracts the post-state MST root CID (i.e. inner
// commit.Data) from a #commit event's CAR diff.
func innerCommitDataCID(t *testing.T, commit *comatproto.SyncSubscribeRepos_Commit) cbor.CID {
	t.Helper()
	store, _, err := repo.LoadBlocksFromCAR(bytes.NewReader(commit.Blocks))
	require.NoError(t, err)
	commitCID, err := cbor.ParseCIDString(commit.Commit.Link)
	require.NoError(t, err)
	commitData, err := store.GetBlock(commitCID)
	require.NoError(t, err)
	innerCommit, err := repo.DecodeCommitCBOR(commitData)
	require.NoError(t, err)
	return innerCommit.Data
}

// mustMarshalCBOR is a test convenience for SyncSubscribeRepos_Commit.
func mustMarshalCBOR(t *testing.T, c *comatproto.SyncSubscribeRepos_Commit) []byte {
	t.Helper()
	data, err := c.MarshalCBOR()
	require.NoError(t, err)
	return data
}

// buildVerifiedChainFrames produces N commit frames using Update ops on
// "shared" so that each successive commit's PrevData matches the prior
// commit's post-state Data CID — i.e., a real verifier-acceptable
// chain. Used by the HappyPath test.
func buildVerifiedChainFrames(t *testing.T, did atmos.DID, key crypto.PrivateKey, n int) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, n)

	r, _ := testutil.BuildEmptyRepo(t, did)
	// Initial create so subsequent updates have something to mutate.
	require.NoError(t, r.Create("app.bsky.feed.post", "shared",
		map[string]any{"text": "v0"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
			Action:     string(ActionUpdate),
			Collection: "app.bsky.feed.post",
			RKey:       "shared",
			Record:     map[string]any{"text": fmt.Sprintf("v%d", i+1)},
		}})
		commit.Seq = int64(i + 1)
		body, err := commit.MarshalCBOR()
		require.NoError(t, err)
		frames = append(frames, buildFrame("#commit", body))
		prevData = innerCommitDataCID(t, commit)
	}
	return frames
}

func TestVerifiedStream_HappyPath(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:vstream1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	const n = 5
	frames := buildVerifiedChainFrames(t, did, key, n)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	sc := sync.NewClient(sync.Options{
		Client:    &xrpc.Client{Host: "https://nope.invalid"},
		Directory: gt.Some(dir),
	})

	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient:  sc,
		Directory:   dir,
		ChainStore:  sync.NewMemChainStore(),
		Policy:      sync.PolicyResync,
		ResyncLimit: rate.Inf,
		ResyncBurst: 1,
	})
	require.NoError(t, err)

	client := mustNewClient(t, Options{
		URL:      wsURL(srv),
		Verifier: gt.Some(verifier),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var totalUpdates int
	for batch, batchErr := range client.Events(ctx) {
		require.NoError(t, batchErr)
		for _, evt := range batch {
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionUpdate {
					totalUpdates++
				}
			}
		}
		if totalUpdates >= n {
			cancel()
		}
	}
	assert.Equal(t, n, totalUpdates)
	stats := verifier.Stats()
	assert.Equal(t, uint64(n), stats.EventsVerified)
}

func TestVerifiedStream_NoVerifierUnchangedBehavior(t *testing.T) {
	t.Parallel()
	// Same scripted firehose, no verifier configured. The CAR-decode path
	// must produce the same op count — regression check that the verifier
	// opt-in didn't break the legacy code path.

	did := atmos.DID("did:plc:vstream2")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Use independent-update frames here: without a verifier, the
	// PrevData chain is ignored, so we don't need a verifier-acceptable
	// chain. We just want N op-bearing commits.
	const n = 5
	frames := buildUpdateChainFrames(t, did, key, n)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	client := mustNewClient(t, Options{URL: wsURL(srv)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var totalUpdates int
	for batch, batchErr := range client.Events(ctx) {
		require.NoError(t, batchErr)
		for _, evt := range batch {
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionUpdate {
					totalUpdates++
				}
			}
		}
		if totalUpdates >= n {
			cancel()
		}
	}
	assert.Equal(t, n, totalUpdates)
}

func TestVerifiedStream_VerifierErrorDoesNotTriggerSpuriousGap(t *testing.T) {
	t.Parallel()
	// Emit 3 commits where the middle one (seq=2) has a forged chain
	// break (PrevData mutated to a bogus CID). Under PolicyError the
	// verifier surfaces ChainBreakError but state advances to commit-2's
	// data CID; commit #3 is built atop that and must verify cleanly.
	// The seq-gap-after-verifier-error fix from Task 14 must prevent a
	// spurious GapError between the verifier-error event and commit #3.
	//
	// All three commits Update the same "shared" record so inversion
	// uses Insert (not Remove), avoiding an MST.Remove dirtying issue
	// unrelated to the streaming-layer wiring under test.

	did := atmos.DID("did:plc:vgap1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	require.NoError(t, r.Create("app.bsky.feed.post", "shared",
		map[string]any{"text": "v0"}))
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Commit 1: clean Update under first-sighting accept.
	c1 := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action: string(ActionUpdate), Collection: "app.bsky.feed.post", RKey: "shared",
		Record: map[string]any{"text": "v1"},
	}})
	c1.Seq = 1
	prevDataAfterC1 := innerCommitDataCID(t, c1)

	// Commit 2: structurally valid but with a forged PrevData (bogus CID).
	c2 := testutil.BuildSyntheticCommit(t, r, key, prevDataAfterC1, []testutil.OpAction{{
		Action: string(ActionUpdate), Collection: "app.bsky.feed.post", RKey: "shared",
		Record: map[string]any{"text": "v2"},
	}})
	c2.Seq = 2
	bogusCID, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)
	c2.PrevData = gt.Some(lextypes.LexCIDLink{Link: bogusCID.String()})
	// c2's actual post-state data CID is what the verifier advances to
	// under PolicyError after the chain-break.
	prevDataAfterC2 := innerCommitDataCID(t, c2)

	// Commit 3: clean Update built atop c2's actual data root (matching
	// the verifier's post-failure state).
	c3 := testutil.BuildSyntheticCommit(t, r, key, prevDataAfterC2, []testutil.OpAction{{
		Action: string(ActionUpdate), Collection: "app.bsky.feed.post", RKey: "shared",
		Record: map[string]any{"text": "v3"},
	}})
	c3.Seq = 3

	frames := [][]byte{
		buildFrame("#commit", mustMarshalCBOR(t, c1)),
		buildFrame("#commit", mustMarshalCBOR(t, c2)),
		buildFrame("#commit", mustMarshalCBOR(t, c3)),
	}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		ChainStore: sync.NewMemChainStore(),
		Policy:     sync.PolicyError,
	})
	require.NoError(t, err)

	client := mustNewClient(t, Options{URL: wsURL(srv), Verifier: gt.Some(verifier)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		chainBreaks int
		gapErrors   int
		updates     int
	)
	for batch, batchErr := range client.Events(ctx) {
		if batchErr != nil {
			var cb *sync.ChainBreakError
			var ge *GapError
			switch {
			case stderrors.As(batchErr, &cb):
				chainBreaks++
			case stderrors.As(batchErr, &ge):
				gapErrors++
			}
			if updates+chainBreaks >= 3 {
				cancel()
			}
			continue
		}
		for _, evt := range batch {
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionUpdate {
					updates++
				}
			}
		}
		if updates+chainBreaks >= 3 {
			cancel()
		}
	}
	assert.Equal(t, 1, chainBreaks, "expected exactly one ChainBreakError")
	assert.Equal(t, 0, gapErrors, "verifier-error path must not trigger spurious GapError")
	assert.GreaterOrEqual(t, updates, 2, "commits 1 and 3 should verify cleanly and yield update ops")
}
