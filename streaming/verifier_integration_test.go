// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	stdsync "sync"
	"sync/atomic"
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
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// buildUpdateChainFrames returns N synthetic commit frames, each an
// Update of a single shared record key. Each commit's Seq is set to
// (i+1). The frames are independent: each is built on a fresh repo, so
// successive commits' PrevData values do NOT chain. Suitable only for
// the no-verifier regression test, where the verifier-acceptable chain
// is not enforced.
//
// Either Update or Create works correctly here post-MST-fix (see
// 0e155c4 for the mst.Tree.Remove parent-dirty propagation fix that
// removed the previous Update-only constraint); we keep Update because
// the action is incidental for this no-verifier path.
func buildUpdateChainFrames(t *testing.T, did atmos.DID, key crypto.PrivateKey, n int) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, n)

	for i := range n {
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

// mustMarshalCBOR is a test convenience for SyncSubscribeRepos_Commit.
func mustMarshalCBOR(t *testing.T, c *comatproto.SyncSubscribeRepos_Commit) []byte {
	t.Helper()
	data, err := c.MarshalCBOR()
	require.NoError(t, err)
	return data
}

// buildVerifiedChainFrames produces N commit frames forming a real
// verifier-acceptable chain: each successive commit's PrevData matches
// the prior commit's post-state Data CID. Each commit is a Create of a
// distinct rkey, exercising the inversion path's tree.Remove (which
// requires the parent-dirty propagation fix in 0e155c4 to round-trip
// correctly on multi-level trees). Used by the HappyPath test for
// closer parity with real-firehose Create-heavy traffic.
func buildVerifiedChainFrames(t *testing.T, did atmos.DID, key crypto.PrivateKey, n int) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, n)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	for i := range n {
		commit := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
			Action:     testutil.ActionCreate,
			Collection: "app.bsky.feed.post",
			RKey:       fmt.Sprintf("rec%d", i),
			Record:     map[string]any{"text": fmt.Sprintf("v%d", i)},
		}})
		commit.Seq = int64(i + 1)
		body, err := commit.MarshalCBOR()
		require.NoError(t, err)
		frames = append(frames, buildFrame("#commit", body))
		var ok bool
		prevData, ok = testutil.InnerCommitDataCID(commit)
		require.True(t, ok, "couldn't extract data CID from synthetic commit")
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
		SyncClient:  gt.Some(sc),
		Directory:   dir,
		StateStore:  sync.NewMemStateStore(),
		Policy:      gt.Some(sync.PolicyResync),
		ResyncLimit: gt.Some(rate.Inf),
		ResyncBurst: gt.Some(1),
	})
	require.NoError(t, err)

	client := mustNewClient(t, Options{
		URL:      wsURL(srv),
		Verifier: gt.Some(verifier),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var totalCreates int
	for batch, batchErr := range client.Events(ctx) {
		require.NoError(t, batchErr)
		for _, evt := range batch {
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionCreate {
					totalCreates++
				}
			}
		}
		if totalCreates >= n {
			cancel()
		}
	}
	assert.Equal(t, n, totalCreates)
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

	client := mustNewClient(t, Options{
		URL:      wsURL(srv),
		Verifier: gt.Some[*sync.Verifier](nil),
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
	// The chain-break is forged via PrevData rewrite, so the action
	// type (Update vs Create) is incidental to what's being tested.

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
	prevDataAfterC1, ok := testutil.InnerCommitDataCID(c1)
	require.True(t, ok, "couldn't extract data CID from c1")

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
	prevDataAfterC2, ok := testutil.InnerCommitDataCID(c2)
	require.True(t, ok, "couldn't extract data CID from c2")

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
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
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

// buildAccountBodyWithStatus mirrors buildAccountBody (client_test.go)
// but includes the optional Status field used by takedown/suspend
// states. Used by the #account-wiring tests below.
func buildAccountBodyWithStatus(seq int64, did string, active bool, status string) []byte {
	evt := &comatproto.SyncSubscribeRepos_Account{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#account",
		DID:           did,
		Seq:           seq,
		Active:        active,
		Time:          "2024-01-01T00:00:00Z",
	}
	if status != "" {
		evt.Status = gt.Some(status)
	}
	data, err := evt.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

// TestVerifiedStream_AccountEventGatesSubsequentCommit is the headline
// test for the streaming↔verifier #account wiring: a takedown account
// event arriving on the firehose must populate HostingState before the
// next #commit for the same DID, so HostingGate can drop it.
//
// Frame sequence: clean commit (seq=1) → takedown account event (seq=2)
// → another commit (seq=3) for the now-takendown DID. The third commit
// must surface as AccountInactiveError.
func TestVerifiedStream_AccountEventGatesSubsequentCommit(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:gatedstream1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Build two chained commits.
	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData0, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	c1 := testutil.BuildSyntheticCommit(t, r, key, prevData0, []testutil.OpAction{{
		Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec1",
		Record: map[string]any{"text": "v1"},
	}})
	c1.Seq = 1
	prevData1, ok := testutil.InnerCommitDataCID(c1)
	require.True(t, ok, "couldn't extract data CID from c1")

	c2 := testutil.BuildSyntheticCommit(t, r, key, prevData1, []testutil.OpAction{{
		Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec2",
		Record: map[string]any{"text": "v2"},
	}})
	c2.Seq = 3 // seq=2 is the account event between them.

	frames := [][]byte{
		buildFrame("#commit", mustMarshalCBOR(t, c1)),
		buildFrame("#account", buildAccountBodyWithStatus(2, string(did), false, sync.StatusTakendown)),
		buildFrame("#commit", mustMarshalCBOR(t, c2)),
	}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:     dir,
		StateStore:    sync.NewMemStateStore(),
		Policy:        gt.Some(sync.PolicyError),
		HostingPolicy: gt.Some(sync.HostingGate),
	})
	require.NoError(t, err)

	client := mustNewClient(t, Options{URL: wsURL(srv), Verifier: gt.Some(verifier)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		creates       int
		accountSeen   bool
		gateRejection bool
	)
	for batch, batchErr := range client.Events(ctx) {
		if batchErr != nil {
			var aiErr *sync.AccountInactiveError
			if stderrors.As(batchErr, &aiErr) {
				gateRejection = true
				assert.Equal(t, did, aiErr.DID)
				assert.Equal(t, sync.StatusTakendown, aiErr.Status)
			}
			if creates >= 1 && gateRejection {
				cancel()
			}
			continue
		}
		for _, evt := range batch {
			if evt.Account != nil {
				accountSeen = true
				assert.Equal(t, string(did), evt.Account.DID,
					"#account event must still be delivered to the consumer")
			}
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionCreate {
					creates++
				}
			}
		}
		if creates >= 1 && gateRejection {
			cancel()
		}
	}
	assert.Equal(t, 1, creates, "first commit should pass; second must be gated")
	assert.True(t, accountSeen, "consumer must still observe the #account event itself")
	assert.True(t, gateRejection, "second commit must surface AccountInactiveError")

	// Verifier counter sanity check: the gate ticked, the first commit
	// was verified, the verifier saw exactly one account event.
	stats := verifier.Stats()
	assert.Equal(t, uint64(1), stats.AccountsInactive)
	assert.Equal(t, uint64(1), stats.EventsVerified)

	// HostingState in the StateStore must reflect the takedown.
	hosting, err := verifier.StateStore().LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, hosting, "account event should have populated hosting state")
	assert.False(t, hosting.Active)
	assert.Equal(t, sync.StatusTakendown, hosting.Status)
}

// TestVerifiedStream_AccountEventTrackedUnderHostingTrack covers the
// default HostingTrack policy: the streaming layer still feeds account
// events into the verifier (so consumers can read state via
// StateStore.LoadHosting), but commits are not gated.
func TestVerifiedStream_AccountEventTrackedUnderHostingTrack(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:tracked1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	r, _ := testutil.BuildEmptyRepo(t, did)
	prevData, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	c1 := testutil.BuildSyntheticCommit(t, r, key, prevData, []testutil.OpAction{{
		Action: testutil.ActionCreate, Collection: "app.bsky.feed.post", RKey: "rec1",
		Record: map[string]any{"text": "v1"},
	}})
	c1.Seq = 2 // seq=1 is the account event.

	frames := [][]byte{
		buildFrame("#account", buildAccountBodyWithStatus(1, string(did), false, sync.StatusSuspended)),
		buildFrame("#commit", mustMarshalCBOR(t, c1)),
	}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
		// HostingPolicy left unset → HostingTrack default.
	})
	require.NoError(t, err)

	client := mustNewClient(t, Options{URL: wsURL(srv), Verifier: gt.Some(verifier)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var creates int
	for batch, batchErr := range client.Events(ctx) {
		require.NoError(t, batchErr)
		for _, evt := range batch {
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionCreate {
					creates++
				}
			}
		}
		if creates >= 1 {
			cancel()
		}
	}
	assert.Equal(t, 1, creates, "HostingTrack must NOT gate commits")

	// State was tracked, just not gated.
	hosting, err := verifier.StateStore().LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, hosting, "HostingTrack must still persist account state")
	assert.False(t, hosting.Active)
	assert.Equal(t, sync.StatusSuspended, hosting.Status)
	assert.Equal(t, uint64(0), verifier.Stats().AccountsInactive)
}

// TestVerifiedStream_AccountEventReplayDropped covers the seq-based
// replay gate inside Verifier.OnAccountEvent, observed end-to-end:
// re-delivering an account event at the same seq must not double-count
// in the AccountEventReplaysDropped stat.
func TestVerifiedStream_AccountEventReplayDropped(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:replayacct1")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	frames := [][]byte{
		// First account event lands cleanly.
		buildFrame("#account", buildAccountBodyWithStatus(10, string(did), true, "")),
		// Re-deliver same seq — must be silently dropped (not an error).
		buildFrame("#account", buildAccountBodyWithStatus(10, string(did), false, sync.StatusTakendown)),
		// And one more at a lower seq, also a replay.
		buildFrame("#account", buildAccountBodyWithStatus(5, string(did), false, sync.StatusTakendown)),
	}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)

	client := mustNewClient(t, Options{URL: wsURL(srv), Verifier: gt.Some(verifier)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var accountEvents int
	for batch, batchErr := range client.Events(ctx) {
		require.NoError(t, batchErr,
			"replay drops are silent inside the verifier; the streaming layer must not surface them as errors")
		for _, evt := range batch {
			if evt.Account != nil {
				accountEvents++
			}
		}
		if accountEvents >= 3 {
			cancel()
		}
	}
	assert.Equal(t, 3, accountEvents, "consumer must still see all three account events")

	// Two of three were replays from the verifier's view.
	stats := verifier.Stats()
	assert.Equal(t, uint64(2), stats.AccountEventReplaysDropped,
		"verifier should have dropped the second and third events as seq replays")

	// Persisted state reflects the FIRST event (Active=true), since the
	// later events were rejected as replays.
	hosting, err := verifier.StateStore().LoadHosting(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, hosting)
	assert.True(t, hosting.Active, "state must reflect first event, not dropped re-deliveries")
	assert.Equal(t, int64(10), hosting.Seq)
}

// blockingStateStore wraps a sync.StateStore and delays SaveChain.
// Tests use it to provoke queue buildup in the streaming scheduler.
type blockingStateStore struct {
	inner     sync.StateStore
	saveDelay time.Duration
}

func (s *blockingStateStore) LoadChain(ctx context.Context, did atmos.DID) (*sync.ChainState, error) {
	return s.inner.LoadChain(ctx, did)
}

func (s *blockingStateStore) SaveChain(ctx context.Context, did atmos.DID, state sync.ChainState) error {
	delay := s.saveDelay
	if delay == 0 {
		delay = 50 * time.Millisecond
	}

	// Add a delay to each SaveChain to make verification slow. This
	// allows commits to pile up and overflow bounded per-DID queues.
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.inner.SaveChain(ctx, did, state)
}

func (s *blockingStateStore) LoadHosting(ctx context.Context, did atmos.DID) (*sync.HostingState, error) {
	return s.inner.LoadHosting(ctx, did)
}

func (s *blockingStateStore) SaveHosting(ctx context.Context, did atmos.DID, state sync.HostingState) error {
	return s.inner.SaveHosting(ctx, did, state)
}

func (s *blockingStateStore) Delete(ctx context.Context, did atmos.DID) error {
	return s.inner.Delete(ctx, did)
}

func TestVerifiedStream_DropErrorOnQueueOverflow(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:dropoverflow")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	const n = 100
	frames := buildVerifiedChainFrames(t, did, key, n)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	slowStore := &blockingStateStore{
		inner:     sync.NewMemStateStore(),
		saveDelay: 10 * time.Millisecond,
	}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: slowStore,
		// No SyncClient required: we'll never resync (single DID, valid chain).
		Policy: gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	defer func() { _ = v.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Parallelism=2 → per-DID queue cap = 2 * 2 = 4.
	// (At Parallelism=1 the per-key queue is unbounded by design — see
	// TestSerialMode_NoDropErrorUnderBackpressure — so this test must
	// use Parallelism > 1 to exercise the drop-oldest path at all.)
	// With 100 commits arriving rapidly and delayed SaveChain calls,
	// the dispatch goroutine will fill the queue faster than the
	// workers can drain it, triggering drop-oldest.
	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Verifier:    gt.Some(v),
		Parallelism: gt.Some(2),
	})

	dropCount := 0
	var firstDrop *DropError

	for batch, err := range client.Events(ctx) {
		if err != nil {
			var de *DropError
			if stderrors.As(err, &de) {
				dropCount++
				if firstDrop == nil {
					firstDrop = de
				}
			}
		}
		_ = batch
		if dropCount > 0 {
			cancel()
			break
		}
	}

	require.Greater(t, dropCount, 0, "expected at least one DropError")
	require.Equal(t, string(did), firstDrop.DID, "DropError should reference the offending DID")
	require.Greater(t, firstDrop.Seq, int64(0), "DropError seq should be > 0")
	require.Equal(t, 4, firstDrop.QueueLen, "QueueLen should match parallelism * 2")
}

// TestVerifiedStream_ParallelGapDetectionAcrossReconnect asserts that
// the parallel readLoop seeds lastSeenSeq from the persisted cursor on
// startup, so a relay that resumes outside our cursor window surfaces a
// GapError. Pre-fix, lastSeenSeq was zero-initialized per readLoop
// invocation, so the first frame after a reconnect always passed the
// `lastSeenSeq > 0` guard and gaps were silent.
func TestVerifiedStream_ParallelGapDetectionAcrossReconnect(t *testing.T) {
	t.Parallel()

	// Skip the verifier — gap detection runs in the dispatch goroutine
	// before scheduler dispatch and is independent of verification.
	// Drive #identity events directly.
	var connCount atomic.Int32
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		n := connCount.Add(1)
		if n == 1 {
			// First connection: cursor advances to 100.
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(100, "did:plc:gap1")))
			_ = conn.CloseNow()
			return
		}
		// Reconnect: relay's outbox window has advanced past our
		// cursor. Pre-fix: parallel mode silently accepted this.
		// Post-fix: GapError surfaces.
		_ = conn.Write(ctx, websocket.MessageBinary,
			buildFrame("#identity", buildIdentityBody(5000, "did:plc:gap2")))
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Parallelism: gt.Some(4),
		Backoff: gt.Some(BackoffPolicy{
			InitialDelay: gt.Some(10 * time.Millisecond),
			MaxDelay:     gt.Some(50 * time.Millisecond),
			Multiplier:   gt.Some(2.0),
			Jitter:       gt.Some(false),
		}),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		gapSeen *GapError
		evts    int
	)
	for batch, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			var ge *GapError
			if stderrors.As(iterErr, &ge) {
				gapSeen = ge
			}
			continue
		}
		evts += len(batch)
		if gapSeen != nil && evts >= 2 {
			cancel()
		}
	}

	require.NotNil(t, gapSeen, "parallel mode must surface a GapError when the relay resumes past our cursor")
	require.Equal(t, int64(101), gapSeen.Expected, "expected gap = lastSeenSeq+1 = 101")
	require.Equal(t, int64(5000), gapSeen.Got)
}

// failingHostingStateStore returns errFailingHosting from SaveHosting,
// so OnAccountEvent surfaces an infrastructure error. All other
// methods delegate to inner.
type failingHostingStateStore struct {
	inner sync.StateStore
}

var errFailingHosting = stderrors.New("synthetic SaveHosting failure")

func (s *failingHostingStateStore) LoadChain(ctx context.Context, did atmos.DID) (*sync.ChainState, error) {
	return s.inner.LoadChain(ctx, did)
}
func (s *failingHostingStateStore) SaveChain(ctx context.Context, did atmos.DID, st sync.ChainState) error {
	return s.inner.SaveChain(ctx, did, st)
}
func (s *failingHostingStateStore) LoadHosting(ctx context.Context, did atmos.DID) (*sync.HostingState, error) {
	return s.inner.LoadHosting(ctx, did)
}
func (s *failingHostingStateStore) SaveHosting(ctx context.Context, did atmos.DID, _ sync.HostingState) error {
	return errFailingHosting
}
func (s *failingHostingStateStore) Delete(ctx context.Context, did atmos.DID) error {
	return s.inner.Delete(ctx, did)
}

// TestVerifiedStream_AccountErrorStillDeliversEvent_Parallel asserts
// that under Parallelism > 1, an OnAccountEvent infrastructure failure
// surfaces the error to the consumer AND still delivers the raw
// #account event. The result handler in readLoop must yield the
// account event after yielding accountErr — swallowing the event
// would silently mask takedowns/suspensions from consumers that don't
// rely on the verifier's HostingState bookkeeping.
func TestVerifiedStream_AccountErrorStillDeliversEvent_Parallel(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:accterr")

	frames := [][]byte{
		buildFrame("#account", buildAccountBodyWithStatus(1, string(did), false, sync.StatusTakendown)),
	}

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	store := &failingHostingStateStore{inner: sync.NewMemStateStore()}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  &identity.Directory{Resolver: testutil.NewTrackingResolver()},
		StateStore: store,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	defer func() { _ = v.Close() }()

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Verifier:    gt.Some(v),
		Parallelism: gt.Some(2),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		accountEventDelivered bool
		accountErrorYielded   bool
	)
	for batch, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			if stderrors.Is(iterErr, errFailingHosting) {
				accountErrorYielded = true
			}
			continue
		}
		for _, evt := range batch {
			if evt.Account != nil && evt.Account.DID == string(did) {
				accountEventDelivered = true
			}
		}
		if accountEventDelivered && accountErrorYielded {
			cancel()
		}
	}

	require.True(t, accountErrorYielded, "verifier infra failure must surface to consumer")
	require.True(t, accountEventDelivered,
		"raw #account event must still be delivered after OnAccountEvent failure")
}

// cancellableStateStore wraps a StateStore and blocks SaveChain on a
// gate that only unblocks via context cancellation. Used to assert
// that consumer ctx cancellation propagates into in-flight verifier I/O
// under parallel mode (Finding 2).
type cancellableStateStore struct {
	inner       sync.StateStore
	saveStarted chan struct{}
	saveOnce    stdsync.Once
}

func (s *cancellableStateStore) LoadChain(ctx context.Context, did atmos.DID) (*sync.ChainState, error) {
	return s.inner.LoadChain(ctx, did)
}

func (s *cancellableStateStore) SaveChain(ctx context.Context, did atmos.DID, state sync.ChainState) error {
	s.saveOnce.Do(func() { close(s.saveStarted) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *cancellableStateStore) LoadHosting(ctx context.Context, did atmos.DID) (*sync.HostingState, error) {
	return s.inner.LoadHosting(ctx, did)
}

func (s *cancellableStateStore) SaveHosting(ctx context.Context, did atmos.DID, state sync.HostingState) error {
	return s.inner.SaveHosting(ctx, did, state)
}

func (s *cancellableStateStore) Delete(ctx context.Context, did atmos.DID) error {
	return s.inner.Delete(ctx, did)
}

// TestVerifiedStream_ParallelCtxCancelsInflightVerifier asserts that
// cancelling Events(ctx) under Parallelism > 1 promptly cancels the
// scheduler's worker context, which propagates into VerifyCommit and
// the StateStore I/O it does. Without ctx threading, the worker would
// block in SaveChain indefinitely and Events() would hang on
// sched.Shutdown() during the deferred teardown.
func TestVerifiedStream_ParallelCtxCancelsInflightVerifier(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:ctxcancel")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	frames := buildVerifiedChainFrames(t, did, key, 1)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
		// Hold the WS open so the connection isn't the thing that ends
		// the iterator — only ctx cancellation should.
		<-time.After(30 * time.Second)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	store := &cancellableStateStore{
		inner:       sync.NewMemStateStore(),
		saveStarted: make(chan struct{}),
	}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: store,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	defer func() { _ = v.Close() }()

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Verifier:    gt.Some(v),
		Parallelism: gt.Some(2),
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range client.Events(ctx) {
		}
	}()

	// Wait for the verifier to enter SaveChain.
	select {
	case <-store.saveStarted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("SaveChain never started")
	}

	// Cancel and assert Events() returns promptly. Pre-fix, the
	// scheduler's defer Shutdown() would wg.Wait() forever because the
	// worker held context.Background() and never unblocked.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Events() did not return within 2s of ctx cancellation; in-flight verifier work was not cancelled")
	}
}

// TestDropError_SuppressedAccounting is a focused unit test for the
// suppression counter logic in readLoop's onDrop closure. The
// closure is closed-over locals (suppressedDrops, asyncErr, queueCap),
// so we re-implement the same logic in-line to exercise it directly.
// This guards the contract: once asyncErr fills, subsequent drops are
// counted via suppressedDrops and rolled into the next successful send.
func TestDropError_SuppressedAccounting(t *testing.T) {
	t.Parallel()

	asyncErr := make(chan error, 4)
	const queueCap = 4
	var suppressedDrops uint64

	emit := func(seq int64) {
		err := &DropError{DID: "did:plc:test", Seq: seq, QueueLen: queueCap, AdditionalDropsSuppressed: suppressedDrops}
		select {
		case asyncErr <- err:
			suppressedDrops = 0
		default:
			suppressedDrops++
		}
	}

	// Fill the buffer (4 sends), then 6 more — those should be
	// suppressed because nothing drains the channel.
	for i := int64(1); i <= 4; i++ {
		emit(i)
	}
	require.Equal(t, uint64(0), suppressedDrops)

	for i := int64(5); i <= 10; i++ {
		emit(i)
	}
	require.Equal(t, uint64(6), suppressedDrops, "6 drops should be suppressed once the buffer is full")

	// Drain one slot to make room.
	<-asyncErr

	// Next emit should succeed AND carry the accumulated suppression.
	emit(11)
	require.Equal(t, uint64(0), suppressedDrops, "successful emit must reset the suppression counter")

	// Drain remaining slots and verify the carried suppressed count.
	var seenSuppressed uint64
	for len(asyncErr) > 0 {
		err := <-asyncErr
		var de *DropError
		require.True(t, stderrors.As(err, &de))
		seenSuppressed += de.AdditionalDropsSuppressed
	}
	require.Equal(t, uint64(6), seenSuppressed, "consumer summing AdditionalDropsSuppressed must recover the lost count exactly")
}

// TestVerifiedStream_CursorAdvancesPastDroppedSeq is the regression
// test for the cursor-freeze bug where the parallel scheduler's onDrop
// callback did not remove the displaced seq from the readLoop's
// inflight set. Symptoms before the fix: the watermark cursor stays
// pinned to the dropped seq forever, so persisted cursor never advances
// past the first drop, and drainResults() hangs on connection close
// because inflight.Len() never reaches zero.
//
// The test forces several drops, lets all surviving verifications drain,
// and asserts the persisted cursor has advanced past every dropped seq.
func TestVerifiedStream_CursorAdvancesPastDroppedSeq(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:dropcursor")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	const n = 60
	frames := buildVerifiedChainFrames(t, did, key, n)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	// Brief delay per SaveChain provokes the dispatch goroutine to
	// outrun the workers and trigger drops, but is short enough that
	// the surviving frames clear within the test deadline.
	slowStore := &blockingStateStore{
		inner:     sync.NewMemStateStore(),
		saveDelay: 10 * time.Millisecond,
	}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: slowStore,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	defer func() { _ = v.Close() }()

	store := &mockCursorStore{}

	// Parallelism=2 -> keyQueueCap = 4. With 60 commits and delayed
	// SaveChain calls, the first ~5 fit (workers + queue) and the rest
	// shed via drop-oldest. The fix ensures inflight tracking releases
	// dropped seqs so the cursor watermark can still advance.
	client := mustNewClient(t, Options{
		URL:            wsURL(srv),
		Verifier:       gt.Some(v),
		Parallelism:    gt.Some(2),
		CursorStore:    gt.Some[CursorStore](store),
		CursorInterval: gt.Some(int64(1)), // checkpoint after every batch
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		dropCount   int
		highestDrop int64
		batches     int
	)
	for batch, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			var de *DropError
			if stderrors.As(iterErr, &de) {
				dropCount++
				if de.Seq > highestDrop {
					highestDrop = de.Seq
				}
			}
			continue
		}
		batches++
		// Stop once we've observed both drops and at least one delivered
		// batch with a seq above the highest drop, so we know the cursor
		// has had a chance to advance past the freeze point.
		if dropCount > 0 && len(batch) > 0 {
			lastSeq := batch[len(batch)-1].seqOf()
			if highestDrop > 0 && lastSeq > highestDrop {
				cancel()
			}
		}
	}

	require.Greater(t, dropCount, 0, "test must observe drops to be meaningful")
	require.Greater(t, highestDrop, int64(0))

	// Cursor must have advanced past the highest dropped seq: the bug
	// pinned it permanently below highestDrop.
	finalCursor := client.Cursor()
	require.Greater(t, finalCursor, highestDrop,
		"cursor (%d) must advance past highest dropped seq (%d); the watermark must release dropped seqs from inflight",
		finalCursor, highestDrop)

	// Persisted cursor must reflect the same advance.
	store.mu.Lock()
	persisted := store.cursor
	store.mu.Unlock()
	require.Greater(t, persisted, highestDrop,
		"persisted cursor (%d) must also advance past highest dropped seq (%d)",
		persisted, highestDrop)
}

// TestSerialMode_NoDropErrorUnderBackpressure pins down the contract
// that Parallelism=1 preserves backpressure: a slow verifier under a
// single hot DID must never emit a DropError. The single-worker setup
// implies an unbounded per-key queue (or equivalent backpressure via the
// reader goroutine blocking on conn.Read), so the dispatch goroutine
// can never outrun the worker enough to displace queued work.
//
// This is the headline behavioral difference between N=1 and N>1: the
// strict-order escape hatch must not silently drop events. The
// blockingStateStore makes every SaveChain take 5ms; with 30 commits
// and N=1, drops would surface within the test window if keyQueueCap
// were finite.
func TestSerialMode_NoDropErrorUnderBackpressure(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:serialnoback")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	const n = 30
	frames := buildVerifiedChainFrames(t, did, key, n)

	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		writeFrames(conn, frames...)
	})

	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	slowStore := &blockingStateStore{
		inner:     sync.NewMemStateStore(),
		saveDelay: 5 * time.Millisecond,
	}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: slowStore,
		Policy:     gt.Some(sync.PolicyError),
	})
	require.NoError(t, err)
	defer func() { _ = v.Close() }()

	client := mustNewClient(t, Options{
		URL:         wsURL(srv),
		Verifier:    gt.Some(v),
		Parallelism: gt.Some(1),
	})

	// Allow plenty of time for all 30 commits at 5ms each (~150ms) plus
	// scheduling slack.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		dropCount int
		creates   int
	)
	for batch, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			var de *DropError
			if stderrors.As(iterErr, &de) {
				dropCount++
			}
			continue
		}
		for _, evt := range batch {
			for op, opErr := range evt.Operations() {
				require.NoError(t, opErr)
				if op.Action == ActionCreate {
					creates++
				}
			}
		}
		if creates >= n {
			cancel()
		}
	}

	require.Equal(t, 0, dropCount,
		"Parallelism=1 must preserve backpressure semantics: no DropError under sustained slow verification")
	require.Equal(t, n, creates, "all commits must verify and surface as ops")
}

// TestSerialMode_CursorEqualsLastYieldedSeq pins the cursor semantic
// for Parallelism=1: at every flushBatch boundary, the persisted cursor
// equals the highest seq in the just-yielded batch. The watermark
// formula min(inflight)-1 used by the parallel path is mathematically
// equivalent at N=1 (only one event in flight at a time → after that
// event yields, inflight is empty → cursor falls back to max yielded
// seq), but this test pins the equivalence so the unification can't
// accidentally regress strict-mode users that rely on cursor exactness.
func TestSerialMode_CursorEqualsLastYieldedSeq(t *testing.T) {
	t.Parallel()

	const n = 10
	srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()
		for i := int64(1); i <= n; i++ {
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildFrame("#identity", buildIdentityBody(i, "did:plc:serialcur")))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:          wsURL(srv),
		BatchSize:    gt.Some(3),
		BatchTimeout: gt.Some(5 * time.Second),
		Parallelism:  gt.Some(1),
	})

	// Track every batch yielded along with the cursor visible *before*
	// the next yield (i.e. the cursor stored after the previous batch).
	var (
		batches      [][]Event
		cursorsAtTop []int64
		seen         int
	)
	for batch, iterErr := range client.Events(ctx) {
		if iterErr != nil {
			continue
		}
		cursorsAtTop = append(cursorsAtTop, client.Cursor())
		batches = append(batches, batch)
		seen += len(batch)
		if seen >= n {
			cancel()
		}
	}

	require.GreaterOrEqual(t, len(batches), 2, "need multiple batches to verify cursor advancement")
	// Cursor at the top of batch i (i>0) must equal the highest seq
	// from batch i-1.
	for i := 1; i < len(batches); i++ {
		prev := batches[i-1]
		highest := prev[len(prev)-1].Seq
		require.Equal(t, highest, cursorsAtTop[i],
			"cursor at top of yield %d (%d) must equal max seq of prior batch (%d)",
			i, cursorsAtTop[i], highest)
	}
	// After the iterator exits, the cursor reflects the last batch's
	// highest seq.
	final := batches[len(batches)-1]
	require.Equal(t, final[len(final)-1].Seq, client.Cursor(),
		"final cursor must equal max seq of final yielded batch")
}
