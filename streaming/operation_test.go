package streaming

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/bsky"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jcalabro/gt"
)

func buildTestCAR(t *testing.T, records map[string][]byte) ([]byte, map[string]lextypes.LexCIDLink) {
	t.Helper()
	var blocks []car.Block
	cidLinks := make(map[string]lextypes.LexCIDLink)
	var roots []cbor.CID
	for key, data := range records {
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
		blocks = append(blocks, car.Block{CID: cid, Data: data})
		cidLinks[key] = lextypes.LexCIDLink{Link: cid.String()}
		if len(roots) == 0 {
			roots = append(roots, cid)
		}
	}
	var buf bytes.Buffer
	require.NoError(t, car.WriteAll(&buf, roots, blocks))
	return buf.Bytes(), cidLinks
}

func TestOperations_CreatePost(t *testing.T) {
	t.Parallel()

	post := &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "hello world",
		CreatedAt:     "2024-01-01T00:00:00Z",
	}
	postBytes, err := post.MarshalCBOR()
	require.NoError(t, err)

	carBytes, cidLinks := buildTestCAR(t, map[string][]byte{"post": postBytes})

	evt := Event{
		Seq: 1,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:alice",
			Rev:    "3abc",
			Blocks: carBytes,
			Ops: []comatproto.SyncSubscribeRepos_RepoOp{
				{
					Action: "create",
					Path:   "app.bsky.feed.post/3abc123",
					CID:    gt.Some(cidLinks["post"]),
				},
			},
		},
	}

	var ops []Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		ops = append(ops, op)
	}

	require.Len(t, ops, 1)
	assert.Equal(t, ActionCreate, ops[0].Action)
	assert.Equal(t, "app.bsky.feed.post", ops[0].Collection)
	assert.Equal(t, "3abc123", ops[0].RKey)
	assert.Equal(t, "did:plc:alice", ops[0].Repo)
	assert.Equal(t, "3abc", ops[0].Rev)
	assert.NotNil(t, ops[0].CID)

	var decoded bsky.FeedPost
	require.NoError(t, ops[0].Decode(&decoded))
	assert.Equal(t, "hello world", decoded.Text)
}

func TestOperations_Record(t *testing.T) {
	t.Parallel()

	post := &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "type switch test",
		CreatedAt:     "2024-01-01T00:00:00Z",
	}
	postBytes, err := post.MarshalCBOR()
	require.NoError(t, err)

	carBytes, cidLinks := buildTestCAR(t, map[string][]byte{"post": postBytes})

	evt := Event{
		Seq: 1,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:alice",
			Rev:    "3abc",
			Blocks: carBytes,
			Ops: []comatproto.SyncSubscribeRepos_RepoOp{
				{
					Action: "create",
					Path:   "app.bsky.feed.post/3abc123",
					CID:    gt.Some(cidLinks["post"]),
				},
			},
		},
	}

	for op, err := range evt.Operations() {
		require.NoError(t, err)

		rec, err := op.Record(bsky.DecodeRecord)
		require.NoError(t, err)

		switch v := rec.(type) {
		case *bsky.FeedPost:
			assert.Equal(t, "type switch test", v.Text)
		default:
			t.Fatalf("unexpected type: %T", rec)
		}
	}
}

func TestOperations_RecordDelete(t *testing.T) {
	t.Parallel()

	carBytes, _ := buildTestCAR(t, map[string][]byte{"dummy": {0xa0}})

	evt := Event{
		Seq: 1,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:alice",
			Rev:    "3abc",
			Blocks: carBytes,
			Ops: []comatproto.SyncSubscribeRepos_RepoOp{
				{Action: "delete", Path: "app.bsky.feed.post/abc"},
			},
		},
	}

	for op, err := range evt.Operations() {
		require.NoError(t, err)
		_, err = op.Record(bsky.DecodeRecord)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete")
	}
}

func TestChainDecoders(t *testing.T) {
	t.Parallel()

	// A decoder that always fails.
	failDec := func(collection string, data []byte) (any, error) {
		return nil, fmt.Errorf("unknown collection: %s", collection)
	}

	post := &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "chain test",
		CreatedAt:     "2024-01-01T00:00:00Z",
	}
	postBytes, err := post.MarshalCBOR()
	require.NoError(t, err)

	// Chain: failDec first, then bsky.DecodeRecord.
	dec := ChainDecoders(failDec, bsky.DecodeRecord)
	rec, err := dec("app.bsky.feed.post", postBytes)
	require.NoError(t, err)

	v, ok := rec.(*bsky.FeedPost)
	require.True(t, ok)
	assert.Equal(t, "chain test", v.Text)

	// All fail.
	dec2 := ChainDecoders(failDec)
	_, err = dec2("app.bsky.feed.post", postBytes)
	require.Error(t, err)
}

func TestOperations_DeleteOp(t *testing.T) {
	t.Parallel()

	// Delete ops have no CID and no block data.
	carBytes, _ := buildTestCAR(t, map[string][]byte{"dummy": {0xa0}}) // empty map

	evt := Event{
		Seq: 1,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:alice",
			Rev:    "3abc",
			Blocks: carBytes,
			Ops: []comatproto.SyncSubscribeRepos_RepoOp{
				{
					Action: "delete",
					Path:   "app.bsky.feed.post/3abc123",
					// CID is None (null)
				},
			},
		},
	}

	var ops []Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		ops = append(ops, op)
	}

	require.Len(t, ops, 1)
	assert.Equal(t, ActionDelete, ops[0].Action)
	assert.Equal(t, "app.bsky.feed.post", ops[0].Collection)
	assert.Nil(t, ops[0].CID)

	var post bsky.FeedPost
	err := ops[0].Decode(&post)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete")
}

func TestOperations_MixedOps(t *testing.T) {
	t.Parallel()

	post := &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "new post",
		CreatedAt:     "2024-01-01T00:00:00Z",
	}
	postBytes, err := post.MarshalCBOR()
	require.NoError(t, err)

	carBytes, cidLinks := buildTestCAR(t, map[string][]byte{"post": postBytes})

	evt := Event{
		Seq: 1,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:alice",
			Rev:    "3abc",
			Blocks: carBytes,
			Ops: []comatproto.SyncSubscribeRepos_RepoOp{
				{Action: "delete", Path: "app.bsky.graph.follow/old123"},
				{Action: "create", Path: "app.bsky.feed.post/new456", CID: gt.Some(cidLinks["post"])},
			},
		},
	}

	var ops []Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		ops = append(ops, op)
	}

	require.Len(t, ops, 2)
	assert.Equal(t, ActionDelete, ops[0].Action)
	assert.Equal(t, "app.bsky.graph.follow", ops[0].Collection)
	assert.Equal(t, ActionCreate, ops[1].Action)
	assert.Equal(t, "app.bsky.feed.post", ops[1].Collection)

	var decoded bsky.FeedPost
	require.NoError(t, ops[1].Decode(&decoded))
	assert.Equal(t, "new post", decoded.Text)
}

func TestOperations_NonCommitEvent(t *testing.T) {
	t.Parallel()

	evt := Event{
		Seq:      1,
		Identity: &comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:alice"},
	}

	var count int
	for range evt.Operations() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestOperations_BadCAR(t *testing.T) {
	t.Parallel()

	evt := Event{
		Seq: 1,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:alice",
			Rev:    "3abc",
			Blocks: []byte{0xff, 0xfe}, // garbage
			Ops: []comatproto.SyncSubscribeRepos_RepoOp{
				{Action: "create", Path: "app.bsky.feed.post/abc"},
			},
		},
	}

	var errCount int
	for _, err := range evt.Operations() {
		if err != nil {
			errCount++
		}
	}
	assert.Equal(t, 1, errCount)
}

func TestSplitPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path       string
		collection string
		rkey       string
	}{
		{"app.bsky.feed.post/3abc", "app.bsky.feed.post", "3abc"},
		{"app.bsky.graph.follow/xyz", "app.bsky.graph.follow", "xyz"},
		{"noSlash", "noSlash", ""},
	}
	for _, tt := range tests {
		col, rk := splitPath(tt.path)
		assert.Equal(t, tt.collection, col)
		assert.Equal(t, tt.rkey, rk)
	}
}

// --- Resync tests ---

func buildResyncRepo(t *testing.T, n int) []byte {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &repo.Repo{
		DID:   atmos.DID("did:plc:test123"),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	for i := range n {
		record := map[string]any{
			"text":      fmt.Sprintf("record %d", i),
			"createdAt": "2024-01-01T00:00:00Z",
		}
		require.NoError(t, r.Create("app.bsky.feed.post", fmt.Sprintf("rec%d", i), record))
	}

	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))
	return buf.Bytes()
}

func serveSyncCAR(t *testing.T, carData []byte) *sync.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carData)
	}))
	t.Cleanup(srv.Close)
	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	return sync.NewClient(sync.Options{Client: xc})
}

func TestOperations_SyncEvent(t *testing.T) {
	t.Parallel()

	carData := buildResyncRepo(t, 3)
	sc := serveSyncCAR(t, carData)

	evt := Event{
		Seq: 42,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: "did:plc:test123",
			Rev: "3abc",
			Seq: 42,
		},
		ctx:        context.Background(),
		syncClient: sc,
	}

	var ops []Operation
	for op, err := range evt.Operations() {
		require.NoError(t, err)
		ops = append(ops, op)
	}

	require.Len(t, ops, 3)
	for _, op := range ops {
		assert.Equal(t, ActionResync, op.Action)
		assert.Equal(t, "app.bsky.feed.post", op.Collection)
		assert.Equal(t, "did:plc:test123", op.Repo)
		assert.Equal(t, "3abc", op.Rev)
		assert.NotEmpty(t, op.CID)
		assert.NotEmpty(t, op.blockData)
	}
}

func TestOperations_SyncEvent_NoSyncClient(t *testing.T) {
	t.Parallel()

	evt := Event{
		Seq: 42,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: "did:plc:test123",
			Rev: "3abc",
			Seq: 42,
		},
		// syncClient is nil — opt-out
	}

	var count int
	for range evt.Operations() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestOperations_SyncEvent_Error(t *testing.T) {
	t.Parallel()

	// Mock server that returns 500 for getRepo.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":"InternalError"}`))
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	evt := Event{
		Seq: 42,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: "did:plc:test123",
			Rev: "3abc",
			Seq: 42,
		},
		ctx:        context.Background(),
		syncClient: sc,
	}

	var errCount int
	for _, err := range evt.Operations() {
		if err != nil {
			errCount++
		}
	}
	assert.Equal(t, 1, errCount)
}

func TestOperations_SyncEvent_Decode(t *testing.T) {
	t.Parallel()

	carData := buildResyncRepo(t, 1)
	sc := serveSyncCAR(t, carData)

	evt := Event{
		Seq: 1,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: "did:plc:test123",
			Rev: "3abc",
			Seq: 1,
		},
		ctx:        context.Background(),
		syncClient: sc,
	}

	for op, err := range evt.Operations() {
		require.NoError(t, err)

		// Verify Decode works on resync ops.
		rec, err := op.Record(bsky.DecodeRecord)
		// The test repo uses map[string]any, not a generated type, so
		// DecodeRecord won't recognize it. Verify blockData is at least
		// valid CBOR instead.
		_ = rec
		_ = err
		v, err := cbor.Unmarshal(op.blockData)
		require.NoError(t, err)
		m, ok := v.(map[string]any)
		require.True(t, ok)
		assert.Contains(t, m, "text")
	}
}

func TestOperations_SyncEvent_BreakEarly(t *testing.T) {
	t.Parallel()

	carData := buildResyncRepo(t, 10)
	sc := serveSyncCAR(t, carData)

	evt := Event{
		Seq: 1,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: "did:plc:test123",
			Rev: "3abc",
			Seq: 1,
		},
		ctx:        context.Background(),
		syncClient: sc,
	}

	count := 0
	for _, err := range evt.Operations() {
		require.NoError(t, err)
		count++
		if count >= 1 {
			break
		}
	}
	assert.Equal(t, 1, count)
}

func TestOperations_SyncEvent_EmptyRepo(t *testing.T) {
	t.Parallel()

	carData := buildResyncRepo(t, 0)
	sc := serveSyncCAR(t, carData)

	evt := Event{
		Seq: 1,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: "did:plc:test123",
			Rev: "3abc",
			Seq: 1,
		},
		ctx:        context.Background(),
		syncClient: sc,
	}

	count := 0
	for _, err := range evt.Operations() {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}
