// Package testutil provides shared test helpers for atmos sync 1.1
// verifier tests. Used by sync_test and streaming_test packages.
package testutil

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// Action constants mirror streaming.Action without creating an import
// cycle (streaming tests import this package). Values match exactly
// (and match the on-the-wire RepoOp.Action strings).
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
)

// OpAction describes one record mutation for BuildSyntheticCommit.
// Action is one of the ActionCreate/ActionUpdate/ActionDelete constants
// (or any string equal to streaming.Action's underlying string).
type OpAction struct {
	Action     string // "create", "update", "delete"
	Collection string
	RKey       string
	Record     any // ignored for delete
}

// CommitBuildResult is the byproduct of BuildAndStoreSignedCommit.
type CommitBuildResult struct {
	CID cbor.CID
	Rev string
}

// InnerCommitDataCID decodes the commit block referenced by the
// firehose-frame commit's `Commit` link from the CAR diff in `Blocks`
// and returns the decoded inner commit's Data field (the post-state
// MST root CID). Returns (CID{}, false) on any decode failure —
// callers should treat that as "data CID unavailable, do not advance
// state."
func InnerCommitDataCID(commit *comatproto.SyncSubscribeRepos_Commit) (cbor.CID, bool) {
	store, _, err := repo.LoadBlocksFromCAR(bytes.NewReader(commit.Blocks))
	if err != nil {
		return cbor.CID{}, false
	}
	commitCID, err := cbor.ParseCIDString(commit.Commit.Link)
	if err != nil {
		return cbor.CID{}, false
	}
	data, err := store.GetBlock(commitCID)
	if err != nil {
		return cbor.CID{}, false
	}
	c, err := repo.DecodeCommitCBOR(data)
	if err != nil {
		return cbor.CID{}, false
	}
	return c.Data, true
}

// BuildEmptyRepo returns a freshly created Repo and the CID of its
// (empty) MST root. The empty MST has a well-defined root CID
// regardless of which key signs it.
//
// Accepts testing.TB so benchmarks can reuse the helper.
func BuildEmptyRepo(t testing.TB, did atmos.DID) (*repo.Repo, cbor.CID) {
	t.Helper()
	store := mst.NewMemBlockStore()
	r := &repo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	rootCID, err := r.Tree.RootCID()
	require.NoError(t, err)
	return r, rootCID
}

// BuildAndStoreSignedCommit creates and signs a commit pointing at
// rootCID, stores the encoded commit block in r.Store, and returns
// the commit's CID + rev.
func BuildAndStoreSignedCommit(r *repo.Repo, key crypto.PrivateKey, rootCID cbor.CID) (CommitBuildResult, error) {
	rev := r.Clock.Next()
	c := &repo.Commit{
		DID:     string(r.DID),
		Version: 3,
		Data:    rootCID,
		Rev:     string(rev),
	}
	if err := c.Sign(key); err != nil {
		return CommitBuildResult{}, err
	}
	data, err := c.EncodeCBOR()
	if err != nil {
		return CommitBuildResult{}, err
	}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	if err := r.Store.PutBlock(cid, data); err != nil {
		return CommitBuildResult{}, err
	}
	return CommitBuildResult{CID: cid, Rev: string(rev)}, nil
}

// BuildSyntheticCommit applies ops to r in order, then constructs a
// synthetic SyncSubscribeRepos_Commit whose Blocks CAR contains the
// post-state MST blocks plus all pre-state nodes the inverter will
// need (i.e., everything currently in r.Store). The commit is signed
// with key. PrevData is set to prevData.
//
// Accepts testing.TB so benchmarks can reuse the helper.
func BuildSyntheticCommit(t testing.TB, r *repo.Repo, key crypto.PrivateKey, prevData cbor.CID, ops []OpAction) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()

	// Capture pre-state record CIDs for update/delete ops before
	// mutating the tree.
	type prevSnap struct {
		cid cbor.CID
		had bool
	}
	prevSnaps := make([]prevSnap, len(ops))
	for i, op := range ops {
		if op.Action == ActionUpdate || op.Action == ActionDelete {
			cid, _, err := r.Get(op.Collection, op.RKey)
			require.NoError(t, err, "pre-state Get %s/%s", op.Collection, op.RKey)
			prevSnaps[i] = prevSnap{cid: cid, had: true}
		}
	}

	// Apply ops to the live tree.
	postCIDs := make([]cbor.CID, len(ops))
	for i, op := range ops {
		switch op.Action {
		case ActionCreate, ActionUpdate:
			require.NoError(t, r.Create(op.Collection, op.RKey, op.Record))
			cid, _, err := r.Get(op.Collection, op.RKey)
			require.NoError(t, err)
			postCIDs[i] = cid
		case ActionDelete:
			require.NoError(t, r.Delete(op.Collection, op.RKey))
		default:
			t.Fatalf("unsupported op action %q", op.Action)
		}
	}

	// Compute the post-state root and persist all touched MST nodes.
	postRoot, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)

	// Build and store the signed commit block.
	commitBlock, err := BuildAndStoreSignedCommit(r, key, postRoot)
	require.NoError(t, err)

	// Write CAR. Iterate every block in r.Store; the inverter only
	// strictly needs the nodes touched by the inverse path, but
	// dumping the whole store is always sufficient.
	memStore, ok := r.Store.(*mst.MemBlockStore)
	require.True(t, ok, "test setup expected MemBlockStore")

	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{commitBlock.CID})
	require.NoError(t, err)
	for cid, data := range memStore.All() {
		require.NoError(t, cw.WriteBlock(cid, data))
	}

	c := &comatproto.SyncSubscribeRepos_Commit{
		Repo:     string(r.DID),
		Rev:      commitBlock.Rev,
		Commit:   lextypes.LexCIDLink{Link: commitBlock.CID.String()},
		Blocks:   carBuf.Bytes(),
		PrevData: gt.Some(lextypes.LexCIDLink{Link: prevData.String()}),
	}
	for i, op := range ops {
		repoOp := comatproto.SyncSubscribeRepos_RepoOp{
			Action: op.Action,
			Path:   op.Collection + "/" + op.RKey,
		}
		if op.Action != ActionDelete {
			repoOp.CID = gt.Some(lextypes.LexCIDLink{Link: postCIDs[i].String()})
		}
		if prevSnaps[i].had {
			repoOp.Prev = gt.Some(lextypes.LexCIDLink{Link: prevSnaps[i].cid.String()})
		}
		c.Ops = append(c.Ops, repoOp)
	}
	return c
}

// BuildSyncEventBlocks constructs the `Blocks` field for a
// SyncSubscribeRepos_Sync event: a CAR file containing exactly one
// block (the signed commit) with the commit CID as the CAR root.
// This matches what a 1.1-compliant PDS emits — the event's CAR
// intentionally carries no MST nodes or records, just the commit.
//
// The returned bytes are ready to assign to syncEvt.Blocks.
func BuildSyncEventBlocks(t *testing.T, r *repo.Repo, key crypto.PrivateKey, dataCID cbor.CID) []byte {
	t.Helper()
	commitBlock, err := BuildAndStoreSignedCommit(r, key, dataCID)
	require.NoError(t, err)
	commitBytes, err := r.Store.GetBlock(commitBlock.CID)
	require.NoError(t, err)

	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{commitBlock.CID})
	require.NoError(t, err)
	require.NoError(t, cw.WriteBlock(commitBlock.CID, commitBytes))
	return carBuf.Bytes()
}

// BuildDIDDoc constructs a minimal DID document for did with the
// given signing key as the "atproto" verification method.
func BuildDIDDoc(did atmos.DID, key crypto.PublicKey) *identity.DIDDocument {
	return &identity.DIDDocument{
		ID: string(did),
		VerificationMethod: []identity.VerificationMethod{{
			ID:                 string(did) + "#atproto",
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: key.Multibase(),
		}},
	}
}

// TrackingResolver is an in-memory identity.Resolver that tracks how
// many times each DID was resolved.
type TrackingResolver struct {
	Docs        map[atmos.DID]*identity.DIDDocument
	ResolveHits map[atmos.DID]int
	mu          stdsync.Mutex
}

// NewTrackingResolver returns a fresh TrackingResolver with empty maps.
func NewTrackingResolver() *TrackingResolver {
	return &TrackingResolver{
		Docs:        make(map[atmos.DID]*identity.DIDDocument),
		ResolveHits: make(map[atmos.DID]int),
	}
}

// ResolveDID implements identity.Resolver.
func (r *TrackingResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ResolveHits[did]++
	doc, ok := r.Docs[did]
	if !ok {
		return nil, fmt.Errorf("not found: %s", did)
	}
	return doc, nil
}

// ResolveHandle implements identity.Resolver. Always returns "not implemented".
func (r *TrackingResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", fmt.Errorf("not implemented")
}

// NewFakeSyncServerMulti returns an *xrpc.Client whose getRepo
// endpoint dispatches to a caller-provided lookup. carFor is invoked
// per request with the requested DID; returning ok=false yields a
// 404 to the caller. Useful when a single test exercises many DIDs
// or when the served CAR changes over time (e.g. a swarm test where
// the verifier's idea of "current state" advances per event).
//
// carFor MUST be safe for concurrent use; the verifier's per-DID
// resync work runs on whichever goroutine triggered the fault.
func NewFakeSyncServerMulti(t testing.TB, carFor func(atmos.DID) ([]byte, bool)) *xrpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.getRepo" {
			w.WriteHeader(404)
			return
		}
		did := atmos.DID(r.URL.Query().Get("did"))
		carBytes, ok := carFor(did)
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carBytes)
	}))
	t.Cleanup(srv.Close)
	return &xrpc.Client{
		Host:  srv.URL,
		Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
}

// NewFakeSyncServer returns an *xrpc.Client whose getRepo endpoint
// returns the given CAR bytes for the matching DID.
func NewFakeSyncServer(t *testing.T, did atmos.DID, carBytes []byte) *xrpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.getRepo":
			if r.URL.Query().Get("did") != string(did) {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(carBytes)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return &xrpc.Client{
		Host:  srv.URL,
		Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
}
