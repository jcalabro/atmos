package sync_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTestRepo creates a repo with n records and returns its CAR bytes.
func buildTestRepo(t *testing.T, n int) ([]byte, crypto.PrivateKey) {
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
		err := r.Create("app.bsky.feed.post", fmt.Sprintf("rec%d", i), record)
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))
	return buf.Bytes(), key
}

func serveCAR(t *testing.T, carData []byte) (*httptest.Server, *xrpc.Client) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.getRepo":
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(carData)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
}

func TestIterRecords_SmallRepo(t *testing.T) {
	t.Parallel()

	carData, _ := buildTestRepo(t, 5)
	_, xc := serveCAR(t, carData)

	sc := sync.NewClient(sync.Options{Client: xc})
	var records []sync.Record
	for rec, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
		require.NoError(t, err)
		records = append(records, rec)
	}

	assert.Len(t, records, 5)
	for _, rec := range records {
		assert.Equal(t, "app.bsky.feed.post", rec.Collection)
		assert.NotEmpty(t, rec.RKey)
		assert.NotEmpty(t, rec.Data)
		assert.True(t, rec.CID.Defined())
		assert.NotEmpty(t, rec.Rev, "Rev should be populated from commit")
	}
}

func TestIterRecords_LargeRepo(t *testing.T) {
	t.Parallel()

	carData, _ := buildTestRepo(t, 5000)
	_, xc := serveCAR(t, carData)

	sc := sync.NewClient(sync.Options{Client: xc})
	count := 0
	for _, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
		require.NoError(t, err)
		count++
	}

	assert.Equal(t, 5000, count)
}

func TestIterRecords_BreakEarly(t *testing.T) {
	t.Parallel()

	carData, _ := buildTestRepo(t, 100)
	_, xc := serveCAR(t, carData)

	sc := sync.NewClient(sync.Options{Client: xc})
	count := 0
	for _, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
		require.NoError(t, err)
		count++
		if count >= 1 {
			break
		}
	}
	assert.Equal(t, 1, count)
}

func TestIterRecords_EmptyRepo(t *testing.T) {
	t.Parallel()

	carData, _ := buildTestRepo(t, 0)
	_, xc := serveCAR(t, carData)

	sc := sync.NewClient(sync.Options{Client: xc})
	count := 0
	for _, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestIterRecords_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "InternalError"})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	for _, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
		require.Error(t, err)
		break
	}
}

func TestGetLatestCommit(t *testing.T) {
	t.Parallel()

	// Compute a valid CID to use in the mock response.
	testCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte{0xa0}) // empty CBOR map
	cidStr := testCID.String()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/xrpc/com.atproto.sync.getLatestCommit", r.URL.Path)
		assert.Equal(t, "did:plc:test123", r.URL.Query().Get("did"))
		_ = json.NewEncoder(w).Encode(map[string]string{
			"rev": "3jqfcqzm3fp2j",
			"cid": cidStr,
		})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	rev, cid, err := sc.GetLatestCommit(context.Background(), "did:plc:test123")
	require.NoError(t, err)
	assert.Equal(t, "3jqfcqzm3fp2j", rev)
	assert.True(t, cid.Defined())
	assert.True(t, cid.Equal(testCID))
}

type listReposPage struct {
	Cursor string          `json:"cursor,omitempty"`
	Repos  []listReposRepo `json:"repos"`
}

type listReposRepo struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

func TestListRepos_Pagination(t *testing.T) {
	t.Parallel()

	pages := []listReposPage{
		{
			Cursor: "cursor1",
			Repos: []listReposRepo{
				{DID: "did:plc:aaa", Head: "bafyaaa", Rev: "rev1", Active: true},
				{DID: "did:plc:bbb", Head: "bafybbb", Rev: "rev2", Active: true},
			},
		},
		{
			Cursor: "cursor2",
			Repos: []listReposRepo{
				{DID: "did:plc:ccc", Head: "bafyccc", Rev: "rev3", Active: true},
			},
		},
		{
			Repos: []listReposRepo{
				{DID: "did:plc:ddd", Head: "bafyddd", Rev: "rev4", Active: true},
			},
		},
	}

	callIdx := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		if callIdx < len(pages) {
			_ = json.NewEncoder(w).Encode(pages[callIdx])
			callIdx++
		} else {
			_ = json.NewEncoder(w).Encode(listReposPage{})
		}
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var entries []sync.ListReposEntry
	for entry, err := range sc.ListRepos(context.Background()) {
		require.NoError(t, err)
		entries = append(entries, entry)
	}

	assert.Len(t, entries, 4)
	assert.Equal(t, atmos.DID("did:plc:aaa"), entries[0].DID)
	assert.Equal(t, atmos.DID("did:plc:ddd"), entries[3].DID)
}

func TestListRepos_Empty(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(listReposPage{})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	count := 0
	for _, err := range sc.ListRepos(context.Background()) {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestSplitKey(t *testing.T) {
	t.Parallel()

	// Test via IterRecords — splitKey is internal but exercised through record iteration.
	carData, _ := buildTestRepo(t, 1)
	_, xc := serveCAR(t, carData)

	sc := sync.NewClient(sync.Options{Client: xc})
	for rec, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
		require.NoError(t, err)
		assert.Equal(t, "app.bsky.feed.post", rec.Collection)
		assert.Equal(t, "rec0", rec.RKey)
		break
	}
}

// TestVerifyCommit_Valid tests commit verification with a real key.
func TestVerifyCommit_Valid(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &repo.Repo{
		DID:   atmos.DID("did:plc:test123"),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	commit, err := r.Commit(key)
	require.NoError(t, err)

	pubkey := key.PublicKey()
	require.NoError(t, commit.VerifySignature(pubkey))
}

// TestVerifyCommit_Invalid tests that a tampered signature fails.
func TestVerifyCommit_Invalid(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &repo.Repo{
		DID:   atmos.DID("did:plc:test123"),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	commit, err := r.Commit(key)
	require.NoError(t, err)

	// Tamper with signature.
	commit.Sig[0] ^= 0xff

	pubkey := key.PublicKey()
	err = commit.VerifySignature(pubkey)
	require.Error(t, err)
}

// --- VerifyCommit via sync.Client ---

type fakeResolver struct {
	doc *identity.DIDDocument
	err error
}

func (f *fakeResolver) ResolveDID(_ context.Context, _ atmos.DID) (*identity.DIDDocument, error) {
	return f.doc, f.err
}

func (f *fakeResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", errors.New("not implemented")
}

func TestClientVerifyCommit_NoDirectory(t *testing.T) {
	t.Parallel()
	sc := sync.NewClient(sync.Options{}) // no Directory
	commit := &repo.Commit{DID: "did:plc:test123"}
	err := sc.VerifyCommit(context.Background(), commit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no directory configured")
}

func TestClientVerifyCommit_InvalidDID(t *testing.T) {
	t.Parallel()
	dir := &identity.Directory{Resolver: &fakeResolver{}}
	sc := sync.NewClient(sync.Options{Directory: gt.Some(dir)})
	commit := &repo.Commit{DID: "notadid"}
	err := sc.VerifyCommit(context.Background(), commit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid DID")
}

func TestClientVerifyCommit_LookupFails(t *testing.T) {
	t.Parallel()
	dir := &identity.Directory{Resolver: &fakeResolver{err: errors.New("network error")}}
	sc := sync.NewClient(sync.Options{Directory: gt.Some(dir)})
	commit := &repo.Commit{DID: "did:plc:test123"}
	err := sc.VerifyCommit(context.Background(), commit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolving DID")
}

func TestClientVerifyCommit_NoKey(t *testing.T) {
	t.Parallel()
	// DID doc with no verification methods.
	dir := &identity.Directory{Resolver: &fakeResolver{doc: &identity.DIDDocument{
		ID: "did:plc:test123",
	}}}
	sc := sync.NewClient(sync.Options{Directory: gt.Some(dir)})

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	store := mst.NewMemBlockStore()
	r := &repo.Repo{DID: "did:plc:test123", Clock: atmos.NewTIDClock(0), Store: store, Tree: mst.NewTree(store)}
	commit, err := r.Commit(key)
	require.NoError(t, err)

	err = sc.VerifyCommit(context.Background(), commit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting public key")
}

func TestClientVerifyCommit_BadSignature(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	p256pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	multibase := p256pub.DIDKey()[8:] // strip "did:key:" prefix

	dir := &identity.Directory{Resolver: &fakeResolver{doc: &identity.DIDDocument{
		ID: "did:plc:test123",
		VerificationMethod: []identity.VerificationMethod{
			{ID: "#atproto", Type: "Multikey", Controller: "did:plc:test123", PublicKeyMultibase: multibase},
		},
	}}}
	sc := sync.NewClient(sync.Options{Directory: gt.Some(dir)})

	store := mst.NewMemBlockStore()
	r := &repo.Repo{DID: "did:plc:test123", Clock: atmos.NewTIDClock(0), Store: store, Tree: mst.NewTree(store)}
	commit, err := r.Commit(key)
	require.NoError(t, err)

	// Tamper with signature.
	commit.Sig[0] ^= 0xff

	err = sc.VerifyCommit(context.Background(), commit)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signature verification failed")
}

func TestClientVerifyCommit_Valid(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	p256pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)
	multibase := p256pub.DIDKey()[8:]

	dir := &identity.Directory{Resolver: &fakeResolver{doc: &identity.DIDDocument{
		ID: "did:plc:test123",
		VerificationMethod: []identity.VerificationMethod{
			{ID: "#atproto", Type: "Multikey", Controller: "did:plc:test123", PublicKeyMultibase: multibase},
		},
	}}}
	sc := sync.NewClient(sync.Options{Directory: gt.Some(dir)})

	store := mst.NewMemBlockStore()
	r := &repo.Repo{DID: "did:plc:test123", Clock: atmos.NewTIDClock(0), Store: store, Tree: mst.NewTree(store)}
	commit, err := r.Commit(key)
	require.NoError(t, err)

	require.NoError(t, sc.VerifyCommit(context.Background(), commit))
}

// --- ListRepos and GetLatestCommit gap tests ---

func TestListRepos_ContextCanceled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(listReposPage{
			Repos: []listReposRepo{{DID: "did:plc:aaa", Head: "bafyaaa", Rev: "rev1", Active: true}},
		})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before iterating

	count := 0
	for range sc.ListRepos(ctx) {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestListRepos_InvalidDID(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(listReposPage{
			Repos: []listReposRepo{
				{DID: "not-a-did", Head: "bafyaaa", Rev: "rev1", Active: true},
				{DID: "did:plc:valid1234567890abcde", Head: "bafybbb", Rev: "rev2", Active: true},
			},
		})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	var gotError, gotEntry bool
	for entry, err := range sc.ListRepos(context.Background()) {
		if err != nil {
			gotError = true
			continue
		}
		if entry.DID == "did:plc:valid1234567890abcde" {
			gotEntry = true
		}
	}
	assert.True(t, gotError, "should have received an error for invalid DID")
	assert.True(t, gotEntry, "should have received the valid entry")
}

func TestListRepos_BreakEarly(t *testing.T) {
	t.Parallel()
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		_ = json.NewEncoder(w).Encode(listReposPage{
			Cursor: "more",
			Repos: []listReposRepo{
				{DID: "did:plc:aaa", Head: "bafyaaa", Rev: "rev1", Active: true},
				{DID: "did:plc:bbb", Head: "bafybbb", Rev: "rev2", Active: true},
			},
		})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	count := 0
	for _, err := range sc.ListRepos(context.Background()) {
		require.NoError(t, err)
		count++
		if count >= 1 {
			break
		}
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, 1, callCount, "should only have fetched one page")
}

func TestGetLatestCommit_InvalidCID(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"rev": "3jqfcqzm3fp2j",
			"cid": "not-a-valid-cid",
		})
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
	sc := sync.NewClient(sync.Options{Client: xc})

	_, _, err := sc.GetLatestCommit(context.Background(), "did:plc:test123")
	require.Error(t, err)
}

// BenchmarkIterRecords benchmarks record iteration over repos of various sizes.
func BenchmarkIterRecords(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			key, err := crypto.GenerateP256()
			require.NoError(b, err)

			store := mst.NewMemBlockStore()
			r := &repo.Repo{
				DID:   atmos.DID("did:plc:test123"),
				Clock: atmos.NewTIDClock(0),
				Store: store,
				Tree:  mst.NewTree(store),
			}
			for i := range size {
				record := map[string]any{"text": fmt.Sprintf("record %d", i)}
				require.NoError(b, r.Create("app.bsky.feed.post", fmt.Sprintf("rec%d", i), record))
			}
			var buf bytes.Buffer
			require.NoError(b, r.ExportCAR(&buf, key))
			carData := buf.Bytes()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
				_, _ = w.Write(carData)
			}))
			defer srv.Close()

			xc := &xrpc.Client{Host: srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
			sc := sync.NewClient(sync.Options{Client: xc})

			b.ResetTimer()
			for b.Loop() {
				for _, err := range sc.IterRecords(context.Background(), "did:plc:test123") {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
