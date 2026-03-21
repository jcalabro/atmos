package repo

import (
	"bytes"
	"os"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------
// Loading from real CAR fixtures
// -------------------------------------------------------------------

func TestLoadGreenground(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/greenground.repo.car")
	require.NoError(t, err)

	repo, commit, err := LoadFromCAR(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, repo)
	require.NotNil(t, commit)

	// Verify known fields from the greenground fixture.
	require.Equal(t, "did:plc:kzcqyc3unb33eh5sxzsfs25z", commit.DID)
	require.True(t, commit.Data.Defined())
	require.NotEmpty(t, commit.Sig)
	require.Equal(t, string(repo.DID), commit.DID)

	// Walk the MST and verify records exist.
	var count int
	err = repo.Tree.Walk(func(key string, val cbor.CID) error {
		count++
		require.True(t, val.Defined(), "record CID should be defined for key %s", key)
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, count, 0, "expected at least one record")
}

func TestLoadRepoSlice(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/repo_slice.car")
	require.NoError(t, err)

	repo, commit, err := LoadFromCAR(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, repo)
	require.NotNil(t, commit)

	// Verify known DID from repo_slice fixture.
	require.Equal(t, "did:plc:6evlgoug7wwijzxhzt2riyic", commit.DID)

	// Verify the known record exists.
	_, recordData, err := repo.Get("app.bsky.feed.post", "3jquh3emtzo2o")
	require.NoError(t, err)
	require.NotEmpty(t, recordData)

	// Decode and verify it's a valid CBOR map with the expected type.
	val, err := cbor.Unmarshal(recordData)
	require.NoError(t, err)
	m, ok := val.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "app.bsky.feed.post", m["$type"])
}

// -------------------------------------------------------------------
// Commit signing and verification
// -------------------------------------------------------------------

func TestCommitSignAndVerify_P256(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}

	require.NoError(t, c.Sign(key))
	require.NotEmpty(t, c.Sig)
	require.Len(t, c.Sig, 64, "signature should be 64 bytes (compact R||S)")
	require.NoError(t, c.VerifySignature(key.PublicKey()))
}

func TestCommitSignAndVerify_K256(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateK256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}

	require.NoError(t, c.Sign(key))
	require.NoError(t, c.VerifySignature(key.PublicKey()))
}

func TestCommitVerify_WrongKey(t *testing.T) {
	t.Parallel()
	key1, err := crypto.GenerateP256()
	require.NoError(t, err)
	key2, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key1))

	// Verification with a different key should fail.
	err = c.VerifySignature(key2.PublicKey())
	require.Error(t, err)
}

func TestCommitVerify_TamperedData(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	// Tamper with the DID after signing.
	c.DID = "did:plc:tampered12345678901234"
	err = c.VerifySignature(key.PublicKey())
	require.Error(t, err)
}

func TestCommitVerify_NoSignature(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
	}
	err = c.VerifySignature(key.PublicKey())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no signature")
}

// -------------------------------------------------------------------
// Commit CBOR round-trip
// -------------------------------------------------------------------

func TestCommitRoundTrip(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	data, err := encodeCommit(c)
	require.NoError(t, err)

	decoded, err := decodeCommit(data)
	require.NoError(t, err)
	require.Equal(t, c.DID, decoded.DID)
	require.Equal(t, c.Version, decoded.Version)
	require.Equal(t, c.Rev, decoded.Rev)
	require.True(t, c.Data.Equal(decoded.Data))
	require.Equal(t, c.Sig, decoded.Sig)
	require.Nil(t, decoded.Prev)

	// The decoded commit should verify with the same key.
	require.NoError(t, decoded.VerifySignature(key.PublicKey()))
}

func TestCommitRoundTrip_WithPrev(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	prevCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("prev"))
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Prev:    &prevCID,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	data, err := encodeCommit(c)
	require.NoError(t, err)

	decoded, err := decodeCommit(data)
	require.NoError(t, err)
	require.NotNil(t, decoded.Prev)
	require.True(t, prevCID.Equal(*decoded.Prev))
	require.NoError(t, decoded.VerifySignature(key.PublicKey()))
}

func TestCommitEncoding_Deterministic(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	data1, err := encodeCommit(c)
	require.NoError(t, err)
	data2, err := encodeCommit(c)
	require.NoError(t, err)
	require.Equal(t, data1, data2, "commit encoding should be deterministic")
}

func TestCommitUnsignedBytes_ExcludesSig(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}

	unsigned1, err := c.UnsignedBytes()
	require.NoError(t, err)

	require.NoError(t, c.Sign(key))

	unsigned2, err := c.UnsignedBytes()
	require.NoError(t, err)

	// UnsignedBytes should be the same before and after signing.
	require.Equal(t, unsigned1, unsigned2)

	// The unsigned bytes should be valid DAG-CBOR.
	val, err := cbor.Unmarshal(unsigned1)
	require.NoError(t, err)
	m, ok := val.(map[string]any)
	require.True(t, ok)

	// Should have 5 fields (no sig).
	require.Len(t, m, 5)
	_, hasSig := m["sig"]
	require.False(t, hasSig, "unsigned bytes should not contain 'sig'")
}

// -------------------------------------------------------------------
// Repo CRUD
// -------------------------------------------------------------------

func TestRepoCRUD(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	repo := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	record := map[string]any{
		"$type": "app.bsky.feed.post",
		"text":  "hello world",
	}
	require.NoError(t, repo.Create("app.bsky.feed.post", "3jqfcqzm3fo2j", record))

	cid, data, err := repo.Get("app.bsky.feed.post", "3jqfcqzm3fo2j")
	require.NoError(t, err)
	require.True(t, cid.Defined())
	require.NotEmpty(t, data)

	// Decode and verify the stored record.
	val, err := cbor.Unmarshal(data)
	require.NoError(t, err)
	m, ok := val.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "hello world", m["text"])

	// Update.
	record["text"] = "updated"
	require.NoError(t, repo.Update("app.bsky.feed.post", "3jqfcqzm3fo2j", record))

	cid2, data2, err := repo.Get("app.bsky.feed.post", "3jqfcqzm3fo2j")
	require.NoError(t, err)
	require.False(t, cid.Equal(cid2), "CID should change after update")

	val2, err := cbor.Unmarshal(data2)
	require.NoError(t, err)
	m2, ok := val2.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "updated", m2["text"])

	// Delete.
	require.NoError(t, repo.Delete("app.bsky.feed.post", "3jqfcqzm3fo2j"))
	_, _, err = repo.Get("app.bsky.feed.post", "3jqfcqzm3fo2j")
	require.Error(t, err)
}

func TestRepoGet_NotFound(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	repo := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	_, _, err := repo.Get("app.bsky.feed.post", "nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestRepoMultipleCollections(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	repo := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	// Create records in different collections.
	require.NoError(t, repo.Create("app.bsky.feed.post", "tid1", map[string]any{"text": "post"}))
	require.NoError(t, repo.Create("app.bsky.feed.like", "tid2", map[string]any{"subject": "x"}))
	require.NoError(t, repo.Create("app.bsky.graph.follow", "tid3", map[string]any{"subject": "y"}))

	// All should be retrievable.
	_, _, err := repo.Get("app.bsky.feed.post", "tid1")
	require.NoError(t, err)
	_, _, err = repo.Get("app.bsky.feed.like", "tid2")
	require.NoError(t, err)
	_, _, err = repo.Get("app.bsky.graph.follow", "tid3")
	require.NoError(t, err)

	// Walk should show all 3 in sorted order.
	var keys []string
	err = repo.Tree.Walk(func(key string, _ cbor.CID) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, keys, 3)
	// Verify sorted order.
	for i := 1; i < len(keys); i++ {
		require.Less(t, keys[i-1], keys[i])
	}
}

// -------------------------------------------------------------------
// Export and reload
// -------------------------------------------------------------------

func TestExportAndReload(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	repo1 := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	for _, rkey := range []string{"3jqfcqzm3fo2j", "3jqfcqzm3fp2j", "3jqfcqzm3fq2j"} {
		require.NoError(t, repo1.Create("app.bsky.feed.post", rkey, map[string]any{
			"$type": "app.bsky.feed.post",
			"text":  "post " + rkey,
		}))
	}

	var buf bytes.Buffer
	require.NoError(t, repo1.ExportCAR(&buf, key))

	repo2, commit, err := LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, string(repo1.DID), commit.DID)
	require.Equal(t, int64(3), commit.Version)
	require.NotEmpty(t, commit.Rev)
	require.NotEmpty(t, commit.Sig)

	// Verify all records present.
	var count int
	err = repo2.Tree.Walk(func(key string, _ cbor.CID) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Verify signature.
	require.NoError(t, commit.VerifySignature(key.PublicKey()))

	// Verify each record is readable.
	for _, rkey := range []string{"3jqfcqzm3fo2j", "3jqfcqzm3fp2j", "3jqfcqzm3fq2j"} {
		_, data, err := repo2.Get("app.bsky.feed.post", rkey)
		require.NoError(t, err)
		require.NotEmpty(t, data)
	}
}

func TestExportAndReload_K256(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateK256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	repo1 := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	require.NoError(t, repo1.Create("app.bsky.feed.post", "3jqfcqzm3fo2j", map[string]any{
		"$type": "app.bsky.feed.post",
		"text":  "k256 test",
	}))

	var buf bytes.Buffer
	require.NoError(t, repo1.ExportCAR(&buf, key))

	_, commit, err := LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NoError(t, commit.VerifySignature(key.PublicKey()))
}

func TestExportAndReload_EmptyRepo(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	repo1 := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	var buf bytes.Buffer
	require.NoError(t, repo1.ExportCAR(&buf, key))

	repo2, commit, err := LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, string(repo1.DID), commit.DID)

	var count int
	err = repo2.Tree.Walk(func(_ string, _ cbor.CID) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestExportReload_ManyRecords(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	repo1 := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	// Create 50 records across multiple collections.
	clock := atmos.NewTIDClock(0)
	for i := range 50 {
		collection := "app.bsky.feed.post"
		if i%3 == 0 {
			collection = "app.bsky.feed.like"
		}
		rkey := clock.Next().String()
		require.NoError(t, repo1.Create(collection, rkey, map[string]any{
			"$type": collection,
			"text":  "record",
		}))
	}

	var buf bytes.Buffer
	require.NoError(t, repo1.ExportCAR(&buf, key))

	repo2, commit, err := LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NoError(t, commit.VerifySignature(key.PublicKey()))

	var count int
	err = repo2.Tree.Walk(func(_ string, _ cbor.CID) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 50, count)
}

// -------------------------------------------------------------------
// Record path validation
// -------------------------------------------------------------------

func TestRecordPathValidation_Create(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	r := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	record := map[string]any{"text": "test"}

	tests := []struct {
		name       string
		collection string
		rkey       string
	}{
		{"empty collection", "", "abc"},
		{"empty rkey", "app.bsky.feed.post", ""},
		{"dot rkey", "app.bsky.feed.post", "."},
		{"dotdot rkey", "app.bsky.feed.post", ".."},
		{"no slash (both empty-ish)", "noslash", ""},
		{"rkey with slash", "app.bsky.feed.post", "a/b"},
		{"collection with space", "app bsky.feed.post", "abc"},
		{"rkey with space", "app.bsky.feed.post", "a b"},
		{"rkey with hash", "app.bsky.feed.post", "a#b"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := r.Create(tc.collection, tc.rkey, record)
			require.Error(t, err, "Create(%q, %q) should fail", tc.collection, tc.rkey)
			require.Contains(t, err.Error(), "invalid record path")
		})
	}
}

func TestRecordPathValidation_Get(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	r := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	_, _, err := r.Get("", "abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid record path")

	_, _, err = r.Get("app.bsky.feed.post", "a/b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid record path")
}

func TestRecordPathValidation_Delete(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	r := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}

	err := r.Delete("", "abc")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid record path")
}

// -------------------------------------------------------------------
// Commit decode validation
// -------------------------------------------------------------------

func TestDecodeCommit_AcceptVersion2(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 2,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	data, err := encodeCommit(c)
	require.NoError(t, err)

	decoded, err := decodeCommit(data)
	require.NoError(t, err)
	require.Equal(t, int64(2), decoded.Version)
}

func TestDecodeCommit_RejectVersion1(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 1,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	data, err := encodeCommit(c)
	require.NoError(t, err)

	_, err = decodeCommit(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported commit version")
}

func TestDecodeCommit_RejectVersion999(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 999,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	require.NoError(t, c.Sign(key))

	data, err := encodeCommit(c)
	require.NoError(t, err)

	_, err = decodeCommit(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported commit version")
}

func TestDecodeCommit_RejectV3MissingRev(t *testing.T) {
	t.Parallel()
	// Build a v3 commit CBOR manually without the "rev" field.
	buf := make([]byte, 0, 128)
	buf = cbor.AppendMapHeader(buf, 4) // only 4 fields: did, sig, data, version (no rev)

	buf = cbor.AppendText(buf, "did")
	buf = cbor.AppendText(buf, "did:plc:testuser1234567890abcde")

	buf = cbor.AppendText(buf, "sig")
	buf = cbor.AppendBytes(buf, make([]byte, 64))

	dataCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	buf = cbor.AppendText(buf, "data")
	buf = cbor.AppendCIDLink(buf, &dataCID)

	buf = cbor.AppendText(buf, "version")
	buf = cbor.AppendUint(buf, 3)

	_, err := decodeCommit(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required 'rev'")
}

func TestDecodeCommit_RejectV3MissingSig(t *testing.T) {
	t.Parallel()
	// Build a v3 commit CBOR manually without the "sig" field.
	buf := make([]byte, 0, 128)
	buf = cbor.AppendMapHeader(buf, 4) // only 4 fields: did, rev, data, version (no sig)

	buf = cbor.AppendText(buf, "did")
	buf = cbor.AppendText(buf, "did:plc:testuser1234567890abcde")

	buf = cbor.AppendText(buf, "rev")
	buf = cbor.AppendText(buf, "3jqfcqzm3fo2j")

	dataCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	buf = cbor.AppendText(buf, "data")
	buf = cbor.AppendCIDLink(buf, &dataCID)

	buf = cbor.AppendText(buf, "version")
	buf = cbor.AppendUint(buf, 3)

	_, err := decodeCommit(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required 'sig'")
}

func TestDecodeCommit_V2AllowsOptionalRevAndSig(t *testing.T) {
	t.Parallel()
	// Build a v2 commit CBOR without rev and sig — should be accepted.
	buf := make([]byte, 0, 128)
	buf = cbor.AppendMapHeader(buf, 3) // only 3 fields: did, data, version

	buf = cbor.AppendText(buf, "did")
	buf = cbor.AppendText(buf, "did:plc:testuser1234567890abcde")

	dataCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	buf = cbor.AppendText(buf, "data")
	buf = cbor.AppendCIDLink(buf, &dataCID)

	buf = cbor.AppendText(buf, "version")
	buf = cbor.AppendUint(buf, 2)

	decoded, err := decodeCommit(buf)
	require.NoError(t, err)
	require.Equal(t, int64(2), decoded.Version)
	require.Empty(t, decoded.Rev)
	require.Nil(t, decoded.Sig)
}

func TestRecordPathValidation_ValidPathsAccepted(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	r := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	record := map[string]any{"text": "test"}

	// These should all succeed.
	require.NoError(t, r.Create("app.bsky.feed.post", "3jqfcqzm3fo2j", record))
	require.NoError(t, r.Create("com.example.record", "self", record))
	require.NoError(t, r.Create("app.bsky.feed.like", "abc~def", record))
	require.NoError(t, r.Create("app.bsky.graph.follow", "key-with-dashes", record))
	require.NoError(t, r.Create("app.bsky.actor.profile", "key_with_underscores", record))
}

// -------------------------------------------------------------------
// LoadBlocksFromCAR
// -------------------------------------------------------------------

func TestLoadBlocksFromCAR(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/greenground.repo.car")
	require.NoError(t, err)

	store, rootCID, err := LoadBlocksFromCAR(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, store)
	require.True(t, rootCID.Defined())

	// The root CID should be retrievable from the store.
	block, err := store.GetBlock(rootCID)
	require.NoError(t, err)
	require.NotEmpty(t, block)
}

func TestLoadBlocksFromCAR_RepoSlice(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/repo_slice.car")
	require.NoError(t, err)

	store, rootCID, err := LoadBlocksFromCAR(bytes.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, store)
	require.True(t, rootCID.Defined())

	block, err := store.GetBlock(rootCID)
	require.NoError(t, err)
	require.NotEmpty(t, block)
}

func TestLoadBlocksFromCAR_Invalid(t *testing.T) {
	t.Parallel()

	// Empty input.
	_, _, err := LoadBlocksFromCAR(bytes.NewReader(nil))
	require.Error(t, err)

	// Garbage input.
	_, _, err = LoadBlocksFromCAR(bytes.NewReader([]byte("not a car file")))
	require.Error(t, err)
}
