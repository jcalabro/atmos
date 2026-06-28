package repo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

// buildTestRepo creates an in-memory repo with n records spread across a few
// collections, large enough that its MST has interior nodes (so a boundary
// truncation can sever an interior node as well as a leaf record block). It
// returns the repo and the private key it was signed with.
func buildTestRepo(t *testing.T, n int) (*Repo, crypto.PrivateKey) {
	t.Helper()
	store := mst.NewMemBlockStore()
	rp := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"}
	for i := range n {
		coll := colls[i%len(colls)]
		rkey := string(atmos.NewTID(int64(2000)*3_600_000_000+int64(i), uint(i%1024)))
		rec := map[string]any{
			"$type":     coll,
			"text":      fmt.Sprintf("record number %d with some padding text to grow blocks", i),
			"createdAt": "2023-11-14T18:13:20.000Z",
			"seq":       i,
		}
		require.NoError(t, rp.Create(coll, rkey, rec))
	}
	key, err := crypto.GenerateK256()
	require.NoError(t, err)
	return rp, key
}

// TestCheckComplete_FullRepoPasses: a complete repo (exported + reloaded)
// passes CheckComplete and LoadCompleteFromCAR.
func TestCheckComplete_FullRepoPasses(t *testing.T) {
	t.Parallel()
	rp, key := buildTestRepo(t, 50)

	var buf bytes.Buffer
	require.NoError(t, rp.ExportCAR(&buf, key))

	loaded, commit, err := LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NoError(t, loaded.CheckComplete(), "complete repo must pass CheckComplete")
	require.True(t, commit.Data.Defined())

	loaded2, _, err := LoadCompleteFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NotNil(t, loaded2)
}

// TestCheckComplete_EmptyRepo: a repo with no records (and a nil/empty tree)
// is trivially complete.
func TestCheckComplete_EmptyRepo(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	rp := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	require.NoError(t, rp.CheckComplete())
}

// TestCheckComplete_MissingLeafRecordBlock: deleting a single record's block
// from the store (leaving the MST referencing it) is detected, classified as a
// transient truncation (io.ErrUnexpectedEOF), and matchable as
// mst.ErrBlockNotFound.
func TestCheckComplete_MissingLeafRecordBlock(t *testing.T) {
	t.Parallel()
	rp, _ := buildTestRepo(t, 30)

	// Find a leaf record CID and drop its block from a fresh store that
	// otherwise mirrors rp's blocks. We rebuild via CAR so we control the
	// MemBlockStore contents precisely.
	var buf bytes.Buffer
	key, err := crypto.GenerateK256()
	require.NoError(t, err)
	require.NoError(t, rp.ExportCAR(&buf, key))
	loaded, _, err := LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Pick the first record block CID via a walk.
	var victim cbor.CID
	require.NoError(t, loaded.Tree.Walk(func(_ string, val cbor.CID) error {
		if !victim.Defined() {
			victim = val
		}
		return nil
	}))
	require.True(t, victim.Defined())

	// Reload into a store with the victim block removed.
	partial := newDroppingStore(t, buf.Bytes(), victim)
	loaded.Store = partial

	err = loaded.CheckComplete()
	require.Error(t, err)
	require.ErrorIs(t, err, mst.ErrBlockNotFound, "must wrap the matchable sentinel")
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "must classify as a transient truncation")
	require.Contains(t, err.Error(), "incomplete CAR")
}

// TestCheckComplete_MissingInteriorMSTNode: dropping an interior MST node
// block (not a leaf record) is also detected and classified transient. This is
// the exact failure the oracle hit: a boundary-truncated CAR that severed an
// interior node, which previously surfaced as a NON-transient walk error.
func TestCheckComplete_MissingInteriorMSTNode(t *testing.T) {
	t.Parallel()
	rp, key := buildTestRepo(t, 200) // big enough to have interior nodes

	var buf bytes.Buffer
	require.NoError(t, rp.ExportCAR(&buf, key))

	// LoadBlocksFromCAR returns the CAR root (the COMMIT CID); the MST data
	// root is commit.Data, which we get from LoadFromCAR.
	store, commitCID, err := LoadBlocksFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	commitData, err := store.GetBlock(commitCID)
	require.NoError(t, err)
	commit, err := decodeCommit(commitData)
	require.NoError(t, err)
	dataRoot := commit.Data
	require.True(t, dataRoot.Defined())

	// Identify an interior MST node: a block that decodes as NodeData and has
	// at least one child (Left or a Right), i.e. NOT a leaf record block and
	// not the commit. The data-root node itself qualifies and is fine to drop.
	var interior cbor.CID
	for cid, data := range store.All() {
		if cid.Equal(commitCID) {
			continue
		}
		nd, decErr := mst.DecodeNodeData(data)
		if decErr != nil {
			continue // record block
		}
		hasChild := nd.Left.HasVal()
		for _, e := range nd.Entries {
			if e.Right.HasVal() {
				hasChild = true
			}
		}
		// Prefer a NON-root interior node so the walk descends past the root
		// before hitting the gap, exercising mid-tree truncation.
		if hasChild && !cid.Equal(dataRoot) {
			interior = cid
			break
		}
	}
	require.True(t, interior.Defined(), "test repo must have a non-root interior MST node")

	partial := newDroppingStore(t, buf.Bytes(), interior)
	loaded := &Repo{
		DID:   rp.DID,
		Clock: atmos.NewTIDClock(0),
		Store: partial,
		Tree:  mst.LoadTree(partial, dataRoot),
	}

	err = loaded.CheckComplete()
	require.Error(t, err)
	require.ErrorIs(t, err, mst.ErrBlockNotFound)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "interior-node miss must also classify transient")
}

// TestCheckComplete_BoundaryTruncationSweep is the end-to-end regression for
// the oracle bug. It exports a real repo, truncates the CAR at EVERY prefix
// length, and asserts:
//   - LoadCompleteFromCAR NEVER returns a successfully-loaded-but-incomplete
//     repo: every offset is either a clean complete prefix, or an error that
//     is classifiable as transient (io.ErrUnexpectedEOF).
//   - At least one offset is a block-boundary cut that LoadFromCAR accepts but
//     CheckComplete rejects (proving the boundary case exists and is caught).
func TestCheckComplete_BoundaryTruncationSweep(t *testing.T) {
	t.Parallel()
	rp, key := buildTestRepo(t, 120)

	var buf bytes.Buffer
	require.NoError(t, rp.ExportCAR(&buf, key))
	full := buf.Bytes()

	// The first blocks region begins after the CAR header ([varint len][header
	// bytes]). Truncations that land INSIDE the header are a distinct "not even
	// a parseable CAR yet" regime (a cut in the leading length varint can yield
	// a bare io.EOF), not the block-boundary case this test targets. Start the
	// meaningful sweep at the first block so every offset has a fully-delivered
	// header and represents a real "blocks were cut" truncation.
	headerEnd := carHeaderEnd(t, full)

	boundaryCaughtByCheck := 0
	for n := headerEnd; n < len(full); n++ {
		prefix := full[:n]

		// Permissive load: may succeed with a partial block set.
		loaded, _, loadErr := LoadFromCAR(bytes.NewReader(prefix))
		if loadErr == nil {
			// Loaded clean — a block-boundary cut. CheckComplete must reject it
			// as incomplete and classify it transient. (If the prefix happened
			// to carry every block, CheckComplete returns nil; that is fine.)
			checkErr := loaded.CheckComplete()
			if checkErr != nil {
				require.ErrorIs(t, checkErr, io.ErrUnexpectedEOF,
					"boundary truncation at n=%d must be classified transient", n)
				require.ErrorIs(t, checkErr, mst.ErrBlockNotFound,
					"boundary truncation at n=%d must wrap the matchable sentinel", n)
				boundaryCaughtByCheck++
			}
			continue
		}
		// LoadFromCAR rejected it: a mid-block truncation, which must itself be
		// transient-classifiable.
		require.ErrorIs(t, loadErr, io.ErrUnexpectedEOF,
			"mid-block truncation at n=%d must be transient", n)

		// And the strict loader must agree (transient) for the same prefix.
		_, _, strictErr := LoadCompleteFromCAR(bytes.NewReader(prefix))
		require.ErrorIs(t, strictErr, io.ErrUnexpectedEOF,
			"LoadCompleteFromCAR at n=%d must be transient", n)
	}

	require.Positive(t, boundaryCaughtByCheck,
		"expected at least one block-boundary truncation that LoadFromCAR accepts but CheckComplete rejects")
}

// TestLoadFromCAR_StaysPermissive proves the diff-safe contract: LoadFromCAR
// does NOT verify completeness (so it remains usable for intentional diff
// CARs). We feed a boundary-truncated CAR that loads cleanly and assert
// LoadFromCAR returns no error while CheckComplete does.
func TestLoadFromCAR_StaysPermissive(t *testing.T) {
	t.Parallel()
	rp, key := buildTestRepo(t, 120)
	var buf bytes.Buffer
	require.NoError(t, rp.ExportCAR(&buf, key))
	full := buf.Bytes()

	// Find a prefix that LoadFromCAR accepts but CheckComplete rejects.
	var found bool
	for n := 1; n < len(full); n++ {
		loaded, _, loadErr := LoadFromCAR(bytes.NewReader(full[:n]))
		if loadErr != nil {
			continue
		}
		if err := loaded.CheckComplete(); err != nil {
			// LoadFromCAR was permissive (no error) but the repo is incomplete.
			require.NoError(t, loadErr)
			require.Error(t, err)
			found = true
			break
		}
	}
	require.True(t, found, "expected a boundary prefix LoadFromCAR accepts but CheckComplete flags")
}

// carHeaderEnd returns the byte offset just past the CAR v1 header, i.e. the
// start of the first block region: [uvarint headerLen][headerLen header bytes].
func carHeaderEnd(t *testing.T, car []byte) int {
	t.Helper()
	var headerLen uint64
	var shift uint
	i := 0
	for ; i < len(car); i++ {
		b := car[i]
		headerLen |= uint64(b&0x7f) << shift
		if b < 0x80 {
			i++
			break
		}
		shift += 7
	}
	end := i + int(headerLen)
	require.Less(t, end, len(car), "CAR header must be smaller than the full CAR")
	return end
}

// droppingStore is a read-only MemBlockStore-backed store with one CID removed,
// used to simulate a CAR that arrived missing exactly one block.
func newDroppingStore(t *testing.T, carBytes []byte, drop cbor.CID) mst.BlockStore {
	t.Helper()
	store, _, err := LoadBlocksFromCAR(bytes.NewReader(carBytes))
	require.NoError(t, err)
	s := &droppingStore{base: store, dropped: drop}
	return s
}

type droppingStore struct {
	base    *mst.MemBlockStore
	dropped cbor.CID
}

func (s *droppingStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if cid.Equal(s.dropped) {
		return nil, fmt.Errorf("%w: %s", mst.ErrBlockNotFound, cid.String())
	}
	return s.base.GetBlock(cid)
}

func (s *droppingStore) PutBlock(cid cbor.CID, data []byte) error {
	return s.base.PutBlock(cid, data)
}

// TestMemBlockStore_ErrBlockNotFound: the sentinel is matchable via errors.Is.
func TestMemBlockStore_ErrBlockNotFound(t *testing.T) {
	t.Parallel()
	store := mst.NewMemBlockStore()
	_, err := store.GetBlock(cbor.ComputeCID(cbor.CodecDagCBOR, []byte("absent")))
	require.Error(t, err)
	require.ErrorIs(t, err, mst.ErrBlockNotFound)
	require.False(t, errors.Is(err, io.ErrUnexpectedEOF),
		"the raw store error is not itself a truncation; only CheckComplete adds that classification")
}
