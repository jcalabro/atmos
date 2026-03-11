package mst

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testValueCID(t *testing.T) cbor.CID {
	t.Helper()
	cid, err := cbor.ParseCIDString("bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454")
	require.NoError(t, err)
	return cid
}

// buildTreeFromKeys creates a tree where all keys map to the test value CID.
func buildTreeFromKeys(t *testing.T, keys []string) (*Tree, *MemBlockStore) {
	t.Helper()
	store := NewMemBlockStore()
	tree := NewTree(store)
	val := testValueCID(t)
	for _, k := range keys {
		require.NoError(t, tree.Insert(k, val))
	}
	return tree, store
}

// -------------------------------------------------------------------
// Height computation interop vectors
// -------------------------------------------------------------------

func TestHeightForKey(t *testing.T) {
	t.Parallel()
	// All 9 vectors from atproto-interop-tests/mst/key_heights.json.
	tests := []struct {
		key    string
		height uint8
	}{
		{"", 0},
		{"asdf", 0},
		{"blue", 1},
		{"2653ae71", 0},
		{"88bfafc7", 2},
		{"2a92d355", 4},
		{"884976f5", 6},
		{"app.bsky.feed.post/454397e440ec", 4},
		{"app.bsky.feed.post/9adeb165882c", 8},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.height, HeightForKey(tc.key), "HeightForKey(%q)", tc.key)
	}
}

func TestHeightForKey_ExampleKeys(t *testing.T) {
	t.Parallel()
	// Validate all 156 keys from indigo's example_keys.txt.
	// Key format: "{Letter}{Height}/{Number}" e.g. "A0/374913" has height 0.
	f, err := os.Open("testdata/example_keys.txt")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	count := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Parse expected height from the second character.
		require.True(t, len(line) >= 2, "line too short: %q", line)
		expectedHeight, err := strconv.Atoi(string(line[1]))
		require.NoError(t, err, "parsing height from %q", line)
		assert.Equal(t, uint8(expectedHeight), HeightForKey(line), "HeightForKey(%q)", line)
		count++
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, 156, count, "expected 156 example keys")
}

func TestHeightForKey_CommitProofFixtureKeys(t *testing.T) {
	t.Parallel()
	// The fixture keys encode height in the second character. Verify consistency.
	keys := []string{
		"A0/374913", "B1/986427", "C0/451630", "D2/269196",
		"E0/670489", "F1/085263", "G0/765327", "C2/014073",
		"B2/827649", "E2/819540", "H0/131238", "A2/827942",
		"G2/611528", "R2/742766",
	}
	for _, key := range keys {
		expected, _ := strconv.Atoi(string(key[1]))
		assert.Equal(t, uint8(expected), HeightForKey(key), "HeightForKey(%q)", key)
	}
}

// TestHeightFromHash_MultiWordZeros verifies the height formula is correct
// when the SHA-256 hash has one or more full 64-bit words of zeros at the
// start. This exercises the i*4 base calculation that was previously buggy
// (using i/2*4, which undercounted by 50% for i >= 8).
func TestHeightFromHash_MultiWordZeros(t *testing.T) {
	t.Parallel()

	// Reference: count leading zero 2-bit pairs byte-by-byte (simple, obviously correct).
	refHeight := func(h *[32]byte) uint8 {
		var count uint8
		for _, b := range h {
			if b < 64 {
				count++
			}
			if b < 16 {
				count++
			}
			if b < 4 {
				count++
			}
			if b == 0 {
				count++
			} else {
				break
			}
		}
		return count
	}

	tests := []struct {
		name   string
		hash   [32]byte
		height uint8
	}{
		{
			name:   "no leading zeros",
			hash:   [32]byte{0xFF},
			height: 0,
		},
		{
			name:   "one zero byte then 0x01",
			hash:   [32]byte{0x00, 0x01},
			height: 7, // 4 pairs from zero byte + 3 pairs from 0x01
		},
		{
			name:   "8 zero bytes then 0x01 (crosses word boundary)",
			hash:   [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0x01},
			height: 35, // 32 pairs from 8 zero bytes + 3 from 0x01
		},
		{
			name:   "8 zero bytes then 0x30",
			hash:   [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0x30},
			height: 33, // 32 + 1
		},
		{
			name:   "16 zero bytes then 0x01",
			hash:   [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01},
			height: 67, // 64 + 3
		},
		{
			name:   "24 zero bytes then 0xFF",
			hash:   [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF},
			height: 96, // 24*4 + 0
		},
		{
			name:   "all zeros",
			hash:   [32]byte{},
			height: 128,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := heightFromHash(&tc.hash)
			ref := refHeight(&tc.hash)
			assert.Equal(t, tc.height, got, "heightFromHash mismatch")
			assert.Equal(t, ref, got, "heightFromHash disagrees with reference impl")
		})
	}
}

// -------------------------------------------------------------------
// Prefix length tests
// -------------------------------------------------------------------

func TestSharedPrefixLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b     string
		expected int
	}{
		{"", "", 0},
		{"", "abc", 0},
		{"abc", "", 0},
		{"abc", "abc", 3},
		{"abc", "abd", 2},
		{"abcdef", "abcxyz", 3},
		{"hello", "hello world", 5},
		{"abc\x00", "abc\x01", 3},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, sharedPrefixLen(tc.a, tc.b),
			"sharedPrefixLen(%q, %q)", tc.a, tc.b)
	}
}

func TestSharedPrefixLen_Unicode(t *testing.T) {
	t.Parallel()
	// UTF-8 byte-level comparison, not rune-level.
	assert.Equal(t, 6, sharedPrefixLen("jalapeño", "jalapeno")) // diverge at ñ vs n
	// "co" = 2 bytes, then ö=0xC3B6 and ü=0xC3BC share 0xC3, so 3 bytes shared.
	assert.Equal(t, 3, sharedPrefixLen("coöperative", "coüperative"))
}

// -------------------------------------------------------------------
// Known root CID interop vectors
// -------------------------------------------------------------------

func TestEmptyTree_RootCID(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)
	cid, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm", cid.String())
}

func TestSingleEntry_RootCID(t *testing.T) {
	t.Parallel()
	tree, _ := buildTreeFromKeys(t, []string{
		"com.example.record/3jqfcqzm3fo2j",
	})
	cid, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu", cid.String())
}

func TestSingleEntryLayer2_RootCID(t *testing.T) {
	t.Parallel()
	tree, _ := buildTreeFromKeys(t, []string{
		"com.example.record/3jqfcqzm3fx2j",
	})
	cid, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreih7wfei65pxzhauoibu3ls7jgmkju4bspy4t2ha2qdjnzqvoy33ai", cid.String())
}

func TestFiveEntries_RootCID(t *testing.T) {
	t.Parallel()
	tree, _ := buildTreeFromKeys(t, []string{
		"com.example.record/3jqfcqzm3fp2j",
		"com.example.record/3jqfcqzm3fr2j",
		"com.example.record/3jqfcqzm3fs2j",
		"com.example.record/3jqfcqzm3ft2j",
		"com.example.record/3jqfcqzm4fc2j",
	})
	cid, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreicmahysq4n6wfuxo522m6dpiy7z7qzym3dzs756t5n7nfdgccwq7m", cid.String())
}

// -------------------------------------------------------------------
// Edge case: trim top of tree on delete
// -------------------------------------------------------------------

func TestEdgeCase_TrimTopOnDelete(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)

	tree, _ := buildTreeFromKeys(t, []string{
		"com.example.record/3jqfcqzm3fn2j", // level 0
		"com.example.record/3jqfcqzm3fo2j", // level 0
		"com.example.record/3jqfcqzm3fp2j", // level 0
		"com.example.record/3jqfcqzm3fs2j", // level 0
		"com.example.record/3jqfcqzm3ft2j", // level 0
		"com.example.record/3jqfcqzm3fu2j", // level 1
	})
	_ = val

	cidBefore, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreifnqrwbk6ffmyaz5qtujqrzf5qmxf7cbxvgzktl4e3gabuxbtatv4", cidBefore.String())

	// Remove the level-1 key — tree should trim to height 0.
	require.NoError(t, tree.Remove("com.example.record/3jqfcqzm3fs2j"))

	cidAfter, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreie4kjuxbwkhzg2i5dljaswcroeih4dgiqq6pazcmunwt2byd725vi", cidAfter.String())
}

// -------------------------------------------------------------------
// Edge case: insertion splits two layers down
// -------------------------------------------------------------------

func TestEdgeCase_InsertionSplitsTwoLayersDown(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)

	tree, _ := buildTreeFromKeys(t, []string{
		"com.example.record/3jqfcqzm3fo2j", // A; level 0
		"com.example.record/3jqfcqzm3fp2j", // B; level 0
		"com.example.record/3jqfcqzm3fr2j", // C; level 0
		"com.example.record/3jqfcqzm3fs2j", // D; level 1
		"com.example.record/3jqfcqzm3ft2j", // E; level 0
		"com.example.record/3jqfcqzm3fz2j", // G; level 0
		"com.example.record/3jqfcqzm4fc2j", // H; level 0
		"com.example.record/3jqfcqzm4fd2j", // I; level 1
		"com.example.record/3jqfcqzm4ff2j", // J; level 0
		"com.example.record/3jqfcqzm4fg2j", // K; level 0
		"com.example.record/3jqfcqzm4fh2j", // L; level 0
	})

	cidBefore, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreiettyludka6fpgp33stwxfuwhkzlur6chs4d2v4nkmq2j3ogpdjem", cidBefore.String())

	// Insert F (level 2) — splits two layers down.
	require.NoError(t, tree.Insert("com.example.record/3jqfcqzm3fx2j", val))

	cidAfter, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreid2x5eqs4w4qxvc5jiwda4cien3gw2q6cshofxwnvv7iucrmfohpm", cidAfter.String())

	// Remove F — should return to original state.
	require.NoError(t, tree.Remove("com.example.record/3jqfcqzm3fx2j"))

	cidFinal, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreiettyludka6fpgp33stwxfuwhkzlur6chs4d2v4nkmq2j3ogpdjem", cidFinal.String())
}

// -------------------------------------------------------------------
// Edge case: new layers two higher than existing
// -------------------------------------------------------------------

func TestEdgeCase_NewLayersTwoHigher(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)

	tree, _ := buildTreeFromKeys(t, []string{
		"com.example.record/3jqfcqzm3ft2j", // A; level 0
		"com.example.record/3jqfcqzm3fz2j", // C; level 0
	})

	cidBefore, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreidfcktqnfmykz2ps3dbul35pepleq7kvv526g47xahuz3rqtptmky", cidBefore.String())

	// Insert B (level 2) — jumps two levels.
	require.NoError(t, tree.Insert("com.example.record/3jqfcqzm3fx2j", val))

	cidAfter, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreiavxaxdz7o7rbvr3zg2liox2yww46t7g6hkehx4i4h3lwudly7dhy", cidAfter.String())

	// Remove B — back to original.
	require.NoError(t, tree.Remove("com.example.record/3jqfcqzm3fx2j"))

	cidAgain, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreidfcktqnfmykz2ps3dbul35pepleq7kvv526g47xahuz3rqtptmky", cidAgain.String())

	// Insert B (level 2) and D (level 1).
	require.NoError(t, tree.Insert("com.example.record/3jqfcqzm3fx2j", val))
	require.NoError(t, tree.Insert("com.example.record/3jqfcqzm4fd2j", val))

	cidBoth, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreig4jv3vuajbsybhyvb7gggvpwh2zszwfyttjrj6qwvcsp24h6popu", cidBoth.String())
}

// -------------------------------------------------------------------
// Commit proof fixtures (6 complex interop vectors)
// -------------------------------------------------------------------

type commitProofFixture struct {
	Comment          string   `json:"comment"`
	LeafValue        string   `json:"leafValue"`
	Keys             []string `json:"keys"`
	Adds             []string `json:"adds"`
	Dels             []string `json:"dels"`
	RootBeforeCommit string   `json:"rootBeforeCommit"`
	RootAfterCommit  string   `json:"rootAfterCommit"`
	BlocksInProof    []string `json:"blocksInProof"`
}

func TestCommitProofFixtures(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/commit-proof-fixtures.json")
	require.NoError(t, err)

	var fixtures []commitProofFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 6)

	for _, f := range fixtures {
		t.Run(f.Comment, func(t *testing.T) {
			t.Parallel()
			val, err := cbor.ParseCIDString(f.LeafValue)
			require.NoError(t, err)

			// Build initial tree.
			store := NewMemBlockStore()
			tree := NewTree(store)
			for _, key := range f.Keys {
				require.NoError(t, tree.Insert(key, val))
			}

			rootBefore, err := tree.RootCID()
			require.NoError(t, err)
			require.Equal(t, f.RootBeforeCommit, rootBefore.String(),
				"root before commit mismatch for %q", f.Comment)

			// Apply adds.
			for _, key := range f.Adds {
				require.NoError(t, tree.Insert(key, val))
			}

			// Apply deletes.
			for _, key := range f.Dels {
				require.NoError(t, tree.Remove(key))
			}

			rootAfter, err := tree.RootCID()
			require.NoError(t, err)
			require.Equal(t, f.RootAfterCommit, rootAfter.String(),
				"root after commit mismatch for %q", f.Comment)
		})
	}
}

// -------------------------------------------------------------------
// Basic operations
// -------------------------------------------------------------------

func TestInsertAndGet(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	require.NoError(t, tree.Insert("a", val))

	got, err := tree.Get("a")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Equal(val))

	got, err = tree.Get("b")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestInsertUpdate(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val1 := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v1"))
	val2 := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v2"))

	require.NoError(t, tree.Insert("key", val1))

	got, err := tree.Get("key")
	require.NoError(t, err)
	require.True(t, got.Equal(val1))

	// Update to new value.
	require.NoError(t, tree.Insert("key", val2))

	got, err = tree.Get("key")
	require.NoError(t, err)
	require.True(t, got.Equal(val2))
}

func TestInsertAndRemove(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		require.NoError(t, tree.Insert(key, val))
	}

	require.NoError(t, tree.Remove("c"))

	got, err := tree.Get("c")
	require.NoError(t, err)
	require.Nil(t, got)

	for _, key := range []string{"a", "b", "d", "e"} {
		got, err := tree.Get(key)
		require.NoError(t, err)
		require.NotNil(t, got, "expected key %q to exist", key)
	}
}

func TestRemoveAllKeys(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		require.NoError(t, tree.Insert(key, val))
	}
	for _, key := range keys {
		require.NoError(t, tree.Remove(key))
	}

	// Should produce the same CID as an empty tree.
	cid, err := tree.RootCID()
	require.NoError(t, err)
	require.Equal(t, "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm", cid.String())
}

func TestRemoveNonexistent(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	require.NoError(t, tree.Insert("a", val))

	// Removing a key that doesn't exist should be a no-op.
	require.NoError(t, tree.Remove("nonexistent"))

	got, err := tree.Get("a")
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestGetFromEmptyTree(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	got, err := tree.Get("anything")
	require.NoError(t, err)
	require.Nil(t, got)
}

// -------------------------------------------------------------------
// Walk
// -------------------------------------------------------------------

func TestWalk(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := []string{"z", "a", "m", "b", "x"}
	for _, key := range keys {
		require.NoError(t, tree.Insert(key, val))
	}

	var walked []string
	err := tree.Walk(func(key string, _ cbor.CID) error {
		walked = append(walked, key)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "m", "x", "z"}, walked)
}

func TestWalkEmptyTree(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	var walked []string
	err := tree.Walk(func(key string, _ cbor.CID) error {
		walked = append(walked, key)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, walked)
}

// -------------------------------------------------------------------
// Write, load, and persistence
// -------------------------------------------------------------------

func TestWriteAndLoad(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		require.NoError(t, tree.Insert(key, val))
	}

	rootCID, err := tree.WriteBlocks(store)
	require.NoError(t, err)

	tree2 := LoadTree(store, rootCID)
	for _, key := range keys {
		got, err := tree2.Get(key)
		require.NoError(t, err)
		require.NotNil(t, got, "expected key %q after load", key)
		require.True(t, got.Equal(val))
	}

	cid2, err := tree2.RootCID()
	require.NoError(t, err)
	require.True(t, rootCID.Equal(cid2))
}

func TestWriteLoadModify(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)

	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, tree.Insert(key, val))
	}

	rootCID, err := tree.WriteBlocks(store)
	require.NoError(t, err)

	// Load, modify, write again.
	tree2 := LoadTree(store, rootCID)
	require.NoError(t, tree2.Remove("c"))
	require.NoError(t, tree2.Insert("f", val))

	rootCID2, err := tree2.WriteBlocks(store)
	require.NoError(t, err)
	require.False(t, rootCID.Equal(rootCID2), "root CID should change after modification")

	// Verify state of modified tree.
	tree3 := LoadTree(store, rootCID2)
	got, err := tree3.Get("c")
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = tree3.Get("f")
	require.NoError(t, err)
	require.NotNil(t, got)
}

// -------------------------------------------------------------------
// Diff
// -------------------------------------------------------------------

func TestDiff(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()

	val1 := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v1"))
	val2 := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v2"))

	tree1 := NewTree(store)
	require.NoError(t, tree1.Insert("a", val1))
	require.NoError(t, tree1.Insert("b", val1))
	require.NoError(t, tree1.Insert("c", val1))
	oldRoot, err := tree1.WriteBlocks(store)
	require.NoError(t, err)

	tree2 := NewTree(store)
	require.NoError(t, tree2.Insert("a", val1)) // unchanged
	require.NoError(t, tree2.Insert("b", val2)) // updated
	require.NoError(t, tree2.Insert("d", val1)) // created
	newRoot, err := tree2.WriteBlocks(store)
	require.NoError(t, err)

	ops, err := Diff(store, oldRoot, newRoot)
	require.NoError(t, err)

	creates := map[string]bool{}
	updates := map[string]bool{}
	deletes := map[string]bool{}
	for _, op := range ops {
		if op.Old == nil {
			creates[op.Key] = true
		} else if op.New == nil {
			deletes[op.Key] = true
		} else {
			updates[op.Key] = true
		}
	}

	assert.True(t, creates["d"])
	assert.True(t, updates["b"])
	assert.True(t, deletes["c"])
	assert.Len(t, ops, 3)
}

func TestDiffIdentical(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()

	tree := NewTree(store)
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("v"))
	require.NoError(t, tree.Insert("a", val))
	root, err := tree.WriteBlocks(store)
	require.NoError(t, err)

	ops, err := Diff(store, root, root)
	require.NoError(t, err)
	require.Empty(t, ops)
}

// -------------------------------------------------------------------
// NodeData round-trip
// -------------------------------------------------------------------

func TestNodeDataRoundTrip(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	nd := &NodeData{
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("abc"), Value: cid},
			{PrefixLen: 2, KeySuffix: []byte("d"), Value: cid, Right: gt.Some(cid)},
		},
	}

	data, err := encodeNodeData(nd)
	require.NoError(t, err)

	decoded, err := DecodeNodeData(data)
	require.NoError(t, err)
	require.Len(t, decoded.Entries, 2)
	require.Equal(t, 0, decoded.Entries[0].PrefixLen)
	require.Equal(t, []byte("abc"), decoded.Entries[0].KeySuffix)
	require.Equal(t, 2, decoded.Entries[1].PrefixLen)
	require.True(t, decoded.Entries[1].Right.HasVal())
	require.True(t, decoded.Left.IsNone())
}

func TestNodeDataRoundTrip_WithLeft(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("test"))
	nd := &NodeData{
		Left: gt.Some(cid),
		Entries: []EntryData{
			{PrefixLen: 0, KeySuffix: []byte("key"), Value: cid, Right: gt.Some(cid)},
		},
	}

	data, err := encodeNodeData(nd)
	require.NoError(t, err)

	decoded, err := DecodeNodeData(data)
	require.NoError(t, err)
	require.True(t, decoded.Left.HasVal())
	require.True(t, cid.Equal(decoded.Left.Val()))
}

func TestNodeDataRoundTrip_EmptyEntries(t *testing.T) {
	t.Parallel()
	nd := &NodeData{Entries: []EntryData{}}

	data, err := encodeNodeData(nd)
	require.NoError(t, err)

	decoded, err := DecodeNodeData(data)
	require.NoError(t, err)
	require.Empty(t, decoded.Entries)
	require.True(t, decoded.Left.IsNone())
}

// -------------------------------------------------------------------
// Larger trees
// -------------------------------------------------------------------

func TestManyKeys(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))

	for i := range 100 {
		key := "com.example.record/" + string(rune('A'+i/26)) + string(rune('a'+i%26))
		require.NoError(t, tree.Insert(key, val))
	}

	var keys []string
	err := tree.Walk(func(key string, _ cbor.CID) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, keys, 100)
	for i := 1; i < len(keys); i++ {
		require.Less(t, keys[i-1], keys[i], "keys not sorted at index %d", i)
	}

	rootCID, err := tree.WriteBlocks(store)
	require.NoError(t, err)

	tree2 := LoadTree(store, rootCID)
	cid2, err := tree2.RootCID()
	require.NoError(t, err)
	require.True(t, rootCID.Equal(cid2))
}

func TestManyKeysInsertAndRemoveHalf(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))

	var keys []string
	for i := range 50 {
		key := "com.example.record/" + string(rune('A'+i/26)) + string(rune('a'+i%26))
		keys = append(keys, key)
		require.NoError(t, tree.Insert(key, val))
	}

	// Remove every other key.
	for i := 0; i < len(keys); i += 2 {
		require.NoError(t, tree.Remove(keys[i]))
	}

	// Verify removed keys are gone and remaining keys exist.
	for i, key := range keys {
		got, err := tree.Get(key)
		require.NoError(t, err)
		if i%2 == 0 {
			require.Nil(t, got, "key %q should be removed", key)
		} else {
			require.NotNil(t, got, "key %q should still exist", key)
		}
	}

	// Walk should show exactly 25 entries.
	var count int
	err := tree.Walk(func(_ string, _ cbor.CID) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 25, count)
}

// -------------------------------------------------------------------
// Manual node encoding (matches indigo TestManualNode)
// -------------------------------------------------------------------

func TestManualNodeEncoding(t *testing.T) {
	t.Parallel()
	val := testValueCID(t)

	nd := &NodeData{
		Entries: []EntryData{
			{
				PrefixLen: 0,
				KeySuffix: []byte("com.example.record/3jqfcqzm3fo2j"),
				Value:     val,
			},
		},
	}

	data, err := encodeNodeData(nd)
	require.NoError(t, err)

	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	require.Equal(t, "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu", cid.String())
}

// -------------------------------------------------------------------
// Order independence (from TS mst.test.ts "is order independent")
// -------------------------------------------------------------------

func TestOrderIndependence(t *testing.T) {
	t.Parallel()
	val := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("val"))

	// Generate 200 keys.
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("com.example.record/%06d", i))
	}

	// Insert in original order.
	store1 := NewMemBlockStore()
	tree1 := NewTree(store1)
	for _, k := range keys {
		require.NoError(t, tree1.Insert(k, val))
	}
	cid1, err := tree1.RootCID()
	require.NoError(t, err)

	// Insert in reverse order.
	store2 := NewMemBlockStore()
	tree2 := NewTree(store2)
	for i := len(keys) - 1; i >= 0; i-- {
		require.NoError(t, tree2.Insert(keys[i], val))
	}
	cid2, err := tree2.RootCID()
	require.NoError(t, err)

	require.True(t, cid1.Equal(cid2), "tree should be order-independent")

	// Insert in a different shuffled order (every-other interleave).
	store3 := NewMemBlockStore()
	tree3 := NewTree(store3)
	for i := 0; i < len(keys); i += 2 {
		require.NoError(t, tree3.Insert(keys[i], val))
	}
	for i := 1; i < len(keys); i += 2 {
		require.NoError(t, tree3.Insert(keys[i], val))
	}
	cid3, err := tree3.RootCID()
	require.NoError(t, err)

	require.True(t, cid1.Equal(cid3), "tree should be order-independent (interleaved)")
}

// -------------------------------------------------------------------
// MST key validation (from TS mst.test.ts "MST Interop Allowable Keys")
// -------------------------------------------------------------------

func TestIsValidMstKey(t *testing.T) {
	t.Parallel()

	valid := []string{
		"com.example.record/3jui7kd54zh2y",
		"coll/example.com",
		"coll/self",
		"coll/lang:en",
		"coll/pre:fix",
		"coll/" + strings.Repeat("a", 512),
	}
	for _, k := range valid {
		assert.True(t, IsValidMstKey(k), "expected valid: %q", k)
	}

	invalid := []string{
		"",                // empty
		"coll",            // no slash
		"coll/",           // empty rkey
		"/rkey",           // empty collection
		"coll/rkey/extra", // too many segments
		"col l/rkey",      // space in collection
		"coll/rke y",      // space in rkey
		"coll/rkey!",      // exclamation
		"coll/rkey@",      // at sign
		strings.Repeat("a", 500) + "/" + strings.Repeat("b", 525), // > 1024 chars
	}
	for _, k := range invalid {
		assert.False(t, IsValidMstKey(k), "expected invalid: %q", k)
	}
}

// -------------------------------------------------------------------
// Height algorithm equivalence with TypeScript
// -------------------------------------------------------------------

func TestWriteBlocks_EmptyTree(t *testing.T) {
	t.Parallel()
	store := NewMemBlockStore()
	tree := NewTree(store)
	cid, err := tree.WriteBlocks(store)
	require.NoError(t, err)
	require.True(t, cid.Defined())

	// Block should be in the store.
	data, err := store.GetBlock(cid)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Loading from the CID should give an equivalent empty tree.
	tree2 := LoadTree(store, cid)
	var count int
	require.NoError(t, tree2.Walk(func(_ string, _ cbor.CID) error {
		count++
		return nil
	}))
	require.Equal(t, 0, count)
}

func TestWalk_ErrorPropagation(t *testing.T) {
	t.Parallel()
	tree, _ := buildTreeFromKeys(t, []string{"a", "b", "c", "d", "e"})

	stopErr := fmt.Errorf("stop walking")
	var visited int
	err := tree.Walk(func(_ string, _ cbor.CID) error {
		visited++
		if visited == 2 {
			return stopErr
		}
		return nil
	})
	require.ErrorIs(t, err, stopErr)
	require.Equal(t, 2, visited)
}

func TestHeightForKey_TSEquivalence(t *testing.T) {
	t.Parallel()
	// Verify our Go implementation matches the TypeScript leading-zeros algorithm.
	// TS counts: if byte < 64 → +1, if byte < 16 → +1, if byte < 4 → +1, if byte == 0 → +1, else break.
	// This is equivalent to counting leading zero 2-bit pairs in the SHA-256 hash.
	tests := []struct {
		key    string
		height uint8
	}{
		{"", 0},
		{"blue", 1},
		{"88bfafc7", 2},
		{"2a92d355", 4},
		{"884976f5", 6},
		{"app.bsky.feed.post/454397e440ec", 4},
		{"app.bsky.feed.post/9adeb165882c", 8},
		{"com.example.record/3jqfcqzm3fo2j", 0},
		{"com.example.record/3jqfcqzm3fx2j", 2},
		{"com.example.record/3jqfcqzm3fs2j", 1},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.height, HeightForKey(tc.key), "HeightForKey(%q)", tc.key)
	}
}
