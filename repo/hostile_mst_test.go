package repo

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// diamondCAR builds a signed repo CAR whose MST is a "diamond": two nodes
// per level, each pointing at both nodes of the level below, so a full walk
// would visit 2^depth paths from only 2*depth blocks. With validHeights,
// each level's keys are mined to sit at that level's height, so every node
// passes the checks made one node at a time.
func diamondCAR(t *testing.T, depth int, validHeights bool) []byte {
	t.Helper()
	var blocks []car.Block
	put := func(data []byte) cbor.CID {
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
		blocks = append(blocks, car.Block{CID: cid, Data: data})
		return cid
	}
	record, err := cbor.Marshal(map[string]any{"$type": "com.example.dummy"})
	require.NoError(t, err)
	val := put(record)

	key := func(level, j int) string {
		for i := 0; ; i++ {
			k := fmt.Sprintf("com.example.lvl%02d/%d%d", level, j, i)
			if !validHeights || int(mst.HeightForKey(k)) == level {
				return k
			}
		}
	}
	var level [2]cbor.CID
	for i := range depth {
		var next [2]cbor.CID
		for j := range 2 {
			nd := &mst.NodeData{Entries: []mst.EntryData{{KeySuffix: []byte(key(i, j)), Value: val}}}
			if i > 0 {
				nd.Left = gt.Some(level[j])
				nd.Entries[0].Right = gt.Some(level[(j+1)%2])
			}
			next[j] = put(encodeNode(t, nd))
		}
		level = next
	}

	priv, err := crypto.GenerateK256()
	require.NoError(t, err)
	commit := &Commit{DID: "did:plc:testuser1234567890abcde", Version: 3, Data: level[0], Rev: "3l3qo2vutsw2b"}
	require.NoError(t, commit.Sign(priv))
	commitData, err := commit.EncodeCBOR()
	require.NoError(t, err)
	commitCID := put(commitData)

	var buf bytes.Buffer
	require.NoError(t, car.WriteAll(&buf, []cbor.CID{commitCID}, blocks))
	return buf.Bytes()
}

// encodeNode encodes nd as DAG-CBOR. The mst package keeps its encoder
// private, so this goes through the generic encoder and checks that the
// result decodes, which only the canonical encoding does.
func encodeNode(t *testing.T, nd *mst.NodeData) []byte {
	t.Helper()
	m := map[string]any{"e": []any{}, "l": nil}
	if nd.Left.HasVal() {
		m["l"] = nd.Left.Val()
	}
	var entries []any
	for _, e := range nd.Entries {
		var right any
		if e.Right.HasVal() {
			right = e.Right.Val()
		}
		entries = append(entries, map[string]any{"k": e.KeySuffix, "p": int64(e.PrefixLen), "t": right, "v": e.Value})
	}
	if entries != nil {
		m["e"] = entries
	}
	data, err := cbor.Marshal(m)
	require.NoError(t, err)
	_, err = mst.DecodeNodeData(data)
	require.NoError(t, err, "test node is not canonically encoded")
	return data
}

// A hostile repo CAR whose MST reaches the same subtrees many times over
// must be rejected when loaded for backfill, quickly, instead of walking
// an exponential number of paths. The reference implementation's version
// of this case (depth 40, keys at arbitrary heights) describes 2^40 paths.
func TestLoadCompleteFromCAR_RejectsSharedSubtrees(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		depth        int
		validHeights bool
	}{
		{"reference diamond", 40, false},
		{"diamond with valid key heights", 8, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := diamondCAR(t, tc.depth, tc.validHeights)
			start := time.Now()
			_, _, err := LoadCompleteFromCAR(bytes.NewReader(data))
			require.ErrorIs(t, err, mst.ErrInvalidTree)
			require.Less(t, time.Since(start), 5*time.Second)
		})
	}
}
