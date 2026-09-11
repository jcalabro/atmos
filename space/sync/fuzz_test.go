package sync

import (
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/stretchr/testify/require"
)

func FuzzApplyOperationFailureIsAtomic(f *testing.F) {
	f.Add([]byte{0xff}, true, true)
	f.Add([]byte{0xa1, 0x61, 0x78, 0x01}, true, false)
	f.Fuzz(func(t *testing.T, body []byte, includeCID, matchingPrev bool) {
		path, err := spaces.ParseRecordPath("com.example.post/one")
		require.NoError(t, err)
		oldData, err := cbor.Marshal(map[string]any{"$type": "com.example.post", "v": "old"})
		require.NoError(t, err)
		oldCID := cbor.ComputeCID(cbor.CodecDagCBOR, oldData)
		index := spaces.RepoIndex{path: oldCID}
		commit, err := spaces.NewRepoCommitFromIndex(index)
		require.NoError(t, err)
		stage := Stage{Index: index.Clone(), Records: map[spaces.RecordPath]Record{path: {CID: oldCID, Data: oldData}}, State: commit.State()}
		before := stage.Clone()
		prev := oldCID
		if !matchingPrev {
			prev = cbor.ComputeCID(cbor.CodecDagCBOR, []byte{0xa0})
		}
		var next *cbor.CID
		if includeCID {
			cid := cbor.ComputeCID(cbor.CodecDagCBOR, body)
			next = &cid
		}
		err = applyOperation(&stage, Operation{Revision: atmos.NewTID(2, 0), Path: path, CID: next, Prev: &prev, Body: body})
		if err != nil {
			require.Equal(t, before, stage)
		}
	})
}
