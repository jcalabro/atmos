package sync

import (
	"strconv"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
)

func BenchmarkApplyOperationSamePath(b *testing.B) {
	path, _ := spaces.ParseRecordPath("com.example.post/one")
	records := make([]Record, 1024)
	for i := range records {
		records[i].Data, _ = cbor.Marshal(map[string]any{"$type": "com.example.post", "v": strconv.Itoa(i)})
		records[i].CID = cbor.ComputeCID(cbor.CodecDagCBOR, records[i].Data)
	}
	data := records[len(records)-1].Data
	current := records[len(records)-1].CID
	commit, _ := spaces.NewRepoCommitFromIndex(spaces.RepoIndex{path: current})
	stage := Stage{Index: spaces.RepoIndex{path: current}, Records: map[spaces.RecordPath]Record{path: {CID: current, Data: data}}, State: commit.State()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next := records[i%len(records)]
		if err := applyOperation(&stage, Operation{Revision: atmos.NewTID(int64(i+2), 0), Path: path, Prev: &current, CID: &next.CID, Body: next.Data}); err != nil {
			b.Fatal(err)
		}
		current = next.CID
	}
}
