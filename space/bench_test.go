package space

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
)

func benchmarkSnapshot(size int) *memorySnapshot {
	data := cbor.AppendMapHeader(nil, 1)
	data = cbor.AppendText(data, "text")
	data = cbor.AppendText(data, "benchmark")
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	snapshot := &memorySnapshot{
		refs:    make([]RecordRef, 0, size),
		records: make(map[RecordPath][]byte, size),
	}
	for i := range size {
		path := RecordPath(fmt.Sprintf("com.example.post/%08d", i))
		snapshot.refs = append(snapshot.refs, RecordRef{Path: path, CID: cid})
		snapshot.records[path] = data
	}
	return snapshot
}

func BenchmarkRepoIndex(b *testing.B) {
	for _, size := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("records-%d", size), func(b *testing.B) {
			snapshot := benchmarkSnapshot(size)
			limits := CARLimits{
				MaxHeaderSize: 4096,
				MaxCommitSize: 4096,
				MaxIndexSize:  64 << 20,
				MaxRecordSize: 4096,
				MaxTotalSize:  128 << 20,
				MaxRecords:    size,
			}
			b.ReportAllocs()
			for b.Loop() {
				index, err := BuildRepoIndex(context.Background(), snapshot, limits)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := EncodeRepoIndex(index); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSerializeRepoCAR(b *testing.B) {
	const size = 1_000
	snapshot := benchmarkSnapshot(size)
	limits := CARLimits{
		MaxHeaderSize: 4096,
		MaxCommitSize: 4096,
		MaxIndexSize:  64 << 20,
		MaxRecordSize: 4096,
		MaxTotalSize:  128 << 20,
		MaxRecords:    size,
	}
	index, err := BuildRepoIndex(context.Background(), snapshot, limits)
	if err != nil {
		b.Fatal(err)
	}
	repo, err := NewRepoCommitFromIndex(index)
	if err != nil {
		b.Fatal(err)
	}
	ctx, err := NewCommitContext(
		"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.forum/3jzfcijpj2z2a",
		"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
		"3jzfcijpj2z2a",
	)
	if err != nil {
		b.Fatal(err)
	}
	key, err := atmoscrypto.ParsePrivateP256(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		b.Fatal(err)
	}
	commit, err := repo.SignWithRandom(ctx, key, bytes.NewReader(bytes.Repeat([]byte{2}, 32)))
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []CARMode{CARIndexOnly, CARFull} {
		b.Run(fmt.Sprintf("mode-%d", mode), func(b *testing.B) {
			var out bytes.Buffer
			b.ReportAllocs()
			for b.Loop() {
				out.Reset()
				if err := SerializeRepoCAR(context.Background(), &out, commit, snapshot, mode, limits); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(out.Len()), "car-bytes")
		})
	}
}
