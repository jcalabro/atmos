package car

import (
	"bytes"
	"os"
	"testing"

	"github.com/jcalabro/atmos/cbor"
)

var benchBlocks []Block
var benchCAR []byte

func init() {
	data, err := os.ReadFile("testdata/repo_slice.car")
	if err != nil {
		panic(err)
	}
	benchCAR = data
	_, benchBlocks, err = ReadAll(bytes.NewReader(data))
	if err != nil {
		panic(err)
	}
}

func BenchmarkReadAll(b *testing.B) {
	for b.Loop() {
		_, _, _ = ReadAll(bytes.NewReader(benchCAR))
	}
}

func BenchmarkReadBlocks(b *testing.B) {
	for b.Loop() {
		r, _ := NewReader(bytes.NewReader(benchCAR))
		for {
			_, err := r.Next()
			if err != nil {
				break
			}
		}
	}
}

func BenchmarkReadBlocksInto(b *testing.B) {
	for b.Loop() {
		r, _ := NewReader(bytes.NewReader(benchCAR))
		for {
			_, err := r.NextInto()
			if err != nil {
				break
			}
		}
	}
}

func BenchmarkWriteAll(b *testing.B) {
	roots := []cbor.CID{benchBlocks[0].CID}
	var buf bytes.Buffer
	b.ResetTimer()
	for b.Loop() {
		buf.Reset()
		_ = WriteAll(&buf, roots, benchBlocks)
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	for b.Loop() {
		_, _ = RoundTrip(benchCAR)
	}
}
