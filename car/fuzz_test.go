package car

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jcalabro/atmos/cbor"
)

// FuzzReader tests that the CAR reader never panics on arbitrary input.
func FuzzReader(f *testing.F) {
	// Seed with real CAR files.
	greenground, _ := os.ReadFile("testdata/greenground.repo.car")
	repoSlice, _ := os.ReadFile("testdata/repo_slice.car")
	if len(greenground) > 0 {
		f.Add(greenground)
	}
	if len(repoSlice) > 0 {
		f.Add(repoSlice)
	}

	// Seed with a minimal valid CAR.
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))
	var buf bytes.Buffer
	w, _ := NewWriter(&buf, []cbor.CID{cid})
	_ = w.WriteBlock(cid, []byte("x"))
	f.Add(buf.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := NewReader(bytes.NewReader(data))
		if err != nil {
			return
		}
		// Drain all blocks — should never panic.
		for {
			_, err := r.Next()
			if err != nil {
				break
			}
		}
	})
}

// FuzzRoundTrip tests that reading then writing a CAR produces valid output
// that can be re-read.
func FuzzRoundTrip(f *testing.F) {
	greenground, _ := os.ReadFile("testdata/greenground.repo.car")
	if len(greenground) > 0 {
		f.Add(greenground)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		header, blocks, err := ReadAll(bytes.NewReader(data))
		if err != nil {
			return
		}

		// Write it back.
		var buf bytes.Buffer
		err = WriteAll(&buf, header.Roots, blocks)
		if err != nil {
			return
		}

		// Re-read should produce the same blocks.
		header2, blocks2, err := ReadAll(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("re-read failed: %v", err)
		}
		if len(blocks) != len(blocks2) {
			t.Fatalf("block count mismatch: %d vs %d", len(blocks), len(blocks2))
		}
		if len(header.Roots) != len(header2.Roots) {
			t.Fatalf("root count mismatch")
		}
		for i := range blocks {
			if !blocks[i].CID.Equal(blocks2[i].CID) {
				t.Fatalf("block %d CID mismatch", i)
			}
		}

		// Round-trip should be byte-identical.
		rt, err := RoundTrip(buf.Bytes())
		if err != nil {
			t.Fatalf("second round-trip failed: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), rt) {
			t.Fatalf("round-trip not byte-identical")
		}
	})
}

// FuzzWriteRead tests that anything we write can be read back.
func FuzzWriteRead(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"), []byte("test"))

	f.Fuzz(func(t *testing.T, d1, d2, d3 []byte) {
		if len(d1) == 0 {
			d1 = []byte{0}
		}
		cid1 := cbor.ComputeCID(cbor.CodecRaw, d1)
		cid2 := cbor.ComputeCID(cbor.CodecRaw, d2)
		cid3 := cbor.ComputeCID(cbor.CodecRaw, d3)

		var buf bytes.Buffer
		w, err := NewWriter(&buf, []cbor.CID{cid1})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteBlock(cid1, d1); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteBlock(cid2, d2); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteBlock(cid3, d3); err != nil {
			t.Fatal(err)
		}

		r, err := NewReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}

		b1, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b1.Data, d1) {
			t.Fatal("block 1 data mismatch")
		}
		b2, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b2.Data, d2) {
			t.Fatal("block 2 data mismatch")
		}
		b3, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b3.Data, d3) {
			t.Fatal("block 3 data mismatch")
		}
		_, err = r.Next()
		if !errors.Is(err, io.EOF) {
			t.Fatal("expected EOF")
		}
	})
}
