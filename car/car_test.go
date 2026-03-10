package car

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------
// Fixture loading and round-trip
// -------------------------------------------------------------------

func TestReadGreenground(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/greenground.repo.car")
	require.NoError(t, err)

	header, blocks, err := ReadAll(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, 1, header.Version)
	require.NotEmpty(t, header.Roots)
	require.NotEmpty(t, blocks)

	for _, b := range blocks {
		require.True(t, b.CID.Defined())
		require.NotEmpty(t, b.Data)
	}
}

func TestReadRepoSlice(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/repo_slice.car")
	require.NoError(t, err)

	header, blocks, err := ReadAll(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, 1, header.Version)
	require.NotEmpty(t, header.Roots)
	require.NotEmpty(t, blocks)
}

func TestRoundTrip_Greenground(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/greenground.repo.car")
	require.NoError(t, err)

	result, err := RoundTrip(data)
	require.NoError(t, err)
	require.Equal(t, data, result)
}

func TestRoundTrip_RepoSlice(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/repo_slice.car")
	require.NoError(t, err)

	result, err := RoundTrip(data)
	require.NoError(t, err)
	require.Equal(t, data, result)
}

// -------------------------------------------------------------------
// Write and read back
// -------------------------------------------------------------------

func TestWriteAndRead(t *testing.T) {
	t.Parallel()
	data1 := []byte("hello world")
	data2 := []byte("test data")
	cid1 := cbor.ComputeCID(cbor.CodecRaw, data1)
	cid2 := cbor.ComputeCID(cbor.CodecRaw, data2)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{cid1})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid1, data1))
	require.NoError(t, w.WriteBlock(cid2, data2))

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, 1, r.Header().Version)
	require.Len(t, r.Header().Roots, 1)
	require.True(t, r.Header().Roots[0].Equal(cid1))

	b1, err := r.Next()
	require.NoError(t, err)
	require.True(t, b1.CID.Equal(cid1))
	require.Equal(t, data1, b1.Data)

	b2, err := r.Next()
	require.NoError(t, err)
	require.True(t, b2.CID.Equal(cid2))
	require.Equal(t, data2, b2.Data)

	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestWriteAndRead_MultipleRoots(t *testing.T) {
	t.Parallel()
	data1 := []byte("root1")
	data2 := []byte("root2")
	cid1 := cbor.ComputeCID(cbor.CodecRaw, data1)
	cid2 := cbor.ComputeCID(cbor.CodecRaw, data2)

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{cid1, cid2})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid1, data1))
	require.NoError(t, w.WriteBlock(cid2, data2))

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Len(t, r.Header().Roots, 2)
	require.True(t, r.Header().Roots[0].Equal(cid1))
	require.True(t, r.Header().Roots[1].Equal(cid2))
}

func TestWriteAndRead_EmptyBlockData(t *testing.T) {
	t.Parallel()
	// A block with empty data is valid (e.g. raw codec with zero bytes).
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte{})

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, []byte{}))

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	b, err := r.Next()
	require.NoError(t, err)
	require.True(t, b.CID.Equal(cid))
	require.Empty(t, b.Data)
}

// -------------------------------------------------------------------
// CID integrity: block data should match its CID
// -------------------------------------------------------------------

func TestBlockCIDIntegrity_Greenground(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/greenground.repo.car")
	require.NoError(t, err)

	_, blocks, err := ReadAll(bytes.NewReader(data))
	require.NoError(t, err)

	for i, b := range blocks {
		computed := cbor.ComputeCID(b.CID.Codec(), b.Data)
		require.True(t, b.CID.Equal(computed),
			"block %d CID mismatch: expected %s, got %s", i, b.CID.String(), computed.String())
	}
}

func TestBlockCIDIntegrity_RepoSlice(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/repo_slice.car")
	require.NoError(t, err)

	_, blocks, err := ReadAll(bytes.NewReader(data))
	require.NoError(t, err)

	for i, b := range blocks {
		computed := cbor.ComputeCID(b.CID.Codec(), b.Data)
		require.True(t, b.CID.Equal(computed),
			"block %d CID mismatch: expected %s, got %s", i, b.CID.String(), computed.String())
	}
}

// -------------------------------------------------------------------
// Error cases
// -------------------------------------------------------------------

func TestNewReader_InvalidVersion(t *testing.T) {
	t.Parallel()
	headerBytes, err := cbor.Marshal(map[string]any{
		"version": int64(2),
		"roots":   []any{cbor.ComputeCID(cbor.CodecRaw, []byte("x"))},
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, uint64(len(headerBytes))))
	buf.Write(headerBytes)

	_, err = NewReader(bytes.NewReader(buf.Bytes()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported version")
}

func TestNewReader_MissingRoots(t *testing.T) {
	t.Parallel()
	headerBytes, err := cbor.Marshal(map[string]any{
		"version": int64(1),
		"roots":   []any{},
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, uint64(len(headerBytes))))
	buf.Write(headerBytes)

	_, err = NewReader(bytes.NewReader(buf.Bytes()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty")
}

func TestNewReader_MissingVersionField(t *testing.T) {
	t.Parallel()
	headerBytes, err := cbor.Marshal(map[string]any{
		"roots": []any{cbor.ComputeCID(cbor.CodecRaw, []byte("x"))},
	})
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, uint64(len(headerBytes))))
	buf.Write(headerBytes)

	_, err = NewReader(bytes.NewReader(buf.Bytes()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}

func TestNewReader_EmptyInput(t *testing.T) {
	t.Parallel()
	_, err := NewReader(bytes.NewReader(nil))
	require.Error(t, err)
}

func TestNewReader_TruncatedHeader(t *testing.T) {
	t.Parallel()
	// Write a varint claiming 100 bytes of header, but only provide 5.
	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, 100))
	buf.Write([]byte{1, 2, 3, 4, 5})

	_, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.Error(t, err)
}

func TestNewWriter_EmptyRoots(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	_, err := NewWriter(&buf, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty")
}

func TestNext_TruncatedBlock(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("data"))

	// Write a valid header, then a block varint claiming 1000 bytes but provide only 10.
	var buf bytes.Buffer
	_, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)

	// Manually write a truncated block.
	require.NoError(t, writeUvarint(&buf, 1000))
	buf.Write(make([]byte, 10))

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	_, err = r.Next()
	require.Error(t, err)
}

// -------------------------------------------------------------------
// Round-trip determinism
// -------------------------------------------------------------------

func TestRoundTrip_Deterministic(t *testing.T) {
	t.Parallel()
	// Write → read → write should produce identical bytes.
	data := []byte("test content for determinism")
	cid := cbor.ComputeCID(cbor.CodecRaw, data)

	var buf1 bytes.Buffer
	w, err := NewWriter(&buf1, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, data))

	result, err := RoundTrip(buf1.Bytes())
	require.NoError(t, err)
	require.Equal(t, buf1.Bytes(), result)
}

func TestReadAll_NoBlocks(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("root"))

	var buf bytes.Buffer
	_, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)
	// Don't write any blocks.

	header, blocks, err := ReadAll(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, 1, header.Version)
	require.Len(t, header.Roots, 1)
	require.Empty(t, blocks)
}

// -------------------------------------------------------------------
// CAR file fixture from TypeScript reference implementation
// -------------------------------------------------------------------

type carFixture struct {
	Root   string `json:"root"`
	Blocks []struct {
		CID   string `json:"cid"`
		Bytes string `json:"bytes"` // base64
	} `json:"blocks"`
	Car string `json:"car"` // base64
}

func TestCARFixture_Read(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/car-file-fixtures.json")
	require.NoError(t, err)

	var fixtures []carFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.NotEmpty(t, fixtures)

	for _, f := range fixtures {
		carBytes, err := base64.RawStdEncoding.DecodeString(f.Car)
		require.NoError(t, err)

		cr, err := NewReader(bytes.NewReader(carBytes))
		require.NoError(t, err)
		require.Equal(t, 1, cr.Header().Version)
		require.Len(t, cr.Header().Roots, 1)
		require.Equal(t, f.Root, cr.Header().Roots[0].String())

		// Read all blocks and verify they match the fixture.
		var blocks []Block
		for {
			b, err := cr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			blocks = append(blocks, b)
		}
		require.Len(t, blocks, len(f.Blocks))

		for i, b := range blocks {
			require.Equal(t, f.Blocks[i].CID, b.CID.String())
			expectedBytes, err := base64.RawStdEncoding.DecodeString(f.Blocks[i].Bytes)
			require.NoError(t, err)
			require.Equal(t, expectedBytes, b.Data, "block %d data mismatch", i)
		}
	}
}

func TestCARFixture_Write(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/car-file-fixtures.json")
	require.NoError(t, err)

	var fixtures []carFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))

	for _, f := range fixtures {
		rootCID, err := cbor.ParseCIDString(f.Root)
		require.NoError(t, err)

		var buf bytes.Buffer
		w, err := NewWriter(&buf, []cbor.CID{rootCID})
		require.NoError(t, err)

		for _, fb := range f.Blocks {
			blockCID, err := cbor.ParseCIDString(fb.CID)
			require.NoError(t, err)
			blockData, err := base64.RawStdEncoding.DecodeString(fb.Bytes)
			require.NoError(t, err)
			require.NoError(t, w.WriteBlock(blockCID, blockData))
		}

		expected, err := base64.RawStdEncoding.DecodeString(f.Car)
		require.NoError(t, err)
		require.Equal(t, expected, buf.Bytes(), "CAR file output doesn't match TS reference")
	}
}

// -------------------------------------------------------------------
// CID mismatch detection (from TS car.test.ts "verifies CIDs")
// -------------------------------------------------------------------

func TestBlockCIDMismatch(t *testing.T) {
	t.Parallel()
	// Write a CAR with a block whose data doesn't match its CID.
	goodData := []byte("correct data")
	badData := []byte("wrong data")
	goodCID := cbor.ComputeCID(cbor.CodecRaw, goodData)
	rootCID := cbor.ComputeCID(cbor.CodecRaw, []byte("root"))

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{rootCID})
	require.NoError(t, err)
	// Write block with mismatched CID and data.
	require.NoError(t, w.WriteBlock(goodCID, badData))

	// Read it back — the CID won't match the data.
	cr, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	b, err := cr.Next()
	require.NoError(t, err)

	// Verify CID mismatch.
	computed := cbor.ComputeCID(b.CID.Codec(), b.Data)
	require.False(t, b.CID.Equal(computed), "CID should not match tampered data")
}
