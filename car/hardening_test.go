package car

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// buildHeader constructs a CAR header with a caller-controlled roots array
// count, emitting only `nCIDs` actual CID links. When declaredRoots greatly
// exceeds nCIDs this exercises the attacker-controlled allocation path.
func buildHeader(t *testing.T, declaredRoots uint64, nCIDs int) []byte {
	t.Helper()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))

	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = cbor.AppendText(hdr, "roots")
	hdr = cbor.AppendArrayHeader(hdr, declaredRoots)
	for range nCIDs {
		hdr = cbor.AppendCIDLink(hdr, &cid)
	}
	hdr = cbor.AppendText(hdr, "version")
	hdr = cbor.AppendUint(hdr, 1)

	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, uint64(len(hdr))))
	buf.Write(hdr)
	return buf.Bytes()
}

// C1: a header declaring an enormous roots array must return an error rather
// than panicking (slice bounds / makeslice) or allocating gigabytes before the
// per-root parse fails.
func TestNewReader_HugeRootsArray_NoPanic(t *testing.T) {
	t.Parallel()
	// Header is only a few bytes but claims billions of roots.
	data := buildHeader(t, 1<<60, 0)

	_, err := NewReader(bytes.NewReader(data))
	require.Error(t, err)
}

// C1 (amplification): even a "modest" oversized count that is far larger than
// the header could possibly contain must be rejected without allocating.
func TestNewReader_OversizedRootsArray_Rejected(t *testing.T) {
	t.Parallel()
	data := buildHeader(t, 5_000_000, 0)

	_, err := NewReader(bytes.NewReader(data))
	require.Error(t, err)
}

// A header whose declared roots count matches the emitted CIDs must still work.
func TestNewReader_RootsArray_HonestCount(t *testing.T) {
	t.Parallel()
	data := buildHeader(t, 1, 1)

	r, err := NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, r.Header().Roots, 1)
}

// H1: a stream that ends in the middle of a block-length varint (a continuation
// byte followed by EOF) is truncation, not a clean end-of-blocks. It must be
// reported as an error, never silently accepted as a complete CAR.
func TestNext_TruncatedLengthVarint_NotCleanEOF(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, []byte("x")))

	// Append a lone varint continuation byte then let the stream end.
	buf.WriteByte(0x80)

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// First block reads fine.
	_, err = r.Next()
	require.NoError(t, err)

	// The dangling continuation byte must surface as an error, not io.EOF.
	_, err = r.Next()
	require.Error(t, err)
	require.False(t, errors.Is(err, io.EOF), "truncated varint must not read as clean EOF")
}

// A mid-varint truncation is an unexpected end of stream, so the error must
// satisfy errors.Is(err, io.ErrUnexpectedEOF). Callers (e.g. a backfill retry
// loop) classify retryable network truncations by that sentinel; if a
// truncated block-length varint does not match it, a transient short read is
// misclassified as a permanent failure and the partially-downloaded repo is
// dropped instead of retried.
func TestNext_TruncatedLengthVarint_IsUnexpectedEOF(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, []byte("x")))
	buf.WriteByte(0x80)

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	_, err = r.Next()
	require.NoError(t, err)

	_, err = r.Next()
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"a mid-varint truncation must report as io.ErrUnexpectedEOF so retry classification treats it as a transient short read")
}

// H1 via the non-ByteReader (slow) path.
func TestNext_TruncatedLengthVarint_SlowPath(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))

	var buf bytes.Buffer
	w, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, []byte("x")))
	buf.WriteByte(0x80)

	// bareReader (defined in car_test.go) does not implement io.ByteReader,
	// forcing the slow path.
	r, err := NewReader(&bareReader{bytes.NewReader(buf.Bytes())})
	require.NoError(t, err)

	_, err = r.Next()
	require.NoError(t, err)

	_, err = r.Next()
	require.Error(t, err)
	require.False(t, errors.Is(err, io.EOF))
}

// M5: a non-minimally encoded block-length varint must be rejected.
func TestNext_NonMinimalLengthVarint_Rejected(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))

	var buf bytes.Buffer
	_, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)

	// Encode block length 38 non-minimally as [0xa6, 0x00] (== 38 but 2 bytes).
	buf.Write([]byte{0xa6, 0x00})
	buf.Write(make([]byte, 38))

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	_, err = r.Next()
	require.Error(t, err)
}

// M5: an overflowing 10-byte varint must be rejected, not silently wrapped.
func TestNext_OverflowLengthVarint_Rejected(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecRaw, []byte("x"))

	var buf bytes.Buffer
	_, err := NewWriter(&buf, []cbor.CID{cid})
	require.NoError(t, err)

	// 10 continuation-ish bytes that exceed 64 bits.
	buf.Write([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02})

	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	_, err = r.Next()
	require.Error(t, err)
}
