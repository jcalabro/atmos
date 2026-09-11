// Package car implements CAR v1 (Content Addressable aRchive) file I/O.
//
// CAR v1 is a sequential format: a varint-length-prefixed DAG-CBOR header
// followed by varint-length-prefixed blocks (CID + data).
package car

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/jcalabro/atmos/cbor"
)

const (
	// DefaultMaxHeaderSize is the default maximum encoded CAR header size.
	DefaultMaxHeaderSize uint64 = 1 << 20 // 1 MiB
	// DefaultMaxBlockSize is the default maximum encoded CAR block size,
	// including its CID prefix.
	DefaultMaxBlockSize uint64 = 1 << 20 // 1 MiB
)

// MaxBlockSize is the legacy default maximum header and block size used by
// [NewReader]. New code that needs different limits should use
// [NewReaderWithOptions], whose limits are immutable and reader-local.
//
// Deprecated: use DefaultReaderOptions and NewReaderWithOptions. Mutating this
// package variable concurrently with NewReader is unsafe.
var MaxBlockSize uint64 = DefaultMaxBlockSize

// ErrInvalidReaderOptions classifies invalid reader limit configuration.
var ErrInvalidReaderOptions = errors.New("car: invalid reader options")

// ReaderOptions configures immutable limits for one Reader. Both fields must
// be nonzero. Use [DefaultReaderOptions] when the public-repo defaults apply.
type ReaderOptions struct {
	MaxHeaderSize uint64
	MaxBlockSize  uint64
}

// DefaultReaderOptions returns the public-repo CAR reader defaults.
func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		MaxHeaderSize: DefaultMaxHeaderSize,
		MaxBlockSize:  DefaultMaxBlockSize,
	}
}

// LimitError reports that a framed CAR item exceeded its configured limit.
type LimitError struct {
	Kind   string
	Actual uint64
	Limit  uint64
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("car: %s length %d exceeds max size %d", e.Kind, e.Actual, e.Limit)
}

// Header is the CAR v1 header.
type Header struct {
	Version int
	Roots   []cbor.CID
}

// Block is a single block in a CAR file.
type Block struct {
	CID  cbor.CID
	Data []byte
}

// Reader reads blocks from a CAR v1 file.
type Reader struct {
	r            io.Reader
	header       Header
	maxBlockSize uint64
	buf          []byte // reusable read buffer for NextInto
}

// NewReader creates a Reader by reading and validating the CAR v1 header.
func NewReader(r io.Reader) (*Reader, error) {
	legacyLimit := MaxBlockSize
	return NewReaderWithOptions(r, ReaderOptions{
		MaxHeaderSize: legacyLimit,
		MaxBlockSize:  legacyLimit,
	})
}

// NewReaderWithOptions creates a Reader with immutable, reader-local limits
// and reads and validates its CAR v1 header.
func NewReaderWithOptions(r io.Reader, opts ReaderOptions) (*Reader, error) {
	if opts.MaxHeaderSize == 0 {
		return nil, fmt.Errorf("%w: MaxHeaderSize must be nonzero", ErrInvalidReaderOptions)
	}
	if opts.MaxBlockSize == 0 {
		return nil, fmt.Errorf("%w: MaxBlockSize must be nonzero", ErrInvalidReaderOptions)
	}
	maxInt := uint64(^uint(0) >> 1)
	if opts.MaxHeaderSize > maxInt || opts.MaxBlockSize > maxInt {
		return nil, fmt.Errorf("%w: limits exceed platform allocation range", ErrInvalidReaderOptions)
	}
	// Read header length varint.
	headerLen, err := readUvarintFromReader(r)
	if err != nil {
		return nil, fmt.Errorf("car: reading header length: %w", err)
	}

	if headerLen > opts.MaxHeaderSize {
		return nil, &LimitError{Kind: "header", Actual: headerLen, Limit: opts.MaxHeaderSize}
	}

	// Read header bytes.
	headerBuf := make([]byte, headerLen)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, fmt.Errorf("car: reading header: %w", err)
	}

	// Decode header directly using CBOR decode helpers (no generic Unmarshal).
	var (
		roots   []cbor.CID
		ver     uint64
		hasVer  bool
		hasRoot bool
	)

	count, pos, err := cbor.ReadMapHeader(headerBuf, 0)
	if err != nil {
		return nil, fmt.Errorf("car: header: %w", err)
	}

	for range count {
		key, newPos, err := cbor.ReadText(headerBuf, pos)
		if err != nil {
			return nil, fmt.Errorf("car: header key: %w", err)
		}
		pos = newPos

		switch key {
		case "roots":
			if hasRoot {
				return nil, errors.New("car: header has duplicate 'roots' key")
			}
			hasRoot = true
			arrLen, newPos, err := cbor.ReadArrayHeader(headerBuf, pos)
			if err != nil {
				return nil, fmt.Errorf("car: header roots: %w", err)
			}
			pos = newPos
			// Each CID link occupies at least one header byte, so a declared
			// count larger than the remaining header is impossible. Bounding
			// here prevents an attacker-controlled count from triggering a
			// makeslice panic or a multi-gigabyte pre-allocation (C1).
			if arrLen > uint64(len(headerBuf)-pos) {
				return nil, fmt.Errorf("car: header declares %d roots but only %d header bytes remain", arrLen, len(headerBuf)-pos)
			}
			roots = make([]cbor.CID, 0, arrLen)
			for range arrLen {
				var root cbor.CID
				root, pos, err = cbor.ReadCIDLink(headerBuf, pos)
				if err != nil {
					return nil, fmt.Errorf("car: root %d: %w", len(roots), err)
				}
				roots = append(roots, root)
			}
		case "version":
			if hasVer {
				return nil, errors.New("car: header has duplicate 'version' key")
			}
			hasVer = true
			ver, pos, err = cbor.ReadUint(headerBuf, pos)
			if err != nil {
				return nil, fmt.Errorf("car: header version: %w", err)
			}
		default:
			pos, err = cbor.SkipValue(headerBuf, pos)
			if err != nil {
				return nil, fmt.Errorf("car: skipping header key %q: %w", key, err)
			}
		}
	}

	// Reject trailing bytes after the CBOR header map: a well-formed header
	// consumes exactly the declared header length. Extra bytes mean a malformed
	// or smuggled header.
	if pos != len(headerBuf) {
		return nil, fmt.Errorf("car: %d trailing bytes after header map", len(headerBuf)-pos)
	}

	if !hasVer {
		return nil, errors.New("car: header missing 'version'")
	}
	if ver != 1 {
		return nil, fmt.Errorf("car: unsupported version %d, expected 1", ver)
	}
	if !hasRoot {
		return nil, errors.New("car: header missing 'roots'")
	}
	if len(roots) == 0 {
		return nil, errors.New("car: header 'roots' must be non-empty")
	}

	return &Reader{
		r:            r,
		maxBlockSize: opts.MaxBlockSize,
		header: Header{
			Version: int(ver),
			Roots:   roots,
		},
	}, nil
}

// Header returns the CAR header.
func (r *Reader) Header() Header {
	header := r.header
	header.Roots = append([]cbor.CID(nil), r.header.Roots...)
	return header
}

// Next reads the next block. Returns io.EOF when there are no more blocks.
func (r *Reader) Next() (Block, error) {
	return r.next(r.maxBlockSize, false)
}

// NextWithLimit reads the next block with a role-specific limit. limit must be
// nonzero and no larger than the Reader's configured MaxBlockSize.
func (r *Reader) NextWithLimit(limit uint64) (Block, error) {
	return r.next(limit, false)
}

func (r *Reader) next(limit uint64, reuse bool) (Block, error) {
	if limit == 0 || limit > r.maxBlockSize {
		return Block{}, fmt.Errorf("%w: per-block limit must be in [1, %d], got %d", ErrInvalidReaderOptions, r.maxBlockSize, limit)
	}
	// Read block length varint.
	blockLen, err := readUvarintFromReader(r.r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Block{}, io.EOF
		}
		return Block{}, fmt.Errorf("car: reading block length: %w", err)
	}

	if blockLen == 0 {
		return Block{}, errors.New("car: zero-length block")
	}
	if blockLen > limit {
		return Block{}, &LimitError{Kind: "block", Actual: blockLen, Limit: limit}
	}

	// Read block bytes (CID + data).
	n := int(blockLen)
	var buf []byte
	if reuse {
		if cap(r.buf) < n {
			r.buf = make([]byte, n)
		} else {
			r.buf = r.buf[:n]
		}
		buf = r.buf
	} else {
		buf = make([]byte, n)
	}
	if _, err := io.ReadFull(r.r, buf); err != nil {
		// The block length was fully read, so any EOF here (including a clean
		// io.EOF when zero body bytes follow) is a stream truncated mid-block,
		// not a legitimate end-of-blocks. Surface it as a truncation so callers
		// never mistake a cut-off CAR for a complete one. The clean terminus is
		// detected earlier, at the block-length varint (a bare io.EOF there).
		if errors.Is(err, io.EOF) {
			return Block{}, errVarintTruncated
		}
		return Block{}, fmt.Errorf("car: reading block: %w", err)
	}

	// Parse CID from front of buffer.
	cid, cidLen, err := cbor.ParseCIDPrefix(buf)
	if err != nil {
		return Block{}, fmt.Errorf("car: parsing block CID: %w", err)
	}

	data := buf[cidLen:]

	// Verify the block data matches the claimed CID.
	expected := cbor.ComputeCID(cid.Codec(), data)
	if !expected.Equal(cid) {
		return Block{}, fmt.Errorf("car: block CID mismatch: claimed %s, computed %s", cid.String(), expected.String())
	}

	return Block{
		CID:  cid,
		Data: data,
	}, nil
}

// NextInto reads the next block, reusing the Reader's internal buffer to
// reduce allocations when reading many blocks sequentially. The returned
// Block.Data is valid only until the next call to NextInto — callers must
// copy it if they need to retain it.
func (r *Reader) NextInto() (Block, error) {
	return r.next(r.maxBlockSize, true)
}

// NextIntoWithLimit is like [Reader.NextWithLimit] but reuses the Reader's
// internal buffer. The returned data remains valid only until the next read.
func (r *Reader) NextIntoWithLimit(limit uint64) (Block, error) {
	return r.next(limit, true)
}

// Writer writes a CAR v1 file.
type Writer struct {
	w   io.Writer
	buf [46]byte // scratch: varint(≤10) + CID(36)
}

// ErrInvalidWriterOptions classifies invalid writer limit configuration.
var ErrInvalidWriterOptions = errors.New("car: invalid writer options")

// WriterOptions configures immutable limits for one Writer.
type WriterOptions struct {
	MaxHeaderSize uint64
}

// NewWriter creates a Writer and writes the CAR v1 header.
func NewWriter(w io.Writer, roots []cbor.CID) (*Writer, error) {
	return newWriter(w, roots, 0)
}

// NewWriterWithOptions creates a Writer, enforces its reader-symmetric header
// limit before emitting bytes, and writes the CAR v1 header.
func NewWriterWithOptions(w io.Writer, roots []cbor.CID, opts WriterOptions) (*Writer, error) {
	if opts.MaxHeaderSize == 0 {
		return nil, fmt.Errorf("%w: MaxHeaderSize must be nonzero", ErrInvalidWriterOptions)
	}
	return newWriter(w, roots, opts.MaxHeaderSize)
}

func newWriter(w io.Writer, roots []cbor.CID, maxHeaderSize uint64) (*Writer, error) {
	if len(roots) == 0 {
		return nil, errors.New("car: roots must be non-empty")
	}

	// Encode header as DAG-CBOR directly (no generic Marshal to avoid allocations).
	// DAG-CBOR key sort: "roots" (5) before "version" (7).
	headerBytes := make([]byte, 0, 64+len(roots)*40)
	headerBytes = cbor.AppendMapHeader(headerBytes, 2)
	headerBytes = cbor.AppendText(headerBytes, "roots")
	headerBytes = cbor.AppendArrayHeader(headerBytes, uint64(len(roots)))

	for i := range roots {
		headerBytes = cbor.AppendCIDLink(headerBytes, &roots[i])
	}

	headerBytes = cbor.AppendText(headerBytes, "version")
	headerBytes = cbor.AppendUint(headerBytes, 1)
	if maxHeaderSize > 0 && uint64(len(headerBytes)) > maxHeaderSize {
		return nil, &LimitError{Kind: "header", Actual: uint64(len(headerBytes)), Limit: maxHeaderSize}
	}

	// Write header length varint + header in one write.
	out := make([]byte, 0, 10+len(headerBytes))
	out = cbor.AppendUvarint(out, uint64(len(headerBytes)))
	out = append(out, headerBytes...)

	if err := writeFull(w, out); err != nil {
		return nil, fmt.Errorf("car: writing header: %w", err)
	}

	return &Writer{w: w}, nil
}

// WriteBlock writes a single block (CID + data) to the CAR file.
func (w *Writer) WriteBlock(cid cbor.CID, data []byte) error {
	cidLen := cbor.CIDByteLen(&cid)
	blockLen := uint64(cidLen + len(data))

	// Build varint + CID into scratch buffer (no allocation).
	buf := w.buf[:0]
	buf = cbor.AppendUvarint(buf, blockLen)
	buf = cid.AppendBytes(buf)

	if err := writeFull(w.w, buf); err != nil {
		return err
	}

	return writeFull(w.w, data)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// maxVarintLen is the maximum number of bytes a length varint may occupy.
// The multiformats unsigned-varint spec caps practical varints at 9 bytes
// (63 bits), which comfortably covers any legitimate CAR block length.
const maxVarintLen = 9

// errVarintTruncated indicates the stream ended partway through a varint (after
// at least one continuation byte). This is distinct from a clean io.EOF that
// occurs before any varint byte, which legitimately marks end-of-blocks.
//
// It wraps io.ErrUnexpectedEOF: a mid-varint cut is a short read of an
// otherwise-valid stream, semantically the same as a mid-block-data short read
// (which io.ReadFull already reports as io.ErrUnexpectedEOF). Wrapping keeps
// both truncation sites classifiable by a single sentinel so retry logic
// treats a cut-off download as transient rather than a permanent parse error.
var errVarintTruncated = fmt.Errorf("car: truncated varint: %w", io.ErrUnexpectedEOF)

// readUvarintFromReader reads a single unsigned varint from an io.Reader.
//
// A clean io.EOF before the first byte is returned verbatim (end of blocks).
// An EOF after one or more continuation bytes is reported as a truncation error
// so callers never mistake a cut-off stream for a complete CAR (H1). Non-minimal
// and overflowing encodings are rejected (M5).
func readUvarintFromReader(r io.Reader) (uint64, error) {
	// Fast path for types that implement io.ByteReader (bytes.Reader, bufio.Reader, etc.).
	if br, ok := r.(io.ByteReader); ok {
		return readUvarint(br)
	}
	// Slow path: a stack-allocated adapter avoids a per-call closure allocation.
	return readUvarint(&byteReaderAdapter{r: r})
}

// byteReaderAdapter turns a plain io.Reader into an io.ByteReader without
// allocating per read.
type byteReaderAdapter struct {
	r   io.Reader
	buf [1]byte
}

func (a *byteReaderAdapter) ReadByte() (byte, error) {
	if _, err := io.ReadFull(a.r, a.buf[:]); err != nil {
		return 0, err
	}
	return a.buf[0], nil
}

// readUvarint reads a varint one byte at a time, enforcing minimal encoding, an
// overflow/length bound, and truncation detection.
func readUvarint(br io.ByteReader) (uint64, error) {
	var x uint64
	var s uint
	for i := range maxVarintLen {
		b, err := br.ReadByte()
		if err != nil {
			if i > 0 && errors.Is(err, io.EOF) {
				// Mid-varint EOF is truncation, not a clean terminus.
				return 0, errVarintTruncated
			}
			return 0, err
		}

		if b < 0x80 {
			if i > 0 && b == 0 {
				// A trailing zero group means the value could have been encoded
				// in fewer bytes.
				return 0, errors.New("car: non-minimal varint encoding")
			}
			return x | uint64(b)<<s, nil
		}

		x |= uint64(b&0x7F) << s
		s += 7
	}

	return 0, errors.New("car: varint too long")
}

// writeUvarint writes an unsigned varint to a writer.
func writeUvarint(w io.Writer, v uint64) error {
	var buf [10]byte
	n := 0

	for v >= 0x80 {
		buf[n] = byte(v) | 0x80
		v >>= 7
		n++
	}

	buf[n] = byte(v)
	n++

	_, err := w.Write(buf[:n])
	return err
}

// ReadAll reads all blocks from a CAR v1 reader into memory.
func ReadAll(r io.Reader) (Header, []Block, error) {
	cr, err := NewReader(r)
	if err != nil {
		return Header{}, nil, err
	}

	var blocks []Block
	for {
		b, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return Header{}, nil, err
		}

		blocks = append(blocks, b)
	}

	return cr.Header(), blocks, nil
}

// WriteAll writes a complete CAR v1 file.
func WriteAll(w io.Writer, roots []cbor.CID, blocks []Block) error {
	cw, err := NewWriter(w, roots)
	if err != nil {
		return err
	}

	for _, b := range blocks {
		if err := cw.WriteBlock(b.CID, b.Data); err != nil {
			return err
		}
	}

	return nil
}

// RoundTrip reads a CAR file and writes it back, useful for testing.
func RoundTrip(data []byte) ([]byte, error) {
	header, blocks, err := ReadAll(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := WriteAll(&buf, header.Roots, blocks); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
