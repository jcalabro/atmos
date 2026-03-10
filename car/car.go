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

// MaxBlockSize is the maximum block size allowed when reading CAR files.
var MaxBlockSize uint64 = 1 << 20 // 1 MiB

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
	r      io.Reader
	header Header
}

// NewReader creates a Reader by reading and validating the CAR v1 header.
func NewReader(r io.Reader) (*Reader, error) {
	// Read header length varint.
	headerLen, err := readUvarintFromReader(r)
	if err != nil {
		return nil, fmt.Errorf("car: reading header length: %w", err)
	}

	if headerLen > MaxBlockSize {
		return nil, fmt.Errorf("car: header length %d exceeds max size", headerLen)
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
			hasRoot = true
			arrLen, newPos, err := cbor.ReadArrayHeader(headerBuf, pos)
			if err != nil {
				return nil, fmt.Errorf("car: header roots: %w", err)
			}
			pos = newPos
			roots = make([]cbor.CID, arrLen)
			for i := range roots {
				roots[i], pos, err = cbor.ReadCIDLink(headerBuf, pos)
				if err != nil {
					return nil, fmt.Errorf("car: root %d: %w", i, err)
				}
			}
		case "version":
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
		r: r,
		header: Header{
			Version: int(ver),
			Roots:   roots,
		},
	}, nil
}

// Header returns the CAR header.
func (r *Reader) Header() Header {
	return r.header
}

// Next reads the next block. Returns io.EOF when there are no more blocks.
func (r *Reader) Next() (Block, error) {
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
	if blockLen > MaxBlockSize {
		return Block{}, fmt.Errorf("car: block length %d exceeds max size", blockLen)
	}

	// Read block bytes (CID + data).
	buf := make([]byte, blockLen)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return Block{}, fmt.Errorf("car: reading block: %w", err)
	}

	// Parse CID from front of buffer.
	cid, cidLen, err := cbor.ParseCIDPrefix(buf)
	if err != nil {
		return Block{}, fmt.Errorf("car: parsing block CID: %w", err)
	}

	return Block{
		CID:  cid,
		Data: buf[cidLen:],
	}, nil
}

// Writer writes a CAR v1 file.
type Writer struct {
	w   io.Writer
	buf [46]byte // scratch: varint(≤10) + CID(36)
}

// NewWriter creates a Writer and writes the CAR v1 header.
func NewWriter(w io.Writer, roots []cbor.CID) (*Writer, error) {
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

	// Write header length varint + header in one write.
	out := make([]byte, 0, 10+len(headerBytes))
	out = cbor.AppendUvarint(out, uint64(len(headerBytes)))
	out = append(out, headerBytes...)

	if _, err := w.Write(out); err != nil {
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

	if _, err := w.w.Write(buf); err != nil {
		return err
	}

	_, err := w.w.Write(data)
	return err
}

// readUvarintFromReader reads a single unsigned varint from an io.Reader.
func readUvarintFromReader(r io.Reader) (uint64, error) {
	// Fast path for types that implement io.ByteReader (bytes.Reader, bufio.Reader, etc.).
	if br, ok := r.(io.ByteReader); ok {
		return readUvarintByteReader(br)
	}

	var buf [1]byte
	var x uint64
	var s uint
	for range 10 {
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return 0, err
		}

		b := buf[0]
		if b < 0x80 {
			x |= uint64(b) << s
			return x, nil
		}

		x |= uint64(b&0x7F) << s
		s += 7
	}

	return 0, errors.New("varint too long")
}

// readUvarintByteReader reads a varint using the io.ByteReader interface directly.
func readUvarintByteReader(br io.ByteReader) (uint64, error) {
	var x uint64
	var s uint
	for range 10 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}

		if b < 0x80 {
			x |= uint64(b) << s
			return x, nil
		}

		x |= uint64(b&0x7F) << s
		s += 7
	}

	return 0, errors.New("varint too long")
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
