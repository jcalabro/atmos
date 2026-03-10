package cbor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

// MaxSize is the maximum allocation size (in bytes or elements) allowed when
// decoding untrusted CBOR input. It can be overridden by users.
var MaxSize uint64 = 1 << 20 // 1 MiB

// MaxDepth is the maximum nesting depth allowed when decoding CBOR.
var MaxDepth = 128

// Decoder reads DAG-CBOR from an io.Reader with strict validation.
type Decoder struct {
	r     io.Reader
	buf   [9]byte
	depth int
}

// NewDecoder creates a new DAG-CBOR decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// readByte reads a single byte.
func (d *Decoder) readByte() (byte, error) {
	_, err := io.ReadFull(d.r, d.buf[:1])
	return d.buf[0], err
}

// readN reads exactly n bytes.
func (d *Decoder) readN(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(d.r, buf)
	return buf, err
}

// readHeader reads a CBOR item header and returns the major type, additional info, and argument.
// Validates minimal encoding for non-simple types.
func (d *Decoder) readHeader() (major byte, additional byte, arg uint64, err error) {
	b, err := d.readByte()
	if err != nil {
		return 0, 0, 0, err
	}

	major = b >> 5
	additional = b & 0x1F

	// For major type 7 (simple values/floats), additional info has different semantics.
	if major == majorSimple {
		switch {
		case additional < 24:
			return major, additional, uint64(additional), nil
		case additional == 24:
			v, err := d.readByte()
			if err != nil {
				return 0, 0, 0, err
			}
			return major, additional, uint64(v), nil
		case additional == 25:
			// float16 — not allowed in DAG-CBOR
			return 0, 0, 0, errors.New("cbor: float16 not allowed in DAG-CBOR")
		case additional == 26:
			// float32 — not allowed in DAG-CBOR
			return 0, 0, 0, errors.New("cbor: float32 not allowed in DAG-CBOR")
		case additional == 27:
			// float64
			if _, err := io.ReadFull(d.r, d.buf[:8]); err != nil {
				return 0, 0, 0, err
			}
			return major, additional, binary.BigEndian.Uint64(d.buf[:8]), nil
		case additional == 31:
			return 0, 0, 0, errors.New("cbor: break stop code not allowed in DAG-CBOR")
		default:
			return 0, 0, 0, fmt.Errorf("cbor: reserved additional info %d", additional)
		}
	}

	// For all other major types, validate minimal encoding.
	switch {
	case additional < 24:
		return major, additional, uint64(additional), nil
	case additional == 24:
		v, err := d.readByte()
		if err != nil {
			return 0, 0, 0, err
		}
		if v < 24 {
			return 0, 0, 0, errors.New("cbor: non-minimal integer encoding")
		}
		return major, additional, uint64(v), nil
	case additional == 25:
		if _, err := io.ReadFull(d.r, d.buf[:2]); err != nil {
			return 0, 0, 0, err
		}
		v := binary.BigEndian.Uint16(d.buf[:2])
		if v <= 0xFF {
			return 0, 0, 0, errors.New("cbor: non-minimal integer encoding")
		}
		return major, additional, uint64(v), nil
	case additional == 26:
		if _, err := io.ReadFull(d.r, d.buf[:4]); err != nil {
			return 0, 0, 0, err
		}
		v := binary.BigEndian.Uint32(d.buf[:4])
		if v <= 0xFFFF {
			return 0, 0, 0, errors.New("cbor: non-minimal integer encoding")
		}
		return major, additional, uint64(v), nil
	case additional == 27:
		if _, err := io.ReadFull(d.r, d.buf[:8]); err != nil {
			return 0, 0, 0, err
		}
		v := binary.BigEndian.Uint64(d.buf[:8])
		if v <= 0xFFFFFFFF {
			return 0, 0, 0, errors.New("cbor: non-minimal integer encoding")
		}
		return major, additional, v, nil
	case additional == 31:
		return 0, 0, 0, errors.New("cbor: indefinite length not allowed in DAG-CBOR")
	default:
		return 0, 0, 0, fmt.Errorf("cbor: reserved additional info %d", additional)
	}
}

// ReadValue reads one DAG-CBOR value and returns it as a Go value.
func (d *Decoder) ReadValue() (any, error) {
	d.depth++
	defer func() { d.depth-- }()
	if d.depth > MaxDepth {
		return nil, fmt.Errorf("cbor: exceeded max nesting depth of %d", MaxDepth)
	}

	major, additional, arg, err := d.readHeader()
	if err != nil {
		return nil, err
	}

	switch major {
	case majorUint:
		if arg > math.MaxInt64 {
			return nil, errors.New("cbor: unsigned integer exceeds int64 range")
		}
		return int64(arg), nil

	case majorNegInt:
		if arg > math.MaxInt64 {
			return nil, errors.New("cbor: negative integer overflow")
		}
		return -1 - int64(arg), nil

	case majorBytes:
		if arg > MaxSize {
			return nil, fmt.Errorf("cbor: byte string length %d exceeds max size %d", arg, MaxSize)
		}
		if arg > uint64(math.MaxInt) {
			return nil, errors.New("cbor: byte string length exceeds platform int size")
		}
		return d.readN(int(arg))

	case majorText:
		if arg > MaxSize {
			return nil, fmt.Errorf("cbor: text string length %d exceeds max size %d", arg, MaxSize)
		}
		if arg > uint64(math.MaxInt) {
			return nil, errors.New("cbor: text string length exceeds platform int size")
		}
		b, err := d.readN(int(arg))
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(b) {
			return nil, errors.New("cbor: text string contains invalid UTF-8")
		}
		return string(b), nil

	case majorArray:
		if arg > MaxSize {
			return nil, fmt.Errorf("cbor: array length %d exceeds max size %d", arg, MaxSize)
		}
		arr := make([]any, arg)
		for i := range arg {
			arr[i], err = d.ReadValue()
			if err != nil {
				return nil, err
			}
		}
		return arr, nil

	case majorMap:
		if arg > MaxSize {
			return nil, fmt.Errorf("cbor: map length %d exceeds max size %d", arg, MaxSize)
		}
		return d.readMap(arg)

	case majorTag:
		if arg != tagCIDLink {
			return nil, fmt.Errorf("cbor: unsupported tag %d, only tag 42 allowed in DAG-CBOR", arg)
		}
		return d.readCIDLink()

	case majorSimple:
		return d.readSimple(additional, arg)

	default:
		return nil, fmt.Errorf("cbor: unknown major type %d", major)
	}
}

// readMap reads a CBOR map, validating string keys, ordering, and no duplicates.
func (d *Decoder) readMap(count uint64) (map[string]any, error) {
	m := make(map[string]any, count)
	var prevKey string
	for i := range count {
		major, _, arg, err := d.readHeader()
		if err != nil {
			return nil, err
		}
		if major != majorText {
			return nil, fmt.Errorf("cbor: map key must be text string, got major type %d", major)
		}
		if arg > MaxSize {
			return nil, fmt.Errorf("cbor: map key length %d exceeds max size %d", arg, MaxSize)
		}
		if arg > uint64(math.MaxInt) {
			return nil, errors.New("cbor: map key length exceeds platform int size")
		}
		keyBytes, err := d.readN(int(arg))
		if err != nil {
			return nil, err
		}
		key := string(keyBytes)

		if i > 0 {
			cmp := compareCBORKeys(prevKey, key)
			if cmp == 0 {
				return nil, fmt.Errorf("cbor: duplicate map key %q", key)
			}
			if cmp > 0 {
				return nil, errors.New("cbor: map keys not sorted (DAG-CBOR requires sorted keys)")
			}
		}
		prevKey = key

		val, err := d.ReadValue()
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

// readCIDLink reads a CID link (tag 42 already consumed).
func (d *Decoder) readCIDLink() (CID, error) {
	major, _, arg, err := d.readHeader()
	if err != nil {
		return CID{}, err
	}
	if major != majorBytes {
		return CID{}, errors.New("cbor: tag 42 must wrap a byte string")
	}
	if arg > MaxSize {
		return CID{}, fmt.Errorf("cbor: CID byte string length %d exceeds max size %d", arg, MaxSize)
	}
	if arg > uint64(math.MaxInt) {
		return CID{}, errors.New("cbor: CID byte string length exceeds platform int size")
	}
	payload, err := d.readN(int(arg))
	if err != nil {
		return CID{}, err
	}
	if len(payload) == 0 {
		return CID{}, errors.New("cbor: tag 42 payload is empty")
	}
	if payload[0] != 0x00 {
		return CID{}, fmt.Errorf("cbor: tag 42 payload must start with 0x00, got 0x%02x", payload[0])
	}
	return ParseCIDBytes(payload[1:])
}

// readSimple handles major type 7 (simple values and floats).
func (d *Decoder) readSimple(additional byte, arg uint64) (any, error) {
	switch additional {
	case simpleFalse:
		return false, nil
	case simpleTrue:
		return true, nil
	case simpleNull:
		return nil, nil
	case simpleFloat64:
		f := math.Float64frombits(arg)
		if math.IsNaN(f) {
			return nil, errors.New("cbor: NaN not allowed in DAG-CBOR")
		}
		if math.IsInf(f, 0) {
			return nil, errors.New("cbor: Infinity not allowed in DAG-CBOR")
		}
		return f, nil
	default:
		return nil, fmt.Errorf("cbor: unsupported simple value (additional info %d)", additional)
	}
}
