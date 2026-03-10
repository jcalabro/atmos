package cbor

import (
	"errors"
	"io"
	"math"
	"sort"
)

// CBOR major types.
const (
	majorUint     byte = 0 // 0x00..0x1b
	majorNegInt   byte = 1 // 0x20..0x3b
	majorBytes    byte = 2 // 0x40..0x5b
	majorText     byte = 3 // 0x60..0x7b
	majorArray    byte = 4 // 0x80..0x9b
	majorMap      byte = 5 // 0xa0..0xbb
	majorTag      byte = 6 // 0xc0..0xdb
	majorSimple   byte = 7 // 0xe0..0xfb
	tagCIDLink         = 42
	simpleFalse   byte = 20
	simpleTrue    byte = 21
	simpleNull    byte = 22
	simpleFloat64 byte = 27
)

// Encoder writes DAG-CBOR to an io.Writer.
type Encoder struct {
	w   io.Writer
	buf [9]byte // scratch buffer for headers
}

// NewEncoder creates a new DAG-CBOR encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// writeHeader writes a CBOR major type + argument using minimal encoding.
func (e *Encoder) writeHeader(major byte, val uint64) error {
	m := major << 5
	switch {
	case val < 24:
		e.buf[0] = m | byte(val)
		_, err := e.w.Write(e.buf[:1])
		return err
	case val <= 0xFF:
		e.buf[0] = m | 24
		e.buf[1] = byte(val)
		_, err := e.w.Write(e.buf[:2])
		return err
	case val <= 0xFFFF:
		e.buf[0] = m | 25
		e.buf[1] = byte(val >> 8)
		e.buf[2] = byte(val)
		_, err := e.w.Write(e.buf[:3])
		return err
	case val <= 0xFFFFFFFF:
		e.buf[0] = m | 26
		e.buf[1] = byte(val >> 24)
		e.buf[2] = byte(val >> 16)
		e.buf[3] = byte(val >> 8)
		e.buf[4] = byte(val)
		_, err := e.w.Write(e.buf[:5])
		return err
	default:
		e.buf[0] = m | 27
		e.buf[1] = byte(val >> 56)
		e.buf[2] = byte(val >> 48)
		e.buf[3] = byte(val >> 40)
		e.buf[4] = byte(val >> 32)
		e.buf[5] = byte(val >> 24)
		e.buf[6] = byte(val >> 16)
		e.buf[7] = byte(val >> 8)
		e.buf[8] = byte(val)
		_, err := e.w.Write(e.buf[:9])
		return err
	}
}

// WriteNull writes a CBOR null.
func (e *Encoder) WriteNull() error {
	e.buf[0] = (majorSimple << 5) | simpleNull
	_, err := e.w.Write(e.buf[:1])
	return err
}

// WriteBool writes a CBOR boolean.
func (e *Encoder) WriteBool(v bool) error {
	if v {
		e.buf[0] = (majorSimple << 5) | simpleTrue
	} else {
		e.buf[0] = (majorSimple << 5) | simpleFalse
	}
	_, err := e.w.Write(e.buf[:1])
	return err
}

// WriteInt writes a signed integer with minimal CBOR encoding.
func (e *Encoder) WriteInt(v int64) error {
	if v >= 0 {
		return e.writeHeader(majorUint, uint64(v))
	}
	// CBOR negative: -1 - val, so val = -(v+1) = -v - 1
	return e.writeHeader(majorNegInt, uint64(-1-v))
}

// WriteFloat64 writes a 64-bit float. Rejects NaN and Infinity.
func (e *Encoder) WriteFloat64(v float64) error {
	if math.IsNaN(v) {
		return errors.New("cbor: NaN not allowed in DAG-CBOR")
	}
	if math.IsInf(v, 0) {
		return errors.New("cbor: Infinity not allowed in DAG-CBOR")
	}
	bits := math.Float64bits(v)
	e.buf[0] = (majorSimple << 5) | simpleFloat64
	e.buf[1] = byte(bits >> 56)
	e.buf[2] = byte(bits >> 48)
	e.buf[3] = byte(bits >> 40)
	e.buf[4] = byte(bits >> 32)
	e.buf[5] = byte(bits >> 24)
	e.buf[6] = byte(bits >> 16)
	e.buf[7] = byte(bits >> 8)
	e.buf[8] = byte(bits)
	_, err := e.w.Write(e.buf[:9])
	return err
}

// WriteString writes a CBOR text string.
func (e *Encoder) WriteString(s string) error {
	if err := e.writeHeader(majorText, uint64(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(e.w, s)
	return err
}

// WriteBytes writes a CBOR byte string.
func (e *Encoder) WriteBytes(b []byte) error {
	if err := e.writeHeader(majorBytes, uint64(len(b))); err != nil {
		return err
	}
	_, err := e.w.Write(b)
	return err
}

// WriteArrayHeader writes the header for a definite-length array.
func (e *Encoder) WriteArrayHeader(length uint64) error {
	return e.writeHeader(majorArray, length)
}

// WriteMapHeader writes the header for a definite-length map.
func (e *Encoder) WriteMapHeader(length uint64) error {
	return e.writeHeader(majorMap, length)
}

// WriteLink writes a CID link (CBOR tag 42 + byte string with 0x00 prefix).
func (e *Encoder) WriteLink(c CID) error {
	if err := e.writeHeader(majorTag, tagCIDLink); err != nil {
		return err
	}
	cidBytes := c.Bytes()
	if err := e.writeHeader(majorBytes, uint64(1+len(cidBytes))); err != nil {
		return err
	}
	e.buf[0] = 0x00
	if _, err := e.w.Write(e.buf[:1]); err != nil {
		return err
	}
	_, err := e.w.Write(cidBytes)
	return err
}

// WriteSortedMap writes a map with keys sorted by their CBOR-encoded bytes.
// This is required for DAG-CBOR canonical encoding.
func (e *Encoder) WriteSortedMap(pairs []MapPair) error {
	// Sort by CBOR-encoded key bytes.
	sort.Slice(pairs, func(i, j int) bool {
		return compareCBORKeys(pairs[i].Key, pairs[j].Key) < 0
	})
	if err := e.WriteMapHeader(uint64(len(pairs))); err != nil {
		return err
	}
	for _, p := range pairs {
		if err := e.WriteString(p.Key); err != nil {
			return err
		}
		if err := e.WriteValue(p.Value); err != nil {
			return err
		}
	}
	return nil
}

// MapPair is a key-value pair for map encoding.
type MapPair struct {
	Key   string // Map key (must be a string per DAG-CBOR rules).
	Value any    // Value to encode.
}

// WriteValue encodes an arbitrary Go value as DAG-CBOR.
func (e *Encoder) WriteValue(v any) error {
	switch val := v.(type) {
	case nil:
		return e.WriteNull()
	case bool:
		return e.WriteBool(val)
	case int64:
		return e.WriteInt(val)
	case int:
		return e.WriteInt(int64(val))
	case float64:
		return e.WriteFloat64(val)
	case string:
		return e.WriteString(val)
	case []byte:
		return e.WriteBytes(val)
	case CID:
		return e.WriteLink(val)
	case []any:
		if err := e.WriteArrayHeader(uint64(len(val))); err != nil {
			return err
		}
		for _, item := range val {
			if err := e.WriteValue(item); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		pairs := make([]MapPair, 0, len(val))
		for k, v := range val {
			pairs = append(pairs, MapPair{Key: k, Value: v})
		}
		return e.WriteSortedMap(pairs)
	default:
		return errors.New("cbor: unsupported type")
	}
}

// compareCBORKeys compares two string keys by their DAG-CBOR encoded form.
// For DAG-CBOR text string keys: shorter strings sort first (because their
// CBOR headers encode to fewer bytes), and equal-length strings sort
// lexicographically.
func compareCBORKeys(a, b string) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
