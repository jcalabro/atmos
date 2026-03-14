package cbor

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"unicode/utf8"
)

// Marshal encodes a Go value to DAG-CBOR bytes.
// Supported types: nil, bool, int64, int, float64, string, []byte, CID, []any, map[string]any.
func Marshal(v any) ([]byte, error) {
	buf := make([]byte, 0, 64)
	return appendValue(buf, v)
}

// appendValue encodes an arbitrary Go value as DAG-CBOR using buffer appending.
func appendValue(buf []byte, v any) ([]byte, error) {
	switch val := v.(type) {
	case nil:
		return AppendNull(buf), nil
	case bool:
		return AppendBool(buf, val), nil
	case int64:
		return AppendInt(buf, val), nil
	case int:
		return AppendInt(buf, int64(val)), nil
	case float64:
		if math.IsNaN(val) {
			return nil, errors.New("cbor: NaN not allowed in DAG-CBOR")
		}
		if math.IsInf(val, 0) {
			return nil, errors.New("cbor: Infinity not allowed in DAG-CBOR")
		}
		return AppendFloat64(buf, val), nil
	case string:
		return AppendText(buf, val), nil
	case []byte:
		return AppendBytes(buf, val), nil
	case CID:
		return AppendCIDLink(buf, &val), nil
	case []any:
		buf = AppendArrayHeader(buf, uint64(len(val)))
		for _, item := range val {
			var err error
			buf, err = appendValue(buf, item)
			if err != nil {
				return nil, err
			}
		}
		return buf, nil
	case map[string]any:
		return appendSortedMap(buf, val)
	default:
		return nil, errors.New("cbor: unsupported type")
	}
}

// appendSortedMap encodes a map with keys sorted by DAG-CBOR order.
func appendSortedMap(buf []byte, m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return compareCBORKeys(keys[i], keys[j]) < 0
	})
	buf = AppendMapHeader(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = AppendText(buf, k)
		var err error
		buf, err = appendValue(buf, m[k])
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// Unmarshal decodes DAG-CBOR bytes to a Go value.
// Returns: nil, bool, int64, float64, string, []byte, CID, []any, map[string]any.
// Validates that the input contains exactly one CBOR value with no trailing bytes.
func Unmarshal(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, errors.New("cbor: empty input")
	}
	val, pos, err := unmarshalValue(data, 0, 0)
	if err != nil {
		return nil, err
	}
	if pos != len(data) {
		return nil, errors.New("cbor: trailing bytes after value")
	}
	return val, nil
}

// unmarshalValue decodes a single DAG-CBOR value at position pos using position-tracking.
func unmarshalValue(data []byte, pos int, depth int) (any, int, error) {
	depth++
	if depth > MaxDepth {
		return nil, 0, fmt.Errorf("cbor: exceeded max nesting depth of %d", MaxDepth)
	}

	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return nil, 0, err
	}

	switch major {
	case 0: // unsigned int
		if val > math.MaxInt64 {
			return nil, 0, errors.New("cbor: unsigned integer exceeds int64 range")
		}
		return int64(val), newPos, nil

	case 1: // negative int
		if val > math.MaxInt64 {
			return nil, 0, errors.New("cbor: negative integer overflow")
		}
		return -1 - int64(val), newPos, nil

	case 2: // byte string
		if val > MaxSize {
			return nil, 0, fmt.Errorf("cbor: byte string length %d exceeds max size %d", val, MaxSize)
		}
		end := newPos + int(val)
		if end > len(data) {
			return nil, 0, errors.New("cbor: byte string truncated")
		}
		out := make([]byte, val)
		copy(out, data[newPos:end])
		return out, end, nil

	case 3: // text string
		if val > MaxSize {
			return nil, 0, fmt.Errorf("cbor: text string length %d exceeds max size %d", val, MaxSize)
		}
		end := newPos + int(val)
		if end > len(data) {
			return nil, 0, errors.New("cbor: text string truncated")
		}
		b := data[newPos:end]
		if !utf8.Valid(b) {
			return nil, 0, errors.New("cbor: text string contains invalid UTF-8")
		}
		return string(b), end, nil

	case 4: // array
		if val > MaxSize {
			return nil, 0, fmt.Errorf("cbor: array length %d exceeds max size %d", val, MaxSize)
		}
		arr := make([]any, val)
		p := newPos
		for i := range val {
			arr[i], p, err = unmarshalValue(data, p, depth)
			if err != nil {
				return nil, 0, err
			}
		}
		return arr, p, nil

	case 5: // map
		if val > MaxSize {
			return nil, 0, fmt.Errorf("cbor: map length %d exceeds max size %d", val, MaxSize)
		}
		return unmarshalMap(data, newPos, val, depth)

	case 6: // tag
		if val != tagCIDLink {
			return nil, 0, fmt.Errorf("cbor: unsupported tag %d, only tag 42 allowed in DAG-CBOR", val)
		}
		return unmarshalCIDLink(data, newPos)

	case 7: // simple/float
		return unmarshalSimple(data, pos, val)

	default:
		return nil, 0, fmt.Errorf("cbor: unknown major type %d", major)
	}
}

// unmarshalMap decodes a CBOR map with validation.
func unmarshalMap(data []byte, pos int, count uint64, depth int) (map[string]any, int, error) {
	m := make(map[string]any, count)
	var prevKey string
	for i := range count {
		major, val, newPos, err := ReadHeader(data, pos)
		if err != nil {
			return nil, 0, err
		}
		if major != 3 {
			return nil, 0, fmt.Errorf("cbor: map key must be text string, got major type %d", major)
		}
		if val > MaxSize {
			return nil, 0, fmt.Errorf("cbor: map key length %d exceeds max size %d", val, MaxSize)
		}
		end := newPos + int(val)
		if end > len(data) {
			return nil, 0, errors.New("cbor: map key truncated")
		}
		keyBytes := data[newPos:end]
		if !utf8.Valid(keyBytes) {
			return nil, 0, errors.New("cbor: map key contains invalid UTF-8")
		}
		key := string(keyBytes)

		if i > 0 {
			cmp := compareCBORKeys(prevKey, key)
			if cmp == 0 {
				return nil, 0, fmt.Errorf("cbor: duplicate map key %q", key)
			}
			if cmp > 0 {
				return nil, 0, errors.New("cbor: map keys not sorted (DAG-CBOR requires sorted keys)")
			}
		}
		prevKey = key

		var v any
		v, pos, err = unmarshalValue(data, end, depth)
		if err != nil {
			return nil, 0, err
		}
		m[key] = v
	}
	return m, pos, nil
}

// unmarshalCIDLink decodes a CID link (tag 42 already consumed).
func unmarshalCIDLink(data []byte, pos int) (CID, int, error) {
	major, val, newPos, err := ReadHeader(data, pos)
	if err != nil {
		return CID{}, 0, err
	}
	if major != 2 {
		return CID{}, 0, errors.New("cbor: tag 42 must wrap a byte string")
	}
	if val > MaxSize {
		return CID{}, 0, fmt.Errorf("cbor: CID byte string length %d exceeds max size %d", val, MaxSize)
	}
	end := newPos + int(val)
	if end > len(data) {
		return CID{}, 0, errors.New("cbor: CID link data truncated")
	}
	payload := data[newPos:end]
	if len(payload) == 0 {
		return CID{}, 0, errors.New("cbor: tag 42 payload is empty")
	}
	if payload[0] != 0x00 {
		return CID{}, 0, fmt.Errorf("cbor: tag 42 payload must start with 0x00, got 0x%02x", payload[0])
	}
	cid, _, err := ParseCIDPrefix(payload[1:])
	if err != nil {
		return CID{}, 0, err
	}
	return cid, end, nil
}

// unmarshalSimple handles major type 7 (simple values and floats).
func unmarshalSimple(data []byte, pos int, arg uint64) (any, int, error) {
	b := data[pos]
	additional := b & 0x1f
	switch additional {
	case 20: // false
		return false, pos + 1, nil
	case 21: // true
		return true, pos + 1, nil
	case 22: // null
		return nil, pos + 1, nil
	case 27: // float64
		f := math.Float64frombits(arg)
		if math.IsNaN(f) {
			return nil, 0, errors.New("cbor: NaN not allowed in DAG-CBOR")
		}
		if math.IsInf(f, 0) {
			return nil, 0, errors.New("cbor: Infinity not allowed in DAG-CBOR")
		}
		return f, pos + 9, nil
	default:
		return nil, 0, fmt.Errorf("cbor: unsupported simple value (additional info %d)", additional)
	}
}

// UnmarshalReader decodes a single DAG-CBOR value from a reader.
func UnmarshalReader(r io.Reader) (any, error) {
	dec := NewDecoder(r)
	return dec.ReadValue()
}
