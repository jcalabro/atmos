package cbor

import (
	"bytes"
	"errors"
	"io"
)

// Marshal encodes a Go value to DAG-CBOR bytes.
// Supported types: nil, bool, int64, int, float64, string, []byte, CID, []any, map[string]any.
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(64)
	enc := NewEncoder(&buf)
	if err := enc.WriteValue(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal decodes DAG-CBOR bytes to a Go value.
// Returns: nil, bool, int64, float64, string, []byte, CID, []any, map[string]any.
// Validates that the input contains exactly one CBOR value with no trailing bytes.
func Unmarshal(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, errors.New("cbor: empty input")
	}
	r := bytes.NewReader(data)
	dec := NewDecoder(r)
	val, err := dec.ReadValue()
	if err != nil {
		return nil, err
	}
	// Ensure no trailing bytes.
	if r.Len() > 0 {
		return nil, errors.New("cbor: trailing bytes after value")
	}
	return val, nil
}

// UnmarshalReader decodes a single DAG-CBOR value from a reader.
func UnmarshalReader(r io.Reader) (any, error) {
	dec := NewDecoder(r)
	return dec.ReadValue()
}
