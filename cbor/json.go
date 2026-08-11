package cbor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ToJSON converts an atproto data-model value to JSON bytes. Bytes become
// {"$bytes":"<base64>"}, and CID links become {"$link":"<cid-string>"}.
// Values outside the atproto data model, including floats, are rejected.
func ToJSON(v any) ([]byte, error) {
	converted, err := toJSONValue(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(converted)
}

// FromJSON parses exactly one atproto JSON data-model value. JSON integer tokens
// are decoded directly to signed int64 without passing through float64; fractions
// and out-of-range integers are rejected. $bytes and $link sentinel objects are
// converted to []byte and CID, respectively.
func FromJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := decodeJSONValue(dec)
	if err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("cbor/json: multiple JSON values")
		}
		return nil, err
	}
	return fromJSONValue(value)
}

func toJSONValue(v any) (any, error) {
	switch val := v.(type) {
	case nil, bool, int64, string:
		return val, nil
	case int:
		return int64(val), nil
	case []byte:
		return map[string]any{"$bytes": base64.RawStdEncoding.EncodeToString(val)}, nil
	case CID:
		return map[string]any{"$link": val.String()}, nil
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			converted, err := toJSONValue(item)
			if err != nil {
				return nil, fmt.Errorf("cbor/json: array item %d: %w", i, err)
			}
			out[i] = converted
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(val))
		for key, item := range val {
			converted, err := toJSONValue(item)
			if err != nil {
				return nil, fmt.Errorf("cbor/json: field %q: %w", key, err)
			}
			out[key] = converted
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cbor/json: unsupported atproto value %T", v)
	}
}

// decodeJSONValue uses Decoder.Token so object key occurrences remain visible;
// decoding directly into map[string]any would silently accept duplicate keys by
// retaining only the last value.
func decodeJSONValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}

	switch delim {
	case '[':
		var values []any
		for dec.More() {
			value, err := decodeJSONValue(dec)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		end, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim(']') {
			return nil, fmt.Errorf("cbor/json: expected array end, got %v", end)
		}
		return values, nil
	case '{':
		values := make(map[string]any)
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("cbor/json: object key has type %T", keyToken)
			}
			if _, exists := values[key]; exists {
				return nil, fmt.Errorf("cbor/json: duplicate object key %q", key)
			}
			value, err := decodeJSONValue(dec)
			if err != nil {
				return nil, fmt.Errorf("cbor/json: field %q: %w", key, err)
			}
			values[key] = value
		}
		end, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim('}') {
			return nil, fmt.Errorf("cbor/json: expected object end, got %v", end)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("cbor/json: unexpected delimiter %q", delim)
	}
}

func fromJSONValue(v any) (any, error) {
	switch val := v.(type) {
	case nil, bool, string:
		return val, nil
	case json.Number:
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cbor/json: %q is not a signed 64-bit integer: %w", val, err)
		}
		return i, nil
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			converted, err := fromJSONValue(item)
			if err != nil {
				return nil, fmt.Errorf("cbor/json: array item %d: %w", i, err)
			}
			out[i] = converted
		}
		return out, nil
	case map[string]any:
		return fromJSONMap(val)
	default:
		return nil, fmt.Errorf("cbor/json: unsupported JSON type %T", v)
	}
}

// PeekJSONType scans a JSON object for the "$type" key and returns its string
// value without using encoding/json. Returns ("", nil) if "$type" is not found.
// This is a zero-allocation fast path for union type dispatch.
func PeekJSONType(data []byte) (string, error) {
	i := skipWS(data, 0)
	if i >= len(data) || data[i] != '{' {
		return "", fmt.Errorf("json: expected '{' at pos %d", i)
	}
	i++ // skip '{'
	i = skipWS(data, i)
	if i < len(data) && data[i] == '}' {
		return "", nil // empty object
	}
	for i < len(data) {
		// Read key.
		i = skipWS(data, i)
		if i >= len(data) {
			return "", fmt.Errorf("json: unexpected end of input")
		}
		if data[i] == '}' {
			return "", nil // no $type found
		}
		if data[i] != '"' {
			return "", fmt.Errorf("json: expected '\"' at pos %d", i)
		}
		keyStart := i + 1
		keyEnd, err := skipJSONString(data, i)
		if err != nil {
			return "", err
		}
		// Check if key is "$type" (fast path: no escapes in "$type").
		isType := keyEnd-keyStart-1 == 5 && string(data[keyStart:keyEnd-1]) == "$type"
		i = skipWS(data, keyEnd)
		if i >= len(data) || data[i] != ':' {
			return "", fmt.Errorf("json: expected ':' at pos %d", i)
		}
		i++ // skip ':'
		i = skipWS(data, i)
		if isType {
			// Read the value string.
			if i >= len(data) || data[i] != '"' {
				return "", fmt.Errorf("json: expected string value for $type at pos %d", i)
			}
			valEnd, err := skipJSONString(data, i)
			if err != nil {
				return "", err
			}
			return string(data[i+1 : valEnd-1]), nil
		}
		// Skip value.
		i, err = skipJSONValue(data, i)
		if err != nil {
			return "", err
		}
		i = skipWS(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return "", nil
}

func skipWS(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// skipJSONString skips a JSON string starting at data[i] (which must be '"')
// and returns the position after the closing '"'.
func skipJSONString(data []byte, i int) (int, error) {
	i++ // skip opening '"'
	for i < len(data) {
		if data[i] == '\\' {
			i += 2 // skip escape sequence
			continue
		}
		if data[i] == '"' {
			return i + 1, nil
		}
		i++
	}
	return 0, fmt.Errorf("json: unterminated string")
}

// skipJSONValue skips a JSON value starting at data[i].
func skipJSONValue(data []byte, i int) (int, error) {
	if i >= len(data) {
		return 0, fmt.Errorf("json: unexpected end of input")
	}
	switch data[i] {
	case '"':
		return skipJSONString(data, i)
	case '{':
		return skipJSONNested(data, i, '{', '}')
	case '[':
		return skipJSONNested(data, i, '[', ']')
	case 't': // true
		if i+4 <= len(data) {
			return i + 4, nil
		}
	case 'f': // false
		if i+5 <= len(data) {
			return i + 5, nil
		}
	case 'n': // null
		if i+4 <= len(data) {
			return i + 4, nil
		}
	default: // number
		for i < len(data) {
			c := data[i]
			if c == ',' || c == '}' || c == ']' || c == ' ' || c == '\t' || c == '\n' || c == '\r' {
				return i, nil
			}
			i++
		}
		return i, nil
	}
	return 0, fmt.Errorf("json: unexpected end of input")
}

// skipJSONNested skips a nested JSON object or array, handling string escapes.
func skipJSONNested(data []byte, i int, open, close byte) (int, error) {
	depth := 1
	i++ // skip opening brace/bracket
	for i < len(data) && depth > 0 {
		switch data[i] {
		case '"':
			end, err := skipJSONString(data, i)
			if err != nil {
				return 0, err
			}
			i = end
			continue
		case open:
			depth++
		case close:
			depth--
		}
		i++
	}
	if depth != 0 {
		return 0, fmt.Errorf("json: unterminated object/array")
	}
	return i, nil
}

func fromJSONMap(m map[string]any) (any, error) {
	// Check for $bytes sentinel.
	if b, ok := m["$bytes"]; ok && len(m) == 1 {
		s, ok := b.(string)
		if !ok {
			return nil, errors.New("cbor/json: $bytes value must be a string")
		}
		decoded, err := base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			decoded, err = base64.StdEncoding.DecodeString(s)
		}
		if err != nil {
			return nil, fmt.Errorf("cbor/json: invalid $bytes base64: %w", err)
		}
		return decoded, nil
	}

	// Check for $link sentinel.
	if l, ok := m["$link"]; ok && len(m) == 1 {
		s, ok := l.(string)
		if !ok {
			return nil, errors.New("cbor/json: $link value must be a string")
		}
		cid, err := ParseCIDString(s)
		if err != nil {
			return nil, fmt.Errorf("cbor/json: invalid $link CID: %w", err)
		}
		return cid, nil
	}

	// Regular map.
	out := make(map[string]any, len(m))
	for k, v := range m {
		var err error
		out[k], err = fromJSONValue(v)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
