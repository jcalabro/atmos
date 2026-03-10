package cbor

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// ToJSON converts a CBOR data model value to ATProto JSON bytes.
// Bytes become {"$bytes": "<base64>"}, CID links become {"$link": "<cid-string>"}.
func ToJSON(v any) ([]byte, error) {
	converted := toJSONValue(v)
	return json.Marshal(converted)
}

// FromJSON parses ATProto JSON bytes back to a CBOR data model value.
// Recognizes {"$bytes": "..."} and {"$link": "..."} sentinel objects.
func FromJSON(data []byte) (any, error) {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return fromJSONValue(raw)
}

func toJSONValue(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case int64:
		return val
	case float64:
		return val
	case string:
		return val
	case []byte:
		return map[string]any{
			"$bytes": base64.RawStdEncoding.EncodeToString(val),
		}
	case CID:
		return map[string]any{
			"$link": val.String(),
		}
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = toJSONValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, v := range val {
			out[k] = toJSONValue(v)
		}
		return out
	default:
		return val
	}
}

func fromJSONValue(v any) (any, error) {
	switch val := v.(type) {
	case nil:
		return nil, nil
	case bool:
		return val, nil
	case float64:
		// JSON numbers come as float64. If it's a whole number, convert to int64.
		if val == float64(int64(val)) && val >= -9007199254740992 && val <= 9007199254740992 {
			return int64(val), nil
		}
		return val, nil
	case string:
		return val, nil
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			var err error
			out[i], err = fromJSONValue(item)
			if err != nil {
				return nil, err
			}
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
