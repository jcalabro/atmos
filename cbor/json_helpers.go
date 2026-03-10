package cbor

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// --- JSON Encoding Helpers ---

// AppendJSONString appends a JSON-encoded string (with quotes and escaping) to buf.
// Matches encoding/json escaping: control chars, ", \, and HTML-unsafe <, >, &.
func AppendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		b := s[i]
		switch {
		case b == '"':
			buf = append(buf, '\\', '"')
			i++
		case b == '\\':
			buf = append(buf, '\\', '\\')
			i++
		case b < 0x20:
			// Control characters.
			switch b {
			case '\b':
				buf = append(buf, '\\', 'b')
			case '\f':
				buf = append(buf, '\\', 'f')
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				buf = append(buf, '\\', 'u', '0', '0', hexDigit(b>>4), hexDigit(b&0xf))
			}
			i++
		case b == '<' || b == '>' || b == '&':
			// HTML-safe escaping (matches encoding/json).
			buf = append(buf, '\\', 'u', '0', '0', hexDigit(b>>4), hexDigit(b&0xf))
			i++
		case b < utf8.RuneSelf:
			buf = append(buf, b)
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				buf = append(buf, '\\', 'u', 'f', 'f', 'f', 'd')
			} else {
				buf = append(buf, s[i:i+size]...)
			}
			i += size
		}
	}
	buf = append(buf, '"')
	return buf
}

func hexDigit(b byte) byte {
	const hex = "0123456789abcdef"
	return hex[b&0xf]
}

// AppendJSONInt appends a JSON integer to buf.
func AppendJSONInt(buf []byte, n int64) []byte {
	return strconv.AppendInt(buf, n, 10)
}

// AppendJSONBool appends a JSON boolean to buf.
func AppendJSONBool(buf []byte, b bool) []byte {
	if b {
		return append(buf, "true"...)
	}
	return append(buf, "false"...)
}

// AppendJSONNull appends the JSON null literal to buf.
func AppendJSONNull(buf []byte) []byte {
	return append(buf, "null"...)
}

// AppendJSONBytes appends a JSON-encoded base64 string (for bytes fields) to buf.
func AppendJSONBytes(buf []byte, data []byte) []byte {
	buf = append(buf, '"')
	encoded := base64.RawStdEncoding.EncodeToString(data)
	buf = append(buf, encoded...)
	buf = append(buf, '"')
	return buf
}

// --- JSON Decoding Helpers ---

// SkipJSONWS skips whitespace in data starting at pos.
func SkipJSONWS(data []byte, pos int) int {
	return skipWS(data, pos)
}

// SkipJSONValue skips a JSON value starting at pos, returning the position after it.
func SkipJSONValue(data []byte, pos int) (int, error) {
	pos = skipWS(data, pos)
	return skipJSONValue(data, pos)
}

// SkipJSONComma skips an optional comma and whitespace.
func SkipJSONComma(data []byte, pos int) int {
	pos = skipWS(data, pos)
	if pos < len(data) && data[pos] == ',' {
		pos++
		pos = skipWS(data, pos)
	}
	return pos
}

// ReadJSONObjectStart reads '{' and returns position after it (with whitespace skipped).
func ReadJSONObjectStart(data []byte, pos int) (int, error) {
	pos = skipWS(data, pos)
	if pos >= len(data) || data[pos] != '{' {
		return 0, fmt.Errorf("json: expected '{' at pos %d", pos)
	}
	return skipWS(data, pos+1), nil
}

// ReadJSONObjectEnd checks if the current position is '}'. Returns (newPos, true) if found.
func ReadJSONObjectEnd(data []byte, pos int) (int, bool) {
	pos = skipWS(data, pos)
	if pos < len(data) && data[pos] == '}' {
		return pos + 1, true
	}
	return pos, false
}

// ReadJSONKey reads a JSON object key and the following colon.
// Returns the unescaped key string and position after the colon.
func ReadJSONKey(data []byte, pos int) (string, int, error) {
	pos = skipWS(data, pos)
	key, pos, err := ReadJSONString(data, pos)
	if err != nil {
		return "", 0, err
	}
	pos = skipWS(data, pos)
	if pos >= len(data) || data[pos] != ':' {
		return "", 0, fmt.Errorf("json: expected ':' after key at pos %d", pos)
	}
	return key, skipWS(data, pos+1), nil
}

// ReadJSONArrayStart reads '[' and returns position after it.
func ReadJSONArrayStart(data []byte, pos int) (int, error) {
	pos = skipWS(data, pos)
	if pos >= len(data) || data[pos] != '[' {
		return 0, fmt.Errorf("json: expected '[' at pos %d", pos)
	}
	return skipWS(data, pos+1), nil
}

// ReadJSONArrayEnd checks if the current position is ']'. Returns (newPos, true) if found.
func ReadJSONArrayEnd(data []byte, pos int) (int, bool) {
	pos = skipWS(data, pos)
	if pos < len(data) && data[pos] == ']' {
		return pos + 1, true
	}
	return pos, false
}

// ReadJSONString reads a JSON string value starting at pos (which must be '"').
// Returns the unescaped string value and position after the closing '"'.
func ReadJSONString(data []byte, pos int) (string, int, error) {
	pos = skipWS(data, pos)
	if pos >= len(data) || data[pos] != '"' {
		return "", 0, fmt.Errorf("json: expected '\"' at pos %d", pos)
	}
	pos++ // skip opening '"'

	// Fast path: no escapes.
	start := pos
	for pos < len(data) {
		if data[pos] == '\\' {
			goto slow
		}
		if data[pos] == '"' {
			return string(data[start:pos]), pos + 1, nil
		}
		pos++
	}
	return "", 0, fmt.Errorf("json: unterminated string")

slow:
	// Slow path: has escapes.
	var buf []byte
	buf = append(buf, data[start:pos]...)
	for pos < len(data) {
		if data[pos] == '"' {
			return string(buf), pos + 1, nil
		}
		if data[pos] == '\\' {
			pos++
			if pos >= len(data) {
				return "", 0, fmt.Errorf("json: unterminated escape")
			}
			switch data[pos] {
			case '"', '\\', '/':
				buf = append(buf, data[pos])
			case 'b':
				buf = append(buf, '\b')
			case 'f':
				buf = append(buf, '\f')
			case 'n':
				buf = append(buf, '\n')
			case 'r':
				buf = append(buf, '\r')
			case 't':
				buf = append(buf, '\t')
			case 'u':
				if pos+4 >= len(data) {
					return "", 0, fmt.Errorf("json: truncated \\u escape")
				}
				r, err := strconv.ParseUint(string(data[pos+1:pos+5]), 16, 32)
				if err != nil {
					return "", 0, fmt.Errorf("json: invalid \\u escape: %w", err)
				}
				pos += 4
				// Handle surrogate pairs.
				codepoint := rune(r)
				if codepoint >= 0xD800 && codepoint <= 0xDBFF {
					if pos+2 < len(data) && data[pos+1] == '\\' && data[pos+2] == 'u' {
						if pos+6 < len(data) {
							r2, err := strconv.ParseUint(string(data[pos+3:pos+7]), 16, 32)
							if err == nil && rune(r2) >= 0xDC00 && rune(r2) <= 0xDFFF {
								codepoint = 0x10000 + (codepoint-0xD800)*0x400 + (rune(r2) - 0xDC00)
								pos += 6
							}
						}
					}
				}
				var ubuf [4]byte
				n := utf8.EncodeRune(ubuf[:], codepoint)
				buf = append(buf, ubuf[:n]...)
			default:
				return "", 0, fmt.Errorf("json: invalid escape '\\%c'", data[pos])
			}
			pos++
			continue
		}
		buf = append(buf, data[pos])
		pos++
	}
	return "", 0, fmt.Errorf("json: unterminated string")
}

// ReadJSONInt reads a JSON integer starting at pos.
func ReadJSONInt(data []byte, pos int) (int64, int, error) {
	pos = skipWS(data, pos)
	start := pos
	if pos < len(data) && data[pos] == '-' {
		pos++
	}
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}
	if pos == start || (pos == start+1 && data[start] == '-') {
		return 0, 0, fmt.Errorf("json: expected integer at pos %d", start)
	}
	n, err := strconv.ParseInt(string(data[start:pos]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("json: invalid integer at pos %d: %w", start, err)
	}
	return n, pos, nil
}

// ReadJSONFloat reads a JSON number (including decimals) starting at pos.
func ReadJSONFloat(data []byte, pos int) (float64, int, error) {
	pos = skipWS(data, pos)
	start := pos
	// Skip sign.
	if pos < len(data) && data[pos] == '-' {
		pos++
	}
	// Skip digits.
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}
	// Skip decimal.
	if pos < len(data) && data[pos] == '.' {
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}
	// Skip exponent.
	if pos < len(data) && (data[pos] == 'e' || data[pos] == 'E') {
		pos++
		if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
			pos++
		}
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}
	if pos == start {
		return 0, 0, fmt.Errorf("json: expected number at pos %d", start)
	}
	f, err := strconv.ParseFloat(string(data[start:pos]), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("json: invalid number at pos %d: %w", start, err)
	}
	return f, pos, nil
}

// ReadJSONBool reads a JSON boolean starting at pos.
func ReadJSONBool(data []byte, pos int) (bool, int, error) {
	pos = skipWS(data, pos)
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
		return true, pos + 4, nil
	}
	if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
		return false, pos + 5, nil
	}
	return false, 0, fmt.Errorf("json: expected boolean at pos %d", pos)
}

// IsJSONNull returns true if the value at pos is null.
func IsJSONNull(data []byte, pos int) bool {
	pos = skipWS(data, pos)
	return pos+4 <= len(data) && string(data[pos:pos+4]) == "null"
}

// SkipJSONNull skips a JSON null literal.
func SkipJSONNull(data []byte, pos int) (int, error) {
	pos = skipWS(data, pos)
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return pos + 4, nil
	}
	return 0, fmt.Errorf("json: expected null at pos %d", pos)
}
