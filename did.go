package atmos

import "strings"

// DID represents a Decentralized Identifier (e.g., "did:plc:abcde1234").
type DID string

// ParseDID validates and returns a DID.
func ParseDID(raw string) (DID, error) {
	if len(raw) == 0 {
		return "", syntaxErr("DID", raw, "empty")
	}
	if len(raw) > 2048 {
		return "", syntaxErr("DID", raw, "too long")
	}

	// Fast-path for did:plc: DIDs (always exactly 32 chars).
	if len(raw) == 32 && strings.HasPrefix(raw, "did:plc:") {
		for i := 8; i < 32; i++ {
			c := raw[i]
			if !isAlphanumeric(c) {
				return "", syntaxErr("DID", raw, "invalid character in did:plc identifier")
			}
		}
		return DID(raw), nil
	}

	// Must start with "did:".
	if !strings.HasPrefix(raw, "did:") {
		return "", syntaxErr("DID", raw, "must start with \"did:\"")
	}

	// Find the method segment (lowercase alpha only).
	i := 4
	for i < len(raw) && raw[i] != ':' {
		if !isLowerAlpha(raw[i]) {
			return "", syntaxErr("DID", raw, "method must be lowercase alpha")
		}
		i++
	}
	if i == 4 {
		return "", syntaxErr("DID", raw, "empty method")
	}
	if i >= len(raw) {
		return "", syntaxErr("DID", raw, "missing identifier after method")
	}

	// Skip the colon after method.
	i++
	if i >= len(raw) {
		return "", syntaxErr("DID", raw, "empty identifier")
	}

	// Validate identifier: [a-zA-Z0-9._:%-]* ending with [a-zA-Z0-9._-].
	for j := i; j < len(raw); j++ {
		c := raw[j]
		if !isDIDIdentChar(c) {
			return "", syntaxErr("DID", raw, "invalid character in identifier")
		}
	}

	// Last char cannot be '%' or ':'.
	last := raw[len(raw)-1]
	if last == '%' || last == ':' {
		return "", syntaxErr("DID", raw, "identifier cannot end with '%' or ':'")
	}

	// Validate percent-encoding: every '%' must be followed by exactly two hex digits.
	for j := i; j < len(raw); j++ {
		if raw[j] == '%' {
			if j+2 >= len(raw) || !isHexDigit(raw[j+1]) || !isHexDigit(raw[j+2]) {
				return "", syntaxErr("DID", raw, "invalid percent-encoding")
			}
		}
	}

	return DID(raw), nil
}

// Method returns the DID method (e.g., "plc" for "did:plc:abc123").
func (d DID) Method() string {
	s := string(d)
	if !strings.HasPrefix(s, "did:") {
		return ""
	}
	rest := s[4:]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return ""
	}
	return rest[:idx]
}

// Identifier returns the method-specific identifier (e.g., "abc123" for "did:plc:abc123").
func (d DID) Identifier() string {
	s := string(d)
	if !strings.HasPrefix(s, "did:") {
		return ""
	}
	rest := s[4:]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return ""
	}
	return rest[idx+1:]
}

// ATIdentifier converts the DID to an [ATIdentifier].
func (d DID) ATIdentifier() ATIdentifier {
	return ATIdentifier(d)
}

func (d DID) String() string {
	return string(d)
}

func (d DID) MarshalText() ([]byte, error) {
	return []byte(d), nil
}

func (d *DID) UnmarshalText(b []byte) error {
	parsed, err := ParseDID(string(b))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

func isDIDIdentChar(c byte) bool {
	return isAlphanumeric(c) || c == '.' || c == '_' || c == ':' || c == '%' || c == '-'
}

func isLowerAlpha(c byte) bool {
	return c >= 'a' && c <= 'z'
}

func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isAlphanumericOrHyphen(c byte) bool {
	return isAlphanumeric(c) || c == '-'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
