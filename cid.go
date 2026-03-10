package atmos

import "strings"

// CID represents a Content Identifier as a string (string-level validation only).
// Full binary CID parsing lives in the cbor package.
type CID string

// ParseCID validates and returns a CID string.
func ParseCID(raw string) (CID, error) {
	if len(raw) == 0 {
		return "", syntaxErr("CID", raw, "empty")
	}
	if len(raw) < 8 {
		return "", syntaxErr("CID", raw, "too short")
	}
	if len(raw) > 256 {
		return "", syntaxErr("CID", raw, "too long")
	}
	// Reject CIDv0 (all start with "Qm").
	if strings.HasPrefix(raw, "Qm") {
		return "", syntaxErr("CID", raw, "CIDv0 not allowed")
	}
	for i := 0; i < len(raw); i++ {
		if !isCIDChar(raw[i]) {
			return "", syntaxErr("CID", raw, "invalid character")
		}
	}
	return CID(raw), nil
}

func (c CID) String() string {
	return string(c)
}

func (c CID) MarshalText() ([]byte, error) {
	return []byte(c), nil
}

func (c *CID) UnmarshalText(b []byte) error {
	parsed, err := ParseCID(string(b))
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

func isCIDChar(c byte) bool {
	return isAlphanumeric(c) || c == '+' || c == '='
}
