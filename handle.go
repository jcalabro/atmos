package atmos

import "strings"

// Handle represents a domain-name-based user identifier (e.g., "alice.bsky.social").
type Handle string

// HandleInvalid is the sentinel value for failed handle resolution.
const HandleInvalid = Handle("handle.invalid")

var disallowedTLDs = map[string]bool{
	"local":     true,
	"arpa":      true,
	"invalid":   true,
	"localhost": true,
	"internal":  true,
	"example":   true,
	"onion":     true,
	"alt":       true,
}

// ParseHandle validates and returns a Handle.
func ParseHandle(raw string) (Handle, error) {
	if len(raw) == 0 {
		return "", syntaxErr("Handle", raw, "empty")
	}
	if len(raw) > 253 {
		return "", syntaxErr("Handle", raw, "too long")
	}

	// Walk through dot-separated labels.
	labelCount := 0
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == '.' {
			label := raw[start:i]
			if len(label) == 0 {
				return "", syntaxErr("Handle", raw, "empty label")
			}
			if len(label) > 63 {
				return "", syntaxErr("Handle", raw, "label too long")
			}
			if !isAlphanumeric(label[0]) {
				return "", syntaxErr("Handle", raw, "label must start with alphanumeric")
			}
			if !isAlphanumeric(label[len(label)-1]) {
				return "", syntaxErr("Handle", raw, "label must end with alphanumeric")
			}
			for j := 1; j < len(label)-1; j++ {
				if !isAlphanumericOrHyphen(label[j]) {
					return "", syntaxErr("Handle", raw, "invalid character in label")
				}
			}
			labelCount++
			start = i + 1
		}
	}

	if labelCount < 2 {
		return "", syntaxErr("Handle", raw, "must have at least two labels")
	}

	// TLD (last label) must start with a letter.
	lastDot := strings.LastIndexByte(raw, '.')
	tld := raw[lastDot+1:]
	if !isAlpha(tld[0]) {
		return "", syntaxErr("Handle", raw, "TLD must start with a letter")
	}

	return Handle(raw), nil
}

// Normalize returns the handle lowercased.
func (h Handle) Normalize() Handle {
	return Handle(strings.ToLower(string(h)))
}

// TLD returns the top-level domain label, lowercased.
func (h Handle) TLD() string {
	s := string(h)
	idx := strings.LastIndexByte(s, '.')
	if idx < 0 {
		return ""
	}
	return strings.ToLower(s[idx+1:])
}

// AllowedTLD returns false for TLDs that should not be used in production handles.
func (h Handle) AllowedTLD() bool {
	return !disallowedTLDs[h.TLD()]
}

// IsInvalidHandle returns true if this is the "handle.invalid" sentinel.
func (h Handle) IsInvalidHandle() bool {
	return h.Normalize() == HandleInvalid
}

// AtIdentifier converts the Handle to an [AtIdentifier].
func (h Handle) AtIdentifier() AtIdentifier {
	return AtIdentifier(h)
}

func (h Handle) String() string {
	return string(h)
}

func (h Handle) MarshalText() ([]byte, error) {
	return []byte(h), nil
}

func (h *Handle) UnmarshalText(b []byte) error {
	parsed, err := ParseHandle(string(b))
	if err != nil {
		return err
	}
	*h = parsed
	return nil
}
