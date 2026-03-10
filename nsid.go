package atmos

import "strings"

// NSID represents a Namespaced Identifier (e.g., "app.bsky.feed.post").
type NSID string

// ParseNSID validates and returns an NSID.
func ParseNSID(raw string) (NSID, error) {
	if len(raw) == 0 {
		return "", syntaxErr("NSID", raw, "empty")
	}
	if len(raw) > 317 {
		return "", syntaxErr("NSID", raw, "too long")
	}

	// Single-pass validation: walk segments separated by '.'.
	segCount := 0
	start := 0
	lastDot := -1
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == '.' {
			seg := raw[start:i]
			segCount++

			if i < len(raw) {
				// Domain segment (not the last one).
				if err := validateDomainLabel(seg); err != nil {
					return "", syntaxErr("NSID", raw, err.Error())
				}
				// First segment (TLD) must start with a letter.
				if segCount == 1 && len(seg) > 0 && !isAlpha(seg[0]) {
					return "", syntaxErr("NSID", raw, "first segment must start with a letter")
				}
				lastDot = i
			}
			start = i + 1
		}
	}

	if segCount < 3 {
		return "", syntaxErr("NSID", raw, "must have at least 3 segments")
	}

	// Validate name segment (last): must start with letter, alphanumeric only.
	name := raw[lastDot+1:]
	if len(name) == 0 || len(name) > 63 {
		return "", syntaxErr("NSID", raw, "name segment must be 1-63 characters")
	}
	if !isAlpha(name[0]) {
		return "", syntaxErr("NSID", raw, "name segment must start with a letter")
	}
	for j := 1; j < len(name); j++ {
		if !isAlphanumeric(name[j]) {
			return "", syntaxErr("NSID", raw, "name segment must be alphanumeric only")
		}
	}

	return NSID(raw), nil
}

// Authority returns the reversed domain portion in normal DNS order, lowercased.
// For "com.example.fooBar" returns "example.com".
func (n NSID) Authority() string {
	s := string(n)
	if s == "" {
		return ""
	}
	lastDot := strings.LastIndexByte(s, '.')
	if lastDot < 0 {
		return ""
	}
	// Domain portion is everything before the last dot.
	domain := s[:lastDot]

	// Count dots to know how many segments.
	dotCount := strings.Count(domain, ".")
	if dotCount == 0 {
		// Single domain segment.
		return strings.ToLower(domain)
	}

	// Reverse segments using a builder.
	var b strings.Builder
	b.Grow(len(domain))
	// Walk backwards through segments.
	end := len(domain)
	for i := len(domain) - 1; i >= 0; i-- {
		if domain[i] == '.' {
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			seg := domain[i+1 : end]
			for _, c := range []byte(seg) {
				if c >= 'A' && c <= 'Z' {
					b.WriteByte(c + 32)
				} else {
					b.WriteByte(c)
				}
			}
			end = i
		}
	}
	// First segment (last in reversed order).
	if b.Len() > 0 {
		b.WriteByte('.')
	}
	seg := domain[:end]
	for _, c := range []byte(seg) {
		if c >= 'A' && c <= 'Z' {
			b.WriteByte(c + 32)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// Name returns the final segment (the method/record name).
func (n NSID) Name() string {
	s := string(n)
	idx := strings.LastIndexByte(s, '.')
	if idx < 0 {
		return s
	}
	return s[idx+1:]
}

// Normalize lowercases the domain segments but preserves the name segment's case.
func (n NSID) Normalize() NSID {
	s := string(n)
	idx := strings.LastIndexByte(s, '.')
	if idx < 0 {
		return n
	}
	return NSID(strings.ToLower(s[:idx]) + s[idx:])
}

func (n NSID) String() string {
	return string(n)
}

func (n NSID) MarshalText() ([]byte, error) {
	return []byte(n), nil
}

func (n *NSID) UnmarshalText(b []byte) error {
	parsed, err := ParseNSID(string(b))
	if err != nil {
		return err
	}
	*n = parsed
	return nil
}

// validateDomainLabel validates a single DNS-style domain label.
func validateDomainLabel(label string) error {
	if len(label) == 0 {
		return syntaxErr("label", label, "empty label")
	}
	if len(label) > 63 {
		return syntaxErr("label", label, "label too long")
	}
	if !isAlphanumeric(label[0]) {
		return syntaxErr("label", label, "must start with alphanumeric")
	}
	if !isAlphanumeric(label[len(label)-1]) {
		return syntaxErr("label", label, "must end with alphanumeric")
	}
	for i := 1; i < len(label)-1; i++ {
		if !isAlphanumericOrHyphen(label[i]) {
			return syntaxErr("label", label, "invalid character")
		}
	}
	return nil
}
