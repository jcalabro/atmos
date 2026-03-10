package atmos

// URI represents a generic URI.
type URI string

// ParseURI validates and returns a URI.
func ParseURI(raw string) (URI, error) {
	if len(raw) == 0 {
		return "", syntaxErr("URI", raw, "empty")
	}
	if len(raw) > 8192 {
		return "", syntaxErr("URI", raw, "too long")
	}

	// Scheme: starts with lowercase letter, followed by lowercase letters/dots/hyphens, then ':'.
	if !isLowerAlpha(raw[0]) {
		return "", syntaxErr("URI", raw, "scheme must start with lowercase letter")
	}

	i := 1
	for i < len(raw) && raw[i] != ':' {
		c := raw[i]
		if !isLowerAlpha(c) && !isDigit(c) && c != '+' && c != '.' && c != '-' {
			return "", syntaxErr("URI", raw, "invalid character in scheme")
		}
		i++
		if i-1 > 80 {
			return "", syntaxErr("URI", raw, "scheme too long")
		}
	}
	if i >= len(raw) {
		return "", syntaxErr("URI", raw, "missing ':' after scheme")
	}

	// Skip ':'.
	i++
	if i >= len(raw) {
		return "", syntaxErr("URI", raw, "empty URI body")
	}

	// Rest must be printable non-whitespace (graphic) characters.
	for j := i; j < len(raw); j++ {
		c := raw[j]
		if c <= ' ' || c == 0x7F {
			return "", syntaxErr("URI", raw, "invalid character in URI body")
		}
	}

	return URI(raw), nil
}

func (u URI) String() string {
	return string(u)
}

func (u URI) MarshalText() ([]byte, error) {
	return []byte(u), nil
}

func (u *URI) UnmarshalText(b []byte) error {
	parsed, err := ParseURI(string(b))
	if err != nil {
		return err
	}
	*u = parsed
	return nil
}
