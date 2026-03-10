package atmos

// Language represents a BCP-47 language tag (naive validation).
type Language string

// ParseLanguage validates and returns a Language.
func ParseLanguage(raw string) (Language, error) {
	if len(raw) == 0 {
		return "", syntaxErr("Language", raw, "empty")
	}
	if len(raw) > 128 {
		return "", syntaxErr("Language", raw, "too long")
	}

	// Primary subtag: "i" or 2-3 lowercase letters.
	i := 0
	for i < len(raw) && raw[i] != '-' {
		if !isLowerAlpha(raw[i]) {
			return "", syntaxErr("Language", raw, "primary subtag must be lowercase alpha")
		}
		i++
	}

	if i == 0 {
		return "", syntaxErr("Language", raw, "empty primary subtag")
	}
	if i == 1 && raw[0] != 'i' {
		return "", syntaxErr("Language", raw, "single-char primary subtag must be 'i'")
	}
	if i > 3 {
		return "", syntaxErr("Language", raw, "primary subtag too long")
	}

	// Subsequent subtags: hyphen-separated alphanumeric.
	for i < len(raw) {
		if raw[i] != '-' {
			return "", syntaxErr("Language", raw, "expected hyphen")
		}
		i++ // skip hyphen
		start := i
		for i < len(raw) && raw[i] != '-' {
			if !isAlphanumeric(raw[i]) {
				return "", syntaxErr("Language", raw, "subtag must be alphanumeric")
			}
			i++
		}
		if i == start {
			return "", syntaxErr("Language", raw, "empty subtag")
		}
	}

	return Language(raw), nil
}

func (l Language) String() string {
	return string(l)
}

func (l Language) MarshalText() ([]byte, error) {
	return []byte(l), nil
}

func (l *Language) UnmarshalText(b []byte) error {
	parsed, err := ParseLanguage(string(b))
	if err != nil {
		return err
	}
	*l = parsed
	return nil
}
