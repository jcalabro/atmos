package atmos

// RecordKey represents an AT Protocol record key.
type RecordKey string

// ParseRecordKey validates and returns a RecordKey.
func ParseRecordKey(raw string) (RecordKey, error) {
	if len(raw) == 0 {
		return "", syntaxErr("RecordKey", raw, "empty")
	}
	if len(raw) > 512 {
		return "", syntaxErr("RecordKey", raw, "too long")
	}
	if raw == "." || raw == ".." {
		return "", syntaxErr("RecordKey", raw, "disallowed value")
	}
	for i := 0; i < len(raw); i++ {
		if !isRecordKeyChar(raw[i]) {
			return "", syntaxErr("RecordKey", raw, "invalid character")
		}
	}
	return RecordKey(raw), nil
}

func (r RecordKey) String() string {
	return string(r)
}

func (r RecordKey) MarshalText() ([]byte, error) {
	return []byte(r), nil
}

func (r *RecordKey) UnmarshalText(b []byte) error {
	parsed, err := ParseRecordKey(string(b))
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

func isRecordKeyChar(c byte) bool {
	return isAlphanumeric(c) || c == '_' || c == '~' || c == '.' || c == ':' || c == '-'
}
