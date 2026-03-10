package atmos

import "strings"

// AtIdentifier represents either a DID or a Handle.
type AtIdentifier string

// ParseAtIdentifier validates the string as either a DID or Handle.
func ParseAtIdentifier(raw string) (AtIdentifier, error) {
	if strings.HasPrefix(raw, "did:") {
		d, err := ParseDID(raw)
		if err != nil {
			return "", err
		}
		return AtIdentifier(d), nil
	}
	h, err := ParseHandle(raw)
	if err != nil {
		return "", err
	}
	return AtIdentifier(h), nil
}

// IsDID reports whether this identifier is a DID.
func (a AtIdentifier) IsDID() bool {
	return strings.HasPrefix(string(a), "did:")
}

// IsHandle reports whether this identifier is a Handle.
func (a AtIdentifier) IsHandle() bool {
	return !a.IsDID() && len(a) > 0
}

// AsDID returns the identifier as a DID, or an error if it is a Handle.
func (a AtIdentifier) AsDID() (DID, error) {
	if !a.IsDID() {
		return "", syntaxErr("AtIdentifier", string(a), "not a DID")
	}
	return DID(a), nil
}

// AsHandle returns the identifier as a Handle, or an error if it is a DID.
func (a AtIdentifier) AsHandle() (Handle, error) {
	if !a.IsHandle() {
		return "", syntaxErr("AtIdentifier", string(a), "not a Handle")
	}
	return Handle(a), nil
}

// DID returns the identifier as a DID, or empty if it is a Handle.
func (a AtIdentifier) DID() DID {
	if a.IsDID() {
		return DID(a)
	}
	return ""
}

// Handle returns the identifier as a Handle, or empty if it is a DID.
func (a AtIdentifier) Handle() Handle {
	if a.IsHandle() {
		return Handle(a)
	}
	return ""
}

// Normalize lowercases handles; DIDs are returned as-is.
func (a AtIdentifier) Normalize() AtIdentifier {
	if a.IsHandle() {
		return AtIdentifier(strings.ToLower(string(a)))
	}
	return a
}

func (a AtIdentifier) String() string {
	return string(a)
}

func (a AtIdentifier) MarshalText() ([]byte, error) {
	return []byte(a), nil
}

func (a *AtIdentifier) UnmarshalText(b []byte) error {
	parsed, err := ParseAtIdentifier(string(b))
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}
