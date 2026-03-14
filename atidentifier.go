package atmos

import "strings"

// ATIdentifier represents either a DID or a Handle.
type ATIdentifier string

// ParseATIdentifier validates the string as either a DID or Handle.
func ParseATIdentifier(raw string) (ATIdentifier, error) {
	if strings.HasPrefix(raw, "did:") {
		d, err := ParseDID(raw)
		if err != nil {
			return "", err
		}
		return ATIdentifier(d), nil
	}
	h, err := ParseHandle(raw)
	if err != nil {
		return "", err
	}
	return ATIdentifier(h), nil
}

// IsDID reports whether this identifier is a DID.
func (a ATIdentifier) IsDID() bool {
	return strings.HasPrefix(string(a), "did:")
}

// IsHandle reports whether this identifier is a Handle.
func (a ATIdentifier) IsHandle() bool {
	return !a.IsDID() && len(a) > 0
}

// AsDID returns the identifier as a DID, or an error if it is a Handle.
func (a ATIdentifier) AsDID() (DID, error) {
	if !a.IsDID() {
		return "", syntaxErr("ATIdentifier", string(a), "not a DID")
	}
	return DID(a), nil
}

// AsHandle returns the identifier as a Handle, or an error if it is a DID.
func (a ATIdentifier) AsHandle() (Handle, error) {
	if !a.IsHandle() {
		return "", syntaxErr("ATIdentifier", string(a), "not a Handle")
	}
	return Handle(a), nil
}

// DID returns the identifier as a DID, or empty if it is a Handle.
func (a ATIdentifier) DID() DID {
	if a.IsDID() {
		return DID(a)
	}
	return ""
}

// Handle returns the identifier as a Handle, or empty if it is a DID.
func (a ATIdentifier) Handle() Handle {
	if a.IsHandle() {
		return Handle(a)
	}
	return ""
}

// Normalize lowercases handles; DIDs are returned as-is.
func (a ATIdentifier) Normalize() ATIdentifier {
	if a.IsHandle() {
		return ATIdentifier(strings.ToLower(string(a)))
	}
	return a
}

func (a ATIdentifier) String() string {
	return string(a)
}

func (a ATIdentifier) MarshalText() ([]byte, error) {
	return []byte(a), nil
}

func (a *ATIdentifier) UnmarshalText(b []byte) error {
	parsed, err := ParseATIdentifier(string(b))
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}
