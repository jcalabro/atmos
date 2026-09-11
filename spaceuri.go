package atmos

import (
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

// SpaceRef identifies a space by authority DID, space-type NSID, and space key.
type SpaceRef string

// ParseSpaceRef validates and returns a complete space reference.
func ParseSpaceRef(raw string) (SpaceRef, error) {
	parts, err := parseSpaceURI(raw, false)
	if err != nil {
		return "", err
	}
	if parts.author != "" {
		return "", syntaxErr("SpaceRef", raw, "record URI is not a space reference")
	}
	return SpaceRef(raw), nil
}

// Authority returns the space authority DID.
func (s SpaceRef) Authority() DID {
	parts, _ := parseSpaceURI(string(s), false)
	return parts.authority
}

// Type returns the space-type NSID.
func (s SpaceRef) Type() NSID {
	parts, _ := parseSpaceURI(string(s), false)
	return parts.spaceType
}

// Key returns the space key.
func (s SpaceRef) Key() RecordKey {
	parts, _ := parseSpaceURI(string(s), false)
	return parts.skey
}

// Validate reports whether s is a complete space reference.
func (s SpaceRef) Validate() error {
	_, err := ParseSpaceRef(string(s))
	return err
}

func (s SpaceRef) String() string { return string(s) }

// MarshalText implements encoding.TextMarshaler.
func (s SpaceRef) MarshalText() ([]byte, error) { return []byte(s), s.Validate() }

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *SpaceRef) UnmarshalText(data []byte) error {
	parsed, err := ParseSpaceRef(string(data))
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

// SpaceURI identifies a record in an author's repo within a space.
type SpaceURI string

// ParseSpaceURI validates and returns a complete space record URI.
func ParseSpaceURI(raw string) (SpaceURI, error) {
	parts, err := parseSpaceURI(raw, true)
	if err != nil {
		return "", err
	}
	if parts.author == "" {
		return "", syntaxErr("SpaceURI", raw, "space reference is not a record URI")
	}
	return SpaceURI(raw), nil
}

// Space returns the containing space reference.
func (s SpaceURI) Space() SpaceRef {
	parts, _ := parseSpaceURI(string(s), true)
	if parts.authority == "" {
		return ""
	}
	return SpaceRef("at://" + string(parts.authority) + "/space/" + string(parts.spaceType) + "/" + string(parts.skey))
}

// Author returns the record author's DID.
func (s SpaceURI) Author() DID {
	parts, _ := parseSpaceURI(string(s), true)
	return parts.author
}

// Collection returns the record collection NSID.
func (s SpaceURI) Collection() NSID {
	parts, _ := parseSpaceURI(string(s), true)
	return parts.collection
}

// RecordKey returns the record key.
func (s SpaceURI) RecordKey() RecordKey {
	parts, _ := parseSpaceURI(string(s), true)
	return parts.rkey
}

// Validate reports whether s is a complete space record URI.
func (s SpaceURI) Validate() error {
	_, err := ParseSpaceURI(string(s))
	return err
}

func (s SpaceURI) String() string { return string(s) }

// MarshalText implements encoding.TextMarshaler.
func (s SpaceURI) MarshalText() ([]byte, error) { return []byte(s), s.Validate() }

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *SpaceURI) UnmarshalText(data []byte) error {
	parsed, err := ParseSpaceURI(string(data))
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

type spaceURIParts struct {
	authority  DID
	spaceType  NSID
	skey       RecordKey
	author     DID
	collection NSID
	rkey       RecordKey
}

func parseSpaceURI(raw string, allowRecord bool) (spaceURIParts, error) {
	var out spaceURIParts
	kind := "SpaceRef"
	if allowRecord {
		kind = "SpaceURI"
	}
	if raw == "" {
		return out, syntaxErr(kind, raw, "empty")
	}
	if len(raw) > 8192 {
		return out, syntaxErr(kind, raw, "too long")
	}
	if !utf8.ValidString(raw) {
		return out, syntaxErr(kind, raw, "invalid UTF-8")
	}
	if !strings.HasPrefix(raw, "at://") {
		return out, syntaxErr(kind, raw, "must start with \"at://\"")
	}
	if strings.ContainsAny(raw[5:], "?#") {
		return out, syntaxErr(kind, raw, "query and fragment not allowed")
	}
	segments := strings.Split(raw[5:], "/")
	if len(segments) != 4 && len(segments) != 7 {
		return out, syntaxErr(kind, raw, "must contain a complete space or record path")
	}
	for _, segment := range segments {
		if segment == "" {
			return out, syntaxErr(kind, raw, "empty path segment")
		}
	}
	if segments[1] != "space" {
		return out, syntaxErr(kind, raw, "missing space marker")
	}
	var err error
	out.authority, err = ParseDID(segments[0])
	if err != nil {
		return spaceURIParts{}, syntaxErr(kind, raw, "invalid authority DID: "+err.Error())
	}
	out.spaceType, err = ParseNSID(segments[2])
	if err != nil {
		return spaceURIParts{}, syntaxErr(kind, raw, "invalid space type: "+err.Error())
	}
	out.skey, err = ParseRecordKey(segments[3])
	if err != nil {
		return spaceURIParts{}, syntaxErr(kind, raw, "invalid space key: "+err.Error())
	}
	if len(segments) == 4 {
		return out, nil
	}
	if !allowRecord {
		return out, syntaxErr(kind, raw, "record URI is not a space reference")
	}
	out.author, err = ParseDID(segments[4])
	if err != nil {
		return spaceURIParts{}, syntaxErr(kind, raw, "invalid author DID: "+err.Error())
	}
	out.collection, err = ParseNSID(segments[5])
	if err != nil {
		return spaceURIParts{}, syntaxErr(kind, raw, "invalid collection: "+err.Error())
	}
	out.rkey, err = ParseRecordKey(segments[6])
	if err != nil {
		return spaceURIParts{}, syntaxErr(kind, raw, "invalid record key: "+err.Error())
	}
	return out, nil
}

// ValidateLexiconATURI validates the general at-uri Lexicon string format.
// It accepts existing public AT URIs and complete space references/record URIs.
// A valid RFC 6901 JSON-pointer fragment is allowed, but query strings are not.
func ValidateLexiconATURI(raw string) error {
	if len(raw) > 8192 {
		return syntaxErr("ATURI", raw, "too long")
	}
	base, fragment, hasFragment := strings.Cut(raw, "#")
	if hasFragment {
		if strings.Contains(fragment, "#") || !strings.HasPrefix(fragment, "/") {
			return syntaxErr("ATURI", raw, "invalid JSON-pointer fragment")
		}
		decoded, err := url.PathUnescape(fragment)
		if err != nil || !utf8.ValidString(decoded) {
			return syntaxErr("ATURI", raw, "invalid percent-encoding in fragment")
		}
		if !validJSONPointer(decoded) {
			return syntaxErr("ATURI", raw, "invalid JSON-pointer fragment")
		}
		for i := range len(fragment) {
			c := fragment[i]
			if c <= 0x20 || c >= 0x7f || strings.ContainsRune("\\?\"<>`^{}|", rune(c)) {
				return syntaxErr("ATURI", raw, fmt.Sprintf("invalid fragment character %q", c))
			}
		}
	}
	if strings.Contains(base, "?") {
		return syntaxErr("ATURI", raw, "query not allowed")
	}
	if strings.Contains(base, "/space/") {
		if _, err := ParseSpaceRef(base); err == nil {
			return nil
		}
		if _, err := ParseSpaceURI(base); err == nil {
			return nil
		}
		return syntaxErr("ATURI", raw, "invalid space URI")
	}
	_, err := ParseATURI(base)
	return err
}

func validJSONPointer(pointer string) bool {
	if pointer == "" || pointer[0] != '/' {
		return false
	}
	for i := 1; i < len(pointer); i++ {
		if pointer[i] != '~' {
			continue
		}
		if i+1 >= len(pointer) || (pointer[i+1] != '0' && pointer[i+1] != '1') {
			return false
		}
		i++
	}
	return true
}
