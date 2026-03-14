package atmos

import "strings"

// ATURI represents an AT Protocol URI (e.g., "at://did:plc:abc123/app.bsky.feed.post/tid").
type ATURI string

// ParseATURI validates and returns an ATURI.
func ParseATURI(raw string) (ATURI, error) {
	if len(raw) == 0 {
		return "", syntaxErr("ATURI", raw, "empty")
	}
	if len(raw) > 8192 {
		return "", syntaxErr("ATURI", raw, "too long")
	}
	if len(raw) < 5 || raw[:5] != "at://" {
		return "", syntaxErr("ATURI", raw, "must start with \"at://\"")
	}

	// Reject query and fragment by scanning for ? or #.
	for i := 5; i < len(raw); i++ {
		if raw[i] == '?' || raw[i] == '#' {
			return "", syntaxErr("ATURI", raw, "query and fragment not allowed")
		}
	}

	rest := raw[5:]
	if len(rest) == 0 {
		return "", syntaxErr("ATURI", raw, "empty authority")
	}

	// Find the first slash to separate authority from path.
	slash1 := strings.IndexByte(rest, '/')
	var authority string
	if slash1 < 0 {
		authority = rest
	} else {
		authority = rest[:slash1]
	}

	if _, err := ParseATIdentifier(authority); err != nil {
		return "", syntaxErr("ATURI", raw, "invalid authority: "+err.Error())
	}

	// No path — just authority.
	if slash1 < 0 {
		return ATURI(raw), nil
	}

	afterAuth := rest[slash1+1:]
	if len(afterAuth) == 0 {
		return "", syntaxErr("ATURI", raw, "trailing slash without collection")
	}

	// Find second slash to separate collection from rkey.
	slash2 := strings.IndexByte(afterAuth, '/')
	var collection string
	if slash2 < 0 {
		collection = afterAuth
	} else {
		collection = afterAuth[:slash2]
	}

	if _, err := ParseNSID(collection); err != nil {
		return "", syntaxErr("ATURI", raw, "invalid collection: "+err.Error())
	}

	// No record key.
	if slash2 < 0 {
		return ATURI(raw), nil
	}

	afterColl := afterAuth[slash2+1:]
	if len(afterColl) == 0 {
		return "", syntaxErr("ATURI", raw, "trailing slash without record key")
	}

	// Reject additional path segments.
	if strings.IndexByte(afterColl, '/') >= 0 {
		return "", syntaxErr("ATURI", raw, "too many path segments")
	}

	if _, err := ParseRecordKey(afterColl); err != nil {
		return "", syntaxErr("ATURI", raw, "invalid record key: "+err.Error())
	}

	return ATURI(raw), nil
}

// Authority returns the parsed authority portion.
func (a ATURI) Authority() ATIdentifier {
	s := string(a)
	if len(s) < 5 || s[:5] != "at://" {
		return ""
	}
	rest := s[5:]
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return ATIdentifier(rest)
	}
	return ATIdentifier(rest[:idx])
}

// Path returns the path portion without leading slash.
func (a ATURI) Path() string {
	s := string(a)
	if len(s) < 5 || s[:5] != "at://" {
		return ""
	}
	rest := s[5:]
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return ""
	}
	return rest[idx+1:]
}

// Collection returns the collection NSID, or empty if not present.
func (a ATURI) Collection() NSID {
	p := a.Path()
	if p == "" {
		return ""
	}
	idx := strings.IndexByte(p, '/')
	if idx < 0 {
		return NSID(p)
	}
	return NSID(p[:idx])
}

// RecordKey returns the record key, or empty if not present.
func (a ATURI) RecordKey() RecordKey {
	p := a.Path()
	if p == "" {
		return ""
	}
	idx := strings.IndexByte(p, '/')
	if idx < 0 {
		return ""
	}
	return RecordKey(p[idx+1:])
}

// Normalize normalizes the authority and collection.
func (a ATURI) Normalize() ATURI {
	authority := a.Authority().Normalize()
	collection := a.Collection().Normalize()
	rkey := a.RecordKey()

	result := "at://" + string(authority)
	if collection != "" {
		result += "/" + string(collection)
		if rkey != "" {
			result += "/" + string(rkey)
		}
	}
	return ATURI(result)
}

func (a ATURI) String() string {
	return string(a)
}

func (a ATURI) MarshalText() ([]byte, error) {
	return []byte(a), nil
}

func (a *ATURI) UnmarshalText(b []byte) error {
	parsed, err := ParseATURI(string(b))
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}
