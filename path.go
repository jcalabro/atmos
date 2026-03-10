package atmos

import "strings"

// ParseRepoPath splits a "collection/recordkey" string and validates both parts.
func ParseRepoPath(raw string) (NSID, RecordKey, error) {
	parts := strings.SplitN(raw, "/", 2)
	if len(parts) != 2 {
		return "", "", syntaxErr("RepoPath", raw, "expected collection/recordkey")
	}

	nsid, err := ParseNSID(parts[0])
	if err != nil {
		return "", "", syntaxErr("RepoPath", raw, "invalid collection: "+err.Error())
	}

	rkey, err := ParseRecordKey(parts[1])
	if err != nil {
		return "", "", syntaxErr("RepoPath", raw, "invalid record key: "+err.Error())
	}

	return nsid, rkey, nil
}
