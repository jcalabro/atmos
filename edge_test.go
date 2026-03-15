package atmos

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Edge case tests for comprehensive validation coverage.

func TestDID_EdgeCases(t *testing.T) {
	t.Parallel()
	// Max length DID (2048 chars).
	longID := "did:plc:" + strings.Repeat("a", 2040)
	_, err := ParseDID(longID)
	require.NoError(t, err)

	// One over max.
	tooLong := "did:plc:" + strings.Repeat("a", 2041)
	_, err = ParseDID(tooLong)
	require.Error(t, err)

	// Ends with '%' — invalid.
	_, err = ParseDID("did:plc:abc%")
	require.Error(t, err)

	// Ends with ':' — invalid.
	_, err = ParseDID("did:plc:abc:")
	require.Error(t, err)

	// Uppercase DID prefix.
	_, err = ParseDID("DID:plc:abc123")
	require.Error(t, err)

	// Uppercase method.
	_, err = ParseDID("did:PLC:abc123")
	require.Error(t, err)

	// Digits in method.
	_, err = ParseDID("did:pl1c:abc123")
	require.Error(t, err)

	// No identifier after method.
	_, err = ParseDID("did:plc:")
	require.Error(t, err)

	// Just "did:".
	_, err = ParseDID("did:")
	require.Error(t, err)

	// did:plc: fast path — exactly 32 chars, valid.
	d, err := ParseDID("did:plc:z234567890abcdefghijklmn")
	require.NoError(t, err)
	require.Equal(t, "plc", d.Method())

	// did:plc: fast path — 32 chars but non-alphanumeric.
	_, err = ParseDID("did:plc:z234567890abcdef.hijklmn")
	require.Error(t, err)
}

func TestHandle_EdgeCases(t *testing.T) {
	t.Parallel()
	// Max total length (253 chars).
	// 63 + 1 + 63 + 1 + 63 + 1 + 57 + 1 + 3 = 253
	long := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 57) + ".com"
	require.Equal(t, 253, len(long))
	_, err := ParseHandle(long)
	require.NoError(t, err)

	// Single label — invalid.
	_, err = ParseHandle("localhost")
	require.Error(t, err)

	// Underscore — invalid.
	_, err = ParseHandle("alice_bob.social")
	require.Error(t, err)

	// Hyphen at start of label.
	_, err = ParseHandle("-alice.social")
	require.Error(t, err)

	// Hyphen at end of label.
	_, err = ParseHandle("alice-.social")
	require.Error(t, err)

	// TLD starts with digit.
	_, err = ParseHandle("alice.123")
	require.Error(t, err)

	// Empty string.
	_, err = ParseHandle("")
	require.Error(t, err)

	// Double dot.
	_, err = ParseHandle("alice..social")
	require.Error(t, err)
}

func TestNSID_EdgeCases(t *testing.T) {
	t.Parallel()
	// Two segments only — invalid.
	_, err := ParseNSID("com.example")
	require.Error(t, err)

	// Name segment starts with digit — invalid.
	_, err = ParseNSID("com.example.1foo")
	require.Error(t, err)

	// Name segment with hyphen — invalid.
	_, err = ParseNSID("com.example.foo-bar")
	require.Error(t, err)

	// Name segment with underscore — invalid.
	_, err = ParseNSID("com.example.foo_bar")
	require.Error(t, err)

	// Empty string.
	_, err = ParseNSID("")
	require.Error(t, err)

	// Max length (317).
	seg := strings.Repeat("a", 63)
	nsid := seg + "." + seg + "." + seg + "." + seg + "." + "fooBar"
	require.LessOrEqual(t, len(nsid), 317)
	_, err = ParseNSID(nsid)
	require.NoError(t, err)
}

func TestATURI_EdgeCases(t *testing.T) {
	t.Parallel()
	// Authority only.
	a, err := ParseATURI("at://did:plc:abc123")
	require.NoError(t, err)
	require.Equal(t, NSID(""), a.Collection())
	require.Equal(t, RecordKey(""), a.RecordKey())

	// Trailing slash — invalid.
	_, err = ParseATURI("at://did:plc:abc123/")
	require.Error(t, err)

	// Double trailing slash — invalid.
	_, err = ParseATURI("at://did:plc:abc123/com.example.foo/")
	require.Error(t, err)

	// Fragment — invalid.
	_, err = ParseATURI("at://did:plc:abc123#frag")
	require.Error(t, err)

	// Query — invalid.
	_, err = ParseATURI("at://did:plc:abc123?query")
	require.Error(t, err)

	// Too many segments — invalid.
	_, err = ParseATURI("at://did:plc:abc123/com.example.foo/rkey/extra")
	require.Error(t, err)

	// Wrong scheme.
	_, err = ParseATURI("http://did:plc:abc123")
	require.Error(t, err)

	// Empty authority.
	_, err = ParseATURI("at://")
	require.Error(t, err)
}

func TestTID_EdgeCases(t *testing.T) {
	t.Parallel()
	// Wrong length.
	_, err := ParseTID("222222222222") // 12 chars
	require.Error(t, err)
	_, err = ParseTID("22222222222222") // 14 chars
	require.Error(t, err)

	// High bit set in first char (k-z) — invalid.
	_, err = ParseTID("k222222222222")
	require.Error(t, err)
	_, err = ParseTID("z222222222222")
	require.Error(t, err)

	// Uppercase — invalid.
	_, err = ParseTID("2222222222222")
	require.NoError(t, err) // all '2' is valid
	_, err = ParseTID("222222222222A")
	require.Error(t, err)

	// Digits 0 and 1 not in alphabet.
	_, err = ParseTID("2222222222220")
	require.Error(t, err)
	_, err = ParseTID("2222222222221")
	require.Error(t, err)
}

func TestRecordKey_EdgeCases(t *testing.T) {
	t.Parallel()
	// Max length (512).
	long := strings.Repeat("a", 512)
	_, err := ParseRecordKey(long)
	require.NoError(t, err)

	// One over max.
	_, err = ParseRecordKey(strings.Repeat("a", 513))
	require.Error(t, err)

	// Slash — invalid.
	_, err = ParseRecordKey("abc/def")
	require.Error(t, err)

	// Space — invalid.
	_, err = ParseRecordKey("abc def")
	require.Error(t, err)

	// Various allowed special chars.
	_, err = ParseRecordKey("abc_def~ghi.jkl:mno-pqr")
	require.NoError(t, err)
}

func TestDatetime_EdgeCases(t *testing.T) {
	t.Parallel()
	// Timezone +00:00 is fine.
	_, err := ParseDatetime("1985-04-12T23:20:50.123+00:00")
	require.NoError(t, err)

	// Z is fine.
	_, err = ParseDatetime("1985-04-12T23:20:50.123Z")
	require.NoError(t, err)

	// -00:00 rejected.
	_, err = ParseDatetime("1985-04-12T23:20:50.123-00:00")
	require.Error(t, err)

	// No fractional seconds — valid.
	_, err = ParseDatetime("1985-04-12T23:20:50Z")
	require.NoError(t, err)

	// Lowercase 'z' — invalid.
	_, err = ParseDatetime("1985-04-12T23:20:50.123z")
	require.Error(t, err)

	// Space instead of T — invalid.
	_, err = ParseDatetime("1985-04-12 23:20:50.123Z")
	require.Error(t, err)

	// Incomplete timezone.
	_, err = ParseDatetime("1985-04-12T23:20:50.123+00:0")
	require.Error(t, err)

	// No timezone — invalid.
	_, err = ParseDatetime("1985-04-12T23:20:50.123")
	require.Error(t, err)

	// Leading/trailing space — invalid.
	_, err = ParseDatetime(" 1985-04-12T23:20:50.123Z")
	require.Error(t, err)
	_, err = ParseDatetime("1985-04-12T23:20:50.123Z ")
	require.Error(t, err)
}

func TestURI_EdgeCases(t *testing.T) {
	t.Parallel()
	// Valid schemes.
	_, err := ParseURI("https://example.com")
	require.NoError(t, err)
	_, err = ParseURI("at://did:plc:abc123")
	require.NoError(t, err)

	// Uppercase scheme — invalid.
	_, err = ParseURI("HTTPS://example.com")
	require.Error(t, err)

	// No colon — invalid.
	_, err = ParseURI("httpexample.com")
	require.Error(t, err)

	// Empty body after colon — invalid.
	_, err = ParseURI("https:")
	require.Error(t, err)

	// Whitespace in body — invalid.
	_, err = ParseURI("https://example.com/foo bar")
	require.Error(t, err)
}

func TestLanguage_EdgeCases(t *testing.T) {
	t.Parallel()
	// Valid.
	_, err := ParseLanguage("en")
	require.NoError(t, err)
	_, err = ParseLanguage("en-US")
	require.NoError(t, err)
	_, err = ParseLanguage("i-klingon")
	require.NoError(t, err)

	// Primary subtag too long (>3).
	_, err = ParseLanguage("engl")
	require.Error(t, err)

	// Uppercase primary — invalid.
	_, err = ParseLanguage("EN")
	require.Error(t, err)

	// Single non-'i' char — invalid.
	_, err = ParseLanguage("e")
	require.Error(t, err)

	// Empty subtag after hyphen — invalid.
	_, err = ParseLanguage("en-")
	require.Error(t, err)
}
