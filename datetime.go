package atmos

import (
	"regexp"
	"strings"
	"time"
)

// AtprotoDatetimeLayout is the preferred output format for AT Protocol datetimes:
// fixed millisecond precision matching JavaScript's Date.toISOString(). Fixed
// (rather than trailing-zero-trimmed) precision keeps serialized timestamps
// lexicographically sortable.
const AtprotoDatetimeLayout = "2006-01-02T15:04:05.000Z"

// Datetime represents an AT Protocol datetime (RFC 3339 subset).
type Datetime string

// ParseDatetime validates and returns a Datetime (strict).
func ParseDatetime(raw string) (Datetime, error) {
	if len(raw) == 0 {
		return "", syntaxErr("Datetime", raw, "empty")
	}
	if len(raw) > 64 {
		return "", syntaxErr("Datetime", raw, "too long")
	}

	if err := validateDatetimeSyntax(raw); err != nil {
		return "", err
	}

	// Also validate semantically via Go's parser.
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return "", syntaxErr("Datetime", raw, "invalid datetime: "+err.Error())
	}

	// Reject dates before year 0000.
	if t.Before(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return "", syntaxErr("Datetime", raw, "date before year 0000")
	}

	// Reject datetimes that normalize (in UTC) to a year beyond 9999. The
	// reference rejects these because they cannot be re-serialized with a
	// 4-digit year. A non-UTC offset can push an in-range local year past the
	// boundary, so this check runs after parsing.
	if t.UTC().Year() > 9999 {
		return "", syntaxErr("Datetime", raw, "year after 9999")
	}

	return Datetime(raw), nil
}

// ParseDatetimeLenient tries strict parsing first, then attempts fixes for common issues.
func ParseDatetimeLenient(raw string) (Datetime, error) {
	if d, err := ParseDatetime(raw); err == nil {
		return d, nil
	}

	fixed := raw

	// Convert -00:00 to +00:00.
	if strings.HasSuffix(fixed, "-00:00") {
		fixed = fixed[:len(fixed)-6] + "+00:00"
	}

	// Convert -0000 or +0000 to +00:00.
	if strings.HasSuffix(fixed, "-0000") || strings.HasSuffix(fixed, "+0000") {
		fixed = fixed[:len(fixed)-5] + "+00:00"
	}

	// Append Z if no timezone detected.
	if !hasTimezone(fixed) {
		fixed += "Z"
	}

	return ParseDatetime(fixed)
}

// DatetimeNow returns the current time as a Datetime.
func DatetimeNow() Datetime {
	return Datetime(time.Now().UTC().Format(AtprotoDatetimeLayout))
}

// Time converts the Datetime to a time.Time.
func (d Datetime) Time() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, string(d))
	return t
}

func (d Datetime) String() string {
	return string(d)
}

func (d Datetime) MarshalText() ([]byte, error) {
	return []byte(d), nil
}

func (d *Datetime) UnmarshalText(b []byte) error {
	parsed, err := ParseDatetime(string(b))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// lexiconDatetimeRegex mirrors the canonical @atproto/syntax DATETIME_REGEX used
// by the TS LEXICON validator's "datetime" format (via isDatetimeStringLenient).
// It bounds month/day/time fields structurally but — like the reference — does
// NOT reject calendar rollovers (e.g. Feb 31), because a TS-based PDS accepts
// and serves such records (new Date() rolls them over). Using the strict
// ParseDatetime here would reject real network data the reference accepts.
var lexiconDatetimeRegex = regexp.MustCompile(
	`^[0-9]{4}-(0[1-9]|1[012])-([0-2][0-9]|3[01])T([0-1][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?(Z|[+-]([0-1][0-9]|2[0-3]):[0-5][0-9])$`,
)

// ValidateDatetimeLexicon reports whether s is acceptable as a lexicon
// "datetime" format value, matching the canonical TS lexicon validator
// (isDatetimeStringLenient). It is intentionally more lenient than
// [ParseDatetime]: it accepts syntactically well-formed datetimes whose
// day-of-month would roll over (Feb 31, etc.), since the reference stack does.
// It still rejects "-00:00", over-length input, and (after parsing) datetimes
// whose UTC-normalized year falls outside 0000–9999.
func ValidateDatetimeLexicon(s string) error {
	if len(s) == 0 {
		return syntaxErr("Datetime", s, "empty")
	}
	if len(s) > 64 {
		return syntaxErr("Datetime", s, "too long")
	}
	if strings.HasSuffix(s, "-00:00") {
		return syntaxErr("Datetime", s, "-00:00 not allowed, use +00:00 or Z")
	}
	if !lexiconDatetimeRegex.MatchString(s) {
		return syntaxErr("Datetime", s, "not a valid datetime format")
	}
	// Bound the normalized year to 0000–9999, matching the reference's
	// parseDate (which rejects a NaN/negative/>9999 year). Go's time.Parse
	// rolls over an out-of-range day just like JS new Date(), so a value that
	// passed the regex parses here unless the year is out of range.
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		if y := t.UTC().Year(); y < 0 || y > 9999 {
			return syntaxErr("Datetime", s, "year out of range 0000–9999")
		}
	}
	return nil
}

// validateDatetimeSyntax checks the structural format without semantic validation.
// Format: YYYY-MM-DDThh:mm:ss[.fractional](Z|[+-]hh:mm)
func validateDatetimeSyntax(raw string) error {
	n := len(raw)
	if n < 20 { // Minimum: "0000-01-01T00:00:00Z"
		return syntaxErr("Datetime", raw, "too short")
	}

	// YYYY-MM-DD
	for _, i := range []int{0, 1, 2, 3} {
		if !isDigit(raw[i]) {
			return syntaxErr("Datetime", raw, "invalid year")
		}
	}
	if raw[4] != '-' {
		return syntaxErr("Datetime", raw, "expected '-' after year")
	}
	if !isDigit(raw[5]) || !isDigit(raw[6]) {
		return syntaxErr("Datetime", raw, "invalid month")
	}
	if raw[7] != '-' {
		return syntaxErr("Datetime", raw, "expected '-' after month")
	}
	if !isDigit(raw[8]) || !isDigit(raw[9]) {
		return syntaxErr("Datetime", raw, "invalid day")
	}

	// T
	if raw[10] != 'T' {
		return syntaxErr("Datetime", raw, "expected 'T' separator")
	}

	// hh:mm:ss
	if !isDigit(raw[11]) || !isDigit(raw[12]) {
		return syntaxErr("Datetime", raw, "invalid hour")
	}
	if raw[13] != ':' {
		return syntaxErr("Datetime", raw, "expected ':' after hour")
	}
	if !isDigit(raw[14]) || !isDigit(raw[15]) {
		return syntaxErr("Datetime", raw, "invalid minute")
	}
	if raw[16] != ':' {
		return syntaxErr("Datetime", raw, "expected ':' after minute")
	}
	if !isDigit(raw[17]) || !isDigit(raw[18]) {
		return syntaxErr("Datetime", raw, "invalid second")
	}

	i := 19

	// Optional fractional seconds.
	if i < n && raw[i] == '.' {
		i++
		fracStart := i
		for i < n && isDigit(raw[i]) {
			i++
		}
		// The spec allows arbitrary fractional-second precision; the only bound
		// is the overall 64-character length check in ParseDatetime. A dot with
		// no following digits is still invalid.
		if i == fracStart {
			return syntaxErr("Datetime", raw, "invalid fractional seconds")
		}
	}

	// Timezone: Z or [+-]hh:mm
	if i >= n {
		return syntaxErr("Datetime", raw, "missing timezone")
	}
	switch raw[i] {
	case 'Z':
		i++
	case '+', '-':
		// Reject -00:00.
		if raw[i:] == "-00:00" {
			return syntaxErr("Datetime", raw, "-00:00 not allowed, use +00:00 or Z")
		}
		i++
		if i+5 > n {
			return syntaxErr("Datetime", raw, "incomplete timezone offset")
		}
		if !isDigit(raw[i]) || !isDigit(raw[i+1]) {
			return syntaxErr("Datetime", raw, "invalid timezone hour")
		}
		// Offset hour must be 00–23 (Go's time.Parse accepts 24+).
		offHour := int(raw[i]-'0')*10 + int(raw[i+1]-'0')
		if offHour > 23 {
			return syntaxErr("Datetime", raw, "timezone hour out of range")
		}
		if raw[i+2] != ':' {
			return syntaxErr("Datetime", raw, "expected ':' in timezone offset")
		}
		if !isDigit(raw[i+3]) || !isDigit(raw[i+4]) {
			return syntaxErr("Datetime", raw, "invalid timezone minute")
		}
		// Offset minute must be 00–59.
		offMin := int(raw[i+3]-'0')*10 + int(raw[i+4]-'0')
		if offMin > 59 {
			return syntaxErr("Datetime", raw, "timezone minute out of range")
		}
		i += 5
	default:
		return syntaxErr("Datetime", raw, "invalid timezone")
	}

	if i != n {
		return syntaxErr("Datetime", raw, "trailing characters")
	}

	return nil
}

func hasTimezone(s string) bool {
	if len(s) == 0 {
		return false
	}
	if s[len(s)-1] == 'Z' {
		return true
	}
	// Check for +hh:mm or -hh:mm at end.
	if len(s) >= 6 {
		c := s[len(s)-6]
		if c == '+' || c == '-' {
			return true
		}
	}
	return false
}
