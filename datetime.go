package atmos

import (
	"strings"
	"time"
)

// AtprotoDatetimeLayout is the preferred output format for AT Protocol datetimes.
const AtprotoDatetimeLayout = "2006-01-02T15:04:05.999Z"

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
		fracLen := i - fracStart
		if fracLen == 0 || fracLen > 20 {
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
		if raw[i+2] != ':' {
			return syntaxErr("Datetime", raw, "expected ':' in timezone offset")
		}
		if !isDigit(raw[i+3]) || !isDigit(raw[i+4]) {
			return syntaxErr("Datetime", raw, "invalid timezone minute")
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
