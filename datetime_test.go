package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDatetime_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "datetime_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			d, err := ParseDatetime(v)
			require.NoError(t, err)
			require.Equal(t, v, d.String())
		})
	}
}

func TestParseDatetime_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "datetime_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.Error(t, err)
		})
	}
}

func TestParseDatetime_ParseInvalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "datetime_parse_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.Error(t, err)
		})
	}
}

func TestDatetimeNow(t *testing.T) {
	t.Parallel()
	d := DatetimeNow()
	require.NotEmpty(t, d.String())
	require.False(t, d.Time().IsZero())
}

func TestDatetime_RejectNegativeZero(t *testing.T) {
	t.Parallel()
	_, err := ParseDatetime("1985-04-12T23:20:50.123-00:00")
	require.Error(t, err)
}

func TestDatetime_ZeroValue(t *testing.T) {
	t.Parallel()
	var d Datetime
	require.Equal(t, "", d.String())
	require.True(t, d.Time().IsZero())
}

func TestDatetime_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	raw := "2024-06-15T12:30:00.000Z"
	d, err := ParseDatetime(raw)
	require.NoError(t, err)
	b, err := d.MarshalText()
	require.NoError(t, err)
	require.Equal(t, raw, string(b))
	var d2 Datetime
	require.NoError(t, d2.UnmarshalText(b))
	require.Equal(t, d, d2)

	var bad Datetime
	require.Error(t, bad.UnmarshalText([]byte("not-a-datetime")))
}

func TestParseDatetimeLenient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"already valid", "2024-06-15T12:30:00.000Z", "2024-06-15T12:30:00.000Z"},
		{"negative zero offset", "2024-06-15T12:30:00.000-00:00", "2024-06-15T12:30:00.000+00:00"},
		{"compact negative zero", "2024-06-15T12:30:00.000-0000", "2024-06-15T12:30:00.000+00:00"},
		{"compact positive zero", "2024-06-15T12:30:00.000+0000", "2024-06-15T12:30:00.000+00:00"},
		{"no timezone", "2024-06-15T12:30:00.000", "2024-06-15T12:30:00.000Z"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := ParseDatetimeLenient(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(d))
		})
	}

	// Still-invalid even after lenient fixing.
	_, err := ParseDatetimeLenient("totally invalid")
	require.Error(t, err)
}

// --- Comprehensive datetime edge case tests per Lexicon spec ---

func TestParseDatetime_SpecExamples_Valid(t *testing.T) {
	t.Parallel()
	// All valid examples explicitly listed in the Lexicon spec.
	valid := []string{
		"1985-04-12T23:20:50.123Z",
		"1985-04-12T23:20:50.123456Z",
		"1985-04-12T23:20:50.120Z",
		"1985-04-12T23:20:50.120000Z",
		"0001-01-01T00:00:00.000Z",
		"0000-01-01T00:00:00.000Z",
		"1985-04-12T23:20:50Z",
		"1985-04-12T23:20:50.123+00:00",
		"1985-04-12T23:20:50.123-07:00",
	}
	for _, v := range valid {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.NoError(t, err, "expected valid: %s", v)
		})
	}
}

func TestParseDatetime_SpecExamples_Invalid(t *testing.T) {
	t.Parallel()
	// All invalid examples from the Lexicon spec.
	invalid := []string{
		"1985-04-12",                    // date only
		"1985-04-12T23:20:50.123",       // no timezone
		"1985-04-12t23:20:50.123Z",      // lowercase t
		"1985-04-12T23:20:50.123z",      // lowercase z
		"1985-04-12T23:99:50.123Z",      // invalid minute (99)
		"1985-04-12T23:20:50.123-00:00", // negative zero
	}
	for _, v := range invalid {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.Error(t, err, "expected invalid: %s", v)
		})
	}
}

func TestParseDatetime_LowercaseT_Rejected(t *testing.T) {
	t.Parallel()
	// Spec: "Must be an uppercase T character (lowercase t is invalid)"
	_, err := ParseDatetime("1985-04-12t23:20:50.123Z")
	require.Error(t, err)
	require.Contains(t, err.Error(), "'T' separator")
}

func TestParseDatetime_LowercaseZ_Rejected(t *testing.T) {
	t.Parallel()
	// Spec: "capital Z suffix (lowercase z is invalid)"
	_, err := ParseDatetime("1985-04-12T23:20:50.123z")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid timezone")
}

func TestParseDatetime_NegativeZero_Rejected(t *testing.T) {
	t.Parallel()
	// Spec: "negative zero (-00:00) is specifically disallowed"
	_, err := ParseDatetime("1985-04-12T23:20:50.123-00:00")
	require.Error(t, err)
	require.Contains(t, err.Error(), "-00:00 not allowed")
}

func TestParseDatetime_YearZero_Allowed(t *testing.T) {
	t.Parallel()
	// Spec: "0000-01-01T00:00:00.000Z" is valid (year zero allowed)
	d, err := ParseDatetime("0000-01-01T00:00:00.000Z")
	require.NoError(t, err)
	require.Equal(t, "0000-01-01T00:00:00.000Z", d.String())
}

func TestParseDatetime_Hour24_Rejected(t *testing.T) {
	t.Parallel()
	// ISO 8601 allows 24:00:00, but RFC 3339 does not. Spec uses the intersection.
	_, err := ParseDatetime("1985-04-12T24:00:00Z")
	require.Error(t, err)
}

func TestParseDatetime_Feb29_LeapYear(t *testing.T) {
	t.Parallel()
	// 2024 is a leap year — Feb 29 is valid.
	_, err := ParseDatetime("2024-02-29T00:00:00Z")
	require.NoError(t, err)
}

func TestParseDatetime_Feb29_NonLeapYear(t *testing.T) {
	t.Parallel()
	// 2023 is NOT a leap year — Feb 29 is invalid.
	_, err := ParseDatetime("2023-02-29T00:00:00Z")
	require.Error(t, err)
}

func TestParseDatetime_MaxTimezoneOffsets(t *testing.T) {
	t.Parallel()
	// Large but valid offsets.
	_, err := ParseDatetime("2024-01-01T12:00:00+14:00")
	require.NoError(t, err)
	_, err = ParseDatetime("2024-01-01T12:00:00-12:00")
	require.NoError(t, err)
}

func TestParseDatetime_FractionalSecondsExceed20(t *testing.T) {
	t.Parallel()
	// Our implementation caps fractional seconds at 20 digits (zeptosecond precision).
	_, err := ParseDatetime("1985-04-12T23:20:50.123456789012345678901Z")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid fractional seconds")
}

func TestParseDatetime_EmptyFractionalSeconds(t *testing.T) {
	t.Parallel()
	// A dot with no following digits is invalid.
	_, err := ParseDatetime("1985-04-12T23:20:50.Z")
	require.Error(t, err)
}

func TestParseDatetime_CompactTimezone_Rejected(t *testing.T) {
	t.Parallel()
	// +0000 without colon is not valid RFC 3339.
	_, err := ParseDatetime("1985-04-12T23:20:50.123+0000")
	require.Error(t, err)
}

func TestParseDatetime_TrailingContent(t *testing.T) {
	t.Parallel()
	_, err := ParseDatetime("1985-04-12T23:20:50.123Z extra")
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing characters")
}

func TestParseDatetime_ArbitraryFractionalPrecision(t *testing.T) {
	t.Parallel()
	// Spec: "arbitrary fractional second precision is allowed"
	// Test various precisions from 1 to 20.
	precisions := []string{
		"1985-04-12T23:20:50.1Z",
		"1985-04-12T23:20:50.12Z",
		"1985-04-12T23:20:50.123Z",
		"1985-04-12T23:20:50.1234Z",
		"1985-04-12T23:20:50.12345Z",
		"1985-04-12T23:20:50.123456Z",
		"1985-04-12T23:20:50.1234567Z",
		"1985-04-12T23:20:50.12345678Z",
		"1985-04-12T23:20:50.123456789Z",
		"1985-04-12T23:20:50.1234567890Z",
		"1985-04-12T23:20:50.12345678901Z",
		"1985-04-12T23:20:50.123456789012Z",
		"1985-04-12T23:20:50.12345678901234567890Z", // 20 digits
	}
	for _, v := range precisions {
		t.Run(v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.NoError(t, err, "expected valid: %s", v)
		})
	}
}

func TestParseDatetime_NoFractionalSeconds_Valid(t *testing.T) {
	t.Parallel()
	// Spec: "Whole seconds are mandatory" — no fractional part required.
	_, err := ParseDatetime("1985-04-12T23:20:50Z")
	require.NoError(t, err)

	_, err = ParseDatetime("1985-04-12T23:20:50+05:30")
	require.NoError(t, err)
}
