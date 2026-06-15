package atmos

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// M4: timezone offsets must be range-bounded per the reference grammar
// ([+-]([01][0-9]|2[0-3]):[0-5][0-9]); Go's time.Parse alone is too lax.
func TestParseDatetime_TimezoneOffsetRanges(t *testing.T) {
	t.Parallel()
	invalid := []string{
		"1985-04-12T23:20:50+24:00", // hour 24 out of range
		"1985-04-12T23:20:50-24:00",
		"1985-04-12T23:20:50+00:60", // minute 60 out of range
		"1985-04-12T23:20:50+99:00",
	}
	for _, v := range invalid {
		t.Run("invalid/"+v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.Error(t, err, "%q must be rejected", v)
		})
	}

	valid := []string{
		"1985-04-12T23:20:50+23:59",
		"1985-04-12T23:20:50-23:59",
		"1985-04-12T23:20:50+00:00",
		"1985-04-12T23:20:50+05:30",
	}
	for _, v := range valid {
		t.Run("valid/"+v, func(t *testing.T) {
			_, err := ParseDatetime(v)
			require.NoError(t, err, "%q must be accepted", v)
		})
	}
}

// M4: a datetime that normalizes to a year beyond 9999 must be rejected (the
// reference rejects these because they cannot be re-serialized as 4-digit years).
func TestParseDatetime_YearBeyond9999_Rejected(t *testing.T) {
	t.Parallel()
	_, err := ParseDatetime("9999-12-31T23:00:00-02:00") // normalizes to year 10000 UTC
	require.Error(t, err)
}

// Nit: DatetimeNow must emit fixed-millisecond precision so that successive
// values sort lexicographically (the spec's preferred toISOString() form), and
// the output must itself be a valid Datetime.
func TestDatetimeNow_FixedMillisecondPrecision(t *testing.T) {
	t.Parallel()
	d := string(DatetimeNow())
	_, err := ParseDatetime(d)
	require.NoError(t, err)
	// "2006-01-02T15:04:05.000Z" => exactly 24 chars, always 3 fractional digits.
	require.Len(t, d, len("2006-01-02T15:04:05.000Z"))
	require.Equal(t, byte('.'), d[19])
	require.Equal(t, byte('Z'), d[len(d)-1])
}

// M4: arbitrary fractional precision is allowed by the spec; the only bound is
// the 64-character total length. A 21+ digit fraction within that bound must be
// accepted.
func TestParseDatetime_FractionalBeyond20_Accepted(t *testing.T) {
	t.Parallel()
	// 25 fractional digits, total length < 64.
	v := "1985-04-12T23:20:50." + strings.Repeat("1", 25) + "Z"
	require.Less(t, len(v), 65)
	_, err := ParseDatetime(v)
	require.NoError(t, err, "%q (len %d) must be accepted", v, len(v))
}
