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
