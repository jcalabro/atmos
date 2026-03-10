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
