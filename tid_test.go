package atmos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTID_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "tid_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseTID(v)
			require.NoError(t, err)
		})
	}
}

func TestParseTID_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "tid_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseTID(v)
			require.Error(t, err)
		})
	}
}

func TestTID_RoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tid := NewTIDFromTime(now, 42)
	parsed, err := ParseTID(tid.String())
	require.NoError(t, err)
	require.Equal(t, tid, parsed)
	require.Equal(t, uint(42), parsed.ClockID())
	require.Equal(t, now.UnixMicro(), parsed.Time().UnixMicro())
}

func TestTID_IntegerRoundTrip(t *testing.T) {
	t.Parallel()
	tid := NewTIDFromInteger(123456789)
	require.Equal(t, uint64(123456789), tid.Integer())
}

func TestTIDClock_Monotonic(t *testing.T) {
	t.Parallel()
	clock := NewTIDClock(1)
	prev := clock.Next()
	for range 100 {
		next := clock.Next()
		require.Greater(t, next.Integer(), prev.Integer())
		prev = next
	}
}

func TestTID_ZeroValue(t *testing.T) {
	t.Parallel()
	var tid TID
	require.Equal(t, "", tid.String())
	require.Equal(t, uint64(0), tid.Integer())
	require.True(t, tid.Time().IsZero())
	require.Equal(t, uint(0), tid.ClockID())
}

func TestTIDClock_ClockIDGetter(t *testing.T) {
	t.Parallel()
	clock := NewTIDClock(42)
	require.Equal(t, uint(42), clock.ClockID())

	tid := NewTIDFromTime(time.Now(), 7)
	clock2 := ClockFromTID(tid)
	require.Equal(t, uint(7), clock2.ClockID())
}
