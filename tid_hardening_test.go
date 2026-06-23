package atmos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// H3: NewTID must reject (panic on) timestamps outside the 53-bit range rather
// than silently wrapping into a corrupt-but-syntactically-valid TID.
func TestNewTID_NegativeTimestamp_Panics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { NewTID(-1, 0) })
}

func TestNewTID_TimestampTooLarge_Panics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { NewTID(1<<53, 0) })
}

// H3: clockID is a 10-bit field; values that don't fit are programmer errors
// and must panic rather than silently truncate.
func TestNewTID_ClockIDTooLarge_Panics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { NewTID(0, 1<<10) })
}

// NewTIDFromTime must reject pre-epoch times.
func TestNewTIDFromTime_PreEpoch_Panics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { NewTIDFromTime(time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC), 0) })
}

// TIDClock.Next() must not panic when seeded at (or driven to) the 53-bit
// timestamp ceiling — a long-running or adversarially-seeded clock (e.g. via
// ClockFromTID from an untrusted repo rev) must saturate, not crash.
func TestTIDClock_Next_AtCeiling_NoPanic(t *testing.T) {
	t.Parallel()
	const maxMicros = int64(1)<<53 - 1

	// Seed a clock exactly at the ceiling via a max-valued TID.
	maxTID := NewTID(maxMicros, 0)
	clock := ClockFromTID(maxTID)

	require.NotPanics(t, func() {
		tid := clock.Next()
		// Saturates at the max timestamp.
		require.Equal(t, maxMicros, tid.Time().UnixMicro())
		// A second call also must not panic.
		_ = clock.Next()
	})
}

// In-range values must continue to round-trip exactly.
func TestNewTID_InRange_RoundTrips(t *testing.T) {
	t.Parallel()
	const maxMicros = int64(1)<<53 - 1
	for _, micros := range []int64{0, 1, 1_000_000, maxMicros} {
		tid := NewTID(micros, 1023)
		parsed, err := ParseTID(tid.String())
		require.NoError(t, err)
		require.Equal(t, micros, parsed.Time().UnixMicro())
		require.Equal(t, uint(1023), parsed.ClockID())
	}
}
