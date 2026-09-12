package space

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlphaCARLimits(t *testing.T) {
	t.Parallel()
	limits := AlphaCARLimits()
	require.NoError(t, limits.Validate())
	require.Equal(t, 100_000, limits.MaxRecords)
	require.GreaterOrEqual(t, limits.MaxIndexSize, uint64(6_400_003))
}
