package sync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlphaLimits(t *testing.T) {
	t.Parallel()
	limits := AlphaLimits()
	require.NoError(t, limits.Validate())
	require.Equal(t, limits.MaxAuthors, limits.DirectoryPageSize*limits.MaxDirectoryPages)
	require.LessOrEqual(t, limits.MaxRepoBytes, int64(limits.CAR.MaxTotalSize))
}

func TestAlphaSchedulerOptions(t *testing.T) {
	t.Parallel()
	options := AlphaSchedulerOptions(func(JobResult) {})
	require.NoError(t, options.Validate())
	require.Equal(t, AlphaLimits().MaxAuthors, options.QueueCapacity)
	require.Positive(t, options.Workers)
}
