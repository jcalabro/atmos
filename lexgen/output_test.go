package lexgen

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFilesDoesNotModifyOutputsWhenPreparationFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.go")
	require.NoError(t, os.WriteFile(existing, []byte("old"), 0o644))

	blocker := filepath.Join(dir, "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, nil, 0o644))

	err := WriteFiles(map[string][]byte{
		existing:                               []byte("new"),
		filepath.Join(blocker, "generated.go"): []byte("unreachable"),
	})
	require.Error(t, err)

	got, err := os.ReadFile(existing)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), got)
}
