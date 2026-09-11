package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunMultipleRoots(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	rootA := filepath.Join(tmp, "a")
	rootB := filepath.Join(tmp, "b")
	require.NoError(t, os.MkdirAll(rootA, 0o755))
	require.NoError(t, os.MkdirAll(rootB, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rootA, "defs.json"), []byte(`{"lexicon":1,"id":"com.example.defs","defs":{"thing":{"type":"object","properties":{"value":{"type":"string"}}}}}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(rootB, "query.json"), []byte(`{"lexicon":1,"id":"com.example.getThing","defs":{"main":{"type":"query","output":{"encoding":"application/json","schema":{"type":"ref","ref":"com.example.defs#thing"}}}}}`), 0o600))
	cfg := filepath.Join(tmp, "config.json")
	require.NoError(t, os.WriteFile(cfg, []byte(`{"packages":[{"prefix":"com.example","package":"example","outDir":"api/example","import":"example/api"}]}`), 0o600))
	require.NoError(t, run([]string{rootA, rootB}, cfg, tmp))
	_, err := os.Stat(filepath.Join(tmp, "api/example/examplegetthing.go"))
	require.NoError(t, err)
}

func TestRunRejectsDuplicateAcrossRoots(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	for _, name := range []string{"a", "b"} {
		root := filepath.Join(tmp, name)
		require.NoError(t, os.MkdirAll(root, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "same.json"), []byte(`{"lexicon":1,"id":"com.example.same","defs":{"main":{"type":"object"}}}`), 0o600))
	}
	cfg := filepath.Join(tmp, "config.json")
	require.NoError(t, os.WriteFile(cfg, []byte(`{"packages":[]}`), 0o600))
	err := run([]string{filepath.Join(tmp, "a"), filepath.Join(tmp, "b")}, cfg, tmp)
	require.ErrorContains(t, err, "duplicate schema")
}

func TestRunRejectsMissingRoot(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	cfg := filepath.Join(tmp, "config.json")
	require.NoError(t, os.WriteFile(cfg, []byte(`{"packages":[]}`), 0o600))
	err := run([]string{filepath.Join(tmp, "missing")}, cfg, tmp)
	require.Error(t, err)
}
