package lexgen

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHydrateScriptPublishesAndRollsBack(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "js" {
		t.Skip("the hydration script requires POSIX shell tools")
	}
	t.Parallel()
	root, commits := newHydrationFixture(t)

	t.Run("publish", func(t *testing.T) {
		runHydrationScript(t, root, "")
		body, err := os.ReadFile(filepath.Join(root, "lexicons", "com", "example.json"))
		require.NoError(t, err)
		require.JSONEq(t, `{"lexicon":1}`, string(body))
		_, err = os.Stat(filepath.Join(root, "lexicons", "old.json"))
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	for _, mode := range []string{"failure", "HUP", "INT", "TERM"} {
		mode := mode
		t.Run("rollback_"+mode, func(t *testing.T) {
			resetHydrationCache(t, root)
			installMoveFailure(t, root, mode)
			cmd := hydrationCommand(t.Context(), root)
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"))
			err := cmd.Run()
			require.Error(t, err)
			body, readErr := os.ReadFile(filepath.Join(root, "lexicons", "old.json"))
			require.NoError(t, readErr)
			require.Equal(t, []byte("old\n"), body)
			_, statErr := os.Stat(filepath.Join(root, "lexicons", "com", "example.json"))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}

	_ = commits
}

func newHydrationFixture(t *testing.T) (string, map[string]string) {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "scripts"), 0o755))
	script, err := os.ReadFile("../scripts/hydrate-lexicons.sh")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "scripts", "hydrate-lexicons.sh"), script, 0o755))

	type sourceFile struct{ path, body string }
	sources := map[string][]sourceFile{
		"atproto": {{"lexicons/com/example.json", `{"lexicon":1}`}},
		"bsky":    {{"lexicons/app/example.json", `{"lexicon":1,"id":"app.example"}`}},
		"status":  {{"lexicons/xyz/status.json", `{"lexicon":1,"id":"xyz.status"}`}},
	}
	commits := make(map[string]string, len(sources))
	for name, files := range sources {
		repo := filepath.Join(root, "sources", name)
		for _, file := range files {
			path := filepath.Join(repo, filepath.FromSlash(file.path))
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, []byte(file.body), 0o600))
		}
		runGit(t, repo, "init", "-q")
		runGit(t, repo, "add", ".")
		runGit(t, repo, "-c", "user.name=atmos-test", "-c", "user.email=atmos@example.invalid", "commit", "-qm", "fixture")
		out, err := exec.CommandContext(t.Context(), "git", "-C", repo, "rev-parse", "HEAD").Output()
		require.NoError(t, err)
		commits[name] = string(out[:40])
	}

	lock := fmt.Sprintf("atproto %s %s\nbsky %s %s\nstatusphere-example-app %s %s\n",
		filepath.Join(root, "sources", "atproto"), commits["atproto"],
		filepath.Join(root, "sources", "bsky"), commits["bsky"],
		filepath.Join(root, "sources", "status"), commits["status"])
	require.NoError(t, os.WriteFile(filepath.Join(root, "lexgen.lock"), []byte(lock), 0o600))

	manifest := ""
	for _, entry := range []struct{ path, body string }{
		{"./app/example.json", `{"lexicon":1,"id":"app.example"}`},
		{"./com/example.json", `{"lexicon":1}`},
		{"./xyz/status.json", `{"lexicon":1,"id":"xyz.status"}`},
	} {
		manifest += fmt.Sprintf("%x  %s\n", sha256.Sum256([]byte(entry.body)), entry.path)
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "lexicons.manifest"), []byte(manifest), 0o600))
	resetHydrationCache(t, root)
	return root, commits
}

func resetHydrationCache(t *testing.T, root string) {
	t.Helper()
	require.NoError(t, os.RemoveAll(filepath.Join(root, "lexicons")))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "lexicons"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "lexicons", "old.json"), []byte("old\n"), 0o600))
	require.NoError(t, os.RemoveAll(filepath.Join(root, "bin")))
}

func installMoveFailure(t *testing.T, root, mode string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bin"), 0o755))
	realMove, err := exec.LookPath("mv")
	require.NoError(t, err)
	action := "exit 91"
	if mode != "failure" {
		action = "kill -" + mode + " \"$PPID\"; exit 0"
	}
	wrapper := "#!/usr/bin/env bash\nset -eu\nlast=\"${!#}\"\nif [[ \"$1\" == */lexicons && \"$last\" == lexicons ]]; then " + action + "; fi\nexec " + realMove + " \"$@\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, "bin", "mv"), []byte(wrapper), 0o755))
}

func runHydrationScript(t *testing.T, root, path string) {
	t.Helper()
	cmd := hydrationCommand(t.Context(), root)
	if path != "" {
		cmd.Env = append(os.Environ(), "PATH="+path)
	}
	require.NoError(t, cmd.Run())
}

func hydrationCommand(ctx context.Context, root string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(root, "scripts", "hydrate-lexicons.sh"))
	cmd.Dir = root
	return cmd
}

func runGit(t *testing.T, repo string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", repo}, args...)...)
	require.NoError(t, cmd.Run())
}
