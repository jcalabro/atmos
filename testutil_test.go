package atmos

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// loadTestVectors reads a test vector file from testdata/.
// Lines starting with # are comments, blank lines are skipped.
func loadTestVectors(t *testing.T, filename string) []string {
	t.Helper()

	path := filepath.Join("testdata", filename)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open test vectors %s: %v", filename, err)
	}
	defer func() { require.NoError(t, f.Close()) }()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// Skip blank lines and comment lines (comments start with # after optional whitespace).
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan test vectors %s: %v", filename, err)
	}
	if len(lines) == 0 {
		t.Fatalf("no test vectors in %s", filename)
	}
	return lines
}
