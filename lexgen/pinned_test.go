package lexgen

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jcalabro/atmos/lexicon"
	"github.com/stretchr/testify/require"
)

func TestPinnedLexiconManifest(t *testing.T) {
	t.Parallel()
	want, err := os.ReadFile("../lexicons.manifest")
	require.NoError(t, err)

	var entries []string
	err = filepath.WalkDir("../lexicons", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel("../lexicons", path)
		if err != nil {
			return err
		}
		entries = append(entries, fmt.Sprintf("%x  ./%s", sha256.Sum256(body), filepath.ToSlash(relative)))
		return nil
	})
	require.NoError(t, err)
	got := strings.Join(entries, "\n") + "\n"
	require.Equal(t, string(want), got, "run just hydrate-lexicons to restore the pinned cache")
}

func TestPinnedCombinedCatalogDeterministic(t *testing.T) {
	t.Parallel()
	mainSchemas, err := lexicon.ParseDir("../lexicons")
	require.NoError(t, err, "run just hydrate-lexicons before testing")
	spaceSchemas, err := lexicon.ParseDir("../lexicons-space")
	require.NoError(t, err)
	require.Len(t, spaceSchemas, 29)

	cat := lexicon.NewCatalog()
	require.NoError(t, cat.AddAll(mainSchemas))
	require.NoError(t, cat.AddAll(spaceSchemas))
	require.NoError(t, cat.Resolve())
	require.NotNil(t, cat.Schema("com.atproto.space.getRepo"))
	require.NotNil(t, cat.Schema("com.atproto.simplespace.putMember"))

	cfgBytes, err := os.ReadFile("../lexgen.json")
	require.NoError(t, err)
	var cfg Config
	require.NoError(t, json.Unmarshal(cfgBytes, &cfg))
	first, err := Generate(&cfg, cat)
	require.NoError(t, err)
	second, err := Generate(&cfg, cat)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Contains(t, first, "api/comatproto/spacegetrepo.go")
	require.Contains(t, first, "api/comatproto/simplespaceputmember.go")
}
