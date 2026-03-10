package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRepoPath(t *testing.T) {
	t.Parallel()
	nsid, rkey, err := ParseRepoPath("app.bsky.feed.post/abc123")
	require.NoError(t, err)
	require.Equal(t, NSID("app.bsky.feed.post"), nsid)
	require.Equal(t, RecordKey("abc123"), rkey)
}

func TestParseRepoPath_Invalid(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoPath("noslash")
	require.Error(t, err)

	_, _, err = ParseRepoPath("not-an-nsid/abc123")
	require.Error(t, err)

	_, _, err = ParseRepoPath("app.bsky.feed.post/.")
	require.Error(t, err)
}
