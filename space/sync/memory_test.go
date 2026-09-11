package sync

import (
	"context"
	"strings"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/stretchr/testify/require"
)

func TestMemoryStoreEnforcesLogicalByteAndStateBounds(t *testing.T) {
	t.Parallel()
	store, err := NewMemoryStore(1, 1, 4, 3000)
	require.NoError(t, err)
	lifecycle, err := store.Lifecycle(context.Background(), testSpace)
	require.NoError(t, err)
	key := RepoKey{Space: testSpace, Author: testAuthor}
	stage, err := store.Begin(context.Background(), key, 0, lifecycle.Generation)
	require.NoError(t, err)
	path, err := spaces.ParseRecordPath("com.example.post/one")
	require.NoError(t, err)
	data, err := cbor.Marshal(map[string]any{"$type": "com.example.post", "text": strings.Repeat("x", 2048)})
	require.NoError(t, err)
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	stage.Index[path] = cid
	stage.Records[path] = Record{CID: cid, Data: data}
	repoCommit, err := spaces.NewRepoCommitFromIndex(stage.Index)
	require.NoError(t, err)
	stage.State = repoCommit.State()
	require.ErrorContains(t, store.SaveStage(context.Background(), stage), "byte capacity")
	_, err = store.Lifecycle(context.Background(), "at://did:plc:other/space/com.example.space/main")
	require.ErrorContains(t, err, "space-state capacity")
	_, err = store.RepoLifecycle(context.Background(), RepoKey{Space: testSpace, Author: "did:plc:other"})
	require.ErrorContains(t, err, "repo-state capacity")
}
