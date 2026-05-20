package sync_test

import (
	"context"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemChainStore_LoadMissingReturnsNilNil(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	state, err := store.Load(context.Background(), atmos.DID("did:plc:abc"))
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemChainStore_SaveThenLoad(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	want := sync.ChainState{Rev: "3l3qo2vutsw2b", Data: cid}
	require.NoError(t, store.Save(context.Background(), did, want))

	got, err := store.Load(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.Rev, got.Rev)
	assert.True(t, got.Data.Equal(want.Data))
}

func TestMemChainStore_Delete(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	did := atmos.DID("did:plc:abc")
	cid, err := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")
	require.NoError(t, err)

	require.NoError(t, store.Save(context.Background(), did, sync.ChainState{Rev: "r1", Data: cid}))
	require.NoError(t, store.Delete(context.Background(), did))

	state, err := store.Load(context.Background(), did)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func TestMemChainStore_DeleteMissingNoError(t *testing.T) {
	t.Parallel()

	store := sync.NewMemChainStore()
	require.NoError(t, store.Delete(context.Background(), atmos.DID("did:plc:never-saved")))
}
