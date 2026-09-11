package sync_test

import (
	"context"
	"testing"

	spacesync "github.com/jcalabro/atmos/space/sync"
	"github.com/jcalabro/atmos/space/sync/storetest"
	"github.com/stretchr/testify/require"
)

func TestMemoryStoreConformance(t *testing.T) {
	t.Parallel()
	err := storetest.Run(context.Background(), func() (spacesync.Store, spacesync.FailureInjector, error) {
		store, err := spacesync.NewMemoryStore(10, 10, 20, 10<<20)
		return store, store, err
	})
	require.NoError(t, err)
}
