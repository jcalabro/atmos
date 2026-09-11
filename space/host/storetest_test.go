package host_test

import (
	"context"
	"testing"

	spacehost "github.com/jcalabro/atmos/space/host"
	"github.com/jcalabro/atmos/space/host/storetest"
	"github.com/stretchr/testify/require"
)

func TestMemoryStoreConformance(t *testing.T) {
	t.Parallel()
	err := storetest.Run(context.Background(), func() (spacehost.Store, error) {
		return spacehost.NewMemoryStore(spacehost.MemoryStoreOptions{MaxSpaces: 4, MaxMembers: 8, MaxWriters: 8, MaxOutbox: 8})
	})
	require.NoError(t, err)
}
