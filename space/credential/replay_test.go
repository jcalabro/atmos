package credential

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryReplayStoreAtomicAndBounded(t *testing.T) {
	t.Parallel()
	store, err := NewMemoryReplayStore(2)
	require.NoError(t, err)
	now := time.Unix(1_000, 0)
	store.now = func() time.Time { return now }
	require.NoError(t, store.Consume(context.Background(), ReplayDelegation, "same", now.Add(time.Minute)))
	require.ErrorIs(t, store.Consume(context.Background(), ReplayDelegation, "same", now.Add(time.Minute)), ErrReplay)
	require.NoError(t, store.Consume(context.Background(), ReplayDPoP, "same", now.Add(time.Minute)))
	require.ErrorIs(t, store.Consume(context.Background(), ReplayAttestation, "third", now.Add(time.Minute)), ErrReplayCapacity)
	store.now = func() time.Time { return now.Add(2 * time.Minute) }
	require.NoError(t, store.Consume(context.Background(), ReplayAttestation, "third", now.Add(3*time.Minute)))
}

func TestMemoryReplayStoreConcurrentWinner(t *testing.T) {
	t.Parallel()
	store, err := NewMemoryReplayStore(32)
	require.NoError(t, err)
	var winners atomic.Int64
	var wg sync.WaitGroup
	for i := range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := store.Consume(context.Background(), ReplayDPoP, "proof", time.Now().Add(time.Minute))
			if err == nil {
				winners.Add(1)
				return
			}
			require.ErrorIs(t, err, ErrReplay)
		}()
		_ = i
	}
	wg.Wait()
	require.EqualValues(t, 1, winners.Load())
}

func TestMemoryReplayStoreValidation(t *testing.T) {
	t.Parallel()
	_, err := NewMemoryReplayStore(0)
	require.Error(t, err)
	store, err := NewMemoryReplayStore(1)
	require.NoError(t, err)
	now := time.Now()
	for _, tc := range []struct {
		ns  ReplayNamespace
		key string
		exp time.Time
	}{
		{"", "x", now.Add(time.Minute)},
		{ReplayDPoP, "", now.Add(time.Minute)},
		{ReplayDPoP, string(make([]byte, maxReplayIDBytes+1)), now.Add(time.Minute)},
		{ReplayDPoP, "x", time.Time{}},
		{ReplayDPoP, "past", now.Add(-time.Second)},
		{ReplayNamespace("unknown"), "x", now.Add(time.Minute)},
	} {
		require.Error(t, store.Consume(context.Background(), tc.ns, tc.key, tc.exp), fmt.Sprint(tc))
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, store.Consume(cancelled, ReplayDPoP, "x", time.Now().Add(time.Minute)), context.Canceled)
}

type failingReplayStore struct{ calls atomic.Int64 }

func (s *failingReplayStore) Consume(context.Context, ReplayNamespace, string, time.Time) error {
	s.calls.Add(1)
	return errors.New("backend unavailable")
}
