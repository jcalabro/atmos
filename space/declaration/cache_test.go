package declaration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryCacheBoundsTTLAndCopies(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(2, 500)
	require.NoError(t, err)
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }
	ctx := context.Background()
	value := CacheValue{Resolved: resolvedFixture("com.example.one")}
	require.NoError(t, cache.Set(ctx, "one", value, time.Minute))
	value.Resolved.Declaration.Name = "mutated"
	got, err := cache.Get(ctx, "one")
	require.NoError(t, err)
	require.Equal(t, "Forum", got.Resolved.Declaration.Name)
	got.Resolved.Declaration.Name = "also mutated"
	again, err := cache.Get(ctx, "one")
	require.NoError(t, err)
	require.Equal(t, "Forum", again.Resolved.Declaration.Name)

	require.NoError(t, cache.Set(ctx, "two", CacheValue{NotFound: true}, time.Minute))
	require.NoError(t, cache.Set(ctx, "three", CacheValue{NotFound: true}, time.Minute))
	_, err = cache.Get(ctx, "one")
	require.ErrorIs(t, err, ErrCacheMiss)
	now = now.Add(2 * time.Minute)
	_, err = cache.Get(ctx, "two")
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestMemoryCacheRejectsOversizedEntry(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(1, 32)
	require.NoError(t, err)
	resolved := resolvedFixture("com.example.one")
	resolved.Declaration.Name = strings.Repeat("x", 100)
	err = cache.Set(context.Background(), "key", CacheValue{Resolved: resolved}, time.Minute)
	require.ErrorIs(t, err, ErrCacheEntryTooLarge)
}

func TestMemoryCacheValidatesConfigurationAndEntries(t *testing.T) {
	t.Parallel()
	_, err := NewMemoryCache(0, 1)
	require.Error(t, err)
	cache, err := NewMemoryCache(1, 100)
	require.NoError(t, err)
	require.Error(t, cache.Set(context.Background(), "", CacheValue{NotFound: true}, time.Minute))
	require.Error(t, cache.Set(context.Background(), "x", CacheValue{}, time.Minute))
	require.Error(t, cache.Set(context.Background(), "x", CacheValue{NotFound: true, Resolved: resolvedFixture("com.example.one")}, time.Minute))
	require.Error(t, cache.Set(context.Background(), "x", CacheValue{NotFound: true}, 0))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, cache.Delete(cancelled, "x"), context.Canceled)
}

type failingCache struct{ err error }

func (c failingCache) Get(context.Context, string) (CacheValue, error)              { return CacheValue{}, c.err }
func (c failingCache) Set(context.Context, string, CacheValue, time.Duration) error { return c.err }
func (c failingCache) Delete(context.Context, string) error                         { return c.err }

func TestDirectoryPropagatesCacheFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("backend unavailable")
	directory := Directory{Resolver: &fakeResolver{key: "trust"}, Cache: failingCache{err: want}}
	_, err := directory.Resolve(context.Background(), "com.example.forum")
	require.ErrorIs(t, err, want)
}
