package declaration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/lexicon"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

const fixtureCID = "bafyreiangzeq6wkzgdywr6x6mfzue6plymwjcn5yc45kx27migmeahilme"

func rawFixture(nsid string) *RawResult {
	schema, _ := json.Marshal(map[string]any{
		"lexicon": 1, "id": nsid,
		"defs": map[string]any{"main": map[string]any{
			"type": "space", "key": "any", "name": "Forum", "collections": []string{"com.example.post"},
		}},
	})
	return &RawResult{Schema: schema, URI: "at://did:plc:resolver/com.atproto.lexicon.schema/" + nsid, CID: fixtureCID}
}

func resolvedFixture(nsid string) *Resolved {
	raw := rawFixture(nsid)
	schema, _ := lexiconFromRaw(raw.Schema)
	declaration, _ := Validate(schema)
	uri, _ := atmos.ParseATURI(raw.URI)
	return &Resolved{Declaration: declaration, URI: uri, CID: raw.CID}
}

func lexiconFromRaw(raw []byte) (*lexicon.Schema, error) { return lexicon.Parse(raw) }

type fakeResolver struct {
	key     string
	result  *RawResult
	err     error
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

type deleteFailCache struct {
	*MemoryCache
	err error
}

func (c *deleteFailCache) Delete(context.Context, string) error { return c.err }

func (r *fakeResolver) CacheKey() string { return r.key }
func (r *fakeResolver) Resolve(ctx context.Context, nsid atmos.NSID) (*RawResult, error) {
	r.calls.Add(1)
	if r.started != nil {
		select {
		case r.started <- struct{}{}:
		default:
		}
	}
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.result != nil {
		copy := *r.result
		copy.Schema = append([]byte(nil), r.result.Schema...)
		return &copy, nil
	}
	return rawFixture(string(nsid)), nil
}

func TestDirectoryResolveCachesAndCopies(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	resolver := &fakeResolver{key: "resolver-a"}
	directory := Directory{Resolver: resolver, Cache: cache}
	first, err := directory.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	first.Declaration.Name = "mutated"
	second, err := directory.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.Equal(t, "Forum", second.Declaration.Name)
	require.Equal(t, int32(1), resolver.calls.Load())
}

func TestDirectoryCoalescesConcurrentLookups(t *testing.T) {
	t.Parallel()
	resolver := &fakeResolver{key: "resolver-a", started: make(chan struct{}, 1), release: make(chan struct{})}
	cache, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	directory := Directory{Resolver: resolver, Cache: cache}
	const workers = 32
	errs := make(chan error, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	for range workers {
		go func() {
			ready.Done()
			_, err := directory.Resolve(context.Background(), "com.example.forum")
			errs <- err
		}()
	}
	ready.Wait()
	<-resolver.started
	close(resolver.release)
	for range workers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), resolver.calls.Load())
}

func TestDirectoryNegativeCache(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	resolver := &fakeResolver{key: "resolver-a", err: ErrNotFound}
	directory := Directory{Resolver: resolver, Cache: cache}
	for range 2 {
		_, err := directory.Resolve(context.Background(), "com.example.forum")
		require.ErrorIs(t, err, ErrNotFound)
	}
	require.Equal(t, int32(1), resolver.calls.Load())
}

func TestDirectoryAuxiliaryStateIsBoundedByActiveWork(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(5, 10_000)
	require.NoError(t, err)
	directory := Directory{Resolver: &fakeResolver{key: "resolver-a"}, Cache: cache}
	for i := range 200 {
		nsid := atmos.NSID(fmt.Sprintf("com.example.forum%d", i))
		_, err := directory.Resolve(context.Background(), nsid)
		require.NoError(t, err)
		require.NoError(t, directory.Purge(context.Background(), nsid))
	}

	directory.mu.Lock()
	defer directory.mu.Unlock()
	require.Empty(t, directory.flights)
	require.Empty(t, directory.generations)
	require.Empty(t, directory.cacheLocks)
}

func TestDirectoryPurgeFencesInflightPopulation(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	resolver := &fakeResolver{key: "resolver-a", started: make(chan struct{}, 1), release: make(chan struct{})}
	directory := Directory{Resolver: resolver, Cache: cache}
	done := make(chan error, 1)
	go func() {
		_, err := directory.Resolve(context.Background(), "com.example.forum")
		done <- err
	}()
	<-resolver.started
	require.NoError(t, directory.Purge(context.Background(), "com.example.forum"))
	close(resolver.release)
	require.NoError(t, <-done)
	_, err = directory.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.Equal(t, int32(2), resolver.calls.Load())
}

func TestDirectoryFailedPurgeDoesNotFenceInflightPopulation(t *testing.T) {
	t.Parallel()
	memory, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	want := fmt.Errorf("delete unavailable")
	cache := &deleteFailCache{MemoryCache: memory, err: want}
	resolver := &fakeResolver{key: "resolver-a", started: make(chan struct{}, 1), release: make(chan struct{})}
	directory := Directory{Resolver: resolver, Cache: cache}
	done := make(chan error, 1)
	go func() {
		_, resolveErr := directory.Resolve(context.Background(), "com.example.forum")
		done <- resolveErr
	}()
	<-resolver.started
	require.ErrorIs(t, directory.Purge(context.Background(), "com.example.forum"), want)
	close(resolver.release)
	require.NoError(t, <-done)
	_, err = directory.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.Equal(t, int32(1), resolver.calls.Load(), "a failed purge must not partially fence cache publication")
}

func TestDirectoryResolveAfterPurgeDoesNotJoinStaleFlight(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	resolver := &fakeResolver{key: "resolver-a", started: make(chan struct{}, 2), release: make(chan struct{})}
	directory := Directory{Resolver: resolver, Cache: cache}
	first := make(chan error, 1)
	go func() {
		_, resolveErr := directory.Resolve(context.Background(), "com.example.forum")
		first <- resolveErr
	}()
	<-resolver.started
	require.NoError(t, directory.Purge(context.Background(), "com.example.forum"))
	second := make(chan error, 1)
	go func() {
		_, resolveErr := directory.Resolve(context.Background(), "com.example.forum")
		second <- resolveErr
	}()
	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("post-purge lookup joined the stale in-flight lookup")
	}
	close(resolver.release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.Equal(t, int32(2), resolver.calls.Load())
	_, err = directory.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.Equal(t, int32(2), resolver.calls.Load())
}

func TestDirectoryTrustPartition(t *testing.T) {
	t.Parallel()
	cache, err := NewMemoryCache(10, 10_000)
	require.NoError(t, err)
	a := &fakeResolver{key: "resolver-a"}
	b := &fakeResolver{key: "resolver-b"}
	_, err = (&Directory{Resolver: a, Cache: cache}).Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	_, err = (&Directory{Resolver: b, Cache: cache}).Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.Equal(t, int32(1), a.calls.Load())
	require.Equal(t, int32(1), b.calls.Load())
}

func TestDirectoryRejectsUntrustedResultShapes(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*RawResult){
		"wrong id":   func(r *RawResult) { r.Schema = rawFixture("com.example.other").Schema },
		"bad schema": func(r *RawResult) { r.Schema = []byte(`{"lexicon":1}`) },
		"oversize":   func(r *RawResult) { r.Schema = make([]byte, 100) },
		"bad uri":    func(r *RawResult) { r.URI = "https://example.com" },
		"bad cid":    func(r *RawResult) { r.CID = "not-a-cid" },
		// Syntactically valid provenance for the wrong record is not evidence for
		// the requested declaration.
		"unrelated collection": func(r *RawResult) { r.URI = "at://did:plc:resolver/com.example.other/com.example.forum" },
		"unrelated record":     func(r *RawResult) { r.URI = "at://did:plc:resolver/com.atproto.lexicon.schema/com.example.other" },
		"partial uri":          func(r *RawResult) { r.URI = "at://did:plc:resolver" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			raw := rawFixture("com.example.forum")
			mutate(raw)
			directory := Directory{Resolver: &fakeResolver{key: "resolver", result: raw}}
			if name == "oversize" {
				directory.MaxSchema = 50
			}
			_, err := directory.Resolve(context.Background(), "com.example.forum")
			require.Error(t, err)
		})
	}
}

func TestXRPCResolver(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "com.example.forum", r.URL.Query().Get("nsid"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cid": fixtureCID, "uri": rawFixture("com.example.forum").URI,
			"schema": json.RawMessage(rawFixture("com.example.forum").Schema),
		})
	}))
	t.Cleanup(srv.Close)
	resolver := &XRPCResolver{Client: &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(srv.Client()), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}}
	raw, err := resolver.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.NotEmpty(t, raw.Schema)
	require.Equal(t, srv.URL, resolver.CacheKey())
}

func TestXRPCResolverNotFound(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "LexiconNotFound"})
	}))
	t.Cleanup(srv.Close)
	resolver := &XRPCResolver{Client: &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(srv.Client()), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}}
	_, err := resolver.Resolve(context.Background(), "com.example.forum")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDirectoryWaiterCancellation(t *testing.T) {
	t.Parallel()
	resolver := &fakeResolver{key: "resolver", started: make(chan struct{}, 1), release: make(chan struct{})}
	directory := Directory{Resolver: resolver}
	leader := make(chan error, 1)
	go func() { _, err := directory.Resolve(context.Background(), "com.example.forum"); leader <- err }()
	<-resolver.started
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := directory.Resolve(ctx, "com.example.forum")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(resolver.release)
	require.NoError(t, <-leader)
}

func TestDirectoryLeaderCancellationDoesNotCancelWaiter(t *testing.T) {
	t.Parallel()
	resolver := &fakeResolver{key: "resolver", started: make(chan struct{}, 1), release: make(chan struct{})}
	directory := Directory{Resolver: resolver}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() { _, err := directory.Resolve(leaderCtx, "com.example.forum"); leader <- err }()
	<-resolver.started

	waiter := make(chan error, 1)
	go func() { _, err := directory.Resolve(context.Background(), "com.example.forum"); waiter <- err }()
	require.Eventually(t, func() bool {
		directory.mu.Lock()
		defer directory.mu.Unlock()
		flight := directory.flights["resolver\x00com.example.forum"]
		return flight != nil && flight.waiters == 2
	}, time.Second, time.Millisecond)

	cancelLeader()
	require.ErrorIs(t, <-leader, context.Canceled)
	close(resolver.release)
	require.NoError(t, <-waiter)
	require.Equal(t, int32(1), resolver.calls.Load())
}

func TestDirectoryLastWaiterCancellationAbandonsFlight(t *testing.T) {
	t.Parallel()
	resolver := &fakeResolver{key: "resolver", started: make(chan struct{}, 1), release: make(chan struct{})}
	directory := Directory{Resolver: resolver}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := directory.Resolve(ctx, "com.example.forum"); done <- err }()
	<-resolver.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool {
		directory.mu.Lock()
		defer directory.mu.Unlock()
		return len(directory.flights) == 0 && len(directory.generations) == 0 && len(directory.cacheLocks) == 0
	}, time.Second, time.Millisecond)

	close(resolver.release)
	_, err := directory.Resolve(context.Background(), "com.example.forum")
	require.NoError(t, err)
	require.Equal(t, int32(2), resolver.calls.Load())
}
