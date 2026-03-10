package identity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDIDDocJSON = `{
	"id": "did:plc:testuser123",
	"alsoKnownAs": ["at://alice.test"],
	"verificationMethod": [{
		"id": "#atproto",
		"type": "Multikey",
		"controller": "did:plc:testuser123",
		"publicKeyMultibase": "zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169"
	}],
	"service": [{
		"id": "#atproto_pds",
		"type": "AtprotoPersonalDataServer",
		"serviceEndpoint": "https://pds.example.com"
	}]
}`

func TestParseDIDDocument(t *testing.T) {
	t.Parallel()

	doc, err := ParseDIDDocument([]byte(testDIDDocJSON))
	require.NoError(t, err)
	assert.Equal(t, "did:plc:testuser123", doc.ID)
	assert.Equal(t, []string{"at://alice.test"}, doc.AlsoKnownAs)
	assert.Len(t, doc.VerificationMethod, 1)
	assert.Equal(t, "#atproto", doc.VerificationMethod[0].ID)
	assert.Len(t, doc.Service, 1)
}

func TestIdentityFromDocument(t *testing.T) {
	t.Parallel()

	doc, err := ParseDIDDocument([]byte(testDIDDocJSON))
	require.NoError(t, err)

	id, err := IdentityFromDocument(doc)
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:testuser123"), id.DID)
	assert.Equal(t, atmos.Handle("alice.test"), id.Handle)
	assert.Contains(t, id.Keys, "atproto")
	assert.Equal(t, "zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169", id.Keys["atproto"].Multibase)
}

func TestIdentity_PDSEndpoint(t *testing.T) {
	t.Parallel()

	doc, err := ParseDIDDocument([]byte(testDIDDocJSON))
	require.NoError(t, err)
	id, err := IdentityFromDocument(doc)
	require.NoError(t, err)
	assert.Equal(t, "https://pds.example.com", id.PDSEndpoint())
}

func TestIdentity_PDSEndpointMissing(t *testing.T) {
	t.Parallel()
	id := &Identity{Services: map[string]ServiceEndpoint{}}
	assert.Equal(t, "", id.PDSEndpoint())
}

func TestIdentity_PublicKey(t *testing.T) {
	t.Parallel()

	doc, err := ParseDIDDocument([]byte(testDIDDocJSON))
	require.NoError(t, err)
	id, err := IdentityFromDocument(doc)
	require.NoError(t, err)

	pk, err := id.PublicKey()
	require.NoError(t, err)
	assert.NotNil(t, pk)
	assert.Equal(t, "zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169", pk.Multibase())
}

func TestIdentity_PublicKeyMissing(t *testing.T) {
	t.Parallel()
	id := &Identity{Keys: map[string]Key{}}
	_, err := id.PublicKey()
	assert.Error(t, err)
}

func TestIdentityFromDocument_NoHandle(t *testing.T) {
	t.Parallel()

	raw := `{"id":"did:plc:abc","alsoKnownAs":[],"verificationMethod":[],"service":[]}`
	doc, err := ParseDIDDocument([]byte(raw))
	require.NoError(t, err)
	id, err := IdentityFromDocument(doc)
	require.NoError(t, err)
	assert.Equal(t, atmos.HandleInvalid, id.Handle)
}

func TestFragmentFromID(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "atproto", fragmentFromID("did:plc:abc#atproto"))
	assert.Equal(t, "atproto", fragmentFromID("#atproto"))
	assert.Equal(t, "", fragmentFromID("nofragment"))
	assert.Equal(t, "", fragmentFromID("trailing#"))
}

// mockResolver is a test helper that simulates network resolution.
type mockResolver struct {
	docs    map[string]*DIDDocument
	handles map[string]atmos.DID
}

func (m *mockResolver) ResolveDID(_ context.Context, did atmos.DID) (*DIDDocument, error) {
	doc, ok := m.docs[string(did)]
	if !ok {
		return nil, ErrDIDNotFound
	}
	return doc, nil
}

func (m *mockResolver) ResolveHandle(_ context.Context, handle atmos.Handle) (atmos.DID, error) {
	did, ok := m.handles[string(handle)]
	if !ok {
		return "", ErrHandleNotFound
	}
	return did, nil
}

func makeDIDDoc(did, handle string) *DIDDocument {
	doc := &DIDDocument{
		ID:                 did,
		VerificationMethod: []VerificationMethod{},
		Service:            []Service{},
	}
	if handle != "" {
		doc.AlsoKnownAs = []string{"at://" + handle}
	}
	return doc
}

func TestDirectory_LookupHandle_Success(t *testing.T) {
	t.Parallel()

	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	dir := &Directory{Resolver: r}

	id, err := dir.LookupHandle(context.Background(), "alice.test")
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:alice"), id.DID)
	assert.Equal(t, atmos.Handle("alice.test"), id.Handle)
}

func TestDirectory_LookupHandle_Mismatch(t *testing.T) {
	t.Parallel()

	// DID doc declares "bob.test" but we lookup "alice.test"
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "bob.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	dir := &Directory{Resolver: r}

	id, err := dir.LookupHandle(context.Background(), "alice.test")
	require.NoError(t, err)
	assert.Equal(t, atmos.HandleInvalid, id.Handle)
}

func TestDirectory_LookupDID_Success(t *testing.T) {
	t.Parallel()

	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	dir := &Directory{Resolver: r}

	id, err := dir.LookupDID(context.Background(), "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, atmos.Handle("alice.test"), id.Handle)
}

func TestDirectory_LookupDID_HandleVerificationFails(t *testing.T) {
	t.Parallel()

	// Handle resolves to a different DID.
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:other"},
	}
	dir := &Directory{Resolver: r}

	id, err := dir.LookupDID(context.Background(), "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, atmos.HandleInvalid, id.Handle)
}

func TestDirectory_Lookup_DIDInput(t *testing.T) {
	t.Parallel()

	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	dir := &Directory{Resolver: r}

	atid, err := atmos.ParseAtIdentifier("did:plc:alice")
	require.NoError(t, err)

	id, err := dir.Lookup(context.Background(), atid)
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:alice"), id.DID)
}

func TestDirectory_Lookup_HandleInput(t *testing.T) {
	t.Parallel()

	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	dir := &Directory{Resolver: r}

	atid, err := atmos.ParseAtIdentifier("alice.test")
	require.NoError(t, err)

	id, err := dir.Lookup(context.Background(), atid)
	require.NoError(t, err)
	assert.Equal(t, atmos.Handle("alice.test"), id.Handle)
}

func TestDirectory_Cache(t *testing.T) {
	t.Parallel()

	var resolveCount atomic.Int32
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	counting := &countingResolver{r: r, count: &resolveCount}

	dir := &Directory{
		Resolver: counting,
		Cache:    NewLRUCache(100, time.Hour),
	}
	ctx := context.Background()

	// First lookup should resolve.
	_, err := dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, int32(1), resolveCount.Load())

	// Second lookup should hit cache.
	_, err = dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, int32(1), resolveCount.Load())
}

func TestDirectory_Purge(t *testing.T) {
	t.Parallel()

	var resolveCount atomic.Int32
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	counting := &countingResolver{r: r, count: &resolveCount}

	dir := &Directory{
		Resolver: counting,
		Cache:    NewLRUCache(100, time.Hour),
	}
	ctx := context.Background()

	// Populate cache.
	_, err := dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, int32(1), resolveCount.Load())

	// Cached — no new resolve.
	_, err = dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, int32(1), resolveCount.Load())

	// Purge forces re-resolve.
	dir.Purge(ctx, "did:plc:alice")

	_, err = dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, int32(2), resolveCount.Load())
}

func TestDirectory_Purge_NoCache(t *testing.T) {
	t.Parallel()

	// Purge on directory with no cache should not panic.
	dir := &Directory{Resolver: &mockResolver{}}
	dir.Purge(context.Background(), "did:plc:alice")
}

type countingResolver struct {
	r     Resolver
	count *atomic.Int32
}

func (c *countingResolver) ResolveDID(ctx context.Context, did atmos.DID) (*DIDDocument, error) {
	c.count.Add(1)
	return c.r.ResolveDID(ctx, did)
}

func (c *countingResolver) ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	return c.r.ResolveHandle(ctx, handle)
}

func TestResolver_SkipDNS(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/atproto-did" {
			_, _ = fmt.Fprint(w, "did:plc:alice")
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(&http.Client{
			Transport: &rewriteTransport{target: srv.URL},
			Timeout:   5 * time.Second,
		}),
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}

	// Should skip DNS and go straight to HTTP.
	did, err := resolver.ResolveHandle(context.Background(), "alice.bsky.social")
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:alice"), did)
}

func TestDirectory_Coalescing(t *testing.T) {
	t.Parallel()

	var resolveCount atomic.Int32
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	slow := &slowResolver{r: r, count: &resolveCount, delay: 50 * time.Millisecond}

	dir := &Directory{Resolver: slow}
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			_, err := dir.LookupDID(ctx, "did:plc:alice")
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	// Should have made only 1 resolve call due to coalescing.
	assert.Equal(t, int32(1), resolveCount.Load())
}

type slowResolver struct {
	r     Resolver
	count *atomic.Int32
	delay time.Duration
}

func (s *slowResolver) ResolveDID(ctx context.Context, did atmos.DID) (*DIDDocument, error) {
	s.count.Add(1)
	time.Sleep(s.delay)
	return s.r.ResolveDID(ctx, did)
}

func (s *slowResolver) ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	return s.r.ResolveHandle(ctx, handle)
}

func TestDirectory_LookupHandle_Cache(t *testing.T) {
	t.Parallel()

	var resolveCount atomic.Int32
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	counting := &countingResolver{r: r, count: &resolveCount}

	dir := &Directory{
		Resolver: counting,
		Cache:    NewLRUCache(100, time.Hour),
	}
	ctx := context.Background()

	_, err := dir.LookupHandle(ctx, "alice.test")
	require.NoError(t, err)
	assert.Equal(t, int32(1), resolveCount.Load())

	// Second lookup should hit cache.
	_, err = dir.LookupHandle(ctx, "alice.test")
	require.NoError(t, err)
	assert.Equal(t, int32(1), resolveCount.Load())
}

func TestDirectory_LookupDID_NoHandle(t *testing.T) {
	t.Parallel()

	// DID doc has no alsoKnownAs — should return HandleInvalid without
	// attempting handle resolution.
	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "")},
		handles: map[string]atmos.DID{},
	}
	dir := &Directory{Resolver: r}

	id, err := dir.LookupDID(context.Background(), "did:plc:alice")
	require.NoError(t, err)
	assert.Equal(t, atmos.HandleInvalid, id.Handle)
}

func TestDirectory_Coalescing_Error(t *testing.T) {
	t.Parallel()

	var resolveCount atomic.Int32
	// Resolver that always fails.
	failing := &slowResolver{
		r:     &mockResolver{docs: map[string]*DIDDocument{}, handles: map[string]atmos.DID{}},
		count: &resolveCount,
		delay: 50 * time.Millisecond,
	}

	dir := &Directory{Resolver: failing}
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range 10 {
		wg.Go(func() {
			_, errs[i] = dir.LookupDID(ctx, "did:plc:nonexistent")
		})
	}
	wg.Wait()

	// All goroutines should get an error.
	for i, err := range errs {
		assert.Error(t, err, "goroutine %d should get error", i)
	}
	// Only one resolve call due to coalescing.
	assert.Equal(t, int32(1), resolveCount.Load())
}

func TestDirectory_CacheDefensiveCopy(t *testing.T) {
	t.Parallel()

	r := &mockResolver{
		docs:    map[string]*DIDDocument{"did:plc:alice": makeDIDDoc("did:plc:alice", "alice.test")},
		handles: map[string]atmos.DID{"alice.test": "did:plc:alice"},
	}
	dir := &Directory{
		Resolver: r,
		Cache:    NewLRUCache(100, time.Hour),
	}
	ctx := context.Background()

	id1, err := dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)

	// Mutate the returned identity.
	id1.Services["evil"] = ServiceEndpoint{Type: "evil", URL: "http://evil.com"}

	// Second lookup from cache should not see the mutation.
	id2, err := dir.LookupDID(ctx, "did:plc:alice")
	require.NoError(t, err)
	assert.NotContains(t, id2.Services, "evil")
}

func TestIdentityFromDocument_SkipsBadHandle(t *testing.T) {
	t.Parallel()

	// First at:// entry is invalid, second is valid — should use second.
	raw := `{"id":"did:plc:abc","alsoKnownAs":["at://","at://bob.test"],"verificationMethod":[],"service":[]}`
	doc, err := ParseDIDDocument([]byte(raw))
	require.NoError(t, err)
	id, err := IdentityFromDocument(doc)
	require.NoError(t, err)
	assert.Equal(t, atmos.Handle("bob.test"), id.Handle)
}

// --- httptest-based resolver tests ---

func TestDefaultResolver_ResolveDID_PLC(t *testing.T) {
	t.Parallel()

	docJSON := `{"id":"did:plc:test123","alsoKnownAs":["at://alice.test"],"verificationMethod":[],"service":[]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/did:plc:test123" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, docJSON)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(srv.Client()),
		PLCURL:     gt.Some(srv.URL),
	}

	doc, err := resolver.ResolveDID(context.Background(), "did:plc:test123")
	require.NoError(t, err)
	assert.Equal(t, "did:plc:test123", doc.ID)
}

func TestDefaultResolver_ResolveDID_Web(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/did.json" {
			w.Header().Set("Content-Type", "application/json")
			// The doc ID must match the DID we're resolving.
			_, _ = fmt.Fprint(w, `{"id":"did:web:alice.test","alsoKnownAs":[],"verificationMethod":[],"service":[]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(&http.Client{
			Transport: &rewriteTransport{target: srv.URL},
			Timeout:   5 * time.Second,
		}),
	}

	doc, err := resolver.ResolveDID(context.Background(), "did:web:alice.test")
	require.NoError(t, err)
	assert.Equal(t, "did:web:alice.test", doc.ID)
}

func TestDefaultResolver_ResolveDID_WebIDMismatch(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"did:web:evil.test","alsoKnownAs":[],"verificationMethod":[],"service":[]}`)
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(&http.Client{
			Transport: &rewriteTransport{target: srv.URL},
			Timeout:   5 * time.Second,
		}),
	}

	_, err := resolver.ResolveDID(context.Background(), "did:web:alice.test")
	assert.ErrorIs(t, err, ErrDIDNotFound)
}

func TestDefaultResolver_ResolveDID_UnsupportedMethod(t *testing.T) {
	t.Parallel()
	resolver := &DefaultResolver{}
	_, err := resolver.ResolveDID(context.Background(), "did:unsupported:xyz")
	assert.ErrorIs(t, err, ErrUnsupportedDIDMethod)
}

// rewriteTransport rewrites all requests to point at the target test server.
type rewriteTransport struct {
	target string
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Rewrite the URL to our test server but preserve the path.
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.target, "http://")
	return http.DefaultTransport.RoundTrip(req)
}

func TestDefaultResolver_ResolveDID_PLCIDMismatch(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Returns a doc with a different DID than requested.
		_, _ = fmt.Fprint(w, `{"id":"did:plc:wrong","alsoKnownAs":[],"verificationMethod":[],"service":[]}`)
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(srv.Client()),
		PLCURL:     gt.Some(srv.URL),
	}

	_, err := resolver.ResolveDID(context.Background(), "did:plc:test123")
	assert.ErrorIs(t, err, ErrDIDNotFound)
}

func TestDefaultResolver_ResolveDID_NotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(srv.Client()),
		PLCURL:     gt.Some(srv.URL),
	}

	_, err := resolver.ResolveDID(context.Background(), "did:plc:nonexistent")
	assert.ErrorIs(t, err, ErrDIDNotFound)
}

// --- ResolveHandle race tests ---

func TestResolveHandle_HTTPWinsRace(t *testing.T) {
	t.Parallel()

	// HTTP responds immediately, DNS is slow. HTTP should win.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/atproto-did" {
			_, _ = fmt.Fprint(w, "did:plc:httpwins")
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(&http.Client{
			Transport: &rewriteTransport{target: srv.URL},
			Timeout:   5 * time.Second,
		}),
	}

	// DNS will fail (no real DNS for "alice.test"), HTTP will succeed.
	did, err := resolver.ResolveHandle(context.Background(), "alice.test")
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:httpwins"), did)
}

func TestResolveHandle_BothFail(t *testing.T) {
	t.Parallel()

	// HTTP returns 404, DNS will fail (no real DNS).
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(&http.Client{
			Transport: &rewriteTransport{target: srv.URL},
			Timeout:   5 * time.Second,
		}),
	}

	_, err := resolver.ResolveHandle(context.Background(), "nonexistent.test")
	assert.Error(t, err)
}

func TestResolveHandle_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Server that hangs forever.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	resolver := &DefaultResolver{
		HTTPClient: gt.Some(&http.Client{
			Transport: &rewriteTransport{target: srv.URL},
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := resolver.ResolveHandle(ctx, "slow.test")
	assert.Error(t, err)
}

// --- Fuzz test ---

func FuzzParseDIDDocument(f *testing.F) {
	f.Add([]byte(testDIDDocJSON))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"id":"did:plc:x","alsoKnownAs":["at://h.test"]}`))
	f.Add([]byte(`not json`))

	f.Fuzz(func(t *testing.T, data []byte) {
		doc, err := ParseDIDDocument(data)
		if err != nil {
			return
		}
		// Re-marshal and re-parse should not fail.
		b, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = ParseDIDDocument(b)
	})
}
