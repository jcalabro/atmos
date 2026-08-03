package backfill_test

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
	"github.com/jcalabro/atmos/backfill"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

type fleetPDS struct {
	hostname  string
	dids      []string
	repos     map[string][]byte
	server    *httptest.Server
	list      func(http.ResponseWriter, *http.Request) bool
	get       func(http.ResponseWriter, *http.Request) bool
	listCalls atomic.Int32
	getCalls  atomic.Int32
}

func startFleetPDS(t *testing.T, p *fleetPDS) {
	t.Helper()
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.listRepos":
			p.listCalls.Add(1)
			if p.list != nil && p.list(w, r) {
				return
			}
			page := listPage{}
			for _, did := range p.dids {
				page.Repos = append(page.Repos, listRepo{DID: did, Head: "head", Rev: "rev", Active: true})
			}
			_ = json.NewEncoder(w).Encode(page)
		case "/xrpc/com.atproto.sync.getRepo":
			p.getCalls.Add(1)
			if p.get != nil && p.get(w, r) {
				return
			}
			data, ok := p.repos[r.URL.Query().Get("did")]
			if !ok {
				http.Error(w, "missing", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.server.Close)
}

func fleetOptions(t *testing.T, store backfill.Store, hosts ...*fleetPDS) backfill.Options {
	t.Helper()
	byName := make(map[string]*atmossync.Client, len(hosts))
	for _, host := range hosts {
		startFleetPDS(t, host)
		byName[host.hostname] = atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{
			Host: host.server.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}})
	}
	var relayListRepos atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xrpc/com.atproto.sync.listRepos" {
			relayListRepos.Add(1)
			http.Error(w, "relay listRepos must not be used", http.StatusInternalServerError)
			return
		}
		require.Equal(t, "/xrpc/com.atproto.sync.listHosts", r.URL.Path)
		entries := make([]map[string]any, 0, len(hosts))
		for i, host := range hosts {
			entries = append(entries, map[string]any{"hostname": host.hostname, "status": "active", "accountCount": int64(len(host.dids))*10_000 + int64(len(hosts)-i)})
		}
		out := map[string]any{"hosts": entries}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(relay.Close)
	relayClient := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: relay.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}})
	return backfill.Options{
		Relay: relayClient, Store: store,
		NewHostClient: gt.Some(func(hostname string) (*atmossync.Client, error) {
			client, ok := byName[hostname]
			if !ok {
				return nil, fmt.Errorf("unknown test host %q", hostname)
			}
			return client, nil
		}),
		Handler:         backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { return nil }),
		HostBackoffBase: gt.Some(time.Millisecond), HostBackoffMax: gt.Some(time.Millisecond),
	}
}

func TestEngineFleet_RelayGapUsesDirectPDS(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	aDID, bDID := "did:plc:fleet-a", "did:plc:fleet-b"
	a := &fleetPDS{hostname: "a.example.test", dids: []string{aDID}, repos: map[string][]byte{aDID: buildTestRepoCAR(t, aDID, 1)}}
	b := &fleetPDS{hostname: "b.example.test", dids: []string{bDID}, repos: map[string][]byte{bDID: buildTestRepoCAR(t, bDID, 1)}}
	opts := fleetOptions(t, store, a, b)
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Equal(t, int32(2), store.completeCalls.Load())
	require.Equal(t, a.hostname, store.discoveryHosts[aDID])
	require.Equal(t, b.hostname, store.discoveryHosts[bDID])
	require.True(t, store.hostDrained[a.hostname])
	require.True(t, store.hostDrained[b.hostname])
}

func TestEngineFleet_DiscoverOnlyNeverDownloads(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	did := "did:plc:discover-only"
	host := &fleetPDS{hostname: "discover.example.test", dids: []string{did}, repos: map[string][]byte{did: buildTestRepoCAR(t, did, 1)}}
	opts := fleetOptions(t, store, host)
	opts.DiscoverOnly = gt.Some(true)
	var handled atomic.Int32
	opts.Handler = backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { handled.Add(1); return nil })
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Zero(t, host.getCalls.Load())
	require.Zero(t, handled.Load())
	require.Equal(t, backfill.StateDiscovered, store.state[did])
}

func TestEngineFleet_CrossHostDIDDeduplicated(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	did := "did:plc:migrating"
	car := buildTestRepoCAR(t, did, 1)
	a := &fleetPDS{hostname: "old.example.test", dids: []string{did}, repos: map[string][]byte{did: car}}
	b := &fleetPDS{hostname: "new.example.test", dids: []string{did}, repos: map[string][]byte{did: car}}
	opts := fleetOptions(t, store, a, b)
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Equal(t, int32(1), store.completeCalls.Load())
	require.Equal(t, int32(1), a.getCalls.Load()+b.getCalls.Load())
}

func TestEngineFleet_HostFailureRecoversAndPermanentFailureExhausts(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	recoverDID, healthyDID := "did:plc:recover", "did:plc:healthy"
	recovering := &fleetPDS{hostname: "recover.example.test", dids: []string{recoverDID}, repos: map[string][]byte{recoverDID: buildTestRepoCAR(t, recoverDID, 1)}}
	recovering.list = func(w http.ResponseWriter, _ *http.Request) bool {
		if recovering.listCalls.Load() == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return true
		}
		return false
	}
	dead := &fleetPDS{hostname: "dead.example.test", list: func(w http.ResponseWriter, _ *http.Request) bool {
		http.Error(w, "dead", http.StatusServiceUnavailable)
		return true
	}}
	healthy := &fleetPDS{hostname: "healthy.example.test", dids: []string{healthyDID}, repos: map[string][]byte{healthyDID: buildTestRepoCAR(t, healthyDID, 1)}}
	opts := fleetOptions(t, store, recovering, dead, healthy)
	opts.HostMaxAttempts = gt.Some(2)
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Equal(t, int32(2), store.completeCalls.Load())
	require.Equal(t, 2, store.hostExhausted[dead.hostname])
	require.True(t, store.hostDrained[recovering.hostname])
}

func TestEngineFleet_GlobalSlotReleasedDuringRetrySleep(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	aDID, bDID := "did:plc:rate-a", "did:plc:rate-b"
	aFirst := make(chan struct{})
	var aAttempts atomic.Int32
	a := &fleetPDS{hostname: "rate-a.example.test", dids: []string{aDID}, repos: map[string][]byte{aDID: buildTestRepoCAR(t, aDID, 1)}}
	a.get = func(w http.ResponseWriter, _ *http.Request) bool {
		if aAttempts.Add(1) == 1 {
			close(aFirst)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"RateLimitExceeded"}`))
			return true
		}
		return false
	}
	b := &fleetPDS{hostname: "rate-b.example.test", dids: []string{bDID}, repos: map[string][]byte{bDID: buildTestRepoCAR(t, bDID, 1)}}
	b.list = func(_ http.ResponseWriter, _ *http.Request) bool { <-aFirst; return false }
	opts := fleetOptions(t, store, a, b)
	opts.GlobalDownloads = gt.Some(1)
	opts.RetryBaseDelay = gt.Some(50 * time.Millisecond)
	opts.RetryMaxDelay = gt.Some(50 * time.Millisecond)
	var mu sync.Mutex
	var order []string
	opts.Handler = backfill.HandlerFunc(func(_ context.Context, did atmos.DID, _ *atmosrepo.Repo, _ *atmosrepo.Commit) error {
		mu.Lock()
		order = append(order, string(did))
		mu.Unlock()
		return nil
	})
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Equal(t, []string{bDID, aDID}, order)
}

func TestEngineFleet_ResumesPerHostCursor(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	hostname := "resume.example.test"
	store.hostCursors[hostname] = "cursor-7"
	did := "did:plc:resumed"
	host := &fleetPDS{hostname: hostname, dids: []string{did}, repos: map[string][]byte{did: buildTestRepoCAR(t, did, 1)}}
	host.list = func(_ http.ResponseWriter, r *http.Request) bool {
		require.Equal(t, "cursor-7", r.URL.Query().Get("cursor"))
		return false
	}
	opts := fleetOptions(t, store, host)
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Equal(t, int32(1), store.completeCalls.Load())
}

func TestEngineFleet_GlobalDownloadCapIsFleetWide(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	var active, peak atomic.Int32
	makeHost := func(name, prefix string) *fleetPDS {
		dids := []string{"did:plc:" + prefix + "0", "did:plc:" + prefix + "1", "did:plc:" + prefix + "2"}
		repos := make(map[string][]byte, len(dids))
		for _, did := range dids {
			repos[did] = buildTestRepoCAR(t, did, 1)
		}
		host := &fleetPDS{hostname: name, dids: dids, repos: repos}
		host.get = func(_ http.ResponseWriter, _ *http.Request) bool {
			n := active.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
			return false
		}
		return host
	}
	a := makeHost("slots-a.example.test", "slots-a")
	b := makeHost("slots-b.example.test", "slots-b")
	opts := fleetOptions(t, store, a, b)
	opts.GlobalDownloads = gt.Some(2)
	done := make(chan error, 1)
	go func() { done <- backfill.NewEngine(opts).Run(context.Background()) }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for two download attempts")
		}
	}
	select {
	case <-entered:
		t.Fatal("a third download entered while the two fleet slots were occupied")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int32(2), peak.Load())
}

func TestEngineFleet_MaxActiveHostsCapsListReposLoops(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	var active, peak atomic.Int32
	hosts := make([]*fleetPDS, 0, 5)
	for i := range 5 {
		host := &fleetPDS{hostname: fmt.Sprintf("active-%d.example.test", i)}
		host.list = func(_ http.ResponseWriter, _ *http.Request) bool {
			n := active.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
			return false
		}
		hosts = append(hosts, host)
	}
	opts := fleetOptions(t, store, hosts...)
	opts.MaxActiveHosts = gt.Some(2)
	done := make(chan error, 1)
	go func() { done <- backfill.NewEngine(opts).Run(context.Background()) }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for active hosts")
		}
	}
	select {
	case <-entered:
		t.Fatal("a third listRepos loop entered above MaxActiveHosts")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int32(2), peak.Load())
}

func TestEngineFleet_RetriesHostClientBuilder(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	did := "did:plc:builder-retry"
	host := &fleetPDS{hostname: "builder.example.test", dids: []string{did}, repos: map[string][]byte{did: buildTestRepoCAR(t, did, 1)}}
	opts := fleetOptions(t, store, host)
	client := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: host.server.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}})
	var builds atomic.Int32
	opts.NewHostClient = gt.Some(func(string) (*atmossync.Client, error) {
		if builds.Add(1) == 1 {
			return nil, fmt.Errorf("temporary builder failure")
		}
		return client, nil
	})
	opts.HostMaxAttempts = gt.Some(2)
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.Equal(t, int32(2), builds.Load())
	require.Equal(t, int32(1), store.completeCalls.Load())
}

func TestEngineFleet_FinalRelistDiscoversNewHost(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	aDID, bDID := "did:plc:relist-a", "did:plc:relist-b"
	a := &fleetPDS{hostname: "relist-a.example.test", dids: []string{aDID}, repos: map[string][]byte{aDID: buildTestRepoCAR(t, aDID, 1)}}
	b := &fleetPDS{hostname: "relist-b.example.test", dids: []string{bDID}, repos: map[string][]byte{bDID: buildTestRepoCAR(t, bDID, 1)}}
	startFleetPDS(t, a)
	startFleetPDS(t, b)
	clients := map[string]*atmossync.Client{}
	for _, host := range []*fleetPDS{a, b} {
		clients[host.hostname] = atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: host.server.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}})
	}
	var lists atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hosts := []map[string]any{{"hostname": a.hostname, "status": "active", "accountCount": 1}}
		if lists.Add(1) >= 2 {
			hosts = append(hosts, map[string]any{"hostname": b.hostname, "status": "active", "accountCount": 1})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hosts": hosts})
	}))
	t.Cleanup(relay.Close)
	opts := backfill.Options{
		Relay: atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: relay.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}}),
		Store: store, Handler: backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { return nil }),
		NewHostClient: gt.Some(func(hostname string) (*atmossync.Client, error) { return clients[hostname], nil }),
	}
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.GreaterOrEqual(t, lists.Load(), int32(3))
	require.Equal(t, int32(2), store.completeCalls.Load())
}

func TestEngineFleet_RetriesTransientListHostsFailure(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	did := "did:plc:list-hosts-retry"
	host := &fleetPDS{hostname: "list-hosts-retry.example.test", dids: []string{did}, repos: map[string][]byte{did: buildTestRepoCAR(t, did, 1)}}
	startFleetPDS(t, host)
	hostClient := atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: host.server.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}})
	var calls atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "temporary roster failure", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hosts": []map[string]any{{"hostname": host.hostname, "status": "active", "accountCount": 1}}})
	}))
	t.Cleanup(relay.Close)
	opts := backfill.Options{
		Relay: atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: relay.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}}),
		Store: store, Handler: backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { return nil }),
		NewHostClient:   gt.Some(func(string) (*atmossync.Client, error) { return hostClient, nil }),
		HostMaxAttempts: gt.Some(2), HostBackoffBase: gt.Some(time.Millisecond), HostBackoffMax: gt.Some(time.Millisecond),
	}
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	require.GreaterOrEqual(t, calls.Load(), int32(3), "initial failure, successful crawl, and final proof crawl")
	require.Equal(t, int32(1), store.completeCalls.Load())
}

func TestEngineFleet_RejectsInvalidBounds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		set  func(*backfill.Options)
		want string
	}{
		{"batch size zero", func(o *backfill.Options) { o.BatchSize = gt.Some(0) }, "BatchSize must be positive"},
		{"negative retries", func(o *backfill.Options) { o.MaxRetries = gt.Some(-1) }, "MaxRetries must be non-negative"},
		{"negative rate retries", func(o *backfill.Options) { o.RetryRateLimitMaxAttempts = gt.Some(-1) }, "RetryRateLimitMaxAttempts must be non-negative"},
		{"negative retry delay", func(o *backfill.Options) { o.RetryBaseDelay = gt.Some(-time.Second) }, "RetryBaseDelay must be non-negative"},
		{"negative host delay", func(o *backfill.Options) { o.HostBackoffMax = gt.Some(-time.Second) }, "HostBackoffMax must be non-negative"},
		{"negative timeout", func(o *backfill.Options) { o.DownloadTimeout = gt.Some(-time.Second) }, "DownloadTimeout must be non-negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := backfill.Options{Relay: &atmossync.Client{}, Store: newMemStore(), Handler: backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { return nil })}
			tt.set(&opts)
			require.ErrorContains(t, backfill.NewEngine(opts).Run(context.Background()), tt.want)
		})
	}
}
