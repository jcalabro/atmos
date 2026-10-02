package backfill_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/backfill"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func (s *memStore) requireContract(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Empty(t, s.contractErrs)
}

// pagedPDS serves listRepos from fixed pages; the cursor is the page index.
func pagedPDS(hostname string, pages [][]listRepo, repos map[string][]byte) *fleetPDS {
	p := &fleetPDS{hostname: hostname, repos: repos}
	p.list = func(w http.ResponseWriter, r *http.Request) bool {
		i := 0
		if c := r.URL.Query().Get("cursor"); c != "" {
			i, _ = strconv.Atoi(c)
		}
		out := listPage{Repos: []listRepo{}}
		if i < len(pages) {
			out.Repos = pages[i]
		}
		if i+1 < len(pages) {
			out.Cursor = strconv.Itoa(i + 1)
		}
		_ = json.NewEncoder(w).Encode(out)
		return true
	}
	return p
}

func repoEntry(did string, active bool) listRepo {
	return listRepo{DID: did, Head: "head", Rev: "rev", Active: active}
}

// TestStore_ReconcilesWholePages runs a mixed listing — unknown, complete,
// failed, and discovered DIDs, active flips both ways, inactive entries, and a
// DID repeated within a page with a different Active value — and pins both
// the batches the Store sees and the state the per-entry contract implies.
func TestStore_ReconcilesWholePages(t *testing.T) {
	t.Parallel()
	dids := []string{"did:plc:new1", "did:plc:new2", "did:plc:done", "did:plc:failed", "did:plc:flipoff", "did:plc:flipon", "did:plc:dead", "did:plc:dup", "did:plc:new3"}
	repos := make(map[string][]byte, len(dids))
	for _, did := range dids {
		repos[did] = buildTestRepoCAR(t, did, 1)
	}
	pages := [][]listRepo{
		{repoEntry("did:plc:new1", true), repoEntry("did:plc:done", true), repoEntry("did:plc:dup", true), repoEntry("did:plc:flipoff", false), repoEntry("did:plc:dup", false)},
		{repoEntry("did:plc:failed", true), repoEntry("did:plc:flipon", true), repoEntry("did:plc:dead", false)},
		{repoEntry("did:plc:new2", true), repoEntry("did:plc:new3", true)},
	}
	store := newMemStore()
	store.preset2("did:plc:done", backfill.StateComplete, true)
	store.preset2("did:plc:failed", backfill.StateFailed, true)
	store.preset2("did:plc:flipoff", backfill.StateComplete, true)
	store.preset2("did:plc:flipon", backfill.StateDiscovered, false)
	opts := fleetOptions(t, store, pagedPDS("paged.example.test", pages, repos))
	require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
	store.requireContract(t)

	store.mu.Lock()
	defer store.mu.Unlock()
	// The repeated DID splits page one into [new1 done dup flipoff] and
	// [dup]: the second sighting reconciles against the first one's
	// discovery and becomes an update.
	require.Equal(t, []int{4, 1, 3, 2}, store.lookupBatches)
	require.Equal(t, int32(5), store.discoverCalls.Load(), "new1, dup, dead, new2, new3")
	require.Equal(t, int32(3), store.updateCalls.Load(), "flipoff, dup's second sighting, flipon")
	require.False(t, store.updates["did:plc:dup"].Active)
	require.False(t, store.active["did:plc:flipoff"])
	require.True(t, store.active["did:plc:flipon"])
	require.False(t, store.entries["did:plc:dead"].Active)
	for _, did := range []string{"did:plc:new1", "did:plc:new2", "did:plc:new3", "did:plc:dup", "did:plc:failed", "did:plc:flipon", "did:plc:done", "did:plc:flipoff"} {
		require.Equal(t, backfill.StateComplete, store.state[did], did)
	}
	require.Equal(t, backfill.StateDiscovered, store.state["did:plc:dead"], "inactive DIDs are recorded, never downloaded")
	require.Equal(t, int32(6), store.completeCalls.Load(), "new1, dup, failed, flipon, new2, new3")
	require.NotContains(t, store.commits, "did:plc:done")
	require.NotContains(t, store.commits, "did:plc:flipoff")
}

// TestStore_ShortLookupFails proves a Lookup that answers for fewer DIDs than
// asked is a Store error, not a misaligned reconcile.
func TestStore_ShortLookupFails(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	store.shortLookup = true
	pages := [][]listRepo{{repoEntry("did:plc:a", true), repoEntry("did:plc:b", true)}}
	opts := fleetOptions(t, store, pagedPDS("short.example.test", pages, nil))
	err := backfill.NewEngine(opts).Run(context.Background())
	require.ErrorContains(t, err, "returned 1 entries for 2 dids")
	require.Zero(t, store.discoverCalls.Load())
}

// TestStore_HostRosterBatches covers listHosts batching: one OnHost per page,
// one HostCursor per page for the hosts not already terminal, drained and
// banned hosts recorded but not crawled, and the roster cap truncating a
// page mid-way.
func TestStore_HostRosterBatches(t *testing.T) {
	t.Parallel()
	listHosts := func(t *testing.T, hosts []map[string]any) *atmossync.Client {
		relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			out := map[string]any{"hosts": []map[string]any{}}
			// Two pages: the first host alone, then the rest.
			switch r.URL.Query().Get("cursor") {
			case "":
				out["hosts"], out["cursor"] = hosts[:1], "1"
			case "1":
				out["hosts"] = hosts[1:]
			}
			_ = json.NewEncoder(w).Encode(out)
		}))
		t.Cleanup(relay.Close)
		return atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: relay.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}})
	}
	newHost := func(name, did string) *fleetPDS {
		p := &fleetPDS{hostname: name, dids: []string{did}, repos: map[string][]byte{did: buildTestRepoCAR(t, did, 1)}}
		startFleetPDS(t, p)
		return p
	}

	t.Run("drained and banned", func(t *testing.T) {
		t.Parallel()
		live, drained, banned := newHost("live.example.test", "did:plc:live"), newHost("drained.example.test", "did:plc:drained"), newHost("banned.example.test", "did:plc:banned")
		byName := map[string]*fleetPDS{live.hostname: live, drained.hostname: drained, banned.hostname: banned}
		store := newMemStore()
		store.hostDrained[drained.hostname] = true
		opts := backfill.Options{
			Relay: listHosts(t, []map[string]any{
				{"hostname": live.hostname, "status": "active", "accountCount": 1},
				{"hostname": drained.hostname, "status": "active", "accountCount": 1},
				{"hostname": banned.hostname, "status": "banned", "accountCount": 1},
			}),
			Store:   store,
			Handler: backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { return nil }),
			NewHostClient: gt.Some(func(hostname string) (*atmossync.Client, error) {
				return atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: byName[hostname].server.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}}), nil
			}),
		}
		require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
		store.requireContract(t)
		require.Equal(t, int32(1), store.completeCalls.Load())
		require.Positive(t, live.listCalls.Load())
		require.Zero(t, drained.listCalls.Load(), "a host drained by a prior run must not be crawled")
		require.Zero(t, banned.listCalls.Load(), "a banned host must not be crawled")

		store.mu.Lock()
		defer store.mu.Unlock()
		require.Len(t, store.hostInfos, 3, "every listed host is recorded, crawled or not")
		require.Equal(t, "banned", store.hostInfos[banned.hostname].RelayStatus)
		// The crawl pass and the final proof pass each record two pages.
		require.Equal(t, []int{1, 2, 1, 2}, store.hostBatches)
		// The crawl pass asks for both pages' cursors, then the producer
		// asks for the crawled host's resume cursor. The proof pass asks
		// only for hosts not already terminal: the banned one.
		require.Equal(t, [][]string{
			{live.hostname},
			{drained.hostname, banned.hostname},
			{live.hostname},
			{banned.hostname},
		}, store.hostCursorBatches)
	})

	t.Run("roster cap mid-page", func(t *testing.T) {
		t.Parallel()
		store := newMemStore()
		var capped atomic.Int32
		hosts := make([]map[string]any, 0, 4)
		for i := range 4 {
			hosts = append(hosts, map[string]any{"hostname": fmt.Sprintf("h%d.example.test", i), "status": "active", "accountCount": 1})
		}
		opts := backfill.Options{
			Relay:          listHosts(t, hosts),
			Store:          store,
			Handler:        backfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error { return nil }),
			NewHostClient:  gt.Some(func(string) (*atmossync.Client, error) { return nil, fmt.Errorf("no crawl") }),
			MaxHosts:       gt.Some(2),
			OnRosterCapped: gt.Some(func(int) { capped.Add(1) }),
			// One attempt with no backoff keeps the unreachable hosts cheap.
			HostMaxAttempts: gt.Some(1),
		}
		require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
		store.requireContract(t)
		require.Positive(t, capped.Load())
		store.mu.Lock()
		defer store.mu.Unlock()
		require.Len(t, store.hostInfos, 2, "only the hosts under the cap are recorded")
		require.Contains(t, store.hostInfos, "h0.example.test")
		require.Contains(t, store.hostInfos, "h1.example.test")
	})
}

// TestStore_FleetSwarm runs seeded fleets in which DIDs repeat across hosts
// (migration windows) and within pages, with random page sizes, active flags,
// and prior state. Every unknown DID must be discovered exactly once, every
// dispatchable DID downloaded exactly once, and no batch may break the Store
// contract.
func TestStore_FleetSwarm(t *testing.T) {
	t.Parallel()
	for seed := range uint64(16) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, seed^0xba7c4))
			universe := make([]string, 40)
			repos := make(map[string][]byte, len(universe))
			for i := range universe {
				universe[i] = fmt.Sprintf("did:plc:swarm%d-%02d", seed, i)
				repos[universe[i]] = buildTestRepoCAR(t, universe[i], 1)
			}
			// A DID's Active value is fixed per run so no sighting's flip
			// races another's.
			active := make(map[string]bool, len(universe))
			for _, did := range universe {
				active[did] = rng.IntN(5) != 0
			}
			hosts := make([]*fleetPDS, 2+rng.IntN(5))
			listed := map[string]struct{}{}
			for h := range hosts {
				var pages [][]listRepo
				for range 1 + rng.IntN(4) {
					page := make([]listRepo, 1+rng.IntN(12))
					for i := range page {
						did := universe[rng.IntN(len(universe))]
						page[i] = repoEntry(did, active[did])
						listed[did] = struct{}{}
					}
					pages = append(pages, page)
				}
				hosts[h] = pagedPDS(fmt.Sprintf("swarm%d.example.test", h), pages, repos)
			}
			store := newMemStore()
			preset := map[string]backfill.State{}
			for _, did := range universe {
				switch rng.IntN(6) {
				case 0:
					preset[did] = backfill.StateComplete
				case 1:
					preset[did] = backfill.StateDiscovered
				}
				if st, ok := preset[did]; ok {
					store.preset2(did, st, !active[did])
				}
			}
			opts := fleetOptions(t, store, hosts...)
			opts.HostWorkers = gt.Some(1 + rng.IntN(4))
			require.NoError(t, backfill.NewEngine(opts).Run(context.Background()))
			store.requireContract(t)

			var unknownListed, wantDownloads int32
			for did := range listed {
				if _, ok := preset[did]; !ok {
					unknownListed++
				}
				if active[did] && preset[did] != backfill.StateComplete {
					wantDownloads++
				}
			}
			var gets int32
			for _, p := range hosts {
				gets += p.getCalls.Load()
			}
			require.Equal(t, unknownListed, store.discoverCalls.Load(), "each unknown DID is discovered exactly once")
			require.Equal(t, wantDownloads, store.completeCalls.Load(), "each dispatchable DID completes exactly once")
			require.Equal(t, wantDownloads, gets, "each dispatchable DID downloads exactly once")
			store.mu.Lock()
			defer store.mu.Unlock()
			for did := range listed {
				if preset[did] != backfill.StateComplete && slices.Contains(universe, did) {
					require.Equal(t, active[did], store.active[did], "%s's Active flag converges", did)
				}
			}
		})
	}
}
