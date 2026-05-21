package identity

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/puzpuzpuz/xsync/v4"
)

// identityCopy returns a shallow copy of id so callers cannot mutate cached entries.
// Returns nil if id is nil.
func identityCopy(id *Identity) *Identity {
	if id == nil {
		return nil
	}
	cp := *id
	cp.Keys = make(map[string]Key, len(id.Keys))
	maps.Copy(cp.Keys, id.Keys)
	cp.Services = make(map[string]ServiceEndpoint, len(id.Services))
	maps.Copy(cp.Services, id.Services)
	return &cp
}

// inflight tracks an in-progress lookup for request coalescing.
type inflight struct {
	done chan struct{}
	id   *Identity
	err  error
}

// Directory resolves identities with bi-directional verification and optional caching.
type Directory struct {
	Resolver Resolver
	Cache    Cache // nil = no caching

	// SkipHandleVerification disables the bi-directional handle check
	// during [LookupDID]: the DID document is fetched and parsed, but
	// the declared handle is NOT resolved back to a DID. The returned
	// Identity's Handle is set to [atmos.HandleInvalid].
	//
	// On the firehose verify hot path the handle is irrelevant —
	// signature verification only needs the atproto signing key — and
	// the second resolution (DNS plus an HTTPS GET to the user's own
	// domain) is the dominant cost. Skipping it is the single biggest
	// throughput win for verifier consumers; mirrors indigo's
	// [identity.BaseDirectory.SkipHandleVerification].
	//
	// Leave false (default) for callers that need to know whether the
	// account currently controls its declared handle (e.g. AppViews
	// rendering @handles or auth flows). Set true for the firehose
	// verifier and any consumer that only needs the signing key.
	SkipHandleVerification bool

	// flights coalesces in-progress lookups so concurrent callers
	// requesting the same key share one resolver round-trip. Lazily
	// initialized so &Directory{Resolver: ...} struct literals stay
	// valid; flightMap returns the live map.
	flightsOnce sync.Once
	flights     *xsync.Map[string, *inflight]

	// Diagnostic counters. See debug.go.
	debug directoryDebug
}

// flightMap returns the lazily-initialized in-flight request coalesce
// map, constructing it on first access.
func (d *Directory) flightMap() *xsync.Map[string, *inflight] {
	d.flightsOnce.Do(func() {
		d.flights = xsync.NewMap[string, *inflight]()
	})
	return d.flights
}

// Lookup resolves an ATIdentifier (DID or handle) to a verified Identity.
func (d *Directory) Lookup(ctx context.Context, id atmos.ATIdentifier) (*Identity, error) {
	if id.IsDID() {
		did, err := id.AsDID()
		if err != nil {
			return nil, err
		}
		return d.LookupDID(ctx, did)
	}
	handle, err := id.AsHandle()
	if err != nil {
		return nil, err
	}
	return d.LookupHandle(ctx, handle)
}

// LookupHandle resolves a handle to a verified Identity.
func (d *Directory) LookupHandle(ctx context.Context, handle atmos.Handle) (*Identity, error) {
	handle = handle.Normalize()
	key := "handle:" + string(handle)

	return d.coalesce(ctx, key, func(ctx context.Context) (*Identity, error) {
		// Check cache.
		if d.Cache != nil {
			if id, ok := d.Cache.Get(ctx, key); ok {
				return identityCopy(id), nil
			}
		}

		// Resolve handle → DID.
		did, err := d.Resolver.ResolveHandle(ctx, handle)
		if err != nil {
			return nil, err
		}

		// Resolve DID → document.
		doc, err := d.Resolver.ResolveDID(ctx, did)
		if err != nil {
			return nil, err
		}

		id, err := IdentityFromDocument(doc)
		if err != nil {
			return nil, err
		}

		// Bi-directional verification: declared handle must match input.
		// Both sides are already normalized (lowercased).
		if id.Handle != handle {
			id.Handle = atmos.HandleInvalid
		}

		d.cacheIdentity(ctx, id, handle)
		return id, nil
	})
}

// LookupDID resolves a DID to a verified Identity.
func (d *Directory) LookupDID(ctx context.Context, did atmos.DID) (*Identity, error) {
	d.debug.lookupDIDCalls.Add(1)
	key := "did:" + string(did)

	// Fast-path cache hit: avoid the coalesce machinery entirely on the
	// common case. The coalesce closure still re-checks the cache
	// (covering the race where another goroutine populated it between
	// the check here and joining a flight).
	if d.Cache != nil {
		if id, ok := d.Cache.Get(ctx, key); ok {
			d.debug.lookupDIDCacheHits.Add(1)
			return identityCopy(id), nil
		}
	}

	// didLead is set true on the path where this goroutine actually ran
	// the resolver closure; if the coalesce returns without invoking the
	// closure (we joined an in-flight resolve), it stays false and we
	// account a coalesced hit.
	var didLead bool
	id, err := d.coalesce(ctx, key, func(ctx context.Context) (*Identity, error) {
		didLead = true

		// Race window: another goroutine may have populated the cache
		// between our pre-check and acquiring the inflight slot.
		if d.Cache != nil {
			if id, ok := d.Cache.Get(ctx, key); ok {
				d.debug.lookupDIDCacheHits.Add(1)
				return identityCopy(id), nil
			}
		}

		// Resolve DID → document.
		resolveStart := time.Now()
		doc, err := d.Resolver.ResolveDID(ctx, did)
		d.debug.addResolveNs(resolveStart)
		if err != nil {
			return nil, err
		}

		id, err := IdentityFromDocument(doc)
		if err != nil {
			return nil, err
		}

		// Bi-directional verification: resolve declared handle back to
		// DID. Skipped when SkipHandleVerification is set; in that
		// case any handle the doc declares is reported as
		// HandleInvalid because we have not confirmed it.
		if d.SkipHandleVerification {
			id.Handle = atmos.HandleInvalid
		} else if id.Handle != atmos.HandleInvalid {
			resolvedDID, err := d.Resolver.ResolveHandle(ctx, id.Handle)
			if err != nil || resolvedDID != did {
				id.Handle = atmos.HandleInvalid
			}
		}

		d.cacheIdentity(ctx, id, "")
		return id, nil
	})
	if !didLead {
		d.debug.lookupDIDCoalescedHits.Add(1)
	}
	return id, err
}

// Purge removes the cached identity for a DID, forcing the next lookup to
// re-resolve from the network. This is useful when a signing key rotation is
// detected (e.g. service auth signature verification fails).
func (d *Directory) Purge(ctx context.Context, did atmos.DID) {
	d.debug.purgeCalls.Add(1)
	if d.Cache == nil {
		return
	}
	d.Cache.Delete(ctx, "did:"+string(did))
}

func (d *Directory) cacheIdentity(ctx context.Context, id *Identity, handle atmos.Handle) {
	if d.Cache == nil {
		return
	}
	cp := identityCopy(id)
	d.Cache.Set(ctx, "did:"+string(cp.DID), cp)
	if cp.Handle != atmos.HandleInvalid {
		d.Cache.Set(ctx, "handle:"+string(cp.Handle), cp)
	}
	// Also cache by the input handle if different from declared handle.
	if handle != "" && handle != cp.Handle {
		d.Cache.Set(ctx, "handle:"+string(handle), cp)
	}
}

func (d *Directory) coalesce(ctx context.Context, key string, fn func(context.Context) (*Identity, error)) (*Identity, error) {
	flights := d.flightMap()

	// Check for in-flight request.
	if f, ok := flights.Load(key); ok {
		select {
		case <-f.done:
			return identityCopy(f.id), f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f := &inflight{done: make(chan struct{})}
	if actual, loaded := flights.LoadOrStore(key, f); loaded {
		// Another goroutine won the race.
		select {
		case <-actual.done:
			return identityCopy(actual.id), actual.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// We won — do the work.
	f.id, f.err = fn(ctx)
	close(f.done)
	flights.Delete(key)
	return f.id, f.err
}
