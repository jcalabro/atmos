package identity

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/jcalabro/atmos"
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

	flights sync.Map // map[string]*inflight
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
	key := "did:" + string(did)

	return d.coalesce(ctx, key, func(ctx context.Context) (*Identity, error) {
		// Check cache.
		if d.Cache != nil {
			if id, ok := d.Cache.Get(ctx, key); ok {
				return identityCopy(id), nil
			}
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

		// Bi-directional verification: resolve declared handle back to DID.
		if id.Handle != atmos.HandleInvalid {
			resolvedDID, err := d.Resolver.ResolveHandle(ctx, id.Handle)
			if err != nil || resolvedDID != did {
				id.Handle = atmos.HandleInvalid
			}
		}

		d.cacheIdentity(ctx, id, "")
		return id, nil
	})
}

// Purge removes the cached identity for a DID, forcing the next lookup to
// re-resolve from the network. This is useful when a signing key rotation is
// detected (e.g. service auth signature verification fails).
func (d *Directory) Purge(ctx context.Context, did atmos.DID) {
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
	// Check for in-flight request.
	if val, ok := d.flights.Load(key); ok {
		f, ok := val.(*inflight)
		if !ok {
			return nil, fmt.Errorf("unexpected type in flights map")
		}
		select {
		case <-f.done:
			return identityCopy(f.id), f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f := &inflight{done: make(chan struct{})}
	actual, loaded := d.flights.LoadOrStore(key, f)
	if loaded {
		// Another goroutine won the race.
		f, ok := actual.(*inflight)
		if !ok {
			return nil, fmt.Errorf("unexpected type in flights map")
		}
		select {
		case <-f.done:
			return identityCopy(f.id), f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// We won — do the work.
	f.id, f.err = fn(ctx)
	close(f.done)
	d.flights.Delete(key)
	return f.id, f.err
}
