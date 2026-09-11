package declaration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/lexicon"
	"github.com/jcalabro/atmos/xrpc"
)

const (
	defaultPositiveTTL = time.Hour
	defaultNegativeTTL = 5 * time.Minute
	defaultMaxSchema   = 1 << 20
)

// RawResult is the untrusted response from a declaration source.
type RawResult struct {
	Schema []byte
	URI    string
	CID    string
}

// Resolver fetches declarations within one explicit trust configuration.
type Resolver interface {
	Resolve(ctx context.Context, nsid atmos.NSID) (*RawResult, error)
	CacheKey() string
}

// Resolved combines a validated declaration with its resolver provenance.
type Resolved struct {
	Declaration *Declaration
	URI         atmos.ATURI
	CID         string
}

// XRPCResolver resolves declarations through one caller-selected XRPC service.
type XRPCResolver struct {
	Client   *xrpc.Client
	TrustKey string
}

// CacheKey implements Resolver.
func (r *XRPCResolver) CacheKey() string {
	if r.TrustKey != "" {
		return r.TrustKey
	}
	if r.Client != nil {
		return r.Client.Host
	}
	return ""
}

// Resolve implements Resolver.
func (r *XRPCResolver) Resolve(ctx context.Context, nsid atmos.NSID) (*RawResult, error) {
	if r.Client == nil || r.Client.Host == "" {
		return nil, fmt.Errorf("space declaration: XRPC resolver client is not configured")
	}
	var output struct {
		CID    string          `json:"cid"`
		Schema json.RawMessage `json:"schema"`
		URI    string          `json:"uri"`
	}
	err := r.Client.Query(ctx, "com.atproto.lexicon.resolveLexicon", map[string]any{"nsid": string(nsid)}, &output)
	if err != nil {
		var xerr *xrpc.Error
		if errors.As(err, &xerr) && (xerr.StatusCode == 404 || xerr.Name == "LexiconNotFound") {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("space declaration: resolve %s: %w", nsid, err)
	}
	return &RawResult{Schema: append([]byte(nil), output.Schema...), URI: output.URI, CID: output.CID}, nil
}

type flight struct {
	done       chan struct{}
	result     *Resolved
	err        error
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	waiters    int
	complete   bool
}

type generationState struct {
	current uint64
	active  int
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// Directory resolves, validates, coalesces, and optionally caches declarations.
type Directory struct {
	Resolver    Resolver
	Cache       Cache
	PositiveTTL time.Duration
	NegativeTTL time.Duration
	MaxSchema   int

	mu          sync.Mutex
	flights     map[string]*flight
	generations map[string]*generationState
	cacheLocks  map[string]*keyedLock
}

// Resolve validates nsid and resolves its declaration.
func (d *Directory) Resolve(ctx context.Context, nsid atmos.NSID) (*Resolved, error) {
	if err := nsid.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.Resolver == nil {
		return nil, fmt.Errorf("space declaration: resolver and trust key are required")
	}
	trustKey := d.Resolver.CacheKey()
	if trustKey == "" {
		return nil, fmt.Errorf("space declaration: resolver and trust key are required")
	}
	key := trustKey + "\x00" + string(nsid)
	unlockKey := d.lockCacheKey(key)
	if cached, err := d.getCachedLocked(ctx, key); err != nil || cached != nil {
		unlockKey()
		return cached, err
	}

	d.mu.Lock()
	if d.flights == nil {
		d.flights = make(map[string]*flight)
	}
	if d.generations == nil {
		d.generations = make(map[string]*generationState)
	}
	state := d.generations[key]
	if state == nil {
		state = &generationState{}
		d.generations[key] = state
	}
	generation := state.current
	if current, ok := d.flights[key]; ok && current.generation == generation {
		current.waiters++
		d.mu.Unlock()
		unlockKey()
		return d.waitForFlight(ctx, key, current)
	}
	flightCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	current := &flight{
		done: make(chan struct{}), generation: generation,
		ctx: flightCtx, cancel: cancel, waiters: 1,
	}
	d.flights[key] = current
	state.active++
	d.mu.Unlock()
	unlockKey()

	go d.runFlight(key, nsid, current)
	return d.waitForFlight(ctx, key, current)
}

func (d *Directory) runFlight(key string, nsid atmos.NSID, current *flight) {
	result, err := d.resolve(current.ctx, key, nsid, current.generation)
	current.cancel()
	d.mu.Lock()
	current.result = result
	current.err = err
	if d.flights[key] == current {
		delete(d.flights, key)
	}
	state := d.generations[key]
	if state == nil || state.active < 1 {
		panic("space declaration: corrupt generation state")
	}
	state.active--
	if state.active == 0 {
		delete(d.generations, key)
	}
	current.complete = true
	close(current.done)
	d.mu.Unlock()
}

func (d *Directory) waitForFlight(ctx context.Context, key string, current *flight) (*Resolved, error) {
	defer d.releaseFlightWaiter(key, current)
	select {
	case <-current.done:
		return cloneResolved(current.result), current.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *Directory) releaseFlightWaiter(key string, current *flight) {
	unlock := d.lockCacheKey(key)
	defer unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if current.waiters < 1 {
		panic("space declaration: corrupt flight waiter state")
	}
	current.waiters--
	if current.waiters != 0 || current.complete {
		return
	}
	current.cancel()
	if d.flights[key] == current {
		delete(d.flights, key)
	}
	state := d.generations[key]
	if state == nil || state.active < 1 {
		panic("space declaration: corrupt abandoned flight state")
	}
	state.current++
}

func (d *Directory) resolve(ctx context.Context, key string, nsid atmos.NSID, generation uint64) (*Resolved, error) {
	// Recheck after winning the flight: another caller may have filled the cache.
	if cached, err := d.getCached(ctx, key); err != nil || cached != nil {
		return cached, err
	}
	raw, err := d.Resolver.Resolve(ctx, nsid)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if err := d.storeIfCurrent(ctx, key, CacheValue{NotFound: true}, d.negativeTTL(), generation); err != nil {
				return nil, err
			}
			return nil, ErrNotFound
		}
		return nil, err
	}
	if raw == nil {
		return nil, fmt.Errorf("space declaration: resolver returned nil result")
	}
	if len(raw.Schema) == 0 || len(raw.Schema) > d.maxSchema() {
		return nil, fmt.Errorf("space declaration: schema size %d outside 1..%d", len(raw.Schema), d.maxSchema())
	}
	schema, err := lexicon.Parse(raw.Schema)
	if err != nil {
		return nil, fmt.Errorf("space declaration: parse resolved schema: %w", err)
	}
	if schema.ID != string(nsid) {
		return nil, fmt.Errorf("space declaration: resolved id %q does not match %q", schema.ID, nsid)
	}
	declaration, err := Validate(schema)
	if err != nil {
		return nil, err
	}
	uri, err := atmos.ParseATURI(raw.URI)
	if err != nil {
		return nil, fmt.Errorf("space declaration: invalid provenance URI: %w", err)
	}
	if _, err := cbor.ParseCIDString(raw.CID); err != nil {
		return nil, fmt.Errorf("space declaration: invalid provenance CID: %w", err)
	}
	resolved := &Resolved{Declaration: declaration, URI: uri, CID: raw.CID}
	if err := d.storeIfCurrent(ctx, key, CacheValue{Resolved: resolved}, d.positiveTTL(), generation); err != nil {
		return nil, err
	}
	return resolved, nil
}

func (d *Directory) getCached(ctx context.Context, key string) (*Resolved, error) {
	if d.Cache == nil {
		return nil, nil
	}
	unlock := d.lockCacheKey(key)
	defer unlock()
	return d.getCachedLocked(ctx, key)
}

func (d *Directory) getCachedLocked(ctx context.Context, key string) (*Resolved, error) {
	if d.Cache == nil {
		return nil, nil
	}
	value, err := d.Cache.Get(ctx, key)
	if errors.Is(err, ErrCacheMiss) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("space declaration: cache get: %w", err)
	}
	if value.NotFound {
		return nil, ErrNotFound
	}
	if value.Resolved == nil {
		return nil, fmt.Errorf("space declaration: cache returned invalid empty entry")
	}
	return cloneResolved(value.Resolved), nil
}

func (d *Directory) storeIfCurrent(ctx context.Context, key string, value CacheValue, ttl time.Duration, generation uint64) error {
	if d.Cache == nil {
		return nil
	}
	unlock := d.lockCacheKey(key)
	defer unlock()
	d.mu.Lock()
	state := d.generations[key]
	current := state != nil && state.current == generation
	d.mu.Unlock()
	if !current {
		return nil
	}
	if err := d.Cache.Set(ctx, key, value, ttl); err != nil {
		return fmt.Errorf("space declaration: cache set: %w", err)
	}
	return nil
}

// Purge invalidates a cached declaration and prevents older in-flight work from repopulating it.
func (d *Directory) Purge(ctx context.Context, nsid atmos.NSID) error {
	if err := nsid.Validate(); err != nil {
		return err
	}
	if d.Resolver == nil {
		return fmt.Errorf("space declaration: resolver and trust key are required")
	}
	trustKey := d.Resolver.CacheKey()
	if trustKey == "" {
		return fmt.Errorf("space declaration: resolver and trust key are required")
	}
	key := trustKey + "\x00" + string(nsid)
	unlock := d.lockCacheKey(key)
	defer unlock()
	if d.Cache != nil {
		if err := d.Cache.Delete(ctx, key); err != nil {
			return fmt.Errorf("space declaration: cache delete: %w", err)
		}
	}
	d.mu.Lock()
	if state := d.generations[key]; state != nil {
		state.current++
	}
	d.mu.Unlock()
	return nil
}

func (d *Directory) lockCacheKey(key string) func() {
	d.mu.Lock()
	if d.cacheLocks == nil {
		d.cacheLocks = make(map[string]*keyedLock)
	}
	lock := d.cacheLocks[key]
	if lock == nil {
		lock = &keyedLock{}
		d.cacheLocks[key] = lock
	}
	lock.refs++
	d.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		d.mu.Lock()
		defer d.mu.Unlock()
		lock.refs--
		if lock.refs < 0 || d.cacheLocks[key] != lock {
			panic("space declaration: corrupt keyed lock state")
		}
		if lock.refs == 0 {
			delete(d.cacheLocks, key)
		}
	}
}

func (d *Directory) positiveTTL() time.Duration {
	if d.PositiveTTL > 0 {
		return d.PositiveTTL
	}
	return defaultPositiveTTL
}

func (d *Directory) negativeTTL() time.Duration {
	if d.NegativeTTL > 0 {
		return d.NegativeTTL
	}
	return defaultNegativeTTL
}

func (d *Directory) maxSchema() int {
	if d.MaxSchema > 0 {
		return d.MaxSchema
	}
	return defaultMaxSchema
}

func cloneResolved(in *Resolved) *Resolved {
	if in == nil {
		return nil
	}
	out := *in
	out.Declaration = cloneDeclaration(in.Declaration)
	return &out
}
