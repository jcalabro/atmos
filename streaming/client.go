package streaming

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming/parallel"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

const defaultMaxMessageSize = 2 * 1024 * 1024 // 2 MiB

// Conn is the WebSocket surface the client consumes. *websocket.Conn
// satisfies it; tests inject an in-memory implementation via Options.Dial.
type Conn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Close(code websocket.StatusCode, reason string) error
	CloseNow() error
	SetReadLimit(n int64)
}

// DialFunc opens a connection to the resolved WebSocket URL (cursor and
// query already appended). resp is the upgrade HTTP response, used to
// classify a non-101 status as a non-retryable DialError; it may be nil.
type DialFunc func(ctx context.Context, url string) (Conn, *http.Response, error)

// Options configures a streaming client.
type Options struct {
	// URL is the full WebSocket URL. The decoder is auto-detected based
	// on the HTTP path. Examples of such URLs:
	//
	//   Firehose:  "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	//   Jetstream: "wss://jetstream1.us-east.bsky.network/subscribe"
	//   Labeler:   "wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels"
	//
	// This field is required.
	URL string

	// Cursor is the initial sequence number to resume from. None means start
	// from live. If both Cursor and CursorStore are set, Cursor wins.
	Cursor gt.Option[int64]

	// CursorStore enables durable cursor persistence for crash recovery.
	// If set and Cursor is not, the cursor is loaded from the store on
	// NewClient. Cursors are persisted every CursorInterval events and on
	// Close.
	CursorStore gt.Option[CursorStore]

	// CursorInterval controls how often the cursor is persisted (every N
	// events). None means 100. Only used when CursorStore is set.
	CursorInterval gt.Option[int64]

	// BatchSize controls how many events are accumulated before yielding
	// a batch to the caller. None means 50.
	BatchSize gt.Option[int]

	// BatchTimeout is the maximum time to wait for a full batch before
	// flushing whatever has accumulated. None means 500ms.
	BatchTimeout gt.Option[time.Duration]

	// Backoff controls reconnection timing. None uses sensible defaults
	// (1s initial, 30s max, full jitter).
	Backoff gt.Option[BackoffPolicy]

	// MaxMessageSize limits WebSocket message size. None means 2MB.
	MaxMessageSize gt.Option[int64]

	// OnReconnect is called each time a reconnection attempt begins, with the
	// attempt number and delay. Useful for logging. None means no callback.
	OnReconnect gt.Option[func(attempt int, delay time.Duration)]

	// Locker enables distributed lock coordination for high availability
	// deployments. When set, only the lock holder consumes events from the
	// stream. Other nodes wait idle and attempt to acquire the lock
	// periodically. None uses a noop lock (always leader, suitable for
	// single-node deployments).
	Locker gt.Option[DistributedLockerOptions]

	// Collections filters a Jetstream subscription to these collection
	// NSIDs. Ignored for firehose/label streams. Optional.
	Collections gt.Option[[]string]

	// DIDs filters a Jetstream subscription to these DIDs. Ignored for
	// firehose/label streams. Optional.
	DIDs gt.Option[[]string]

	// SyncClient overrides the sync client used for automatic #sync
	// re-fetching. By default, a sync client is auto-created from
	// the WebSocket URL (the relay typically 302-redirects getRepo
	// requests to the account's PDS). To disable automatic resync,
	// set DisableAutoResync. This option is only used for repo
	// consumption, not labels.
	SyncClient gt.Option[*sync.Client]

	// Verifier, when set, runs Sync 1.1 verification on every #commit
	// and #sync event before they reach the consumer's Operations()
	// iterator, AND feeds every #account event into the verifier's
	// hosting-state tracker (so HostingPolicy=HostingGate can drop
	// commits for takendown DIDs). None means no verification — events
	// flow through unchanged (existing behavior).
	//
	// Only relevant for firehose streams.
	//
	// Pass gt.Some[*sync.Verifier](nil) to disable
	Verifier gt.Option[*sync.Verifier]

	// StrictValidation makes [Event.Operations] validate each op's
	// typed fields (NSID, RecordKey, DID, TID) against the
	// corresponding [atmos] syntax types before yielding. None or
	// false (the default) is the relaxed behavior: typed fields
	// carry whatever value the wire produced, and consumers that
	// care about strict syntax call [atmos.NSID.Validate] etc.
	// themselves. Set to gt.Some(true) to make the iterator yield
	// (Operation{}, error) for any op whose fields don't parse.
	StrictValidation gt.Option[bool]

	// Parallelism is the number of workers in the per-DID FIFO scheduler
	// that runs verification (and decoded-event dispatch when no verifier
	// is configured). Default 32. Set to 1 to preserve strict global seq
	// ordering across DIDs at the cost of throughput.
	//
	// All values share a single readLoop implementation; Parallelism = 1
	// is the same code path as Parallelism > 1 with one worker and an
	// unbounded per-key queue.
	//
	// With Parallelism > 1:
	//   - Events for the same DID are delivered in seq order.
	//   - Events for different DIDs may interleave; a single yielded
	//     batch can contain seqs in completion order, not seq order.
	//   - Cursor checkpoints advance on a watermark equal to (smallest
	//     in-flight seq - 1) so on-restart no event is skipped.
	//   - Global GapError detection still fires (the dispatch goroutine
	//     reads frames single-threaded, so the relay's monotonic seq is
	//     observable before scheduler dispatch).
	//   - Per-DID queue overflow surfaces as *DropError; under sustained
	//     loss faster than the consumer drains, additional drops are
	//     coalesced via DropError.AdditionalDropsSuppressed rather than
	//     blocking the dispatch goroutine.
	//
	// With Parallelism = 1:
	//   - Strict cross-DID seq ordering: events yield in the order the
	//     relay sent them.
	//   - The per-key queue is unbounded, so DropError is unreachable.
	//     A stalled worker pushes back through the bounded msgCh and
	//     websocket OS buffer rather than shedding events.
	//   - All other guarantees from Parallelism > 1 still hold.
	Parallelism gt.Option[int]

	// Dial, when set, replaces the default websocket dial. The client
	// uses the returned Conn for all transport. Intended for tests that
	// drive the client over an in-memory connection.
	Dial gt.Option[DialFunc]
}

// Client connects to an ATProto event stream (firehose or label stream).
type Client struct {
	opts           Options
	cursor         atomic.Int64
	conn           atomic.Pointer[Conn]
	cursorCount    int64 // only used in readLoop goroutine
	cursorInterval int64
	batchSize      int
	batchTimeout   time.Duration
	parallelism    int
	decode         func([]byte) (Event, error)
	syncClient     *sync.Client // nil disables automatic #sync resync
	isJetstream    bool

	// ownsVerifier is true if NewClient auto-attached the default
	// verifier (i.e. the caller didn't supply one). Close() shuts down
	// the verifier only in that case; user-supplied verifiers are the
	// user's responsibility to close.
	ownsVerifier bool

	// Lock-related fields, initialized in NewClient.
	lock                DistributedLocker
	lockOpts            *DistributedLockerOptions // nil when no lock configured
	leaseDuration       time.Duration
	renewalInterval     time.Duration
	acquisitionInterval time.Duration
	isLeader            atomic.Bool
	lockSleep           func(ctx context.Context, d time.Duration) error // overridable for testing
}

// NewClient creates a new streaming client. Does not connect until Events is
// called. If CursorStore is set and Cursor is not, the cursor is loaded from
// the store. Returns an error if loading fails.
func NewClient(opts Options) (*Client, error) {
	if !opts.MaxMessageSize.HasVal() {
		opts.MaxMessageSize = gt.Some(int64(defaultMaxMessageSize))
	}
	if !opts.Backoff.HasVal() {
		opts.Backoff = gt.Some(defaultBackoff)
	}

	interval := opts.CursorInterval.ValOr(100)
	batchSize := opts.BatchSize.ValOr(50)
	if batchSize < 1 {
		return nil, errors.New("BatchSize must be >= 1")
	}
	batchTimeout := opts.BatchTimeout.ValOr(500 * time.Millisecond)
	if batchTimeout <= 0 {
		return nil, errors.New("BatchTimeout must be > 0")
	}
	parallelism := opts.Parallelism.ValOr(32)
	if parallelism < 1 {
		return nil, errors.New("parallelism must be >= 1")
	}

	lk := DistributedLocker(NoopLock{})
	var lockOpts *DistributedLockerOptions
	leaseDur := defaultLeaseDuration
	renewInt := defaultRenewalInterval
	acqInt := defaultAcquisitionInterval

	if opts.Locker.HasVal() {
		lo := opts.Locker.Val()
		if lo.Locker == nil {
			return nil, errors.New("DistributedLockerOptions.Locker must not be nil")
		}
		lockOpts = &lo
		lk = lo.Locker
		if lo.LeaseDuration > 0 {
			leaseDur = lo.LeaseDuration
		}
		if lo.RenewalInterval > 0 {
			renewInt = lo.RenewalInterval
		}
		if lo.AcquisitionInterval > 0 {
			acqInt = lo.AcquisitionInterval
		}
	}

	decode := decodeFrame
	isJS := false
	isLabels := isSubscribeLabels(opts.URL)
	if isLabels {
		decode = decodeLabelFrame
	} else if isJetstreamURL(opts.URL) {
		decode = decodeJetstreamFrame
		isJS = true
	}

	ownsVerifier := false

	// Resolve the sync client for automatic #sync resync.
	// Jetstream and label streams don't need a sync client.
	var sc *sync.Client
	switch {
	case opts.SyncClient.HasVal():
		if !opts.SyncClient.Val().DisableAutoResync() {
			sc = opts.SyncClient.Val()
		}
	case !isJS && !isLabels:
		// Auto-create from the WebSocket URL for repo streams.
		httpURL, err := deriveHTTPURL(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("derive HTTP URL for sync client: %w", err)
		}

		sc = sync.NewClient(sync.Options{
			Client: &xrpc.Client{
				Host:       httpURL,
				HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
			},
		})

		if opts.Verifier.HasVal() {
			if opts.Verifier.Val() == nil {
				opts.Verifier = gt.None[*sync.Verifier]() // disable
			}
		} else {
			// Auto-set a sync 1.1 verifier if none is provided and sync 1.1
			// is not explicitly disabled
			v, err := sync.NewVerifier(sync.VerifierOptions{
				Directory:  identity.NewInMemoryDirectory(),
				StateStore: sync.NewMemStateStore(),
				SyncClient: gt.Some(sc),
			})
			if err != nil {
				return nil, fmt.Errorf("create default verifier: %w", err)
			}

			opts.Verifier = gt.Some(v)
			ownsVerifier = true
		}
	}

	c := &Client{
		opts:                opts,
		cursorInterval:      interval,
		batchSize:           batchSize,
		batchTimeout:        batchTimeout,
		parallelism:         parallelism,
		decode:              decode,
		syncClient:          sc,
		isJetstream:         isJS,
		ownsVerifier:        ownsVerifier,
		lock:                lk,
		lockOpts:            lockOpts,
		leaseDuration:       leaseDur,
		renewalInterval:     renewInt,
		acquisitionInterval: acqInt,
	}

	c.lockSleep = sleep

	// When no lock is configured, the client is always the leader.
	if !opts.Locker.HasVal() {
		c.isLeader.Store(true)
	}

	if opts.Cursor.HasVal() {
		c.cursor.Store(opts.Cursor.Val())
	} else if opts.CursorStore.HasVal() {
		cur, err := opts.CursorStore.Val().LoadCursor(context.Background())
		if err != nil {
			return nil, fmt.Errorf("load cursor: %w", err)
		}
		if cur > 0 {
			c.cursor.Store(cur)
		}
	}

	return c, nil
}

func isSubscribeLabels(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	// EqualFold because even though it's invalid, certain web frameworks
	// that shall remain unnamed treat HTTP paths as case insensitive
	// by default.
	return strings.EqualFold(parsed.Path, "/xrpc/com.atproto.label.subscribeLabels")
}

func isJetstreamURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	return strings.EqualFold(parsed.Path, "/subscribe")
}

// Cursor returns the sequence number of the last successfully yielded event.
// Safe to call concurrently.
func (c *Client) Cursor() int64 {
	return c.cursor.Load()
}

// Close gracefully shuts down the WebSocket connection. If a CursorStore is
// configured, the current cursor is persisted (best-effort). If the client
// auto-attached a verifier (no Verifier option supplied to NewClient), the
// verifier is closed too — its worker pool stops and ResyncEvents /
// AsyncErrors channels are drained. User-supplied verifiers are the
// caller's responsibility.
//
// Close does not release the distributed lock — the lock is released when
// the [Events] iterator terminates (via context cancellation or the caller
// breaking out of the range loop).
func (c *Client) Close() error {
	if c.opts.CursorStore.HasVal() {
		cur := c.cursor.Load()
		if cur > 0 {
			// Best-effort: use background context so we don't fail on
			// cancelled parent contexts.
			_ = c.opts.CursorStore.Val().SaveCursor(context.Background(), cur)
		}
	}

	if c.ownsVerifier && c.opts.Verifier.HasVal() {
		// Best-effort: verifier Close() is documented to be idempotent
		// and never returns an error from in-flight workers.
		_ = c.opts.Verifier.Val().Close()
	}

	conn := c.conn.Load()
	if conn == nil {
		return nil
	}

	return (*conn).Close(websocket.StatusNormalClosure, "client closing")
}

// IsLeader returns whether this node currently believes it holds the
// distributed lock. Always returns true when no lock is configured.
// Safe to call concurrently.
func (c *Client) IsLeader() bool {
	return c.isLeader.Load()
}

// Events returns an iterator over stream events (repository or label). It
// connects to the WebSocket, automatically handles ping/pong, reconnects with
// backoff on connection loss, and resumes from the last seen cursor.
//
// When a distributed lock is configured via [DistributedLockerOptions], the
// iterator first acquires the lock before consuming events. If the lock is
// lost, consumption pauses and resumes once the lock is reacquired, picking
// up from the last cursor. Events are delivered at least once; in rare cases
// during leader failover the same event may be emitted more than once.
// Consumers must handle events idempotently.
//
// The iterator yields events until the context is cancelled. Cancel the
// context to stop; [Client.Close] only closes the current WebSocket but
// does not stop the iterator.
//
// Events must not be called concurrently from multiple goroutines.
func (c *Client) Events(ctx context.Context) iter.Seq2[[]Event, error] {
	return func(yield func([]Event, error) bool) {
		defer c.releaseOnShutdown()

		for {
			if ctx.Err() != nil {
				return
			}

			// Acquire the distributed lock before consuming.
			if err := c.waitForLock(ctx); err != nil {
				return // context cancelled
			}

			// Create an inner context that is cancelled when the lock is
			// lost, stopping event consumption promptly.
			innerCtx, innerCancel := context.WithCancel(ctx)

			// Renew the lock in the background. If renewal fails, innerCancel
			// is called to stop the consume loop. Skipped for NoopLock
			// (single-node) since there is nothing to renew.
			renewDone := make(chan struct{})
			if c.lockOpts != nil {
				go c.renewLoop(innerCtx, innerCancel, renewDone)
			} else {
				close(renewDone)
			}

			// Consume events while holding the lock.
			yieldStopped := c.consumeLoop(innerCtx, yield)

			innerCancel()
			<-renewDone

			if yieldStopped {
				return // caller stopped iteration
			}

			// Either the lock was lost (innerCtx cancelled by renewLoop) or
			// the parent ctx was cancelled. The loop check at the top handles
			// parent cancellation; otherwise we try to reacquire.
		}
	}
}

// consumeLoop connects to the WebSocket and reads events, reconnecting with
// backoff on connection loss. Returns true only when the caller wants to stop
// iterating. Returns false when the context is cancelled or connection errors occur.
func (c *Client) consumeLoop(ctx context.Context, yield func([]Event, error) bool) bool {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return false
		}

		conn, err := c.dial(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}

			// Non-retryable dial errors (e.g. wrong URL, non-WebSocket
			// endpoint) are yielded to the caller.
			if err, ok := errors.AsType[*DialError](err); ok {
				return !yield(nil, err)
			}

			// Dial failed — backoff and retry.
			attempt = c.backoffAndRetry(ctx, attempt)
			continue
		}

		// Reset backoff after successful connection.
		attempt = 0

		// Read loop — process messages until error.
		yieldStopped := c.readLoop(ctx, conn, yield)
		_ = conn.CloseNow()
		if yieldStopped {
			return true
		}

		if ctx.Err() != nil {
			return false
		}

		// Connection lost — reconnect with backoff.
		attempt = c.backoffAndRetry(ctx, attempt)
	}
}

// waitForLock polls the distributed lock until it is acquired or the context
// is cancelled. With [NoopLock] this returns immediately.
func (c *Client) waitForLock(ctx context.Context) error {
	for {
		err := c.lock.Acquire(ctx, c.leaseDuration)
		if err == nil {
			c.isLeader.Store(true)
			if c.lockOpts != nil && c.lockOpts.OnBecameLeader != nil {
				c.lockOpts.OnBecameLeader()
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Lock held by another node or infra error — wait and retry.
		if sleepErr := c.lockSleep(ctx, c.acquisitionInterval); sleepErr != nil {
			return sleepErr
		}
	}
}

// renewLoop renews the distributed lock periodically. If renewal fails
// (because we lost the lock), it calls cancel to stop event consumption.
// Always closes done when it returns.
func (c *Client) renewLoop(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer close(done)
	for {
		if err := c.lockSleep(ctx, c.renewalInterval); err != nil {
			return // context cancelled
		}
		if err := c.lock.Renew(ctx, c.leaseDuration); err != nil {
			if ctx.Err() != nil {
				return // context cancelled during Renew; let shutdown handle it
			}
			// Genuine lock loss.
			c.isLeader.Store(false)
			if c.lockOpts != nil && c.lockOpts.OnLostLeadership != nil {
				c.lockOpts.OnLostLeadership()
			}
			cancel()
			return
		}
	}
}

// releaseOnShutdown attempts to release the distributed lock if this node
// is the current leader. Uses a background context with timeout so it works
// even when the parent context is already cancelled. Safe to call multiple
// times (idempotent via atomic compare-and-swap).
func (c *Client) releaseOnShutdown() {
	if !c.isLeader.CompareAndSwap(true, false) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = c.lock.Release(ctx)

	if c.lockOpts != nil && c.lockOpts.OnLostLeadership != nil {
		c.lockOpts.OnLostLeadership()
	}
}

// dial connects to the WebSocket endpoint with the current cursor.
func (c *Client) dial(ctx context.Context) (Conn, error) {
	u := c.opts.URL
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}
	q := parsed.Query()

	cur := c.cursor.Load()
	if cur > 0 {
		q.Set("cursor", fmt.Sprintf("%d", cur))
	}

	if c.isJetstream {
		if c.opts.Collections.HasVal() {
			q.Set("wantedCollections", strings.Join(c.opts.Collections.Val(), ","))
		}
		if c.opts.DIDs.HasVal() {
			q.Set("wantedDids", strings.Join(c.opts.DIDs.Val(), ","))
		}
	}

	if len(q) > 0 {
		parsed.RawQuery = q.Encode()
		u = parsed.String()
	}

	dialFn := dial
	if c.opts.Dial.HasVal() {
		dialFn = c.opts.Dial.Val()
	}
	conn, resp, err := dialFn(ctx, u)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		// If the server returned an HTTP response but didn't upgrade to
		// WebSocket (e.g. 200 "Welcome to Jetstream", 404 Not Found),
		// wrap it as a non-retryable DialError so consumeLoop surfaces
		// it to the caller instead of retrying forever.
		if resp != nil && resp.StatusCode != 101 {
			return nil, &DialError{StatusCode: resp.StatusCode, Err: err}
		}
		return nil, err
	}

	conn.SetReadLimit(c.opts.MaxMessageSize.Val())
	c.conn.Store(&conn)
	return conn, nil
}

// readResult is a raw WebSocket message read by the reader goroutine.
type readResult struct {
	data []byte
	err  error
}

// readLoop runs the firehose with per-event work (verification and
// dispatch) handled by a per-DID FIFO worker pool of c.parallelism
// goroutines. Events for the same DID are always strictly ordered;
// events for different DIDs may complete out of seq order at
// parallelism > 1. Cursor advances on a watermark equal to the
// smallest in-flight seq minus 1; a global seq tracker drives
// GapError.
//
// At parallelism = 1 the per-key queue is unbounded (keyQueueCap = 0),
// preserving backpressure semantics: if the lone worker stalls the
// dispatch goroutine grows the per-key queue rather than displacing
// work, and the bounded msgCh + websocket buffer eventually push back
// on the relay. As a corollary, *DropError is unreachable at
// parallelism = 1. At parallelism > 1, keyQueueCap = parallelism * 2
// and per-DID queue overflow surfaces as *DropError.
//
// Returns true when the caller's yield function asked to stop
// iterating; false on connection errors or context cancellation
// (caller should reconnect or exit).
func (c *Client) readLoop(ctx context.Context, conn Conn, yield func([]Event, error) bool) bool {
	// schedJob carries one decoded event through the scheduler.
	type schedJob struct {
		evt Event
	}

	// Result channel shared by all workers; the dispatch goroutine
	// drains it and selects on the multiple channels listed below.
	resultCh := make(chan verifyResult, 4096) // headroom for typical concurrent completions before backpressure on workers

	// asyncErr fires drop notifications. Buffered so onDrop never
	// blocks the scheduler.
	asyncErr := make(chan error, 256) // enough for typical drop-rate spikes; overflow drops the drop notification

	// At parallelism = 1 we deliberately disable per-key drop-oldest
	// (keyQueueCap = 0) so the strict-order escape hatch preserves
	// backpressure: a stalled lone worker grows the per-key queue, the
	// dispatch goroutine eventually blocks on the bounded msgCh, and
	// the websocket reader fills its OS buffer until the relay pushes
	// back. DropError is therefore never emitted at parallelism = 1.
	// At parallelism > 1, we cap the per-key queue at parallelism * 2;
	// further arrivals for a key with a full queue surface as DropError
	// via onDrop.
	queueCap := 0
	if c.parallelism > 1 {
		queueCap = c.parallelism * 2
	}

	// inflight tracks seqs dispatched to the scheduler that have not
	// yet produced a verifyResult. Drives the watermark cursor (cursor
	// = min(inflight) - 1) and the connection-close drain. Owned by
	// the dispatch goroutine; the scheduler's onDrop callback also
	// mutates it, but onDrop is invoked synchronously on the dispatch
	// goroutine inside AddWork, so no locking is required.
	var inflight inflightSeqs
	// suppressedDrops tracks DropError notifications that couldn't fit
	// in the asyncErr buffer; the next successful send rolls the
	// accumulated count into AdditionalDropsSuppressed so the consumer
	// can reconcile total loss. Owned by the dispatch goroutine.
	var suppressedDrops uint64
	// pendingResults tracks all jobs dispatched to the scheduler, including
	// seq-less control frames such as #info and server-side error frames.
	// inflight only tracks seq-bearing events for cursor watermarking; drains
	// must use pendingResults so a connection close cannot race ahead of a
	// seq-less scheduler result and drop it.
	var pendingResults int
	// inflightByDID counts events dispatched to the scheduler but not
	// yet collected, per DID. Owned by this goroutine (dispatch,
	// onDrop, and collection all run here). The timer sweep skips DIDs
	// with in-flight events: a result already queued for the DID could
	// predate the resync's registration, and emitting the resync first
	// would invert per-DID order. Such DIDs deliver their resyncs via
	// verifyOne's claim, or via a later sweep tick once collected.
	inflightByDID := make(map[string]int)
	noteCollected := func(evt *Event) {
		did := evt.repoOf()
		if did == "" {
			return
		}
		if n := inflightByDID[did] - 1; n > 0 {
			inflightByDID[did] = n
		} else {
			delete(inflightByDID, did)
		}
	}
	// Seed lastSeenSeq from the persisted cursor so global gap
	// detection survives reconnects. Without this, every reconnect
	// resets to 0 and the first frame trivially passes the
	// `lastSeenSeq > 0` guard, masking gaps when the relay's outbox
	// window has advanced past our cursor.
	lastSeenSeq := c.cursor.Load()

	// workCtx runs the scheduler workers (verification) and their result
	// delivery. It is derived from ctx WITHOUT inheriting cancellation, so a
	// graceful consumer-ctx cancel at a phase cutover does not abandon a
	// verification that is already in flight: the verifier advances its durable
	// per-DID rev as a side effect of VerifyCommit, so a result computed but
	// then dropped would be lost forever (the re-subscribe re-delivers the
	// event, but the now-advanced verifier rev-replay-drops it). On consumer
	// cancel we instead drain those in-flight results (bounded — see the
	// ctx.Done() branch below) and THEN cancelWork to unblock any verifier
	// network I/O. cancelWork on every readLoop exit prevents a worker leak.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))

	sched := parallel.NewSchedulerWithContext(
		workCtx,
		c.parallelism,
		queueCap,
		func(jctx context.Context, j schedJob) error {
			// jctx is workCtx; it is cancelled when readLoop returns (or when
			// the bounded post-cancel drain elapses), which propagates into
			// VerifyCommit/VerifySync, OnAccountEvent, and the PLC/CAR network
			// calls they make, so shutdown still unblocks blocking verifier I/O.
			res := c.verifyOne(jctx, j.evt)
			select {
			case resultCh <- res:
			case <-workCtx.Done():
			}
			return nil
		},
		func(j schedJob) {
			seq := j.evt.seqOf()
			did := j.evt.repoOf()
			// Release the dropped seq from the watermark cursor's
			// inflight set: no worker will ever produce a verifyResult
			// for this work, so without this the watermark stays pinned
			// to seq forever (cursor freezes; drainResults hangs on
			// connection close because inflight.Len() never reaches
			// zero).
			//
			// Safe to mutate inflight here without locking: AddWork
			// invokes onDrop synchronously on the caller's goroutine,
			// and the only caller of AddWork is the dispatch goroutine
			// — the same goroutine that owns inflight.
			if seq > 0 {
				inflight.Remove(seq)
			}
			if did != "" {
				if n := inflightByDID[did] - 1; n > 0 {
					inflightByDID[did] = n
				} else {
					delete(inflightByDID, did)
				}
			}
			pendingResults--
			// suppressedDrops accumulates drop notifications that
			// couldn't fit into the asyncErr buffer. When the next
			// notification *does* land, it carries the suppressed count
			// so consumers can reconcile total loss without us blocking
			// the dispatch goroutine on a slow consumer. Owned by the
			// dispatch goroutine (onDrop runs synchronously inside
			// AddWork, on the dispatch goroutine).
			err := &DropError{
				DID:                       did,
				Seq:                       seq,
				QueueLen:                  queueCap,
				AdditionalDropsSuppressed: suppressedDrops,
			}
			select {
			case asyncErr <- err:
				// Successfully sent — reset the suppressed counter
				// since we've now accounted for everything up to this
				// drop.
				suppressedDrops = 0
			default:
				// asyncErr full: track the loss instead of blocking the
				// dispatch goroutine. The next successful send will
				// surface the accumulated count.
				suppressedDrops++
			}
		},
	)
	// cancelWork BEFORE sched.Shutdown: Shutdown's wg.Wait() blocks until every
	// worker returns, and a worker wedged in verifier I/O only unblocks when
	// workCtx is cancelled. Deferring them separately would run Shutdown first
	// (LIFO) and hang. Any lossless drain on a graceful cancel happens earlier,
	// before readLoop returns; by here we want a prompt teardown.
	defer func() {
		cancelWork()
		sched.Shutdown()
	}()

	// Reader goroutine: reads raw frames and sends them to msgCh.
	// Guaranteed not to leak because consumeLoop calls conn.CloseNow()
	// after readLoop returns, which forces the blocking conn.Read to
	// fail. The ctx.Done() branch handles the case where the context
	// is cancelled before the read completes.
	msgCh := make(chan readResult, 1)
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			select {
			case msgCh <- readResult{data, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	// Verifier auxiliary channels (resync events, async errors).
	var verifier *sync.Verifier
	var resyncEvents <-chan sync.ResyncEvent
	var verifierAsyncErrs <-chan error
	if c.opts.Verifier.HasVal() && c.opts.Verifier.Val() != nil {
		verifier = c.opts.Verifier.Val()
		// Ordered mode: completed async resyncs are claimed via
		// TakeCompletedResyncs — by verifyOne after each per-DID
		// verification, and by the timer sweep below for DIDs with no
		// follow-on events — instead of flowing through resyncEvents.
		// Claim-at-verify is what guarantees a resync is emitted
		// before any post-resync event for its DID; a select over a
		// separate completion channel cannot order across channels.
		// The resyncEvents arm is kept for events buffered before a
		// prior consumer enabled ordered mode (sticky), and is quiet
		// afterwards.
		verifier.EnableOrderedResyncDelivery()
		resyncEvents = verifier.ResyncEvents()
		verifierAsyncErrs = verifier.AsyncErrors()
	}
	batch := make([]Event, 0, c.batchSize)
	timer := time.NewTimer(c.batchTimeout)
	defer timer.Stop()

	// The timer also drives ordered-resync sweeps while the batch is
	// empty. When a new batch starts, drain any stale empty-sweep tick
	// so the first event gets a full BatchTimeout window.
	resetBatchTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(c.batchTimeout)
	}

	appendBatch := func(evt Event) {
		if len(batch) == 0 {
			resetBatchTimer()
		}
		batch = append(batch, evt)
	}

	// flushBatch yields the current batch and updates the cursor to
	// max(currentCursor, watermark). Returns true if the caller
	// stopped iterating.
	flushBatch := func() bool {
		if len(batch) == 0 {
			return false
		}
		stopped := !yield(batch, nil)
		if !stopped {
			watermark := inflight.Min()
			cur := c.cursor.Load()
			var newCur int64
			if watermark > 0 {
				newCur = watermark - 1
			} else {
				// inflight empty: cursor = highest yielded seq.
				for _, e := range batch {
					if s := e.seqOf(); s > newCur {
						newCur = s
					}
				}
			}
			if newCur > cur {
				c.cursor.Store(newCur)
				if c.opts.CursorStore.HasVal() {
					prevCount := c.cursorCount
					c.cursorCount += int64(len(batch))
					if c.cursorCount/c.cursorInterval > prevCount/c.cursorInterval {
						_ = c.opts.CursorStore.Val().SaveCursor(ctx, newCur)
					}
				}
			}
		}
		batch = make([]Event, 0, c.batchSize)
		return stopped
	}

	// emitResyncs appends claimed completed resyncs to the batch as
	// synthetic ResyncAsync events. Returns true if the caller stopped
	// iterating (flush on a full batch).
	emitResyncs := func(rs []sync.ResyncEvent, resetTimer bool) bool {
		for _, r := range rs {
			appendBatch(eventFromAsyncResync(r))
			if len(batch) >= c.batchSize {
				if flushBatch() {
					return true
				}
				if resetTimer {
					resetBatchTimer()
				}
			}
		}
		return false
	}

	handleVerifyResult := func(vr verifyResult, resetTimer bool) bool {
		noteCollected(&vr.evt)
		if emitResyncs(vr.resyncs, resetTimer) {
			return true
		}
		// Invariant: a single Event carries one wire frame, so
		// accountErr (from #account) and hookErr/silentDrop (from
		// #commit/#sync) are mutually exclusive in practice.
		// verify_worker.go's defensive co-execution path stays
		// correct only because of this invariant.
		//
		// accountErr — yield the error, then fall through to also
		// deliver the raw #account event. The verifier's
		// bookkeeping failed, but consumers may still want to react
		// to the takedown/suspension wire-event itself, so we surface
		// both rather than swallowing the event.
		if vr.accountErr != nil {
			if flushBatch() {
				return true
			}
			if resetTimer {
				resetBatchTimer()
			}
			if !yield(nil, vr.accountErr) {
				return true
			}
			appendBatch(vr.evt)
			if seq := vr.evt.seqOf(); seq > 0 {
				inflight.Remove(seq)
			}
			if len(batch) >= c.batchSize {
				if flushBatch() {
					return true
				}
				if resetTimer {
					resetBatchTimer()
				}
			}
			return false
		}
		if vr.hookErr != nil {
			if seq := vr.evt.seqOf(); seq > 0 {
				inflight.Remove(seq)
			}
			if flushBatch() {
				return true
			}
			if resetTimer {
				resetBatchTimer()
			}
			if !yield(nil, vr.hookErr) {
				return true
			}
			return false
		}
		if vr.silentDrop {
			if seq := vr.evt.seqOf(); seq > 0 {
				inflight.Remove(seq)
			}
			return false
		}
		appendBatch(vr.evt)
		if seq := vr.evt.seqOf(); seq > 0 {
			inflight.Remove(seq)
		}
		if len(batch) >= c.batchSize {
			if flushBatch() {
				return true
			}
			if resetTimer {
				resetBatchTimer()
			}
		}
		return false
	}

	// drainResults is declared before dispatch so dispatch can call it
	// from its DecodeError / GapError branches; the body is assigned
	// below. This is a forward-reference dance: a func var declared
	// here, then assigned by name later, lets dispatch reference
	// drainResults without circular-init worries.
	var drainResults func() bool

	// dispatch decodes a frame and either (a) sends it directly to
	// resultCh for events with no DID or (b) AddWork to scheduler.
	// Returns false if the caller wants to stop iterating.
	dispatch := func(data []byte) bool {
		evt, err := c.decode(data)
		if errors.Is(err, errUnknownType) || errors.Is(err, errUnknownOp) {
			return true
		}
		if err != nil {
			// Drain any results that completed while this frame was
			// waiting in msgCh so events that arrived BEFORE the bad
			// frame land in the batch ahead of the DecodeError. Without
			// this, the dispatch goroutine could yield the error while
			// pre-error events were still in flight in the scheduler,
			// breaking the "partial batch flushes before error" contract
			// that consumers rely on for ordered error handling.
			if drainResults() {
				return false
			}
			if flushBatch() {
				return false
			}
			resetBatchTimer()
			return yield(nil, &DecodeError{Frame: data, Err: fmt.Errorf("decode: %w", err)})
		}

		// Plumb context, sync client, and strict validation onto the
		// event before it heads into the scheduler. Operations() reads
		// these to lazy-fetch a #sync repo and to gate strict syntax
		// validation per yielded op.
		if evt.Sync != nil && c.syncClient != nil {
			evt.ctx = ctx
			evt.syncClient = c.syncClient
		}
		evt.strictValidation = c.opts.StrictValidation.ValOr(false)

		// Global gap detection. The dispatch goroutine reads frames
		// from msgCh single-threaded, so the relay's monotonic seq is
		// observable here. Skipped on Jetstream (whose cursor is
		// time_us, not seq).
		seq := evt.seqOf()
		did := evt.repoOf()
		if seq > 0 && !c.isJetstream {
			if lastSeenSeq > 0 && seq > lastSeenSeq+1 {
				// Drain in-flight results so pre-gap events (any seq
				// already dispatched but still verifying) reach the
				// consumer ahead of the GapError. Same contract as the
				// DecodeError path above.
				if drainResults() {
					return false
				}
				if flushBatch() {
					return false
				}
				resetBatchTimer()
				if !yield(nil, &GapError{Expected: lastSeenSeq + 1, Got: seq}) {
					return false
				}
			}
			lastSeenSeq = seq
		}

		// Track in-flight for watermark cursor.
		if seq > 0 {
			inflight.Add(seq)
		}

		// All events flow through the scheduler — DID-less events
		// (#info, label-stream Info) use a fixed empty key. The
		// scheduler serializes work for any single key, so events for
		// the empty key serialize with each other AND with the lone
		// worker at Parallelism=1, preserving global cross-DID
		// ordering. Pre-unification, DID-less events bypassed the
		// scheduler with a direct resultCh send, which raced with
		// in-flight DID work at N=1 and broke strict ordering (e.g.
		// #info arriving before a still-verifying #commit it should
		// follow).
		if err := sched.AddWork(ctx, did, schedJob{evt: evt}); err != nil {
			// Context cancelled mid-dispatch.
			if seq > 0 {
				inflight.Remove(seq) // we will not see a result
			}
			return false
		}
		if did != "" {
			inflightByDID[did]++
		}
		pendingResults++
		return true
	}

	// drainResults pulls any pending results from resultCh into the
	// batch (calling flushBatch on overflow) until every scheduler job
	// has produced a result. Used on connection-close paths AND before
	// yielding GapError / DecodeError, so pre-error events in flight
	// reach the consumer ahead of the error. Returns true if the caller
	// stopped iterating.
	drainResults = func() bool {
		for pendingResults > 0 {
			select {
			case vr := <-resultCh:
				pendingResults--
				if handleVerifyResult(vr, false) {
					return true
				}
			case <-ctx.Done():
				return false
			}
		}
		return false
	}

	// drainCancelDoneResults runs on a graceful consumer-ctx cancel. It pulls
	// in-flight verification results into the batch so an event whose
	// verification completed — advancing the verifier's durable per-DID rev as a
	// side effect — is delivered rather than dropped. A dropped completed event
	// would be lost forever: the next subscribe re-delivers it, but the
	// now-advanced verifier rev-replay-drops it (ops==nil → silentDrop).
	//
	// The drain waits for results until cancelGrace elapses, then triggers
	// cancelWork to unblock any verifier still wedged in blocking I/O (e.g. a
	// slow SaveChain / PLC fetch) and stops. This is the safe middle ground:
	//   - A verification that already finished delivers its result within the
	//     grace window (it only needs to send on resultCh — microseconds), so
	//     no committed-rev event is lost.
	//   - A verification still running at grace expiry has NOT committed its rev
	//     (SaveChain is its last step), so abandoning it is safe — re-delivery
	//     re-verifies cleanly — and cancelWork makes shutdown prompt regardless.
	// cancelGrace is well under any consumer's shutdown deadline; the common
	// case (all work already done) returns immediately when pendingResults hits
	// zero without waiting on the timer.
	drainCancelDoneResults := func() {
		if pendingResults <= 0 {
			return
		}
		const cancelGrace = 750 * time.Millisecond
		graceTimer := time.NewTimer(cancelGrace)
		defer graceTimer.Stop()
		for pendingResults > 0 {
			select {
			case vr := <-resultCh:
				pendingResults--
				_ = handleVerifyResult(vr, false)
			case <-graceTimer.C:
				// Unblock any wedged verifier I/O; stragglers' results (if any
				// land now) are abandoned, but they have no committed rev.
				cancelWork()
				return
			}
		}
	}

	for {
		select {
		case res := <-msgCh:
			if res.err != nil {
				// A consumer-ctx cancel usually surfaces HERE first: conn.Read
				// observes ctx.Done and the reader delivers its error before the
				// ctx.Done() select arm below can run. Treat that case as a
				// graceful cancel and drain in-flight results losslessly (see
				// drainCancelDoneResults) rather than the bail-on-ctx.Done
				// drainResults, so a completed verification (verifier rev already
				// advanced) is delivered, not dropped-then-rev-replay-lost.
				if ctx.Err() != nil {
					drainCancelDoneResults()
					flushBatch()
					return false
				}
				if drainResults() {
					return true
				}
				if flushBatch() {
					return true
				}
				return false
			}
			if !dispatch(res.data) {
				return flushBatch()
			}

		case vr := <-resultCh:
			pendingResults--
			if handleVerifyResult(vr, true) {
				return true
			}
			continue

		case res, ok := <-resyncEvents:
			if !ok {
				resyncEvents = nil
				continue
			}
			appendBatch(eventFromAsyncResync(res))
			if len(batch) >= c.batchSize {
				if flushBatch() {
					return true
				}
				resetBatchTimer()
			}

		case err, ok := <-verifierAsyncErrs:
			if !ok {
				verifierAsyncErrs = nil
				continue
			}
			if flushBatch() {
				return true
			}
			resetBatchTimer()
			if !yield(nil, err) {
				return true
			}

		case err := <-asyncErr:
			// asyncErr is function-local and never closed by anyone, so
			// the comma-ok form (which would handle channel close) is
			// not needed.
			if flushBatch() {
				return true
			}
			resetBatchTimer()
			if !yield(nil, err) {
				return true
			}

		case <-timer.C:
			if verifier != nil && verifier.CompletedResyncCount() > 0 {
				for _, did := range verifier.CompletedResyncDIDs() {
					if inflightByDID[string(did)] > 0 {
						continue
					}
					if emitResyncs(verifier.TakeCompletedResyncs(did), true) {
						return true
					}
				}
			}
			if flushBatch() {
				return true
			}
			resetBatchTimer()

		case <-ctx.Done():
			// Graceful consumer cancel. Drain results for work already
			// dispatched to the scheduler before returning, so an in-flight
			// verification (whose verifier rev-state has advanced) is delivered
			// rather than silently lost — a dropped tail event re-delivered on
			// the next subscribe would be rev-replay-dropped and lost forever.
			// Bounded by drainGrace so a genuinely stuck verifier cannot hang
			// shutdown; workCtx is left alive for the drain and cancelled (via
			// the deferred cancelWork) only after we return, which unblocks any
			// verifier I/O that outlived the grace window.
			drainCancelDoneResults()
			if flushBatch() {
				return true
			}
			return false
		}
	}
}

// backoffAndRetry sleeps with exponential backoff and returns the next attempt number.
func (c *Client) backoffAndRetry(ctx context.Context, attempt int) int {
	b := c.opts.Backoff.Val()
	d := b.delay(attempt)
	if c.opts.OnReconnect.HasVal() {
		c.opts.OnReconnect.Val()(attempt, d)
	}
	_ = sleep(ctx, d)
	return attempt + 1
}

// deriveHTTPURL converts a WebSocket URL to an HTTP base URL.
// "wss://bsky.network/xrpc/..." → "https://bsky.network"
func deriveHTTPURL(wsURL string) (string, error) {
	parsed, err := url.Parse(wsURL)
	if err != nil {
		return "", err
	}
	switch parsed.Scheme {
	case "wss":
		parsed.Scheme = "https"
	case "ws":
		parsed.Scheme = "http"
	default:
		return "", fmt.Errorf("unexpected scheme: %s", parsed.Scheme)
	}
	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

// eventFromAsyncResync maps a completed verifier worker result into the
// normal streaming Event shape. Async resyncs have no upstream seq; the
// synthetic Sync envelope carries the DID and new rev so consumers can
// handle them like #sync-driven resyncs.
func eventFromAsyncResync(res sync.ResyncEvent) Event {
	return Event{
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID: string(res.DID),
			Rev: res.NewRev,
		},
		Resync:      ResyncAsync,
		verifiedOps: convertVerifierOps(res.Ops),
		verifierRan: true,
	}
}

// convertVerifierOps maps a slice of sync.VerifierOp to the streaming
// layer's Operation type. Both types carry identical fields; this
// helper exists to keep the readLoop and verifier-worker call sites
// from drifting if a field is added.
func convertVerifierOps(vos []sync.VerifierOp) []Operation {
	ops := make([]Operation, len(vos))
	for i, vo := range vos {
		ops[i] = Operation{
			Action:     vo.Action,
			Collection: vo.Collection,
			RKey:       vo.RKey,
			Repo:       vo.Repo,
			Rev:        vo.Rev,
			CID:        vo.CID,
			blockData:  vo.BlockData,
		}
	}
	return ops
}
