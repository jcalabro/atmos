package streaming

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

const defaultMaxMessageSize = 2 * 1024 * 1024 // 2 MiB

// Options configures a streaming client.
type Options struct {
	// URL is the full WebSocket URL. The decoder is auto-detected: URLs
	// containing "subscribeLabels" use the label decoder, all others use
	// the repository decoder.
	//
	// Examples:
	//   "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	//   "wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels"
	//
	// Required.
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
	// re-fetching. By default, a sync client is auto-created from the
	// WebSocket URL (the relay typically 302-redirects getRepo requests to
	// the account's PDS). To disable automatic resync, set to
	// gt.Some[*sync.Client](nil). This option is only used for repo
	// consumption, not labels.
	SyncClient gt.Option[*sync.Client]
}

// Client connects to an ATProto event stream (firehose or label stream).
type Client struct {
	opts           Options
	cursor         atomic.Int64
	conn           atomic.Pointer[websocket.Conn]
	cursorCount    int64 // only used in readLoop goroutine
	cursorInterval int64
	decode         func([]byte) (Event, error)
	syncClient     *sync.Client // nil disables automatic #sync resync
	isJetstream    bool

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
	if strings.Contains(opts.URL, "subscribeLabels") {
		decode = decodeLabelFrame
	} else if isJetstreamURL(opts.URL) {
		decode = decodeJetstreamFrame
		isJS = true
	}

	// Resolve the sync client for automatic #sync resync.
	// Jetstream and label streams don't need a sync client.
	var sc *sync.Client
	if opts.SyncClient.HasVal() {
		sc = opts.SyncClient.Val() // may be nil (opt-out)
	} else if !isJS && !strings.Contains(opts.URL, "subscribeLabels") {
		// Auto-create from the WebSocket URL for repo streams.
		httpURL, err := deriveHTTPURL(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("derive HTTP URL for sync client: %w", err)
		}
		sc = sync.NewClient(sync.Options{
			Client: &xrpc.Client{Host: httpURL},
		})
	}

	c := &Client{
		opts:                opts,
		cursorInterval:      interval,
		decode:              decode,
		syncClient:          sc,
		isJetstream:         isJS,
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

// Cursor returns the sequence number of the last successfully yielded event.
// Safe to call concurrently.
func (c *Client) Cursor() int64 {
	return c.cursor.Load()
}

// Close gracefully shuts down the WebSocket connection. If a CursorStore is
// configured, the current cursor is persisted (best-effort). Close does not
// release the distributed lock — the lock is released when the [Events]
// iterator terminates (via context cancellation or the caller breaking out
// of the range loop).
func (c *Client) Close() error {
	if c.opts.CursorStore.HasVal() {
		cur := c.cursor.Load()
		if cur > 0 {
			// Best-effort: use background context so we don't fail on
			// cancelled parent contexts.
			_ = c.opts.CursorStore.Val().SaveCursor(context.Background(), cur)
		}
	}

	conn := c.conn.Load()
	if conn == nil {
		return nil
	}

	return conn.Close(websocket.StatusNormalClosure, "client closing")
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
func (c *Client) Events(ctx context.Context) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
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
// backoff on connection loss. Returns true only when the yield function
// returns false (caller wants to stop iterating). Returns false when the
// context is cancelled or connection errors occur.
func (c *Client) consumeLoop(ctx context.Context, yield func(Event, error) bool) bool {
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
func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
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
			q.Set("collections", strings.Join(c.opts.Collections.Val(), ","))
		}
		if c.opts.DIDs.HasVal() {
			q.Set("dids", strings.Join(c.opts.DIDs.Val(), ","))
		}
	}

	if len(q) > 0 {
		parsed.RawQuery = q.Encode()
		u = parsed.String()
	}

	conn, resp, err := dial(ctx, u)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	conn.SetReadLimit(c.opts.MaxMessageSize.Val())
	c.conn.Store(conn)
	return conn, nil
}

// readLoop reads messages from the WebSocket and yields events. Returns true
// only when the yield function returns false (caller wants to stop). Returns
// false for context cancellation and connection errors (caller should
// reconnect or exit).
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, yield func(Event, error) bool) bool {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return false // context cancelled or connection error
		}

		evt, err := c.decode(data)
		if errors.Is(err, errUnknownType) {
			continue // skip unrecognized event types for forward compat
		}
		if err != nil {
			// Decode error — yield to caller but don't reconnect.
			if !yield(Event{}, &DecodeError{Frame: data, Err: fmt.Errorf("decode: %w", err)}) {
				return true
			}
			continue
		}

		// Attach context and sync client for lazy #sync handling.
		if evt.Sync != nil && c.syncClient != nil {
			evt.ctx = ctx
			evt.syncClient = c.syncClient
		}

		// Sequence gap detection (firehose/labels only — Jetstream uses
		// time_us as cursor which is not sequential).
		seq := evt.seqOf()
		if seq > 0 && !c.isJetstream {
			lastSeq := c.cursor.Load()
			if lastSeq > 0 && seq > lastSeq+1 {
				if !yield(Event{}, &GapError{Expected: lastSeq + 1, Got: seq}) {
					return true
				}
			}
		}

		if !yield(evt, nil) {
			return true
		}

		// Update cursor after successful yield.
		if seq > 0 {
			c.cursor.Store(seq)
			if c.opts.CursorStore.HasVal() {
				c.cursorCount++
				if c.cursorCount%c.cursorInterval == 0 {
					_ = c.opts.CursorStore.Val().SaveCursor(ctx, seq)
				}
			}
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
