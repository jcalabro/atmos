package streaming

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
)

const defaultMaxMessageSize = 2 * 1024 * 1024 // 2 MiB

// Options configures a streaming client.
type Options struct {
	// URL is the full WebSocket URL
	// (e.g. "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos").
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
}

const defaultCursorInterval int64 = 100

// Client connects to an ATProto event stream (firehose).
type Client struct {
	opts           Options
	cursor         atomic.Int64
	conn           atomic.Pointer[websocket.Conn]
	cursorCount    int64 // only used in readLoop goroutine
	cursorInterval int64
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

	interval := opts.CursorInterval.ValOr(defaultCursorInterval)

	c := &Client{opts: opts, cursorInterval: interval}

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
// configured, the current cursor is persisted (best-effort).
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

// Events returns an iterator over firehose events. It connects to the
// WebSocket, automatically handles ping/pong, reconnects with backoff on
// connection loss, and resumes from the last seen cursor.
//
// The iterator yields events until the context is cancelled. Cancel the
// context to stop; [Client.Close] only closes the current WebSocket but
// does not stop the iterator.
//
// Events must not be called concurrently from multiple goroutines.
func (c *Client) Events(ctx context.Context) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		attempt := 0
		for {
			if ctx.Err() != nil {
				return
			}

			conn, err := c.dial(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Dial failed — backoff and retry.
				attempt = c.backoffAndRetry(ctx, attempt)
				continue
			}

			// Reset backoff after successful connection.
			attempt = 0

			// Read loop — process messages until error.
			shouldReturn := c.readLoop(ctx, conn, yield)
			_ = conn.CloseNow()
			if shouldReturn {
				return
			}

			// Connection lost — reconnect with backoff.
			attempt = c.backoffAndRetry(ctx, attempt)
		}
	}
}

// dial connects to the WebSocket endpoint with the current cursor.
func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	u := c.opts.URL
	cur := c.cursor.Load()
	if cur > 0 {
		parsed, err := url.Parse(u)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := parsed.Query()
		q.Set("cursor", fmt.Sprintf("%d", cur))
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

// readLoop reads messages from the WebSocket and yields events. Returns true if
// the iterator should stop (context cancelled or non-retryable error).
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, yield func(Event, error) bool) bool {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return true // context cancelled
			}
			if isConsumerTooSlow(err) {
				return false // reconnect
			}
			// Other connection errors — reconnect.
			return false
		}

		evt, err := decodeFrame(data)
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

		// Sequence gap detection.
		seq := evt.seqOf()
		if seq > 0 {
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

// isConsumerTooSlow checks if the error is a WebSocket close with policy
// violation status (1008), which ATProto relays use for "ConsumerTooSlow".
func isConsumerTooSlow(err error) bool {
	if closeErr, ok := errors.AsType[websocket.CloseError](err); ok {
		return closeErr.Code == websocket.StatusPolicyViolation
	}
	return false
}
