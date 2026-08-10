package streaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildV1JSONFrame wraps a lexicon message in the v1 JSON envelope for
// tests that don't want the *testing.T-taking helper.
func buildV1JSONFrame(payload any) []byte {
	p, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return fmt.Appendf(nil, `{"$type":"message","payload":%s}`, p)
}

func v1IdentityFrame(seq int64, did string) memFrame {
	return memFrame{websocket.MessageText, buildV1JSONFrame(&comatproto.SyncSubscribeRepos_Identity{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#identity",
		Seq:           seq,
		DID:           did,
		Time:          "2024-01-01T00:00:00Z",
	})}
}

// TestSubprotocolOptionValidation covers NewClient's Subprotocols checks.
func TestSubprotocolOptionValidation(t *testing.T) {
	t.Parallel()

	t.Run("unknown token rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewClient(Options{
			URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Subprotocols: gt.Some([]xrpc.Subprotocol{"xrpc.v1.cbor"}),
		})
		require.ErrorContains(t, err, "unsupported subprotocol")
	})

	t.Run("empty list rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewClient(Options{
			URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Subprotocols: gt.Some([]xrpc.Subprotocol{}),
		})
		require.ErrorContains(t, err, "must not be empty")
	})

	t.Run("duplicate tokens rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewClient(Options{
			URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV1JSON}),
		})
		require.ErrorContains(t, err, "duplicate subprotocol")
	})

	t.Run("jetstream rejected", func(t *testing.T) {
		t.Parallel()
		_, err := NewClient(Options{
			URL:          "wss://jetstream.example/subscribe",
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
		})
		require.ErrorContains(t, err, "not supported for Jetstream")
	})

	t.Run("valid offers accepted", func(t *testing.T) {
		t.Parallel()
		_, err := NewClient(Options{
			URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV0CBOR}),
		})
		require.NoError(t, err)
	})
}

// TestV1JSONClientDecode drives the client over an injected conn whose
// handshake negotiated xrpc.v1.json and asserts text frames decode
// through the v1 pipeline.
func TestV1JSONClientDecode(t *testing.T) {
	t.Parallel()

	conn := newMemConnTyped(string(xrpc.SubprotocolV1JSON),
		v1IdentityFrame(1, "did:plc:alice"),
		v1IdentityFrame(2, "did:plc:bob"),
	)

	client := mustNewClient(t, Options{
		URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
		Parallelism:  gt.Some(1),
		Dial: gt.Some(DialFunc(func(_ context.Context, _ string, cfg DialConfig) (Conn, *http.Response, error) {
			assert.Equal(t, []xrpc.Subprotocol{xrpc.SubprotocolV1JSON}, cfg.Subprotocols)
			return conn, nil, nil
		})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var events []Event
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, batch...)
		if len(events) >= 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
	assert.Equal(t, "did:plc:bob", events[1].Identity.DID)
	assert.Equal(t, int64(2), client.Cursor())
}

// TestV1JSONRejectsBinaryFrames asserts strictness: on a v1.json
// connection a binary frame is a DecodeError, and reading continues.
func TestV1JSONRejectsBinaryFrames(t *testing.T) {
	t.Parallel()

	conn := newMemConnTyped(string(xrpc.SubprotocolV1JSON),
		v1IdentityFrame(1, "did:plc:alice"),
		// A valid v0 CBOR frame, but sent binary on a v1.json stream.
		memFrame{websocket.MessageBinary, buildFrame("#identity", buildIdentityBody(2, "did:plc:mallory"))},
		v1IdentityFrame(3, "did:plc:carol"),
	)

	client := mustNewClient(t, Options{
		URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
		Parallelism:  gt.Some(1),
		BatchSize:    gt.Some(1),
		Dial: gt.Some(DialFunc(func(_ context.Context, _ string, _ DialConfig) (Conn, *http.Response, error) {
			return conn, nil, nil
		})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var events []Event
	var decodeErrs []*DecodeError
	var gapErrs []*GapError
	for batch, err := range client.Events(ctx) {
		if err != nil {
			if de, ok := errors.AsType[*DecodeError](err); ok {
				decodeErrs = append(decodeErrs, de)
				continue
			}
			// The rejected frame consumed seq 2, so the next recognized
			// frame fires a GapError — the same contract as a malformed
			// frame's DecodeError. The rejected frame IS data loss.
			ge, ok := errors.AsType[*GapError](err)
			require.True(t, ok, "unexpected error type: %v", err)
			gapErrs = append(gapErrs, ge)
			continue
		}
		events = append(events, batch...)
		if len(events) >= 2 {
			cancel()
		}
	}

	require.Len(t, events, 2)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
	assert.Equal(t, "did:plc:carol", events[1].Identity.DID)
	require.Len(t, decodeErrs, 1)
	assert.Contains(t, decodeErrs[0].Error(), "unexpected websocket message type")
	require.Len(t, gapErrs, 1)
	assert.Equal(t, int64(2), gapErrs[0].Expected)
	assert.Equal(t, int64(3), gapErrs[0].Got)
}

// TestSubprotocolEchoStrictness asserts RFC 6455 §4.1 step 6: a
// non-empty server selection that was not in the client's offer (an
// unoffered token, or a case-variant — tokens are case-sensitive per
// RFC 7936) fails the connection with a non-retryable DialError rather
// than silently proceeding on a guessed codec. coder/websocket's own
// echo verification is EqualFold and custom DialFuncs may not verify
// at all, so the client enforces this itself.
func TestSubprotocolEchoStrictness(t *testing.T) {
	t.Parallel()

	// The iterator must yield the DialError once and then terminate on
	// its own — even though the consumer keeps accepting (no cancel, no
	// break) — because redialing would renegotiate the same violation
	// in a backoff-free loop. The 5s ctx bounds the test if it doesn't.
	expectDialError := func(t *testing.T, opts Options) *DialError {
		t.Helper()
		client := mustNewClient(t, opts)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var de *DialError
		for batch, err := range client.Events(ctx) {
			require.Empty(t, batch)
			require.Error(t, err)
			require.Nil(t, de, "iterator must terminate after the first DialError")
			var ok bool
			de, ok = errors.AsType[*DialError](err)
			require.True(t, ok, "want DialError, got %v", err)
		}
		require.NoError(t, ctx.Err(), "iterator must self-terminate, not run out the clock")
		require.NotNil(t, de)
		return de
	}

	t.Run("case-variant echo fails the connection", func(t *testing.T) {
		t.Parallel()
		conn := newMemConnTyped("XRPC.V1.JSON",
			memFrame{websocket.MessageBinary, buildFrame("#identity", buildIdentityBody(1, "did:plc:case"))})

		de := expectDialError(t, Options{
			URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV0CBOR}),
			Parallelism:  gt.Some(1),
			Dial: gt.Some(DialFunc(func(_ context.Context, _ string, _ DialConfig) (Conn, *http.Response, error) {
				return conn, nil, nil
			})),
		})
		assert.Contains(t, de.Error(), "unoffered subprotocol")
	})

	t.Run("unoffered echo fails the connection", func(t *testing.T) {
		t.Parallel()
		// Server claims v1.json but the client never offered anything.
		conn := newMemConnTyped(string(xrpc.SubprotocolV1JSON),
			memFrame{websocket.MessageBinary, buildFrame("#identity", buildIdentityBody(1, "did:plc:unoffered"))})

		de := expectDialError(t, Options{
			URL:         "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Parallelism: gt.Some(1),
			Dial: gt.Some(DialFunc(func(_ context.Context, _ string, _ DialConfig) (Conn, *http.Response, error) {
				return conn, nil, nil
			})),
		})
		assert.Contains(t, de.Error(), "unoffered subprotocol")
	})

	t.Run("offered v0 echo enforces binary frames", func(t *testing.T) {
		t.Parallel()
		// An explicit v0 selection (not fallback) enforces binary
		// message typing; the frame decodes on the legacy path.
		conn := newMemConnTyped(string(xrpc.SubprotocolV0CBOR),
			memFrame{websocket.MessageBinary, buildFrame("#identity", buildIdentityBody(1, "did:plc:v0"))})

		client := mustNewClient(t, Options{
			URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV0CBOR}),
			Parallelism:  gt.Some(1),
			Dial: gt.Some(DialFunc(func(_ context.Context, _ string, _ DialConfig) (Conn, *http.Response, error) {
				return conn, nil, nil
			})),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var events []Event
		for batch, err := range client.Events(ctx) {
			require.NoError(t, err)
			events = append(events, batch...)
			if len(events) >= 1 {
				cancel()
			}
		}
		require.Len(t, events, 1)
		assert.Equal(t, "did:plc:v0", events[0].Identity.DID)
	})
}

// TestUnnegotiatedConnStaysCBOR asserts that when the server echoes no
// subprotocol, the connection keeps the legacy v0 CBOR decoder even
// though the client offered v1.json.
func TestUnnegotiatedConnStaysCBOR(t *testing.T) {
	t.Parallel()

	// memConn with no subprotocol: frames are legacy binary CBOR.
	conn := newMemConn(buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))

	client := mustNewClient(t, Options{
		URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV0CBOR}),
		Parallelism:  gt.Some(1),
		Dial: gt.Some(DialFunc(func(_ context.Context, _ string, _ DialConfig) (Conn, *http.Response, error) {
			return conn, nil, nil
		})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var events []Event
	for batch, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, batch...)
		if len(events) >= 1 {
			cancel()
		}
	}

	require.Len(t, events, 1)
	assert.Equal(t, "did:plc:alice", events[0].Identity.DID)
}

// TestV1JSONErrorFrame asserts a v1 error frame surfaces as StreamError.
func TestV1JSONErrorFrame(t *testing.T) {
	t.Parallel()

	conn := newMemConnTyped(string(xrpc.SubprotocolV1JSON),
		memFrame{websocket.MessageText, []byte(`{"$type":"error","error":"FutureCursor","message":"cursor in the future"}`)},
	)

	client := mustNewClient(t, Options{
		URL:          "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos",
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
		Parallelism:  gt.Some(1),
		Dial: gt.Some(DialFunc(func(_ context.Context, _ string, _ DialConfig) (Conn, *http.Response, error) {
			return conn, nil, nil
		})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, err := range client.Events(ctx) {
		if err != nil {
			se, ok := errors.AsType[*StreamError](err)
			require.True(t, ok, "want StreamError, got %v", err)
			assert.Equal(t, "FutureCursor", se.Code)
			cancel()
		}
	}
}
