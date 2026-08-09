// Tests require a real WebSocket dial, unavailable in Node/WASI.
//go:build !js && !wasip1

package xrpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testNSID = "com.example.test.subscribeThings"

func identityUnion(seq int64, did string) comatproto.SyncSubscribeRepos_Message {
	return comatproto.SyncSubscribeRepos_Message{
		SyncSubscribeRepos_Identity: gt.SomeRef(comatproto.SyncSubscribeRepos_Identity{
			Seq:  seq,
			DID:  did,
			Time: "2024-01-01T00:00:00Z",
		}),
	}
}

// startSubscriptionServer registers one subscription endpoint and
// returns the test server.
func startSubscriptionServer(t *testing.T, cfg SubscriptionConfig, fn SubscriptionHandler) *httptest.Server {
	t.Helper()
	var s Server
	require.NoError(t, s.HandleSubscription(testNSID, cfg, fn))
	srv := httptest.NewServer(&s)
	t.Cleanup(srv.Close)
	return srv
}

func dialSub(t *testing.T, srv *httptest.Server, subprotocols ...string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/" + testNSID
	var opts *websocket.DialOptions
	if len(subprotocols) > 0 {
		opts = &websocket.DialOptions{Subprotocols: subprotocols}
	}
	conn, resp, err := websocket.Dial(ctx, u, opts)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func TestHandleSubscriptionValidation(t *testing.T) {
	t.Parallel()

	handler := func(context.Context, Params, *Stream) error { return nil }

	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()
		var s Server
		require.Error(t, s.HandleSubscription(testNSID, SubscriptionConfig{}, nil))
	})

	t.Run("invalid default subprotocol", func(t *testing.T) {
		t.Parallel()
		var s Server
		err := s.HandleSubscription(testNSID, SubscriptionConfig{
			Subprotocol: gt.Some(xrpc.Subprotocol("xrpc.v1.cbor")),
		}, handler)
		require.ErrorContains(t, err, "unsupported default subprotocol")
	})

	t.Run("supported set missing default", func(t *testing.T) {
		t.Parallel()
		var s Server
		err := s.HandleSubscription(testNSID, SubscriptionConfig{
			Subprotocol:  gt.Some(xrpc.SubprotocolV0CBOR),
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
		}, handler)
		require.ErrorContains(t, err, "must include the default")
	})

	t.Run("invalid member of supported set", func(t *testing.T) {
		t.Parallel()
		var s Server
		err := s.HandleSubscription(testNSID, SubscriptionConfig{
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, "bogus"}),
		}, handler)
		require.ErrorContains(t, err, "unsupported subprotocol")
	})

	t.Run("non-positive write timeout", func(t *testing.T) {
		t.Parallel()
		var s Server
		err := s.HandleSubscription(testNSID, SubscriptionConfig{
			WriteTimeout: gt.Some(time.Duration(0)),
		}, handler)
		require.ErrorContains(t, err, "WriteTimeout")
	})

	t.Run("defaults accepted", func(t *testing.T) {
		t.Parallel()
		var s Server
		require.NoError(t, s.HandleSubscription(testNSID, SubscriptionConfig{}, handler))
	})
}

// TestSubscriptionV0Wire asserts the default (unnegotiated) path emits
// v0 CBOR binary frames that decode with the standard header+body
// framing.
func TestSubscriptionV0Wire(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{},
		func(ctx context.Context, _ Params, stream *Stream) error {
			assert.Equal(t, xrpc.SubprotocolV0CBOR, stream.Subprotocol())
			msg := identityUnion(1, "did:plc:alice")
			return stream.Send(ctx, "#identity", msg)
		})

	conn := dialSub(t, srv)
	assert.Empty(t, conn.Subprotocol())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgType, data, err := conn.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageBinary, msgType)

	// Header: {op:1, t:"#identity"}.
	count, pos, err := cbor.ReadMapHeader(data, 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	hdr := map[string]any{}
	for range count {
		key, next, err := cbor.ReadText(data, pos)
		require.NoError(t, err)
		switch key {
		case "op":
			var op int64
			op, next, err = cbor.ReadInt(data, next)
			require.NoError(t, err)
			hdr["op"] = op
		case "t":
			var tv string
			tv, next, err = cbor.ReadText(data, next)
			require.NoError(t, err)
			hdr["t"] = tv
		}
		pos = next
	}
	assert.Equal(t, int64(1), hdr["op"])
	assert.Equal(t, "#identity", hdr["t"])

	// Body: the identity message.
	var identity comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, identity.UnmarshalCBOR(data[pos:]))
	assert.Equal(t, "did:plc:alice", identity.DID)
	assert.Equal(t, int64(1), identity.Seq)
}

// TestSubscriptionV1JSONWire asserts a negotiated v1.json connection
// emits single-object JSON text frames per proposal 0015.
func TestSubscriptionV1JSONWire(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, xrpc.SubprotocolV1JSON}),
	},
		func(ctx context.Context, _ Params, stream *Stream) error {
			assert.Equal(t, xrpc.SubprotocolV1JSON, stream.Subprotocol())
			msg := identityUnion(7, "did:plc:bob")
			return stream.Send(ctx, "#identity", msg)
		})

	conn := dialSub(t, srv, string(xrpc.SubprotocolV1JSON))
	assert.Equal(t, string(xrpc.SubprotocolV1JSON), conn.Subprotocol())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgType, data, err := conn.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageText, msgType)

	var env struct {
		Type    string          `json:"$type"`
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(data, &env))
	assert.Equal(t, "message", env.Type)

	var payload struct {
		Type string `json:"$type"`
		Seq  int64  `json:"seq"`
		DID  string `json:"did"`
	}
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	assert.Equal(t, "com.atproto.sync.subscribeRepos#identity", payload.Type)
	assert.Equal(t, int64(7), payload.Seq)
	assert.Equal(t, "did:plc:bob", payload.DID)
}

// TestSubscriptionErrorFrames asserts SendError emits the right frame
// shape per subprotocol and that the stream goes terminal.
func TestSubscriptionErrorFrames(t *testing.T) {
	t.Parallel()

	t.Run("v0", func(t *testing.T) {
		t.Parallel()
		srv := startSubscriptionServer(t, SubscriptionConfig{},
			func(ctx context.Context, _ Params, stream *Stream) error {
				require.NoError(t, stream.SendError(ctx, "FutureCursor", "cursor is ahead"))
				// Terminal: further sends fail.
				err := stream.Send(ctx, "#identity", identityUnion(1, "did:plc:x"))
				assert.ErrorIs(t, err, ErrStreamTerminal)
				assert.ErrorIs(t, stream.SendError(ctx, "Again", ""), ErrStreamTerminal)
				return nil
			})

		conn := dialSub(t, srv)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		msgType, data, err := conn.Read(ctx)
		require.NoError(t, err)
		assert.Equal(t, websocket.MessageBinary, msgType)

		// Header {op:-1}, body {error, message}.
		count, pos, err := cbor.ReadMapHeader(data, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
		key, pos, err := cbor.ReadText(data, pos)
		require.NoError(t, err)
		require.Equal(t, "op", key)
		op, pos, err := cbor.ReadInt(data, pos)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), op)

		body := map[string]string{}
		count, pos, err = cbor.ReadMapHeader(data, pos)
		require.NoError(t, err)
		for range count {
			k, next, err := cbor.ReadText(data, pos)
			require.NoError(t, err)
			v, next, err := cbor.ReadText(data, next)
			require.NoError(t, err)
			body[k] = v
			pos = next
		}
		assert.Equal(t, len(data), pos, "no trailing bytes")
		assert.Equal(t, "FutureCursor", body["error"])
		assert.Equal(t, "cursor is ahead", body["message"])
	})

	t.Run("v1 json", func(t *testing.T) {
		t.Parallel()
		srv := startSubscriptionServer(t, SubscriptionConfig{
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, xrpc.SubprotocolV1JSON}),
		},
			func(ctx context.Context, _ Params, stream *Stream) error {
				return stream.SendError(ctx, "ConsumerTooSlow", "")
			})

		conn := dialSub(t, srv, string(xrpc.SubprotocolV1JSON))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		msgType, data, err := conn.Read(ctx)
		require.NoError(t, err)
		assert.Equal(t, websocket.MessageText, msgType)
		assert.JSONEq(t, `{"$type":"error","error":"ConsumerTooSlow"}`, string(data))
	})
}

// TestSubscriptionNegotiationFallback asserts an unrecognized client
// offer falls back to the lexicon default rather than failing.
func TestSubscriptionNegotiationFallback(t *testing.T) {
	t.Parallel()

	got := make(chan xrpc.Subprotocol, 1)
	srv := startSubscriptionServer(t, SubscriptionConfig{},
		func(ctx context.Context, _ Params, stream *Stream) error {
			got <- stream.Subprotocol()
			return stream.Send(ctx, "#identity", identityUnion(1, "did:plc:alice"))
		})

	// Offer a token the server does not support.
	conn := dialSub(t, srv, "acme.custom.proto")
	assert.Empty(t, conn.Subprotocol())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgType, _, err := conn.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageBinary, msgType)
	assert.Equal(t, xrpc.SubprotocolV0CBOR, <-got)
}

// TestSubscriptionV1JSONDefault asserts a stream whose lexicon declares
// xrpc.v1.json serves JSON to unnegotiated clients.
func TestSubscriptionV1JSONDefault(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{
		Subprotocol: gt.Some(xrpc.SubprotocolV1JSON),
	},
		func(ctx context.Context, _ Params, stream *Stream) error {
			assert.Equal(t, xrpc.SubprotocolV1JSON, stream.Subprotocol())
			return stream.Send(ctx, "#identity", identityUnion(3, "did:plc:carol"))
		})

	conn := dialSub(t, srv) // no offer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgType, data, err := conn.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageText, msgType)
	assert.Contains(t, string(data), `"$type":"message"`)
}

// TestSubscriptionValidate asserts pre-upgrade validation errors write
// a normal XRPC error envelope and skip the upgrade.
func TestSubscriptionValidate(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{
		Validate: gt.Some(func(_ context.Context, p Params) error {
			if _, err := p.Int64("cursor"); err != nil {
				return err
			}
			return nil
		}),
	},
		func(context.Context, Params, *Stream) error {
			t.Error("handler must not run when validation fails")
			return nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/xrpc/"+testNSID, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "InvalidRequest", body.Error)
}

// TestSubscriptionHandlerError asserts a handler error closes the
// connection with StatusInternalError and leaks no detail.
func TestSubscriptionHandlerError(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{},
		func(context.Context, Params, *Stream) error {
			return errors.New("database exploded: secret detail")
		})

	conn := dialSub(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	status := websocket.CloseStatus(err)
	assert.Equal(t, websocket.StatusInternalError, status)
	assert.NotContains(t, err.Error(), "secret detail")
}

// TestSubscriptionClientDisconnectCancelsContext asserts the handler
// context is cancelled when the client goes away.
func TestSubscriptionClientDisconnectCancelsContext(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	srv := startSubscriptionServer(t, SubscriptionConfig{},
		func(ctx context.Context, _ Params, _ *Stream) error {
			<-ctx.Done()
			close(done)
			return nil
		})

	conn := dialSub(t, srv)
	_ = conn.CloseNow()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was not cancelled on client disconnect")
	}
}

// TestSubscriptionSendValidation covers Send's argument checks.
func TestSubscriptionSendValidation(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{
		Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV0CBOR, xrpc.SubprotocolV1JSON}),
	},
		func(ctx context.Context, _ Params, stream *Stream) error {
			// Missing '#' prefix.
			err := stream.Send(ctx, "identity", identityUnion(1, "did:plc:x"))
			assert.ErrorContains(t, err, "must start with '#'")

			// v1 payload without a $type: a bare struct with no
			// LexiconTypeID set.
			err = stream.Send(ctx, "#identity", &comatproto.SyncSubscribeRepos_Identity{
				Seq: 1, DID: "did:plc:x", Time: "2024-01-01T00:00:00Z",
			})
			assert.ErrorContains(t, err, "no $type")

			// Empty error code.
			assert.ErrorContains(t, stream.SendError(ctx, "", "x"), "must not be empty")

			// And the stream still works afterwards.
			return stream.Send(ctx, "#identity", identityUnion(2, "did:plc:ok"))
		})

	conn := dialSub(t, srv, string(xrpc.SubprotocolV1JSON))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, data, err := conn.Read(ctx)
	require.NoError(t, err)
	assert.Contains(t, string(data), "did:plc:ok")
}

// TestSubscriptionMethodNotAllowed asserts POST to a subscription is
// rejected before any upgrade.
func TestSubscriptionMethodNotAllowed(t *testing.T) {
	t.Parallel()

	srv := startSubscriptionServer(t, SubscriptionConfig{},
		func(context.Context, Params, *Stream) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/xrpc/"+testNSID, nil)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}
