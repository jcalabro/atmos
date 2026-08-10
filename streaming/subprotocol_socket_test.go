// Tests require a real WebSocket (browser API), unavailable in Node/WASI.
//go:build !js && !wasip1

package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNegotiationOverSocket exercises the real dial path: the default
// DialFunc must send the Sec-WebSocket-Protocol offer, and decoder
// selection must follow the server's echo.
func TestNegotiationOverSocket(t *testing.T) {
	t.Parallel()

	t.Run("server selects v1.json", func(t *testing.T) {
		t.Parallel()

		srv := startMockRelayWithSubprotocols(t, []string{string(xrpc.SubprotocolV1JSON)},
			func(conn *websocket.Conn, r *http.Request) {
				require.Equal(t, string(xrpc.SubprotocolV1JSON), conn.Subprotocol())
				ctx := context.Background()
				_ = conn.Write(ctx, websocket.MessageText, buildV1JSONFrame(identityMsg(1, "did:plc:alice")))
				_ = conn.Close(websocket.StatusNormalClosure, "done")
			})

		client := mustNewClient(t, Options{
			URL:          wsURL(srv),
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON}),
			Parallelism:  gt.Some(1),
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
	})

	t.Run("server without v1 support falls back to v0", func(t *testing.T) {
		t.Parallel()

		// A legacy relay: no Subprotocols configured, so it echoes
		// nothing and speaks binary v0 CBOR.
		srv := startMockRelay(t, func(conn *websocket.Conn, _ *http.Request) {
			writeFrames(conn, buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		})

		client := mustNewClient(t, Options{
			URL:          wsURL(srv),
			Subprotocols: gt.Some([]xrpc.Subprotocol{xrpc.SubprotocolV1JSON, xrpc.SubprotocolV0CBOR}),
			Parallelism:  gt.Some(1),
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
	})

	t.Run("no offer sends no header", func(t *testing.T) {
		t.Parallel()

		var gotHeader string
		srv := startMockRelay(t, func(conn *websocket.Conn, r *http.Request) {
			gotHeader = r.Header.Get("Sec-WebSocket-Protocol")
			writeFrames(conn, buildFrame("#identity", buildIdentityBody(1, "did:plc:alice")))
		})

		client := mustNewClient(t, Options{URL: wsURL(srv), Parallelism: gt.Some(1)})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for batch, err := range client.Events(ctx) {
			require.NoError(t, err)
			if len(batch) > 0 {
				cancel()
			}
		}
		assert.Empty(t, gotHeader)
	})
}

// startMockRelayWithSubprotocols is startMockRelay with server-side
// subprotocol negotiation enabled.
func startMockRelayWithSubprotocols(t *testing.T, subs []string, handler func(conn *websocket.Conn, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: subs})
		if err != nil {
			t.Logf("accept error: %v", err)
			return
		}
		handler(conn, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// identityMsg builds an identity message payload for v1 JSON frames.
func identityMsg(seq int64, did string) *comatproto.SyncSubscribeRepos_Identity {
	return &comatproto.SyncSubscribeRepos_Identity{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#identity",
		Seq:           seq,
		DID:           did,
		Time:          "2024-01-01T00:00:00Z",
	}
}
