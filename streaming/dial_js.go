//go:build js

package streaming

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

func dial(ctx context.Context, u string, cfg DialConfig) (Conn, *http.Response, error) {
	var opts *websocket.DialOptions
	if len(cfg.Subprotocols) > 0 {
		// The browser WebSocket API owns the rest of the handshake
		// (headers, compression); only the subprotocol offer is ours.
		subs := make([]string, len(cfg.Subprotocols))
		for i, s := range cfg.Subprotocols {
			subs[i] = string(s)
		}
		opts = &websocket.DialOptions{Subprotocols: subs}
	}
	// Platform limitation: the browser never exposes the HTTP upgrade
	// response, so every dial failure here has a nil *http.Response and
	// stays retryable — including a handshake the browser itself failed
	// for an unoffered subprotocol echo (RFC 6455 §4.1), which native
	// builds surface as a terminal DialError. The two are
	// indistinguishable from a transient network failure in this API,
	// and terminating on every browser dial error would break
	// reconnect-with-backoff for ordinary connectivity blips, so
	// backoff-paced retry is the lesser evil. Use Options.OnReconnect
	// to observe a stream stuck redialing a misconfigured server.
	return websocket.Dial(ctx, u, opts)
}
