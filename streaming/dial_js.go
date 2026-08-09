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
	return websocket.Dial(ctx, u, opts)
}
