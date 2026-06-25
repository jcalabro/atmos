//go:build js

package streaming

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

func dial(ctx context.Context, u string) (Conn, *http.Response, error) {
	return websocket.Dial(ctx, u, nil)
}
