//go:build !js

package streaming

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

func dial(ctx context.Context, u string) (*websocket.Conn, *http.Response, error) {
	return websocket.Dial(ctx, u, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"User-Agent": []string{"mono/v0.1"},
		},
	})
}
