package sync

import (
	"context"
	"io"

	atmos "github.com/jcalabro/atmos"
)

// GetRepoStream downloads a repo as a streaming CAR. The caller MUST close
// the returned ReadCloser. If since is non-empty, requests a diff from that rev.
func (c *Client) GetRepoStream(ctx context.Context, did atmos.DID, since string) (io.ReadCloser, error) {
	params := map[string]any{
		"did": string(did),
	}
	if since != "" {
		params["since"] = since
	}
	return c.opts.Client.QueryStream(ctx, "com.atproto.sync.getRepo", params)
}
