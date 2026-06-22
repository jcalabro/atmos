package sync

import (
	"context"
	"io"

	"github.com/jcalabro/atmos"
)

// GetRepoStream downloads a repo as a streaming CAR. The caller MUST close
// the returned ReadCloser. If since is non-empty, requests a diff from that rev.
func (c *Client) GetRepoStream(ctx context.Context, did atmos.DID, since string) (io.ReadCloser, error) {
	body, _, err := c.GetRepoStreamHost(ctx, did, since)
	return body, err
}

// GetRepoStreamHost is [GetRepoStream] that also reports the host that
// served the CAR — the final URL's host after any relay 302 redirect to
// the account's PDS. See [xrpc.Client.QueryStreamHost] for the host
// semantics (including the [*xrpc.Error] carrying Host on the error
// path). The caller MUST close the returned ReadCloser.
func (c *Client) GetRepoStreamHost(ctx context.Context, did atmos.DID, since string) (io.ReadCloser, string, error) {
	params := map[string]any{
		"did": string(did),
	}
	if since != "" {
		params["since"] = since
	}
	return c.opts.Client.QueryStreamHost(ctx, "com.atproto.sync.getRepo", params)
}
