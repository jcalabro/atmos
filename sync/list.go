package sync

import (
	"context"
	"iter"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// ListRepos paginates through all repos on the service, yielding one page at a time
// so callers can perform batch operations. Per-entry parse errors are yielded with a
// nil batch and iteration continues; transport errors terminate iteration.
func (c *Client) ListRepos(ctx context.Context, limit int64) iter.Seq2[[]ListReposEntry, error] {
	return func(yield func([]ListReposEntry, error) bool) {
		cursor := ""
		for {
			if ctx.Err() != nil {
				return
			}

			out, err := comatproto.SyncListRepos(ctx, c.opts.Client, cursor, limit)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(out.Repos) == 0 {
				return
			}

			batch := make([]ListReposEntry, 0, len(out.Repos))
			for _, r := range out.Repos {
				did, err := atmos.ParseDID(r.DID)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}

				batch = append(batch, ListReposEntry{
					DID:    did,
					Rev:    r.Rev,
					Head:   r.Head,
					Active: r.Active.ValOr(true),
				})
			}

			if len(batch) > 0 {
				if !yield(batch, nil) {
					return
				}
			}

			if !out.Cursor.HasVal() || out.Cursor.Val() == "" {
				return
			}
			cursor = out.Cursor.Val()
		}
	}
}

// GetLatestCommit returns the current revision and commit CID for a repo.
func (c *Client) GetLatestCommit(ctx context.Context, did atmos.DID) (rev string, commitCID cbor.CID, err error) {
	out, err := comatproto.SyncGetLatestCommit(ctx, c.opts.Client, string(did))
	if err != nil {
		return "", cbor.CID{}, err
	}

	cid, err := cbor.ParseCIDString(out.CID)
	if err != nil {
		return "", cbor.CID{}, err
	}

	return out.Rev, cid, nil
}
