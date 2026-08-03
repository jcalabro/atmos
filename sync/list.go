package sync

import (
	"context"
	"fmt"
	"iter"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// ListRepos paginates through repos on the service starting at
// startCursor, yielding one page at a time so callers can perform
// batch operations and persist the relay's cursor for resume across
// process restarts.
//
// Pass startCursor="" to start from the beginning. Each yielded
// ListReposPage carries the entries from that page plus NextCursor,
// the cursor a subsequent call to ListRepos should pass to resume
// past this page. Iteration ends when the relay reports there are
// no more pages.
//
// Per-entry parse errors are yielded with an empty page and
// iteration continues; transport errors terminate iteration.
func (c *Client) ListRepos(ctx context.Context, limit int64, startCursor string) iter.Seq2[ListReposPage, error] {
	return func(yield func(ListReposPage, error) bool) {
		cursor := startCursor
		seen := map[string]struct{}{}
		if cursor != "" {
			seen[cursor] = struct{}{}
		}
		for {
			if ctx.Err() != nil {
				return
			}

			out, err := comatproto.SyncListRepos(ctx, c.opts.Client, cursor, limit)
			if err != nil {
				yield(ListReposPage{}, err)
				return
			}

			if len(out.Repos) == 0 {
				return
			}

			batch := make([]ListReposEntry, 0, len(out.Repos))
			for i, r := range out.Repos {
				did, err := atmos.ParseDID(r.DID)
				if err != nil {
					if !yield(ListReposPage{}, &ListEntryError{Endpoint: "listRepos", Index: i, Err: err}) {
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

			next := ""
			if out.Cursor.HasVal() {
				next = out.Cursor.Val()
			}

			if len(batch) > 0 {
				if !yield(ListReposPage{Entries: batch, NextCursor: next}, nil) {
					return
				}
			}

			if next == "" {
				return
			}
			if _, duplicate := seen[next]; duplicate {
				yield(ListReposPage{}, fmt.Errorf("sync: listRepos cursor loop at %q", next))
				return
			}
			seen[next] = struct{}{}
			cursor = next
		}
	}
}

// ListHosts paginates through upstream hosts known by a relay. Malformed
// entries are yielded as [ListEntryError] values and skipped; transport and
// response errors terminate iteration.
func (c *Client) ListHosts(ctx context.Context, limit int64, startCursor string) iter.Seq2[ListHostsPage, error] {
	return func(yield func(ListHostsPage, error) bool) {
		cursor := startCursor
		seen := map[string]struct{}{}
		if cursor != "" {
			seen[cursor] = struct{}{}
		}
		for {
			if ctx.Err() != nil {
				return
			}

			out, err := comatproto.SyncListHosts(ctx, c.opts.Client, cursor, limit)
			if err != nil {
				yield(ListHostsPage{}, err)
				return
			}
			if len(out.Hosts) == 0 {
				return
			}

			entries := make([]ListHostsEntry, 0, len(out.Hosts))
			for i, host := range out.Hosts {
				if host.Hostname == "" {
					if !yield(ListHostsPage{}, &ListEntryError{Endpoint: "listHosts", Index: i, Err: fmt.Errorf("empty hostname")}) {
						return
					}
					continue
				}
				accountCount := host.AccountCount.ValOr(0)
				seq := host.Seq.ValOr(0)
				if accountCount < 0 || seq < 0 {
					if !yield(ListHostsPage{}, &ListEntryError{Endpoint: "listHosts", Index: i, Err: fmt.Errorf("negative accountCount or seq")}) {
						return
					}
					continue
				}
				entries = append(entries, ListHostsEntry{
					Hostname: host.Hostname, Status: host.Status.ValOr(""),
					AccountCount: accountCount, Seq: seq,
				})
			}

			next := out.Cursor.ValOr("")
			if len(entries) > 0 {
				if !yield(ListHostsPage{Entries: entries, NextCursor: next}, nil) {
					return
				}
			}
			if next == "" {
				return
			}
			if _, duplicate := seen[next]; duplicate {
				yield(ListHostsPage{}, fmt.Errorf("sync: listHosts cursor loop at %q", next))
				return
			}
			seen[next] = struct{}{}
			cursor = next
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
