package sync

import (
	"bufio"
	"context"
	"errors"
	"iter"
	"strings"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
)

var errBreak = errors.New("break")

// IterRecords downloads a repo and yields every record as an iterator.
// The HTTP body is streamed directly into the CAR reader (not buffered).
// All blocks are loaded into a MemBlockStore for MST traversal.
//
// If Options.Directory is set, the commit signature is verified before
// yielding any records.
func (c *Client) IterRecords(ctx context.Context, did atmos.DID) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		body, err := c.GetRepoStream(ctx, did, "")
		if err != nil {
			yield(Record{}, err)
			return
		}
		defer func() { _ = body.Close() }()

		// Wrap in bufio.Reader to batch small reads from the network.
		// CAR varint parsing reads 1 byte at a time; without buffering
		// each would be a separate syscall over HTTP.
		rp, commit, err := repo.LoadFromCAR(bufio.NewReader(body))
		if err != nil {
			yield(Record{}, err)
			return
		}

		// Optional signature verification.
		if c.opts.Directory.HasVal() {
			if err := c.VerifyCommit(ctx, commit); err != nil {
				yield(Record{}, err)
				return
			}
		}

		// Walk MST, yield each record.
		_ = rp.Tree.Walk(func(key string, val cbor.CID) error {
			col, rkey := splitKey(key)

			data, err := rp.Store.GetBlock(val)
			if err != nil {
				if !yield(Record{}, err) {
					return errBreak
				}
				return nil
			}

			if !yield(Record{Collection: col, RKey: rkey, CID: val, Data: data, Rev: commit.Rev}, nil) {
				return errBreak
			}
			return nil
		})
	}
}

// splitKey splits an MST key "collection/rkey" into its parts.
func splitKey(key string) (collection, rkey string) {
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return key, ""
	}
	return key[:i], key[i+1:]
}
