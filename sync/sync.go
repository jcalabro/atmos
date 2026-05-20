// Package sync implements AT Protocol repository sync: streaming repo
// downloads, record iteration, commit verification, and repo
// enumeration.
//
// For firehose consumers (com.atproto.sync.subscribeRepos), [Verifier]
// implements the full Sync 1.1 validation pipeline: MST inversion
// against prevData, per-DID chain tracking, signature verification
// with key-rotation handling, op-CID consistency, and transparent
// resync via getRepo on failure. Construct one with [NewVerifier] and
// pass it in streaming.Options.Verifier.
//
// Most [VerifierOptions] fields are wrapped in gt.Option so an unset
// option (the zero value) can be distinguished from a deliberately
// chosen value. The defaults (PolicyResync, LegacyAccept, 5-minute
// future-rev tolerance, 5/min resync rate limit with burst 5) are
// suitable for most consumer applications.
package sync

import (
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// Record is a single record yielded during repo iteration.
type Record struct {
	Collection string   // e.g. "app.bsky.feed.post"
	RKey       string   // record key
	CID        cbor.CID // content hash
	Data       []byte   // raw DAG-CBOR bytes
	Rev        string   // commit rev from the repo download
}

// ListReposEntry is a single repo from ListRepos pagination.
type ListReposEntry struct {
	DID    atmos.DID
	Rev    string
	Head   string
	Active bool
}

// ListReposPage is one page of results yielded by Client.ListRepos.
// NextCursor is the cursor a caller can pass on a subsequent ListRepos
// call to resume past this page; empty when the relay reports there
// are no more pages.
//
// Per-entry parse errors during pagination still surface as the err
// argument of the iter.Seq2 yield alongside an empty Entries slice.
type ListReposPage struct {
	Entries    []ListReposEntry
	NextCursor string
}

// Options configures a sync Client.
type Options struct {
	// Client points at the PDS or relay. Required.
	Client *xrpc.Client

	// Directory enables commit signature verification via DID
	// resolution. None disables signature checks.
	Directory gt.Option[*identity.Directory]
}

// Client performs sync operations against a PDS or relay.
type Client struct {
	opts Options
}

// NewClient creates a new sync client.
func NewClient(opts Options) *Client {
	return &Client{opts: opts}
}
