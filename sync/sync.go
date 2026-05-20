// Package sync implements ATProto repository sync: streaming repo
// downloads, record iteration, commit verification, and repo
// enumeration.
//
// For consumers of the firehose (com.atproto.sync.subscribeRepos), this
// package also provides full Sync 1.1 verification via [Verifier]:
// per-commit MST inversion against prevData, per-DID (rev, data) chain
// tracking, signature verification with key-rotation handling, and
// transparent resync via getRepo when a chain break or inversion
// failure is detected. Pass a [*Verifier] in
// streaming.Options.Verifier to opt in.
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

// Options configures a sync client.
type Options struct {
	Client *xrpc.Client // required: points at PDS or relay

	// Directory enables commit signature verification via DID resolution.
	// None = signatures not verified.
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
