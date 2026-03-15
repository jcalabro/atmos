// Package sync implements ATProto repository sync: streaming repo downloads,
// record iteration, commit verification, and repo enumeration.
package sync

import (
	atmos "github.com/jcalabro/atmos"
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
