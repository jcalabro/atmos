package streaming

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
)

// Action is the type of record mutation.
type Action string

const (
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
	ActionResync Action = "resync"
)

// CBORUnmarshaler is implemented by all generated ATProto types.
type CBORUnmarshaler interface {
	UnmarshalCBOR([]byte) error
}

// Operation is a single record mutation from a #commit or #sync event,
// with convenient access to the underlying record data.
type Operation struct {
	Action     Action // ActionCreate, ActionUpdate, ActionDelete, or ActionResync
	Collection string // e.g. "app.bsky.feed.post"
	RKey       string // record key, e.g. "3abc123"
	Repo       string // DID of the repo
	Rev        string // commit revision
	CID        []byte // raw CID bytes of the new record (nil for deletes)
	blockData  []byte // CBOR bytes of the record block (nil for deletes)
}

// Decode unmarshals the record block into dst. Returns an error for delete
// ops which have no record data.
func (o *Operation) Decode(dst CBORUnmarshaler) error {
	if o.blockData == nil {
		return errors.New("no record data (delete operation)")
	}
	return dst.UnmarshalCBOR(o.blockData)
}

// Operations returns an iterator over record mutations in this event.
// For #commit events, the CAR diff is decoded and each repo operation is
// yielded. For #sync events (when a [sync.Client] is configured), the full
// repository is fetched via HTTP and every record is yielded as an
// [ActionResync] operation. For all other event types, yields nothing.
//
// Each call re-decodes the CAR (#commit) or re-fetches the repo (#sync);
// callers that need to iterate multiple times should collect the results.
func (e *Event) Operations() iter.Seq2[Operation, error] {
	return func(yield func(Operation, error) bool) {
		if e.Commit != nil {
			e.yieldCommitOps(yield)
			return
		}
		if e.Sync != nil && e.syncClient != nil {
			e.yieldResyncOps(yield)
			return
		}
	}
}

// yieldCommitOps yields operations from a #commit event's CAR diff.
func (e *Event) yieldCommitOps(yield func(Operation, error) bool) {
	commit := e.Commit

	// Decode the CAR once for all ops.
	_, blocks, err := car.ReadAll(bytes.NewReader(commit.Blocks))
	if err != nil {
		yield(Operation{}, err)
		return
	}

	// Index blocks by CID string for fast lookup.
	blockIdx := make(map[string][]byte, len(blocks))
	for _, b := range blocks {
		blockIdx[b.CID.String()] = b.Data
	}

	for _, op := range commit.Ops {
		collection, rkey := splitPath(op.Path)

		o := Operation{
			Action:     Action(op.Action),
			Collection: collection,
			RKey:       rkey,
			Repo:       commit.Repo,
			Rev:        commit.Rev,
		}

		if op.CID.HasVal() {
			cidLink := op.CID.Val().Link
			o.CID = []byte(cidLink)
			o.blockData = blockIdx[cidLink]
		}

		if !yield(o, nil) {
			return
		}
	}
}

// yieldResyncOps fetches the full repo for a #sync event and yields every
// record as an ActionResync operation.
func (e *Event) yieldResyncOps(yield func(Operation, error) bool) {
	ctx := e.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	did, err := atmos.ParseDID(e.Sync.DID)
	if err != nil {
		_ = yield(Operation{}, fmt.Errorf("resync: parse DID: %w", err))
		return
	}

	for rec, err := range e.syncClient.IterRecords(ctx, did) {
		if err != nil {
			if !yield(Operation{}, fmt.Errorf("resync: %w", err)) {
				return
			}
			continue
		}

		op := Operation{
			Action:     ActionResync,
			Collection: rec.Collection,
			RKey:       rec.RKey,
			Repo:       e.Sync.DID,
			Rev:        e.Sync.Rev,
			CID:        rec.CID.Bytes(),
			blockData:  rec.Data,
		}

		if !yield(op, nil) {
			return
		}
	}
}

// RecordDecoder decodes a CBOR record by collection NSID. Each generated
// api package (bsky, comatproto, etc.) provides a DecodeRecord function
// matching this signature.
type RecordDecoder func(collection string, data []byte) (any, error)

// Record decodes the operation's record using the provided decoder and
// returns the result as an any (actually a pointer to a generated type).
// Use a type switch to handle specific record types.
func (o *Operation) Record(decode RecordDecoder) (any, error) {
	if o.blockData == nil {
		return nil, errors.New("no record data (delete operation)")
	}
	return decode(o.Collection, o.blockData)
}

// ChainDecoders combines multiple RecordDecoders into one. Each decoder is
// tried in order until one succeeds. A decoder that returns an error
// containing "unknown collection" is skipped; any other error is returned
// immediately. Useful when consuming records from multiple lexicon packages.
func ChainDecoders(decoders ...RecordDecoder) RecordDecoder {
	return func(collection string, data []byte) (any, error) {
		for _, dec := range decoders {
			v, err := dec(collection, data)
			if err == nil {
				return v, nil
			}
			if !strings.Contains(err.Error(), "unknown collection") {
				return nil, err
			}
		}
		return nil, fmt.Errorf("unknown collection: %s", collection)
	}
}

// splitPath splits "app.bsky.feed.post/3abc" into ("app.bsky.feed.post", "3abc").
func splitPath(path string) (collection, rkey string) {
	i := strings.LastIndexByte(path, '/')
	if i < 0 {
		return path, ""
	}
	return path[:i], path[i+1:]
}
