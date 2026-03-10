package streaming

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/jcalabro/atmos/car"
)

// Action is the type of record mutation.
type Action string

const (
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
)

// CBORUnmarshaler is implemented by all generated ATProto types.
type CBORUnmarshaler interface {
	UnmarshalCBOR([]byte) error
}

// Operation is a single record mutation from a commit event, with
// convenient access to the underlying record data.
type Operation struct {
	Action     Action // ActionCreate, ActionUpdate, or ActionDelete
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
// For non-commit events, yields nothing. The CAR archive is decoded
// each time this method is called; callers that need to iterate multiple
// times should collect the results.
func (e *Event) Operations() iter.Seq2[Operation, error] {
	return func(yield func(Operation, error) bool) {
		if e.Commit == nil {
			return
		}

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
