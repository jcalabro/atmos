package streaming

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
)

// Action is the type of record mutation. Aliases [atmos.Action] so
// streaming consumers and verifier consumers use the same type at the
// boundary — no string-cast required when passing ops between layers.
type Action = atmos.Action

// Action constants. Re-exported from package atmos for ergonomic
// access via streaming.ActionCreate etc.
const (
	ActionCreate = atmos.ActionCreate
	ActionUpdate = atmos.ActionUpdate
	ActionDelete = atmos.ActionDelete
	ActionResync = atmos.ActionResync
)

// CBORUnmarshaler is implemented by all generated ATProto types.
type CBORUnmarshaler interface {
	UnmarshalCBOR([]byte) error
}

// Operation is a single record mutation from a #commit or #sync event,
// with convenient access to the underlying record data.
//
// CID identifies the new record block for create/update/resync ops.
// For delete ops the CID is the zero value (use [cbor.CID.Defined] to
// check). Jetstream commits carry the CID as a base32 string in the
// JSON payload; on parse failure the field is left undefined and an
// error is yielded alongside the op.
//
// Strongly-typed fields (Collection, RKey, Repo, Rev) carry the
// canonical ATproto syntax types so consumers can call methods like
// [atmos.TID.Time] or [atmos.DID.Method] without re-parsing. The
// streaming layer does NOT re-validate these against the type's
// strict syntax — the values come from the upstream PDS and from
// repo path splits, and a malformed wire value yields a malformed
// typed value. Consumers that need strict syntax checks should call
// the corresponding Parse* function on the field.
type Operation struct {
	Action     Action          // ActionCreate, ActionUpdate, ActionDelete, or ActionResync
	Collection atmos.NSID      // e.g. "app.bsky.feed.post"
	RKey       atmos.RecordKey // record key, e.g. "3abc123"
	Repo       atmos.DID       // DID of the repo
	Rev        atmos.TID       // commit revision
	CID        cbor.CID        // record content hash; zero for deletes
	blockData  []byte          // CBOR bytes of the record block (nil for deletes)
}

// BlockData returns the raw CBOR bytes of the record block, or nil for
// delete operations which have no record data.
func (o *Operation) BlockData() []byte {
	return o.blockData
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
		if e.verifierRan {
			for _, op := range e.verifiedOps {
				if !yield(op, nil) {
					return
				}
			}
			return
		}
		if e.Commit != nil {
			e.yieldCommitOps(yield)
			return
		}
		if e.Jetstream != nil && e.Jetstream.Commit != nil {
			e.yieldJetstreamOp(yield)
			return
		}
		if e.Sync != nil && e.syncClient != nil {
			e.yieldResyncOps(yield)
			return
		}
	}
}

// yieldJetstreamOp yields a single operation from a Jetstream commit event.
// Note: blockData is nil because Jetstream records are JSON (not CBOR), so
// [Operation.Decode] will return an error. Use [JetstreamCommit.Record]
// directly for the raw JSON payload.
//
// If the Jetstream commit's CID string fails to parse, the op is yielded
// with an undefined CID alongside a parse error so callers can decide
// whether to skip the event or surface it.
func (e *Event) yieldJetstreamOp(yield func(Operation, error) bool) {
	jc := e.Jetstream.Commit
	op := Operation{
		Action:     Action(jc.Operation),
		Collection: atmos.NSID(jc.Collection),
		RKey:       atmos.RecordKey(jc.RKey),
		Repo:       atmos.DID(e.Jetstream.DID),
		Rev:        atmos.TID(jc.Rev),
	}
	if jc.CID != "" {
		cid, err := cbor.ParseCIDString(jc.CID)
		if err != nil {
			yield(op, fmt.Errorf("jetstream: parse CID %q: %w", jc.CID, err))
			return
		}
		op.CID = cid
	}
	yield(op, nil)
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

	// Index blocks by CID for fast lookup.
	blockIdx := make(map[cbor.CID][]byte, len(blocks))
	for _, b := range blocks {
		blockIdx[b.CID] = b.Data
	}

	for _, op := range commit.Ops {
		collection, rkey := repo.SplitMSTKey(op.Path)

		o := Operation{
			Action:     Action(op.Action),
			Collection: atmos.NSID(collection),
			RKey:       atmos.RecordKey(rkey),
			Repo:       atmos.DID(commit.Repo),
			Rev:        atmos.TID(commit.Rev),
		}

		if op.CID.HasVal() {
			cid, err := cbor.ParseCIDString(op.CID.Val().Link)
			if err != nil {
				if !yield(o, fmt.Errorf("op %s: parse CID: %w", op.Path, err)) {
					return
				}
				continue
			}
			o.CID = cid
			o.blockData = blockIdx[cid]
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
			Collection: atmos.NSID(rec.Collection),
			RKey:       atmos.RecordKey(rec.RKey),
			Repo:       atmos.DID(e.Sync.DID),
			Rev:        atmos.TID(e.Sync.Rev),
			CID:        rec.CID,
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
	return decode(string(o.Collection), o.blockData)
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
