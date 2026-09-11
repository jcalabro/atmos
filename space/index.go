package space

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
)

// RecordPath is the canonical collection/rkey coordinate used by a repo index.
type RecordPath string

// ParseRecordPath validates a collection/rkey path.
func ParseRecordPath(raw string) (RecordPath, error) {
	if strings.Count(raw, "/") != 1 {
		return "", fmt.Errorf("space: invalid record path %q", raw)
	}
	collection, rkey, _ := strings.Cut(raw, "/")
	if _, err := atmos.ParseNSID(collection); err != nil {
		return "", fmt.Errorf("space: invalid NSID in record path %q: %w", raw, err)
	}
	if _, err := atmos.ParseRecordKey(rkey); err != nil {
		return "", fmt.Errorf("space: invalid record key in path %q: %w", raw, err)
	}
	return RecordPath(raw), nil
}

// Validate checks the path syntax.
func (p RecordPath) Validate() error {
	_, err := ParseRecordPath(string(p))
	return err
}

// Collection returns the path's validated collection.
func (p RecordPath) Collection() atmos.NSID {
	collection, _, _ := strings.Cut(string(p), "/")
	return atmos.NSID(collection)
}

// RecordKey returns the path's validated record key.
func (p RecordPath) RecordKey() atmos.RecordKey {
	_, rkey, _ := strings.Cut(string(p), "/")
	return atmos.RecordKey(rkey)
}

// CanonicalPathLess reports DAG-CBOR text-key order: UTF-8 byte length, then
// bytewise lexical order.
func CanonicalPathLess(a, b RecordPath) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

// RepoIndex maps every record path to its current DAG-CBOR CID.
type RepoIndex map[RecordPath]cbor.CID

// Clone returns an independent copy.
func (idx RepoIndex) Clone() RepoIndex {
	out := make(RepoIndex, len(idx))
	for path, cid := range idx {
		out[path] = cid
	}
	return out
}

// Paths returns paths in canonical DAG-CBOR key order.
func (idx RepoIndex) Paths() []RecordPath {
	paths := make([]RecordPath, 0, len(idx))
	for path := range idx {
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool { return CanonicalPathLess(paths[i], paths[j]) })
	return paths
}

// EncodeRepoIndex returns the canonical DAG-CBOR index encoding.
func EncodeRepoIndex(index RepoIndex) ([]byte, error) {
	paths := index.Paths()
	capacity := 9
	for _, path := range paths {
		if len(path) > maxInt()-capacity-50 {
			return nil, errors.New("space: repo index exceeds platform allocation range")
		}
		capacity += len(path) + 50
	}
	buf := make([]byte, 0, capacity)
	buf = cbor.AppendMapHeader(buf, uint64(len(index)))
	for _, path := range paths {
		cid := index[path]
		if err := path.Validate(); err != nil {
			return nil, err
		}
		if !cid.Defined() || cid.Codec() != cbor.CodecDagCBOR {
			return nil, fmt.Errorf("space: index CID at %s must be a defined dag-cbor CID", path)
		}
		buf = cbor.AppendText(buf, string(path))
		buf = cbor.AppendCIDLink(buf, &cid)
	}
	return buf, nil
}

// DecodeRepoIndex validates and decodes a canonical DAG-CBOR index. maxRecords
// must be positive and bounds decoded cardinality.
func DecodeRepoIndex(data []byte, maxRecords int) (RepoIndex, error) {
	if maxRecords <= 0 {
		return nil, errors.New("space: max index records must be positive")
	}
	value, err := cbor.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("space: decode repo index: %w", err)
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("space: repo index must be a map")
	}
	if len(m) > maxRecords {
		return nil, fmt.Errorf("space: repo index has %d records, exceeds limit %d", len(m), maxRecords)
	}
	index := make(RepoIndex, len(m))
	for rawPath, rawCID := range m {
		path, err := ParseRecordPath(rawPath)
		if err != nil {
			return nil, err
		}
		cid, ok := rawCID.(cbor.CID)
		if !ok || !cid.Defined() || cid.Codec() != cbor.CodecDagCBOR {
			return nil, fmt.Errorf("space: index CID at %s must be a defined dag-cbor CID", path)
		}
		index[path] = cid
	}
	return index, nil
}

// NewRepoCommitFromIndex folds a validated repo index into an LtHash.
func NewRepoCommitFromIndex(index RepoIndex) (*RepoCommit, error) {
	repo := NewRepoCommit()
	for path, cid := range index {
		if err := repo.Add(path.Collection().String(), path.RecordKey().String(), cid); err != nil {
			return nil, err
		}
	}
	return repo, nil
}

// RecordRef is immutable snapshot metadata for one current record.
type RecordRef struct {
	Path RecordPath
	CID  cbor.CID
}

// RepoSnapshot is a stable, replayable repository snapshot. ForEach and every
// OpenRecord call must observe the same immutable generation. Implementations
// should page metadata and open values without holding a database write
// transaction across serialization or network I/O.
type RepoSnapshot interface {
	ForEach(ctx context.Context, yield func(RecordRef) error) error
	OpenRecord(ctx context.Context, path RecordPath) (io.ReadCloser, error)
}

// BuildRepoIndex enumerates and validates snapshot metadata without opening
// record bodies.
func BuildRepoIndex(ctx context.Context, snapshot RepoSnapshot, limits CARLimits) (RepoIndex, error) {
	if snapshot == nil {
		return nil, errors.New("space: repo snapshot is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	index := make(RepoIndex)
	err := snapshot.ForEach(ctx, func(ref RecordRef) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ref.Path.Validate(); err != nil {
			return err
		}
		if !ref.CID.Defined() || ref.CID.Codec() != cbor.CodecDagCBOR {
			return fmt.Errorf("space: snapshot CID at %s must be a defined dag-cbor CID", ref.Path)
		}
		if _, exists := index[ref.Path]; exists {
			return fmt.Errorf("space: duplicate snapshot path %s", ref.Path)
		}
		if len(index) >= limits.MaxRecords {
			return fmt.Errorf("space: snapshot exceeds record limit %d", limits.MaxRecords)
		}
		index[ref.Path] = ref.CID
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("space: enumerate repo snapshot: %w", err)
	}
	return index, nil
}
