package space

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/jcalabro/atmos/cbor"
)

// SourceRecord transfers ownership of Body to SpoolRecords. Body is closed on
// every success and failure path.
type SourceRecord struct {
	RecordRef
	Body io.ReadCloser
}

// RecordStream is a one-pass source for a file-backed replayable snapshot.
// Next returns io.EOF at the clean terminus. SpoolRecords always calls Close.
type RecordStream interface {
	Next(ctx context.Context) (SourceRecord, error)
	Close() error
}

// SpoolLimits bounds temporary disk usage, per-record memory, and cardinality.
type SpoolLimits struct {
	MaxRecords    int
	MaxRecordSize uint64
	MaxTotalSize  uint64
}

// Validate rejects absent spool limits.
func (l SpoolLimits) Validate() error {
	if l.MaxRecords <= 0 {
		return errors.New("space: spool record limit must be positive")
	}
	if l.MaxRecordSize == 0 || l.MaxTotalSize == 0 {
		return errors.New("space: spool byte limits must be positive")
	}
	return nil
}

type spoolEntry struct {
	ref    RecordRef
	offset int64
	length int64
}

// FileSpool is a bounded, stable, replayable RepoSnapshot backed by one
// caller-located temporary file. Close removes the file and is idempotent.
type FileSpool struct {
	mu      sync.RWMutex
	path    string
	entries map[RecordPath]spoolEntry
	refs    []RecordRef
	closed  bool
}

// SpoolRecords consumes stream into a private file inside dir. It verifies
// canonical record CBOR and each declared CID before publishing the snapshot.
// No implicit system temporary directory is selected.
func SpoolRecords(ctx context.Context, dir string, stream RecordStream, limits SpoolLimits) (result *FileSpool, retErr error) {
	if stream == nil {
		return nil, errors.New("space: record stream is nil")
	}
	var (
		file      *os.File
		path      string
		published bool
	)
	// Register cleanup before stream closure so LIFO execution lets a stream
	// close failure invalidate and remove an otherwise completed spool.
	defer func() {
		if file == nil || (published && retErr == nil) {
			return
		}
		_ = file.Close()
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("space: remove failed record spool: %w", removeErr))
		}
		result = nil
	}()
	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("space: close record stream: %w", closeErr))
			result = nil
		}
	}()
	if dir == "" {
		return nil, errors.New("space: spool directory must be explicit")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	var err error
	file, err = os.CreateTemp(dir, ".atmos-space-spool-*")
	if err != nil {
		return nil, fmt.Errorf("space: create record spool: %w", err)
	}
	path = file.Name()

	entries := make(map[RecordPath]spoolEntry)
	var (
		refs   []RecordRef
		offset int64
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, err := stream.Next(ctx)
		if err != nil {
			if record.Body != nil {
				closeErr := record.Body.Close()
				if errors.Is(err, io.EOF) {
					return nil, errors.Join(errors.New("space: record stream returned a body with EOF"), closeErr)
				}
				return nil, errors.Join(fmt.Errorf("space: read record stream: %w", err), closeErr)
			}
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("space: read record stream: %w", err)
		}
		if record.Body == nil {
			return nil, errors.New("space: source record body is nil")
		}
		if err := record.Path.Validate(); err != nil {
			_ = record.Body.Close()
			return nil, err
		}
		if !record.CID.Defined() || record.CID.Codec() != cbor.CodecDagCBOR {
			_ = record.Body.Close()
			return nil, fmt.Errorf("space: source CID at %s must be a defined dag-cbor CID", record.Path)
		}
		if _, exists := entries[record.Path]; exists {
			_ = record.Body.Close()
			return nil, fmt.Errorf("space: duplicate source record path %s", record.Path)
		}
		if len(entries) >= limits.MaxRecords {
			_ = record.Body.Close()
			return nil, fmt.Errorf("space: record stream exceeds record limit %d", limits.MaxRecords)
		}
		data, readErr := readAllBounded(record.Body, limits.MaxRecordSize)
		closeErr := record.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("space: read source record %s: %w", record.Path, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("space: close source record %s: %w", record.Path, closeErr)
		}
		if err := validateRecordData(data); err != nil {
			return nil, fmt.Errorf("space: invalid source record %s: %w", record.Path, err)
		}
		actualCID := cbor.ComputeCID(cbor.CodecDagCBOR, data)
		if !actualCID.Equal(record.CID) {
			return nil, fmt.Errorf("space: source record %s CID mismatch: expected %s, got %s", record.Path, record.CID, actualCID)
		}
		if uint64(offset) > limits.MaxTotalSize || uint64(len(data)) > limits.MaxTotalSize-uint64(offset) {
			return nil, fmt.Errorf("space: record spool exceeds total byte limit %d", limits.MaxTotalSize)
		}
		if err := writeFull(file, data); err != nil {
			return nil, fmt.Errorf("space: write source record %s: %w", record.Path, err)
		}
		entry := spoolEntry{ref: record.RecordRef, offset: offset, length: int64(len(data))}
		entries[record.Path] = entry
		refs = append(refs, record.RecordRef)
		offset += int64(len(data))
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("space: sync record spool: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("space: close record spool: %w", err)
	}
	sort.Slice(refs, func(i, j int) bool { return CanonicalPathLess(refs[i].Path, refs[j].Path) })
	result = &FileSpool{path: path, entries: entries, refs: refs}
	published = true
	return result, nil
}

// Path returns the spool's filesystem path for observability and accounting.
func (s *FileSpool) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// ForEach implements RepoSnapshot with canonical ordering.
func (s *FileSpool) ForEach(ctx context.Context, yield func(RecordRef) error) error {
	if s == nil {
		return errors.New("space: file spool is nil")
	}
	if yield == nil {
		return errors.New("space: file spool yield callback is nil")
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("space: file spool is closed")
	}
	refs := append([]RecordRef(nil), s.refs...)
	s.mu.RUnlock()
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := yield(ref); err != nil {
			return err
		}
	}
	return nil
}

// OpenRecord implements RepoSnapshot. Each call opens an independent reader,
// so concurrent replays do not share offsets.
func (s *FileSpool) OpenRecord(ctx context.Context, path RecordPath) (io.ReadCloser, error) {
	if s == nil {
		return nil, errors.New("space: file spool is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("space: file spool is closed")
	}
	entry, ok := s.entries[path]
	if !ok {
		return nil, fmt.Errorf("space: record %s is absent from spool", path)
	}
	file, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("space: open record spool: %w", err)
	}
	return &sectionReadCloser{Reader: io.NewSectionReader(file, entry.offset, entry.length), closer: file}, nil
}

// Close removes the spool. Readers opened before Close retain their file
// descriptor and may finish; no new readers can open after Close begins.
func (s *FileSpool) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("space: remove record spool: %w", err)
	}
	s.closed = true
	return nil
}

type sectionReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *sectionReadCloser) Close() error { return r.closer.Close() }

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
