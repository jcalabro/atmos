package space

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

type sliceRecordStream struct {
	records  []SourceRecord
	idx      int
	nextErr  error
	closeErr error
	closed   bool
}

func (s *sliceRecordStream) Next(context.Context) (SourceRecord, error) {
	if s.idx == len(s.records) {
		if s.nextErr != nil {
			return SourceRecord{}, s.nextErr
		}
		return SourceRecord{}, io.EOF
	}
	record := s.records[s.idx]
	s.idx++
	return record, nil
}

func (s *sliceRecordStream) Close() error {
	s.closed = true
	return s.closeErr
}

func testSpoolLimits() SpoolLimits {
	return SpoolLimits{MaxRecords: 100, MaxRecordSize: 4096, MaxTotalSize: 64 << 10}
}

func spoolSourceRecords(t *testing.T) []SourceRecord {
	t.Helper()
	snapshot := testSnapshot(t)
	records := make([]SourceRecord, 0, len(snapshot.refs))
	for _, ref := range snapshot.refs {
		records = append(records, SourceRecord{
			RecordRef: ref,
			Body:      io.NopCloser(bytes.NewReader(snapshot.records[ref.Path])),
		})
	}
	return records
}

func TestFileSpoolProducesStableCanonicalSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	stream := &sliceRecordStream{records: spoolSourceRecords(t)}
	spool, err := SpoolRecords(context.Background(), dir, stream, testSpoolLimits())
	require.NoError(t, err)
	require.True(t, stream.closed)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })

	info, err := os.Stat(spool.Path())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	index, err := BuildRepoIndex(context.Background(), spool, testCARLimits())
	require.NoError(t, err)
	require.Equal(t, []RecordPath{"com.example.post/a", "com.example.longer/b"}, index.Paths())
	for _, path := range index.Paths() {
		first, err := spool.OpenRecord(context.Background(), path)
		require.NoError(t, err)
		firstBytes, err := io.ReadAll(first)
		require.NoError(t, err)
		require.NoError(t, first.Close())
		second, err := spool.OpenRecord(context.Background(), path)
		require.NoError(t, err)
		secondBytes, err := io.ReadAll(second)
		require.NoError(t, err)
		require.NoError(t, second.Close())
		require.Equal(t, firstBytes, secondBytes)
		require.Equal(t, index[path], cbor.ComputeCID(cbor.CodecDagCBOR, firstBytes))
	}

	path := spool.Path()
	require.NoError(t, spool.Close())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, spool.Close(), "Close must be idempotent")
	_, err = spool.OpenRecord(context.Background(), index.Paths()[0])
	require.Error(t, err)
}

func TestFileSpoolCleansUpEveryFailure(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	ref := snapshot.refs[0]
	newRecord := func() SourceRecord {
		return SourceRecord{RecordRef: ref, Body: io.NopCloser(bytes.NewReader(snapshot.records[ref.Path]))}
	}
	want := errors.New("injected")
	for _, tc := range []struct {
		name   string
		stream *sliceRecordStream
		limits SpoolLimits
	}{
		{name: "next", stream: &sliceRecordStream{nextErr: want}, limits: testSpoolLimits()},
		{name: "source close", stream: &sliceRecordStream{closeErr: want}, limits: testSpoolLimits()},
		{name: "record close", stream: &sliceRecordStream{records: []SourceRecord{{RecordRef: ref, Body: &errorCloseReader{Reader: bytes.NewReader(snapshot.records[ref.Path]), err: want}}}}, limits: testSpoolLimits()},
		{name: "duplicate", stream: &sliceRecordStream{records: []SourceRecord{newRecord(), newRecord()}}, limits: testSpoolLimits()},
		{name: "count", stream: &sliceRecordStream{records: spoolSourceRecords(t)}, limits: SpoolLimits{MaxRecords: 1, MaxRecordSize: 4096, MaxTotalSize: 4096}},
		{name: "record bytes", stream: &sliceRecordStream{records: spoolSourceRecords(t)}, limits: SpoolLimits{MaxRecords: 10, MaxRecordSize: 1, MaxTotalSize: 4096}},
		{name: "total bytes", stream: &sliceRecordStream{records: spoolSourceRecords(t)}, limits: SpoolLimits{MaxRecords: 10, MaxRecordSize: 4096, MaxTotalSize: 1}},
		{name: "bad cid", stream: &sliceRecordStream{records: []SourceRecord{{RecordRef: ref, Body: io.NopCloser(bytes.NewReader([]byte{0xa0}))}}}, limits: testSpoolLimits()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			_, err := SpoolRecords(context.Background(), dir, tc.stream, tc.limits)
			require.Error(t, err)
			entries, readErr := os.ReadDir(dir)
			require.NoError(t, readErr)
			require.Empty(t, entries, "failed spool left temporary data")
			require.True(t, tc.stream.closed)
		})
	}
}

func TestFileSpoolRequiresExplicitDirectoryAndLimits(t *testing.T) {
	t.Parallel()
	_, err := SpoolRecords(context.Background(), "", &sliceRecordStream{}, testSpoolLimits())
	require.Error(t, err)
	_, err = SpoolRecords(context.Background(), t.TempDir(), &sliceRecordStream{}, SpoolLimits{})
	require.Error(t, err)
	_, err = SpoolRecords(context.Background(), t.TempDir(), nil, testSpoolLimits())
	require.Error(t, err)
}

func TestFileSpoolCancellationAndConcurrentReplay(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := t.TempDir()
	stream := &sliceRecordStream{records: spoolSourceRecords(t)}
	_, err := SpoolRecords(ctx, dir, stream, testSpoolLimits())
	require.ErrorIs(t, err, context.Canceled)

	stream = &sliceRecordStream{records: spoolSourceRecords(t)}
	spool, err := SpoolRecords(context.Background(), dir, stream, testSpoolLimits())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })
	index, err := BuildRepoIndex(context.Background(), spool, testCARLimits())
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 128)
	for range 32 {
		for _, path := range index.Paths() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				body, openErr := spool.OpenRecord(context.Background(), path)
				if openErr != nil {
					errs <- openErr
					return
				}
				_, readErr := io.ReadAll(body)
				if readErr != nil {
					errs <- readErr
				}
				if closeErr := body.Close(); closeErr != nil {
					errs <- closeErr
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestFileSpoolPathStaysInsideRequestedDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	spool, err := SpoolRecords(context.Background(), dir, &sliceRecordStream{}, testSpoolLimits())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })
	rel, err := filepath.Rel(dir, spool.Path())
	require.NoError(t, err)
	require.NotEqual(t, "..", rel)
	require.NotContains(t, rel, string(filepath.Separator)+".."+string(filepath.Separator))
}

type errorCloseReader struct {
	io.Reader
	err error
}

func (r *errorCloseReader) Close() error { return r.err }
