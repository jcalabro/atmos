package space

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

type memorySnapshot struct {
	refs    []RecordRef
	records map[RecordPath][]byte
	openErr error
}

func (s *memorySnapshot) ForEach(_ context.Context, yield func(RecordRef) error) error {
	for _, ref := range s.refs {
		if err := yield(ref); err != nil {
			return err
		}
	}
	return nil
}

func (s *memorySnapshot) OpenRecord(_ context.Context, path RecordPath) (io.ReadCloser, error) {
	if s.openErr != nil {
		return nil, s.openErr
	}
	data, ok := s.records[path]
	if !ok {
		return nil, fmt.Errorf("missing %s", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func testCARLimits() CARLimits {
	return CARLimits{
		MaxHeaderSize: 4096,
		MaxCommitSize: 4096,
		MaxIndexSize:  4 << 20,
		MaxRecordSize: 1 << 20,
		MaxTotalSize:  64 << 20,
		MaxRecords:    100_000,
	}
}

func testSnapshot(t *testing.T) *memorySnapshot {
	t.Helper()
	data := cbor.AppendMapHeader(nil, 1)
	data = cbor.AppendText(data, "$type")
	data = cbor.AppendText(data, "com.example.post")
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	pathA, err := ParseRecordPath("com.example.post/a")
	require.NoError(t, err)
	pathB, err := ParseRecordPath("com.example.longer/b")
	require.NoError(t, err)
	return &memorySnapshot{
		refs: []RecordRef{
			{Path: pathB, CID: cid},
			{Path: pathA, CID: cid},
		},
		records: map[RecordPath][]byte{pathA: data, pathB: data},
	}
}

func signedSnapshot(t *testing.T, snapshot RepoSnapshot, key atmoscrypto.PrivateKey) (SignedCommit, CommitContext) {
	t.Helper()
	index, err := BuildRepoIndex(context.Background(), snapshot, testCARLimits())
	require.NoError(t, err)
	repo, err := NewRepoCommitFromIndex(index)
	require.NoError(t, err)
	ctx := testCommitContext(t)
	commit, err := repo.SignWithRandom(ctx, key, bytes.NewReader(bytes.Repeat([]byte{0x33}, CommitIKMSize)))
	require.NoError(t, err)
	return commit, ctx
}

func serializeTestRepo(t *testing.T, mode CARMode) ([]byte, SignedCommit, CommitContext, atmoscrypto.PrivateKey) {
	t.Helper()
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	snapshot := testSnapshot(t)
	commit, ctx := signedSnapshot(t, snapshot, key)
	var buf bytes.Buffer
	err = SerializeRepoCAR(context.Background(), &buf, commit, snapshot, mode, testCARLimits())
	require.NoError(t, err)
	return buf.Bytes(), commit, ctx, key
}

func TestRepoCARFullRoundTripAndDuplicateCIDs(t *testing.T) {
	t.Parallel()
	encoded, commit, commitCtx, key := serializeTestRepo(t, CARFull)
	var records []VerifiedRecord
	verified, err := VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
		Context: commitCtx,
		Key:     key.PublicKey(),
		Mode:    CARFull,
		Limits:  testCARLimits(),
	}, func(record VerifiedRecord) error {
		records = append(records, record.Clone())
		return nil
	})
	require.NoError(t, err)
	require.True(t, verified.Complete)
	require.Equal(t, commit, verified.Commit.SignedCommit)
	require.Len(t, records, 2)
	require.Equal(t, records[0].CID, records[1].CID)
	require.NotEqual(t, records[0].Path, records[1].Path)

	reader, err := car.NewReaderWithOptions(bytes.NewReader(encoded), car.ReaderOptions{
		MaxHeaderSize: 4096,
		MaxBlockSize:  testCARLimits().MaxIndexSize,
	})
	require.NoError(t, err)
	var blocks []car.Block
	for {
		block, readErr := reader.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
		blocks = append(blocks, block)
	}
	require.Len(t, blocks, 4, "same CID at two paths still requires two positional blocks")
	require.Equal(t, blocks[2].CID, blocks[3].CID)
}

func TestRepoCARCanonicalIndexAndBlockOrder(t *testing.T) {
	t.Parallel()
	encoded, _, _, _ := serializeTestRepo(t, CARFull)
	r, err := car.NewReaderWithOptions(bytes.NewReader(encoded), car.ReaderOptions{MaxHeaderSize: 4096, MaxBlockSize: 4 << 20})
	require.NoError(t, err)
	_, err = r.Next()
	require.NoError(t, err)
	indexBlock, err := r.Next()
	require.NoError(t, err)
	index, err := DecodeRepoIndex(indexBlock.Data, 10)
	require.NoError(t, err)
	paths := index.Paths()
	require.Equal(t, []RecordPath{"com.example.post/a", "com.example.longer/b"}, paths)
	for _, path := range paths {
		block, readErr := r.Next()
		require.NoError(t, readErr)
		require.Equal(t, index[path], block.CID)
	}
}

func TestRepoCARIndexOnlyIsExplicit(t *testing.T) {
	t.Parallel()
	indexOnly, _, ctx, key := serializeTestRepo(t, CARIndexOnly)
	_, err := VerifyRepoCAR(context.Background(), bytes.NewReader(indexOnly), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "missing")

	verified, err := VerifyRepoCAR(context.Background(), bytes.NewReader(indexOnly), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARIndexOnly, Limits: testCARLimits(),
	}, nil)
	require.NoError(t, err)
	require.False(t, verified.Complete)

	full, _, fullCtx, fullKey := serializeTestRepo(t, CARFull)
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(full), VerifyRepoOptions{
		Context: fullCtx, Key: fullKey.PublicKey(), Mode: CARIndexOnly, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "record block")
}

func TestRepoCAREveryTruncationFailsFullVerification(t *testing.T) {
	t.Parallel()
	encoded, _, ctx, key := serializeTestRepo(t, CARFull)
	for n := range len(encoded) {
		_, err := VerifyRepoCAR(context.Background(), bytes.NewReader(encoded[:n]), VerifyRepoOptions{
			Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
		}, nil)
		require.Error(t, err, "prefix length %d was accepted", n)
	}
}

func TestRepoCARRejectsExtraAndReorderedBlocks(t *testing.T) {
	t.Parallel()
	encoded, _, ctx, key := serializeTestRepo(t, CARFull)
	header, blocks, err := car.ReadAll(bytes.NewReader(encoded))
	require.NoError(t, err)

	withExtra := append([]car.Block(nil), blocks...)
	withExtra = append(withExtra, blocks[len(blocks)-1])
	var extra bytes.Buffer
	require.NoError(t, car.WriteAll(&extra, header.Roots, withExtra))
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(extra.Bytes()), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "extra")

	blocks[0], blocks[1] = blocks[1], blocks[0]
	var reordered bytes.Buffer
	require.NoError(t, car.WriteAll(&reordered, header.Roots, blocks))
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(reordered.Bytes()), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "commit block")
}

func TestRepoCARLargeIndexUsesReaderLocalLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("constructs and verifies a >1 MiB repo index")
	}
	t.Parallel()

	const count = 20_000
	data := cbor.AppendMapHeader(nil, 0)
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	snapshot := &memorySnapshot{records: make(map[RecordPath][]byte, count)}
	for i := range count {
		path, err := ParseRecordPath(fmt.Sprintf("com.example.post/%05d", i))
		require.NoError(t, err)
		snapshot.refs = append(snapshot.refs, RecordRef{Path: path, CID: cid})
		snapshot.records[path] = data
	}
	key, err := atmoscrypto.GenerateK256()
	require.NoError(t, err)
	limits := testCARLimits()
	commit, ctx := signedSnapshot(t, snapshot, key)
	var encoded bytes.Buffer
	require.NoError(t, SerializeRepoCAR(context.Background(), &encoded, commit, snapshot, CARIndexOnly, limits))

	publicReader, err := car.NewReader(bytes.NewReader(encoded.Bytes()))
	require.NoError(t, err)
	_, err = publicReader.Next()
	require.NoError(t, err)
	_, err = publicReader.Next()
	var limitErr *car.LimitError
	require.ErrorAs(t, err, &limitErr)

	verified, err := VerifyRepoCAR(context.Background(), bytes.NewReader(encoded.Bytes()), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARIndexOnly, Limits: limits,
	}, nil)
	require.NoError(t, err)
	require.Len(t, verified.Index, count)
}

func TestRepoCARLimitsAndStagedCallback(t *testing.T) {
	t.Parallel()
	encoded, _, ctx, key := serializeTestRepo(t, CARFull)
	base := testCARLimits()
	for _, tc := range []struct {
		name string
		mut  func(*CARLimits)
	}{
		{name: "header", mut: func(l *CARLimits) { l.MaxHeaderSize = 1 }},
		{name: "commit", mut: func(l *CARLimits) { l.MaxCommitSize = 1 }},
		{name: "index", mut: func(l *CARLimits) { l.MaxIndexSize = 1 }},
		{name: "record", mut: func(l *CARLimits) { l.MaxRecordSize = 1 }},
		{name: "total", mut: func(l *CARLimits) { l.MaxTotalSize = uint64(len(encoded) - 1) }},
		{name: "count", mut: func(l *CARLimits) { l.MaxRecords = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits := base
			tc.mut(&limits)
			_, err := VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
				Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: limits,
			}, nil)
			require.Error(t, err)
		})
	}

	want := errors.New("staging failed")
	calls := 0
	_, err := VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: base,
	}, func(VerifiedRecord) error {
		calls++
		return want
	})
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, calls)
}

func TestSerializeRepoCAREnforcesHeaderLimitBeforeWriting(t *testing.T) {
	t.Parallel()
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	snapshot := testSnapshot(t)
	commit, _ := signedSnapshot(t, snapshot, key)
	limits := testCARLimits()
	limits.MaxHeaderSize = 1
	var output bytes.Buffer
	err = SerializeRepoCAR(context.Background(), &output, commit, snapshot, CARIndexOnly, limits)
	require.ErrorContains(t, err, "header")
	require.Zero(t, output.Len())
}

func TestRecordPathAndIndexValidation(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", "only", "/key", "com.example.post/", "bad/key", "com.example.post/a/b", "com.example.post/.."} {
		_, err := ParseRecordPath(raw)
		require.Error(t, err, raw)
	}

	valid, err := ParseRecordPath("com.example.post/a")
	require.NoError(t, err)
	rawCID := cbor.ComputeCID(cbor.CodecRaw, []byte("raw"))
	_, err = EncodeRepoIndex(RepoIndex{valid: rawCID})
	require.ErrorContains(t, err, "dag-cbor")
}

func TestBuildRepoIndexRejectsDuplicatesAndUnstableSnapshot(t *testing.T) {
	t.Parallel()
	snapshot := testSnapshot(t)
	snapshot.refs = append(snapshot.refs, snapshot.refs[0])
	_, err := BuildRepoIndex(context.Background(), snapshot, testCARLimits())
	require.ErrorContains(t, err, "duplicate")

	snapshot = testSnapshot(t)
	commit, _ := signedSnapshot(t, snapshot, mustP256(t))
	delete(snapshot.records, snapshot.refs[0].Path)
	var out bytes.Buffer
	err = SerializeRepoCAR(context.Background(), &out, commit, snapshot, CARFull, testCARLimits())
	require.ErrorContains(t, err, "missing")
}

func mustP256(t *testing.T) atmoscrypto.PrivateKey {
	t.Helper()
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	return key
}

func TestRepoIndexPathsCanonical(t *testing.T) {
	t.Parallel()
	paths := []string{"com.example.post/zz", "com.example.long/a", "com.example.post/a"}
	index := make(RepoIndex, len(paths))
	for _, raw := range paths {
		path, err := ParseRecordPath(raw)
		require.NoError(t, err)
		index[path] = cbor.ComputeCID(cbor.CodecDagCBOR, []byte(raw))
	}
	got := index.Paths()
	require.True(t, sort.SliceIsSorted(got, func(i, j int) bool { return CanonicalPathLess(got[i], got[j]) }))
}

func FuzzVerifyRepoCAR(f *testing.F) {
	ctx, err := NewCommitContext(
		"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.forum/3jzfcijpj2z2a",
		"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
		"3jzfcijpj2z2a",
	)
	if err != nil {
		f.Fatal(err)
	}
	key, err := atmoscrypto.ParsePrivateP256(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		f.Fatal(err)
	}
	valid, err := os.ReadFile("testdata/repo-p256.car")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = VerifyRepoCAR(context.Background(), bytes.NewReader(data), VerifyRepoOptions{
			Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
		}, nil)
	})
}
