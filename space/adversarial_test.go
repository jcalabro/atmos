package space

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

func writeCustomRepoCAR(t *testing.T, commit SignedCommit, indexBytes []byte, records []car.Block) []byte {
	t.Helper()
	commitBytes, err := commit.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	indexCID := cbor.ComputeCID(cbor.CodecDagCBOR, indexBytes)
	blocks := []car.Block{{CID: commitCID, Data: commitBytes}, {CID: indexCID, Data: indexBytes}}
	blocks = append(blocks, records...)
	var out bytes.Buffer
	require.NoError(t, car.WriteAll(&out, []cbor.CID{commitCID, indexCID}, blocks))
	return out.Bytes()
}

func signIndex(t *testing.T, index RepoIndex) (SignedCommit, CommitContext, atmoscrypto.PrivateKey) {
	t.Helper()
	repo, err := NewRepoCommitFromIndex(index)
	require.NoError(t, err)
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	ctx := testCommitContext(t)
	commit, err := repo.SignWithRandom(ctx, key, bytes.NewReader(bytes.Repeat([]byte{0x72}, 32)))
	require.NoError(t, err)
	return commit, ctx, key
}

func TestRepoCARRejectsReorderedDistinctRecords(t *testing.T) {
	t.Parallel()
	pathA, err := ParseRecordPath("com.example.post/a")
	require.NoError(t, err)
	pathB, err := ParseRecordPath("com.example.post/b")
	require.NoError(t, err)
	dataA := cbor.AppendMapHeader(nil, 0)
	dataB := cbor.AppendMapHeader(nil, 1)
	dataB = cbor.AppendText(dataB, "x")
	dataB = cbor.AppendInt(dataB, 1)
	cidA := cbor.ComputeCID(cbor.CodecDagCBOR, dataA)
	cidB := cbor.ComputeCID(cbor.CodecDagCBOR, dataB)
	index := RepoIndex{pathA: cidA, pathB: cidB}
	commit, ctx, key := signIndex(t, index)
	indexBytes, err := EncodeRepoIndex(index)
	require.NoError(t, err)
	encoded := writeCustomRepoCAR(t, commit, indexBytes, []car.Block{{CID: cidB, Data: dataB}, {CID: cidA, Data: dataA}})
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "expected block")
}

func TestRepoCARRejectsNonMapRecord(t *testing.T) {
	t.Parallel()
	path, err := ParseRecordPath("com.example.post/a")
	require.NoError(t, err)
	data := cbor.AppendText(nil, "not a record")
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	index := RepoIndex{path: cid}
	commit, ctx, key := signIndex(t, index)
	indexBytes, err := EncodeRepoIndex(index)
	require.NoError(t, err)
	encoded := writeCustomRepoCAR(t, commit, indexBytes, []car.Block{{CID: cid, Data: data}})
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "must be a DAG-CBOR map")
}

func TestRepoCARRejectsMalformedIndexEncodings(t *testing.T) {
	t.Parallel()
	pathA := RecordPath("com.example.post/a")
	pathB := RecordPath("com.example.post/b")
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, cbor.AppendMapHeader(nil, 0))
	commit, ctx, key := signIndex(t, nil)
	for _, tc := range []struct {
		name  string
		index []byte
		want  string
	}{
		{
			name: "duplicate path",
			index: func() []byte {
				buf := cbor.AppendMapHeader(nil, 2)
				buf = cbor.AppendText(buf, string(pathA))
				buf = cbor.AppendCIDLink(buf, &cid)
				buf = cbor.AppendText(buf, string(pathA))
				return cbor.AppendCIDLink(buf, &cid)
			}(),
			want: "duplicate map key",
		},
		{
			name: "noncanonical order",
			index: func() []byte {
				buf := cbor.AppendMapHeader(nil, 2)
				buf = cbor.AppendText(buf, string(pathB))
				buf = cbor.AppendCIDLink(buf, &cid)
				buf = cbor.AppendText(buf, string(pathA))
				return cbor.AppendCIDLink(buf, &cid)
			}(),
			want: "not sorted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded := writeCustomRepoCAR(t, commit, tc.index, nil)
			_, err := VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
				Context: ctx, Key: key.PublicKey(), Mode: CARIndexOnly, Limits: testCARLimits(),
			}, nil)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestRepoCARRejectsRootMismatchAndTrailingGarbage(t *testing.T) {
	t.Parallel()
	encoded, _, ctx, key := serializeTestRepo(t, CARFull)
	header, blocks, err := car.ReadAll(bytes.NewReader(encoded))
	require.NoError(t, err)
	header.Roots[0] = cbor.ComputeCID(cbor.CodecDagCBOR, []byte("other"))
	var mismatch bytes.Buffer
	require.NoError(t, car.WriteAll(&mismatch, header.Roots, blocks))
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(mismatch.Bytes()), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.ErrorContains(t, err, "commit block")

	encoded = append(encoded, 0)
	_, err = VerifyRepoCAR(context.Background(), bytes.NewReader(encoded), VerifyRepoOptions{
		Context: ctx, Key: key.PublicKey(), Mode: CARFull, Limits: testCARLimits(),
	}, nil)
	require.Error(t, err)
	var verificationErr *VerificationError
	require.ErrorAs(t, err, &verificationErr)
	require.NotEmpty(t, verificationErr.Stage)
}

func TestRepoCommitOperationRoundTrip(t *testing.T) {
	t.Parallel()
	dataA := cbor.AppendMapHeader(nil, 0)
	dataB := cbor.AppendMapHeader(nil, 1)
	dataB = cbor.AppendText(dataB, "x")
	dataB = cbor.AppendInt(dataB, 1)
	cidA := cbor.ComputeCID(cbor.CodecDagCBOR, dataA)
	cidB := cbor.ComputeCID(cbor.CodecDagCBOR, dataB)
	repo := NewRepoCommit()
	empty := repo.State()
	require.NoError(t, repo.Apply(RepoOp{Collection: "com.example.post", RKey: "a", CID: &cidA}))
	require.NoError(t, repo.Apply(RepoOp{Collection: "com.example.post", RKey: "a", Prev: &cidA, CID: &cidB}))
	require.NoError(t, repo.Apply(RepoOp{Collection: "com.example.post", RKey: "a", Prev: &cidB}))
	require.Equal(t, empty, repo.State())
	require.Error(t, repo.Apply(RepoOp{Collection: "com.example.post", RKey: "a"}))
}

func TestRepoCommitApplyFailureIsAtomic(t *testing.T) {
	t.Parallel()
	data := cbor.AppendMapHeader(nil, 0)
	validCID := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	invalidCID := cbor.ComputeCID(cbor.CodecRaw, data)
	repo := NewRepoCommit()
	require.NoError(t, repo.Add("com.example.post", "a", validCID))
	before := repo.State()
	err := repo.Apply(RepoOp{
		Collection: "com.example.post",
		RKey:       "a",
		Prev:       &validCID,
		CID:        &invalidCID,
	})
	require.Error(t, err)
	require.Equal(t, before, repo.State())
}

func TestCanceledEmptySnapshotDoesNotSerialize(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snapshot := &memorySnapshot{records: map[RecordPath][]byte{}}
	_, err := BuildRepoIndex(ctx, snapshot, testCARLimits())
	require.ErrorIs(t, err, context.Canceled)

	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	commit, err := NewRepoCommit().Sign(testCommitContext(t), key)
	require.NoError(t, err)
	var output bytes.Buffer
	err = SerializeRepoCAR(ctx, &output, commit, snapshot, CARIndexOnly, testCARLimits())
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, output.Len())
}

type cancelAfterEnumerationSnapshot struct {
	cancel context.CancelFunc
}

func (s cancelAfterEnumerationSnapshot) ForEach(context.Context, func(RecordRef) error) error {
	s.cancel()
	return nil
}

func (cancelAfterEnumerationSnapshot) OpenRecord(context.Context, RecordPath) (io.ReadCloser, error) {
	return nil, errors.New("unexpected OpenRecord")
}

type cancelingWriter struct {
	cancel context.CancelFunc
	wrote  bool
}

func (w *cancelingWriter) Write(p []byte) (int, error) {
	w.wrote = true
	w.cancel()
	return len(p), nil
}

func TestSerializeRepoCARCancellationAfterEnumerationAndDuringWrite(t *testing.T) {
	t.Parallel()
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	commit, err := NewRepoCommit().Sign(testCommitContext(t), key)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	snapshot := cancelAfterEnumerationSnapshot{cancel: cancel}
	var output bytes.Buffer
	err = SerializeRepoCAR(ctx, &output, commit, snapshot, CARIndexOnly, testCARLimits())
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, output.Len())

	ctx, cancel = context.WithCancel(context.Background())
	writer := &cancelingWriter{cancel: cancel}
	err = SerializeRepoCAR(ctx, writer, commit, &memorySnapshot{records: map[RecordPath][]byte{}}, CARIndexOnly, testCARLimits())
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, writer.wrote)
}

func TestCommitWrongKeyAndEntropyFailure(t *testing.T) {
	t.Parallel()
	ctx := testCommitContext(t)
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	wrong, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	commit, err := NewRepoCommit().Sign(ctx, key)
	require.NoError(t, err)
	_, err = VerifyCommit(commit, ctx, wrong.PublicKey())
	require.Error(t, err)
	var verificationErr *VerificationError
	require.ErrorAs(t, err, &verificationErr)
	require.Equal(t, "commit", verificationErr.Stage)
	_, err = NewRepoCommit().SignWithRandom(ctx, key, io.LimitReader(bytes.NewReader([]byte{1}), 1))
	require.Error(t, err)
}

func FuzzDecodeSignedCommit(f *testing.F) {
	data, err := os.ReadFile("testdata/space-vectors.json")
	if err != nil {
		f.Fatal(err)
	}
	var vectors phaseOneVectors
	if err := json.Unmarshal(data, &vectors); err != nil {
		f.Fatal(err)
	}
	seed, err := base64.StdEncoding.DecodeString(vectors.Commits[0].CommitCBOR)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0xa0})
	f.Fuzz(func(t *testing.T, data []byte) {
		raw, err := DecodeSignedCommit(data)
		if err == nil {
			_, _ = raw.Validate()
		}
	})
}

func FuzzDecodeRepoIndex(f *testing.F) {
	path := RecordPath("com.example.post/a")
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, cbor.AppendMapHeader(nil, 0))
	seed, err := EncodeRepoIndex(RepoIndex{path: cid})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0xa0})
	f.Fuzz(func(t *testing.T, data []byte) {
		index, err := DecodeRepoIndex(data, 1000)
		if err == nil {
			_, _ = EncodeRepoIndex(index)
		}
	})
}
