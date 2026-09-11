package space

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
)

// CARMode selects whether record bodies are required.
type CARMode uint8

const (
	// CARFull requires one record block for every index entry and EOF after the
	// final record.
	CARFull CARMode = iota + 1
	// CARIndexOnly requires precisely the commit and index blocks and proves no
	// record-body availability.
	CARIndexOnly
)

// CARLimits contains mandatory independent resource limits. There are no
// protocol-wide production defaults: callers must select an operating envelope
// for their deployment.
type CARLimits struct {
	MaxHeaderSize uint64
	MaxCommitSize uint64
	MaxIndexSize  uint64
	MaxRecordSize uint64
	MaxTotalSize  uint64
	MaxRecords    int
}

// Validate rejects absent or internally inconsistent limits.
func (l CARLimits) Validate() error {
	if l.MaxHeaderSize == 0 || l.MaxCommitSize == 0 || l.MaxIndexSize == 0 || l.MaxRecordSize == 0 || l.MaxTotalSize == 0 {
		return errors.New("space: every CAR byte limit must be positive")
	}
	if l.MaxRecords <= 0 {
		return errors.New("space: CAR record limit must be positive")
	}
	return nil
}

// VerifyRepoOptions configures full or index-only repo verification.
type VerifyRepoOptions struct {
	Context CommitContext
	Key     atmoscrypto.PublicKey
	Mode    CARMode
	Limits  CARLimits
}

// VerifiedRecord is a CID-verified, canonical DAG-CBOR map staged during full
// verification. It is provisional until VerifyRepoCAR returns success.
type VerifiedRecord struct {
	Path RecordPath
	CID  cbor.CID
	Data []byte
}

// Clone returns a deep copy suitable for retention by a staging store.
func (r VerifiedRecord) Clone() VerifiedRecord {
	r.Data = append([]byte(nil), r.Data...)
	return r
}

// VerifiedRepo is returned only after the requested mode reaches a clean EOF.
// Complete is true only for a full export whose every body was verified.
type VerifiedRepo struct {
	Commit   VerifiedCommit
	Index    RepoIndex
	State    [LtHashStateSize]byte
	Complete bool
}

// SerializeRepoCAR writes the exact two-root permissioned-repo CAR layout from
// a stable snapshot. Full mode reopens bodies in canonical index order and
// verifies each body's CID before writing it.
func SerializeRepoCAR(ctx context.Context, dst io.Writer, commit SignedCommit, snapshot RepoSnapshot, mode CARMode, limits CARLimits) error {
	if dst == nil {
		return errors.New("space: CAR destination is nil")
	}
	if err := validateCARModeAndLimits(mode, limits); err != nil {
		return err
	}
	if err := commit.Validate(); err != nil {
		return err
	}
	index, err := BuildRepoIndex(ctx, snapshot, limits)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	repo, err := NewRepoCommitFromIndex(index)
	if err != nil {
		return err
	}
	if !repo.Matches(commit) {
		return errors.New("space: snapshot index does not match signed commit hash")
	}
	commitBytes, err := commit.EncodeCBOR()
	if err != nil {
		return err
	}
	if uint64(len(commitBytes)) > limits.MaxCommitSize {
		return fmt.Errorf("space: commit size %d exceeds limit %d", len(commitBytes), limits.MaxCommitSize)
	}
	indexBytes, err := EncodeRepoIndex(index)
	if err != nil {
		return err
	}
	if uint64(len(indexBytes)) > limits.MaxIndexSize {
		return fmt.Errorf("space: index size %d exceeds limit %d", len(indexBytes), limits.MaxIndexSize)
	}
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	indexCID := cbor.ComputeCID(cbor.CodecDagCBOR, indexBytes)
	limited := &boundedWriter{
		w:         &contextWriter{ctx: ctx, w: dst},
		remaining: limits.MaxTotalSize,
		limit:     limits.MaxTotalSize,
	}
	w, err := car.NewWriterWithOptions(limited, []cbor.CID{commitCID, indexCID}, car.WriterOptions{
		MaxHeaderSize: limits.MaxHeaderSize,
	})
	if err != nil {
		return fmt.Errorf("space: write CAR header: %w", err)
	}
	if err := w.WriteBlock(commitCID, commitBytes); err != nil {
		return fmt.Errorf("space: write commit block: %w", err)
	}
	if err := w.WriteBlock(indexCID, indexBytes); err != nil {
		return fmt.Errorf("space: write index block: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mode == CARIndexOnly {
		return nil
	}
	for _, path := range index.Paths() {
		if err := ctx.Err(); err != nil {
			return err
		}
		body, err := snapshot.OpenRecord(ctx, path)
		if err != nil {
			return fmt.Errorf("space: open snapshot record %s: %w", path, err)
		}
		data, readErr := readAllBounded(&contextReader{ctx: ctx, r: body}, limits.MaxRecordSize)
		closeErr := body.Close()
		if readErr != nil {
			return fmt.Errorf("space: read snapshot record %s: %w", path, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("space: close snapshot record %s: %w", path, closeErr)
		}
		if err := validateRecordData(data); err != nil {
			return fmt.Errorf("space: invalid snapshot record %s: %w", path, err)
		}
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
		if !cid.Equal(index[path]) {
			return fmt.Errorf("space: snapshot record %s changed or has wrong CID: expected %s, got %s", path, index[path], cid)
		}
		if err := w.WriteBlock(cid, data); err != nil {
			return fmt.Errorf("space: write record block %s: %w", path, err)
		}
	}
	return ctx.Err()
}

// VerifyRepoCAR verifies the complete CAR stream. onRecord receives
// provisional records one at a time; callers must stage them invisibly and
// promote only after this function returns a VerifiedRepo without error.
func VerifyRepoCAR(ctx context.Context, src io.Reader, opts VerifyRepoOptions, onRecord func(VerifiedRecord) error) (VerifiedRepo, error) {
	verified, err := verifyRepoCAR(ctx, src, opts, onRecord)
	if err != nil {
		return VerifiedRepo{}, wrapVerification("car", err)
	}
	return verified, nil
}

func verifyRepoCAR(ctx context.Context, src io.Reader, opts VerifyRepoOptions, onRecord func(VerifiedRecord) error) (VerifiedRepo, error) {
	if src == nil {
		return VerifiedRepo{}, errors.New("space: CAR source is nil")
	}
	if err := validateCARModeAndLimits(opts.Mode, opts.Limits); err != nil {
		return VerifiedRepo{}, err
	}
	if err := opts.Context.Validate(); err != nil {
		return VerifiedRepo{}, err
	}
	if nilInterface(opts.Key) {
		return VerifiedRepo{}, errors.New("space: CAR verification key is nil")
	}
	maxBlock := max(opts.Limits.MaxCommitSize, opts.Limits.MaxIndexSize, opts.Limits.MaxRecordSize)
	maxFramed, err := framedBlockLimit(maxBlock)
	if err != nil {
		return VerifiedRepo{}, err
	}
	bounded := &boundedReader{ctx: ctx, r: src, remaining: opts.Limits.MaxTotalSize, limit: opts.Limits.MaxTotalSize}
	r, err := car.NewReaderWithOptions(bounded, car.ReaderOptions{
		MaxHeaderSize: opts.Limits.MaxHeaderSize,
		MaxBlockSize:  maxFramed,
	})
	if err != nil {
		return VerifiedRepo{}, fmt.Errorf("space: read CAR header: %w", err)
	}
	header := r.Header()
	if len(header.Roots) != 2 {
		return VerifiedRepo{}, fmt.Errorf("space: expected 2 CAR roots (commit, index), got %d", len(header.Roots))
	}
	for i, root := range header.Roots {
		if !root.Defined() || root.Codec() != cbor.CodecDagCBOR {
			return VerifiedRepo{}, fmt.Errorf("space: CAR root %d must be a dag-cbor CID", i)
		}
	}
	commitBlock, err := nextDataBlock(r, opts.Limits.MaxCommitSize)
	if err != nil {
		return VerifiedRepo{}, fmt.Errorf("space: read commit block: %w", err)
	}
	if !commitBlock.CID.Equal(header.Roots[0]) {
		return VerifiedRepo{}, errors.New("space: expected the commit block to lead the CAR")
	}
	rawCommit, err := DecodeSignedCommit(commitBlock.Data)
	if err != nil {
		return VerifiedRepo{}, err
	}
	commit, err := rawCommit.Validate()
	if err != nil {
		return VerifiedRepo{}, err
	}
	verifiedCommit, err := VerifyCommit(commit, opts.Context, opts.Key)
	if err != nil {
		return VerifiedRepo{}, err
	}
	indexBlock, err := nextDataBlock(r, opts.Limits.MaxIndexSize)
	if err != nil {
		return VerifiedRepo{}, fmt.Errorf("space: read index block: %w", err)
	}
	if !indexBlock.CID.Equal(header.Roots[1]) {
		return VerifiedRepo{}, errors.New("space: expected the index block to follow the commit")
	}
	index, err := DecodeRepoIndex(indexBlock.Data, opts.Limits.MaxRecords)
	if err != nil {
		return VerifiedRepo{}, err
	}
	repo, err := NewRepoCommitFromIndex(index)
	if err != nil {
		return VerifiedRepo{}, err
	}
	if !repo.Matches(commit) {
		return VerifiedRepo{}, errors.New("space: repo index does not match commit hash")
	}
	result := VerifiedRepo{Commit: verifiedCommit, Index: index.Clone(), State: repo.State()}
	if opts.Mode == CARIndexOnly {
		_, err := nextDataBlock(r, opts.Limits.MaxRecordSize)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return VerifiedRepo{}, fmt.Errorf("space: checking index-only record blocks: %w", err)
		}
		return VerifiedRepo{}, errors.New("space: index-only CAR contains a record block")
	}
	paths := index.Paths()
	for i, path := range paths {
		block, err := nextDataBlock(r, opts.Limits.MaxRecordSize)
		if errors.Is(err, io.EOF) {
			return VerifiedRepo{}, fmt.Errorf("space: full CAR is missing %d record block(s)", len(paths)-i)
		}
		if err != nil {
			return VerifiedRepo{}, fmt.Errorf("space: read record block %s: %w", path, err)
		}
		if !block.CID.Equal(index[path]) {
			return VerifiedRepo{}, fmt.Errorf("space: expected block %s at %s, got %s", index[path], path, block.CID)
		}
		if err := validateRecordData(block.Data); err != nil {
			return VerifiedRepo{}, fmt.Errorf("space: invalid record at %s: %w", path, err)
		}
		if onRecord != nil {
			if err := onRecord(VerifiedRecord{Path: path, CID: block.CID, Data: block.Data}); err != nil {
				return VerifiedRepo{}, fmt.Errorf("space: stage record %s: %w", path, err)
			}
		}
	}
	_, err = nextDataBlock(r, opts.Limits.MaxRecordSize)
	if !errors.Is(err, io.EOF) {
		if err != nil {
			return VerifiedRepo{}, fmt.Errorf("space: checking CAR terminus: %w", err)
		}
		return VerifiedRepo{}, errors.New("space: full CAR contains extra record blocks")
	}
	result.Complete = true
	return result, nil
}

func validateCARModeAndLimits(mode CARMode, limits CARLimits) error {
	if mode != CARFull && mode != CARIndexOnly {
		return fmt.Errorf("space: invalid CAR mode %d", mode)
	}
	return limits.Validate()
}

const supportedCIDFrameSize = 36

func framedBlockLimit(dataLimit uint64) (uint64, error) {
	if dataLimit > ^uint64(0)-supportedCIDFrameSize {
		return 0, errors.New("space: CAR block limit overflows uint64")
	}
	return dataLimit + supportedCIDFrameSize, nil
}

func nextDataBlock(r *car.Reader, dataLimit uint64) (car.Block, error) {
	framed, err := framedBlockLimit(dataLimit)
	if err != nil {
		return car.Block{}, err
	}
	block, err := r.NextWithLimit(framed)
	if err != nil {
		return car.Block{}, err
	}
	if uint64(len(block.Data)) > dataLimit {
		return car.Block{}, fmt.Errorf("space: block data size %d exceeds limit %d", len(block.Data), dataLimit)
	}
	return block, nil
}

func validateRecordData(data []byte) error {
	value, err := cbor.Unmarshal(data)
	if err != nil {
		return err
	}
	if _, ok := value.(map[string]any); !ok {
		return errors.New("record must be a DAG-CBOR map")
	}
	return nil
}

func readAllBounded(r io.Reader, limit uint64) ([]byte, error) {
	if limit >= uint64(maxInt()) {
		return nil, errors.New("space: record limit exceeds platform allocation range")
	}
	data, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > limit {
		return nil, fmt.Errorf("space: record size exceeds limit %d", limit)
	}
	return data, nil
}

func maxInt() int { return int(^uint(0) >> 1) }

type boundedReader struct {
	ctx       context.Context
	r         io.Reader
	remaining uint64
	limit     uint64
}

func (r *boundedReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.r.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("space: CAR exceeds total byte limit %d", r.limit)
		}
		return 0, err
	}
	if uint64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= uint64(n)
	return n, err
}

type boundedWriter struct {
	w         io.Writer
	remaining uint64
	limit     uint64
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w *contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.w.Write(p)
	if err != nil {
		return n, err
	}
	if err := w.ctx.Err(); err != nil {
		return n, err
	}
	return n, nil
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if uint64(len(p)) > w.remaining {
		return 0, fmt.Errorf("space: CAR exceeds total byte limit %d", w.limit)
	}
	n, err := w.w.Write(p)
	w.remaining -= uint64(n)
	return n, err
}
