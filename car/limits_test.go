package car

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestReaderOptionsUseIndependentLimits(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte{0x42}, int(DefaultMaxBlockSize)+1)
	cid := cbor.ComputeCID(cbor.CodecRaw, data)
	var encoded bytes.Buffer
	w, err := NewWriter(&encoded, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, data))

	small, err := NewReaderWithOptions(bytes.NewReader(encoded.Bytes()), ReaderOptions{
		MaxHeaderSize: DefaultMaxHeaderSize,
		MaxBlockSize:  DefaultMaxBlockSize,
	})
	require.NoError(t, err)
	_, err = small.Next()
	var limitErr *LimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "block", limitErr.Kind)
	require.Equal(t, DefaultMaxBlockSize, limitErr.Limit)

	large, err := NewReaderWithOptions(bytes.NewReader(encoded.Bytes()), ReaderOptions{
		MaxHeaderSize: DefaultMaxHeaderSize,
		MaxBlockSize:  uint64(len(data)) + 64,
	})
	require.NoError(t, err)
	block, err := large.Next()
	require.NoError(t, err)
	require.Equal(t, data, block.Data)
}

func TestReaderNextWithLimitIsPerBlock(t *testing.T) {
	t.Parallel()

	smallData := []byte("small")
	largeData := bytes.Repeat([]byte{0x23}, 4096)
	smallCID := cbor.ComputeCID(cbor.CodecRaw, smallData)
	largeCID := cbor.ComputeCID(cbor.CodecRaw, largeData)
	var encoded bytes.Buffer
	w, err := NewWriter(&encoded, []cbor.CID{smallCID})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(smallCID, smallData))
	require.NoError(t, w.WriteBlock(largeCID, largeData))

	r, err := NewReaderWithOptions(bytes.NewReader(encoded.Bytes()), ReaderOptions{
		MaxHeaderSize: DefaultMaxHeaderSize,
		MaxBlockSize:  8192,
	})
	require.NoError(t, err)
	_, err = r.NextWithLimit(128)
	require.NoError(t, err)
	_, err = r.NextWithLimit(1024)
	var limitErr *LimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, uint64(1024), limitErr.Limit)
}

func TestReaderOptionsRejectZeroLimits(t *testing.T) {
	t.Parallel()

	valid := DefaultReaderOptions()
	for _, tc := range []struct {
		name string
		mut  func(*ReaderOptions)
	}{
		{name: "header", mut: func(opts *ReaderOptions) { opts.MaxHeaderSize = 0 }},
		{name: "block", mut: func(opts *ReaderOptions) { opts.MaxBlockSize = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := valid
			tc.mut(&opts)
			_, err := NewReaderWithOptions(bytes.NewReader(nil), opts)
			require.Error(t, err)
		})
	}
	_, err := NewReaderWithOptions(bytes.NewReader(nil), valid)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrInvalidReaderOptions))
}

func TestReaderLimitsAreRaceIndependent(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte{0x7f}, 4096)
	cid := cbor.ComputeCID(cbor.CodecRaw, data)
	var encoded bytes.Buffer
	w, err := NewWriter(&encoded, []cbor.CID{cid})
	require.NoError(t, err)
	require.NoError(t, w.WriteBlock(cid, data))

	const readers = 64
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limit := uint64(2048)
			wantSuccess := i%2 == 0
			if wantSuccess {
				limit = 8192
			}
			r, openErr := NewReaderWithOptions(bytes.NewReader(encoded.Bytes()), ReaderOptions{
				MaxHeaderSize: DefaultMaxHeaderSize,
				MaxBlockSize:  limit,
			})
			if openErr != nil {
				errs <- openErr
				return
			}
			_, readErr := r.Next()
			if wantSuccess != (readErr == nil) {
				errs <- readErr
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestWriterRejectsSilentShortWrites(t *testing.T) {
	t.Parallel()
	data := []byte("record")
	cid := cbor.ComputeCID(cbor.CodecRaw, data)
	_, err := NewWriter(shortWriter{}, []cbor.CID{cid})
	require.ErrorIs(t, err, io.ErrShortWrite)

	var header bytes.Buffer
	w, err := NewWriter(&header, []cbor.CID{cid})
	require.NoError(t, err)
	w.w = shortWriter{}
	require.ErrorIs(t, w.WriteBlock(cid, data), io.ErrShortWrite)
}

func TestWriterOptionsEnforceHeaderLimitBeforeWriting(t *testing.T) {
	t.Parallel()
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	var output bytes.Buffer
	_, err := NewWriterWithOptions(&output, []cbor.CID{cid}, WriterOptions{MaxHeaderSize: 1})
	var limitErr *LimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "header", limitErr.Kind)
	require.Zero(t, output.Len())

	_, err = NewWriterWithOptions(&output, []cbor.CID{cid}, WriterOptions{})
	require.ErrorIs(t, err, ErrInvalidWriterOptions)
}
