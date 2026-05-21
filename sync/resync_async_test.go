package sync_test

import (
	"errors"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferOverflowError_ErrorAndUnwrap(t *testing.T) {
	t.Parallel()

	err := &sync.BufferOverflowError{
		DID:     atmos.DID("did:plc:alice"),
		Dropped: 7,
	}
	assert.Contains(t, err.Error(), "did:plc:alice")
	assert.Contains(t, err.Error(), "7")

	// errors.As must work so consumers can branch on the type.
	var typed *sync.BufferOverflowError
	assert.True(t, errors.As(error(err), &typed))
	assert.Equal(t, atmos.DID("did:plc:alice"), typed.DID)
	assert.Equal(t, 7, typed.Dropped)
}

// Verifier construction must accept the new option fields and apply
// the documented defaults when they are omitted.
func TestNewVerifier_AsyncOptionDefaults(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	// ResyncEvents and AsyncErrors must be drainable channels of the
	// configured buffer sizes; we don't expose the buffer size from
	// outside, so we only assert non-nil and that the channels are not
	// closed.
	require.NotNil(t, v.ResyncEvents())
	require.NotNil(t, v.AsyncErrors())

	select {
	case _, ok := <-v.ResyncEvents():
		require.True(t, ok, "ResyncEvents() must not be closed before Close()")
	default:
	}
}

func newTestVerifier(t *testing.T) (*sync.Verifier, error) {
	t.Helper()
	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	return sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		SyncClient: gt.Some(&sync.Client{}),
	})
}

func TestVerifier_Close_Idempotent(t *testing.T) {
	t.Parallel()

	v, err := newTestVerifier(t)
	require.NoError(t, err)

	require.NoError(t, v.Close())
	require.NoError(t, v.Close()) // second call must not panic or err

	// After close, ResyncEvents and AsyncErrors must be drainable to
	// EOF (closed and empty).
	_, ok := <-v.ResyncEvents()
	assert.False(t, ok, "ResyncEvents() should be closed after Close()")
	_, ok = <-v.AsyncErrors()
	assert.False(t, ok, "AsyncErrors() should be closed after Close()")
}
