package sync_test

import (
	"errors"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/assert"
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
