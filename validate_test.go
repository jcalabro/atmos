package atmos_test

import (
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each Validate() is a thin wrapper over the matching Parse* function:
// the parsing tests cover the validity logic exhaustively. These tests
// just confirm the method dispatches correctly and that the error
// returned matches the underlying parse error so callers using
// errors.As / errors.Is can rely on either entry point.

func TestDID_Validate(t *testing.T) {
	t.Parallel()
	require.NoError(t, atmos.DID("did:plc:abc123").Validate())

	bad := atmos.DID("not-a-did")
	err := bad.Validate()
	require.Error(t, err)

	_, parseErr := atmos.ParseDID("not-a-did")
	assert.Equal(t, parseErr.Error(), err.Error(),
		"Validate must surface the same error as ParseDID")
}

func TestHandle_Validate(t *testing.T) {
	t.Parallel()
	require.NoError(t, atmos.Handle("alice.bsky.social").Validate())
	require.Error(t, atmos.Handle("not a handle").Validate())
}

func TestNSID_Validate(t *testing.T) {
	t.Parallel()
	require.NoError(t, atmos.NSID("app.bsky.feed.post").Validate())
	require.Error(t, atmos.NSID("badnsid").Validate())
	require.Error(t, atmos.NSID("").Validate())
}

func TestRecordKey_Validate(t *testing.T) {
	t.Parallel()
	require.NoError(t, atmos.RecordKey("3jqfcqzm3fp2j").Validate())
	require.NoError(t, atmos.RecordKey("self").Validate())
	require.Error(t, atmos.RecordKey("").Validate())
	require.Error(t, atmos.RecordKey("has spaces").Validate())
	require.Error(t, atmos.RecordKey(".").Validate(),
		"the disallowed '.' value must fail")
}

func TestTID_Validate(t *testing.T) {
	t.Parallel()
	require.NoError(t, atmos.NewTIDNow(0).Validate(),
		"a freshly minted TID should always validate")
	require.Error(t, atmos.TID("not-13-chars").Validate())
	require.Error(t, atmos.TID("").Validate())
	require.Error(t, atmos.TID("0aaaaaaaaaaaa").Validate(),
		"first char must be in [234567abcdefghij]")
}
