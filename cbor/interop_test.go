package cbor

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Interop test vectors from:
// https://github.com/bluesky-social/atproto-interop-tests/tree/main/data-model

type dataModelFixture struct {
	JSON       json.RawMessage `json:"json"`
	CBORBase64 string          `json:"cbor_base64"`
	CID        string          `json:"cid"`
}

func TestInterop_DataModelFixtures(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/data-model-fixtures.json")
	require.NoError(t, err)

	var fixtures []dataModelFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))

	for i, f := range fixtures {
		t.Run(f.CID, func(t *testing.T) {
			t.Parallel()
			_ = i

			// Decode expected CBOR.
			expectedCBOR, err := base64.RawStdEncoding.DecodeString(f.CBORBase64)
			require.NoError(t, err)

			// Parse JSON to data model, then encode to CBOR.
			val, err := FromJSON(f.JSON)
			require.NoError(t, err)

			gotCBOR, err := Marshal(val)
			require.NoError(t, err)

			// CBOR bytes must match exactly (DAG-CBOR is deterministic).
			assert.Equal(t, expectedCBOR, gotCBOR, "CBOR encoding mismatch")

			// CID must match.
			gotCID := ComputeCID(CodecDagCBOR, gotCBOR)
			assert.Equal(t, f.CID, gotCID.String(), "CID mismatch")

			// Round-trip: CBOR → data model → JSON → data model → CBOR.
			decoded, err := Unmarshal(expectedCBOR)
			require.NoError(t, err)

			roundTripped, err := Marshal(decoded)
			require.NoError(t, err)
			assert.Equal(t, expectedCBOR, roundTripped, "CBOR round-trip mismatch")
		})
	}
}
