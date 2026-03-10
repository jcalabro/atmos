package plc

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDIDRoundTrip(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	op, did, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PublicKey{rotKey.PublicKey()},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	require.NoError(t, err)

	// DID format.
	assert.True(t, strings.HasPrefix(string(did), "did:plc:"))
	assert.Len(t, string(did), len("did:plc:")+24)

	// Operation is signed.
	assert.NotNil(t, op.Sig)
	assert.Equal(t, "plc_operation", op.Type)
	assert.Nil(t, op.Prev)

	// Verify signature.
	require.NoError(t, op.Verify(sigKey.PublicKey()))

	// CID is computable.
	cid, err := op.CID()
	require.NoError(t, err)
	assert.NotEmpty(t, cid)

	// Doc conversion works.
	doc := op.Doc(did)
	assert.Equal(t, string(did), doc.ID)
	assert.Equal(t, []string{"at://alice.bsky.social"}, doc.AlsoKnownAs)
}

func TestCreateDIDMissingRotationKeys(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	_, _, err = CreateDID(CreateParams{
		SigningKey:   key,
		RotationKeys: nil,
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	assert.Error(t, err)
}

func TestCreateDIDMissingSigningKey(t *testing.T) {
	t.Parallel()

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	_, _, err = CreateDID(CreateParams{
		SigningKey:   nil,
		RotationKeys: []crypto.PublicKey{rotKey.PublicKey()},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	assert.Error(t, err)
}

func TestUpdateChain(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PublicKey{rotKey.PublicKey()},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	require.NoError(t, err)

	genesisCID, err := genesis.CID()
	require.NoError(t, err)

	// Update handle.
	update := UpdateOp(genesis, genesisCID, UpdateParams{
		AlsoKnownAs: []string{"at://alice.example.com"},
	})

	assert.Equal(t, &genesisCID, update.Prev)
	assert.Equal(t, []string{"at://alice.example.com"}, update.AlsoKnownAs)
	// Inherited fields are copies, not shared.
	assert.Equal(t, genesis.RotationKeys, update.RotationKeys)
	assert.Equal(t, genesis.VerificationMethods, update.VerificationMethods)

	require.NoError(t, update.Sign(sigKey))
	require.NoError(t, update.Verify(sigKey.PublicKey()))

	updateCID, err := update.CID()
	require.NoError(t, err)
	assert.NotEqual(t, genesisCID, updateCID)
}

func TestUpdateOpInheritsCopies(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PublicKey{rotKey.PublicKey()},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	require.NoError(t, err)

	genesisCID, err := genesis.CID()
	require.NoError(t, err)

	// Update with no overrides — should deep-copy everything.
	update := UpdateOp(genesis, genesisCID, UpdateParams{})

	// Mutating update should not affect genesis.
	update.RotationKeys[0] = "mutated"
	update.AlsoKnownAs[0] = "mutated"
	update.VerificationMethods["atproto"] = "mutated"
	update.Services["atproto_pds"] = Service{Type: "mutated", Endpoint: "mutated"}

	assert.NotEqual(t, "mutated", genesis.RotationKeys[0])
	assert.NotEqual(t, "mutated", genesis.AlsoKnownAs[0])
	assert.NotEqual(t, "mutated", genesis.VerificationMethods["atproto"])
	assert.NotEqual(t, "mutated", genesis.Services["atproto_pds"].Type)
}

func TestUpdateOpAllOverrides(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PublicKey{rotKey.PublicKey()},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	require.NoError(t, err)

	genesisCID, err := genesis.CID()
	require.NoError(t, err)

	newKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	update := UpdateOp(genesis, genesisCID, UpdateParams{
		VerificationMethods: map[string]string{"atproto": newKey.PublicKey().DIDKey()},
		RotationKeys:        []string{newKey.PublicKey().DIDKey()},
		AlsoKnownAs:         []string{"at://bob.example.com"},
		Services: map[string]Service{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", Endpoint: "https://new-pds.example.com"},
		},
	})

	assert.Equal(t, []string{newKey.PublicKey().DIDKey()}, update.RotationKeys)
	assert.Equal(t, map[string]string{"atproto": newKey.PublicKey().DIDKey()}, update.VerificationMethods)
	assert.Equal(t, []string{"at://bob.example.com"}, update.AlsoKnownAs)
	assert.Equal(t, "https://new-pds.example.com", update.Services["atproto_pds"].Endpoint)

	require.NoError(t, update.Sign(sigKey))
	require.NoError(t, update.Verify(sigKey.PublicKey()))
}

func TestTombstoneChain(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PublicKey{rotKey.PublicKey()},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	require.NoError(t, err)

	genesisCID, err := genesis.CID()
	require.NoError(t, err)

	ts := NewTombstoneOp(genesisCID)
	assert.Equal(t, "plc_tombstone", ts.Type)
	assert.Equal(t, genesisCID, ts.Prev)

	require.NoError(t, ts.Sign(sigKey))
	require.NoError(t, ts.Verify(sigKey.PublicKey()))
}
