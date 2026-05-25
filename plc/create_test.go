package plc

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
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
		RotationKeys: []crypto.PrivateKey{rotKey},
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

	// Verify signature: genesis is signed by the rotation key (PLC v0.1).
	require.NoError(t, op.Verify(rotKey.PublicKey()))
	// The atproto signing key has no authority over the chain — verification
	// under it MUST fail. This locks in the v0.1 invariant.
	require.Error(t, op.Verify(sigKey.PublicKey()))

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
		RotationKeys: []crypto.PrivateKey{rotKey},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	assert.Error(t, err)
}

func TestCreateDIDNilRotationKeyEntry(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	_, _, err = CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PrivateKey{rotKey, nil},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	assert.Error(t, err)
}

// PLC spec: at most 5 rotation keys.
func TestCreateDIDTooManyRotationKeys(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	keys := make([]crypto.PrivateKey, MaxRotationKeys+1)
	for i := range keys {
		k, err := crypto.GenerateP256()
		require.NoError(t, err)
		keys[i] = k
	}

	_, _, err = CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: keys,
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	assert.Error(t, err)
}

// PLC spec: rotation keys must have no duplication.
func TestCreateDIDDuplicateRotationKeys(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	_, _, err = CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PrivateKey{rotKey, rotKey},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	assert.Error(t, err)
}

func TestCreateDIDInvalidHandle(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	cases := []struct {
		name   string
		handle string
	}{
		{"empty", ""},
		{"single label", "alice"},
		{"contains scheme", "at://alice.bsky.social"},
		{"contains space", "alice .bsky.social"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := CreateDID(CreateParams{
				SigningKey:   sigKey,
				RotationKeys: []crypto.PrivateKey{rotKey},
				Handle:       atmos.Handle(tc.handle),
				PDS:          "https://pds.example.com",
			})
			assert.Error(t, err)
		})
	}
}

func TestCreateDIDInvalidPDS(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	cases := []struct {
		name string
		pds  string
	}{
		{"empty", ""},
		{"no scheme", "pds.example.com"},
		{"wrong scheme", "ftp://pds.example.com"},
		{"missing host", "https://"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := CreateDID(CreateParams{
				SigningKey:   sigKey,
				RotationKeys: []crypto.PrivateKey{rotKey},
				Handle:       "alice.bsky.social",
				PDS:          tc.pds,
			})
			assert.Error(t, err)
		})
	}
}

func TestUpdateChain(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PrivateKey{rotKey},
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

	// Updates are signed by a rotation key (PLC v0.1).
	require.NoError(t, update.Sign(rotKey))
	require.NoError(t, update.Verify(rotKey.PublicKey()))
	require.Error(t, update.Verify(sigKey.PublicKey()))

	updateCID, err := update.CID()
	require.NoError(t, err)
	assert.NotEqual(t, genesisCID, updateCID)

	// did:plc is derived from the genesis op only — DID() on an update must
	// refuse rather than silently returning an unrelated string.
	_, err = update.DID()
	assert.ErrorIs(t, err, ErrNotGenesis)
}

func TestUpdateOpInheritsCopies(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PrivateKey{rotKey},
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
		RotationKeys: []crypto.PrivateKey{rotKey},
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

	// Updates are signed by a rotation key (PLC v0.1).
	require.NoError(t, update.Sign(rotKey))
	require.NoError(t, update.Verify(rotKey.PublicKey()))
}

func TestTombstoneChain(t *testing.T) {
	t.Parallel()

	sigKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	rotKey, err := crypto.GenerateP256()
	require.NoError(t, err)

	genesis, _, err := CreateDID(CreateParams{
		SigningKey:   sigKey,
		RotationKeys: []crypto.PrivateKey{rotKey},
		Handle:       "alice.bsky.social",
		PDS:          "https://pds.example.com",
	})
	require.NoError(t, err)

	genesisCID, err := genesis.CID()
	require.NoError(t, err)

	ts := NewTombstoneOp(genesisCID)
	assert.Equal(t, "plc_tombstone", ts.Type)
	assert.Equal(t, genesisCID, ts.Prev)

	// Tombstones are signed by a rotation key (PLC v0.1).
	require.NoError(t, ts.Sign(rotKey))
	require.NoError(t, ts.Verify(rotKey.PublicKey()))
	require.Error(t, ts.Verify(sigKey.PublicKey()))
}
