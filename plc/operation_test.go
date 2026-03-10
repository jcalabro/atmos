package plc

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationSignAndVerify(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	op := testGenesisOp(t, key)

	require.NoError(t, op.Sign(key))
	assert.NotNil(t, op.Sig)

	// Verify with correct key.
	require.NoError(t, op.Verify(key.PublicKey()))

	// Verify with wrong key fails.
	other, err := crypto.GenerateP256()
	require.NoError(t, err)
	assert.Error(t, op.Verify(other.PublicKey()))
}

func TestOperationSignAndVerifyK256(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateK256()
	require.NoError(t, err)

	op := &Operation{
		Type:                "plc_operation",
		RotationKeys:        []string{key.PublicKey().DIDKey()},
		VerificationMethods: map[string]string{"atproto": key.PublicKey().DIDKey()},
		AlsoKnownAs:         []string{"at://test.bsky.social"},
		Services: map[string]Service{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", Endpoint: "https://pds.example.com"},
		},
		Prev: nil,
	}

	require.NoError(t, op.Sign(key))
	require.NoError(t, op.Verify(key.PublicKey()))
}

func TestOperationTamperedSigFails(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	op := testGenesisOp(t, key)
	require.NoError(t, op.Sign(key))

	// Tamper with signature.
	sigBytes, err := base64.RawURLEncoding.DecodeString(*op.Sig)
	require.NoError(t, err)
	sigBytes[0] ^= 0xFF
	tampered := base64.RawURLEncoding.EncodeToString(sigBytes)
	op.Sig = &tampered

	assert.Error(t, op.Verify(key.PublicKey()))
}

func TestOperationCIDDeterminism(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	op := testGenesisOp(t, key)
	require.NoError(t, op.Sign(key))

	cid1, err := op.CID()
	require.NoError(t, err)

	cid2, err := op.CID()
	require.NoError(t, err)

	assert.Equal(t, cid1, cid2)
	assert.True(t, strings.HasPrefix(cid1, "b"))
}

func TestOperationDIDDeterminism(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	op := testGenesisOp(t, key)
	require.NoError(t, op.Sign(key))

	did1, err := op.DID()
	require.NoError(t, err)

	did2, err := op.DID()
	require.NoError(t, err)

	assert.Equal(t, did1, did2)
	assert.True(t, strings.HasPrefix(string(did1), "did:plc:"))
	assert.Len(t, string(did1), len("did:plc:")+24)
}

func TestOperationUnsignedErrors(t *testing.T) {
	t.Parallel()

	op := &Operation{
		Type:                "plc_operation",
		RotationKeys:        []string{"did:key:z123"},
		VerificationMethods: map[string]string{},
		AlsoKnownAs:         []string{},
		Services:            map[string]Service{},
		Prev:                nil,
	}

	_, err := op.CID()
	assert.ErrorIs(t, err, ErrNotSigned)

	_, err = op.DID()
	assert.ErrorIs(t, err, ErrNotSigned)

	err = op.Verify(nil)
	assert.ErrorIs(t, err, ErrNotSigned)
}

func TestOperationDoc(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	op := &Operation{
		Type:                "plc_operation",
		RotationKeys:        []string{key.PublicKey().DIDKey()},
		VerificationMethods: map[string]string{"atproto": key.PublicKey().DIDKey()},
		AlsoKnownAs:         []string{"at://alice.bsky.social"},
		Services: map[string]Service{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", Endpoint: "https://pds.example.com"},
		},
	}

	did := "did:plc:testdid1234567890abcdef"
	doc := op.Doc("did:plc:testdid1234567890abcdef")

	assert.Equal(t, did, doc.ID)
	assert.Equal(t, []string{"at://alice.bsky.social"}, doc.AlsoKnownAs)
	require.Len(t, doc.VerificationMethod, 1)
	assert.Equal(t, did+"#atproto", doc.VerificationMethod[0].ID)
	assert.Equal(t, "Multikey", doc.VerificationMethod[0].Type)
	assert.Equal(t, did, doc.VerificationMethod[0].Controller)
	assert.NotEmpty(t, doc.VerificationMethod[0].PublicKeyMultibase)
	require.Len(t, doc.Service, 1)
	assert.Equal(t, "#atproto_pds", doc.Service[0].ID)
	assert.Equal(t, "AtprotoPersonalDataServer", doc.Service[0].Type)
	assert.Equal(t, "https://pds.example.com", doc.Service[0].ServiceEndpoint)

	// AlsoKnownAs is a copy, not shared with the operation.
	doc.AlsoKnownAs[0] = "mutated"
	assert.Equal(t, "at://alice.bsky.social", op.AlsoKnownAs[0])
}

func TestOperationDocMultipleEntries(t *testing.T) {
	t.Parallel()

	key1, err := crypto.GenerateP256()
	require.NoError(t, err)
	key2, err := crypto.GenerateK256()
	require.NoError(t, err)

	op := &Operation{
		Type:         "plc_operation",
		RotationKeys: []string{key1.PublicKey().DIDKey()},
		VerificationMethods: map[string]string{
			"atproto":  key1.PublicKey().DIDKey(),
			"bsky_app": key2.PublicKey().DIDKey(),
		},
		AlsoKnownAs: []string{"at://alice.bsky.social"},
		Services: map[string]Service{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", Endpoint: "https://pds.example.com"},
			"bsky_notif":  {Type: "BskyNotificationService", Endpoint: "https://notif.example.com"},
		},
	}

	did := atmos.DID("did:plc:testdid1234567890abcdef")

	// Call Doc multiple times to verify deterministic ordering.
	doc1 := op.Doc(did)
	doc2 := op.Doc(did)

	require.Len(t, doc1.VerificationMethod, 2)
	require.Len(t, doc1.Service, 2)

	// Verification methods sorted alphabetically: atproto, bsky_app.
	assert.Equal(t, string(did)+"#atproto", doc1.VerificationMethod[0].ID)
	assert.Equal(t, string(did)+"#bsky_app", doc1.VerificationMethod[1].ID)

	// Services sorted alphabetically: atproto_pds, bsky_notif.
	assert.Equal(t, "#atproto_pds", doc1.Service[0].ID)
	assert.Equal(t, "#bsky_notif", doc1.Service[1].ID)

	// Deterministic across calls.
	assert.Equal(t, doc1.VerificationMethod[0].ID, doc2.VerificationMethod[0].ID)
	assert.Equal(t, doc1.Service[0].ID, doc2.Service[0].ID)
}

func TestOperationJSONRoundTrip(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Genesis (nil prev).
	op := testGenesisOp(t, key)
	require.NoError(t, op.Sign(key))

	data, err := json.Marshal(op)
	require.NoError(t, err)

	var decoded Operation
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, op.Type, decoded.Type)
	assert.Equal(t, op.RotationKeys, decoded.RotationKeys)
	assert.Equal(t, op.VerificationMethods, decoded.VerificationMethods)
	assert.Equal(t, op.AlsoKnownAs, decoded.AlsoKnownAs)
	assert.Equal(t, op.Services, decoded.Services)
	assert.Nil(t, decoded.Prev)
	assert.Equal(t, op.Sig, decoded.Sig)

	// Verify signature still valid after round-trip.
	require.NoError(t, decoded.Verify(key.PublicKey()))

	// Update (non-nil prev).
	cid, err := op.CID()
	require.NoError(t, err)
	update := UpdateOp(op, cid, UpdateParams{
		AlsoKnownAs: []string{"at://new.example.com"},
	})
	require.NoError(t, update.Sign(key))

	data, err = json.Marshal(update)
	require.NoError(t, err)

	var decodedUpdate Operation
	require.NoError(t, json.Unmarshal(data, &decodedUpdate))

	require.NotNil(t, decodedUpdate.Prev)
	assert.Equal(t, cid, *decodedUpdate.Prev)
	require.NoError(t, decodedUpdate.Verify(key.PublicKey()))
}

func TestTombstoneSignAndVerify(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	ts := NewTombstoneOp("bafyreiabc123")
	require.NoError(t, ts.Sign(key))
	assert.NotNil(t, ts.Sig)
	require.NoError(t, ts.Verify(key.PublicKey()))

	// Wrong key fails.
	other, err := crypto.GenerateP256()
	require.NoError(t, err)
	assert.Error(t, ts.Verify(other.PublicKey()))
}

func TestTombstoneUnsignedErrors(t *testing.T) {
	t.Parallel()

	ts := NewTombstoneOp("bafyreiabc123")

	_, err := ts.SignedBytes()
	assert.ErrorIs(t, err, ErrNotSigned)

	err = ts.Verify(nil)
	assert.ErrorIs(t, err, ErrNotSigned)
}

func TestTombstoneJSONRoundTrip(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	ts := NewTombstoneOp("bafyreiabc123")
	require.NoError(t, ts.Sign(key))

	data, err := json.Marshal(ts)
	require.NoError(t, err)

	var decoded TombstoneOp
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, "plc_tombstone", decoded.Type)
	assert.Equal(t, "bafyreiabc123", decoded.Prev)
	assert.Equal(t, ts.Sig, decoded.Sig)

	require.NoError(t, decoded.Verify(key.PublicKey()))
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrNotSigned, ErrNotSigned))
	assert.True(t, errors.Is(ErrNotFound, ErrNotFound))
}

// testGenesisOp creates an unsigned genesis operation for testing.
func testGenesisOp(t *testing.T, key crypto.PrivateKey) *Operation {
	t.Helper()
	return &Operation{
		Type:                "plc_operation",
		RotationKeys:        []string{key.PublicKey().DIDKey()},
		VerificationMethods: map[string]string{"atproto": key.PublicKey().DIDKey()},
		AlsoKnownAs:         []string{"at://test.bsky.social"},
		Services: map[string]Service{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", Endpoint: "https://pds.example.com"},
		},
		Prev: nil,
	}
}
