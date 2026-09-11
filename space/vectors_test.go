package space

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

type phaseOneVectors struct {
	Provenance struct {
		UpstreamCommit string `json:"upstreamCommit"`
		NobleHashes    string `json:"nobleHashes"`
		NobleCurves    string `json:"nobleCurves"`
	} `json:"provenance"`
	LtHash []struct {
		Operation   string `json:"operation"`
		DigestHex   string `json:"digestHex"`
		StateBase64 string `json:"stateBase64"`
	} `json:"ltHash"`
	Context struct {
		Space      string `json:"space"`
		Author     string `json:"author"`
		Rev        string `json:"rev"`
		IKMHex     string `json:"ikmHex"`
		ContextHex string `json:"contextHex"`
		HashHex    string `json:"hashHex"`
		MACHex     string `json:"macHex"`
	} `json:"context"`
	Record struct {
		Paths      []string `json:"paths"`
		CID        string   `json:"cid"`
		CBORBase64 string   `json:"cborBase64"`
	} `json:"record"`
	Commits []struct {
		KeyFamily    string `json:"keyFamily"`
		PublicKeyHex string `json:"publicKeyHex"`
		SignatureHex string `json:"signatureHex"`
		CommitCBOR   string `json:"commitCborBase64"`
		CommitCID    string `json:"commitCid"`
		CARFile      string `json:"carFile"`
		CARSHA256    string `json:"carSha256"`
	} `json:"commits"`
}

func loadPhaseOneVectors(t *testing.T) phaseOneVectors {
	t.Helper()
	data, err := os.ReadFile("testdata/space-vectors.json")
	require.NoError(t, err)
	var vectors phaseOneVectors
	require.NoError(t, json.Unmarshal(data, &vectors))
	require.Equal(t, "9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33", vectors.Provenance.UpstreamCommit)
	require.Equal(t, "1.7.0", vectors.Provenance.NobleHashes)
	require.Equal(t, "1.7.0", vectors.Provenance.NobleCurves)
	return vectors
}

func TestPinnedLtHashStates(t *testing.T) {
	t.Parallel()
	vectors := loadPhaseOneVectors(t)
	hash := NewLtHash()
	for i, vector := range vectors.LtHash {
		switch i {
		case 0:
		case 1:
			hash.Add([]byte("alpha"))
		case 2:
			hash.Add([]byte("snowman-☃"))
		case 3:
			hash.Add([]byte("alpha"))
		case 4:
			hash.Remove([]byte("alpha"))
		default:
			t.Fatalf("unhandled vector operation %q", vector.Operation)
		}
		wantState, err := base64.StdEncoding.DecodeString(vector.StateBase64)
		require.NoError(t, err)
		state := hash.State()
		require.Equal(t, wantState, state[:], vector.Operation)
		digest := hash.Digest()
		require.Equal(t, vector.DigestHex, hex.EncodeToString(digest[:]), vector.Operation)
	}
}

func TestPinnedCommitContextAndMAC(t *testing.T) {
	t.Parallel()
	vectors := loadPhaseOneVectors(t)
	ctx, err := NewCommitContext(vectors.Context.Space, vectors.Context.Author, vectors.Context.Rev)
	require.NoError(t, err)
	ikmBytes, err := hex.DecodeString(vectors.Context.IKMHex)
	require.NoError(t, err)
	var ikm [CommitIKMSize]byte
	copy(ikm[:], ikmBytes)
	ctxBytes, err := ctx.Bytes(ikm)
	require.NoError(t, err)
	require.Equal(t, vectors.Context.ContextHex, hex.EncodeToString(ctxBytes))
	hashBytes, err := hex.DecodeString(vectors.Context.HashHex)
	require.NoError(t, err)
	var hash [CommitHashSize]byte
	copy(hash[:], hashBytes)
	mac, err := computeCommitMAC(ikm, ctxBytes, hash)
	require.NoError(t, err)
	require.Equal(t, vectors.Context.MACHex, hex.EncodeToString(mac[:]))
}

func TestPinnedSignedCommitCARFixtures(t *testing.T) {
	t.Parallel()
	vectors := loadPhaseOneVectors(t)
	ctx, err := NewCommitContext(vectors.Context.Space, vectors.Context.Author, vectors.Context.Rev)
	require.NoError(t, err)
	for _, fixture := range vectors.Commits {
		fixture := fixture
		t.Run(fixture.KeyFamily, func(t *testing.T) {
			t.Parallel()
			pubBytes, err := hex.DecodeString(fixture.PublicKeyHex)
			require.NoError(t, err)
			var pub atmoscrypto.PublicKey
			switch fixture.KeyFamily {
			case "p256":
				pub, err = atmoscrypto.ParsePublicBytesP256(pubBytes)
			case "k256":
				pub, err = atmoscrypto.ParsePublicBytesK256(pubBytes)
			default:
				t.Fatalf("unsupported fixture key family %q", fixture.KeyFamily)
			}
			require.NoError(t, err)

			commitCBOR, err := base64.StdEncoding.DecodeString(fixture.CommitCBOR)
			require.NoError(t, err)
			raw, err := DecodeSignedCommit(commitCBOR)
			require.NoError(t, err)
			commit, err := raw.Validate()
			require.NoError(t, err)
			encoded, err := commit.EncodeCBOR()
			require.NoError(t, err)
			require.Equal(t, commitCBOR, encoded)
			require.Equal(t, fixture.CommitCID, cbor.ComputeCID(cbor.CodecDagCBOR, encoded).String())
			require.Equal(t, fixture.SignatureHex, hex.EncodeToString(commit.Signature[:]))
			_, err = VerifyCommit(commit, ctx, pub)
			require.NoError(t, err)

			carBytes, err := os.ReadFile(filepath.Join("testdata", fixture.CARFile))
			require.NoError(t, err)
			digest := sha256.Sum256(carBytes)
			require.Equal(t, fixture.CARSHA256, hex.EncodeToString(digest[:]))
			var records []VerifiedRecord
			verified, err := VerifyRepoCAR(context.Background(), bytes.NewReader(carBytes), VerifyRepoOptions{
				Context: ctx,
				Key:     pub,
				Mode:    CARFull,
				Limits:  testCARLimits(),
			}, func(record VerifiedRecord) error {
				records = append(records, record.Clone())
				return nil
			})
			require.NoError(t, err)
			require.True(t, verified.Complete)
			require.Equal(t, commit, verified.Commit.SignedCommit)
			require.Len(t, records, 2)
			require.Equal(t, records[0].CID, records[1].CID)
			require.NotEqual(t, records[0].Path, records[1].Path)
			require.Equal(t, vectors.Record.CID, records[0].CID.String())
			recordCBOR, err := base64.StdEncoding.DecodeString(vectors.Record.CBORBase64)
			require.NoError(t, err)
			for i, path := range vectors.Record.Paths {
				require.Equal(t, RecordPath(path), records[i].Path)
				require.Equal(t, recordCBOR, records[i].Data)
			}
		})
	}
}
