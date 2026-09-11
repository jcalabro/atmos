package space

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

func testCommitContext(t *testing.T) CommitContext {
	t.Helper()
	ctx, err := NewCommitContext(
		"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.forum/3jzfcijpj2z2a",
		"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
		"3jzfcijpj2z2a",
	)
	require.NoError(t, err)
	return ctx
}

func TestCommitContextEncoding(t *testing.T) {
	t.Parallel()
	ctx := testCommitContext(t)
	var ikm [CommitIKMSize]byte
	for i := range ikm {
		ikm[i] = byte(i)
	}
	got, err := ctx.Bytes(ikm)
	require.NoError(t, err)

	space := ctx.Space.String()
	author := ctx.Author.String()
	rev := ctx.Rev.String()
	want := append([]byte("atproto-space-v1"), byte(len(space)>>8), byte(len(space)))
	want = append(want, space...)
	want = append(want, byte(len(author)>>8), byte(len(author)))
	want = append(want, author...)
	want = append(want, byte(len(rev)>>8), byte(len(rev)))
	want = append(want, rev...)
	want = append(want, 0, CommitIKMSize)
	want = append(want, ikm[:]...)
	require.Equal(t, want, got)
}

func TestCommitContextValidation(t *testing.T) {
	t.Parallel()
	valid := testCommitContext(t)
	for _, tc := range []struct {
		name string
		ctx  CommitContext
	}{
		{name: "zero", ctx: CommitContext{}},
		{name: "space", ctx: CommitContext{Space: atmos.SpaceRef("bad"), Author: valid.Author, Rev: valid.Rev}},
		{name: "author", ctx: CommitContext{Space: valid.Space, Author: atmos.DID("bad"), Rev: valid.Rev}},
		{name: "rev", ctx: CommitContext{Space: valid.Space, Author: valid.Author, Rev: atmos.TID("bad")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tc.ctx.Validate())
		})
	}
}

func TestSignVerifyCommitBothKeyFamilies(t *testing.T) {
	t.Parallel()
	ctx := testCommitContext(t)
	repo := NewRepoCommit()
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, cbor.AppendMapHeader(nil, 0))
	require.NoError(t, repo.Add("com.example.post", "3jzfcijpj2z2a", cid))

	p256, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	k256, err := atmoscrypto.GenerateK256()
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		key  atmoscrypto.PrivateKey
	}{
		{name: "p256", key: p256},
		{name: "k256", key: k256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			commit, err := repo.Sign(ctx, tc.key)
			require.NoError(t, err)
			require.NoError(t, commit.Validate())
			verified, err := VerifyCommit(commit, ctx, tc.key.PublicKey())
			require.NoError(t, err)
			require.Equal(t, commit, verified.SignedCommit)
			require.Equal(t, ctx, verified.Context)
			require.True(t, repo.Matches(commit))

			encoded, err := commit.EncodeCBOR()
			require.NoError(t, err)
			raw, err := DecodeSignedCommit(encoded)
			require.NoError(t, err)
			roundTrip, err := raw.Validate()
			require.NoError(t, err)
			require.Equal(t, commit, roundTrip)
		})
	}
}

func TestCommitRejectsEveryAuthenticatedMutation(t *testing.T) {
	t.Parallel()
	ctx := testCommitContext(t)
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	commit, err := NewRepoCommit().SignWithRandom(ctx, key, bytes.NewReader(bytes.Repeat([]byte{0x41}, CommitIKMSize)))
	require.NoError(t, err)

	mutations := map[string]func(*SignedCommit, *CommitContext){
		"version":   func(c *SignedCommit, _ *CommitContext) { c.Version++ },
		"hash":      func(c *SignedCommit, _ *CommitContext) { c.Hash[0]++ },
		"ikm":       func(c *SignedCommit, _ *CommitContext) { c.IKM[0]++ },
		"mac":       func(c *SignedCommit, _ *CommitContext) { c.MAC[0]++ },
		"signature": func(c *SignedCommit, _ *CommitContext) { c.Signature[0]++ },
		"revision":  func(c *SignedCommit, _ *CommitContext) { c.Rev = atmos.TID("3jzfcijpj2z2b") },
		"space": func(_ *SignedCommit, c *CommitContext) {
			c.Space = atmos.SpaceRef("at://did:plc:cccccccccccccccccccccccc/space/com.example.forum/3jzfcijpj2z2a")
		},
		"author": func(_ *SignedCommit, c *CommitContext) { c.Author = atmos.DID("did:plc:cccccccccccccccccccccccc") },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changedCommit := commit
			changedContext := ctx
			mutate(&changedCommit, &changedContext)
			_, err := VerifyCommit(changedCommit, changedContext, key.PublicKey())
			require.Error(t, err)
		})
	}
}

func TestCommitDeniabilityChangedHashWithSameSignature(t *testing.T) {
	t.Parallel()
	ctx := testCommitContext(t)
	key, err := atmoscrypto.GenerateK256()
	require.NoError(t, err)
	commit, err := NewRepoCommit().SignWithRandom(ctx, key, bytes.NewReader(bytes.Repeat([]byte{0x9a}, CommitIKMSize)))
	require.NoError(t, err)
	originalSignature := commit.Signature
	commit.Hash[0] ^= 0x80
	ctxBytes, err := ctx.Bytes(commit.IKM)
	require.NoError(t, err)
	commit.MAC, err = computeCommitMAC(commit.IKM, ctxBytes, commit.Hash)
	require.NoError(t, err)
	require.Equal(t, originalSignature, commit.Signature)
	_, err = VerifyCommit(commit, ctx, key.PublicKey())
	require.NoError(t, err)
}

func TestRawSignedCommitValidation(t *testing.T) {
	t.Parallel()
	base := RawSignedCommit{
		Version: CommitVersion,
		Hash:    make([]byte, CommitHashSize),
		IKM:     make([]byte, CommitIKMSize),
		MAC:     make([]byte, CommitMACSize),
		Sig:     make([]byte, CommitSignatureSize),
		Rev:     "3jzfcijpj2z2a",
	}
	for _, tc := range []struct {
		name string
		mut  func(*RawSignedCommit)
	}{
		{name: "version", mut: func(c *RawSignedCommit) { c.Version = 2 }},
		{name: "hash short", mut: func(c *RawSignedCommit) { c.Hash = c.Hash[:31] }},
		{name: "ikm long", mut: func(c *RawSignedCommit) { c.IKM = append(c.IKM, 0) }},
		{name: "mac short", mut: func(c *RawSignedCommit) { c.MAC = c.MAC[:31] }},
		{name: "sig long", mut: func(c *RawSignedCommit) { c.Sig = append(c.Sig, 0) }},
		{name: "rev", mut: func(c *RawSignedCommit) { c.Rev = "invalid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := base.Clone()
			tc.mut(&candidate)
			_, err := candidate.Validate()
			require.Error(t, err)
		})
	}
}

func TestDecodeSignedCommitRejectsUnknownAndNoncanonicalFields(t *testing.T) {
	t.Parallel()
	ctx := testCommitContext(t)
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)
	commit, err := NewRepoCommit().Sign(ctx, key)
	require.NoError(t, err)
	encoded, err := commit.EncodeCBOR()
	require.NoError(t, err)
	value, err := cbor.Unmarshal(encoded)
	require.NoError(t, err)
	m, ok := value.(map[string]any)
	require.True(t, ok)
	m["extra"] = int64(1)
	withExtra, err := cbor.Marshal(m)
	require.NoError(t, err)
	_, err = DecodeSignedCommit(withExtra)
	require.ErrorContains(t, err, "unknown")

	_, err = DecodeSignedCommit(append(encoded, 0))
	require.Error(t, err)
}

func TestExpandOnlyMACVector(t *testing.T) {
	t.Parallel()
	var ikm, hash [32]byte
	for i := range ikm {
		ikm[i] = byte(i)
		hash[i] = byte(255 - i)
	}
	ctxBytes, err := testCommitContext(t).Bytes(ikm)
	require.NoError(t, err)
	mac, err := computeCommitMAC(ikm, ctxBytes, hash)
	require.NoError(t, err)
	// Generated by the pinned @noble/hashes 1.7.0 expand()+hmac() implementation.
	require.Equal(t, "ca7d420990864d0af0b0777fa800aa6084c81e16111598bc0c5270a856a8db34", hex.EncodeToString(mac[:]))
}
