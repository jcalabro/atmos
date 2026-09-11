package credential

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/require"
)

func TestDPoPRoundTripAndThumbprintVector(t *testing.T) {
	t.Parallel()
	rfcJWK := ECPublicJWK{
		KTY: "EC", CRV: "P-256",
		X: "l8tFrhx-34tV3hRICRDY9zCkDlpBhF42UQUfWVAWBFs",
		Y: "9VE4jf_Ok_o64zbTTlcuNJajHmt6v9TDVrU0CdvGRDA",
	}
	rfcThumbprint, err := JWKThumbprint(rfcJWK)
	require.NoError(t, err)
	require.Equal(t, "0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I", rfcThumbprint)

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	jwk, err := PublicJWK(key.PublicKey())
	require.NoError(t, err)
	thumbprint, err := JWKThumbprint(jwk)
	require.NoError(t, err)
	require.Len(t, thumbprint, 43)

	now := time.Unix(2_000_000_000, 0)
	proof, err := CreateDPoPProof(DPoPProofParams{
		Key: key, Method: "get", TargetURL: "https://repo.example/xrpc/com.atproto.space.getRepo?cursor=x#fragment",
		Credential: "credential", Now: now,
	})
	require.NoError(t, err)
	verified, err := VerifyDPoPProof(context.Background(), proof, VerifyDPoPOptions{
		Method: "GET", TargetURL: "https://repo.example/xrpc/com.atproto.space.getRepo?other=y",
		Credential: "credential", ExpectedJKT: thumbprint, Now: now.Add(time.Second), Replay: mustReplay(t),
	})
	require.NoError(t, err)
	require.Equal(t, thumbprint, verified.JKT)
	require.Equal(t, "https://repo.example/xrpc/com.atproto.space.getRepo", verified.HTU)
}

func TestDPoPRejectsWrongBindingsAndReplay(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	proof, err := CreateDPoPProof(DPoPProofParams{Key: key, Method: "POST", TargetURL: "https://authority.example/xrpc/exchange", Now: now})
	require.NoError(t, err)
	replay := mustReplay(t)
	opts := VerifyDPoPOptions{Method: "POST", TargetURL: "https://authority.example/xrpc/exchange", Now: now, Replay: replay}
	_, err = VerifyDPoPProof(context.Background(), proof, opts)
	require.NoError(t, err)
	_, err = VerifyDPoPProof(context.Background(), proof, opts)
	require.ErrorIs(t, err, ErrReplay)

	for name, mutate := range map[string]func(*VerifyDPoPOptions){
		"method":                func(o *VerifyDPoPOptions) { o.Method = "GET" },
		"url":                   func(o *VerifyDPoPOptions) { o.TargetURL += "/other" },
		"unexpected credential": func(o *VerifyDPoPOptions) { o.Credential = "present" },
		"future":                func(o *VerifyDPoPOptions) { o.Now = now.Add(-6 * time.Second) },
		"old":                   func(o *VerifyDPoPOptions) { o.Now = now.Add(66 * time.Second) },
		"jkt":                   func(o *VerifyDPoPOptions) { o.ExpectedJKT = strings.Repeat("x", 43) },
	} {
		t.Run(name, func(t *testing.T) {
			o := VerifyDPoPOptions{Method: "POST", TargetURL: "https://authority.example/xrpc/exchange", Now: now, Replay: mustReplay(t)}
			mutate(&o)
			_, verifyErr := VerifyDPoPProof(context.Background(), proof, o)
			require.Error(t, verifyErr)
		})
	}
}

func TestDPoPVerifiesBeforeReplayConsume(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	proof, err := CreateDPoPProof(DPoPProofParams{Key: key, Method: "GET", TargetURL: "https://repo.example/x", Now: now})
	require.NoError(t, err)
	store := &failingReplayStore{}
	_, err = VerifyDPoPProof(context.Background(), proof, VerifyDPoPOptions{
		Method: "POST", TargetURL: "https://repo.example/x", Now: now, Replay: store,
	})
	require.ErrorIs(t, err, ErrTokenBinding)
	require.Zero(t, store.calls.Load())
	_, err = VerifyDPoPProof(context.Background(), proof, VerifyDPoPOptions{
		Method: "GET", TargetURL: "https://repo.example/x", Now: now, Replay: store,
	})
	require.ErrorContains(t, err, "backend unavailable")
	require.EqualValues(t, 1, store.calls.Load())
}

func TestDPoPRejectsK256PrivateAndPrivateJWK(t *testing.T) {
	t.Parallel()
	k, err := crypto.GenerateK256()
	require.NoError(t, err)
	_, err = CreateDPoPProof(DPoPProofParams{Key: k, Method: "GET", TargetURL: "https://repo.example/x"})
	require.Error(t, err)

	p, err := crypto.GenerateP256()
	require.NoError(t, err)
	raw, err := CreateDPoPProof(DPoPProofParams{Key: p, Method: "GET", TargetURL: "https://repo.example/x", Now: time.Unix(2_000_000_000, 0)})
	require.NoError(t, err)
	parts := strings.Split(raw, ".")
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	header := strings.Replace(string(headerJSON), `"y":`, `"d":"secret","y":`, 1)
	parts[0] = b64(header)
	_, err = VerifyDPoPProof(context.Background(), strings.Join(parts, "."), VerifyDPoPOptions{Method: "GET", TargetURL: "https://repo.example/x", Now: time.Unix(2_000_000_000, 0), Replay: mustReplay(t)})
	require.Error(t, err)
}

func FuzzParseToken(f *testing.F) {
	f.Add("x")
	f.Add("e30.e30.e30")
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > maxTokenBytes+1 {
			return
		}
		_, _ = ParseDelegationToken(raw)
		_, _ = ParseClientAttestationToken(raw)
		_, _ = ParseSpaceCredentialToken(raw)
	})
}

func FuzzVerifyDPoP(f *testing.F) {
	f.Add("x", "GET", "https://example.com/x")
	f.Fuzz(func(t *testing.T, proof, method, target string) {
		if len(proof) > maxTokenBytes+1 || len(target) > 4096 {
			return
		}
		_, _ = VerifyDPoPProof(context.Background(), proof, VerifyDPoPOptions{Method: method, TargetURL: target, Now: time.Unix(2_000_000_000, 0), Replay: mustReplay(t)})
	})
}
