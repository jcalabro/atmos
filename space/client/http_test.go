package client

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngineFreshSignaturePerLogicalAttempt(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		require.Equal(t, "proof-"+string(rune('0'+n)), r.Header.Get("DPoP"))
		if n == 1 {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	var proofs atomic.Int64
	engine, err := newEngine(engineOptions{
		HTTPClient: server.Client(),
		Signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
			n := proofs.Add(1)
			return http.Header{"Dpop": {"proof-" + string(rune('0'+n))}}, nil
		}),
		MaxReadAttempts: 2,
		NetworkPolicy:   NetworkPolicy{AllowPrivateNetworks: true},
	})
	require.NoError(t, err)
	require.NoError(t, engine.json(context.Background(), http.MethodGet, server.URL, nil, nil))
	require.Equal(t, int64(2), requests.Load())
	require.Equal(t, int64(2), proofs.Load())
}

func TestEngineNeverRetriesPOST(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "ambiguous", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	engine, err := newEngine(engineOptions{HTTPClient: server.Client(), MaxReadAttempts: 3, NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: true}})
	require.NoError(t, err)
	err = engine.json(context.Background(), http.MethodPost, server.URL, map[string]string{"x": "y"}, nil)
	require.Error(t, err)
	require.Equal(t, int64(1), requests.Load())
}

func TestEngineMarksAmbiguousPOSTFailures(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		require.True(t, ok)
		connection, _, err := hijacker.Hijack()
		require.NoError(t, err)
		require.NoError(t, connection.Close())
	}))
	defer server.Close()
	engine, err := newEngine(engineOptions{HTTPClient: server.Client(), NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: true}})
	require.NoError(t, err)
	err = engine.json(context.Background(), http.MethodPost, server.URL, map[string]string{"x": "y"}, nil)
	require.ErrorIs(t, err, ErrAmbiguousResult)
}

func TestEngineRejectsRedirectWithoutForwardingAuth(t *testing.T) {
	t.Parallel()
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Store(true) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()

	client := redirect.Client()
	client.CheckRedirect = rejectRedirect
	engine, err := newEngine(engineOptions{HTTPClient: client, Signer: staticSigner("secret", "proof"), NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: true}})
	require.NoError(t, err)
	err = engine.json(context.Background(), http.MethodGet, redirect.URL, nil, nil)
	require.Error(t, err)
	require.False(t, reached.Load())
}

func TestBoundedStreamDetectsOversizeAndDigestMismatch(t *testing.T) {
	t.Parallel()
	t.Run("oversize", func(t *testing.T) {
		s := newBoundedStream(io.NopCloser(&byteReader{data: []byte("12345")}), 4, nil)
		body, err := io.ReadAll(s)
		require.Equal(t, []byte("1234"), body)
		require.ErrorIs(t, err, ErrResponseTooLarge)
		require.NoError(t, s.Close())
	})
	t.Run("digest mismatch", func(t *testing.T) {
		wrong := [32]byte{1}
		s := newBoundedStream(io.NopCloser(&byteReader{data: []byte("body")}), 10, &wrong)
		_, err := io.ReadAll(s)
		require.ErrorIs(t, err, ErrBlobCIDMismatch)
	})
	t.Run("maximum int64 limit", func(t *testing.T) {
		s := newBoundedStream(io.NopCloser(&byteReader{data: []byte("body")}), math.MaxInt64, nil)
		require.NotPanics(t, func() {
			body, err := io.ReadAll(s)
			require.NoError(t, err)
			require.Equal(t, []byte("body"), body)
		})
	})
}

func TestReadBoundedHandlesMaximumInt64Limit(t *testing.T) {
	t.Parallel()
	data, err := readBounded(&byteReader{data: []byte("body")}, -1, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, []byte("body"), data)
}

type byteReader struct{ data []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func staticSigner(auth, proof string) RequestSignerFunc {
	return func(context.Context, string, string) (http.Header, error) {
		return http.Header{"Authorization": {auth}, "Dpop": {proof}}, nil
	}
}

func TestNewEngineRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	_, err := newEngine(engineOptions{JSONLimit: -1})
	require.Error(t, err)
	_, err = newEngine(engineOptions{MaxReadAttempts: -1})
	require.Error(t, err)
	_, err = newEngine(engineOptions{MaxReadAttempts: maxReadAttempts + 1})
	require.Error(t, err)
	require.True(t, errors.Is((&HTTPError{StatusCode: 503}), ErrRemote))
}
