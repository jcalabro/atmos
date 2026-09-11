//go:build !js

package client

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngineHardensInjectedTransportAgainstTransparentReplay(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	var mu sync.Mutex
	var proofs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		mu.Lock()
		proofs = append(proofs, r.Header.Get("DPoP"))
		mu.Unlock()
		if attempt == 2 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer does not support HTTP/1 hijacking")
				return
			}
			connection, _, err := hijacker.Hijack()
			require.NoError(t, err)
			require.NoError(t, connection.Close())
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	var signed atomic.Int32
	engine, err := newEngine(engineOptions{
		HTTPClient: server.Client(),
		Signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
			proof := signed.Add(1)
			return http.Header{"DPoP": {string('0' + proof)}}, nil
		}),
		NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: true},
	})
	require.NoError(t, err)
	require.NoError(t, engine.json(context.Background(), http.MethodGet, server.URL+"/first", nil, nil))
	require.Error(t, engine.json(context.Background(), http.MethodGet, server.URL+"/second", nil, nil))
	require.Equal(t, int32(2), attempts.Load(), "the transport must not replay the second request internally")
	require.Equal(t, int32(2), signed.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"1", "2"}, proofs)
}

func TestEngineRejectsOpaqueTransportThatCannotBeHardened(t *testing.T) {
	t.Parallel()
	_, err := newEngine(engineOptions{HTTPClient: &http.Client{Transport: opaqueRoundTripper{}}})
	require.Error(t, err)
}

func TestEngineReplacesInjectedDialHooksWithNetworkPolicy(t *testing.T) {
	t.Parallel()
	var called atomic.Bool
	client := &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		called.Store(true)
		return nil, io.ErrUnexpectedEOF
	}}}
	engine, err := newEngine(engineOptions{HTTPClient: client})
	require.NoError(t, err)
	err = engine.json(context.Background(), http.MethodGet, "http://127.0.0.1/xrpc/test", nil, nil)
	require.ErrorContains(t, err, "not public")
	require.False(t, called.Load(), "an injected dial hook must not bypass address validation")
}

func TestEngineHardensInjectedTLSAndProtocolConfiguration(t *testing.T) {
	t.Parallel()
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	config := &tls.Config{ //nolint:gosec // Deliberately hostile input verifies that hardening removes this setting.
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS10,
		NextProtos:         []string{"h2"},
	}
	input := &http.Transport{TLSClientConfig: config, Protocols: protocols}
	hardened := input.Clone()
	hardenTransportProtocol(hardened)
	hardenTransportNetwork(hardened, NetworkPolicy{})
	require.False(t, hardened.TLSClientConfig.InsecureSkipVerify)
	require.Equal(t, uint16(tls.VersionTLS12), hardened.TLSClientConfig.MinVersion)
	require.Equal(t, []string{"http/1.1"}, hardened.TLSClientConfig.NextProtos)
	require.True(t, hardened.Protocols.HTTP1())
	require.False(t, hardened.Protocols.HTTP2())
	require.False(t, hardened.Protocols.UnencryptedHTTP2())
	require.Empty(t, hardened.TLSNextProto)
	require.Nil(t, hardened.DialTLSContext)
	require.Nil(t, hardened.DialTLS) //nolint:staticcheck // The deprecated hook is part of the bypass regression surface.
	require.True(t, config.InsecureSkipVerify, "hardening must clone rather than mutate caller configuration")
}

type opaqueRoundTripper struct{}

func (opaqueRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, io.ErrUnexpectedEOF
}

func TestCorrectnessTransportPrivateLiteralHostPolicy(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	_, err = netip.ParseAddr(serverURL.Hostname())
	require.NoError(t, err, "the test server must listen on an IP literal")

	get := func(client *http.Client, target string) error {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
		require.NoError(t, err)
		response, err := client.Do(req)
		if response != nil {
			require.NoError(t, response.Body.Close())
		}
		return err
	}

	literal := NewCorrectnessHTTPClient(NetworkPolicy{AllowPrivateLiteralHosts: true})
	require.NoError(t, get(literal, server.URL), "a private IP literal host must be dialable under AllowPrivateLiteralHosts")
	require.Equal(t, int32(1), calls.Load())

	hostname := *serverURL
	hostname.Host = net.JoinHostPort("localhost", serverURL.Port())
	err = get(literal, hostname.String())
	require.ErrorContains(t, err, "not public", "a private DNS answer for a hostname must stay blocked under AllowPrivateLiteralHosts")

	blocked := NewCorrectnessHTTPClient(NetworkPolicy{})
	err = get(blocked, server.URL)
	require.ErrorContains(t, err, "not public", "the zero policy must block private IP literals")
	require.Equal(t, int32(1), calls.Load(), "blocked dials must never reach the server")
}

func TestCorrectnessTransportRejectsSpecialUseNetworks(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"0.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.169.254", "192.0.2.1",
		"198.18.0.1", "198.51.100.1", "203.0.113.1", "::1", "64:ff9b::7f00:1",
		"64:ff9b:1::1", "2001:db8::1", "::ffff:100.64.0.1",
	} {
		require.True(t, unsafeIP(netip.MustParseAddr(raw)), raw)
	}
	for _, raw := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		require.False(t, unsafeIP(netip.MustParseAddr(raw)), raw)
	}
}
