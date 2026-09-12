//go:build !js

package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestEngineRefreshesProofForTransparentHTTP1Retry(t *testing.T) {
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
	err = engine.json(context.Background(), http.MethodGet, server.URL+"/second", nil, nil)
	require.NoError(t, err)
	require.Equal(t, int32(3), attempts.Load(), "the pooled transport must transparently retry on a replacement connection")
	require.Equal(t, attempts.Load(), signed.Load(), "every wire send must receive a fresh proof")
	mu.Lock()
	defer mu.Unlock()
	for i, proof := range proofs {
		require.Equal(t, fmt.Sprint(i+1), proof)
	}
}

func TestWireSigningTransportRefreshesEveryInternalAttempt(t *testing.T) {
	t.Parallel()
	var signed atomic.Int32
	var proofs []string
	next := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		require.NotNil(t, trace)
		trace.GotConn(httptrace.GotConnInfo{})
		proofs = append(proofs, req.Header.Get("DPoP"))
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		proofs = append(proofs, req.Header.Get("DPoP"))
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	transport := &wireSigningTransport{
		next: next,
		signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
			return http.Header{"DPoP": {fmt.Sprint(signed.Add(1))}}, nil
		}),
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, []string{"1", "2"}, proofs)
}

func TestWireSigningTransportFailsClosedWhenRetrySigningFails(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	next := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		trace.GotConn(httptrace.GotConnInfo{})
		require.NotEmpty(t, req.Header.Get("Authorization"))
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		require.Empty(t, req.Header.Get("Authorization"), "a failed refresh must remove the replayable credential")
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	transport := &wireSigningTransport{next: next, signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
		if calls.Add(1) == 2 {
			return nil, io.ErrUnexpectedEOF
		}
		return http.Header{"Authorization": {"DPoP token"}, "DPoP": {"proof"}}, nil
	})}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(req)
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	require.Nil(t, response)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestWireSigningTransportRefreshesHTTP2RefusedStreamRetry(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	proofs := make(chan string, 2)
	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer func() { _ = conn.Close() }()
		serverErr <- serveSigningRefusedThenOK(conn, proofs)
	}()

	client, signed := newHTTP2WireSigningClient(t, listener)
	require.NoError(t, doWireSigningGET(t, client))
	require.NoError(t, <-serverErr)
	require.Equal(t, []string{"1", "2"}, []string{<-proofs, <-proofs})
	require.Equal(t, int32(2), signed.Load())
}

func TestWireSigningTransportRefreshesHTTP2GoAwayRetry(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	proofs := make(chan string, 2)
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveSigningGoAwayThenOK(listener, proofs) }()

	client, signed := newHTTP2WireSigningClient(t, listener)
	require.NoError(t, doWireSigningGET(t, client))
	require.NoError(t, <-serverErr)
	require.Equal(t, []string{"1", "2"}, []string{<-proofs, <-proofs})
	require.Equal(t, int32(2), signed.Load())
}

func TestWireSigningTransportRejectsNonAuthenticationHeaders(t *testing.T) {
	t.Parallel()
	transport := &wireSigningTransport{next: opaqueRoundTripper{}, signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
		return http.Header{"X-Forwarded-Host": {"attacker.example"}}, nil
	})}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(req)
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	require.ErrorContains(t, err, "non-authentication header")
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
	hardenPooledTransportProtocol(hardened)
	hardenTransportNetwork(hardened, NetworkPolicy{})
	require.False(t, hardened.TLSClientConfig.InsecureSkipVerify)
	require.Equal(t, uint16(tls.VersionTLS12), hardened.TLSClientConfig.MinVersion)
	require.Equal(t, []string{"h2", "http/1.1"}, hardened.TLSClientConfig.NextProtos)
	require.True(t, hardened.Protocols.HTTP1())
	require.True(t, hardened.Protocols.HTTP2())
	require.False(t, hardened.Protocols.UnencryptedHTTP2())
	require.Nil(t, hardened.TLSNextProto)
	require.Nil(t, hardened.DialTLSContext)
	require.Nil(t, hardened.DialTLS) //nolint:staticcheck // The deprecated hook is part of the bypass regression surface.
	require.True(t, config.InsecureSkipVerify, "hardening must clone rather than mutate caller configuration")
}

func TestEngineUsesPooledHTTP2WithFreshProofs(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var proofs []string
	var peers []string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		proofs = append(proofs, r.Header.Get("DPoP"))
		peers = append(peers, r.RemoteAddr)
		mu.Unlock()
		require.Equal(t, 2, r.ProtoMajor)
		_, _ = io.WriteString(w, `{}`)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	var signed atomic.Int32
	engine, err := newEngine(engineOptions{
		HTTPClient: server.Client(),
		Signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
			return http.Header{"DPoP": {fmt.Sprint(signed.Add(1))}}, nil
		}),
		NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: true},
	})
	require.NoError(t, err)
	require.NoError(t, engine.json(t.Context(), http.MethodGet, server.URL+"/first", nil, nil))
	require.NoError(t, engine.json(t.Context(), http.MethodGet, server.URL+"/second", nil, nil))
	require.Equal(t, int32(2), signed.Load())
	mu.Lock()
	require.Equal(t, []string{"1", "2"}, proofs)
	require.Len(t, peers, 2)
	require.Equal(t, peers[0], peers[1], "logical HTTP/2 requests must reuse one pooled connection")
	mu.Unlock()
}

type opaqueRoundTripper struct{}

func (opaqueRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, io.ErrUnexpectedEOF
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func newHTTP2WireSigningClient(t *testing.T, listener net.Listener) (*http.Client, *atomic.Int32) {
	t.Helper()
	base := &http2.Transport{DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}}
	var signed atomic.Int32
	transport := &wireSigningTransport{next: base, signer: RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
		return http.Header{"DPoP": {fmt.Sprint(signed.Add(1))}}, nil
	})}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}, &signed
}

func doWireSigningGET(t *testing.T, client *http.Client) error {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://space.example/xrpc/test", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	return closeErr
}

func serveSigningGoAwayThenOK(listener net.Listener, proofs chan<- string) error {
	for attempt := range 2 {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		framer, err := acceptSigningHTTP2(conn)
		if err != nil {
			_ = conn.Close()
			return err
		}
		streamID, proof, err := readSigningHTTP2Request(framer, hpack.NewDecoder(64<<10, nil))
		if err != nil {
			_ = conn.Close()
			return err
		}
		proofs <- proof
		if attempt == 0 {
			err = framer.WriteGoAway(0, http2.ErrCodeNo, nil)
			_ = conn.Close()
			if err != nil {
				return err
			}
			continue
		}
		err = writeSigningHTTP2NoContent(framer, streamID)
		_ = conn.Close()
		return err
	}
	return nil
}

func serveSigningRefusedThenOK(conn net.Conn, proofs chan<- string) error {
	framer, err := acceptSigningHTTP2(conn)
	if err != nil {
		return err
	}
	decoder := hpack.NewDecoder(64<<10, nil)
	for attempt := range 2 {
		streamID, proof, err := readSigningHTTP2Request(framer, decoder)
		if err != nil {
			return err
		}
		proofs <- proof
		if attempt == 0 {
			if err := framer.WriteRSTStream(streamID, http2.ErrCodeRefusedStream); err != nil {
				return err
			}
			continue
		}
		return writeSigningHTTP2NoContent(framer, streamID)
	}
	return nil
}

func acceptSigningHTTP2(conn net.Conn) (*http2.Framer, error) {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return nil, err
	}
	if !bytes.Equal(preface, []byte(http2.ClientPreface)) {
		return nil, errors.New("unexpected HTTP/2 client preface")
	}
	framer := http2.NewFramer(conn, bufio.NewReader(conn))
	firstFrame, err := framer.ReadFrame()
	if err != nil {
		return nil, err
	}
	settings, ok := firstFrame.(*http2.SettingsFrame)
	if !ok || settings.IsAck() {
		return nil, errors.New("first HTTP/2 frame was not client settings")
	}
	if err := framer.WriteSettings(); err != nil {
		return nil, err
	}
	if err := framer.WriteSettingsAck(); err != nil {
		return nil, err
	}
	return framer, nil
}

func readSigningHTTP2Request(framer *http2.Framer, decoder *hpack.Decoder) (uint32, string, error) {
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return 0, "", err
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if err := framer.WriteSettingsAck(); err != nil {
					return 0, "", err
				}
			}
		case *http2.HeadersFrame:
			var proof string
			decoder.SetEmitFunc(func(field hpack.HeaderField) {
				if field.Name == "dpop" {
					proof = field.Value
				}
			})
			if _, err := decoder.Write(frame.HeaderBlockFragment()); err != nil {
				return 0, "", err
			}
			if !frame.HeadersEnded() {
				return 0, "", errors.New("continued HTTP/2 header blocks are unsupported in test server")
			}
			return frame.StreamID, proof, nil
		}
	}
}

func writeSigningHTTP2NoContent(framer *http2.Framer, streamID uint32) error {
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	if err := encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "204"}); err != nil {
		return err
	}
	return framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: true,
	})
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
