//go:build !js

package oauth

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func newWireDPoPClient(t *testing.T, base http.RoundTripper) *http.Client {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	return &http.Client{Transport: &Transport{
		Base:   base,
		Source: &StaticTokenSource{AccessToken: "token", Key: key},
		Nonces: NewNonceStore(),
	}}
}

func doWireGET(client *http.Client, rawURL string) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return readErr
	}
	return closeErr
}

func TestDPoPTransport_HTTP1ReusedConnectionTransparentReplayGetsFreshProof(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	var mu sync.Mutex
	var proofs []string
	var peers []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		mu.Lock()
		proofs = append(proofs, r.Header.Get("DPoP"))
		peers = append(peers, r.RemoteAddr)
		mu.Unlock()
		if attempt == 2 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("HTTP/1 response writer cannot hijack")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	transport, ok := srv.Client().Transport.(*http.Transport)
	require.True(t, ok)
	base := transport.Clone()
	base.DisableKeepAlives = false
	client := newWireDPoPClient(t, base)
	require.NoError(t, doWireGET(client, srv.URL+"/first"))
	require.NoError(t, doWireGET(client, srv.URL+"/second"))

	require.Equal(t, int32(3), attempts.Load(), "Go's HTTP/1 transport should transparently replay the second GET")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proofs, 3)
	assert.NotEqual(t, proofs[0], proofs[1], "logical requests must receive fresh proofs")
	assert.NotEmpty(t, proofs[2])
	assert.NotEqual(t, proofs[1], proofs[2], "the hidden wire replay must be re-signed with a fresh proof")
	require.Len(t, peers, 3)
	assert.Equal(t, peers[0], peers[1], "the failure must occur on a reused connection")
	assert.NotEqual(t, peers[1], peers[2], "the hidden replay must use a replacement connection")
}

func TestDPoPTransport_HTTP1NoReuseSuppressesTransparentReplay(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	var mu sync.Mutex
	var proofs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		mu.Lock()
		proofs = append(proofs, r.Header.Get("DPoP"))
		mu.Unlock()
		if attempt == 2 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("HTTP/1 response writer cannot hijack")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	transport, ok := srv.Client().Transport.(*http.Transport)
	require.True(t, ok)
	base := transport.Clone()
	base.DisableKeepAlives = true
	base.ForceAttemptHTTP2 = false
	client := newWireDPoPClient(t, base)
	require.NoError(t, doWireGET(client, srv.URL+"/first"))
	require.Error(t, doWireGET(client, srv.URL+"/second"))

	assert.Equal(t, int32(2), attempts.Load(), "a fresh HTTP/1 connection must not be transparently replayed")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proofs, 2)
	assert.NotEqual(t, proofs[0], proofs[1])
}

func TestDPoPTransport_HTTP2RefusedStreamTransparentReplayEvidence(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	proofs := make(chan string, 2)
	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer func() { _ = conn.Close() }()
		serverErr <- serveRefusedThenOK(conn, proofs)
	}()

	base := &http2.Transport{
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		},
	}
	client := newWireDPoPClient(t, base)
	client.Timeout = 5 * time.Second
	require.NoError(t, doWireGET(client, "https://space.example/xrpc/test"))
	require.NoError(t, <-serverErr)

	first := <-proofs
	second := <-proofs
	assert.NotEmpty(t, first)
	assert.NotEmpty(t, second)
	assert.NotEqual(t, first, second, "HTTP/2 REFUSED_STREAM retry must be re-signed with a fresh proof")
}

func TestDPoPTransport_HTTP2GoAwayTransparentReplayEvidence(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	proofs := make(chan string, 2)
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveGoAwayThenOK(listener, proofs) }()

	base := &http2.Transport{
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		},
	}
	client := newWireDPoPClient(t, base)
	client.Timeout = 5 * time.Second
	require.NoError(t, doWireGET(client, "https://space.example/xrpc/test"))
	require.NoError(t, <-serverErr)

	first := <-proofs
	second := <-proofs
	assert.NotEmpty(t, first)
	assert.NotEmpty(t, second)
	assert.NotEqual(t, first, second, "HTTP/2 GOAWAY retry must be re-signed with a fresh proof")
}

func serveGoAwayThenOK(listener net.Listener, proofs chan<- string) error {
	for attempt := range 2 {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		framer, err := acceptHTTP2(conn)
		if err != nil {
			_ = conn.Close()
			return err
		}
		streamID, proof, err := readHTTP2Request(framer, hpack.NewDecoder(64<<10, nil))
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
		err = writeHTTP2NoContent(framer, streamID)
		_ = conn.Close()
		return err
	}
	return nil
}

func acceptHTTP2(conn net.Conn) (*http2.Framer, error) {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return nil, err
	}
	if !bytes.Equal(preface, []byte(http2.ClientPreface)) {
		return nil, assert.AnError
	}
	framer := http2.NewFramer(conn, bufio.NewReader(conn))
	firstFrame, err := framer.ReadFrame()
	if err != nil {
		return nil, err
	}
	settings, ok := firstFrame.(*http2.SettingsFrame)
	if !ok || settings.IsAck() {
		return nil, assert.AnError
	}
	if err := framer.WriteSettings(); err != nil {
		return nil, err
	}
	if err := framer.WriteSettingsAck(); err != nil {
		return nil, err
	}
	return framer, nil
}

func readHTTP2Request(framer *http2.Framer, decoder *hpack.Decoder) (uint32, string, error) {
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
				return 0, "", assert.AnError
			}
			return frame.StreamID, proof, nil
		}
	}
}

func writeHTTP2NoContent(framer *http2.Framer, streamID uint32) error {
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	if err := encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "204"}); err != nil {
		return err
	}
	return framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: true,
	})
}

func serveRefusedThenOK(conn net.Conn, proofs chan<- string) error {
	framer, err := acceptHTTP2(conn)
	if err != nil {
		return err
	}

	decoder := hpack.NewDecoder(64<<10, nil)
	requests := 0
	for requests < 2 {
		streamID, proof, err := readHTTP2Request(framer, decoder)
		if err != nil {
			return err
		}
		proofs <- proof
		requests++
		if requests == 1 {
			if err := framer.WriteRSTStream(streamID, http2.ErrCodeRefusedStream); err != nil {
				return err
			}
			continue
		}
		return writeHTTP2NoContent(framer, streamID)
	}
	return nil
}
