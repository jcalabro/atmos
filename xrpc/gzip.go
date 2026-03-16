package xrpc

import (
	"io"
	"net/http"

	"github.com/klauspost/compress/gzip"
)

// WrapGzip returns a [http.RoundTripper] that uses klauspost/compress for gzip
// decompression, which is significantly faster than the stdlib implementation.
//
// It sets DisableCompression on the underlying transport so that net/http does
// not auto-decompress, then manually adds Accept-Encoding: gzip and
// decompresses matching responses.
func WrapGzip(t *http.Transport) http.RoundTripper {
	t.DisableCompression = true
	return &gzipTransport{next: t}
}

type gzipTransport struct {
	next http.RoundTripper
}

func (g *gzipTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only add Accept-Encoding if the caller hasn't set it.
	if req.Header.Get("Accept-Encoding") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("Accept-Encoding", "gzip")
	}

	resp, err := g.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	if resp.Header.Get("Content-Encoding") != "gzip" {
		return resp, nil
	}

	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}

	resp.Body = &gzipBody{reader: gr, closer: resp.Body}
	resp.Header.Del("Content-Encoding")
	resp.Header.Del("Content-Length")
	resp.ContentLength = -1
	resp.Uncompressed = true

	return resp, nil
}

// gzipBody wraps a gzip reader and the original response body so both are
// closed properly.
type gzipBody struct {
	reader *gzip.Reader
	closer io.Closer
}

func (g *gzipBody) Read(p []byte) (int, error) {
	return g.reader.Read(p)
}

func (g *gzipBody) Close() error {
	_ = g.reader.Close()
	return g.closer.Close()
}
