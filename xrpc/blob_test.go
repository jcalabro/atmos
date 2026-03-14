package xrpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadBlob_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/xrpc/com.atproto.repo.uploadBlob", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "image/png", r.Header.Get("Content-Type"))

		body, _ := io.ReadAll(r.Body)
		assert.Equal(t, "fake-png-data", string(body))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"blob": map[string]any{
				"ref":      map[string]string{"$link": "bafyreiabc123"},
				"mimeType": "image/png",
				"size":     13,
			},
		})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	blob, err := c.UploadBlob(context.Background(), "image/png", strings.NewReader("fake-png-data"))
	require.NoError(t, err)
	assert.Equal(t, "bafyreiabc123", blob.Ref.Link)
	assert.Equal(t, "image/png", blob.MimeType)
	assert.Equal(t, int64(13), blob.Size)
}

func TestUploadBlob_ContentTypeForwarded(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "video/mp4", r.Header.Get("Content-Type"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"blob": map[string]any{
				"ref":      map[string]string{"$link": "bafyrei456"},
				"mimeType": "video/mp4",
				"size":     100,
			},
		})
	}))
	defer srv.Close()

	c := &Client{Host: srv.URL, Retry: gt.Some(noRetry())}
	blob, err := c.UploadBlob(context.Background(), "video/mp4", strings.NewReader("data"))
	require.NoError(t, err)
	assert.Equal(t, "video/mp4", blob.MimeType)
}
