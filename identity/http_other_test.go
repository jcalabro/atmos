//go:build !js

package identity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/gttp"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/assert"
)

func TestDefaultResolver_ResolveDID_DefaultWebClientBlocksLoopback(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer srv.Close()
	authority := strings.TrimPrefix(srv.URL, "http://")
	did := atmos.DID("did:web:" + strings.ReplaceAll(authority, ":", "%3A"))

	_, err := (&DefaultResolver{}).ResolveDID(context.Background(), did)
	assert.ErrorIs(t, err, gttp.ErrBlockedByIPPolicy)
	assert.Zero(t, requests.Load())
}
