package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

const (
	testAccount = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	testSpace   = "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/space/com.example.forum/self"
)

func TestAccountClientOwnRepoWriteValidation(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "account-token", r.Header.Get("Authorization"))
		require.Equal(t, "/xrpc/com.atproto.space.createRecord", r.URL.Path)
		var input map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		require.Equal(t, testAccount, input["repo"])
		_, _ = io.WriteString(w, `{"uri":"`+testSpace+`/`+testAccount+`/com.example.post/one","cid":"`+mustRecordCID(t, `{"$type":"com.example.post","text":"hello"}`)+`","validationStatus":"valid"}`)
	}))
	defer server.Close()
	client := newTestAccountClient(t, server, &requests)
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)

	_, err = client.CreateRecord(context.Background(), spaceRef, "com.example.post", "one", json.RawMessage(`{"$type":"com.example.wrong"}`), ValidateKnown)
	require.Error(t, err)
	require.Zero(t, requests.Load())

	out, err := client.CreateRecord(context.Background(), spaceRef, "com.example.post", "one", json.RawMessage(`{"$type":"com.example.post","text":"hello"}`), ValidateRequired)
	require.NoError(t, err)
	require.Equal(t, "valid", out.ValidationStatus.Val())
	require.Equal(t, int64(1), requests.Load())
}

func TestAccountClientRejectsInvalidRecordDataBeforeRequest(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	client := newTestAccountClient(t, server, &requests)
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)

	tests := []struct {
		name   string
		record json.RawMessage
		mode   ValidationMode
	}{
		{name: "duplicate type", record: json.RawMessage(`{"$type":"com.example.post","$type":"com.example.post"}`), mode: ValidateKnown},
		{name: "floating point", record: json.RawMessage(`{"$type":"com.example.post","score":1.5}`), mode: ValidateKnown},
		{name: "out of range integer", record: json.RawMessage(`{"$type":"com.example.post","score":9223372036854775808}`), mode: ValidateKnown},
		{name: "invalid validation mode", record: json.RawMessage(`{"$type":"com.example.post"}`), mode: ValidationMode(255)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.CreateRecord(context.Background(), spaceRef, "com.example.post", "one", test.record, test.mode)
			require.Error(t, err)
		})
	}
	require.Zero(t, requests.Load())
}

func TestAccountClientRejectsOversizedBatchBeforeRequest(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	client := newTestAccountClient(t, server, &requests)
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	writes := make([]Write, MaxBatchWrites+1)
	for i := range writes {
		writes[i] = Write{Action: WriteDelete, Collection: "com.example.post", RKey: atmos.RecordKey("one")}
	}
	_, err = client.ApplyWrites(context.Background(), spaceRef, writes, ValidateKnown)
	require.Error(t, err)
	require.Zero(t, requests.Load())
}

func TestAccountClientValidatesReadResponseCoordinates(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"uri":"at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/space/com.example.forum/self/did:plc:cccccccccccccccccccccccc/com.example.post/one","cid":"`+mustRecordCID(t, `{"$type":"com.example.post"}`)+`","value":{"$type":"com.example.post"}}`)
	}))
	defer server.Close()
	var requests atomic.Int64
	client := newTestAccountClient(t, server, &requests)
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	_, err = client.GetRecord(context.Background(), spaceRef, "com.example.post", "one")
	require.Error(t, err)
}

func TestAccountClientValidatesReadRecordValues(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("record")).String()

	tests := []struct {
		name  string
		value string
	}{
		{name: "wrong type", value: `{"$type":"com.example.other"}`},
		{name: "duplicate field", value: `{"$type":"com.example.post","x":1,"x":2}`},
		{name: "non data model number", value: `{"$type":"com.example.post","x":1.5}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"uri":"`+testSpace+`/`+testAccount+`/com.example.post/one","cid":"`+cid+`","value":`+test.value+`}`)
			}))
			defer server.Close()
			var requests atomic.Int64
			client := newTestAccountClient(t, server, &requests)
			_, err := client.GetRecord(context.Background(), spaceRef, "com.example.post", "one")
			require.Error(t, err)
		})
	}
}

func TestAccountClientGetDelegationToken(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, testSpace, r.URL.Query().Get("space"))
		_, _ = io.WriteString(w, `{"token":"delegation"}`)
	}))
	defer server.Close()
	var requests atomic.Int64
	client := newTestAccountClient(t, server, &requests)
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	token, err := client.GetDelegationToken(context.Background(), spaceRef)
	require.NoError(t, err)
	require.Equal(t, "delegation", token)
}

func TestAccountClientNeverRetriesDelegationMint(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "do not replay", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	resolver := resolverFunc(func(context.Context, atmos.DID) (*identity.DIDDocument, error) {
		return &identity.DIDDocument{ID: testAccount, Service: []identity.Service{{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: server.URL}}}, nil
	})
	client, err := NewAccountClient(context.Background(), AccountOptions{
		DID: testAccount, Resolver: resolver,
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient:     server.Client(), Signer: staticSigner("account-token", "account-proof"), MaxReadAttempts: 3,
	})
	require.NoError(t, err)
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)

	_, err = client.GetDelegationToken(context.Background(), spaceRef)
	require.Error(t, err)
	require.Equal(t, int64(1), requests.Load())
}

func TestAccountClientBlobUploadRecordAndPrivateDownload(t *testing.T) {
	t.Parallel()

	blobBytes := []byte("private blob bytes")
	blobCID := cbor.ComputeCID(cbor.CodecRaw, blobBytes).String()
	record := json.RawMessage(`{"$type":"com.example.post","blob":{"$type":"blob","ref":{"$link":"` + blobCID + `"},"mimeType":"text/plain","size":18}}`)
	recordCID := mustRecordCID(t, string(record))
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		require.Equal(t, "account-token", req.Header.Get("Authorization"))
		switch req.URL.Path {
		case "/xrpc/com.atproto.repo.uploadBlob":
			require.Equal(t, "text/plain", req.Header.Get("Content-Type"))
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, blobBytes, body)
			_, _ = fmt.Fprintf(w, `{"blob":{"$type":"blob","ref":{"$link":"%s"},"mimeType":"text/plain","size":%d}}`, blobCID, len(blobBytes))
		case "/xrpc/com.atproto.space.createRecord":
			var input comatproto.SpaceCreateRecord_Input
			require.NoError(t, json.NewDecoder(req.Body).Decode(&input))
			require.Equal(t, testAccount, input.Repo)
			require.JSONEq(t, string(record), string(input.Record))
			_, _ = fmt.Fprintf(w, `{"uri":"%s/%s/com.example.post/with-blob","cid":"%s","validationStatus":"valid"}`, testSpace, testAccount, recordCID)
		case "/xrpc/com.atproto.space.getBlob":
			require.Equal(t, testAccount, req.URL.Query().Get("repo"))
			require.Equal(t, testSpace, req.URL.Query().Get("space"))
			require.Equal(t, blobCID, req.URL.Query().Get("cid"))
			_, _ = w.Write(blobBytes)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	client := newTestAccountClient(t, server, &requests)
	space, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)

	uploaded, err := client.UploadBlob(t.Context(), "text/plain", bytes.NewReader(blobBytes))
	require.NoError(t, err)
	require.Equal(t, blobCID, uploaded.Blob.Ref.Link)
	_, err = client.CreateRecord(t.Context(), space, "com.example.post", "with-blob", record, ValidateRequired)
	require.NoError(t, err)
	body, err := client.GetBlob(t.Context(), space, blobCID, int64(len(blobBytes)))
	require.NoError(t, err)
	downloaded, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, blobBytes, downloaded)
	require.Equal(t, int64(3), requests.Load())
}

func TestAccountClientListSpacesEnforcesResponseFilters(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"spaces":[{"uri":"`+testSpace+`"}]}`)
	}))
	defer server.Close()
	var requests atomic.Int64
	client := newTestAccountClient(t, server, &requests)

	_, err := client.ListSpaces(context.Background(), "com.example.other", "", 0, "")
	require.ErrorContains(t, err, "requested type")
	_, err = client.ListSpaces(context.Background(), "", "did:plc:cccccccccccccccccccccccc", 0, "")
	require.ErrorContains(t, err, "requested authority")
}

func TestAccountClientListSpacesRejectsDuplicates(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"spaces":[{"uri":"`+testSpace+`"},{"uri":"`+testSpace+`"}]}`)
	}))
	defer server.Close()
	var requests atomic.Int64
	client := newTestAccountClient(t, server, &requests)

	_, err := client.ListSpaces(context.Background(), "", "", 0, "")
	require.ErrorContains(t, err, "duplicate space URI")
}

func newTestAccountClient(t *testing.T, server *httptest.Server, _ *atomic.Int64) *AccountClient {
	t.Helper()
	did := atmos.DID(testAccount)
	resolver := resolverFunc(func(context.Context, atmos.DID) (*identity.DIDDocument, error) {
		return &identity.DIDDocument{ID: testAccount, Service: []identity.Service{{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: server.URL}}}, nil
	})
	client, err := NewAccountClient(context.Background(), AccountOptions{
		DID: did, Resolver: resolver, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient: server.Client(), Signer: staticSigner("account-token", "account-proof"),
	})
	require.NoError(t, err)
	return client
}

var _ = comatproto.SpaceApplyWrites_Output{}
