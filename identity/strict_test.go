package identity

import (
	"errors"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func strictDocument(t *testing.T, did atmos.DID) *DIDDocument {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	return &DIDDocument{
		ID: string(did),
		VerificationMethod: []VerificationMethod{{
			ID: string(did) + "#atproto_space", Type: "Multikey", Controller: string(did),
			PublicKeyMultibase: key.PublicKey().Multibase(),
		}},
		Service: []Service{{
			ID: string(did) + "#atproto_space_host", Type: "AtprotoSpaceHost",
			ServiceEndpoint: "https://space.example.com/base",
		}},
	}
}

func TestSelectStrictEntries(t *testing.T) {
	t.Parallel()
	for _, did := range []atmos.DID{"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:web:authority.example"} {
		did := did
		t.Run(string(did), func(t *testing.T) {
			t.Parallel()
			doc := strictDocument(t, did)
			method, key, err := SelectVerificationMethod(doc, did, "#atproto_space")
			require.NoError(t, err)
			require.Equal(t, string(did), method.Controller)
			require.NotNil(t, key)
			service, endpoint, err := SelectService(doc, did, "atproto_space_host", "AtprotoSpaceHost", EndpointPolicy{})
			require.NoError(t, err)
			require.Equal(t, "AtprotoSpaceHost", service.Type)
			require.Equal(t, "space.example.com", endpoint.Host)
		})
	}
}

func TestSelectVerificationMethodAcceptsPinnedPDSLegacyKeyType(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	doc := strictDocument(t, did)
	privateKey, err := crypto.GenerateK256()
	require.NoError(t, err)
	doc.VerificationMethod[0].Type = "EcdsaSecp256k1VerificationKey2019"
	doc.VerificationMethod[0].PublicKeyMultibase = "z" + base58.Encode(privateKey.PublicKey().Bytes())
	selected, key, err := SelectVerificationMethod(doc, did, "atproto_space")
	require.NoError(t, err)
	require.Equal(t, "EcdsaSecp256k1VerificationKey2019", selected.Type)
	require.NotNil(t, key)

	doc.VerificationMethod[0].PublicKeyMultibase = strictDocument(t, did).VerificationMethod[0].PublicKeyMultibase
	_, _, err = SelectVerificationMethod(doc, did, "atproto_space")
	require.ErrorIs(t, err, ErrMalformedSelectedEntry)
}

func TestSelectVerificationMethodAcceptsLegacyP256KeyType(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	doc := strictDocument(t, did)
	privateKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	doc.VerificationMethod[0].Type = "EcdsaSecp256r1VerificationKey2019"
	doc.VerificationMethod[0].PublicKeyMultibase = "z" + base58.Encode(privateKey.PublicKey().Bytes())
	_, key, err := SelectVerificationMethod(doc, did, "atproto_space")
	require.NoError(t, err)
	require.NotNil(t, key)
}

func TestSelectedEntryAbsentVersusMalformed(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	doc := strictDocument(t, did)
	_, _, err := SelectService(doc, did, "missing", "AtprotoSpaceHost", EndpointPolicy{})
	require.ErrorIs(t, err, ErrSelectedEntryNotFound)
	require.False(t, errors.Is(err, ErrMalformedSelectedEntry))

	doc.Service[0].ServiceEndpoint = ""
	_, _, err = SelectService(doc, did, "atproto_space_host", "AtprotoSpaceHost", EndpointPolicy{})
	require.ErrorIs(t, err, ErrMalformedSelectedEntry)
	require.False(t, errors.Is(err, ErrSelectedEntryNotFound), "malformed present entry must not trigger fallback")
}

func TestSelectVerificationMethodRejectsAmbiguityAndWrongIdentity(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	tests := map[string]func(*DIDDocument){
		"document mismatch":  func(d *DIDDocument) { d.ID = "did:plc:other" },
		"duplicate fragment": func(d *DIDDocument) { d.VerificationMethod = append(d.VerificationMethod, d.VerificationMethod[0]) },
		"foreign full id":    func(d *DIDDocument) { d.VerificationMethod[0].ID = "did:plc:other#atproto_space" },
		"wrong controller":   func(d *DIDDocument) { d.VerificationMethod[0].Controller = "did:plc:other" },
		"wrong type":         func(d *DIDDocument) { d.VerificationMethod[0].Type = "JsonWebKey2020" },
		"bad key":            func(d *DIDDocument) { d.VerificationMethod[0].PublicKeyMultibase = "not-a-key" },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := strictDocument(t, did)
			mutate(doc)
			_, _, err := SelectVerificationMethod(doc, did, "atproto_space")
			require.ErrorIs(t, err, ErrMalformedSelectedEntry)
		})
	}
}

func TestSelectServiceRejectsUnsafeEndpointAndCollisions(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	tests := map[string]func(*DIDDocument){
		"duplicate":  func(d *DIDDocument) { d.Service = append(d.Service, d.Service[0]) },
		"foreign id": func(d *DIDDocument) { d.Service[0].ID = "did:plc:other#atproto_space_host" },
		"wrong type": func(d *DIDDocument) { d.Service[0].Type = "AtprotoPersonalDataServer" },
		"http":       func(d *DIDDocument) { d.Service[0].ServiceEndpoint = "http://space.example.com" },
		"userinfo":   func(d *DIDDocument) { d.Service[0].ServiceEndpoint = "https://user@space.example.com" },
		"query":      func(d *DIDDocument) { d.Service[0].ServiceEndpoint = "https://space.example.com?x=1" },
		"fragment":   func(d *DIDDocument) { d.Service[0].ServiceEndpoint = "https://space.example.com#x" },
		"loopback":   func(d *DIDDocument) { d.Service[0].ServiceEndpoint = "https://127.0.0.1" },
		"IPv6 zone":  func(d *DIDDocument) { d.Service[0].ServiceEndpoint = "https://[fe80::1%25eth0]/" },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := strictDocument(t, did)
			mutate(doc)
			_, _, err := SelectService(doc, did, "atproto_space_host", "AtprotoSpaceHost", EndpointPolicy{})
			require.ErrorIs(t, err, ErrMalformedSelectedEntry)
		})
	}
}

func TestSelectServiceExplicitDevelopmentPolicy(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:web:localhost%3A3000")
	doc := strictDocument(t, did)
	doc.Service[0].ServiceEndpoint = "http://127.0.0.1:3000"
	_, endpoint, err := SelectService(doc, did, "atproto_space_host", "AtprotoSpaceHost", EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true})
	require.NoError(t, err)
	require.Equal(t, "http", endpoint.Scheme)
	doc.Service[0].ServiceEndpoint = "http://[fe80::1%25eth0]:3000"
	_, endpoint, err = SelectService(doc, did, "atproto_space_host", "AtprotoSpaceHost", EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true})
	require.NoError(t, err)
	require.Equal(t, "fe80::1%eth0", endpoint.Hostname())
	doc.Service[0].ServiceEndpoint = "http://localhost:3000"
	_, endpoint, err = SelectService(doc, did, "atproto_space_host", "AtprotoSpaceHost", EndpointPolicy{AllowHTTP: true, AllowPrivateNetworks: true})
	require.NoError(t, err)
	require.Equal(t, "localhost", endpoint.Hostname())
}

func FuzzStrictSelectedEntries(f *testing.F) {
	f.Add([]byte(`{"id":"did:web:authority.example","verificationMethod":[],"service":[]}`), "atproto_space")
	f.Add([]byte(`{"id":"did:web:authority.example","verificationMethod":[{"id":"#atproto_space","type":"Multikey","controller":"did:web:authority.example","publicKeyMultibase":"bad"}],"service":[{"id":"#atproto_space_host","type":"AtprotoSpaceHost","serviceEndpoint":"https://space.example"}]}`), "atproto_space")
	f.Fuzz(func(t *testing.T, raw []byte, fragment string) {
		if len(raw) > 1<<20 || len(fragment) > 2048 {
			return
		}
		doc, err := ParseDIDDocument(raw)
		if err != nil {
			return
		}
		expected, err := atmos.ParseDID(doc.ID)
		if err != nil {
			return
		}
		_, _, _ = SelectVerificationMethod(doc, expected, fragment)
		_, _, _ = SelectService(doc, expected, fragment, "AtprotoSpaceHost", EndpointPolicy{})
	})
}
