// Package identity provides ATProto identity resolution — resolving DIDs to
// DID documents and handles to DIDs with bi-directional verification.
package identity

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

var (
	// ErrDIDNotFound indicates the DID document could not be found.
	ErrDIDNotFound = errors.New("identity: DID not found")
	// ErrHandleNotFound indicates no DID was found for the handle.
	ErrHandleNotFound = errors.New("identity: handle not found")
	// ErrUnsupportedDIDMethod indicates the DID method is not did:plc or did:web.
	ErrUnsupportedDIDMethod = errors.New("identity: unsupported DID method")
)

// Identity is the resolved, verified identity of an ATProto account.
type Identity struct {
	DID      atmos.DID
	Handle   atmos.Handle
	Keys     map[string]Key
	Services map[string]ServiceEndpoint
}

// Key is a verification key extracted from a DID document.
type Key struct {
	Type      string
	Multibase string
}

// ServiceEndpoint is a service extracted from a DID document.
type ServiceEndpoint struct {
	Type string
	URL  string
}

// PDSEndpoint returns the PDS URL for this identity, or empty string if not present.
func (id *Identity) PDSEndpoint() string {
	if s, ok := id.Services["atproto_pds"]; ok {
		return s.URL
	}
	return ""
}

// PublicKey parses and returns the atproto signing key for this identity.
func (id *Identity) PublicKey() (crypto.PublicKey, error) {
	return id.PublicKeyForFragment("atproto")
}

// PublicKeyForFragment parses and returns the verification key identified by the
// given bare fragment (e.g. "atproto" or "atproto_labeler"). A leading '#' is
// tolerated. An empty fragment defaults to the atproto signing key. This
// supports service-auth issuers of the form did:plc:xxx#atproto_labeler.
func (id *Identity) PublicKeyForFragment(fragment string) (crypto.PublicKey, error) {
	fragment = strings.TrimPrefix(fragment, "#")
	if fragment == "" {
		fragment = "atproto"
	}
	k, ok := id.Keys[fragment]
	if !ok {
		return nil, errors.New("identity: no key for fragment " + fragment)
	}
	return crypto.ParsePublicMultibase(k.Multibase)
}

// DIDDocument is the raw JSON structure from a PLC directory or did:web resolution.
type DIDDocument struct {
	ID                 string               `json:"id"`
	AlsoKnownAs        []string             `json:"alsoKnownAs"`
	VerificationMethod []VerificationMethod `json:"verificationMethod"`
	Service            []Service            `json:"service"`
}

// VerificationMethod is a key entry in a DID document.
type VerificationMethod struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	Controller         string `json:"controller"`
	PublicKeyMultibase string `json:"publicKeyMultibase"`
}

// Service is a service entry in a DID document.
type Service struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	// ServiceEndpoint is the endpoint URL. Per the DID-core spec (and the TS
	// reference), serviceEndpoint may be either a string or an object; the
	// object form is captured as "" here rather than failing the document parse.
	ServiceEndpoint string `json:"-"`
}

// UnmarshalJSON tolerates serviceEndpoint being either a JSON string (the
// atproto form) or an object/array. A non-string endpoint yields an empty
// ServiceEndpoint instead of failing the whole DID document parse, matching the
// DID-core union type z.union([z.string(), z.record(z.unknown())]).
func (s *Service) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID              string          `json:"id"`
		Type            string          `json:"type"`
		ServiceEndpoint json.RawMessage `json:"serviceEndpoint"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.ID = raw.ID
	s.Type = raw.Type
	// Only interpret a JSON-string endpoint as a URL; ignore the object form.
	var url string
	if json.Unmarshal(raw.ServiceEndpoint, &url) == nil {
		s.ServiceEndpoint = url
	}
	return nil
}

// ParseDIDDocument parses a DID document from JSON bytes.
func ParseDIDDocument(data []byte) (*DIDDocument, error) {
	var doc DIDDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

// IdentityFromDocument extracts an Identity from a parsed DID document.
func IdentityFromDocument(doc *DIDDocument) (*Identity, error) {
	did, err := atmos.ParseDID(doc.ID)
	if err != nil {
		return nil, err
	}

	id := &Identity{
		DID:      did,
		Handle:   atmos.HandleInvalid,
		Keys:     make(map[string]Key),
		Services: make(map[string]ServiceEndpoint),
	}

	// Extract handle from alsoKnownAs.
	for _, aka := range doc.AlsoKnownAs {
		if strings.HasPrefix(aka, "at://") {
			raw := aka[len("at://"):]
			h, err := atmos.ParseHandle(raw)
			if err == nil {
				id.Handle = h.Normalize()
				break
			}
		}
	}

	// Extract keys.
	for _, vm := range doc.VerificationMethod {
		fragment := fragmentFromID(vm.ID)
		if fragment == "" {
			continue
		}
		id.Keys[fragment] = Key{
			Type:      vm.Type,
			Multibase: vm.PublicKeyMultibase,
		}
	}

	// Extract services.
	for _, svc := range doc.Service {
		fragment := fragmentFromID(svc.ID)
		if fragment == "" {
			continue
		}
		id.Services[fragment] = ServiceEndpoint{
			Type: svc.Type,
			URL:  svc.ServiceEndpoint,
		}
	}

	return id, nil
}

// fragmentFromID extracts the fragment from an ID like "did:plc:abc#atproto" → "atproto"
// or "#atproto" → "atproto".
func fragmentFromID(id string) string {
	idx := strings.LastIndex(id, "#")
	if idx < 0 || idx == len(id)-1 {
		return ""
	}
	return id[idx+1:]
}
