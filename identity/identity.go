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
	k, ok := id.Keys["atproto"]
	if !ok {
		return nil, errors.New("identity: no atproto key")
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
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
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
