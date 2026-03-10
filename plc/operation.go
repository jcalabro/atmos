// Package plc provides a client for the DID PLC directory.
//
// DID PLC is the primary DID method in ATProto. The PLC directory stores
// a chain of signed operations declaring each DID's signing keys, rotation
// keys, handle, and PDS endpoint.
package plc

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
)

var (
	// ErrNotSigned is returned when an operation requires a signature but has none.
	ErrNotSigned = errors.New("plc: operation is not signed")
	// ErrNotFound is returned when a DID does not exist in the PLC directory.
	ErrNotFound = errors.New("plc: DID not found")
)

// Operation is a PLC operation (plc_operation type).
type Operation struct {
	Type                string             `json:"type"`
	RotationKeys        []string           `json:"rotationKeys"`
	VerificationMethods map[string]string  `json:"verificationMethods"`
	AlsoKnownAs         []string           `json:"alsoKnownAs"`
	Services            map[string]Service `json:"services"`
	Prev                *string            `json:"prev"`
	Sig                 *string            `json:"sig,omitempty"`
}

// Service is a service entry in a PLC operation.
type Service struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

// TombstoneOp deactivates a DID permanently.
type TombstoneOp struct {
	Type string  `json:"type"`
	Prev string  `json:"prev"`
	Sig  *string `json:"sig,omitempty"`
}

// LogEntry is a single entry from the audit log.
type LogEntry struct {
	DID       string          `json:"did"`
	Operation json.RawMessage `json:"operation"`
	CID       string          `json:"cid"`
	Nullified bool            `json:"nullified"`
	CreatedAt string          `json:"createdAt"`
}

var base32Lower = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// UnsignedBytes returns the DAG-CBOR encoding with sig omitted (for signing).
func (op *Operation) UnsignedBytes() ([]byte, error) {
	return cbor.Marshal(op.toMap(false))
}

// SignedBytes returns the DAG-CBOR encoding with sig included.
func (op *Operation) SignedBytes() ([]byte, error) {
	if op.Sig == nil {
		return nil, ErrNotSigned
	}
	return cbor.Marshal(op.toMap(true))
}

// toMap converts the operation to a map[string]any for CBOR marshaling.
func (op *Operation) toMap(includeSig bool) map[string]any {
	m := map[string]any{
		"type":                op.Type,
		"rotationKeys":        toAnySlice(op.RotationKeys),
		"verificationMethods": toAnyMap(op.VerificationMethods),
		"alsoKnownAs":         toAnySlice(op.AlsoKnownAs),
		"services":            servicesToAny(op.Services),
	}
	if op.Prev != nil {
		m["prev"] = *op.Prev
	} else {
		m["prev"] = nil
	}
	if includeSig && op.Sig != nil {
		m["sig"] = *op.Sig
	}
	return m
}

// Sign signs the operation with the given rotation key.
func (op *Operation) Sign(key crypto.PrivateKey) error {
	unsigned, err := op.UnsignedBytes()
	if err != nil {
		return fmt.Errorf("plc: marshal unsigned: %w", err)
	}
	sig, err := key.HashAndSign(unsigned)
	if err != nil {
		return fmt.Errorf("plc: sign: %w", err)
	}
	s := base64.RawURLEncoding.EncodeToString(sig)
	op.Sig = &s
	return nil
}

// Verify checks the signature against a public key.
func (op *Operation) Verify(key crypto.PublicKey) error {
	if op.Sig == nil {
		return ErrNotSigned
	}
	sig, err := base64.RawURLEncoding.DecodeString(*op.Sig)
	if err != nil {
		return fmt.Errorf("plc: decode signature: %w", err)
	}
	unsigned, err := op.UnsignedBytes()
	if err != nil {
		return fmt.Errorf("plc: marshal unsigned: %w", err)
	}
	return key.HashAndVerify(unsigned, sig)
}

// CID computes the CID of the signed operation (dag-cbor, sha-256).
func (op *Operation) CID() (string, error) {
	data, err := op.SignedBytes()
	if err != nil {
		return "", err
	}
	c := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	return c.String(), nil
}

// DID computes the did:plc: identifier from a signed genesis operation.
// The signed CBOR bytes are SHA-256 hashed, base32-lower encoded, and
// truncated to 24 characters.
func (op *Operation) DID() (atmos.DID, error) {
	data, err := op.SignedBytes()
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	suffix := base32Lower.EncodeToString(hash[:])[:24]
	return atmos.ParseDID("did:plc:" + suffix)
}

// Doc converts the operation to a DID document. Keys that fail to parse
// are silently omitted, matching PLC directory behavior.
func (op *Operation) Doc(did atmos.DID) *identity.DIDDocument {
	aka := make([]string, len(op.AlsoKnownAs))
	copy(aka, op.AlsoKnownAs)

	doc := &identity.DIDDocument{
		ID:          string(did),
		AlsoKnownAs: aka,
	}

	// Sort verification method names for deterministic output.
	vmNames := make([]string, 0, len(op.VerificationMethods))
	for name := range op.VerificationMethods {
		vmNames = append(vmNames, name)
	}
	sort.Strings(vmNames)

	for _, name := range vmNames {
		pub, err := crypto.ParsePublicDIDKey(op.VerificationMethods[name])
		if err != nil {
			continue
		}
		doc.VerificationMethod = append(doc.VerificationMethod, identity.VerificationMethod{
			ID:                 string(did) + "#" + name,
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: pub.Multibase(),
		})
	}

	// Sort service IDs for deterministic output.
	svcIDs := make([]string, 0, len(op.Services))
	for id := range op.Services {
		svcIDs = append(svcIDs, id)
	}
	sort.Strings(svcIDs)

	for _, id := range svcIDs {
		svc := op.Services[id]
		doc.Service = append(doc.Service, identity.Service{
			ID:              "#" + id,
			Type:            svc.Type,
			ServiceEndpoint: svc.Endpoint,
		})
	}

	return doc
}

// UnsignedBytes returns the DAG-CBOR encoding of a tombstone with sig omitted.
func (t *TombstoneOp) UnsignedBytes() ([]byte, error) {
	return cbor.Marshal(map[string]any{
		"type": t.Type,
		"prev": t.Prev,
	})
}

// SignedBytes returns the DAG-CBOR encoding of a tombstone with sig included.
func (t *TombstoneOp) SignedBytes() ([]byte, error) {
	if t.Sig == nil {
		return nil, ErrNotSigned
	}
	return cbor.Marshal(map[string]any{
		"type": t.Type,
		"prev": t.Prev,
		"sig":  *t.Sig,
	})
}

// Sign signs the tombstone with the given rotation key.
func (t *TombstoneOp) Sign(key crypto.PrivateKey) error {
	unsigned, err := t.UnsignedBytes()
	if err != nil {
		return fmt.Errorf("plc: marshal unsigned: %w", err)
	}
	sig, err := key.HashAndSign(unsigned)
	if err != nil {
		return fmt.Errorf("plc: sign: %w", err)
	}
	s := base64.RawURLEncoding.EncodeToString(sig)
	t.Sig = &s
	return nil
}

// Verify checks the tombstone signature against a public key.
func (t *TombstoneOp) Verify(key crypto.PublicKey) error {
	if t.Sig == nil {
		return ErrNotSigned
	}
	sig, err := base64.RawURLEncoding.DecodeString(*t.Sig)
	if err != nil {
		return fmt.Errorf("plc: decode signature: %w", err)
	}
	unsigned, err := t.UnsignedBytes()
	if err != nil {
		return fmt.Errorf("plc: marshal unsigned: %w", err)
	}
	return key.HashAndVerify(unsigned, sig)
}

func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func toAnyMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func servicesToAny(m map[string]Service) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = map[string]any{
			"type":     v.Type,
			"endpoint": v.Endpoint,
		}
	}
	return out
}
