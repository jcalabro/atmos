package plc

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

// MaxRotationKeys is the spec-mandated upper bound on rotation key count.
// Per the PLC v0.1 spec: "must include at least 1 key and at most 5 keys,
// with no duplication".
const MaxRotationKeys = 5

// CreateParams holds the parameters for creating a new DID.
//
// SigningKey populates verificationMethods["atproto"] only — it does NOT
// sign the genesis operation. Per the PLC v0.1 spec, every operation
// (including genesis) is signed by a rotation key.
//
// RotationKeys are private keys because CreateDID signs the genesis op
// with RotationKeys[0] (the highest-authority rotation key). The public
// did:key forms are derived from each entry's PublicKey().DIDKey().
type CreateParams struct {
	SigningKey   crypto.PrivateKey
	RotationKeys []crypto.PrivateKey
	Handle       atmos.Handle
	PDS          string
}

// CreateDID builds, signs, and returns a genesis operation and computed DID.
//
// The genesis op is signed by RotationKeys[0]. SigningKey is used only to
// populate verificationMethods["atproto"]. This matches PLC v0.1: rotation
// keys are the only keys with authority over the chain.
func CreateDID(params CreateParams) (*Operation, atmos.DID, error) {
	if params.SigningKey == nil {
		return nil, "", errors.New("plc: signing key is required")
	}
	if len(params.RotationKeys) == 0 {
		return nil, "", errors.New("plc: at least one rotation key is required")
	}
	if len(params.RotationKeys) > MaxRotationKeys {
		return nil, "", fmt.Errorf("plc: at most %d rotation keys allowed, got %d", MaxRotationKeys, len(params.RotationKeys))
	}
	for i, k := range params.RotationKeys {
		if k == nil {
			return nil, "", fmt.Errorf("plc: rotation key %d is nil", i)
		}
	}

	rotKeyDIDs := make([]string, len(params.RotationKeys))
	seen := make(map[string]struct{}, len(params.RotationKeys))
	for i, k := range params.RotationKeys {
		didKey := k.PublicKey().DIDKey()
		if _, dup := seen[didKey]; dup {
			return nil, "", fmt.Errorf("plc: duplicate rotation key at index %d", i)
		}
		seen[didKey] = struct{}{}
		rotKeyDIDs[i] = didKey
	}

	if err := params.Handle.Validate(); err != nil {
		return nil, "", fmt.Errorf("plc: invalid handle: %w", err)
	}
	if err := validatePDSEndpoint(params.PDS); err != nil {
		return nil, "", err
	}

	op := &Operation{
		Type:         "plc_operation",
		RotationKeys: rotKeyDIDs,
		VerificationMethods: map[string]string{
			"atproto": params.SigningKey.PublicKey().DIDKey(),
		},
		AlsoKnownAs: []string{"at://" + string(params.Handle)},
		Services: map[string]Service{
			"atproto_pds": {
				Type:     "AtprotoPersonalDataServer",
				Endpoint: params.PDS,
			},
		},
		Prev: nil,
	}

	signer := params.RotationKeys[0]
	if err := op.Sign(signer); err != nil {
		return nil, "", err
	}
	if err := op.Verify(signer.PublicKey()); err != nil {
		return nil, "", fmt.Errorf("plc: self-verify after sign: %w", err)
	}

	did, err := op.DID()
	if err != nil {
		return nil, "", err
	}

	return op, did, nil
}

// validatePDSEndpoint enforces the PLC spec rule that the atproto_pds
// endpoint be an http or https URL.
func validatePDSEndpoint(endpoint string) error {
	if endpoint == "" {
		return errors.New("plc: PDS endpoint is required")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("plc: invalid PDS endpoint: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("plc: PDS endpoint must be http(s), got scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("plc: PDS endpoint missing host")
	}
	return nil
}

// UpdateParams holds the parameters for updating a DID operation.
// Nil fields are inherited from the previous operation.
type UpdateParams struct {
	VerificationMethods map[string]string
	RotationKeys        []string
	AlsoKnownAs         []string
	Services            map[string]Service
}

// UpdateOp builds an update operation from the previous operation.
// Fields in params that are nil are inherited (copied) from prev.
func UpdateOp(prev *Operation, prevCID string, params UpdateParams) *Operation {
	op := &Operation{
		Type:                "plc_operation",
		RotationKeys:        slices.Clone(prev.RotationKeys),
		VerificationMethods: copyMap(prev.VerificationMethods),
		AlsoKnownAs:         slices.Clone(prev.AlsoKnownAs),
		Services:            copyServiceMap(prev.Services),
		Prev:                &prevCID,
	}

	if params.VerificationMethods != nil {
		op.VerificationMethods = params.VerificationMethods
	}
	if params.RotationKeys != nil {
		op.RotationKeys = params.RotationKeys
	}
	if params.AlsoKnownAs != nil {
		op.AlsoKnownAs = params.AlsoKnownAs
	}
	if params.Services != nil {
		op.Services = params.Services
	}

	return op
}

// NewTombstoneOp builds a tombstone operation.
func NewTombstoneOp(prevCID string) *TombstoneOp {
	return &TombstoneOp{
		Type: "plc_tombstone",
		Prev: prevCID,
	}
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)
	return out
}

func copyServiceMap(m map[string]Service) map[string]Service {
	out := make(map[string]Service, len(m))
	maps.Copy(out, m)
	return out
}
