package plc

import (
	"errors"
	"maps"
	"slices"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

// CreateParams holds the parameters for creating a new DID.
type CreateParams struct {
	SigningKey   crypto.PrivateKey
	RotationKeys []crypto.PublicKey
	Handle       atmos.Handle
	PDS          string
}

// CreateDID builds, signs, and returns a genesis operation and computed DID.
func CreateDID(params CreateParams) (*Operation, atmos.DID, error) {
	if params.SigningKey == nil {
		return nil, "", errors.New("plc: signing key is required")
	}
	if len(params.RotationKeys) == 0 {
		return nil, "", errors.New("plc: at least one rotation key is required")
	}

	rotKeys := make([]string, len(params.RotationKeys))
	for i, k := range params.RotationKeys {
		rotKeys[i] = k.DIDKey()
	}

	op := &Operation{
		Type:         "plc_operation",
		RotationKeys: rotKeys,
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

	if err := op.Sign(params.SigningKey); err != nil {
		return nil, "", err
	}

	did, err := op.DID()
	if err != nil {
		return nil, "", err
	}

	return op, did, nil
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
