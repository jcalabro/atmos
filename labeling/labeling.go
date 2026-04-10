// Package labeling provides label creation, signing, and verification
// for ATProto content labels. It does not provide utilities for subscribing
// to a labeler (see the [streaming] package for that).
package labeling

import (
	"errors"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/gt"
)

// ErrNotSigned is returned when verifying a label that has no signature.
var ErrNotSigned = errors.New("labeling: label is not signed")

// New creates a label with required fields, ver=1, and cts set to now.
// Does not sign.
func New(src atmos.DID, uri string, val string) *comatproto.LabelDefs_Label {
	return NewAt(src, uri, val, time.Now().UTC())
}

// NewAt creates a label with required fields, ver=1, and the given timestamp.
// Does not sign.
func NewAt(src atmos.DID, uri string, val string, cts time.Time) *comatproto.LabelDefs_Label {
	return &comatproto.LabelDefs_Label{
		Src: string(src),
		URI: uri,
		Val: val,
		Cts: cts.UTC().Format(time.RFC3339),
		Ver: gt.Some(int64(1)),
	}
}

// Negate creates a negation label with cts set to now. Does not sign.
func Negate(src atmos.DID, uri string, val string) *comatproto.LabelDefs_Label {
	return NegateAt(src, uri, val, time.Now().UTC())
}

// NegateAt creates a negation label with the given timestamp. Does not sign.
func NegateAt(src atmos.DID, uri string, val string, cts time.Time) *comatproto.LabelDefs_Label {
	l := NewAt(src, uri, val, cts)
	l.Neg = gt.Some(true)
	return l
}

// UnsignedBytes returns the DAG-CBOR encoding with sig cleared.
// Safe for concurrent use on the same label.
func UnsignedBytes(label *comatproto.LabelDefs_Label) ([]byte, error) {
	// Copy the label to avoid mutating the original (thread safety).
	cp := *label
	cp.Sig = nil
	cp.LexiconTypeID = "" // Labels are signed without $type per the label spec.
	data, err := cp.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("labeling: marshal unsigned: %w", err)
	}
	return data, nil
}

// Sign signs a label by encoding it to DAG-CBOR with sig=nil,
// signing the bytes, and setting label.Sig.
func Sign(label *comatproto.LabelDefs_Label, key crypto.PrivateKey) error {
	unsigned, err := UnsignedBytes(label)
	if err != nil {
		return err
	}
	sig, err := key.HashAndSign(unsigned)
	if err != nil {
		return fmt.Errorf("labeling: sign: %w", err)
	}
	label.Sig = sig
	return nil
}

// Verify checks the label signature against a public key.
func Verify(label *comatproto.LabelDefs_Label, key crypto.PublicKey) error {
	if len(label.Sig) == 0 {
		return ErrNotSigned
	}
	unsigned, err := UnsignedBytes(label)
	if err != nil {
		return err
	}
	return key.HashAndVerify(unsigned, label.Sig)
}
