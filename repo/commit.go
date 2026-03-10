package repo

import (
	"errors"
	"fmt"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
)

// Commit is a signed ATProto repository commit.
type Commit struct {
	DID     string    // "did"
	Version int64     // "version"
	Prev    *cbor.CID // "prev" — always serialized, even as null
	Data    cbor.CID  // "data" — MST root CID
	Rev     string    // "rev" — TID revision
	Sig     []byte    // "sig" — signature bytes
}

// Precomputed CBOR key bytes for commit fields.
// DAG-CBOR key order (shorter first, then lex): did(3), rev(3), sig(3), data(4), prev(4), version(7).
var (
	commitKeyDID     = cbor.AppendTextKey(nil, "did")
	commitKeyRev     = cbor.AppendTextKey(nil, "rev")
	commitKeySig     = cbor.AppendTextKey(nil, "sig")
	commitKeyData    = cbor.AppendTextKey(nil, "data")
	commitKeyPrev    = cbor.AppendTextKey(nil, "prev")
	commitKeyVersion = cbor.AppendTextKey(nil, "version")
)

// encodeCommit encodes a commit to DAG-CBOR bytes.
func encodeCommit(c *Commit) ([]byte, error) {
	// Pre-size: map(6) header + 6 keys + values ≈ 256 bytes typical
	buf := make([]byte, 0, 256+len(c.DID)+len(c.Rev)+len(c.Sig))

	buf = cbor.AppendMapHeader(buf, 6)

	// "did"
	buf = append(buf, commitKeyDID...)
	buf = cbor.AppendText(buf, c.DID)

	// "rev"
	buf = append(buf, commitKeyRev...)
	buf = cbor.AppendText(buf, c.Rev)

	// "sig"
	buf = append(buf, commitKeySig...)
	buf = cbor.AppendBytes(buf, c.Sig)

	// "data"
	buf = append(buf, commitKeyData...)
	buf = cbor.AppendCIDLink(buf, &c.Data)

	// "prev"
	buf = append(buf, commitKeyPrev...)
	if c.Prev != nil {
		buf = cbor.AppendCIDLink(buf, c.Prev)
	} else {
		buf = cbor.AppendNull(buf)
	}

	// "version"
	buf = append(buf, commitKeyVersion...)
	buf = cbor.AppendUint(buf, uint64(c.Version))

	return buf, nil
}

// UnsignedBytes returns the DAG-CBOR encoding of the commit without the sig field.
func (c *Commit) UnsignedBytes() ([]byte, error) {
	buf := make([]byte, 0, 192+len(c.DID)+len(c.Rev))

	buf = cbor.AppendMapHeader(buf, 5)

	// "did"
	buf = append(buf, commitKeyDID...)
	buf = cbor.AppendText(buf, c.DID)

	// "rev"
	buf = append(buf, commitKeyRev...)
	buf = cbor.AppendText(buf, c.Rev)

	// "data"
	buf = append(buf, commitKeyData...)
	buf = cbor.AppendCIDLink(buf, &c.Data)

	// "prev"
	buf = append(buf, commitKeyPrev...)
	if c.Prev != nil {
		buf = cbor.AppendCIDLink(buf, c.Prev)
	} else {
		buf = cbor.AppendNull(buf)
	}

	// "version"
	buf = append(buf, commitKeyVersion...)
	buf = cbor.AppendUint(buf, uint64(c.Version))

	return buf, nil
}

// Sign signs the commit with the given private key.
func (c *Commit) Sign(key crypto.PrivateKey) error {
	unsigned, err := c.UnsignedBytes()
	if err != nil {
		return fmt.Errorf("repo: encoding unsigned commit: %w", err)
	}
	sig, err := key.HashAndSign(unsigned)
	if err != nil {
		return fmt.Errorf("repo: signing commit: %w", err)
	}
	c.Sig = sig
	return nil
}

// VerifySignature verifies the commit signature against the given public key.
func (c *Commit) VerifySignature(key crypto.PublicKey) error {
	if len(c.Sig) == 0 {
		return errors.New("repo: commit has no signature")
	}
	unsigned, err := c.UnsignedBytes()
	if err != nil {
		return fmt.Errorf("repo: encoding unsigned commit: %w", err)
	}
	return key.HashAndVerify(unsigned, c.Sig)
}

// decodeCommit decodes a commit from DAG-CBOR bytes.
func decodeCommit(data []byte) (*Commit, error) {
	val, err := cbor.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("repo: decoding commit: %w", err)
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil, errors.New("repo: commit is not a CBOR map")
	}

	c := &Commit{}

	verVal, ok := m["version"]
	if !ok {
		return nil, errors.New("repo: commit missing 'version'")
	}
	ver, ok := verVal.(int64)
	if !ok {
		return nil, errors.New("repo: commit 'version' is not an integer")
	}
	if ver != 2 && ver != 3 {
		return nil, fmt.Errorf("repo: unsupported commit version %d, expected 2 or 3", ver)
	}
	c.Version = ver

	didVal, ok := m["did"]
	if !ok {
		return nil, errors.New("repo: commit missing 'did'")
	}
	did, ok := didVal.(string)
	if !ok {
		return nil, errors.New("repo: commit 'did' is not a string")
	}
	c.DID = did

	dataVal, ok := m["data"]
	if !ok {
		return nil, errors.New("repo: commit missing 'data'")
	}
	dataCID, ok := dataVal.(cbor.CID)
	if !ok {
		return nil, errors.New("repo: commit 'data' is not a CID")
	}
	c.Data = dataCID

	if prevVal, ok := m["prev"]; ok && prevVal != nil {
		prevCID, ok := prevVal.(cbor.CID)
		if !ok {
			return nil, errors.New("repo: commit 'prev' is not a CID")
		}
		c.Prev = &prevCID
	}

	if revVal, ok := m["rev"]; ok {
		rev, ok := revVal.(string)
		if !ok {
			return nil, errors.New("repo: commit 'rev' is not a string")
		}
		c.Rev = rev
	} else if ver == 3 {
		return nil, errors.New("repo: v3 commit missing required 'rev'")
	}

	if sigVal, ok := m["sig"]; ok {
		sig, ok := sigVal.([]byte)
		if !ok {
			return nil, errors.New("repo: commit 'sig' is not bytes")
		}
		c.Sig = sig
	} else if ver == 3 {
		return nil, errors.New("repo: v3 commit missing required 'sig'")
	}

	return c, nil
}
