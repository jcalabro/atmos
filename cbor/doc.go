// Package cbor implements a DAG-CBOR codec for the AT Protocol.
//
// DAG-CBOR is a strict subset of CBOR with deterministic encoding rules:
// map keys must be strings sorted by CBOR-encoded bytes, integers and lengths
// use minimal encoding, only tag 42 (CID links) is allowed, and no
// indefinite-length items are permitted.
package cbor
