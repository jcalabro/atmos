package xrpc

// Subprotocol is a versioned XRPC event-stream wire subprotocol token,
// negotiated at WebSocket connection time via the standard
// Sec-WebSocket-Protocol header (atproto proposal 0015).
//
// The client offers tokens in preference order; the server selects one
// it supports and echoes it in its Sec-WebSocket-Protocol response
// header. When nothing is negotiated (no header, or no recognized
// token), the connection uses the subprotocol declared by the
// subscription's lexicon document, which defaults to [SubprotocolV0CBOR]
// — preserving the behavior of every existing stream.
type Subprotocol string

const (
	// SubprotocolV0CBOR is the legacy default framing: each WebSocket
	// binary frame contains two concatenated DASL-CBOR objects, a header
	// ({op, t}) followed by a payload.
	SubprotocolV0CBOR Subprotocol = "xrpc.v0.cbor"

	// SubprotocolV1JSON is the v1 framing, JSON-encoded: each WebSocket
	// text frame contains exactly one self-describing JSON object
	// discriminated by its "$type" field ("message" or "error"). A
	// message frame's "payload" is the lexicon message itself with the
	// full "<nsid>#<fragment>" value in its "$type" field.
	SubprotocolV1JSON Subprotocol = "xrpc.v1.json"
)

// Valid reports whether s is a subprotocol token this build implements.
// The proposal also specifies "xrpc.v1.cbor", but it is not shipping in
// the near term and atmos does not implement it yet.
func (s Subprotocol) Valid() bool {
	switch s {
	case SubprotocolV0CBOR, SubprotocolV1JSON:
		return true
	}
	return false
}
