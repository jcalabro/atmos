// Package atmos provides syntax types for the AT Protocol: DID, Handle, NSID,
// AT-URI, TID, RecordKey, Datetime, Language, and URI. Each type is a validated
// string with a Parse constructor that enforces the protocol's syntax rules.
//
// All types implement [encoding.TextMarshaler] and [encoding.TextUnmarshaler],
// making them safe for use in JSON, CBOR, and query parameter serialization.
package atmos
