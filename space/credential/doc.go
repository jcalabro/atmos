// Package credential implements the authentication primitives used by AT
// Protocol spaces: strict JWT profiles, replay protection, DPoP proofs, and
// DID-document signing-key selection.
//
// Parsing and verification are deliberately separate operations. An
// [UnverifiedToken] is attacker-controlled data and must never be treated as an
// authorization decision. Only the profile-specific verification functions
// produce a [VerifiedToken].
package credential
