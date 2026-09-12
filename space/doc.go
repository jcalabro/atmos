// Package space implements AT Protocol permissioned-repository primitives.
//
// Commit verification authenticates an author's repo claim only when the
// commit was obtained directly from that author's authenticated, resolved repo
// host. A copied CAR remains internally verifiable but is not independent
// proof of authorship because readers receive the symmetric commit key.
//
// AlphaCARLimits supplies the measured bounded alpha profile. See
// OPERATIONS.md for its operating assumptions, interoperability evidence, and
// unresolved release blockers.
package space
