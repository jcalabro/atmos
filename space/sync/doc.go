// Package sync durably replicates permissioned AT Protocol repositories.
//
// A Syncer publishes only complete, cryptographically verified generations.
// Discovery and notification data are hints; record bodies and commits are
// always obtained directly from the author's strictly resolved repository host.
// Store implementations must provide compare-and-swap promotion and atomically
// enqueue the corresponding outbox event.
package sync
