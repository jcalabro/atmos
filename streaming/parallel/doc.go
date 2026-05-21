// Package parallel provides a per-key FIFO work scheduler used by the
// streaming client to parallelize verifier-bound work across DIDs while
// preserving same-DID order. Mirrors
// indigo/events/schedulers/parallel.Scheduler.
package parallel
