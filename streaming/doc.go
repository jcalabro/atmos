// Package streaming provides a client for consuming ATProto event streams
// (the "firehose"). It handles WebSocket connection management, automatic
// reconnection with exponential backoff, cursor tracking, and decoding of
// repository commit events into individual record operations.
//
// For high availability deployments, the client supports optional distributed
// lock coordination via the [DistributedLocker] interface. When configured,
// only the lock holder consumes from the firehose while other nodes wait idle,
// automatically taking over if the active consumer fails.
//
// Events are delivered at least once. In rare cases during leader failover,
// the same event may be emitted more than once. Consumers must handle events
// idempotently.
package streaming
