// Package streaming provides a client for consuming ATProto event streams.
// It supports both the repository firehose (com.atproto.sync.subscribeRepos)
// and label streams (com.atproto.label.subscribeLabels), auto-detected from
// the WebSocket URL. It handles connection management, automatic reconnection
// with exponential backoff, cursor tracking, and decoding of events.
//
// For repository events, use [Event.Operations] to iterate over record
// mutations. For label events, use [Event.Labels] to access the individual
// labels — including negation labels (Neg=true) that revoke a previous label.
//
// For high availability deployments, the client supports optional distributed
// lock coordination via the [DistributedLocker] interface. When configured,
// only the lock holder consumes from the stream while other nodes wait idle,
// automatically taking over if the active consumer fails.
//
// Events are delivered at least once. In rare cases during leader failover,
// the same event may be emitted more than once. Consumers must handle events
// idempotently.
package streaming
