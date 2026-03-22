// Package streaming provides a client for consuming ATProto event streams.
// It supports three stream types, auto-detected from the WebSocket URL:
//
//   - Repository firehose (com.atproto.sync.subscribeRepos) — CBOR binary frames
//   - Label streams (com.atproto.label.subscribeLabels) — CBOR binary frames
//   - Jetstream (URLs ending in /subscribe) — JSON text frames
//
// The client handles connection management, automatic reconnection with
// exponential backoff, cursor tracking, and decoding of events.
//
// For repository events, use [Event.Operations] to iterate over record
// mutations. When a #sync event arrives (indicating a broken commit chain),
// Operations automatically re-fetches the full repository and yields every
// record as an [ActionResync] operation. This behavior is enabled by default;
// override via [Options.SyncClient] or disable with gt.Some[*sync.Client](nil).
//
// For label events, use [Event.Labels] to access the individual
// labels — including negation labels (Neg=true) that revoke a previous label.
//
// For Jetstream events, [Event.Jetstream] provides the decoded JSON envelope
// including [JetstreamCommit] with the record payload as json.RawMessage.
// Account and identity events also populate the existing [Event.Account] and
// [Event.Identity] fields for compatibility. Use [Options.Collections] and
// [Options.DIDs] to filter Jetstream subscriptions by collection or DID.
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
