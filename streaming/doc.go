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
// override via [Options.SyncClient] or set [Options.DisableAutoResync].
//
// Sync 1.1 verification is auto-attached for firehose streams: every #commit
// is verified for signature, MST inversion, and chain continuity before its
// ops reach the consumer. On chain break or inversion failure, a background
// resync is triggered against the account's PDS; the resync ops eventually
// arrive on the consumer's iterator as [ActionResync] operations,
// transparently chunked into 100-op events. Async resync is invisible to
// consumers: the ops flow through the same Event.Operations() path as
// regular commits, and any background errors (resync failures, buffer
// overflows) flow through the iterator's error slot like any other stream
// error. To opt out, supply Options.Verifier = gt.Some[*sync.Verifier](nil)
// or supply your own configured *sync.Verifier.
//
// Events are delivered in batches for efficient bulk processing. The
// [Options.BatchSize] and [Options.BatchTimeout] fields control batching
// behavior (defaults: 50 events, 500ms). Each yield from [Client.Events]
// delivers a slice of 1 to BatchSize events. Batches flush when full, when
// the timeout elapses, or when an error (decode error, sequence gap,
// verifier error) is encountered — in which case the partial batch is
// yielded first, followed by the error.
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
// Events are delivered at least once in batches. In rare cases during leader
// failover, the same event may be emitted more than once. Consumers must
// handle events idempotently.
//
// When the client auto-attaches a verifier (no Options.Verifier supplied),
// Client.Close() shuts the verifier down too. User-supplied verifiers are
// the caller's responsibility to close.
//
// # Parallel verification (Sync 1.1)
//
// When [Options.Verifier] is configured (or auto-attached by
// NewClient), per-event verification can dominate the readLoop's wall
// clock — primarily because each event's signature check resolves
// the DID through the identity directory, which round-trips to
// plc.directory on cache miss (~30ms). To keep up with line rate,
// the client runs verification on a per-DID FIFO worker pool sized
// by [Options.Parallelism] (default 32). Workers process events
// concurrently across DIDs while preserving same-DID order.
//
// Delivery semantics under Parallelism > 1:
//
//   - Events for the same DID are delivered in seq order.
//   - Events for different DIDs may interleave: an event with seq=N
//     may yield AFTER an event with seq=N+1 if they belong to
//     different DIDs.
//   - The cursor advances to a watermark equal to the smallest seq
//     still in flight, minus 1. Restarting the consumer resumes from
//     this point; some events that completed after the watermark
//     holder may be re-delivered (at-least-once).
//   - [GapError] is fired on global seq gaps observed in the relay's
//     monotonic counter, the same way as in serial mode. The dispatch
//     goroutine reads frames single-threaded, so global ordering is
//     visible even though verification runs concurrently.
//   - Per-DID queue overflow surfaces as [*DropError] on the
//     consumer's iter (alongside [GapError], [DecodeError], and
//     verifier errors).
//
// To preserve the strict global-seq behavior of pre-1.2 atmos, set
// Parallelism to 1.
package streaming
