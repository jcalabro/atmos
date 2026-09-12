# Spaces alpha operations

The spaces packages are an alpha interoperability surface, not a production
release. They are pinned to atproto `9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33`
and Bulletin `0acf237b872c766a60cec597c5fc10f0b1a58b7f`. PR 5187 is
still an open draft, so this branch must not merge to `main` until the external
stability and production gate in `SPACES.md` is satisfied.

## Measured alpha profile

`space.AlphaCARLimits`, `sync.AlphaLimits`, and `host.AlphaLimits` form one
explicit starting profile:

- 100,000 records and 256 MiB per author-space repository;
- an 8 MiB monolithic index, 1 MiB per canonical record, and 64 MiB per
  incremental pass;
- 10,000 authors per space, 1,000 items per page, and two bounded recovery
  attempts;
- a two-minute author pass, 30-second cleanup, 32 sync and authority delivery
  workers, a 10,000-item coalescing sync queue, and bounded registration quotas
  and delivery retries.

On Linux/amd64 with Go 1.26.6 and an AMD Ryzen 9 9950X, five 100,000-record
index build/encode samples took 27.6–28.4 ms and allocated 25.9 MB. A
10,000-record sample took 2.53–2.65 ms and allocated 3.02 MB. These are local
capacity measurements, not latency SLOs. Network RTT, store transactions,
record sizes, downstream service latency, and concurrent watched spaces remain
deployment-specific. Operators must load-test their durable store and network,
and lower the profile when their SLO or memory budget requires it.

The scheduler exposes backpressure through `QueueFullError`, `JobResult`, and
`EventQueueSaturated`; a later sweep reconciles dropped hints. The authority
store returns `ErrQuota` and `ErrOutboxFull` atomically and the host emits
bounded delivery success/failure and queue events. Sync stages remain invisible
until verified promotion. Do not use unbounded queues or retry either ambiguous
writes or credential exchanges.

Native clients use pooled HTTP/1.1 and HTTP/2. Their signer runs once for every
transport connection attempt, including transparent standard-library retries,
so a DPoP proof is never reused for a second wire send. `NewCorrectnessHTTPClient`
remains available as an explicit no-reuse diagnostic baseline. Browser fetch
owns pooling and hidden retries under WebAssembly, so the embedding browser is
part of that transport trust boundary.

## Interoperability gate

Run `just test-spaces-interop`. It checks out the exact pins in a temporary
directory, verifies the atproto PDS space suites and Bulletin suites, starts two
synthetic PDSes and Bulletin, then exercises atmos management, cross-PDS writes,
credential exchange, notification-driven writer discovery, paginated reads,
and deletion. It also sends atmos-signed write and deletion notifications into
Bulletin's independent DID/service-JWT verifier and checks wrong-method denial.
CI runs this same command in its required `spaces-interop` job with blocked-by-
default egress, immutable Actions, exact pnpm versions and no shared caches.
Existing clean checkouts can be reused with
`ATMOS_ATPROTO_CHECKOUT` and `ATMOS_BULLETIN_CHECKOUT`. Set
`ATMOS_KEEP_INTEROP_ROOT=1` to retain logs.

The gate sends no public writes and uses only the dev network's synthetic
accounts. `EndpointPolicy.AllowPrivateNetworks` and the corresponding client
network policy exist solely for such local topologies; they disable the native
SSRF boundary and must not be enabled for attacker-selected production URLs.

## Unresolved external release blockers

Two pinned PDS behaviors prevent a production claim:

1. A blob uploaded and referenced only by a permissioned record is returned by
   unauthenticated `com.atproto.sync.getBlob`. The isolated reproduction
   returned HTTP 200 and the exact body. Treat space blob bytes as public to
   anyone who learns the CID until the reference PDS establishes and enforces a
   private perimeter.
2. A remote writer's first hop calls `resolveNotifyTarget` with the bare
   authority DID. The pinned implementation consequently selects
   `#atproto_pds`, not `#atproto_space_host`. A separately hosted atmos authority
   therefore cannot receive the write notification. Periodic polling cannot
   discover a writer whose first notification was lost.

Atmos resolves dedicated authority endpoints correctly and does not add a
fallback that would misroute credentials or pretend notification delivery
succeeded. Re-run the pinned-stack gate and both blocker reproductions after an
upstream pin change.

## Persistence and migration limits

The supplied memory stores are bounded test/example implementations. Production
embedders own durable store migrations, backups, multi-replica CAS semantics,
replay retention, and rollback. There is no pinned space-repo import API and
atmos deliberately does not implement a repo-host server, so it promises no
permissioned-repo migration. Authority tombstones permanently reserve local
space URIs. Sync outbox consumers are at-least-once and must preserve event-ID
deduplication across their own migrations.

Never log credentials, proofs, keys, record bodies, or blob bodies. Space URIs,
DIDs, and CIDs may also be sensitive or high-cardinality; keep them out of
metric labels and apply caller-controlled redaction to operational events.
