# Spaces in atmos

Design and implementation tracker for [AT Protocol spaces][proposal], on
`jc/spaces`.

Status: Phases 0–4 implemented and verified on `jc/spaces`, 2026-09-11.
Phase 5 has not started. The checkboxes below track implementation; the earlier
review experiments remain separate evidence. Settled and deferred decisions
are recorded at the end.

Spaces are an early alpha. [PR #5187][pr] was still open at review time and its
head still matched `9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33`. Implement against
that snapshot, with explicit departures recorded here. Treat upstream code as
interoperability evidence, not as a security or correctness oracle for every
edge case. Recheck the proposal, handlers, Lexicons, and tests together when
updating the pin.

Do not merge this branch to `main` until the spaces APIs are stable and spaces
are live in production. The [alpha announcement][alpha] targets later in 2026;
that is an upstream goal, not a release guarantee. Keep this work off `main`'s
critical path.

## References and review evidence

- [Study guide][guide], retrieved 2026-09-11. Its implementation caveats are
  relevant to this design, not just background reading.
- [Proposal snapshot][proposal]: `119fa6b63476d30c2516846c714319046e0422f3`.
- [Reference implementation][upstream]: `9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33`.
  Read `packages/space/src`, its tests, the space/simplespace Lexicons, PDS
  handlers, auth verifier, client-attestation verifier, space store, and manager.
- [Bulletin snapshot][bulletin]: `0acf237b872c766a60cec597c5fc10f0b1a58b7f`.
  Read its actions, credential flow, access checks, and sync engine. It is a
  useful integration example; its storage and recovery shortcuts are not the
  contract for atmos.
- Rationale: [Reintroducing Spaces][rationale], [Off the Record][deniability],
  and [To Encrypt or Not to Encrypt][encryption].
- Changes: [alpha release thread][updates] and [invite discussion][invites].
  September 10 split read/write policies and replaced `addMember` with
  `putMember`.
- Cryptographic constructions: [RFC 9449][dpop-rfc], [RFC 7638][jkt-rfc],
  [RFC 5869 §2.3][hkdf-rfc].

Local contracts were inspected at atmos commit
`d157f14790124e518303150acddc4278cbf59cd9`. The review used downloaded source
snapshots and isolated programs outside the repository. It did not run the
reference PDS or its full test suite, contact the shared alpha with test writes,
or establish production interoperability.

Experiments on Linux/amd64, Go 1.26.6 and Node 24.14.0:

| Experiment | Result and design consequence |
|---|---|
| `zeebo/blake3` v0.2.4 `Hasher.Digest()` versus the pinned TS `LtHash`, using its locked `@noble/hashes` 1.7.0 | Read 2048 XOF bytes and matched three digests after add/add/remove. The library supplies the required primitive; Phase 1 now retains the complete states, commit bytes and CARs as pinned fixtures. |
| Canonical index with 20,000 `com.example.post/00000`-shaped paths and DAG-CBOR CID links | Index is 1,280,003 bytes; the existing CAR reader rejects its 1,280,039-byte framed block against the 1 MiB cap. Spaces need bounded, per-reader index limits. |
| `oauth.Transport` outside a default gttp transport, receiving 503 then 200 | Two HTTP requests carried the identical DPoP proof. Disable retries beneath the signer. |
| Two logical GETs with gttp retries disabled; on the second, an HTTP/1 server consumes the request then closes the reused connection without a response | Go's transport made a third request with the second request's proof. `WithNoRetries()` alone is insufficient. Disabling keep-alives and HTTP/2 avoided the hidden replay in this HTTP/1 experiment, returning EOF to the caller instead. |
| `xrpc.Client.Procedure` receiving a truncated HTTP 200 body, with two attempts configured | The server received the POST twice. The response-read error branch retries even non-idempotent writes; this is a prerequisite fix before using that path. |

These are narrow reproductions, not performance measurements. Keep reproducible
regression tests with the implementation changes; do not mark those fixes done
because this document identifies them.

The same primitive experiment passed with `-tags purego` and compiled for
`GOOS=js GOARCH=wasm`; browser execution remains untested. Repository lint,
4,312 short tests, 6,132 race-enabled tests, and `just wasm` passed. The first
test run exposed an incomplete ignored Lexicon cache; restoring missing files
from the existing `lexgen.lock` pins brought it from 27 to 413 schemas without
replacing existing files or changing generated code/pins.

## What the protocol actually guarantees

A space is `(authority DID, type NSID, skey)`. Each author holds their own repo
in that space. The authority holds policy and a writer directory; it does not
hold all authors' record bodies. A permissioned record has two DID coordinates:

```text
Space:  at://{authority}/space/{type}/{skey}
Record: at://{authority}/space/{type}/{skey}/{author}/{collection}/{rkey}
```

The writer directory contains admitted writers' reported DIDs, revisions and
hashes. It is mutable discovery state maintained by notifications, not a signed
space-wide checkpoint. A successful write to an author's PDS, admission of that
writer by the authority, and display of the record by an application are three
separate outcomes. Local writes can succeed even when the authority does not
admit the writer or does not yet know the space. [Source: PDS handlers][space-handlers].

A repo's LtHash commits to the current set of
`"{collection}/{rkey}/{canonical_cid}"` strings. Its state is 2048 bytes: 1024
little-endian uint16 lanes, summed modulo 65536 over BLAKE3 XOF expansions.
The wire digest is `sha256(state)`. There are no inclusion proofs, tree
reconciliation, or cross-author transactions. LtHash is a multiset primitive:
the store must enforce one current CID per path and correct op application.
The digest is neither an idempotency mechanism nor a record count.

Commit signatures deliberately exclude the hash. Anyone holding the disclosed
`ikm` can construct a valid MAC for a different hash while keeping the signature.
Therefore **signature + MAC verification authenticates a repo claim only in the
context of a direct, authenticated connection to the author's resolved repo
host**. A copied CAR is internally verifiable but is not independent proof of
its contents' authorship. Do not accept arbitrary mirrors, CDN origins, or
cached exports as equivalent to that live trust context. An author or its host
can also equivocate; hash agreement proves neither freshness nor global order.
[Source: repo commit implementation][commit-source].

Sync is HTTP pull from authors' hosts, accelerated by best-effort notifications.
A complete verified repo checkpoint can be promised. A complete, instantly
current space-wide view cannot. Unknown writers whose first notification was
lost may never be discovered without another notification or an external source
of writer identities. Polling the same stale directory cannot repair that gap.

## Decisions and scope

Settled decisions retained from the initial plan:

- Work on `jc/spaces` until the release gate above is met.
- Keep permissioned repo and sync semantics in `atmos/space/...`; leave public
  `repo`, `mst`, and `sync` semantics intact. Reuse lower-level `crypto`, `cbor`,
  and `car` primitives after checking their contracts.
- Add distinct `SpaceRef` and `SpaceURI` types in `atmos`. Keep `ATURI`'s current
  public-repo parsing contract. Lexicon `at-uri` compatibility needs a separate
  general-format validator, described below.
- Expose distinct typed account and space-reader clients. Auth is selected by
  operation, not merely by whether an endpoint starts with `space`.
- Use `github.com/zeebo/blake3`; its XOF capability is confirmed above.
- Build an authority host library with credential issuance, writer directory,
  notifications, and simple-space management. Provide mountable handlers and
  functions plus an optional `xrpcserver` registration adapter.
- Accept caller-provided durable stores and ship bounded in-memory stores for
  tests/examples. Production durability and replay storage are caller duties
  enforced by explicit interfaces, not an implicit in-memory fallback.
- Reuse OAuth DPoP cryptographic helpers and shared HTTP construction. Keep
  space-specific exchange, credential renewal, and request policy separate
  from OAuth nonce/refresh policy.
- Resolve declarations at runtime through a caller-configured resolver and
  cache, with an in-memory LRU default. Fail on resolution/validation errors.

In scope: addressing, declarations, credentials, OAuth scope helpers, identity,
repo crypto and CAR, typed reads/writes, blobs, syncing, authority hosting,
simple-space management, and deletion/account-lifecycle integration hooks.

A full permissioned **repo-host server** is a separate undertaking from an
**authority host**: it needs authenticated account writes, record persistence,
oplog retention, blobs, snapshot exports and migration. Whether to include that
server in this effort was settled by Q1: it is excluded. The phases promise
client-side writes and reusable repo primitives, not a drop-in PDS.

Out of scope: E2EE, per-record audiences, spaces record relays/firehoses,
turnkey account migration, and merging to `main`. Lifecycle events from the
public firehose may still be supplied by the embedding app; excluding spaces
record streaming must not exclude account deletion/deactivation handling.

## Package and integration boundaries

```text
atmos/                     SpaceRef, SpaceURI, general Lexicon AT-URI validation
atmos/space/               LtHash, SignedCommit, RepoCommit, commit/CAR primitives
atmos/space/credential/    JWT profiles, exchange, credential/key lifecycle
atmos/space/declaration/   declaration validation, resolver, cache
atmos/space/client/        typed account/reader APIs, endpoint-bound request policy
atmos/space/sync/          scheduler, staging/checkpoint store contract, recovery
atmos/space/host/          authority state, policy, replay, notification delivery
atmos/space/simplespace/   management client helpers and policy types/adapters
```

This is a proposed dependency direction, not a promise of every exported name.
`atmos` stays independent of `space`. Core repo primitives do not depend on
OAuth, networking or generated endpoints. `credential` may import OAuth helpers;
`oauth` must not import `space/credential` in return. Put scope parsing/building
in `oauth`, with explicit inputs for declaration-based expansion. The host uses
management types without importing a package that imports the host.

Existing surfaces that require work:

| Package | Actual contract and planned integration |
|---|---|
| `lexicon` | `Parse` currently checks only the document envelope and drops unknown fields. Add/preserve `space` declaration fields and validate them explicitly; parsing JSON alone is not declaration validation. |
| `lexval` | `at-uri` currently calls `ParseATURI`; `space-ref` currently falls through the unknown-format path. Add both supported checks without weakening public-repo parsing. |
| `lexgen`, `cmd/lexgen`, update script | Merge input roots into one catalog and generation pass. Explicitly support space declarations and retain generated record decoders. |
| `api/comatproto` | Generated wire structs/functions stay low level. Strings and `*xrpc.Client` do not enforce auth separation or validate all wire values. Typed space wrappers validate at the boundary; never hand-edit generated code. |
| `oauth` | Reuse `CreateDPoPProof` and public JWK support; add RFC 7638 thumbprints. Its current client metadata struct lacks `jwks_uri`; it is not a complete authority-side attestation verifier. |
| `xrpc`, `gttp` | `HTTPClient`/`RoundTripper` injection can sign buffered and streaming logical attempts, but does not reach Go's hidden wire retries. A generic xrpc signing hook alone would not solve that either. Resolve the transport blocker below, fix unsafe response-read retries and ensure strict response caps. |
| `identity` | `IdentityFromDocument` collapses fragments into maps, losing duplicate entries, full IDs and key controllers. Validate raw documents before this loss, or expose a strict resolver path. Map lookup alone cannot enforce all proposed checks. |
| `serviceauth` | Reuse for service JWTs only, with required audience, method, expiry and replay checks. Its token profile is deliberately different from the three space JWT profiles. |
| `car`, `cbor` | CAR already supports multiple roots and verifies block CIDs. Its global 1 MiB `MaxBlockSize` cannot safely be changed per concurrent space download. Add reader-local limits; validate the space layout above CAR framing. |

The type-safety guarantee covers the new high-level clients. Existing generated
functions remain explicit low-level escape hatches. Do not expose a credential
through `xrpc.SetAuth`, export a space reader's underlying authenticated client,
or permit that reader to call arbitrary NSIDs or arbitrary URLs. Conversely,
account OAuth is intentionally allowed on its own repo's space paths.

## Addressing, declarations, and lexicon sourcing

### Addressing

`SpaceRef` requires all three coordinates, and `SpaceURI` additionally requires
all three record coordinates. Both authority and author must be DIDs, never
handles. Use the current DID/NSID/record-key syntax limits, a 512-byte skey and
rkey limit, and the 8192-byte URI cap. Reject empty segments, incomplete record
suffixes, query/fragment, trailing slash, and extra segments in these concrete
identity types. Preserve identity bytes; do not silently normalize signed
subjects or resolve handles into them.

The earlier suggestion to test authority-only or type-without-key as valid
space refs was wrong. Those are not complete space identities. Partial public
ATURIs retain their existing meaning. Separately expose a general Lexicon
`at-uri` validator that dispatches between public and space forms, because the
space APIs' **record** `uri` fields use `format: "at-uri"`. Test references
embedded in records, not just endpoint parameters. The pinned TS generic
validator also supports JSON-pointer fragments. As settled under Q2, the
general validator accepts valid RFC 6901 fragments, while strict `SpaceRef` and
`SpaceURI` identities remain fragment-free.

### Declarations and resolution

A declaration's `defs.main` must be `type: "space"` and preserve required
`name`, `key`, `collections` and optional `name:lang`/description. Validate NSID
identity, field types, supported key recommendation syntax and language tags;
bound names/localizations and match the alpha's name vectors (nonempty, at most
64 UTF-16 code units in its `lexSpace` parser). The proposal
requires `key`, while the alpha `lexSpace` parser makes it optional: rejecting
an absent key is an explicit local validation rule, not a claim about that
parser. Collections are NSIDs without `*`. `key` is a recommendation, and
`collections` define default OAuth **write** targets. Neither is permission to reject every
otherwise valid record outside the recommended shape/collections.

`com.atproto.lexicon.resolveLexicon` is an XRPC method on a configured service,
not a trustless resolver inferred from an NSID. Require a caller-selected
resolver endpoint or resolver implementation; document trust in its namespace
resolution. Do not silently choose a public service. Validate the returned
schema ID and declaration, retain URI/CID provenance, and do not claim that a
CID alone proves namespace authority. Full independent DNS/repository proof
resolution is not supplied by this cache.

Use an error-returning cache contract inspired by `identity.Cache`, rather than
copying its inability to report backend failures. Distinguish miss, cached
not-found, malformed declaration and resolver outage. Bound entries/bytes and
TTLs, coalesce concurrent lookups, copy mutable values, and isolate entries by
resolver trust configuration. `nil` means explicitly disabled caching, not
resolution failure. `Purge` must prevent an older in-flight fetch from
repopulating an invalidated entry. Do not serve stale declarations to widen a
grant after an error. Transient failures are not durable negative results.

OAuth default expansion occurs at token issuance and refresh in the reference
provider. The SDK builds requested scopes and may explain/expand them for a
caller, but its cache is not the PDS's grant authority. Explicit collections
freeze the requested set; omitted collections remain declaration-dependent.
[Source: scope implementation][scopes].

### Reproducible generation

- Keep main-branch sources in `lexicons/` and the alpha namespaces in checked-in
  `lexicons-space/`, with an immutable `space-lexgen.lock` and refresh script.
  Record full source SHA and provenance, not just `9d787eb`.
- Read both roots into one `lexicon.Catalog`; fail on duplicate NSIDs and
  unresolved references. Pin any required non-space schema changes explicitly
  if the dependency closure grows. Never silently let directory order select a
  version.
- Generate the whole catalog in **one pass**. A second pass into the same
  package can replace package-wide `decode.go`/shared helpers with an incomplete
  view of the catalog.
- Update **both** `just lexgen` and `scripts/update-lexicons.sh`. The latter
  removes generated files from its staging tree and replaces whole output
  directories; leaving it unchanged would erase generated space endpoints on
  the next normal update. Stage the combined output and publish/rollback source
  pins and generated files together.
- Test regeneration determinism, missing roots, duplicate sources, unresolved
  refs, interrupted publication and a normal upstream refresh retaining spaces.
  When upstream absorbs spaces, deliberately migrate the pin and remove the
  overlay without collisions; this is not automatically a no-change update.
- Provide a locked cache-hydration command for local/CI tests: `lexicons/` is
  ignored, and its absence currently skips some integration coverage while a
  partial cache fails it. Require a complete pinned catalog for the new
  generation/validation gates. Hydration must not advance pins or regenerate
  tracked output as a side effect.

## Authentication and HTTP

### Operation-specific authentication

| Operation | Credential and destination |
|---|---|
| `getDelegationToken` | Account OAuth with covering `read`, to that user's PDS. `read_self` is insufficient. |
| Space record writes and `listSpaces` | Account OAuth to its own PDS, with applicable space scopes; writes cannot target another account. |
| Read/sync own repo | Account OAuth with `read_self` or `read`, or a matching space credential. |
| Read/sync other authors; `listRepos`; register/unregister | Matching space credential + DPoP, directly to the resolved repo host or authority as appropriate. |
| `getSpaceCredential` | POST to authority; **Bearer delegation** plus DPoP without `ath`; optional client attestation in the body. |
| `simplespace` mutations / `listMembers` | Authority's account OAuth. Mutations need `manage`; listing members needs `read_self`. |
| `simplespace.getSpace` | Matching space credential, or the authority's own account OAuth with `read_self`. |
| `notifyWrite`, `notifySpaceDeleted`, `checkUserAccess` | Service auth, with role-specific issuer, audience and `lxm` checks. |

The pinned PDS also accepts legacy account access tokens on several account
paths. That is alpha behavior, not a reason for the high-level SDK to offer an
implicit downgrade from OAuth. `listSpaces` enumerates locally known spaces;
it is not a global membership or invitation directory.

Scope helpers must implement `authority=self` by default, wildcard matching,
repeated collection/action/manage parameters, and explicit empty/default
semantics. Default actions are `read,create,update,delete`; `manage` defaults to
none. `read` includes `read_self`, reads ignore collections, and a wildcard type
without explicit collections has no default write targets. Omitting `action`
while specifying `manage` still requests the default record actions. Do not
silently drop malformed parameters or widen a rejected scope. Space permissions
inside permission sets also need explicit support/coverage if that surface is
exposed; a wildcard type is not allowed there.

### Identity and endpoint selection

Resolve the authority's `#atproto_space_host` service of type
`AtprotoSpaceHost`; use its `#atproto_pds` service only if the dedicated entry
is absent. A present but malformed dedicated entry fails. Resolve each author's
repo host through that author's `#atproto_pds`; do not send account OAuth to the
authority's unrelated host or invent an additional repo-host fragment. Resolve
callbacks/managing apps by their explicit DID plus fragment, defaulting a bare
DID to its PDS as the alpha does. Keep JWT audience identifiers separate from
HTTP destinations.

Use raw DID evidence to check the requested DID against document ID, selected
verification-method controller and full entry IDs, duplicate/colliding selected
fragments, service type and endpoint. Preserve unknown unrelated DID extensions.
Selected endpoints require absolute HTTPS URLs without userinfo/query/fragment
and the appropriate service type; test-only HTTP/private destinations require
explicit configuration. The issuance key fallback and the token's named-key
verification rules below are different operations.

### JWT profiles and credential lifecycle

| Profile (`typ`) | Signer/key | Required binding | Issuance default |
|---|---|---|---|
| `atproto-space-delegation+jwt` | User account key, `kid=#atproto` | User `iss`, exact space `sub`, `aud={authority}#atproto_space_host` | 60s, single-use |
| `atproto-client-attestation+jwt` | App's published client-authentication JWK, matching `kid` | `iss=sub=client_id`, same authority audience | 60s, single-use |
| `atproto-space-credential+jwt` | Authority account or dedicated key | `iss` equals subject space authority; exact `sub`; `cnf.jkt`; no `aud` | 7200s, reusable |

These are issuance defaults, not proof that the alpha verifies a matching
maximum age for every token class. Define explicit verifier age/lifetime limits,
require integer timestamps and `iat/exp/jti`, allow five seconds of skew, and
reject `exp <= iat`, future-issued and overlong tokens. Record stricter rejection
cases as deliberate hardening. Allow only algorithms supported by the specific
profile and key, reject duplicate/ambiguous critical claims and private keys in
embedded public JWKs, and distinguish parse-only from verified types.

For credential verification, allow the named `#atproto` or `#atproto_space` key;
never substitute another key when the named one is absent or invalid. This is
different from choosing a host's **issuance** key: prefer the dedicated entry,
falling back only when absent. Delegations require the account key. A bounded
forced DID refresh on signature failure can handle rotation; rate-limit and
coalesce it so invalid tokens cannot force unlimited network resolution.

Attestation verification must fetch and validate client metadata, including
`client_id`, inline `jwks` or `jwks_uri`, then select an allowed public signing
key and verify audience, subject, signature and time. A `client_id` supplied
without this verification never satisfies an app allowlist. All metadata/JWKS
fetches need the same SSRF, size, timeout and cache discipline as DID fetches.

Provide an atomic, error-returning replay store with expiry and namespaced keys
for delegation, attestation, DPoP and service auth. Share it across accepting
replicas and retain entries through the entire allowed acceptance/skew window.
Verify before consuming a JTI; a replay-store failure fails authorization. Cache
eviction before expiry is not valid replay protection. Policy denial may occur
after consumption; a retry of exchange obtains **new delegation and attestation
tokens and a fresh proof**, not just a fresh DPoP header.

Space credentials are reusable across resolved hosts for their space; they are
not audience-bound to one host. Each request is endpoint-bound by DPoP. Cache
the credential and private key together, partitioned by space, application and
caller security context. Refresh with single-flight, early-expiry margin and
jitter using a caller-provided eligible account session. Publish a new
credential/key pair atomically; retain the old pair while in-flight reads use
it. Generate a fresh P-256 DPoP key for each new credential, separate from the
account OAuth session key and the app's attestation key.

Revocation/deletion does not invalidate already issued credentials at every
repo host. Default residual read access can last roughly two hours plus skew;
DID key caches complicate key-based revocation. The credential contains no
member DID or per-viewer policy. An AppView must authorize each viewer of its
indexed copy independently.

### DPoP and transport construction

Reuse `oauth.CreateDPoPProof` with no nonce, and add RFC 7638 `jkt` over the
canonical public `crv,kty,x,y` members. Spaces use ES256/P-256 proofs, a 60-second
proof-age window plus skew, and no server nonce negotiation. Generate `htu`
from the actual normalized method/URL with query and fragment excluded; reject
proofs that contain those components. DPoP does not bind query parameters or
request bodies, so handlers must separately enforce the requested space, repo,
operation and authenticated caller. Configure the host's external HTTPS origin;
never derive it from arbitrary untrusted forwarding headers.

Use this request order for the new clients:

```text
bounded logical retry -> construct request -> check destination/operation
  -> create fresh proof -> hardened transport with retries disabled
```

A signing `RoundTripper` is valid when every retry re-enters it. A signer outside
a retrying gttp transport is not. Use `gttp.WithNoRetries()` below signing, but
**this does not disable Go's own transparent retries**. The reused-connection
experiment above demonstrates a proof being sent twice after the server has
consumed the first request. A generic xrpc hook or outer RoundTripper cannot
regenerate a proof for that hidden attempt.

Transport implementation is therefore gated on a prototype with one of two
explicit contracts: a hardened transport that exposes every actual send to the
signer, or suppression of hidden retries. A no-reuse HTTP/1 configuration is a
correctness baseline for the reproduced case; validate TLS and stream failures
before treating it as a general solution. It costs connection reuse and HTTP/2
multiplexing, so it is not an acceptable silent performance fallback. Q3 must
establish whether such an explicit initial mode fits the workload or pooled
transport support is required before release. Do not weaken server replay
checking or rely on undocumented header-mutation hooks to hide this problem.

The Phase 0 wire prototype confirmed that Go transparently reuses the same DPoP
proof for an HTTP/1 retry on a stale pooled connection and for HTTP/2
`REFUSED_STREAM` and `GOAWAY` retries. Disabling HTTP/1 reuse suppresses the
reproduced HTTP/1 replay, but is only a correctness baseline. Q3 now requires a
pooled, HTTP/2-capable proof-per-send transport before release; the no-reuse
mode is not a production fallback. Until that transport exists, authenticated
spaces clients remain release-blocked even though the prerequisite failure
modes have regression coverage.

Avoid a second independent logical retry loop. GET failures before publishing a body
can be retried; restart a failed stream from the last verified checkpoint.
Never retry a successful or ambiguous write response merely because its body
was truncated. A fresh proof does not make a POST idempotent. Treat exchange as
one attempt per single-use grant, including 429/timeout paths.

For interactive calls, start with `xrpc.ATProtoOpts`; for CAR/blob streams use
`xrpc.BulkDownloadOpts`, including its absolute `BulkMaxRequestTimeout` as well
as header/idle/throughput limits. Add strict SSRF protection, no proxy, and no
redirects for authenticated space calls. `xrpc.NewTransport` and
`NewHTTPClient` alone are not the complete hardened setup. Resolve destinations
before attaching credentials; do not reattach auth to redirects, even if an
HTTP client stripped the previous Authorization header. Redirects also invalidate
the method/path proof binding on the same origin.

Share clients/transports; do not allocate one per author. Retain bounded shared
connection pools when the transport prototype can meet the proof contract with
reuse. Bound total workers, per-host requests, queued work, retries, and
simultaneous backfills.
Make service/DID/metadata resolution cancellable and apply DNS-rebinding-safe
SSRF checks at dial time. Native Go networking and browser/WASM networking have
different guarantees: a WASM build passing does not prove socket-level SSRF or
stream timeout enforcement in a browser.

Set independent limits for JSON, metadata/JWKS/DID documents, CAR header,
commit, index, record blocks, total export bytes, blob bytes and staging disk.
Read `limit+1` or otherwise detect oversize; `io.LimitReader` alone can turn a
truncated body into apparent EOF. Cap decoded and decompressed sizes as well.
Use streaming APIs for CAR/blobs; generated `QueryRaw` wrappers buffer data.
Always close bodies on errors, cancellations and abandoned streams.

## Permissioned repo and CAR contract

The core should expose validated commit context `(space, author, rev)` and
separate raw/verified commit representations. Commit fields are `ver=1`,
32-byte `hash`, 32-byte `ikm`, 32-byte `mac`, a supported 64-byte EC signature,
and a valid TID `rev`. Space and author are supplied externally, not read from
commit fields.

```text
ctx = UTF8("atproto-space-v1")
   || uint16be(len(space))  || UTF8(space)
   || uint16be(len(author)) || UTF8(author)
   || uint16be(len(rev))    || UTF8(rev)
   || uint16be(len(ikm))    || ikm
sig = Sign(author_key, ctx)
mac = HMAC-SHA256(HKDF-Expand-SHA256(ikm, ctx, 32), hash)
```

Lengths count bytes; reject values above 65535 before encoding. HKDF uses
**Expand only**, not Extract+Expand. Use constant-time MAC comparison and the
existing P-256/K-256 signing conventions. Fresh signing randomness means the
same revision can yield different commit CIDs; compare `(rev, hash)` and
context, not signature/commit-CID equality. Low-level validation must not
mistake alpha schema permissiveness for valid digest/key/signature lengths.

CAR layout is exactly two roots, commit then index, followed by one block for
each index entry in canonical DAG-CBOR key order (length, then bytewise). Two
paths with the same CID still require two positional record blocks; do not
silently deduplicate them. Reject duplicate index paths, invalid NSIDs/rkeys,
noncanonical encodings, unsupported CIDs, reordered/missing/extra blocks and
trailing data. Verify root CIDs as well as record CIDs. A swapped pair of
identical blocks is indistinguishable and harmless.

A full verifier succeeds only after EOF and the expected block count. A
streaming iterator yields provisional data into staging; closing it early is
not verification success. Index-only mode must be explicitly requested and
requires precisely commit + index, with no record blocks. It verifies an index,
not body availability. Do not silently accept a truncated full export as an
index-only export. [Source: CAR provider/consumer][car-source].

The single index block is O(number of records) in bytes. The reference producer
buffers values and sorts paths; do not call it constant-memory streaming.
For atmos serialization, use a stable snapshot and replayable/spooled canonical
record iteration, with bounded memory/disk. A full network fetch or sort must
not hold a production database write transaction open. Per-reader index limits
must coexist with small record/header limits and preserve public CAR defaults.
Q3 sets the initial operating envelope.

## Sync state machine and durable store

### Verified state versus work in progress

Key all state by `(space, author)`; store the path-to-CID index, retained canonical
record bytes, full 2048-byte LtHash state, verified revision/hash, endpoint/key
resolution provenance, and lifecycle generation. The 32-byte digest cannot be
used to resume LtHash updates. Track index verification and completeness of
record values separately; a projection of known record types is not a complete
repo. Unknown valid records remain in the commitment even if the app does not
index them.

Use a caller-provided staged-generation interface: begin from an expected
checkpoint, stage bounded updates, verify, then atomically promote by
compare-and-swap. Never keep a database transaction open over network I/O.
Promotion checks both the repo checkpoint and the space/account lifecycle
generation, so concurrent jobs and a deletion racing a fetch cannot resurrect
old data. Failed or interrupted staging is invisible and reclaimable.

Per-repo workers serialize logical advancement; the store still needs CAS or
fencing across multiple syncer instances. On restart discard unfinished staging
and resume from the last verified checkpoint. Persisting mid-pass progress is
an optimization requiring the original `since`, opaque cursor, working index,
state and fencing token together; it is not the initial implementation contract.

Commit publication and downstream indexing must not have a crash gap. Persist
a versioned change/outbox event with the promoted checkpoint, deliver it at
least once and require idempotent consumers. The SDK cannot atomically commit
an arbitrary application's independent database through a callback. A store
conformance suite must test failure before/after promotion, concurrent promotion,
cancellation, deletion fencing and outbox redelivery.

### Bootstrap and scheduling

Obtain a credential, optionally register a caller's resolvable callback service,
then enumerate the authority directory and pull listed authors directly. Polling
must work for clients without a public callback. Use the returned registration
expiry (one day in this snapshot), renew independently of credential refresh,
and persist lease scheduling where needed.

Schedule periodic directory sweeps **and direct checks of already known
repos**, even when the directory revision has not advanced. Merge hints without
regressing verified checkpoints. Do not delete repo data merely because one
mutable/paginated directory scan omitted a DID. Polls and notifications are
hints; missing downstream notifications can be repaired, but first-hop loss for
an unknown author requires another discovery source. Coalesce notifications into
bounded dirty-repo work; overload must be visible and trigger reconciliation.

### Incremental pass

1. Capture the verified base revision as `since`; keep it unchanged for the
   entire pagination pass. Echo opaque cursors exactly. The reference cursor
   contains `(rev, idx)`, but the wire op has no `idx`; never deduplicate solely
   by revision, path or CID. Multiple operations on one path within a revision
   are legal. Both `since` and cursor filter in the pinned reader, despite the
   Lexicon's claim that cursor takes precedence.
2. Validate ordered operations and apply them exactly once to staging. Check
   `prev` against the staged path index before removing it; creation requires
   absence and updates/deletes require the matching previous CID. A mismatch
   means incremental history cannot be safely applied and calls for recovery.
3. Validate inlined bodies against their CID using canonical DAG-CBOR encoding
   of the AT data model, not a hash of JSON bytes. Preserve unknown fields and
   bytes/CID-link semantics. Intermediate bodies may be omitted after overwrite
   or delete; omission is not a delete operation.
4. Follow cursors to a terminal signed commit. A full page may require one more
   request even when it ends at head. Cursor cycles, no progress, excessive
   pages/bytes/time, or absent terminal commit are bounded incomplete passes,
   never success. An unwritten repo has no commit; it is distinct from a
   previously written, now-empty repo with a valid empty-state commitment.
5. Verify the terminal commit for the expected space/author and its revision,
   ensure it is not behind staged ops or the durable checkpoint, and compare
   the staged LtHash. Equal revision with different hash is an integrity/
   equivocation conflict, not an ordinary update. Do not silently roll back to
   an older revision after migration or restore.
6. Resolve any still-missing **final** record bodies. `getRecord` has no
   historical-CID fetch parameter: accept only the expected CID and exact URI;
   concurrent changes require another bounded pass/recovery. Publish a complete
   checkpoint only when final values are complete, or explicitly expose an
   index-only checkpoint with no full-repo claim.

### Recovery and errors

Fetch a full CAR from the currently resolved author host, verify it into a new
staging generation, and atomically replace the prior state only after complete
success. An index-only CAR can reduce transfer by reusing locally verified
values and fetching changed records, subject to the same final-value race.
It cannot prove that a mutable series of `listRecords` pages is one snapshot.

Digest mismatch and pruning permit recovery; the alpha has no guaranteed
"history too old" error. Auth denial, SSRF rejection, oversized payloads and
malformed crypto are not reasons for an endless full-CAR retry loop. Classify
errors, bound attempts/bytes, back off with jitter, and report incomplete or
blocked progress. Concurrent reference exports can be inconsistent because
`getRepo` does not establish an explicit snapshot transaction; never publish a
mismatching export. Convergence assumes reachable hosts, usable credentials,
retained/discoverable repos and an eventually obtainable consistent snapshot.

## Authority host, management, writes and blobs

The authority host requires interfaces for signer, account authenticator,
strict identity resolver, policy evaluator, replay store, durable authority
store, subscriber-admission policy, delivery queue, clock and structured events.
Starting without a required dependency is an error. Mountable HTTP handlers
must enforce request limits and return XRPC errors; document server header/body
deadlines, trusted proxy configuration and shutdown ownership for the embedding
server.

Management handlers consume a verified account principal from the supplied
authenticator, never an actor DID supplied by the request or generic service
auth claims. The authenticator validates the account OAuth token through its
configured authorization-server/resource-server contract (including issuer,
audience, expiry and DPoP where required) and returns the account DID and granted
scope permissions. This is not necessarily local JWT validation: account tokens
may require introspection. Each handler checks the exact operation's space
scope and that the principal DID is the target space's authority; `createSpace`
derives the authority from the principal. A standalone authority endpoint
cannot simply accept an OAuth token intended for a different PDS. The embedding
service must supply a valid account-auth integration or leave those management
handlers unmounted. Building an OAuth authorization server is not implied.

If management is mounted alongside repo hosting, deleting the authority's own
repo needs an explicit coordinated storage/deletion adapter as well. An
authority-only host cannot erase a repo on another PDS with the current space
API. The Q1 authority-only decision retains that role/deployment limitation;
do not report remote cleanup as completed by local configuration deletion.

Persist policy/member configuration, writer rows, registration leases,
tombstones and notification work. Use transactions/CAS and policy generations:
a slow managing-app answer obtained before a policy change cannot authorize
issuance or a directory update after that change without revalidation. Do not
hold a store lock/transaction while calling the managing app.

| Service call | Required identity checks |
|---|---|
| Author -> authority `notifyWrite` | `iss == repo`, `aud == space authority DID`, exact `lxm`; authority hosted here and writer admitted under write policy. |
| Authority -> subscriber `notifyWrite` | `iss == space authority DID`, `aud == configured subscriber service identifier`, exact `lxm`; do not require `iss == repo` on this hop. |
| Authority -> subscriber `notifySpaceDeleted` | Same authority/subscriber binding, deletion method; invalidate only the named space. |
| Authority -> managing app `checkUserAccess` | Authority issuer, managing-app DID plus optional fragment as audience, exact method; validate `access=read|write` and requesting user. |

Use `serviceauth` with mandatory replay checks and role validation above; it
does not authenticate arbitrary payload authors simply by verifying a signature.
Account/service signing keys and the space-credential issuance key are distinct
roles. A dedicated-key host needs an explicit service-auth signing identity
compatible with the peer, not an assumption that its credential key will work
with an unqualified authority issuer.

Writer updates must be monotonic per `(space, repo)` for valid TID revisions:
older hints cannot overwrite newer ones; equal revision/hash is idempotent;
equal revision/different hash is a conflict. Persist an accepted directory
update and fanout outbox together. Fanout uses bounded concurrency, fresh service
JWTs per retry, backoff, expiry, and cancellation. Preserve deletion deliveries
when removing registrations. Repo hosting is excluded by Q1; any later
repo-host milestone must make write commit + first-hop notification outbox
atomic. The authority host alone cannot repair an originating PDS that never
notified it.

Registration is whole-space at the authority in this alpha; there is no
implemented direct per-repo registration contract. The credential authorizes
register/unregister of the supplied service identifier without proof of owning
that callback DID. This permits unsolicited delivery to another service and
withdrawal of another subscriber's registration. DPoP does not prove callback
ownership; SSRF checks and quotas do not repair that authorization gap. Require
an explicit subscriber-admission/withdrawal policy from the embedding host;
there is no implicit arbitrary-target delivery default. Under Q6 the embedding
host must explicitly choose its registration and withdrawal policy. Document
any stricter policy as a deployment restriction, and do not add an incompatible
wire challenge. An allowlist establishes operator consent to delivery, not
proof that a particular credential holder owns the callback.

Under either policy, apply per-space/per-credential/destination quotas, bounded
leases, strict destination resolution and request limits; authenticate received
notifications independently and retain polling for lost/withdrawn subscriptions.

Simple-space management must use required `readPolicy`, `writePolicy`,
`appAccess`, and nested `managingApp` fields. Unknown policy/app-access variants
are retained by generic decoders but rejected by a host that cannot implement
them. `putMember` replaces **both** booleans. Authority user admission bypasses
member/managing-app checks, but the app allowlist still applies. Write admission
controls directory tracking/forwarding, not local record writes. Removing a
member does not automatically remove old writer rows or application content.
Managing-app failures deny the operation with a distinguishable error; they do
not mean public access.

Typed write helpers enforce own-account targeting, `$type`/collection agreement,
200 operations per batch, ordered results and explicit validation mode. Do not
split an oversized batch automatically: that loses atomicity. Respect the
pinned create/update/delete/no-op semantics and test dependent same-path writes.
The alpha write schemas lack public-repo `swapCommit`/`swapRecord` conditions;
do not promise compare-and-swap writes or make blind retries appear safe.

There is no space upload endpoint. Use account-authenticated
`com.atproto.repo.uploadBlob`, including the required blob OAuth permission,
then reference the blob in a space record. Upload and record commit are separate
operations. Downloads use `space.getBlob`; check the blob CID and size while
streaming, and never fall back to unauthenticated public `sync.getBlob`.
Maintain access references by space/author even if bytes are internally deduped
by CID. Knowing a CID must not authorize a cache read. Separate blob availability
from repo verification; the repo digest covers blob references, not retrieval
of blob bytes.

## Deletion, revocation, and account lifecycle

Implement `notifySpaceDeleted` and explicit `SpaceDeleted` on credential renewal
as authoritative deletion signals only when received from the expected
resolved authority/service-auth issuer. A random repo host's error string is
not authority to wipe an entire space. Persist a lifecycle tombstone, fence and
cancel in-flight work, stop serving the space, then remove repo copies, derived
indexes, blob access references and credential/key material. Cleanup failures
must remain retryable and visible after restart.

A generic 401/403/404, `SpaceNotFound`, network outage or policy denial is not a
deletion signal. Stop unauthorized new reads and surface status; retained data
and serving policy need the explicit contract in Q4. The authority deletes its
own repo on space deletion; it cannot delete other authors' source repos.
Unexpired credentials can still read retained remote repos in the alpha.

Account deactivation/takedown suspends serving; account deletion removes the
account's replicated data; identity changes invalidate endpoints/keys and cause
re-resolution. Expose hooks for trusted account/identity events, accepting them
from the embedding application's existing public sync integration without
mixing public record semantics into the space syncer. Pending deactivation or
deletion must also fence in-flight commits. Polls and credential renewal alone
do not deliver every account lifecycle event.

Same-URI recreation has no protocol incarnation ID: old credentials, old member
repos and delayed notifications can refer to the new space. A local generation
prevents stale **local** work, but cannot distinguish those old remote artifacts.
Under Q5 the atmos host permanently reserves tombstoned URIs; never claim local
fencing solves the wire ambiguity for external authorities that permit reuse.

## Known alpha gaps and release blockers

| Gap confirmed by source inspection | Consequence / required follow-up |
|---|---|
| PDS first-hop notification routing uses bare authority DID -> `#atproto_pds`, not the proposal's dedicated space-host endpoint. | Standalone host support needs a real two-host interoperability test and upstream routing repair. Mock-only success is insufficient. |
| First-hop notifications lack a durable outbox; directory upserts accept older revisions. | Retain the discovery limitation in SDK guarantees. Our host adds monotonic updates/outbox; it cannot fix remote discovery loss. |
| Public `sync.getBlob` uses the shared blob reader without a public-reference check, while space writes make blobs permanent. | Source indicates a space-only blob may be fetched publicly by DID/CID. Reproduce on an isolated pinned PDS and establish the intended perimeter before describing blob support as private. Do not probe other users' blobs. |
| Reference full exports read state then page records without an explicit consistent snapshot. | Detect and reject mismatches; test convergence under writes and bound recovery amplification. |
| Same-URI recreation and no instantaneous credential revocation. | Q4/Q5 require permanent local tombstones and explicit residual-access documentation; no claims of immediate erase or revocation. |
| Registration/withdrawal authenticates a space reader, not ownership of the named subscriber. | Q6 requires explicit host policy; account for unsolicited notifications and third-party subscription removal. |
| No space import API in the pinned Lexicons. | No complete migration promise. The Q1-excluded repo-host migration needs its own design if later added. |
| Low-level token/commit helpers accept inputs looser than protocol intent. | Keep positive interoperability vectors and explicit stricter local rejection tests; do not reproduce unsafe acceptance just to match the alpha. |

These are tracked dependencies, not issues filed or fixes landed upstream.
Recheck each when updating the pin and before release. Changes in sibling public
packages remain in-scope only as concrete prerequisites below; this document
review does not start those implementation changes.

The local transport blocker is separate from alpha bugs: prototype proof
generation versus Go's transparent HTTP retries before selecting a production
transport. `WithNoRetries()` fixes the gttp layer only. Keep the reproduced
reused-connection failure as a mandatory regression case.

## Phased implementation tracker

The order follows dependencies. Every phase includes failing tests first and
ends with its acceptance gate; checked boxes mean implemented and verified.
Phases 0–4 are complete; Phase 5 remains unchecked.

### Phase 0: contracts, pins and prerequisite fixes

- [x] Resolve Q1–Q6 where they affect the selected milestone; record Q2/Q3 and
      explicitly defer Q1/Q4–Q6 to their dependent phases.
- [x] Implement combined lexicon generation, locked overlay, declaration schema
      support and update-script atomicity; test normal refresh retains spaces.
- [x] Implement strict space identities and the general `at-uri` format decision.
- [x] Add declaration resolver/cache with explicit trust, errors and bounds.
- [x] Preserve raw DID evidence for strict selected-entry validation: expected
      document DID, full IDs/controller, duplicate fragments, service type/URL,
      malformed-present versus absent, `did:web` and `did:plc`.
- [x] Fix/cover xrpc non-idempotent response-read retries, strict oversize
      detection, and partial `RetryPolicy` options that currently panic when
      delay fields are absent. Test complete request/response behavior.
- [x] Prototype per-send DPoP signing or suppression of transparent transport
      retries. Test a consumed request followed by connection close, with HTTP/1
      reuse and HTTP/2 failures; evaluate any loss of pooling against Q3.

Done when: generation is reproducible, strict identity tests pass, transport
prerequisites are covered, and public-repo contracts remain intact apart from
explicitly reviewed shared fixes/general-format support.

### Phase 1: repo crypto and CAR

- [x] Add BLAKE3 dependency with pure-Go/WASM checks, LtHash and validated commit
      types, explicit byte order, Expand-only MAC and P-256/K-256 coverage.
- [x] Add cross-language positive vectors, fixed signed verification fixtures,
      deniability and malformed-input tests.
- [x] Add per-reader CAR limits and full/index-only serialization/verification,
      including duplicate CIDs at distinct paths and an index above 1 MiB.
- [x] Verify EOF/completeness, staged streaming and snapshot/spooling contracts;
      benchmark index memory, hashing and export allocation behavior.

Done when: deterministic bytes agree with the pinned implementation, both key
families verify, full exports require complete bodies, and limits are enforced
without mutating a global setting or silently truncating.

Phase 1 implementation notes:

- `space` separates raw, structurally validated and context-verified commits.
  The full 2048-byte LtHash state is resumable; its 32-byte digest is not.
  Operation validation is atomic before hash mutation.
- The checked-in Node generator pins `@noble/hashes` and `@noble/curves` 1.7.0
  and reproduces immutable P-256/K-256 signed commits and complete CARs without
  making Node or network access a test dependency.
- CAR readers capture immutable header/block limits and permit a tighter limit
  for each successive block. Space verification additionally bounds total
  bytes, records, commit, index and record bodies. Public CAR defaults remain
  unchanged; no global limit is modified by space code.
- Full verification consumes the clean EOF before returning success. Record
  callbacks are explicitly provisional staging writes. Index-only mode rejects
  bodies, and full mode never treats absent bodies as an index-only success.
- Serialization requires an immutable replayable snapshot. `FileSpool` converts
  a one-pass source into that contract under explicit record, per-body and disk
  limits, closes every source/body and removes failed or closed spool files.
- Benchmarks cover LtHash element sizes, 100/10,000-entry indexes, and 1,000-body
  full/index-only exports. On the 2026-09-11 Linux/amd64 verification host,
  LtHash updates used one 192-byte allocation after bounded scratch reuse;
  these measurements are evidence, not release-envelope defaults for Q3.

### Phase 2: auth, clients and identity integration

- [x] Add OAuth space scope helpers and explicit default expansion inputs.
- [x] Implement three JWT profiles, app metadata/JWKS validation, replay store,
      strict key selection, DPoP verifier and RFC 7638 thumbprint.
- [x] Implement endpoint-bound typed account/reader clients and single-use
      credential exchange/refresh; use one retry owner and no redirects.
- [x] Implement authority/repo-host resolution and both notification-hop auth
      profiles. Dedicated-host routing is an explicit upstream interop gate.
- [x] Expose all pinned read APIs, streaming CAR/blob variants, and own-account
      writes with batch/result/validation semantics.

Done when: real crypto works through an HTTP mock authority plus two repo hosts,
wrong-space/host/role requests fail, and retry wire tests never reuse a proof or
replay an ambiguous write/exchange.

Phase 2 implementation notes:

- OAuth space permissions parse and canonicalize strictly, preserve repeated
  collection/action/manage parameters, fail closed on malformed scopes, and
  require explicit issuer inputs to resolve `authority=self` and declaration
  collection defaults.
- `space/credential` separates parsed and verified values for all three JWT
  profiles, reparses immutable wire bytes before verification, enforces exact
  key/algorithm/time/binding rules, and supplies bounded refresh, metadata/JWKS
  resolution, RFC 7638 thumbprints, DPoP and atomic replay interfaces. Metadata,
  JWKS, token and replay inputs have independent limits and fail closed.
- Account and exact-space reader clients validate every typed request and
  response boundary, route authority, repo and callback operations through
  strictly selected raw DID-document entries, stream CAR/blob bodies under
  absolute/idle/rate/size limits, and keep own-account writes atomic rather than
  splitting or retrying them. Account OAuth nonce and refresh policy remains a
  caller-provided account-session responsibility, separate from space DPoP.
- Native authenticated requests use an explicit hardened HTTP/1 transport with
  redirects, proxies, connection reuse and HTTP/2 disabled. It replaces custom
  dial hooks, checks resolved IPs at dial time, and enforces TLS verification
  and protocol bounds. This is the reviewed correctness mode, not a pooling or
  performance claim; a proof-per-wire-send pooled HTTP/2 transport remains the
  Q3 release blocker.
- Tests exercise real delegation and credential signatures through one mock
  authority and two independently resolved repo hosts, fresh per-request proofs,
  wrong-host/space/role rejection, ambiguous exchange/write behavior, dedicated
  authority-host fallback, notification roles, malformed boundaries, races,
  fuzzing and native/WASM builds. The pinned upstream's first-hop routing still
  requires the dedicated-host interoperability repair and release gate described
  above.

### Phase 3: durable sync and lifecycle

- [x] Implement staged store/CAS/outbox interfaces and in-memory implementations,
      with a reusable failure-injection conformance suite.
- [x] Add bootstrap, bounded scheduling, sweeps/direct polling, optional callback
      registration/renewal and opaque-cursor incremental sync.
- [x] Verify `prev`, bodies and terminal commit; complete missing final values;
      add bounded full/index-only recovery and typed incomplete states.
- [x] Add deletion/account status hooks, tombstone fencing, restart cleanup and
      credential-denial behavior selected in Q4/Q5.
- [x] Run the model-based harness, crash/retry tests and race detector.

Done when: every published repo generation is complete and verified in its
recorded direct-host context; restart/replay never double-apply; deletion cannot
be undone by old work. Conditional convergence and unknown-writer discovery
limits remain explicit.

Phase 3 implementation notes:

- `space/sync` requires a caller-provided durable `Store`, a strictly routed
  `Source`, explicit resource/time limits and a credential purger. Stages are
  invisible, promotion compares repo, space-lifecycle and account-lifecycle
  fences, and the checkpoint plus versioned outbox event are one atomic store
  operation. The bounded memory store is explicit test/example infrastructure;
  `space/sync/storetest` is the reusable backend conformance harness.
- Each logical author pass opens one immutable direct-host binding: the PDS
  endpoint and `#atproto` key are selected from the same raw DID document and
  retained as checkpoint provenance. Identity changes cancel/fence old work;
  the next pass resolves a new binding. Concurrent work for one repo is
  coalesced locally, while store CAS remains mandatory across replicas.
- Incremental passes keep `since` fixed, echo cursors byte-for-byte, permit
  multiple ordered operations per revision/path, enforce exact `prev`, verify
  canonical bodies and the terminal commit/LtHash, and fetch only missing final
  values. Cursor/page/op/byte/time/cardinality limits produce typed incomplete
  results. History gaps and digest mismatch use bounded full or index-only CAR
  recovery; rollback/equal-revision equivocation and cryptographic failures do
  not become blind recovery loops.
- Directory discovery and already-known-repo polling are independent. Known
  repos are queued even when the authority directory is unavailable; bounded
  notification coalescing reports saturation and the next sweep reconciles it.
  Optional callback leases are persisted and renewed independently of reader
  credential refresh. Directory omission never deletes a repo.
- The Q4 contract below distinguishes active, stale, suspended and deleted
  state. Durable tombstones fence and cancel in-flight work before cleanup;
  failed cleanup remains pending across restart. Account events fence only the
  named author. Outbox consumers are at-least-once and must deduplicate by ID.
- Tests cover store failure boundaries, concurrent CAS/lifecycle fencing,
  cancellation, restart cleanup, outbox redelivery, opaque/cyclic cursors,
  missing/final-racing values, same-revision same-path batches, full/index-only
  recovery, directory loss, queue saturation, lease renewal, credential/account
  denial, randomized model convergence, fuzzed failure atomicity and race runs.

### Phase 4: authority host and management

- [x] Implement policy/member/lease/tombstone storage and mountable XRPC handlers,
      including the account authenticator and principal/scope/ownership checks
      for management; do not infer them from service authentication.
- [x] Implement credential issuance with metadata/JWKS and replay verification,
      separate read/write/app policies and managing-app callbacks.
- [x] Implement monotonic writer admission and durable bounded notification
      fanout, subscriber admission/withdrawal policy, registration quotas,
      deletion delivery and shutdown.
- [x] Implement simple-space client helpers and test required policies, replace
      semantics, unknown policy denial and account-versus-reader access.
- [x] Integrate writes/blobs from two authors, authority notifications, sync
      rebuild and deletion; keep the repo-host server excluded by Q1 and reserve
      its persistence/export/migration work for a separate milestone.

Done when: two authors on separate hosts round-trip through the host library and
syncer under failures, all role boundaries are tested, and every served copy has
an application authorization/lifecycle contract.

Phase 4 implementation notes:

- `space/host` is a mountable authority library with no implicit dependencies:
  the embedding service supplies durable authority/replay stores, exact account
  authentication, strict DID resolution, separate credential/service signing
  roles, metadata/JWKS attestation verification, policy decisions, subscriber
  admission/withdrawal policy, notification transport, clock and event sink.
  Construction rejects missing or typed-nil dependencies and unbounded limits.
- Management derives or checks the authority account and maps each operation to
  its exact expanded OAuth space permission: `read_self`, and management
  `create`, `update` or `delete`. Account OAuth, reader credentials, writer
  service auth, authority service auth, delegation tokens, client attestations
  and DPoP proofs remain separate roles and are tested against substitution and
  replay.
- Authority storage uses policy generations to fence external authorization,
  permanent tombstones to prevent unsafe same-URI recreation, monotonic TID
  writer admission, and atomic directory-plus-fanout mutations. Equal
  revision/hash updates are idempotent; older revisions and equal-revision
  conflicts fail explicitly. The bounded memory implementation is for tests or
  deliberate single-process use; `space/host/storetest` is the reusable durable
  backend conformance harness.
- Notification registrations are expiring whole-space leases with atomic
  per-space, per-credential and per-service quotas. Both admission and
  withdrawal require an explicit embedding policy. Delivery uses durable
  leases, bounded workers, strict service resolution, fresh authority JWTs on
  every attempt, jittered bounded retries, expiry and cancellation-safe
  shutdown. The supplied HTTP transport is an explicit no-reuse HTTP/1
  correctness baseline unless the caller provides a retry-safe transport.
- `space/simplespace` supplies closed policy/config/member types and a typed
  account management client. Unknown or ambiguous generated unions fail closed;
  `putMember` always replaces both booleans. Account and reader `getSpace`
  clients validate the complete returned policy rather than exposing an
  unsupported variant as usable configuration.
- Mounted integration tests exercise typed create/get/update/delete and member
  lifecycle calls with one exact account grant at a time; real
  delegation/DPoP credential exchange and reuse; two independently keyed author
  checkpoints with blob-bearing records; authenticated authority admission;
  registration, withdrawal, durable fanout and full-CAR sync rebuild from two
  distinct resolved host provenances; and verified deletion propagation into
  sync cleanup and credential purge. HTTP callback tests independently verify
  service-JWT bindings, request bounds, redirect denial and fail-closed errors.
  Blob upload is separately round-tripped through the account PDS operation and
  referenced from a space record; the authority host does not become a repo or
  blob host.

### Phase 5: pinned stack interoperability and release readiness

- [ ] Run the reference PDS and Bulletin locally with synthetic accounts; test
      exchange, reads/writes, pagination, notification auth, management and
      deletion in both directions wherever both implementations serve a role.
- [ ] Reproduce/resolve the blob perimeter and dedicated-host routing blockers.
- [ ] Establish Q3 workload limits with load/latency/allocation measurements and
      failure tests; document operating limits and backpressure behavior.
- [ ] Review updated upstream pins and all deliberate divergences; provide
      examples, exported API/package docs, operational events and migration
      limitations. Confirm the external stability/production release gate.

Done when: the supported role combinations work with the pinned reference stack,
known release blockers are resolved, and the original no-merge gate is met.

## Verification strategy

The pinned implementation supplies **positive interoperability** evidence;
protocol invariants and this document supply negative correctness requirements.
A reference acceptance bug does not redefine validity. Preserve original source
attribution/licenses for ported fixtures/tests. Some upstream unit fixtures use
permissive synthetic identifiers; use valid syntax for full-protocol vectors
and keep primitive-only fixtures clearly scoped.

1. **Syntax and schema vectors.** Valid complete refs/records, invalid partial
   forms, both DID positions, generic `at-uri` references, maximum sizes,
   declaration fields, key recommendations, scope defaults/repetition and
   malformed policy unions. Include generated encode/decode and lexval paths.
2. **Crypto/property vectors.** Empty state, order independence, add/remove,
   duplicate-add non-idempotence, uint16 wraparound, explicit endian behavior,
   exact ctx bytes, Expand-only MAC and changed-hash deniability. Use real
   BLAKE3/HMAC/EC keys. Mutation tests cover every authenticated field and key.
3. **Cross-implementation artifacts.** A dev-only Node generator pinned to the
   implementation **and its dependency lock** emits checked-in JSON/binary
   vectors. CI requires no Node/network regeneration. Preserve full fixed signed
   commits/CARs for verification; do not omit `ikm` or `sig` from an allegedly
   valid CAR fixture. Regeneration need not reproduce randomized signatures;
   deterministic fixture inputs/outputs and immutable signed fixtures are
   separate classes. Record provenance with vectors, not by incidentally
   rewriting the lexicon lock.
4. **Hostile decoders.** Fuzz CBOR/CAR/index/oplog/JWT/DPoP/DID/declaration inputs.
   Cover invalid lengths, duplicate fields/paths, extra/truncated blocks,
   unsupported algorithms, wrong key/role/space, time/skew and replay. Bound
   allocation before trusting counts; use explicit resource-limit tests as
   well as fuzzing. Fuzzing alone cannot prove bounded resource use.
5. **Real transport failures.** Scripted transports cover malformed responses
   and errors; local socket/TLS servers cover gttp retries, redirects, DNS/SSRF,
   connection reuse, truncation, slow bodies and deadlines. A fake RoundTripper
   alone does not exercise gttp/socket enforcement. Test 429, ambiguous POST
   outcomes, proof refresh, metadata/JWKS rotation, cancellation and body leaks.
6. **Sync model.** Model authority discovery separately from each author's true
   repo state. Schedule multiple ops per revision/path, missing intermediate and
   final values, empty/absent repos, pruning, replayed/cyclic pages, dropped
   terminal commits, concurrent bootstrap/export, stale directory hints, first-
   and second-hop loss, older/equivocating commits, restart and lifecycle races.
   Assert no unverified generation is visible, no checkpoint outruns values,
   no double-apply, and convergence only under stated reachability/discovery/
   snapshot assumptions. Retain seeds/regressions per fault class.
7. **Host/concurrency.** Race credential rotation with reads; policy changes
   with callbacks/issuance; registration expiry with fanout; two replicas with
   replay/promotion; deletion with every in-flight stage. Test saturated queues,
   store failures, outbox retries, goroutine shutdown and cleanup resumption.
8. **Resources and benchmarks.** Measure LtHash work versus element length,
   index size/memory, canonical record encoding, CAR serialization, paging,
   backfill disk/bytes and host fanout. Test bounded pools, queues, cardinality,
   decompression and leaked bodies. LtHash per-element work is independent of
   repo size, not independent of element length. Use leak checks that account
   for reusable HTTP pools; raw goroutine-count deltas alone are unreliable.
9. **Wire integration.** Run local pinned PDS/Bulletin tests before each network
   milestone, recording implementation SHA, dependency/container digest,
   commands and result. No sensitive data or public service writes are needed.
   These gated tests supplement, not replace, local deterministic CI.

Use typed errors and structured events for credential expiry/denial, resolver
failure, replay/store failure, stale/conflicting hints, backfill cause/bytes,
verification failure, checkpoint promotion, notification acceptance/delivery,
queue saturation, lease renewal, lifecycle suspension and cleanup. Do not log
JWTs, DPoP proofs, private keys or record bodies. Space URIs, DIDs and CIDs can
also be sensitive/high-cardinality: use bounded metric labels and caller-
controlled redaction/correlation.

For implementation changes, run `just` (lint + short tests) and full
`just test-race` before submission, plus `just wasm` for portability. Add
`just test-wasm` coverage where runtime behavior matters. Short fuzz budgets on
PRs and longer scheduled/manual `just fuzz` runs are planned CI work, not a
claim about the current workflow. Regeneration remains a separate deliberate
step. No test-only flags may disable verification.

## Settled and deferred questions

Answers belong here before dependent implementation is called complete.
Deferral means the question does not affect Phase 0; it is not permission for a
later phase to choose silently.

### Q1: Are we building the repo-host server too?

The original host decision clearly includes an **authority** library. Does this
project also need a complete permissioned **repo-host** server (account auth,
record/blob persistence, write transactions, oplog, snapshot exports and
migration), or clients plus reusable repo primitives? Recommend authority host
+ clients/primitives first, with explicit repo-host work as its own milestone if
wanted. That keeps the advertised server scope concrete without pretending the
host library is a PDS.

Answer: **settled for Phase 4**. This effort implements the authority host plus
clients and reusable repo primitives, not a complete repo-host/PDS server.
Account-authenticated writes and `com.atproto.repo.uploadBlob` continue to target
the account's PDS. Adding a repo-host server requires a separately designed
persistence, transaction, oplog, export and migration milestone.

### Q2: How far should generic Lexicon `at-uri` compatibility expand?

Keep `ATURI` public-only and concrete space identities strict, as already
settled. The space wire APIs still need general `at-uri` acceptance for complete
space/record URIs. The pinned generic TS validator additionally accepts
JSON-pointer fragments; atmos currently rejects all fragments. Recommend adding
a separate general validator for complete space/public forms now and deciding
explicitly whether fragment support ships with it. This changes generic Lexicon
validation, not the public repo URI type. Should we include that fragment
compatibility in this branch?

Answer: **settled for Phase 0**. General Lexicon `at-uri` validation accepts
valid RFC 6901 JSON-pointer fragments, including correctly percent-encoded
forms. `ATURI`, `SpaceRef` and `SpaceURI` remain fragment-free identity types.
Invalid JSON-pointer escapes are rejected rather than normalized or ignored.

### Q3: What operating envelope must the first release support?

Specify expected records/bytes per author-space repo, authors per space, watched
spaces per process, desired sync lag and whether multiple active syncer/host
replicas are required immediately. Recommend configurable hard limits, staged
disk/store-backed backfills and CAS/replay contracts that permit replicas from
the outset; select and benchmark concrete defaults against your target workload.
The monolithic index makes these requirements affect the architecture, not just
configuration tuning. Also specify whether shared connections and HTTP/2 are
required for the initial release: strict proof-per-send behavior needs the
transport prototype above, and the demonstrated no-reuse HTTP/1 baseline adds
dials/TLS handshakes. Recommend retaining pooling as a release requirement for
high-scale deployments rather than silently sacrificing it for the alpha.

Answer: **partly settled where Phase 0 requires it**. Pooled shared connections
and HTTP/2 are release requirements. The no-reuse HTTP/1 mode is a correctness
baseline only and must never activate as a silent production fallback. Concrete
repo/cardinality/latency limits and replica targets remain deferred to the
Phase 5 measurements that set release defaults; those measurements cannot
waive the pooled proof-per-send requirement.

### Q4: What retention/serving contract should the syncer expose after access loss?

Confirmed space/account deletion has the purge behavior above. Expired
credentials, policy denial, deactivation and temporary authority outages are
different states. Recommend stopping network access when unauthorized, exposing
explicit suspended/stale status, retaining the last verified copy during
transient outages, and requiring the application to opt into any continued
serving under its own viewer authorization policy. What offline-serving and
retention defaults do you want the library to promise, including when all
eligible renewal sessions are lost?

Answer: **settled for Phase 3**. A directly verified current checkpoint is
`active`. A failed bounded sync pass retains that checkpoint but marks the
author repo `stale`; reconciliation continues, while serving requires an
explicit application callback that applies current viewer authorization.
Credential/policy denial and trusted account deactivation/takedown mark the
affected space or author `suspended`, stop new network work and impose the same
explicit offline-serving requirement. Losing every eligible renewal session is
denial, not deletion and not permission to serve implicitly. A later successful
direct-host verification returns stale state to active; the embedding app must
explicitly resume suspended access after restoring authorization. Confirmed
authority deletion and trusted account deletion are tombstoned and purged as
specified above. Temporary directory failure does not stop direct checks of
known repos, and no state claims discovery of an unknown writer whose first-hop
notification was lost.

### Q5: Should our authority host permit recreation at the same space URI?

The alpha allows it, but has no incarnation binding for old credentials,
notifications or retained member repos. Recommend the atmos host refuse reuse
of a tombstoned URI and require a new skey until upstream defines safe semantics.
Clients must still recognize that external authorities can recreate. This is a
host-policy departure from the alpha, especially relevant to `literal:self`
space conventions. Is that restriction acceptable?

Answer: **settled for Phase 4**. The atmos authority host permanently reserves a
tombstoned space URI and requires a new skey for recreation. This deliberate
departure prevents old credentials, member repos and delayed notifications from
being interpreted as a new local incarnation. Clients still treat external
authority behavior according to the wire protocol and lifecycle evidence they
receive.

### Q6: Should callback registration be open to any space credential holder?

The alpha permits a reader to register any resolvable subscriber DID/service
and to unregister another subscriber. Recommend requiring an explicit host
policy, with operator-approved services for initial deployments. That limits
unsolicited fanout but is stricter than the open alpha and does not prove the
registrant's ownership. Should the first host instead support open registration
with quotas and rely on periodic sync to tolerate third-party withdrawal, or
require restricted subscriber admission? A stronger ownership protocol would
need additional authentication and upstream design work.

Answer: **settled for Phase 4**. Registration and withdrawal both require an
explicit embedding `SubscriberPolicy`; the host has no permissive default. A
deployment may deliberately implement an open or allowlisted policy, but quotas,
strict service resolution and request bounds still apply. That policy expresses
operator consent, not cryptographic ownership of the callback identifier.

## Changelog

- 2026-09-11: initial plan and layered testing strategy. Recorded branch,
  package/type isolation, BLAKE3, authority host library, caller-owned sync
  storage, DPoP reuse and runtime declaration cache decisions.
- 2026-09-11: design review against the pinned proposal, implementation,
  Bulletin, study guide, diaries and auth RFCs. Corrected operation-specific
  auth, DPoP retry composition, identity validation, generation/update paths,
  CAR limits/completeness, sync/discovery guarantees and vector determinism.
  Added durable store/host contracts, deletion/account lifecycle, alpha blockers,
  bounded experiments and six open decisions. Adversarial review clarified the
  management authenticator and callback admission contracts. No implementation
  started.
- 2026-09-11: completed Phase 0. Added strict `SpaceRef`/`SpaceURI` and general
  Lexicon AT-URI validation; a trust-partitioned, bounded declaration resolver
  and cache; strict raw DID entry selection; combined multi-root generation;
  the 29-schema spaces overlay pinned to
  `9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33`; reproducible hydration and
  rollback-safe update scripts; and generated low-level spaces APIs. Hardened
  XRPC body limits, response error metadata, retry/ambiguity policy and partial
  retry options, and made OAuth logical retries body-replay-safe. Wire tests
  cover reused HTTP/1, no-reuse HTTP/1, HTTP/2 `REFUSED_STREAM`, and HTTP/2
  `GOAWAY`; they retain pooled proof-per-send transport as a release blocker.
  Full short/race/WASM suites, repeated race stress, deterministic generation,
  cache corruption recovery, signal-interruption rollback tests and 30 focused
  fuzz campaigns passed. Focused adversarial reviews were iterated to no
  findings.
- 2026-09-11: completed Phase 4. Added the mountable authority host, durable
  store contract and conformance harness, bounded memory store, exact OAuth
  permission adapter, credential/policy enforcement, monotonic writer directory,
  durable notification leases and transports, operational events, closed
  simple-space types and typed management calls. Settled Q1/Q5/Q6 as
  authority-only hosting, permanent URI tombstones and mandatory subscriber
  policy. Integration covers exact account scopes, delegation/DPoP, two signed
  author snapshots with blob references, durable fanout, sync rebuild and
  deletion cleanup; account-PDS blob upload/download and callback HTTP profiles
  have direct boundary tests.

[guide]: https://gist.github.com/jcalabro/41f1738d22647f8896db4cb178161f07
[proposal]: https://github.com/bluesky-social/proposals/blob/119fa6b63476d30c2516846c714319046e0422f3/0016-permissioned-data/README.md
[pr]: https://github.com/bluesky-social/atproto/pull/5187
[upstream]: https://github.com/bluesky-social/atproto/tree/9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33
[bulletin]: https://github.com/bluesky-social/bulletin/tree/0acf237b872c766a60cec597c5fc10f0b1a58b7f
[rationale]: https://dholms.leaflet.pub/3mu3p3ldwrc26
[deniability]: https://dholms.leaflet.pub/3mqtqvjidqs2p
[encryption]: https://dholms.leaflet.pub/3meluqcwky22a
[alpha]: https://atproto.com/blog/atproto-spaces-alpha
[updates]: https://discourse.atmosphere.community/t/atproto-spaces-alpha-updates/1129
[invites]: https://discourse.atmosphere.community/t/thoughts-on-atmosphere-groups-invite-space/1212
[dpop-rfc]: https://www.rfc-editor.org/rfc/rfc9449
[jkt-rfc]: https://www.rfc-editor.org/rfc/rfc7638
[hkdf-rfc]: https://www.rfc-editor.org/rfc/rfc5869#section-2.3
[space-handlers]: https://github.com/bluesky-social/atproto/tree/9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33/packages/pds/src/api/com/atproto/space
[commit-source]: https://github.com/bluesky-social/atproto/blob/9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33/packages/space/src/repo-commit.ts
[car-source]: https://github.com/bluesky-social/atproto/tree/9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33/packages/space/src/sync
[scopes]: https://github.com/bluesky-social/atproto/blob/9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33/packages/oauth/oauth-scopes/src/scopes/space-permission.ts
