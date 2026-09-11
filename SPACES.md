# Spaces in atmos

Plan for implementing [AT Protocol permissioned data / spaces][proposal] in
atmos, on the `jc/spaces` branch.

Status: planning. No spaces code has landed yet.

Spaces are an early alpha. The reference implementation is [PR #5187][pr],
which is open and still changing. Everything here is pinned to one snapshot and
will drift. Check upstream before trusting any specific detail.

Do not merge this branch to `main` until the spaces APIs are stable and spaces
are live in production (expected to be several months). Keep spaces work off
`main`'s critical path.

## References

- Study guide: <https://gist.github.com/jcalabro/41f1738d22647f8896db4cb178161f07>
- Proposal: [bluesky-social/proposals `0016-permissioned-data`][proposal]
- Reference implementation: [bluesky-social/atproto PR #5187][pr]
- Pinned commit: `9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33` (2026-09-10)
- Reference SDK: `packages/space/src/*` at the pinned commit (`lthash.ts`,
  `repo-commit.ts`, `credential.ts`, `dpop.ts`, `sync/*`, `types.ts`)
- Lexicons at the pinned commit: `lexicons/com/atproto/space/*`,
  `lexicons/com/atproto/simplespace/*`
- Reference app: [bluesky-social/bulletin][bulletin]
- Design diary: <https://dholms.leaflet.pub/> (rationale only, not API docs)

Code references in this document were checked against atmos commit `dc88999` on
this branch. Re-check them if that snapshot is stale.

[proposal]: https://github.com/bluesky-social/proposals/tree/main/0016-permissioned-data
[pr]: https://github.com/bluesky-social/atproto/pull/5187
[bulletin]: https://github.com/bluesky-social/bulletin

## How spaces differ from public atproto

The study guide has the full picture. The parts that shape the design:

- Spaces have two authorities. A space is `(authority DID, type NSID, skey)`,
  and a record URI is
  `at://{authority}/space/{type}/{skey}/{author}/{collection}/{rkey}`. The
  first DID in the URI is the space authority, not the record author. Code that
  assumes the URI authority is the author will break.
- Each author has their own repo per space, on their own PDS. The authority's
  writer directory holds DIDs, revisions, and hashes, not record bodies.
- A permissioned repo commits to a set of `"{collection}/{rkey}/{cid}"` strings
  with an incremental LtHash: a 2048-byte state, 1024 little-endian u16 lanes,
  BLAKE3 XOF per element, and a wire digest of `sha256(state)`. There are no
  inclusion proofs and no tree reconciliation.
- Commits are deniable. `sig` covers `ctx` (space, author, rev, ikm) but not
  the digest. The digest is bound by a symmetric MAC keyed from `ikm`.
- Sync is direct HTTP pull: `listRepos` for the writer directory, then
  `getRepo`/`listRepoOps` against each author's PDS. `registerNotify` and
  `notifyWrite` are best-effort accelerators. There is no relay stream.
- There are three token classes: delegation (user key, 60s, single use),
  client attestation (app key, 60s, single use), and space credential
  (authority key, 7200s, reusable, DPoP-bound).

## Decisions

Settled for this branch:

- Work on `jc/spaces`, never `main`, until spaces are in production.
- Keep spaces in their own `atmos/space` tree. Do not add permissioned
  semantics to the public-repo packages (`repo`, `mst`, `sync`).
- Add a distinct `SpaceRef` (the `(authority, type, skey)` triple) and a space
  record URI type. Do not loosen `ATURI`'s public-repo contract for spaces.
- The SDK should make public vs permissioned data obvious at the type level. A
  space credential must not work on a public-repo call path, and a public
  session must not work on a space path.
- Use `github.com/zeebo/blake3`. Confirm it can produce 2048-byte XOF output
  before depending on it.
- Build the space host side too, not just the client: credential issuance,
  writer directory, and notification fan-out. Verification uses `serviceauth`;
  the transport is up to the caller.
- Treat every PDS and DID-doc endpoint as untrusted. All outbound HTTP goes
  through the hardened `gttp` setup with spaces-specific limits. See "HTTP
  client requirements".
- The syncer takes a caller-provided store for durable state. We define the
  store interface and ship an in-memory implementation for tests and examples.
- Reuse `oauth`'s DPoP and transport code instead of duplicating it under
  `space`. Share what can be shared and fork only what must differ.
- Ship the space host as a library. Provide handlers and functions that a caller
  mounts in their own server, plus an `xrpcserver` registration helper on top.
  Mounting on `xrpcserver` is a convenience, not a requirement.
- Resolve space type declarations through a runtime cache with a caller-supplied
  interface, modeled on `identity.Cache`. Ship an in-memory LRU default. A miss
  fetches with `com.atproto.lexicon.resolveLexicon` and parses with `lexicon`.
  A resolution failure is an error, never a silent fallback.

## Scope

In scope:

- Space addressing and `space-ref` syntax and validation.
- Space type declarations (Lexicon `"type": "space"`).
- Permissioned repo: LtHash, deniable commits, CAR export and verification.
- Credentials: delegation, client attestation, DPoP-bound space credential.
- OAuth `space:` scopes.
- Authority and repo-host identity resolution.
- Syncer: bootstrap, incremental oplog sync, recovery.
- Space host: credential issuance, writer directory, notifications.
- Space record writes and blob references.
- `simplespace` management APIs and managing-app `checkUserAccess`.

Out of scope for now:

- End-to-end encryption. The protocol gives access control, not
  confidentiality, and E2EE is out of scope upstream.
- Relay and firehose integration. There is no spaces record stream.
- Per-record audiences. A record belongs to exactly one space.
- Merging any of this to `main`.

## Lexicon sourcing

The spaces lexicons only exist in the unmerged PR, so `just update-lexicons`
(which clones upstream default branches) cannot supply them. Keep them separate
so main-tracking updates and space updates stay independent:

- `lexicons/` stays as is, refreshed by `just update-lexicons`.
- `lexicons-space/` is new and checked in, refreshed by a script pinned to the
  reference PR commit. `space-lexgen.lock` records the sha.
- `cmd/lexgen` and `lexicon` need to accept multiple lexicon roots, or we run a
  second generation pass for the space namespaces.
- When spaces land upstream, delete `lexicons-space/` and the extra script.
  `update-lexicons` then absorbs them with no other change.

## Package layout

```
atmos/                          SpaceRef, space record URI, space-ref format helper
atmos/space/                    core: LtHash, RepoCommit, commit encode/sign/verify
atmos/space/credential/         delegation / client attestation / space credential JWT
atmos/space/declaration/        space type resolution + cache (modeled on identity)
atmos/space/sync/               client-side syncer: bootstrap, oplog, recovery
atmos/space/host/               server-side: credential issuance, writer dir, notify
atmos/space/simplespace/        management API helpers (policies, members)
```

DPoP proof creation and the RFC 7638 `jkt` thumbprint stay in `oauth`. That
package already has a working P-256/ES256 proof builder, which matches the
upstream DPoP algorithm, and a hardened HTTP client. `space` imports them rather
than forking.

Existing packages we touch:

- `lexicon`, `lexval`: `"type": "space"` fields and the `space-ref` format.
- `lexgen`, `cmd/lexgen`, `lexgen.json`, `lexgen.lock`: space generation.
- `api/comatproto`: generated space and simplespace endpoints.
- `oauth`: `space:` scope parsing and building, credential exchange. It already
  builds its client with `xrpc.ATProtoOpts`, `gttp.WithStrictSSRFProtection()`,
  and `gttp.WithNoProxy()`; extend that instead of adding a second stack.
- `xrpc`: per-request signing hook for `Authorization: DPoP` plus its proof.
  Today the client only sets `Authorization: Bearer`.
- `identity`: `Keys` and `Services` are maps keyed by fragment, and
  `PDSEndpoint()` returns empty when absent. Add the strict space fallback rules
  on top of that.
- `serviceauth`: notify verification and `checkUserAccess` (mostly reuse).
- `internal/lru`: reuse the generic LRU backing `identity.NewLRUCache`.
- `go.mod`: `github.com/zeebo/blake3`.

## SDK surfaces to build

Addressing:

- `SpaceRef` parse, format, and equality; `space-ref` lexicon format validation.
- Space record URI parsing that exposes `SpaceRef`, author DID, collection, and
  rkey.

Credentials and auth:

- Space OAuth scope builder and parser (`authority`, `skey`, `collection`,
  `action`, `manage`), including `read` vs `read_self` and default-collection
  expansion.
- Delegation token request (`getDelegationToken`, on the user's PDS).
- Client attestation signing.
- DPoP-bound credential exchange (`getSpaceCredential`, POST).
- Credential lifetime and refresh handling, with single-use token discipline.

Space types:

- Resolve a space type NSID to its declaration: `name`, `name:lang`, `key`, and
  `collections`.
- `Cache` interface mirroring `identity.Cache` (`Get`/`Set`/`Delete`, nil means
  no caching), an LRU + TTL default, and `Purge` for a forced refresh.
- Single-flight resolution so a cold cache does not stampede the resolver, and
  defensive copies so callers cannot mutate cached entries.

Permissioned repo:

- `LtHash` state, add, remove, digest, equals, empty. Pin endianness so it does
  not depend on the host.
- `RepoCommit` fold over an index, op application, and commit match.
- Commit `ctx` encoding, signing, MAC, and verification.
- CAR `serializeRepo` and `verifyRepoCar`, including index-only mode.

Sync:

- `listRepos` bootstrap of the writer directory.
- Per-author `listRepoOps` paging with `(rev, idx)` cursors.
- Durable state (records, digest, revision) checkpointed atomically.
- Recovery from a full or index-only CAR on digest mismatch.
- `registerNotify` and `unregisterNotify`, plus `notifyWrite` verification.

Host:

- Library-shaped handlers and functions a caller mounts in their own server,
  plus an `xrpcserver` registration helper as a convenience.
- Credential issuance on the authority's key.
- Writer directory updates from accepted write notifications.
- Notification fan-out signed by the authority.
- Registration leases (one day).

Management:

- `simplespace` create/get/update/delete/listMembers/putMember/removeMember.
- `checkUserAccess` outbound calls and the inbound handler.

## Phased plan

### Phase 0: pin and address

- [ ] Vendor space lexicons under `lexicons-space/`, pinned to `9d787eb`. Add
      `space-lexgen.lock` and a refresh script.
- [ ] Let lexgen read multiple roots, then regenerate so `com.atproto.space.*`
      and `com.atproto.simplespace.*` appear in `api/comatproto`.
- [ ] Parse and validate space-type declarations (`name`, `name:lang`,
      `collections`), and add the `"space"` def case to lexgen.
- [ ] Add `SpaceRef` and the space record URI types to `atmos`, and `space-ref`
      to lexval.
- [ ] Space type declaration resolver and cache, modeled on the identity
      package. Tests for miss, hit, expiry, single-flight, and resolution
      failure.
- [ ] Tests: syntax and format vectors, including malformed inputs and the
      literal `space` segment versus a real NSID.

Done when: `just test` passes, `space-ref` round-trips, and no public-repo API
changed.

### Phase 1: permissioned repo crypto

- [ ] Add `github.com/zeebo/blake3` and confirm the XOF output length.
- [ ] `LtHash`: state, add, remove, digest, equals, empty.
- [ ] `RepoCommit`: add, remove, applyOp, matches, fromIndex.
- [ ] Commit `ctx` encoding, signing, and verification (sig and MAC). Pin LE
      lanes and BE length prefixes.
- [ ] Port tests from upstream `packages/space/tests/lthash.test.ts` and
      `repo-commit.test.ts`, including the deniability case.
- [ ] Fuzz the commit decoder and the ctx encoder.

Done when: we match upstream test vectors, and a commit with an altered `hash`
still verifies `sig` but fails `mac`.

### Phase 2: CAR

- [ ] `serializeRepo`: commit root, index root, canonical index key order,
      record blocks in index order, and `excludeValues`.
- [ ] `verifyRepoCar`: verify the commit, fold the index against the digest,
      verify each block CID against its index entry, and require a full drain
      to confirm completeness.
- [ ] Port tests from upstream `packages/space/tests/sync.test.ts`.

Done when: index-only CARs verify, and CARs with missing or reordered blocks
fail.

### Phase 3: auth and DPoP

- [ ] Reuse `oauth`'s DPoP proof builder for space request signing, and add the
      RFC 7638 `jkt` thumbprint. The existing builder is P-256/ES256, matching
      upstream.
- [ ] Add the `xrpc` signing hook: `Authorization: DPoP <cred>` with a proof
      generated per attempt. See the retry note below.
- [ ] Credential JWT classes: build and verify, with `typ`/`kid`/`aud`/`cnf`
      rules.
- [ ] Delegation request, client attestation, and the `getSpaceCredential`
      exchange.
- [ ] `space:` OAuth scope parsing and building with default-collection
      expansion.
- [ ] Tests: `ath` present on reads and absent on exchange, `htu` strips query
      and fragment, wrong `jkt` rejected, and expired or replayed tokens
      rejected.

Done when: it works end to end against a mock authority, and no code path sends
a space credential as a bearer or across a redirect.

### Phase 4: identity resolution

- [ ] Resolve the `#atproto_space` key and `#atproto_space_host` service, with
      fallback to `#atproto` / `#atproto_pds` only when the dedicated entry is
      absent. A present but malformed entry is an error.
- [ ] Resolve an author's repo host from their DID document.
- [ ] Tests: fallback, present and valid, present and malformed, `did:web` and
      `did:plc`, and hostile DID-doc payloads.

Done when: a malformed dedicated entry never falls back silently.

### Phase 5: syncer

- [ ] Bootstrap: register notify, enumerate `listRepos`, fetch each author.
- [ ] Incremental: `listRepoOps` with a stable `since` and correct `(rev, idx)`
      cursor handling across pages within one revision.
- [ ] Persist records, digest, and revision atomically. Never advance a durable
      checkpoint before its records are stored.
- [ ] Verify the terminal commit against the locally updated digest.
- [ ] Recover from a CAR on mismatch, verifying body CIDs.
- [ ] Tests: pagination within a revision, pruned oplog, replayed pages,
      concurrent writes during bootstrap, and notification dedup.

Done when: syncer state matches the authority's claimed commit under retry,
replay, and pruning, and it never publishes partially verified state.

### Phase 6: writes, blobs, host, management

- [ ] Space record writes (`createRecord`, `putRecord`, `deleteRecord`,
      `applyWrites`) with the 200-op batch cap.
- [ ] Blob upload and space reference tracking (`listBlobs`, `getBlob`).
- [ ] Space host: credential issuance, writer directory, notification fan-out,
      and leases.
- [ ] `notifyWrite` verification on both hops: author to authority
      (`iss == repo`, `aud == authority`) and authority to subscriber.
- [ ] `simplespace` helpers and `checkUserAccess`.

Done when: a two-author space round-trips through a mock host with write
notifications, and a syncer rebuilds from CAR.

## HTTP client requirements

Every remote PDS, authority, repo host, and DID-doc endpoint is untrusted. Do
not use `http.DefaultClient` or a one-off transport for spaces.

- Use the shared `gttp` constructors (`xrpc.ATProtoOpts`,
  `xrpc.NewHTTPClient`, `xrpc.NewTransport`) so header limits, timeouts, and
  connection-pool bounds are consistent. `oauth` already builds its client as
  `gttp.New(append(xrpc.ATProtoOpts(...), gttp.WithStrictSSRFProtection(),
  gttp.WithNoProxy())...)`; space clients should do the same.
- Use the bulk-download tuning for CAR and blob transfers, not the interactive
  timeout profile: `BulkResponseHeaderTimeout`, `BulkIdleTimeout`,
  `BulkMinTransferBytes`, and `BulkMinTransferWindow`.
- Apply SSRF protection to any URL that comes from attacker-controlled input,
  such as service endpoints from DID docs or `#atproto_space_host`. The existing
  code does this with `gttp.WithStrictSSRFProtection`.
- Do not follow cross-origin redirects while carrying a space credential. Dial
  the resolved repo host directly.
- Cap response sizes. CARs and blobs are attacker-sized.
- Fail closed. A malformed endpoint or key entry is an error, never a fallback.

### DPoP retry note

`xrpc.doInternal` retries the same logical request. A DPoP proof is bound to a
one-time `jti` and to the method and URL, so resending the same header fails
verification. Space request signing has to generate a new proof on every
attempt, inside the retry loop. Another option is to disable transport-level
retries for DPoP requests. A fixed proof header injected by a `RoundTripper`
that is itself retried is a bug.

## Security and correctness rules

- Verify commit `sig` and `mac` before treating `hash` as meaningful.
- Digest agreement tells you the contents match. It says nothing about
  freshness or cross-author ordering.
- LtHash updates are not idempotent. Cursor and checkpoint handling has to be
  correct on its own; the hash will not catch a bad consumer.
- Commit signatures are deniable by design. Do not build provenance claims on a
  copied commit.
- This alpha has no guaranteed "history too old" error. Recover on digest
  mismatch.
- A space credential can read the whole space. Scope it to the resolved host,
  regenerate proofs per request, and never log it.

## Testing strategy

Write the failing test first, and make it fail for a reason that matters. The
hard parts here are cross-implementation agreement, hostile input, and
distributed sync, so the plan is layered and each layer answers one question.

### Ground rules

- Do not use our own code as the correctness reference. The oracle is the
  pinned upstream `@atproto/space` package and the protocol text.
- Do not mock the crypto. Use real keys, signatures, and hashes. Tests may
  inject randomness for determinism, but must not replace BLAKE3, HMAC, or
  ECDSA.
- Bad input must produce an error. Write a test for every parse and verify path
  proving it does not return a default or a fallback.
- Only compare bytes where the protocol is deterministic. Commit `ikm` is
  random and ECDSA signing uses `crypto/rand`, so signatures differ run to run.
  Golden vectors check verification and structure. Byte-for-byte comparison is
  for deterministic values only: ctx bytes, digest state, and MACs under a fixed
  ikm.
- Where practical, pair each positive test with one that corrupts a single field
  and expects rejection.

### Layer 1: syntax and format vectors

- Put `SpaceRef`, space-URI, and `space-ref` vectors in the existing
  `testdata/*_syntax_{valid,invalid}.txt` style. Reuse upstream vectors where
  they exist and add space-specific ones.
- Cover the minimal authority-only form, the type without a key, every valid
  `key` mode, a 512-byte skey, a non-NSID type, the literal `space` versus a
  real NSID collection, an embedded author DID, and malformed or oversized
  input.

### Layer 2: unit and property tests

- LtHash: empty-state digest; add/remove identity; order independence (shuffle
  adds, digest unchanged); non-idempotence (`add` twice differs from `add`
  once); remove then add restores; endianness pinned regardless of host; state
  versus digest; boundary lane values and mod-2^16 wraparound.
- Commit ctx: exact byte layout (domain tag, BE length prefixes) against
  hand-computed fixtures, and rejection of a field longer than 65535.
- Commit sign and verify: check both P-256 and K-256 author keys; reject a wrong
  `ver`, `rev`, author, or space; a tampered `hash` fails the MAC; tampered
  `mac` or `sig` fails. Also cover the deniability property from upstream
  `repo-commit.test.ts`: with a fixed ikm, build a valid MAC for a different
  `hash` while the original `sig` still verifies.
- RepoCommit fold: `fromIndex`; `applyOp` for create, update, and delete with
  the right `prev` and `cid`; and `matches` only after the commit is
  authenticated.

### Layer 3: cross-implementation vectors

- A dev-only Node generator, pinned to the PR commit, runs the upstream
  `@atproto/space` package over fixed inputs and writes JSON vectors into
  `testdata/space/`. Check the output in so CI never needs Node.
- Vector classes: LtHash states and digests over op sequences, ctx bytes, MACs
  under a fixed ikm, CAR bytes for fixed repos (including index-only), and
  rejection cases (bad commits, malformed CARs).
- Leave out the non-deterministic fields (random ikm, ECDSA sigs). Layers 2 and
  4 cover those with verification tests.
- Regenerate deliberately. A script records the upstream sha in
  `space-lexgen.lock`. An unexplained vector diff is a bug, not a rerun.

### Layer 4: adversarial parsing and fuzzing

- Fuzz every decoder that reads untrusted bytes: commit CBOR, CAR reader and
  header, repo index, oplog responses, space JWTs, DPoP proofs, and DID
  documents.
- Fuzz targets assert no panic, no unbounded allocation, and no acceptance of
  structurally invalid input. Seed with valid vectors so the fuzzer reaches
  deep states instead of bouncing off random bytes.
- JWT matrix: wrong `typ`, missing or extra `kid`, `alg` confusion (`none` or
  `HS256` against an EC key), oversized claims, malformed base64url or JSON,
  `aud`/`sub` mismatch, expired and future-dated (`iat`) tokens with skew, and
  `cnf.jkt` absent, present, or wrong.
- DPoP matrix: `htm` mismatch; `htu` with query or fragment (must be stripped);
  missing `jti`; replayed `jti`; a proof older than the 60-second max age;
  `ath` present on exchange and absent on read, and the inverse; embedded-JWK
  thumbprint mismatch; and cross-key substitution.
- Identity matrix: key or service entry absent (fallback allowed) versus
  present but malformed (error, never fallback); multiple or colliding
  fragments; wrong fragment; `did:web` and `did:plc`; hostile service endpoints
  (SSRF-shaped, non-http schemes, redirects); oversized documents; and negative
  caching with TTL.

### Layer 5: transport and failure injection

- A scriptable `RoundTripper` drives the xrpc and space clients through
  connection refused, reset mid-body, every truncation offset of a CAR and a
  blob, malformed JSON, 4xx and 5xx, 429 with `Retry-After`, a slowloris trickle
  below the throughput floor, and responses over the size caps.
- Assert bounded memory and time, typed errors, no partial state published on
  failure, no ambiguous retry of non-idempotent POSTs, and DPoP proof
  regeneration per attempt. A replayed `jti` must never be sent, including
  across retries.
- Check that the credential is never sent as `Bearer`, never follows a
  cross-origin redirect, and is never logged.

### Layer 6: sync correctness harness

The syncer is the riskiest part, so give it a model-based, fault-injecting
harness instead of example tests.

- Simulate an authority and N author PDSes with a controllable clock and an
  oracle that knows the expected digest for each `(author, space)`.
- Fault schedule: reorder, duplicate, and drop notifications; delay writes
  across bootstrap; prune the oplog mid-sync; return one revision across
  multiple pages; replay pages; truncate or corrupt a CAR; drop the terminal
  commit hint; restart the syncer at arbitrary points; and write concurrently
  during recovery.
- Check these after every step and at convergence:
  - syncer state matches the authority digest before anything is published;
  - no durable checkpoint advances before its records are stored;
  - `since` is preserved across pages within a revision;
  - re-applying a page after a retry is idempotent at the checkpoint layer,
    even though LtHash add is not;
  - recovery from a digest mismatch completes without operator input.
- Drive the harness with randomized op sequences (a property test) plus a fixed
  regression corpus, one entry per fault class.

### Layer 7: concurrency and race

- Run `just test-race` on the syncer and host.
- Scenarios: concurrent sync of multiple authors, credential refresh racing
  in-flight reads, notification fan-out under load, writer-directory updates
  concurrent with `listRepos`, duplicate notification delivery, and shutdown
  with requests in flight.
- Assert no data races, no double-apply, no leaked goroutines, and that
  cancellation reaches in-flight HTTP.

### Layer 8: resource bounds and mechanical sympathy

- Assert, and where useful benchmark: LtHash does fixed work per element
  regardless of repo size; CAR serialization allocation behavior; oplog paging
  allocation; and response and body size caps.
- Check for goroutine and body leaks on streaming paths, using
  `runtime.NumGoroutine` deltas or a goleak-style check. Every `io.ReadCloser`
  must close on error paths.
- Keep performance claims out of code and docs unless measured here.

### Layer 9: wire integration (gated, manual)

- Against the PDS in PR #5187 and the bulletin app: real credential exchange,
  `listRepos`/`listRepoOps`/`getRepo`, `registerNotify` and `notifyWrite`, and
  the `simplespace` lifecycle.
- These are not in default CI. They need the reference stack and the alpha is
  still moving. Run them before each spaces milestone and record the pinned
  upstream sha with the result.

### Observability

- Sync and host tests assert on typed errors and structured events, not just
  booleans, so operators can tell what failed. A silent skip is a test failure.

### CI

- Default: `just lint` and `just test`, plus `just test-race` for touched
  packages.
- Short fuzzing on PRs, with a longer budget on a schedule or via `just fuzz`.
- `just wasm` must also pass before each commit.
- Vector regeneration is its own reviewable step. Never a silent side effect of
  a build.

### Out of scope for tests

- Testing against our own implementation as the oracle.
- Bypassing verification behind a test-only flag. Test seams are injection
  points for randomness, clock, and transport, not switches that disable
  checks.

## Changelog

- 2026-09-11: initial plan. Decisions locked: branch, standalone package,
  `SpaceRef`/`SpaceURI`, zeebo/blake3, host SDK in scope, gttp hardening.
- 2026-09-11: expanded the testing strategy into layers (syntax, unit and
  property, cross-implementation vectors, adversarial and fuzz, transport
  failure injection, sync harness, race, resource bounds, gated wire
  integration) with oracle and determinism rules.
- 2026-09-11: language pass for plain prose.
- 2026-09-11: corrected the lexicon-cache question. There is no existing
  lexicon cache in atmos, so the question is whether to build one. Recorded
  answers for the syncer store, DPoP reuse, and the host as a library.
- 2026-09-11: consistency pass. Moved DPoP and jkt into `oauth` to match the
  reuse decision, made the host library-first in the layout and phases, added
  the verified `oauth`/`identity`/`xrpc` details, and named the bulk-download
  constants.
- 2026-09-11: decided space type declarations get a runtime cache modeled on
  `identity.Cache`. Added the `space/declaration` package, the SDK surface, and
  a Phase 0 task. Removed the open questions section.
