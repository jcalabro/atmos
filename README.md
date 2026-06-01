# atmos ☁️

[![Go Reference](https://pkg.go.dev/badge/github.com/jcalabro/atmos.svg)](https://pkg.go.dev/github.com/jcalabro/atmos)
[![Go Version](https://img.shields.io/github/go-mod/go-version/jcalabro/atmos)](https://github.com/jcalabro/atmos/blob/main/go.mod)
[![Latest Release](https://img.shields.io/github/v/release/jcalabro/atmos)](https://github.com/jcalabro/atmos/releases/latest)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](https://github.com/jcalabro/atmos/blob/main/LICENSE-DUAL)
[![CI](https://github.com/jcalabro/atmos/actions/workflows/ci.yml/badge.svg)](https://github.com/jcalabro/atmos/actions/workflows/ci.yml)

[Atmos](https://www.youtube.com/watch?v=cXTlFx5z9_c&list=PLESQxfE6Z-wpSUECVDVsntZ_A33Z3nlqQ) is a go library for [atproto](https://atproto.com). Designed to be correct, fast, and easy to use.

## Packages

| Package | Description |
|-|-|
| [`atmos`](https://pkg.go.dev/github.com/jcalabro/atmos) | syntax types (DID, Handle, NSID, etc.) |
| [`atmos/api/bsky`](https://pkg.go.dev/github.com/jcalabro/atmos/api/bsky) | generated XRPC types and client functions for the `app.bsky.*` lexicons |
| [`atmos/api/chatbsky`](https://pkg.go.dev/github.com/jcalabro/atmos/api/chatbsky) | generated XRPC types and client functions for the `chat.bsky.*` lexicons |
| [`atmos/api/comatproto`](https://pkg.go.dev/github.com/jcalabro/atmos/api/comatproto) | generated XRPC types and client functions for the `com.atproto.*` lexicons |
| [`atmos/api/lextypes`](https://pkg.go.dev/github.com/jcalabro/atmos/api/lextypes) | shared generated Lexicon helper types, including blob, CID link, and open union support |
| [`atmos/api/ozone`](https://pkg.go.dev/github.com/jcalabro/atmos/api/ozone) | generated XRPC types and client functions for the `tools.ozone.*` lexicons |
| [`atmos/backfill`](https://pkg.go.dev/github.com/jcalabro/atmos/backfill) | concurrent engine for downloading and processing all repositories from a relay or PDS |
| [`atmos/car`](https://pkg.go.dev/github.com/jcalabro/atmos/car) | CAR files |
| [`atmos/cbor`](https://pkg.go.dev/github.com/jcalabro/atmos/cbor) | DAG-CBOR implementation |
| [`atmos/crypto`](https://pkg.go.dev/github.com/jcalabro/atmos/crypto) | P-256 and K-256 (secp256k1) key pairs, signing, verification, and did:key encoding |
| [`atmos/identity`](https://pkg.go.dev/github.com/jcalabro/atmos/identity) | DID resolution and handle verification |
| [`atmos/labeling`](https://pkg.go.dev/github.com/jcalabro/atmos/labeling) | label creation, signing, and verification |
| [`atmos/lexgen`](https://pkg.go.dev/github.com/jcalabro/atmos/lexgen) | code generator that produces Go types and functions from Lexicon JSONs |
| [`atmos/lexicon`](https://pkg.go.dev/github.com/jcalabro/atmos/lexicon) | Lexicon JSON schema parser |
| [`atmos/lexval`](https://pkg.go.dev/github.com/jcalabro/atmos/lexval) | data validation against Lexicon schemas |
| [`atmos/mst`](https://pkg.go.dev/github.com/jcalabro/atmos/mst) | Merkle Search Tree implementation |
| [`atmos/oauth`](https://pkg.go.dev/github.com/jcalabro/atmos/oauth) | OAuth 2.0 client with PKCE, PAR, and DPoP |
| [`atmos/plc`](https://pkg.go.dev/github.com/jcalabro/atmos/plc) | client for the DID PLC directory |
| [`atmos/repo`](https://pkg.go.dev/github.com/jcalabro/atmos/repo) | atproto repository operations |
| [`atmos/serviceauth`](https://pkg.go.dev/github.com/jcalabro/atmos/serviceauth) | inter-service JWT authentication |
| [`atmos/streaming`](https://pkg.go.dev/github.com/jcalabro/atmos/streaming) | client for consuming event streams (firehose and labelers) |
| [`atmos/streaming/parallel`](https://pkg.go.dev/github.com/jcalabro/atmos/streaming/parallel) | per-key FIFO scheduler used to parallelize stream verification while preserving same-DID order |
| [`atmos/sync`](https://pkg.go.dev/github.com/jcalabro/atmos/sync) | repository sync, commit verification, Sync 1.1 firehose verifier, and repo enumeration |
| [`atmos/wasm`](https://pkg.go.dev/github.com/jcalabro/atmos/wasm) | WebAssembly build that exposes atmos syntax, CBOR, crypto, OAuth, identity, XRPC, and firehose helpers to browser JavaScript |
| [`atmos/xrpc`](https://pkg.go.dev/github.com/jcalabro/atmos/xrpc) | lexicon-agnostic XRPC HTTP client |
| [`atmos/xrpcserver`](https://pkg.go.dev/github.com/jcalabro/atmos/xrpcserver) | XRPC HTTP server with `/xrpc/{nsid}` routing and standard error envelopes |

## License

Dual-licensed under MIT and Apache 2.0.
