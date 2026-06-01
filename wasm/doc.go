//go:build js && wasm

// Package main builds the atmos WebAssembly module for browser JavaScript.
//
// The module registers a single global object, globalThis.atmos, when the Go
// runtime starts. The checked-in atmos.mjs helper loads wasm_exec.js, fetches
// atmos.wasm.gz, starts the Go runtime, and returns that global object:
//
//	import { initAtmos } from './atmos.mjs';
//
//	const atmos = await initAtmos();
//	const did = atmos.parseDID('did:plc:ewvi7nxzyoun6zhxrhs64oiz');
//	console.log(did.method, did.identifier);
//
// A custom asset URL can be supplied when the WASM file is served from another
// path:
//
//	const atmos = await initAtmos('/assets/atmos.wasm.gz');
//
// # Building
//
// Use the repository's Justfile target to build the browser artifacts:
//
//	just wasm
//
// The target builds wasm/atmos.wasm, writes wasm/atmos.wasm.gz, and copies
// wasm_exec.js from the active Go toolchain. To verify the WASM target, use:
//
//	just test-wasm
//
// Serve wasm/atmos.wasm or wasm/atmos.wasm.gz with wasm_exec.js and atmos.mjs
// from the same origin as the page that imports it. Browser security rules
// apply to all network helpers, including identity resolution, XRPC calls, and
// event streams.
//
// # API
//
// Synchronous helpers validate syntax values and throw a JavaScript Error when
// parsing fails:
//
//	atmos.parseDID('did:plc:ewvi7nxzyoun6zhxrhs64oiz');
//	// { method: 'plc', identifier: 'ewvi7nxzyoun6zhxrhs64oiz' }
//
//	atmos.parseHandle('atproto.com');
//	// 'atproto.com'
//
//	atmos.parseATURI(
//	  'at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.post/abc123',
//	);
//	// { authority: 'did:plc:...', collection: 'app.bsky.feed.post', rkey: 'abc123' }
//
//	atmos.parseNSID('app.bsky.feed.post');
//	// { authority: 'app.bsky.feed', name: 'post' }
//
//	atmos.generateTID();
//	// '3l...'
//
// The cbor namespace accepts ordinary JavaScript objects, encodes them as
// DAG-CBOR, decodes DAG-CBOR back to JavaScript values, and computes CIDs over
// encoded bytes:
//
//	const bytes = atmos.cbor.encode({ text: 'hello', langs: ['en'] });
//	const value = atmos.cbor.decode(bytes);
//	const cid = atmos.cbor.computeCID(bytes);
//
// The crypto namespace supports P-256 and K-256 keys. Key material and
// signatures are Uint8Array values:
//
//	const key = atmos.crypto.generateP256();
//	const msg = new TextEncoder().encode('hello');
//	const sig = atmos.crypto.sign(key.privateKey, msg);
//	const ok = atmos.crypto.verify(key.publicKey, msg, sig);
//
//	const parsed = atmos.crypto.parseDIDKey(key.didKey);
//	// { type: 'p256', publicKey: Uint8Array, multibase: 'z...' }
//
// The oauth namespace exposes browser-side helpers for PKCE, DPoP proofs, and
// public JWK conversion:
//
//	const pkce = atmos.oauth.generatePKCE();
//	const proof = atmos.oauth.createDPoPProof(
//	  key.privateKey,
//	  'GET',
//	  'https://bsky.social/xrpc/app.bsky.actor.getProfile',
//	  '',
//	  accessToken,
//	);
//	const jwk = atmos.oauth.publicJWK(key.publicKey);
//
// Async helpers return Promises and reject with a JavaScript Error.
// identity.resolve accepts a DID or handle. In browsers, handle resolution uses
// HTTPS /.well-known/atproto-did only; DNS TXT lookup is not available from
// browser JavaScript:
//
//	const ident = await atmos.identity.resolve('atproto.com');
//	// { did: 'did:plc:...', handle: 'atproto.com', pds: 'https://...', signingKey: 'did:key:...' }
//
// XRPC helpers call lexicon-agnostic endpoints and return decoded JSON:
//
//	const profile = await atmos.xrpc.query(
//	  'https://bsky.social',
//	  'app.bsky.actor.getProfile',
//	  { actor: 'atproto.com' },
//	);
//
//	const result = await atmos.xrpc.procedure(
//	  'https://bsky.social',
//	  'com.atproto.server.createSession',
//	  { identifier, password },
//	);
//
// The firehose namespace opens a streaming client. It returns a source object
// with onEvent(callback) and close() methods. Jetstream subscriptions can be
// filtered with collections and dids:
//
//	const source = atmos.firehose.connect(
//	  'wss://jetstream1.us-east.bsky.network/subscribe',
//	  { collections: ['app.bsky.feed.post'] },
//	);
//
//	source.onEvent((evt) => {
//	  console.log(evt.did, evt.operation, evt.collection, evt.rkey);
//	});
//
//	// Later:
//	source.close();
//
// See wasm/index.html for a runnable browser demo that exercises syntax, CBOR,
// crypto, OAuth, identity resolution, and live event streaming.
package main
