// atproto.mjs — ES module wrapper for the AT Protocol WASM library.
//
// Usage:
//   import { initATProto } from './atproto.mjs';
//   const atp = await initATProto();
//   const did = atp.parseDID('did:plc:abc123');

/**
 * Initialize the AT Protocol WASM module.
 * @param {string} [wasmURL='./atproto.wasm'] - URL to the WASM binary.
 * @returns {Promise<object>} The `atp` global API object.
 */
export async function initATProto(wasmURL = './atproto.wasm') {
  // Load wasm_exec.js (Go's WASM support file).
  if (typeof globalThis.Go === 'undefined') {
    await import('./wasm_exec.js');
  }

  const go = new Go();
  const result = await WebAssembly.instantiateStreaming(
    fetch(wasmURL),
    go.importObject,
  );

  // Start the Go runtime (non-blocking — it runs in the background).
  go.run(result.instance);

  // The Go code sets globalThis.atp once main() runs.
  if (!globalThis.atp) {
    throw new Error('WASM module did not initialize: globalThis.atp not found');
  }

  return globalThis.atp;
}
