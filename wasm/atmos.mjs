// atmos.mjs — ES module wrapper for the atmos WASM library.
//
// Usage:
//   import { initAtmos } from './atmos.mjs';
//   const atmos = await initAtmos();
//   const did = atmos.parseDID('did:plc:abc123');

/**
 * Initialize the atmos WASM module.
 * @param {string} [wasmURL='./atmos.wasm.gz'] - URL to the gzipped WASM binary.
 * @returns {Promise<object>} The `atmos` global API object.
 */
export async function initAtmos(wasmURL = './atmos.wasm.gz') {
  // Load wasm_exec.js (Go's WASM support file).
  if (typeof globalThis.Go === 'undefined') {
    await import('./wasm_exec.js');
  }

  const go = new Go();
  const resp = await fetch(wasmURL);
  const decompressed = new Response(
    resp.body.pipeThrough(new DecompressionStream('gzip')),
    { headers: { 'Content-Type': 'application/wasm' } },
  );
  const result = await WebAssembly.instantiateStreaming(
    decompressed,
    go.importObject,
  );

  // Start the Go runtime (non-blocking — it runs in the background).
  go.run(result.instance);

  // The Go code sets globalThis.atmos once main() runs.
  if (!globalThis.atmos) {
    throw new Error('WASM module did not initialize: globalThis.atmos not found');
  }

  return globalThis.atmos;
}
