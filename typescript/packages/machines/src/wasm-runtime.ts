import { wasmSource, wasmSourceSync } from "@acyclic-labs/core/wasm";
import initMachinesWasm, {
  initSync as initMachinesWasmSync,
  normalize_identity as normalizeIdentityWasm,
  pageSize as pageSizeWasm,
} from "../generated/wasm/acyclic_machines_wasm.js";

const wasmUrl = new URL("../generated/wasm/acyclic_machines_wasm_bg.wasm", import.meta.url);

// The public identity constructors are intentionally synchronous, so every
// importer sees an initialized module. Node and Bun expose the packaged bytes
// through their built-in filesystem API, which avoids a module-level promise
// that some test workers do not await; other runtimes use top-level await.
if (!initializeMachinesWasmSync()) {
  await initMachinesWasm({ module_or_path: await wasmSource(wasmUrl) });
}

/** Normalizes one public identity through the Rust-owned Machines contract. */
export function normalizeIdentity(kind: string, value: string): string {
  return normalizeIdentityWasm(kind, value);
}

function initializeMachinesWasmSync(): boolean {
  const source = wasmSourceSync(wasmUrl);
  if (source === undefined) return false;
  initMachinesWasmSync({ module: source });
  return true;
}

/** Normalize page and console hints through the shared Rust contract. */
export function pageSize(value: number, chunk: number): number {
  try { return pageSizeWasm(value, chunk); }
  catch (error) { throw new RangeError(error instanceof Error ? error.message : String(error)); }
}
