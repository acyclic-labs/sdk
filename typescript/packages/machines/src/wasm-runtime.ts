import initMachinesWasm, {
  initSync as initMachinesWasmSync,
  normalize_identity as normalizeIdentityWasm,
} from "../generated/wasm/acyclic_machines_wasm.js";

const wasmUrl = new URL("../generated/wasm/acyclic_machines_wasm_bg.wasm", import.meta.url);

// The public identity constructors are intentionally synchronous, so every
// importer sees an initialized module. Node and Bun expose the packaged bytes
// through their built-in filesystem API, which avoids a module-level promise
// that some test workers do not await; other runtimes use top-level await.
if (!initializeMachinesWasmSync()) {
  if (wasmUrl.protocol === "file:") {
    const { readFile } = await import("node:fs/promises");
    await initMachinesWasm({ module_or_path: Uint8Array.from(await readFile(wasmUrl)) });
  } else {
    await initMachinesWasm({ module_or_path: wasmUrl });
  }
}

/** Normalizes one public identity through the Rust-owned Machines contract. */
export function normalizeIdentity(kind: string, value: string): string {
  return normalizeIdentityWasm(kind, value);
}

function initializeMachinesWasmSync(): boolean {
  const runtime = globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: string) => unknown };
  };
  const getBuiltinModule = runtime.process?.getBuiltinModule;
  if (getBuiltinModule === undefined) return false;
  const filesystem = getBuiltinModule("node:fs") as { readFileSync?: (path: URL) => Uint8Array } | undefined;
  if (filesystem?.readFileSync === undefined) return false;
  initMachinesWasmSync({ module: filesystem.readFileSync(wasmUrl) });
  return true;
}
