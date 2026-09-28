import initMachinesWasm, {
  initSync as initMachinesWasmSync,
  normalize_identity as normalizeIdentityWasm,
} from "../generated/wasm/acyclic_machines_wasm.js";

let ready: Promise<void> | undefined;
let initialized = false;

// The public identity constructors are intentionally synchronous. Node and Bun
// expose the packaged WASM bytes through their built-in filesystem API, so load
// that module before any constructor can be called. Browser callers use the
// asynchronous ensureMachinesWasm() path before constructing identities.
initialized = initializeMachinesWasmSync();

export async function ensureMachinesWasm(): Promise<void> {
  if (initialized) return;
  if (ready === undefined) {
    const attempt = (async () => {
      const url = new URL("../generated/wasm/acyclic_machines_wasm_bg.wasm", import.meta.url);
      if (url.protocol === "file:") {
        const { readFile } = await import("node:fs/promises");
        await initMachinesWasm({ module_or_path: Uint8Array.from(await readFile(url)) });
      } else {
        await initMachinesWasm({ module_or_path: url });
      }
      initialized = true;
    })();
    ready = attempt;
    void attempt.catch(() => { if (ready === attempt) ready = undefined; });
  }
  await ready;
}

/** Normalizes one public identity through the Rust-owned Machines contract. */
export function normalizeIdentity(kind: string, value: string): string {
  if (!initialized) {
    throw new Error("Machines identity constructors require await ensureMachinesWasm() in browser runtimes");
  }
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
  const wasmUrl = new URL("../generated/wasm/acyclic_machines_wasm_bg.wasm", import.meta.url);
  initMachinesWasmSync({ module: filesystem.readFileSync(wasmUrl) });
  return true;
}
