import initObjectsWasm, { initSync as initObjectsWasmSync } from "../generated/wasm/acyclic_objects_wasm.js";

let ready: Promise<void> | undefined;

export async function ensureObjectsWasm(): Promise<void> {
  if (ready === undefined) {
    const attempt = (async () => {
      const url = new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url);
      if (url.protocol === "file:") {
        const { readFile } = await import("node:fs/promises");
        await initObjectsWasm({ module_or_path: Uint8Array.from(await readFile(url)) });
      } else {
        await initObjectsWasm({ module_or_path: url });
      }
    })();
    ready = attempt;
    void attempt.catch(() => { if (ready === attempt) ready = undefined; });
  }
  await ready;
}

// HTTP constructors synchronously validate their endpoint. Load the packaged
// module before consumers can construct one, matching the Stream boundary.
if (!initializeObjectsWasmSync()) await ensureObjectsWasm();

function initializeObjectsWasmSync(): boolean {
  const runtime = globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: string) => unknown };
  };
  const getBuiltinModule = runtime.process?.getBuiltinModule;
  if (getBuiltinModule === undefined) return false;
  const filesystem = getBuiltinModule("node:fs") as { readFileSync?: (path: URL) => Uint8Array } | undefined;
  if (filesystem?.readFileSync === undefined) return false;
  const wasmUrl = new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url);
  initObjectsWasmSync({ module: filesystem.readFileSync(wasmUrl) });
  return true;
}
