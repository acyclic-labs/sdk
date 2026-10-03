import initObjectsWasm, { initSync as initObjectsWasmSync } from "../generated/wasm/acyclic_objects_wasm.js";

let ready: Promise<void> | undefined;

export async function ensureObjectsWasm(): Promise<void> {
  if (ready === undefined) {
    const attempt = (async () => {
      const url = new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url);
      if (url.protocol === "file:") {
        const runtime = globalThis as typeof globalThis & {
          process?: { getBuiltinModule?: (name: string) => unknown };
        };
        const filesystem = runtime.process?.getBuiltinModule?.(["node", "fs/promises"].join(":")) as {
          readFile?: (path: URL) => Promise<Uint8Array>;
        } | undefined;
        if (filesystem?.readFile !== undefined) {
          await initObjectsWasm({ module_or_path: Uint8Array.from(await filesystem.readFile(url)) });
        } else {
          // Browser bundlers rewrite this asset URL to the emitted WASM file.
          // Keep the URL path for that case; browsers cannot read file URLs
          // directly, but a served bundle will have an http(s) module URL.
          await initObjectsWasm({ module_or_path: url });
        }
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
// Start browser/async loading without making package import itself fail when
// an optional generated asset was omitted. Callers still await ensureObjectsWasm
// before executing a Rust-backed operation and receive the concrete load error.
if (!initializeObjectsWasmSync()) void ensureObjectsWasm().catch(() => {});

function initializeObjectsWasmSync(): boolean {
  const runtime = globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: string) => unknown };
  };
  const getBuiltinModule = runtime.process?.getBuiltinModule;
  if (getBuiltinModule === undefined) return false;
  const filesystem = getBuiltinModule(["node", "fs"].join(":")) as { readFileSync?: (path: URL) => Uint8Array } | undefined;
  if (filesystem?.readFileSync === undefined) return false;
  const wasmUrl = new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url);
  try {
    initObjectsWasmSync({ module: filesystem.readFileSync(wasmUrl) });
    return true;
  } catch {
    return false;
  }
}
