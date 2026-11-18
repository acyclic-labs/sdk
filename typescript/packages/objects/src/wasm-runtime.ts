import { wasmSource } from "@acyclic-labs/core/wasm";
import initObjectsWasm from "../generated/wasm/acyclic_objects_wasm.js";
import { awaitSignal } from "@acyclic-labs/auth/credential";

let ready: Promise<void> | undefined;
let initialized = false;

export function ensureObjectsWasm(signal?: AbortSignal): void | Promise<void> {
  signal?.throwIfAborted();
  if (initialized) return;
  if (ready === undefined) {
    const attempt = (async () => {
      const url = new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url);
      await initObjectsWasm({ module_or_path: await wasmSource(url) });
      initialized = true;
    })();
    ready = attempt;
    void attempt.catch(() => { if (ready === attempt) ready = undefined; });
  }
  return signal === undefined ? ready : awaitSignal(ready, signal);
}
