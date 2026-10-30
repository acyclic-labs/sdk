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
      if (url.protocol === "file:") {
        const { readFile } = await import("node:fs/promises");
        await initObjectsWasm({ module_or_path: Uint8Array.from(await readFile(url)) });
      } else {
        await initObjectsWasm({ module_or_path: url });
      }
      initialized = true;
    })();
    ready = attempt;
    void attempt.catch(() => { if (ready === attempt) ready = undefined; });
  }
  return signal === undefined ? ready : awaitSignal(ready, signal);
}
