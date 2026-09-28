import { objects_list_page_entries, objects_multipart_parts } from "../generated/wasm/acyclic_objects_wasm.js";
import initObjectsWasm from "../generated/wasm/acyclic_objects_wasm.js";

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

export async function objectsLimits(): Promise<{ readonly listPageEntries: number; readonly multipartParts: number }> {
  await ensureObjectsWasm();
  return { listPageEntries: objects_list_page_entries(), multipartParts: objects_multipart_parts() };
}
