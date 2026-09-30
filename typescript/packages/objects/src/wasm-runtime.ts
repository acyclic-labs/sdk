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
