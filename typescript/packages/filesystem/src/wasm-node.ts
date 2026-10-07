import { readFile } from "node:fs/promises";

import type { WasmBindings } from "./contracts.js";

let bindingsPromise: Promise<WasmBindings> | undefined;

/** Loads the packaged WASM build from disk in Node-compatible runtimes. */
export async function nodeWasmBindings(): Promise<WasmBindings> {
  const generatedModule: string = "../generated/wasm/acyclic_fs_wasm.js";
  bindingsPromise ??= import(generatedModule).then(async (module): Promise<WasmBindings> => {
    const typed = module as WasmBindings;
    const bytes = await readFile(
      new URL("../generated/wasm/acyclic_fs_wasm_bg.wasm", import.meta.url),
    );
    if (!(bytes.buffer instanceof ArrayBuffer)) {
      throw new TypeError("packaged WASM bytes are not backed by an ArrayBuffer");
    }
    const compiled = await WebAssembly.compile(
      new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    );
    await typed.default({ module_or_path: compiled });
    return typed;
  }).catch((error: unknown) => {
    bindingsPromise = undefined;
    throw error;
  });
  return bindingsPromise;
}
