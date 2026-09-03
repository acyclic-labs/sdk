import { readFile } from "node:fs/promises";

import type { FsEngine, MemoryFsOptions, WasmBindings } from "./contracts.js";
import { adaptWasmFs } from "./wasm-adapter.js";

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS } from "./contracts.js";

let bindingsPromise: Promise<WasmBindings> | undefined;

async function bindings(): Promise<WasmBindings> {
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
  });
  return bindingsPromise;
}

export async function openMemoryFs(options: MemoryFsOptions): Promise<FsEngine> {
  if (!Number.isSafeInteger(options.maximumObjectBytes) || options.maximumObjectBytes <= 0) {
    throw new RangeError("memory filesystem object bound must be a positive safe integer");
  }
  return adaptWasmFs(await (await bindings()).openMemoryFs(options));
}
