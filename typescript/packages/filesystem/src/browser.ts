import type { BrowserFsOptions, FsVolumeEngine, WasmBindings } from "./contracts.js";
import { adaptWasmFs } from "./wasm-adapter.js";

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";

let bindingsPromise: Promise<WasmBindings> | undefined;

async function bindings(): Promise<WasmBindings> {
  const generatedModule: string = "../generated/wasm/acyclic_fs_wasm.js";
  bindingsPromise ??= import(generatedModule).then(async (module): Promise<WasmBindings> => {
    const typed = module as WasmBindings;
    await typed.default();
    return typed;
  });
  return bindingsPromise;
}

export async function openBrowserFs(options: BrowserFsOptions): Promise<FsVolumeEngine> {
  if (options.databaseName.length === 0 || options.maximumObjectBytes <= 0) {
    throw new RangeError("browser filesystem options must be bounded and non-empty");
  }
  return adaptWasmFs(await (await bindings()).openBrowserFs(options));
}
