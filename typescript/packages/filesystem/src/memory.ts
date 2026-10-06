import type { FsVolumeEngine, MemoryFsOptions, WasmBindings } from "./contracts.js";
import { resolveMemoryFsOptions } from "./memory-options.js";
import { adaptWasmFs, rustPositiveBoundValidator } from "./wasm-adapter.js";

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";
export { DEFAULT_MEMORY_FS_OPTIONS } from "./memory-options.js";

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

export function openMemoryFs(): Promise<FsVolumeEngine>;
export function openMemoryFs(options: MemoryFsOptions): Promise<FsVolumeEngine>;
export function openMemoryFs(options?: MemoryFsOptions): Promise<FsVolumeEngine> {
  return bindings().then(binding => adaptWasmFs(
    binding.openMemoryFs(resolveMemoryFsOptions(options)),
    rustPositiveBoundValidator(binding.validateHostedPageBound),
  ));
}
