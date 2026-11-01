import { resolveObserver } from "@acyclic-labs/actors/observe";
import type { FsVolumeEngine, MemoryFsOptions } from "./contracts.js";
import type { WasmRawFs } from "./binding-types.js";
import { DEFAULT_OBJECT_CACHE_OPTIONS } from "./contracts.js";
import { adaptFs } from "./filesystem-adapter.js";

/** Memory limits are opt-in; the object cache controls residency independently. */
export const DEFAULT_MEMORY_FS_OPTIONS: MemoryFsOptions = Object.freeze({
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});

export async function openMemoryFsWith(
  options: MemoryFsOptions | undefined,
  load: (options: MemoryFsOptions) => Promise<WasmRawFs>,
): Promise<FsVolumeEngine> {
  return adaptFs(await load(options ?? DEFAULT_MEMORY_FS_OPTIONS), resolveObserver(options?.observer));
}
