import type { FsVolumeEngine, MemoryFsOptions } from "./contracts.js";
import { openMemoryFsWith } from "./memory-options.js";
import { nodeWasmBindings } from "./wasm-node.js";

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";
export { DEFAULT_MEMORY_FS_OPTIONS } from "./memory-options.js";

export function openMemoryFs(): Promise<FsVolumeEngine>;
export function openMemoryFs(options: MemoryFsOptions): Promise<FsVolumeEngine>;
export function openMemoryFs(options?: MemoryFsOptions): Promise<FsVolumeEngine> {
  return openMemoryFsWith(options, async (resolved) => (await nodeWasmBindings()).openMemoryFs(resolved));
}
