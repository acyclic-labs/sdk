import type { FsVolumeEngine, MemoryFsOptions, WasmRawFs } from "./contracts.js";
import { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS } from "./contracts.js";
import { adaptWasmFs } from "./wasm-adapter.js";

const defaultMaximumObjectBytes = Number(DEFAULT_VOLUME_LIMITS.maximumObjectBytes);
if (!Number.isSafeInteger(defaultMaximumObjectBytes)) {
  throw new RangeError("Rust volume object default exceeds JavaScript's safe integer range");
}

/** Conservative defaults for an in-memory engine. Callers can pass the full option set to tune them. */
export const DEFAULT_MEMORY_FS_OPTIONS: MemoryFsOptions = Object.freeze({
  maximumObjectBytes: defaultMaximumObjectBytes,
  maximumMemoryBytes: 1024 * 1024 * 1024,
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});

export function resolveMemoryFsOptions(options: MemoryFsOptions | undefined): MemoryFsOptions {
  // Rust owns memory option admission. Keep this helper limited to resolving
  // the ergonomic JavaScript default before crossing the generated boundary.
  return options ?? DEFAULT_MEMORY_FS_OPTIONS;
}

export async function openMemoryFsWith(
  options: MemoryFsOptions | undefined,
  load: (options: MemoryFsOptions) => Promise<WasmRawFs>,
): Promise<FsVolumeEngine> {
  return adaptWasmFs(await load(resolveMemoryFsOptions(options)));
}
