import type { FsVolumeEngine, MemoryFsOptions, ObjectCacheOptions, WasmRawFs } from "./contracts.js";
import { adaptWasmFs } from "./wasm-adapter.js";

/** Conservative defaults for an in-memory engine. Callers can pass the full option set to tune them. */
export const DEFAULT_MEMORY_FS_OPTIONS: MemoryFsOptions = Object.freeze({
  maximumObjectBytes: 64 * 1024 * 1024,
  maximumMemoryBytes: 1024 * 1024 * 1024,
  objectCache: Object.freeze({
    maximumEntries: 4096,
    maximumBytes: 256 * 1024 * 1024,
    maximumInFlight: 1024,
    maximumWaitersPerObject: 1024,
  }) satisfies ObjectCacheOptions,
});

export function resolveMemoryFsOptions(options: MemoryFsOptions | undefined): MemoryFsOptions {
  const resolved = options ?? DEFAULT_MEMORY_FS_OPTIONS;
  if (!Number.isSafeInteger(resolved.maximumObjectBytes) || resolved.maximumObjectBytes <= 0) {
    throw new RangeError("memory filesystem object bound must be a positive safe integer");
  }
  if (
    !Number.isSafeInteger(resolved.maximumMemoryBytes)
    || resolved.maximumMemoryBytes < resolved.maximumObjectBytes
  ) {
    throw new RangeError("memory filesystem aggregate bound must cover one maximum object");
  }
  return resolved;
}

export async function openMemoryFsWith(
  options: MemoryFsOptions | undefined,
  load: (options: MemoryFsOptions) => Promise<WasmRawFs>,
): Promise<FsVolumeEngine> {
  return adaptWasmFs(await load(resolveMemoryFsOptions(options)));
}
