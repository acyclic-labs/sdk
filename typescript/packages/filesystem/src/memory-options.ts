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
