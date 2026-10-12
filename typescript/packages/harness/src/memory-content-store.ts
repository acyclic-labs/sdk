/** Rust-backed bounded storage for MemoryConversation's ephemeral volume. */
import { WasmContentStore } from "../generated/wasm/acyclic_harness_wasm.js";
import type { FileRef, VolumeRef } from "./conversation.js";
import type { PrivateDirectoryPage } from "./runtime.js";

/** Bounded immutable content owned by the Rust WASM provider. */
export function memoryContentStore(volume: VolumeRef, maximumFileBytes: number, maximumPathBytes: number,
  maximumResidentBytes: number, maximumResidentFiles: number) {
  const native = new WasmContentStore(volume, maximumFileBytes, maximumPathBytes, maximumResidentBytes, maximumResidentFiles);
  return {
    stage(path: string, bytes: Uint8Array, mediaType: string, displayName: string, updatePath: boolean): FileRef {
      return freeze(native.stage(path, Uint8Array.from(bytes), mediaType, displayName, updatePath) as FileRef);
    },
    read: (file: FileRef): Uint8Array => Uint8Array.from(native.read(file)),
    has: (file: FileRef): boolean => native.has(file),
    pathConflicts: native.pathConflicts.bind(native),
    generation: (): bigint => native.generation() as bigint,
    list: (path: string, generation: bigint | null, after: string | null, maximum: number) =>
      native.list(path, generation, after, maximum) as Pick<PrivateDirectoryPage, "entries" | "hasMore">,
    readPath: (path: string, generation: bigint): FileRef => freeze(native.read_path(path, generation) as FileRef),
    free: () => native.free(),
  };
}

function freeze<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
