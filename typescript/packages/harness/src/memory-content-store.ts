/** Rust-backed bounded storage for MemoryConversation's ephemeral volume. */
import { WasmContentStore } from "../generated/wasm/acyclic_harness_wasm.js";
import type { FileRef, VolumeRef } from "./conversation.js";
import type { PrivateDirectoryPage } from "./runtime.js";

/** Bounded immutable content owned by the Rust WASM provider. */
export class MemoryContentStore {
  readonly #native: WasmContentStore;

  constructor(
    volume: VolumeRef,
    maximumFileBytes: number,
    maximumPathBytes: number,
    maximumResidentBytes: number,
    maximumResidentFiles: number,
  ) {
    this.#native = new WasmContentStore(
      volume,
      maximumFileBytes,
      maximumPathBytes,
      maximumResidentBytes,
      maximumResidentFiles,
    );
  }

  stage(
    path: string,
    bytes: Uint8Array,
    mediaType: string,
    displayName: string,
    updatePath: boolean,
  ): FileRef {
    return freeze(this.#native.stage(path, Uint8Array.from(bytes), mediaType, displayName, updatePath) as FileRef);
  }

  read(file: FileRef): Uint8Array {
    return Uint8Array.from(this.#native.read(file));
  }

  has(file: FileRef): boolean {
    return this.#native.has(file);
  }

  pathConflicts(path: string): boolean {
    return this.#native.pathConflicts(path);
  }

  generation(): bigint {
    return this.#native.generation() as bigint;
  }

  list(path: string, generation: bigint | null, after: string | null, maximum: number) {
    return this.#native.list(path, generation, after, maximum) as Pick<PrivateDirectoryPage, "entries" | "hasMore">;
  }

  readPath(path: string, generation: bigint): FileRef {
    return freeze(this.#native.read_path(path, generation) as FileRef);
  }

  free(): void {
    this.#native.free();
  }
}

function freeze<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
