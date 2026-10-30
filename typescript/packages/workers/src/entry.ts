/** Selected immutable entry publication through the shared Rust store. */
import { EntryStore as RustEntryStore, type EntryRequest, type EntryResult, type PublishedDeployment } from "../generated/wasm/acyclic_manifest.js";
import { ObjectsV1Error, ErrorCode, type ObjectsV1BinaryPort, type ObjectsV1BinaryReply } from "@acyclic-labs/objects";
export type { PublishedDeployment } from "../generated/wasm/acyclic_manifest.js";

/** Only this entry's declared artifact and sibling modules are supplied. */
export interface EntrySelection {
  readonly manifest: string;
  readonly environment: string;
  readonly entry: string;
  readonly artifact: Uint8Array;
  readonly files: readonly { readonly path: string; readonly bytes: Uint8Array }[];
}

async function pull(request: EntryRequest, signal?: AbortSignal): Promise<EntryResult> {
  const cancel = () => request.close();
  try {
    signal?.throwIfAborted();
    signal?.addEventListener("abort", cancel, { once: true });
    const result = await request.next();
    signal?.throwIfAborted();
    if (result === null) throw new ObjectsV1Error(ErrorCode.UNAVAILABLE);
    return result;
  } catch (error) {
    signal?.throwIfAborted();
    throw error;
  } finally {
    signal?.removeEventListener("abort", cancel);
    request.close();
    request.free();
  }
}

/** Retain the original admitted account; construction performs no I/O. */
export class EntryStore {
  readonly #inner: RustEntryStore;
  constructor(objects: ObjectsV1BinaryPort) { this.#inner = new RustEntryStore(objects); }

  /** Admit once and retain the complete closure under the caller's stable intent. */
  async publishDeployment(intent: Uint8Array, selection: EntrySelection, idempotencyKey: string, signal?: AbortSignal): Promise<PublishedDeployment> {
    signal?.throwIfAborted();
    const result = await pull(this.#inner.openPublication(intent, selection.manifest, selection.environment, selection.entry,
      selection.artifact, selection.files.map(file => file.path), selection.files.map(file => file.bytes), idempotencyKey), signal);
    if (!("request" in result)) throw new ObjectsV1Error(ErrorCode.UNAVAILABLE);
    return result;
  }

  /** Release only this intent; the reply keeps its original serving revision. */
  async release(intent: Uint8Array, signal?: AbortSignal): Promise<ObjectsV1BinaryReply> {
    signal?.throwIfAborted();
    const result = await pull(this.#inner.openRelease(intent), signal);
    if (!("body" in result)) throw new ObjectsV1Error(ErrorCode.UNAVAILABLE);
    return result;
  }
}
