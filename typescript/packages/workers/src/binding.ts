import type { WorkersClientOptions, WorkersCallOptions, WorkersFailure, WorkersTransportPreference } from "./generated/semantic/workers/readonly.js";
import { WorkersTransportError } from "./client.js";
import { isExpectedNativeAbsence } from "./generated/native-absence.js";
import { WORKERS_BINDING_VERSION } from "./generated/workers-binding.js";

export interface RustClient {
  readonly transport: string;
  call(method: string, bytes: Uint8Array, options: WorkersCallOptions, signal?: AbortSignal): Promise<Uint8Array>;
}
interface Cancellation { cancel(): void; free?(): void }
interface RustBinding { connect(config: WorkersClientOptions, signal?: AbortSignal): Promise<RustClient> }
let native: Promise<RustBinding | undefined> | undefined;
let wasm: Promise<RustBinding> | undefined;
export async function loadBinding(preference?: WorkersTransportPreference): Promise<RustBinding> {
  const isNode = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node !== undefined;
  if (preference !== "wasm" && isNode) {
    const binding = await loadNative();
    if (binding !== undefined) return binding;
  }
  // Explicit native requirement is still admitted/rejected by Rust configuration on this backend.
  return loadWasm();
}
function checkedVersion(Client: { version(): string }): void {
  if (typeof Client?.version !== "function" || Client.version() !== WORKERS_BINDING_VERSION) {
    throw new WorkersTransportError("Workers binding package identity mismatch", "configuration");
  }
}
function cancellation(Cancel: new () => Cancellation, signal?: AbortSignal) {
  if (typeof Cancel !== "function") throw new WorkersTransportError("Workers binding cancellation export missing", "configuration");
  const handle = new Cancel();
  const abort = () => handle.cancel();
  if (signal?.aborted) abort();
  else signal?.addEventListener("abort", abort, { once: true });
  return { handle, cleanup: () => { signal?.removeEventListener("abort", abort); handle.free?.(); } };
}
async function nativeResult<T>(pending: Promise<{ value?: T | null; client?: T | null; error?: WorkersFailure | null }>, key: "value" | "client"): Promise<T> {
  try {
    const result = await pending;
    const value = result[key];
    if (value != null) return value;
    throw new WorkersTransportError(result.error?.message ?? "Workers operation failed", result.error?.code ?? "workers_error", result.error ?? undefined);
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "InvalidArg") {
      throw new WorkersTransportError(error instanceof Error ? error.message : "Invalid Rust binding argument", "invalid_argument");
    }
    throw error;
  }
}
function loadNative(): Promise<RustBinding | undefined> {
  native ??= (async () => {
    // @ts-ignore maintained NAPI output is produced by qualification, not checked in as authored source.
    const imported = await import("../generated/native/binding.cjs");
    const { Buffer } = await import("node:buffer");
    const module = typeof imported.WorkersClient === "function" ? imported : imported.default;
    checkedVersion(module?.WorkersClient);
    if (typeof module.WorkersClient.connectResult !== "function" || typeof module.WorkersCancellation !== "function") {
      throw new WorkersTransportError("Workers native ABI exports missing", "configuration");
    }
    return { async connect(config: WorkersClientOptions, signal?: AbortSignal): Promise<RustClient> {
      const cancel = cancellation(module.WorkersCancellation, signal);
      let client;
      try { client = await nativeResult<InstanceType<typeof module.WorkersClient>>(module.WorkersClient.connectResult(config, cancel.handle), "client"); }
      finally { cancel.cleanup(); }
      return { transport: client.transport, call(method, bytes, options, signal) {
        const snapshot = bytes.slice();
        const cancel = cancellation(module.WorkersCancellation, signal);
        return nativeResult<Uint8Array>(client.call(method, Buffer.from(snapshot), options, cancel.handle), "value").finally(cancel.cleanup);
      } };
    } };
  })().catch(async error => {
    native = undefined;
    if (await isExpectedNativeAbsence(error)) return undefined;
    throw error;
  });
  return native;
}
function loadWasm(): Promise<RustBinding> {
  wasm ??= (async () => {
    // @ts-ignore maintained wasm-bindgen output is produced by qualification.
    const module = await import("../generated/wasm/acyclic_workers_wasm.js");
    const isNode = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node !== undefined;
    if (isNode) {
      const { readFile } = await import("node:fs/promises");
      await module.default(await readFile(new URL("../generated/wasm/acyclic_workers_wasm_bg.wasm", import.meta.url)));
    } else await module.default();
    checkedVersion(module.WorkersClient);
    return { async connect(config: WorkersClientOptions, signal?: AbortSignal): Promise<RustClient> {
      const cancel = cancellation(module.WorkersCancellation, signal);
      let client;
      try { client = await module.WorkersClient.connect(config, cancel.handle).catch(wasmError); }
      finally { cancel.cleanup(); }
      return { transport: client.transport, call(method, bytes, options, signal) {
        const snapshot = bytes.slice();
        const cancel = cancellation(module.WorkersCancellation, signal);
        return client.call(method, snapshot, options, cancel.handle).catch(wasmError).finally(cancel.cleanup);
      } };
    } };
  })().catch(error => { wasm = undefined; throw error; });
  return wasm;
}

function wasmError(error: unknown): never {
  if (error instanceof Error && "code" in error) {
    throw new WorkersTransportError(error.message, String(error.code), "metadata" in error ? error.metadata as WorkersFailure : undefined);
  }
  throw error;
}
