import type { WorkersClientOptions, WorkersCallOptions, WorkersFailure, WorkersTransportPreference } from "./generated/semantic/workers/readonly.js";
import { WorkersTransportError } from "./client.js";
import { WORKERS_BINDING_VERSION } from "./generated/workers-binding.js";
import type * as NativeWorkers from "../generated/native/binding.js";
import type * as WasmWorkers from "../generated/wasm/acyclic_workers_wasm.js";

export interface RustClient {
  readonly transport: string;
  call(method: string, bytes: Uint8Array, options: WorkersCallOptions, signal?: AbortSignal): Promise<Uint8Array>;
}
interface Cancellation { cancel(): void; free?(): void }
interface RustBinding { connect(config: WorkersClientOptions, signal?: AbortSignal): Promise<RustClient> }
let native: Promise<RustBinding | undefined> | undefined;
let wasm: Promise<RustBinding> | undefined;
let nativeModule: Promise<typeof NativeWorkers | undefined> | undefined;
let wasmModule: Promise<typeof WasmWorkers> | undefined;
/** Initialize the canonical binding from a module compiled by the deployment bundler. */
export async function initializeWorkersWasm(compiledModule: WebAssembly.Module): Promise<void> {
  await loadWasm(compiledModule);
}
/** Platform selection without fabricating a process shape in browser runtimes. */
export function isNodeRuntime(): boolean {
  if (!("process" in globalThis)) return false;
  const host = globalThis.process;
  if (host === null || typeof host !== "object" || !("versions" in host)) return false;
  const versions = host.versions;
  return versions !== null && typeof versions === "object" && "node" in versions && typeof versions.node === "string";
}
export async function loadBinding(preference?: WorkersTransportPreference): Promise<RustBinding> {
  const isNode = isNodeRuntime();
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
function cancellation<T extends Cancellation>(Cancel: new () => T, signal?: AbortSignal) {
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
    const module = await loadWorkersNativeModule();
    if (module === undefined) return undefined;
    // node:buffer does not exist in browser or Workerd runtimes.
    const { Buffer } = await import("node:buffer");
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
  })().catch(error => { native = undefined; throw error; });
  return native;
}
function loadWasm(compiledModule?: WebAssembly.Module): Promise<RustBinding> {
  wasm ??= (async () => {
    const module = await loadWorkersWasmModule(compiledModule);
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

/** Share the actual canonical addon with account custody and the RPC client. */
export function loadWorkersNativeModule(): Promise<typeof NativeWorkers | undefined> {
  nativeModule ??= (async () => {
    // Platform-selected native companions are unavailable in browser runtimes.
    const nativeUrl = new URL("../generated/native/binding.cjs", import.meta.url);
    const imported = await import(nativeUrl.href);
    const module: typeof NativeWorkers = typeof imported.WorkersClient === "function" ? imported : imported.default;
    checkedVersion(module?.WorkersClient);
    if (typeof module.WorkersClient.connectResult !== "function" || typeof module.WorkersCancellation !== "function") {
      throw new WorkersTransportError("Workers native ABI exports missing", "configuration");
    }
    return module;
  })().catch(async error => {
    nativeModule = undefined;
    // Native absence checks depend on Node filesystem URLs, absent in browsers.
    const { isExpectedNativeAbsence } = await import("./generated/native-absence.js");
    if (await isExpectedNativeAbsence(error)) return undefined;
    throw error;
  });
  return nativeModule;
}

/** Share one initialized Rust module rather than creating a signing codec copy. */
export function loadWorkersWasmModule(compiledModule?: WebAssembly.Module): Promise<typeof WasmWorkers> {
  wasmModule ??= (async () => {
    // Generated WASM may be absent from native-only installations.
    const module = await import("../generated/wasm/acyclic_workers_wasm.js");
    const isNode = isNodeRuntime();
    if (compiledModule !== undefined) await module.default({ module_or_path: compiledModule });
    else if (isNode) {
      // Node filesystem loading is unavailable in browser and Workerd bundles.
      const { readFile } = await import("node:fs/promises");
      await module.default({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_workers_wasm_bg.wasm", import.meta.url)) });
    } else await module.default();
    checkedVersion(module.WorkersClient);
    return module;
  })().catch(error => { wasmModule = undefined; throw error; });
  return wasmModule;
}

function wasmError(error: unknown): never {
  if (error instanceof Error && "code" in error) {
    throw new WorkersTransportError(error.message, String(error.code), "metadata" in error ? error.metadata as WorkersFailure : undefined);
  }
  throw error;
}
