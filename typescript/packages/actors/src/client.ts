import { create, fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { observed, resolveObserver, type AcyclicObserver } from "./observe.js";
import {
  ACTORS_OPERATION_NAMES,
  installActorsMethods,
  type ActorsClientMethods,
  type ActorsMethod,
  type ActorsOperation,
} from "./generated/actors-service.js";
import {
  configureActorsNominalBinding,
  ActorId,
  CodeSha256,
  PositiveU64,
  CurrentHeadMarker,
  type ActorsNominalBinding,
} from "./generated/nominal.js";
import type { ReadonlyBytes, ReadonlySemantic } from "./generated/readonly.js";
export type { ReadonlyBytes, ReadonlySemantic } from "./generated/readonly.js";
export { ActorId, CodeSha256, PositiveU64, CurrentHeadMarker } from "./generated/nominal.js";

/** Rust operation surface implemented by either the N-API or WASM bridge. */
export type Operation = ActorsOperation;

export type ActorsRustClient = {
  readonly [K in Operation]: (request: Uint8Array, signal?: AbortSignal) => Promise<Uint8Array>;
} & {
  readonly transport?: string;
};

/** Bridge factory supplied by the generated WASM or native package. */
export interface ActorsRustBinding {
  connect(endpoint: string, token: string, signal?: AbortSignal, caCertificate?: Uint8Array): Promise<ActorsRustClient>;
}

export interface ActorsCallOptions {
  readonly signal?: AbortSignal;
}

export interface ActorsOptions {
  readonly endpoint: string;
  readonly token: string;
  /** Optional native trust root for private or test endpoints. */
  readonly caCertificate?: Uint8Array;
  /** Native callers may inject the built N-API binding. */
  readonly binding?: ActorsRustBinding;
  /** Optional secret-free per-operation observer. */
  readonly observer?: AcyclicObserver;
}

/** Errors raised after Rust has returned a structured operation failure. */
export class ActorsTransportError extends Error {
  constructor(message: string, readonly code = "actors_error") { super(message); }
}

/** Typed Actors client backed by one Rust implementation on every platform. */
export class ActorsClient {
  readonly #endpoint: string;
  readonly #token: string;
  readonly #caCertificate: Uint8Array | undefined;
  readonly #binding: ActorsRustBinding;
  #client: Promise<ActorsRustClient> | undefined;
  #connectAbort: AbortController | undefined;
  #connectWaiters = 0;
  readonly #observer: AcyclicObserver | undefined;

  constructor(options: ActorsOptions) {
    this.#observer = resolveObserver(options.observer);
    this.#endpoint = options.endpoint;
    this.#token = options.token;
    this.#caCertificate = options.caCertificate;
    this.#binding = options.binding ?? defaultBinding();
    installActorsMethods(this, (method, request, callOptions) => this.#call(method, request, callOptions));
  }

  get transport(): Promise<string | undefined> { return this.#connection().then(client => client.transport); }

  async #call<Request, Response>(method: ActorsMethod, request: Request, options?: ActorsCallOptions): Promise<Response> {
    const operation = method.localName as Operation;
    const encoded = toBinary(method.input, create(method.input, toWireSemantic(request) as MessageShape<typeof method.input>));
    throwIfAborted(options?.signal);
    const client = await this.#connection(options?.signal);
    return observed(this.#observer, "actors", operation, async sizes => {
      if (sizes) sizes.requestBytes = encoded.byteLength;
      const response = await abortable(client[operation](encoded, options?.signal), options?.signal);
      if (sizes) sizes.responseBytes = response.byteLength;
      return normalizeSemantic(fromBinary(method.output, response), method.output) as Response;
    }) as Promise<Response>;
  }

  #connection(signal?: AbortSignal): Promise<ActorsRustClient> {
    throwIfAborted(signal);
    if (this.#client === undefined) {
      const controller = new AbortController();
      let pending!: Promise<ActorsRustClient>;
      pending = this.#binding.connect(this.#endpoint, this.#token, controller.signal, this.#caCertificate)
        .then(client => {
          if (this.#client === pending) this.#connectAbort = undefined;
          return client;
        })
        .catch(error => {
          if (this.#client === pending) {
            this.#client = undefined;
            this.#connectAbort = undefined;
          }
          throw error;
        });
      this.#connectAbort = controller;
      this.#client = pending;
    }
    const pending = this.#client;
    this.#connectWaiters += 1;
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      this.#connectWaiters -= 1;
      if (this.#connectWaiters === 0 && this.#client === pending && this.#connectAbort !== undefined) {
        // Evict before aborting. A bridge may reject asynchronously after the
        // abort; a new caller must be able to start a fresh connection during
        // that interval, and the old rejection must not clear the replacement.
        const controller = this.#connectAbort;
        this.#client = undefined;
        this.#connectAbort = undefined;
        controller.abort();
      }
    };
    return abortable(pending, signal).finally(release);
  }
}

/** Buf's message objects are the wire boundary; Rust has already validated the
 * response before this exposes the generated semantic shape. Presence remains
 * `undefined` when the Rust declaration marks an optional field as absent. */
function normalizeSemantic(value: unknown, schema?: DescMessage): unknown {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== "object") return value;
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return value.map(item => normalizeSemantic(item, schema));
  const fields = schema?.fields ?? [];
  const byName = new Map(fields.map(field => [field.localName, field]));
  const result = Object.fromEntries(Object.entries(value).filter(([key]) => key !== "$typeName").map(([key, item]) => {
    const field = byName.get(key);
    return [key, normalizeSemantic(item, field?.message)];
  }));
  // Buf omits absent message fields. The Rust Option<T> declarations expose
  // those values as null, while optional scalar presence remains undefined.
  for (const field of fields) {
    if (field.fieldKind === "message" && !(field.localName in result)) result[field.localName] = null;
  }
  return result;
}

export interface ActorsClient extends ActorsClientMethods {}

/** Compatibility name retained while callers migrate to `ActorsClient`. */
export class HttpActorsClient extends ActorsClient {}

/**
 * Clone the public readonly semantic view at the wire boundary. Buf's
 * generated encoder intentionally accepts mutable Uint8Array/Array shapes;
 * cloning here keeps that implementation detail out of the public contract
 * and prevents callers from mutating an input while a Rust connection starts.
 */
function toWireSemantic(value: unknown): unknown {
  if (value instanceof Uint8Array) return new Uint8Array(value);
  if (Array.isArray(value)) return value.map(item => toWireSemantic(item));
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, toWireSemantic(item)]));
}

function defaultBinding(): ActorsRustBinding {
  const scope = globalThis as { process?: { versions?: { node?: string } } };
  return scope.process?.versions?.node === undefined ? wasmBinding() : nativeBinding();
}

function nativeBinding(): ActorsRustBinding {
  return {
    async connect(endpoint, token, signal, caCertificate) {
      // The staged metadata identifies the exact native artifacts present in
      // this package. Select WASM before evaluating the generated loader when
      // the package was installed on another supported platform; the loader's
      // generic missing-binding error otherwise hides its requested paths.
      const module = await loadNativeModule();
      if (module === undefined) {
        return wasmBinding().connect(endpoint, token, signal, caCertificate);
      }
      const Client = module.NativeActorsClient ?? module.default?.NativeActorsClient;
      if (Client === undefined) {
        throw new ActorsTransportError("native Actors companion did not export NativeActorsClient", "configuration");
      }
      const cancellation = nativeCancellation(module, signal);
      let inner: NativeActorsClient;
      try {
        inner = caCertificate === undefined
          ? await Client.connect(endpoint, token, cancellation?.handle)
          : await Client.connectWithCa(endpoint, token, Buffer.from(caCertificate), cancellation?.handle);
      } finally {
        cancellation?.cleanup();
      }
      const client = Object.fromEntries(ACTORS_OPERATION_NAMES.map(operation => {
        const method = `${operation}Result` as keyof NativeActorsMethods;
        return [operation, (request: Uint8Array, signal?: AbortSignal) => {
          const cancellation = nativeCancellation(module, signal);
          return nativeResult(nativeResultWithAbort(inner[method](request, cancellation?.handle), signal))
            .finally(() => cancellation?.cleanup());
        }];
      }));
      const transport = typeof inner.transport === "function" ? inner.transport() : inner.transport;
      return { ...client, transport } as ActorsRustClient;
    },
  };
}

interface NativeActorsOperationResult {
  readonly value?: Uint8Array | null;
  readonly error?: { readonly code?: string; readonly message?: string } | null;
}

type NativeActorsMethods = {
  readonly [K in Operation as `${K}Result`]: (request: Uint8Array, cancellation?: { cancel(): void }) => Promise<NativeActorsOperationResult>;
};

interface NativeActorsClient extends NativeActorsMethods {
  readonly transport: string | (() => string);
}

interface NativeActorsModule extends Partial<ActorsNominalBinding> {
  readonly NativeActorsClient?: {
    connect(endpoint: string, token: string, cancellation?: { cancel(): void }): Promise<NativeActorsClient>;
    connectWithCa(endpoint: string, token: string, ca: Buffer, cancellation?: { cancel(): void }): Promise<NativeActorsClient>;
  };
  readonly NativeActorsCancellation?: new () => { cancel(): void };
  readonly default?: Partial<ActorsNominalBinding> & { readonly NativeActorsClient?: {
    connect(endpoint: string, token: string, cancellation?: { cancel(): void }): Promise<NativeActorsClient>;
    connectWithCa(endpoint: string, token: string, ca: Buffer, cancellation?: { cancel(): void }): Promise<NativeActorsClient>;
  }; readonly NativeActorsCancellation?: new () => { cancel(): void } };
}

interface NativeTargetMetadata {
  readonly artifacts?: readonly { readonly path?: string }[];
}

async function hasNativeArtifactForRuntime(metadata: NativeTargetMetadata): Promise<boolean> {
  const artifacts = metadata.artifacts ?? [];
  const names = artifacts.map(artifact => artifact.path?.split(/[\\/]/).pop() ?? "");
  return (await nativeRuntimeArtifactNames()).some(name => names.includes(name));
}

type NativeMetadataModule = { readonly default?: NativeTargetMetadata } & NativeTargetMetadata;

async function loadNativeModule(): Promise<NativeActorsModule | undefined> {
  let metadata: NativeTargetMetadata;
  try {
    // @ts-ignore generated native metadata is intentionally untracked
    const metadataModule = await import("../generated/native/native-targets.json", { with: { type: "json" } }) as unknown as NativeMetadataModule;
    metadata = metadataModule.default ?? metadataModule;
  } catch (error) {
    if (isMissingNativeMetadata(error)) return loadNativeCompanion();
    throw error;
  }
  if (!(await hasNativeArtifactForRuntime(metadata))) return loadNativeCompanion();
  try {
    // @ts-ignore generated N-API loader is optional in browser/WASM builds
    return await import("../generated/native/binding.cjs") as unknown as NativeActorsModule;
  } catch (error) {
    if (isMissingNativeArtifact(error)) return undefined;
    throw error;
  }
}

/** Resolve the maintained optional N-API package for the current exact target. */
async function loadNativeCompanion(): Promise<NativeActorsModule | undefined> {
  const packageName = await nativeCompanionPackageName();
  if (packageName === undefined) return undefined;
  try {
    // Native addons are CommonJS `.node` entrypoints.  Node's ESM loader
    // rejects a direct dynamic import of that extension, while createRequire
    // delegates to the maintained N-API loader and preserves the same package
    // resolution/error identity for optional companions.
    const { createRequire } = await import("node:module");
    return createRequire(import.meta.url)(packageName) as NativeActorsModule;
  } catch (error) {
    if (isMissingNativePackage(error, packageName)) return undefined;
    throw error;
  }
}

async function nativeCompanionPackageName(): Promise<string | undefined> {
  const names = await nativeRuntimeArtifactNames();
  const artifact = names[0];
  if (artifact === undefined) return undefined;
  const target = artifact.slice("index.".length, -".node".length);
  return `@acyclic-labs/actors-${target}`;
}

/**
 * Keep the preflight selector aligned with the maintained NAPI-RS loader that
 * is staged beside this metadata. In particular, Linux's GNU and musl names
 * are distinct artifacts; a prefix check would select a binary for the wrong
 * libc and turn a clean WASM fallback into a native load failure.
 */
async function nativeRuntimeArtifactNames(): Promise<readonly string[]> {
  const arch = process.arch === "x64" ? "x64" : process.arch === "arm64" ? "arm64" : process.arch;
  if (process.platform === "linux") {
    const libc = (await isMuslRuntime()) ? "musl" : "gnu";
    const suffix = arch === "arm" ? (libc === "musl" ? "musleabihf" : "gnueabihf") : libc;
    return [`index.linux-${arch}-${suffix}.node`];
  }
  if (process.platform === "win32") {
    const config = process.config as { variables?: { shlib_suffix?: string; node_target_type?: string } } | undefined;
    const flavor = config?.variables?.shlib_suffix === "dll.a" || config?.variables?.node_target_type === "shared_library" ? "gnu" : "msvc";
    return [`index.win32-${arch}-${flavor}.node`];
  }
  if (process.platform === "darwin") return ["index.darwin-universal.node", `index.darwin-${arch}.node`];
  return [`index.${process.platform}-${arch}.node`];
}

async function isMuslRuntime(): Promise<boolean> {
  if (process.platform !== "linux") return false;
  try {
    const fs = await import("node:fs");
    if (fs.readFileSync("/usr/bin/ldd", "utf8").includes("musl")) return true;
  } catch { /* use the maintained report/ldd probes below */ }
  const reportProcess = process as typeof process & { report?: { getReport?: () => { header?: { glibcVersionRuntime?: string }; sharedObjects?: readonly string[] } } };
  const report = reportProcess.report?.getReport?.() as { header?: { glibcVersionRuntime?: string }; sharedObjects?: readonly string[] } | undefined;
  if (report?.header?.glibcVersionRuntime) return false;
  if (report?.sharedObjects?.some(path => /(?:^|[\\/])(libc\.musl-|ld-musl-)/.test(path))) return true;
  try {
    const childProcess = await import("node:child_process");
    return childProcess.execFileSync("ldd", ["--version"], { encoding: "utf8" }).includes("musl");
  } catch {
    return false;
  }
}

function nativeCancellation(module: NativeActorsModule, signal?: AbortSignal): { handle: { cancel(): void }; cleanup: () => void } | undefined {
  if (signal === undefined) return undefined;
  const Cancellation = module.NativeActorsCancellation ?? module.default?.NativeActorsCancellation;
  if (Cancellation === undefined) throw new ActorsTransportError("native Actors companion does not export cancellation", "configuration");
  const cancellation = new Cancellation();
  const onAbort = () => cancellation.cancel();
  if (signal.aborted) onAbort();
  else signal.addEventListener("abort", onAbort, { once: true });
  return { handle: cancellation, cleanup: () => signal.removeEventListener("abort", onAbort) };
}

async function nativeResult(result: Promise<NativeActorsOperationResult>): Promise<Uint8Array> {
  const outcome = await result;
  if (outcome.value !== undefined && outcome.value !== null) return outcome.value;
  const error = outcome.error;
  throw new ActorsTransportError(error?.message ?? "Actors operation failed", error?.code ?? "actors_error");
}

function wasmBinding(): ActorsRustBinding {
  return {
    async connect(endpoint, token, signal) {
      const module = await loadWasmModule();
      const inner = await module.ActorsClient.connect(endpoint, token, signal);
      const wasm = inner as unknown as WasmActorsClient;
      const client = Object.fromEntries(ACTORS_OPERATION_NAMES.map(operation => {
        const method = snakeCase(operation);
        return [operation, async (request: Uint8Array, signal?: AbortSignal) => {
          const invoke = (wasm as unknown as Record<string, unknown>)[method];
          if (typeof invoke !== "function") throw new ActorsTransportError(`WASM bridge is missing ${method}`, "configuration");
          throwIfAborted(signal);
          return abortable(invoke.call(wasm, request, signal) as Promise<Uint8Array>, signal);
        }];
      }));
      return { ...client, transport: wasm.transport } as ActorsRustClient;
    },
  };
}

interface WasmActorsModule extends ActorsNominalBinding {
  readonly default: (input?: unknown) => Promise<unknown>;
  readonly ActorsClient: { connect(endpoint: string, token: string, signal?: unknown): Promise<unknown> };
}

async function loadWasmModule(): Promise<WasmActorsModule> {
  // @ts-ignore generated Rust WASM module is intentionally untracked
  const module = await import("../generated/wasm/acyclic_actors_wasm.js") as unknown as WasmActorsModule;
  if (isNodeRuntime()) {
    const fs = await import("node:fs/promises");
    const bytes = await fs.readFile(new URL("../generated/wasm/acyclic_actors_wasm_bg.wasm", import.meta.url));
    await module.default(bytes);
  } else {
    await module.default();
  }
  return module;
}

async function defaultNominalBinding(): Promise<ActorsNominalBinding> {
  if (isNodeRuntime()) {
    const native = await loadNativeModule();
    const candidate = native?.default ?? native;
    if (candidate?.ActorId && candidate.CodeSha256 && candidate.PositiveU64 && candidate.CurrentHeadMarker) {
      return candidate as ActorsNominalBinding;
    }
  }
  return loadWasmModule();
}

configureActorsNominalBinding(defaultNominalBinding);

function isNodeRuntime(): boolean {
  const scope = globalThis as { process?: { versions?: { node?: string } } };
  return scope.process?.versions?.node !== undefined;
}

interface WasmActorsClient {
  readonly transport: string;
}

function snakeCase(value: string): string {
  return value.replace(/[A-Z]/g, character => `_${character.toLowerCase()}`);
}

function isMissingNativeArtifact(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const code = "code" in error && typeof error.code === "string" ? error.code : undefined;
  if (code !== "ERR_MODULE_NOT_FOUND" && code !== "MODULE_NOT_FOUND") return false;
  const message = error instanceof Error ? error.message : String(error);
  const firstLine = message.split(/\r?\n/, 1)[0] ?? message;
  const requested = firstLine.match(/^Cannot find module ['"]([^'"]+)['"]/i)?.[1];
  if (requested === undefined) return false;
  // Only the package's own generated loader is optional. Matching a path
  // suffix would incorrectly turn a broken transitive dependency into a
  // silent WASM fallback.
  let expected = new URL("../generated/native/binding.cjs", import.meta.url).pathname;
  try { expected = decodeURIComponent(expected); } catch { /* keep the URL path */ }
  return normalizeModulePath(requested) === normalizeModulePath(expected);
}

function isMissingNativeMetadata(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const code = "code" in error && typeof error.code === "string" ? error.code : undefined;
  if (code !== "ERR_MODULE_NOT_FOUND" && code !== "MODULE_NOT_FOUND") return false;
  const message = error instanceof Error ? error.message : String(error);
  const firstLine = message.split(/\r?\n/, 1)[0] ?? message;
  const requested = firstLine.match(/^Cannot find module ['"]([^'"]+)['"]/i)?.[1];
  if (requested === undefined) return false;
  let expected = new URL("../generated/native/native-targets.json", import.meta.url).pathname;
  try { expected = decodeURIComponent(expected); } catch { /* keep the URL path */ }
  return normalizeModulePath(requested) === normalizeModulePath(expected);
}

function isMissingNativePackage(error: unknown, packageName: string): boolean {
  if (typeof error !== "object" || error === null) return false;
  const code = "code" in error && typeof error.code === "string" ? error.code : undefined;
  if (code !== "ERR_MODULE_NOT_FOUND" && code !== "MODULE_NOT_FOUND") return false;
  const message = error instanceof Error ? error.message : String(error);
  const firstLine = message.split(/\r?\n/, 1)[0] ?? message;
  const requested = firstLine.match(/^Cannot find package ['"]([^'"]+)['"]/i)?.[1];
  return requested === packageName;
}

function normalizeModulePath(path: string): string {
  let value = path;
  if (value.startsWith("file:")) {
    try { value = new URL(value).pathname; } catch { return ""; }
  }
  try { value = decodeURIComponent(value); } catch { /* keep the original path */ }
  const normalized = value
    .replace(/^[/\\]+(?=[A-Za-z]:)/, "")
    .replaceAll("\\", "/")
    .replace(/\/+/g, "/")
    .replace(/\/$/, "");
  // Windows module paths are case-insensitive; POSIX paths are not. Keeping
  // POSIX case prevents an unrelated transitive module from being treated as
  // this package's optional loader.
  return /^(?:[A-Za-z]:\/|\/\/)/.test(normalized) ? normalized.toLowerCase() : normalized;
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new ActorsTransportError("Actors operation cancelled", "cancelled");
}

async function abortable<T>(operation: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return operation;
  throwIfAborted(signal);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new ActorsTransportError("Actors operation cancelled", "cancelled"));
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      value => { signal.removeEventListener("abort", onAbort); resolve(value); },
      error => { signal.removeEventListener("abort", onAbort); reject(error); },
    );
  });
}

async function nativeResultWithAbort(result: Promise<NativeActorsOperationResult>, signal?: AbortSignal): Promise<NativeActorsOperationResult> {
  return abortable(result, signal);
}
