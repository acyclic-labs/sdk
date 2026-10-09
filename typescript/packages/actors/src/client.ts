import { isExpectedNativeAbsence } from "./generated/native-absence.js";
import { create, fromBinary, toBinary, type MessageShape } from "@bufbuild/protobuf";
import { observed, resolveObserver, type AcyclicObserver } from "./observe.js";
import {
  canonicalSemantic,
  requireBigIntCodec,
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
import type { ErrorMetadata } from "./generated/ErrorMetadata.js";
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

export type ActorsErrorMetadata = ReadonlySemantic<ErrorMetadata>;

/** Errors raised after Rust has returned a structured operation failure. */
export class ActorsTransportError extends Error {
  constructor(message: string, readonly code = "actors_error", readonly metadata?: ActorsErrorMetadata) { super(message); }
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
    requireBigIntCodec();
      const encoded = toBinary(method.input, create(method.input, toWireSemantic(request) as MessageShape<typeof method.input>));
    return observed(this.#observer, "actors", operation, async sizes => {
      if (sizes) sizes.requestBytes = encoded.byteLength;
      throwIfAborted(options?.signal);
      const client = await this.#connection(options?.signal);
      const response = await abortable(client[operation](encoded, options?.signal), options?.signal);
      if (sizes) sizes.responseBytes = response.byteLength;
      return canonicalSemantic(fromBinary(method.output, response)) as Response;
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

export interface ActorsClient extends ActorsClientMethods {}

/** Compatibility name retained while callers migrate to `ActorsClient`. */
export class HttpActorsClient extends ActorsClient {}

/** Snapshot the readonly semantic view before Buf's mutable wire encoder sees it. */
function toWireSemantic(value: unknown): unknown {
  return structuredClone(value);
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
      nativeNominalBinding(module);
      const Client = module.NativeActorsClient ?? module.default?.NativeActorsClient;
      if (Client === undefined) {
        throw new ActorsTransportError("native Actors companion did not export NativeActorsClient", "configuration");
      }
      const cancellation = nativeCancellation(module, signal);
      let inner: NativeActorsClient;
      try {
        inner = await nativeResult(caCertificate === undefined
          ? Client.connectResult(endpoint, token, cancellation?.handle)
          : Client.connectWithCaResult(endpoint, token, Buffer.from(caCertificate), cancellation?.handle), "client");
      } finally {
        cancellation?.cleanup();
      }
      const client = Object.fromEntries(ACTORS_OPERATION_NAMES.map(operation => {
        const method = `${operation}Result` as keyof NativeActorsMethods;
        return [operation, (request: Uint8Array, signal?: AbortSignal) => {
          const cancellation = nativeCancellation(module, signal);
          return nativeResult(abortable(inner[method](request, cancellation?.handle), signal))
            .finally(() => cancellation?.cleanup());
        }];
      }));
      const transport = typeof inner.transport === "function" ? inner.transport() : inner.transport;
      return { ...client, transport } as ActorsRustClient;
    },
  };
}

interface NativeActorsResult<T> {
  readonly value?: T | null;
  readonly client?: T | null;
  readonly error?: ActorsErrorMetadata | null;
}

type NativeActorsMethods = {
  readonly [K in Operation as `${K}Result`]: (request: Uint8Array, cancellation?: { cancel(): void }) => Promise<NativeActorsResult<Uint8Array>>;
};

interface NativeActorsClient extends NativeActorsMethods {
  readonly transport: string | (() => string);
}

interface NativeActorsModule extends Partial<ActorsNominalBinding> {
  readonly NativeActorsClient?: {
    connectResult(endpoint: string, token: string, cancellation?: { cancel(): void }): Promise<NativeActorsResult<NativeActorsClient>>;
    connectWithCaResult(endpoint: string, token: string, ca: Buffer, cancellation?: { cancel(): void }): Promise<NativeActorsResult<NativeActorsClient>>;
  };
  readonly NativeActorsCancellation?: new () => { cancel(): void };
  readonly default?: Partial<ActorsNominalBinding> & { readonly NativeActorsClient?: {
    connectResult(endpoint: string, token: string, cancellation?: { cancel(): void }): Promise<NativeActorsResult<NativeActorsClient>>;
    connectWithCaResult(endpoint: string, token: string, ca: Buffer, cancellation?: { cancel(): void }): Promise<NativeActorsResult<NativeActorsClient>>;
  }; readonly NativeActorsCancellation?: new () => { cancel(): void } };
}

let nativeModulePromise: Promise<NativeActorsModule | undefined> | undefined;

async function loadNativeModule(): Promise<NativeActorsModule | undefined> {
  // @ts-ignore generated N-API loader is optional in browser/WASM builds
  nativeModulePromise ??= import("../generated/native/binding.cjs")
    .then(module => module as unknown as NativeActorsModule)
    .catch(async error => {
      nativeModulePromise = undefined;
      // The generated loader is the maintained target selector. Its aggregate
      // error is fallback-safe only when every candidate failed because the
      // candidate itself was absent; ABI, export, and dependency failures must
      // remain visible instead of silently selecting WASM.
      if (await isExpectedNativeAbsence(error)) return undefined;
      throw error;
    });
  return nativeModulePromise;
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

async function nativeResult<T>(result: Promise<NativeActorsResult<T>>, key: "value" | "client" = "value"): Promise<T> {
  const outcome = await result;
  const value = outcome[key];
  if (value !== undefined && value !== null) return value;
  const error = outcome.error;
  throw new ActorsTransportError(error?.message ?? "Actors operation failed", error?.code ?? "actors_error", error ?? undefined);
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

let wasmModulePromise: Promise<WasmActorsModule> | undefined;

async function loadWasmModule(): Promise<WasmActorsModule> {
  wasmModulePromise ??= (async () => {
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
  })().catch(error => {
    wasmModulePromise = undefined;
    throw error;
  });
  return wasmModulePromise;
}

async function defaultNominalBinding(): Promise<ActorsNominalBinding> {
  if (isNodeRuntime()) {
    const native = await loadNativeModule();
    if (native !== undefined) return nativeNominalBinding(native);
  }
  return loadWasmModule();
}

function nativeNominalBinding(module: NativeActorsModule): ActorsNominalBinding {
  const candidate = typeof module.default?.ActorId === "function" ? module.default : module;
  if (
    typeof candidate.ActorId === "function"
    && typeof candidate.CodeSha256 === "function"
    && typeof candidate.PositiveU64 === "function"
    && typeof candidate.CurrentHeadMarker === "function"
  ) {
    return candidate as ActorsNominalBinding;
  }
  throw new ActorsTransportError("native Actors companion did not export the Rust nominal constructors", "configuration");
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
