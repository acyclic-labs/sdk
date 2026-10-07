import { create, fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import {
  AddSubscriptionRequestSchema, AddSubscriptionResponseSchema,
  CheckpointActorRequestSchema, CheckpointActorResponseSchema,
  CreateActorRequestSchema, CreateActorResponseSchema,
  InspectActorRequestSchema, InspectActorResponseSchema,
  InvokeActorRequestSchema, InvokeActorResponseSchema,
  RemoveSubscriptionRequestSchema, RemoveSubscriptionResponseSchema,
  ResumeSubscriptionRequestSchema, ResumeSubscriptionResponseSchema,
  UpdateActorRequestSchema, UpdateActorResponseSchema,
} from "../generated/proto/actors/v1/actors_pb.js";
import { observed, resolveObserver, type AcyclicObserver } from "./observe.js";
import { HTTP_ROUTES } from "./routes.js";
import type * as Semantic from "./generated/semantic/actors/index.js";

/** Rust operation surface implemented by either the N-API or WASM bridge. */
export type Operation = keyof typeof HTTP_ROUTES;

export type ActorsRustClient = {
  readonly [K in Operation]: (request: Uint8Array, signal?: AbortSignal) => Promise<Uint8Array>;
} & {
  readonly transport?: string;
};

/** Bridge factory supplied by the generated WASM or native package. */
export interface ActorsRustBinding {
  connect(endpoint: string, token: string): Promise<ActorsRustClient>;
}

export interface ActorsCallOptions {
  readonly signal?: AbortSignal;
}

export interface ActorsOptions {
  readonly endpoint: string;
  readonly token: string;
  /** Native callers may inject the built N-API binding. */
  readonly binding?: ActorsRustBinding;
  /** Optional secret-free per-operation observer. */
  readonly observer?: AcyclicObserver;
}

/**
 * The Rust declarations are generated as structural TypeScript types. Keep
 * the public client view immutable, including nested records and collections,
 * while retaining the brands and presence unions emitted by ts-rs.
 */
export type ReadonlySemantic<T> = T extends Uint8Array
  ? T
  : T extends (...args: never[]) => unknown
    ? T
    : T extends readonly (infer Item)[]
      ? readonly ReadonlySemantic<Item>[]
      : T extends object
        ? { readonly [K in keyof T]: ReadonlySemantic<T[K]> }
        : T;

/** Errors raised after Rust has returned a structured operation failure. */
export class ActorsTransportError extends Error {
  constructor(message: string, readonly code = "actors_error") { super(message); }
}

/** Typed Actors client backed by one Rust implementation on every platform. */
export class ActorsClient {
  readonly #client: Promise<ActorsRustClient>;
  readonly #observer: AcyclicObserver | undefined;

  constructor(options: ActorsOptions) {
    this.#observer = resolveObserver(options.observer);
    this.#client = (options.binding ?? defaultBinding()).connect(options.endpoint, options.token);
  }

  get transport(): Promise<string | undefined> { return this.#client.then(client => client.transport); }
  createActor(request: ReadonlySemantic<Semantic.CreateActorRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.CreateActorResponse>> { return this.#call("createActor", CreateActorRequestSchema, request, CreateActorResponseSchema, options); }
  updateActor(request: ReadonlySemantic<Semantic.UpdateActorRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.UpdateActorResponse>> { return this.#call("updateActor", UpdateActorRequestSchema, request, UpdateActorResponseSchema, options); }
  inspectActor(request: ReadonlySemantic<Semantic.InspectActorRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.InspectActorResponse>> { return this.#call("inspectActor", InspectActorRequestSchema, request, InspectActorResponseSchema, options); }
  addSubscription(request: ReadonlySemantic<Semantic.AddSubscriptionRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.AddSubscriptionResponse>> { return this.#call("addSubscription", AddSubscriptionRequestSchema, request, AddSubscriptionResponseSchema, options); }
  removeSubscription(request: ReadonlySemantic<Semantic.RemoveSubscriptionRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.RemoveSubscriptionResponse>> { return this.#call("removeSubscription", RemoveSubscriptionRequestSchema, request, RemoveSubscriptionResponseSchema, options); }
  resumeSubscription(request: ReadonlySemantic<Semantic.ResumeSubscriptionRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.ResumeSubscriptionResponse>> { return this.#call("resumeSubscription", ResumeSubscriptionRequestSchema, request, ResumeSubscriptionResponseSchema, options); }
  checkpointActor(request: ReadonlySemantic<Semantic.CheckpointActorRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.CheckpointActorResponse>> { return this.#call("checkpointActor", CheckpointActorRequestSchema, request, CheckpointActorResponseSchema, options); }
  invokeActor(request: ReadonlySemantic<Semantic.InvokeActorRequest>, options?: ActorsCallOptions): Promise<ReadonlySemantic<Semantic.InvokeActorResponse>> { return this.#call("invokeActor", InvokeActorRequestSchema, request, InvokeActorResponseSchema, options); }

  async #call<I extends DescMessage, O extends DescMessage, Request, Response>(operation: Operation, input: I, request: Request, output: O, options?: ActorsCallOptions): Promise<Response> {
    const encoded = toBinary(input, create(input, request as MessageShape<I>));
    throwIfAborted(options?.signal);
    const client = await this.#client;
    return observed(this.#observer, "actors", operation, async sizes => {
      if (sizes) sizes.requestBytes = encoded.byteLength;
      const response = await abortable(client[operation](encoded, options?.signal), options?.signal);
      if (sizes) sizes.responseBytes = response.byteLength;
      return normalizeSemantic(fromBinary(output, response), output) as Response;
    }) as Promise<Response>;
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

/** Compatibility name retained while callers migrate to `ActorsClient`. */
export class HttpActorsClient extends ActorsClient {}

function defaultBinding(): ActorsRustBinding {
  const scope = globalThis as { process?: { versions?: { node?: string } } };
  return scope.process?.versions?.node === undefined ? wasmBinding() : nativeBinding();
}

function nativeBinding(): ActorsRustBinding {
  return {
    async connect(endpoint, token) {
      let module: NativeActorsModule;
      try {
        // The native build script stages this generated loader and its exact
        // platform artifact into the published package. Keep the import
        // dynamic so browser consumers never resolve Node-only code.
        // @ts-ignore generated N-API loader is optional in browser/WASM builds
        module = await import("../generated/native/binding.cjs") as unknown as NativeActorsModule;
      } catch (error) {
        // The browser-compatible WASM bridge is shipped with this package. A
        // native artifact is preferred on Node, but a package install remains
        // usable when its optional platform artifact is not present.
        if (isMissingNativeArtifact(error)) {
          return wasmBinding().connect(endpoint, token);
        }
        throw error;
      }
      const Client = module.NativeActorsClient ?? module.default?.NativeActorsClient;
      if (Client === undefined) {
        throw new ActorsTransportError("native Actors companion did not export NativeActorsClient", "configuration");
      }
      const inner = await Client.connect(endpoint, token);
      const client = Object.fromEntries(Object.keys(HTTP_ROUTES).map(operation => {
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

interface NativeActorsModule {
  readonly NativeActorsClient?: { connect(endpoint: string, token: string): Promise<NativeActorsClient> };
  readonly NativeActorsCancellation?: new () => { cancel(): void };
  readonly default?: { readonly NativeActorsClient?: { connect(endpoint: string, token: string): Promise<NativeActorsClient> }; readonly NativeActorsCancellation?: new () => { cancel(): void } };
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
    async connect(endpoint, token) {
      // The generated module is produced by `build:wasm` immediately before tsc.
      // @ts-ignore generated Rust WASM module is intentionally untracked
      const module = await import("../generated/wasm/acyclic_actors_wasm.js");
      await module.default();
      const inner = await module.ActorsClient.connect(endpoint, token);
      const wasm = inner as unknown as WasmActorsClient;
      const client = Object.fromEntries(Object.keys(HTTP_ROUTES).map(operation => {
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
  if (/^Cannot find module ['"][^'"]*generated[\\/]native[\\/]binding\.cjs['"]/i.test(firstLine)) return true;
  if (/^Cannot find package ['"]@acyclic-labs[\\/]actors-(?:win32|linux|darwin|freebsd)-[^'"]+['"]/i.test(firstLine)) return true;
  return false;
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
