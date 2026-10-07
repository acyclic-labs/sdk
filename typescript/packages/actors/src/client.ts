import { fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
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
  readonly [K in Operation]: (request: Uint8Array) => Promise<Uint8Array>;
} & {
  readonly transport?: string;
};

/** Bridge factory supplied by the generated WASM or native package. */
export interface ActorsRustBinding {
  connect(endpoint: string, token: string): Promise<ActorsRustClient>;
}

export interface ActorsOptions {
  readonly endpoint: string;
  readonly token: string;
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
  readonly #client: Promise<ActorsRustClient>;
  readonly #observer: AcyclicObserver | undefined;

  constructor(options: ActorsOptions) {
    this.#observer = resolveObserver(options.observer);
    this.#client = (options.binding ?? defaultBinding()).connect(options.endpoint, options.token);
  }

  get transport(): Promise<string | undefined> { return this.#client.then(client => client.transport); }
  createActor(request: Semantic.CreateActorRequest): Promise<Semantic.CreateActorResponse> { return this.#call("createActor", CreateActorRequestSchema, request, CreateActorResponseSchema); }
  updateActor(request: Semantic.UpdateActorRequest): Promise<Semantic.UpdateActorResponse> { return this.#call("updateActor", UpdateActorRequestSchema, request, UpdateActorResponseSchema); }
  inspectActor(request: Semantic.InspectActorRequest): Promise<Semantic.InspectActorResponse> { return this.#call("inspectActor", InspectActorRequestSchema, request, InspectActorResponseSchema); }
  addSubscription(request: Semantic.AddSubscriptionRequest): Promise<Semantic.AddSubscriptionResponse> { return this.#call("addSubscription", AddSubscriptionRequestSchema, request, AddSubscriptionResponseSchema); }
  removeSubscription(request: Semantic.RemoveSubscriptionRequest): Promise<Semantic.RemoveSubscriptionResponse> { return this.#call("removeSubscription", RemoveSubscriptionRequestSchema, request, RemoveSubscriptionResponseSchema); }
  resumeSubscription(request: Semantic.ResumeSubscriptionRequest): Promise<Semantic.ResumeSubscriptionResponse> { return this.#call("resumeSubscription", ResumeSubscriptionRequestSchema, request, ResumeSubscriptionResponseSchema); }
  checkpointActor(request: Semantic.CheckpointActorRequest): Promise<Semantic.CheckpointActorResponse> { return this.#call("checkpointActor", CheckpointActorRequestSchema, request, CheckpointActorResponseSchema); }
  invokeActor(request: Semantic.InvokeActorRequest): Promise<Semantic.InvokeActorResponse> { return this.#call("invokeActor", InvokeActorRequestSchema, request, InvokeActorResponseSchema); }

  async #call<I extends DescMessage, O extends DescMessage, Request, Response>(operation: Operation, input: I, request: Request, output: O): Promise<Response> {
    const client = await this.#client;
    return observed(this.#observer, "actors", operation, async sizes => {
      const encoded = toBinary(input, request as MessageShape<I>);
      if (sizes) sizes.requestBytes = encoded.byteLength;
      const response = await client[operation](encoded);
      if (sizes) sizes.responseBytes = response.byteLength;
      return normalizeSemantic(fromBinary(output, response)) as Response;
    }) as Promise<Response>;
  }
}

/** Buf's message objects are the wire boundary; Rust has already validated the
 * response before this normalization exposes the generated semantic shape. */
function normalizeSemantic(value: unknown): unknown {
  if (value === undefined) return null;
  if (value === null || typeof value !== "object") return value;
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return value.map(normalizeSemantic);
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalizeSemantic(item)]));
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
      const scope = globalThis as { process?: { platform?: string; arch?: string } };
      const platform = scope.process?.platform;
      const arch = scope.process?.arch;
      if (platform === undefined || arch === undefined) {
        throw new ActorsTransportError("native Actors binding requires Node.js", "configuration");
      }
      const module = await import(`@acyclic-labs/actors-${platform}-${arch}`) as unknown as NativeActorsModule;
      const Client = module.NativeActorsClient ?? module.default?.NativeActorsClient;
      if (Client === undefined) {
        throw new ActorsTransportError("native Actors companion did not export NativeActorsClient", "configuration");
      }
      const inner = await Client.connect(endpoint, token);
      const client = Object.fromEntries(Object.keys(HTTP_ROUTES).map(operation => {
        const method = `${operation}Result` as keyof NativeActorsMethods;
        return [operation, (request: Uint8Array) => nativeResult(inner[method](request))];
      }));
      return { ...client, transport: inner.transport } as ActorsRustClient;
    },
  };
}

interface NativeActorsOperationResult {
  readonly value?: Uint8Array | null;
  readonly error?: { readonly code?: string; readonly message?: string } | null;
}

type NativeActorsMethods = {
  readonly [K in Operation as `${K}Result`]: (request: Uint8Array) => Promise<NativeActorsOperationResult>;
};

interface NativeActorsClient extends NativeActorsMethods {
  readonly transport: string;
}

interface NativeActorsModule {
  readonly NativeActorsClient?: { connect(endpoint: string, token: string): Promise<NativeActorsClient> };
  readonly default?: { readonly NativeActorsClient?: { connect(endpoint: string, token: string): Promise<NativeActorsClient> } };
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
      const client = Object.fromEntries(Object.keys(HTTP_ROUTES).map(operation => {
        const method = snakeCase(operation);
        return [operation, (request: Uint8Array) => inner[method](request)];
      }));
      return { ...client, transport: inner.transport } as ActorsRustClient;
    },
  };
}

function snakeCase(value: string): string {
  return value.replace(/[A-Z]/g, character => `_${character.toLowerCase()}`);
}
