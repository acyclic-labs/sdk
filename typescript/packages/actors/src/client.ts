import { fromBinary, toBinary, type Message } from "@bufbuild/protobuf";
import type {
  AddSubscriptionRequest, AddSubscriptionResponse,
  CheckpointActorRequest, CheckpointActorResponse,
  CreateActorRequest, CreateActorResponse,
  InspectActorRequest, InspectActorResponse,
  InvokeActorRequest, InvokeActorResponse,
  RemoveSubscriptionRequest, RemoveSubscriptionResponse,
  ResumeSubscriptionRequest, ResumeSubscriptionResponse,
  UpdateActorRequest, UpdateActorResponse,
} from "../generated/proto/actors/v1/actors_pb.js";
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

/** Rust operation surface implemented by either the N-API or WASM bridge. */
export interface ActorsRustClient {
  readonly transport?: string;
  createActor(request: Uint8Array): Promise<Uint8Array>;
  updateActor(request: Uint8Array): Promise<Uint8Array>;
  inspectActor(request: Uint8Array): Promise<Uint8Array>;
  addSubscription(request: Uint8Array): Promise<Uint8Array>;
  removeSubscription(request: Uint8Array): Promise<Uint8Array>;
  resumeSubscription(request: Uint8Array): Promise<Uint8Array>;
  checkpointActor(request: Uint8Array): Promise<Uint8Array>;
  invokeActor(request: Uint8Array): Promise<Uint8Array>;
}

/** Bridge factory supplied by the generated WASM or native package. */
export interface ActorsRustBinding {
  connect(endpoint: string, token: string): Promise<ActorsRustClient>;
}

export interface ActorsOptions {
  readonly endpoint: string;
  readonly token: string;
  /** Native callers may inject the built N-API binding. */
  readonly binding?: ActorsRustBinding;
}

/** Errors raised after Rust has returned a structured operation failure. */
export class ActorsTransportError extends Error {
  constructor(message: string, readonly code = "actors_error") { super(message); }
}

/** Typed Actors client backed by one Rust implementation on every platform. */
export class ActorsClient {
  readonly #client: Promise<ActorsRustClient>;

  constructor(options: ActorsOptions) {
    this.#client = (options.binding ?? wasmBinding()).connect(options.endpoint, options.token);
  }

  get transport(): Promise<string | undefined> { return this.#client.then(client => client.transport); }
  createActor(request: CreateActorRequest): Promise<CreateActorResponse> { return this.#call("createActor", CreateActorRequestSchema, request, CreateActorResponseSchema); }
  updateActor(request: UpdateActorRequest): Promise<UpdateActorResponse> { return this.#call("updateActor", UpdateActorRequestSchema, request, UpdateActorResponseSchema); }
  inspectActor(request: InspectActorRequest): Promise<InspectActorResponse> { return this.#call("inspectActor", InspectActorRequestSchema, request, InspectActorResponseSchema); }
  addSubscription(request: AddSubscriptionRequest): Promise<AddSubscriptionResponse> { return this.#call("addSubscription", AddSubscriptionRequestSchema, request, AddSubscriptionResponseSchema); }
  removeSubscription(request: RemoveSubscriptionRequest): Promise<RemoveSubscriptionResponse> { return this.#call("removeSubscription", RemoveSubscriptionRequestSchema, request, RemoveSubscriptionResponseSchema); }
  resumeSubscription(request: ResumeSubscriptionRequest): Promise<ResumeSubscriptionResponse> { return this.#call("resumeSubscription", ResumeSubscriptionRequestSchema, request, ResumeSubscriptionResponseSchema); }
  checkpointActor(request: CheckpointActorRequest): Promise<CheckpointActorResponse> { return this.#call("checkpointActor", CheckpointActorRequestSchema, request, CheckpointActorResponseSchema); }
  invokeActor(request: InvokeActorRequest): Promise<InvokeActorResponse> { return this.#call("invokeActor", InvokeActorRequestSchema, request, InvokeActorResponseSchema); }

  async #call(operation: Operation, input: { readonly typeName: string }, request: Message, output: { readonly typeName: string }): Promise<any> {
    const client = await this.#client;
    const response = await client[operation](toBinary(input, request));
    return fromBinary(output, response);
  }
}

type Operation = "createActor" | "updateActor" | "inspectActor" | "addSubscription" | "removeSubscription" | "resumeSubscription" | "checkpointActor" | "invokeActor";

/** Compatibility name retained while callers migrate to `ActorsClient`. */
export class HttpActorsClient extends ActorsClient {}

function wasmBinding(): ActorsRustBinding {
  return {
    async connect(endpoint, token) {
      // The generated module is produced by `build:wasm` immediately before tsc.
      // @ts-ignore generated Rust WASM module is intentionally untracked
      const module = await import("../generated/wasm/acyclic_actors_wasm.js");
      await module.default();
      const inner = await module.ActorsClient.connect(endpoint, token);
      return {
        transport: inner.transport,
        createActor: request => inner.create_actor(request),
        updateActor: request => inner.update_actor(request),
        inspectActor: request => inner.inspect_actor(request),
        addSubscription: request => inner.add_subscription(request),
        removeSubscription: request => inner.remove_subscription(request),
        resumeSubscription: request => inner.resume_subscription(request),
        checkpointActor: request => inner.checkpoint_actor(request),
        invokeActor: request => inner.invoke_actor(request),
      } satisfies ActorsRustClient;
    },
  };
}
