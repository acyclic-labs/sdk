import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { Message, MessageInitShape } from "@bufbuild/protobuf";
import type { GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import {
  AddSubscriptionRequestSchema,
  AddSubscriptionResponseSchema,
  CheckpointActorRequestSchema,
  CheckpointActorResponseSchema,
  CreateActorRequestSchema,
  CreateActorResponseSchema,
  InspectActorRequestSchema,
  InspectActorResponseSchema,
  InvokeActorRequestSchema,
  InvokeActorResponseSchema,
  RemoveSubscriptionRequestSchema,
  RemoveSubscriptionResponseSchema,
  ResumeSubscriptionRequestSchema,
  ResumeSubscriptionResponseSchema,
  UpdateActorRequestSchema,
  UpdateActorResponseSchema,
  ActorsService,
} from "../generated/proto/actors/v1/actors_pb.js";
import type {
  AddSubscriptionResponse,
  CheckpointActorResponse,
  CreateActorResponse,
  InspectActorResponse,
  InvokeActorResponse,
  RemoveSubscriptionResponse,
  ResumeSubscriptionResponse,
  UpdateActorResponse,
} from "../generated/proto/actors/v1/actors_pb.js";
import type {
  AddSubscriptionRequest as RustAddSubscriptionRequest,
  CheckpointActorRequest as RustCheckpointActorRequest,
  CreateActorRequest as RustCreateActorRequest,
  InspectActorRequest as RustInspectActorRequest,
  InvokeActorRequest as RustInvokeActorRequest,
  RemoveSubscriptionRequest as RustRemoveSubscriptionRequest,
  ResumeSubscriptionRequest as RustResumeSubscriptionRequest,
  UpdateActorRequest as RustUpdateActorRequest,
} from "@acyclic-labs/actors/types";

export interface ActorsClientOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: Uint8Array;
}

/** Operation names emitted by the Rust-owned Actors service descriptor. */
type ActorsServiceMethods = typeof ActorsService extends GenService<infer Methods> ? Methods : never;
export type ActorsOperation = Extract<keyof ActorsServiceMethods, string>;

/** Rust-generated error metadata with the normal JavaScript Error surface. */
export type ActorsError = Error & Readonly<Record<string, unknown>>;

type ActorsRequest<S extends GenMessage<Message>> = MessageInitShape<S>;
type Assert<T extends true> = T;

/** The Rust-generated request projections must remain directly encodable by Buf. */
type _ActorsRustRequestShapeChecks = [
  Assert<RustCreateActorRequest extends ActorsRequest<typeof CreateActorRequestSchema> ? true : false>,
  Assert<RustUpdateActorRequest extends ActorsRequest<typeof UpdateActorRequestSchema> ? true : false>,
  Assert<RustInspectActorRequest extends ActorsRequest<typeof InspectActorRequestSchema> ? true : false>,
  Assert<RustAddSubscriptionRequest extends ActorsRequest<typeof AddSubscriptionRequestSchema> ? true : false>,
  Assert<RustRemoveSubscriptionRequest extends ActorsRequest<typeof RemoveSubscriptionRequestSchema> ? true : false>,
  Assert<RustResumeSubscriptionRequest extends ActorsRequest<typeof ResumeSubscriptionRequestSchema> ? true : false>,
  Assert<RustCheckpointActorRequest extends ActorsRequest<typeof CheckpointActorRequestSchema> ? true : false>,
  Assert<RustInvokeActorRequest extends ActorsRequest<typeof InvokeActorRequestSchema> ? true : false>,
];

/** Converts native, WASM, and ordinary JS failures to the shared Rust error surface. */
export function normalizeActorsError(value: unknown): ActorsError {
  if (value instanceof Error) return value as ActorsError;
  const metadata = typeof value === "object" && value !== null ? value : {};
  const error = new Error(
    typeof (metadata as { readonly message?: unknown }).message === "string"
      ? (metadata as { readonly message: string }).message
      : "Actors operation failed",
  ) as ActorsError;
  Object.assign(error, metadata);
  return error;
}

type Binding = {
  readonly transport: string;
  call(operation: ActorsOperation, request: Uint8Array, signal?: AbortSignal): Promise<Uint8Array>;
};

function runningInNode(): boolean {
  const process = (globalThis as { readonly process?: { readonly versions?: { readonly node?: string } } }).process;
  return typeof process?.versions?.node === "string";
}

export class ActorsClient {
  private constructor(private readonly binding: Binding) {}

  /** Selects the canonical Rust native client in Node and Rust gRPC-Web in browsers. */
  static async connect(options: ActorsClientOptions): Promise<ActorsClient> {
    const binding = runningInNode()
      ? await (await import("./native.js")).NativeActorsClient.connect(
        options.endpoint,
        options.token,
        options.caCertificate,
      )
      : await (await import("./browser.js")).BrowserActorsClient.connect(
        options.endpoint,
        options.token,
      );
    return new ActorsClient(binding);
  }

  get transport(): string {
    return this.binding.transport;
  }

  async createActor(request: RustCreateActorRequest, signal?: AbortSignal): Promise<CreateActorResponse> {
    return this.call("createActor", CreateActorRequestSchema, CreateActorResponseSchema, request, signal);
  }
  async updateActor(request: RustUpdateActorRequest, signal?: AbortSignal): Promise<UpdateActorResponse> {
    return this.call("updateActor", UpdateActorRequestSchema, UpdateActorResponseSchema, request, signal);
  }
  async inspectActor(request: RustInspectActorRequest, signal?: AbortSignal): Promise<InspectActorResponse> {
    return this.call("inspectActor", InspectActorRequestSchema, InspectActorResponseSchema, request, signal);
  }
  async addSubscription(request: RustAddSubscriptionRequest, signal?: AbortSignal): Promise<AddSubscriptionResponse> {
    return this.call("addSubscription", AddSubscriptionRequestSchema, AddSubscriptionResponseSchema, request, signal);
  }
  async removeSubscription(request: RustRemoveSubscriptionRequest, signal?: AbortSignal): Promise<RemoveSubscriptionResponse> {
    return this.call("removeSubscription", RemoveSubscriptionRequestSchema, RemoveSubscriptionResponseSchema, request, signal);
  }
  async resumeSubscription(request: RustResumeSubscriptionRequest, signal?: AbortSignal): Promise<ResumeSubscriptionResponse> {
    return this.call("resumeSubscription", ResumeSubscriptionRequestSchema, ResumeSubscriptionResponseSchema, request, signal);
  }
  async checkpointActor(request: RustCheckpointActorRequest, signal?: AbortSignal): Promise<CheckpointActorResponse> {
    return this.call("checkpointActor", CheckpointActorRequestSchema, CheckpointActorResponseSchema, request, signal);
  }
  async invokeActor(request: RustInvokeActorRequest, signal?: AbortSignal): Promise<InvokeActorResponse> {
    return this.call("invokeActor", InvokeActorRequestSchema, InvokeActorResponseSchema, request, signal);
  }

  private async call<I extends Message, O extends Message>(
    operation: ActorsOperation,
    requestSchema: GenMessage<I>,
    responseSchema: GenMessage<O>,
    request: MessageInitShape<GenMessage<I>>,
    signal?: AbortSignal,
  ): Promise<O> {
    let bytes: Uint8Array;
    try {
      bytes = toBinary(requestSchema, create(requestSchema, request));
    } catch (error) {
      throw normalizeActorsError({ code: "invalid_argument", message: `${operation} request could not be encoded: ${String(error)}` });
    }
    try {
      const response = await this.binding.call(operation, bytes, signal);
      return fromBinary(responseSchema, response);
    } catch (error) {
      if (error instanceof Error) throw error;
      throw normalizeActorsError(error);
    }
  }
}
