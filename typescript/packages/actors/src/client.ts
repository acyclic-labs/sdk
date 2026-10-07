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
  InvokeActorResponse,
} from "../generated/proto/actors/v1/actors_pb.js";
import type {
  AddSubscriptionRequest as RustAddSubscriptionRequest,
  AddSubscriptionResponse as RustAddSubscriptionResponse,
  CheckpointActorRequest as RustCheckpointActorRequest,
  CheckpointActorResponse as RustCheckpointActorResponse,
  CreateActorRequest as RustCreateActorRequest,
  CreateActorResponse as RustCreateActorResponse,
  InspectActorRequest as RustInspectActorRequest,
  InspectActorResponse as RustInspectActorResponse,
  InvokeActorRequest as RustInvokeActorRequest,
  InvokeActorResponse as RustInvokeActorResponse,
  RemoveSubscriptionRequest as RustRemoveSubscriptionRequest,
  RemoveSubscriptionResponse as RustRemoveSubscriptionResponse,
  ResumeSubscriptionRequest as RustResumeSubscriptionRequest,
  ResumeSubscriptionResponse as RustResumeSubscriptionResponse,
  UpdateActorRequest as RustUpdateActorRequest,
  UpdateActorResponse as RustUpdateActorResponse,
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
// MessageInitShape recursively removes Buf runtime metadata from nested
// messages while preserving their generated field types.
type ActorsDecodedResponse<S extends GenMessage<Message>> = MessageInitShape<S>;
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

/**
 * Rust response projections must remain representable by the decoded Buf
 * messages. The actor field is optional on the wire, so its nullability is
 * adapted only at the semantic boundary below.
 */
type RustActorResponseWireShape<R extends { readonly actor: unknown }> =
  Omit<R, "actor"> & { readonly actor?: Exclude<R["actor"], null> | undefined };

type _ActorsRustResponseShapeChecks = [
  Assert<RustActorResponseWireShape<RustCreateActorResponse> extends ActorsDecodedResponse<typeof CreateActorResponseSchema> ? true : false>,
  Assert<RustActorResponseWireShape<RustUpdateActorResponse> extends ActorsDecodedResponse<typeof UpdateActorResponseSchema> ? true : false>,
  Assert<RustActorResponseWireShape<RustInspectActorResponse> extends ActorsDecodedResponse<typeof InspectActorResponseSchema> ? true : false>,
  Assert<RustActorResponseWireShape<RustAddSubscriptionResponse> extends ActorsDecodedResponse<typeof AddSubscriptionResponseSchema> ? true : false>,
  Assert<RustActorResponseWireShape<RustRemoveSubscriptionResponse> extends ActorsDecodedResponse<typeof RemoveSubscriptionResponseSchema> ? true : false>,
  Assert<RustActorResponseWireShape<RustResumeSubscriptionResponse> extends ActorsDecodedResponse<typeof ResumeSubscriptionResponseSchema> ? true : false>,
  Assert<RustActorResponseWireShape<RustCheckpointActorResponse> extends ActorsDecodedResponse<typeof CheckpointActorResponseSchema> ? true : false>,
  Assert<RustInvokeActorResponse extends ActorsDecodedResponse<typeof InvokeActorResponseSchema> ? true : false>,
];

/**
 * The native/WASM binding has already run the canonical Rust TryFrom path.
 * This only maps protobuf's absent message to the semantic Option/null shape;
 * validation remains exclusively in Rust.
 */
function decodeRustActorResponse<R extends { readonly actor: unknown }>(
  response: Message & { readonly actor?: unknown },
): R {
  return { ...response, actor: response.actor ?? null } as unknown as R;
}

/** Invoke's Rust and Buf projections have the same runtime field shape. */
function decodeRustInvokeResponse(response: InvokeActorResponse): RustInvokeActorResponse {
  return response as unknown as RustInvokeActorResponse;
}

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

  async createActor(request: RustCreateActorRequest, signal?: AbortSignal): Promise<RustCreateActorResponse> {
    return this.call("createActor", CreateActorRequestSchema, CreateActorResponseSchema, request,
      response => decodeRustActorResponse<RustCreateActorResponse>(response), signal);
  }
  async updateActor(request: RustUpdateActorRequest, signal?: AbortSignal): Promise<RustUpdateActorResponse> {
    return this.call("updateActor", UpdateActorRequestSchema, UpdateActorResponseSchema, request,
      response => decodeRustActorResponse<RustUpdateActorResponse>(response), signal);
  }
  async inspectActor(request: RustInspectActorRequest, signal?: AbortSignal): Promise<RustInspectActorResponse> {
    return this.call("inspectActor", InspectActorRequestSchema, InspectActorResponseSchema, request,
      response => decodeRustActorResponse<RustInspectActorResponse>(response), signal);
  }
  async addSubscription(request: RustAddSubscriptionRequest, signal?: AbortSignal): Promise<RustAddSubscriptionResponse> {
    return this.call("addSubscription", AddSubscriptionRequestSchema, AddSubscriptionResponseSchema, request,
      response => decodeRustActorResponse<RustAddSubscriptionResponse>(response), signal);
  }
  async removeSubscription(request: RustRemoveSubscriptionRequest, signal?: AbortSignal): Promise<RustRemoveSubscriptionResponse> {
    return this.call("removeSubscription", RemoveSubscriptionRequestSchema, RemoveSubscriptionResponseSchema, request,
      response => decodeRustActorResponse<RustRemoveSubscriptionResponse>(response), signal);
  }
  async resumeSubscription(request: RustResumeSubscriptionRequest, signal?: AbortSignal): Promise<RustResumeSubscriptionResponse> {
    return this.call("resumeSubscription", ResumeSubscriptionRequestSchema, ResumeSubscriptionResponseSchema, request,
      response => decodeRustActorResponse<RustResumeSubscriptionResponse>(response), signal);
  }
  async checkpointActor(request: RustCheckpointActorRequest, signal?: AbortSignal): Promise<RustCheckpointActorResponse> {
    return this.call("checkpointActor", CheckpointActorRequestSchema, CheckpointActorResponseSchema, request,
      response => decodeRustActorResponse<RustCheckpointActorResponse>(response), signal);
  }
  async invokeActor(request: RustInvokeActorRequest, signal?: AbortSignal): Promise<RustInvokeActorResponse> {
    return this.call("invokeActor", InvokeActorRequestSchema, InvokeActorResponseSchema, request,
      decodeRustInvokeResponse, signal);
  }

  private async call<I extends Message, O extends Message, R>(
    operation: ActorsOperation,
    requestSchema: GenMessage<I>,
    responseSchema: GenMessage<O>,
    request: MessageInitShape<GenMessage<I>>,
    decodeResponse: (response: O) => R,
    signal?: AbortSignal,
  ): Promise<R> {
    let bytes: Uint8Array;
    try {
      bytes = toBinary(requestSchema, create(requestSchema, request));
    } catch (error) {
      throw normalizeActorsError({ code: "invalid_argument", message: `${operation} request could not be encoded: ${String(error)}` });
    }
    try {
      const response = await this.binding.call(operation, bytes, signal);
      return decodeResponse(fromBinary(responseSchema, response));
    } catch (error) {
      if (error instanceof Error) throw error;
      throw normalizeActorsError(error);
    }
  }
}
