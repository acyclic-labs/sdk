import { fromJsonString, toJsonString } from "@bufbuild/protobuf";
import {
  AddSubscriptionRequestSchema, AddSubscriptionResponseSchema,
  CheckpointActorRequestSchema, CheckpointActorResponseSchema,
  CreateActorRequestSchema, CreateActorResponseSchema,
  InspectActorRequestSchema, InspectActorResponseSchema,
  InvokeActorRequestSchema, InvokeActorResponseSchema,
  RemoveSubscriptionRequestSchema, RemoveSubscriptionResponseSchema,
  ResumeSubscriptionRequestSchema, ResumeSubscriptionResponseSchema,
  UpdateActorRequestSchema, UpdateActorResponseSchema,
  ErrorSchema,
} from "../generated/proto/actors/v1/actors_pb.js";
import type {
  AddSubscriptionRequest, AddSubscriptionResponse,
  CheckpointActorRequest, CheckpointActorResponse,
  CreateActorRequest, CreateActorResponse,
  InspectActorRequest, InspectActorResponse,
  InvokeActorRequest, InvokeActorResponse,
  RemoveSubscriptionRequest, RemoveSubscriptionResponse,
  ResumeSubscriptionRequest, ResumeSubscriptionResponse,
  UpdateActorRequest, UpdateActorResponse,
  ErrorCode,
} from "../generated/proto/actors/v1/actors_pb.js";
import { ACTORS_METHODS, interpolateRustOwnedPath, validateRustOwnedCredential, type RustOwnedMethodMetadata } from "./generated-client.js";
import { validateActorsContentLength, validateActorsCredential, validateActorsEndpoint, validateActorsInvoke, validateActorsResponseChunk, validateActorsResponseLimit } from "./wasm-runtime.js";

export interface HttpActorsOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly fetcher?: typeof fetch;
  readonly maximumResponseBytes?: number;
}

export class ActorsTransportError extends Error {
  constructor(message: string, readonly status: number, readonly code?: ErrorCode) { super(message); }
}

/** Authenticated transport for the generated Actors v1 contract. */
export class HttpActorsClient {
  readonly #endpoint: URL;
  readonly #token: string;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;

  constructor(options: HttpActorsOptions) {
    const endpoint = new URL(options.endpoint);
    validateActorsEndpoint(options.endpoint);
    validateRustOwnedCredential(ACTORS_METHODS.createActor, options.token);
    validateActorsCredential(options.token);
    this.#endpoint = endpoint;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    validateActorsResponseLimit(this.#maximum);
  }

  async createActor(request: CreateActorRequest, signal?: AbortSignal): Promise<CreateActorResponse> {
    return fromJsonString(CreateActorResponseSchema, await this.#post(ACTORS_METHODS.createActor, request, toJsonString(CreateActorRequestSchema, request), signal));
  }
  async updateActor(request: UpdateActorRequest, signal?: AbortSignal): Promise<UpdateActorResponse> {
    return fromJsonString(UpdateActorResponseSchema, await this.#post(ACTORS_METHODS.updateActor, request, toJsonString(UpdateActorRequestSchema, request), signal));
  }
  async inspectActor(request: InspectActorRequest, signal?: AbortSignal): Promise<InspectActorResponse> {
    return fromJsonString(InspectActorResponseSchema, await this.#post(ACTORS_METHODS.inspectActor, request, toJsonString(InspectActorRequestSchema, request), signal));
  }
  async addSubscription(request: AddSubscriptionRequest, signal?: AbortSignal): Promise<AddSubscriptionResponse> {
    return fromJsonString(AddSubscriptionResponseSchema, await this.#post(ACTORS_METHODS.addSubscription, request, toJsonString(AddSubscriptionRequestSchema, request), signal));
  }
  async removeSubscription(request: RemoveSubscriptionRequest, signal?: AbortSignal): Promise<RemoveSubscriptionResponse> {
    return fromJsonString(RemoveSubscriptionResponseSchema, await this.#post(ACTORS_METHODS.removeSubscription, request, toJsonString(RemoveSubscriptionRequestSchema, request), signal));
  }
  async resumeSubscription(request: ResumeSubscriptionRequest, signal?: AbortSignal): Promise<ResumeSubscriptionResponse> {
    return fromJsonString(ResumeSubscriptionResponseSchema, await this.#post(ACTORS_METHODS.resumeSubscription, request, toJsonString(ResumeSubscriptionRequestSchema, request), signal));
  }
  async checkpointActor(request: CheckpointActorRequest, signal?: AbortSignal): Promise<CheckpointActorResponse> {
    return fromJsonString(CheckpointActorResponseSchema, await this.#post(ACTORS_METHODS.checkpointActor, request, toJsonString(CheckpointActorRequestSchema, request), signal));
  }
  /** Invocation is not a Stream append or a durable checkpoint. */
  async invokeActor(request: InvokeActorRequest, signal?: AbortSignal): Promise<InvokeActorResponse> {
    validateActorsInvoke(request.actorId, request.method);
    return fromJsonString(InvokeActorResponseSchema, await this.#post(ACTORS_METHODS.invokeActor, request, toJsonString(InvokeActorRequestSchema, request), signal));
  }

  async #post(method: RustOwnedMethodMetadata, request: unknown, body: string, signal?: AbortSignal): Promise<string> {
    const response = await this.#fetcher(new URL(interpolateRustOwnedPath(method, request), `${this.#endpoint.href.replace(/\/?$/, "/")}`), {
      method: "POST",
      headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" },
      body,
      signal,
    });
    const bytes = await boundedBytes(response, this.#maximum);
    let json: string;
    try { json = new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
    catch { throw new ActorsTransportError("malformed UTF-8 response", response.status); }
    if (!response.ok) {
      try {
        const error = fromJsonString(ErrorSchema, json);
        throw new ActorsTransportError(error.message || `HTTP ${response.status}`, response.status, error.code);
      } catch (error) {
        if (error instanceof ActorsTransportError) throw error;
        throw new ActorsTransportError(json || `HTTP ${response.status}`, response.status);
      }
    }
    return json;
  }
}

async function boundedBytes(response: Response, maximum: number): Promise<Uint8Array> {
  const contentLength = response.headers.get("content-length");
  if (contentLength !== null && /^\d+$/.test(contentLength)) {
    try { validateActorsContentLength(contentLength, maximum); }
    catch { throw new ActorsTransportError("response exceeds configured bound", response.status); }
  }
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      try { size = validateActorsResponseChunk(size, value.byteLength, maximum); }
      catch {
        await reader.cancel().catch(() => undefined);
        throw new ActorsTransportError("response exceeds configured bound", response.status);
      }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}
