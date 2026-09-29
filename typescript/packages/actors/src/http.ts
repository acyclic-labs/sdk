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
import { HTTP_ROUTES } from "./routes.js";

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
    const localHttp = endpoint.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname);
    if ((!localHttp && endpoint.protocol !== "https:") || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      throw new TypeError("endpoint must be HTTPS or loopback HTTP without credentials, query, or fragment");
    }
    if (!options.token.trim()) throw new TypeError("token is required");
    this.#endpoint = endpoint;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? fetch;
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }

  async createActor(request: CreateActorRequest): Promise<CreateActorResponse> {
    return fromJsonString(CreateActorResponseSchema, await this.#post(HTTP_ROUTES.createActor, toJsonString(CreateActorRequestSchema, request)));
  }
  async updateActor(request: UpdateActorRequest): Promise<UpdateActorResponse> {
    return fromJsonString(UpdateActorResponseSchema, await this.#post(HTTP_ROUTES.updateActor, toJsonString(UpdateActorRequestSchema, request)));
  }
  async inspectActor(request: InspectActorRequest): Promise<InspectActorResponse> {
    return fromJsonString(InspectActorResponseSchema, await this.#post(HTTP_ROUTES.inspectActor, toJsonString(InspectActorRequestSchema, request)));
  }
  async addSubscription(request: AddSubscriptionRequest): Promise<AddSubscriptionResponse> {
    return fromJsonString(AddSubscriptionResponseSchema, await this.#post(HTTP_ROUTES.addSubscription, toJsonString(AddSubscriptionRequestSchema, request)));
  }
  async removeSubscription(request: RemoveSubscriptionRequest): Promise<RemoveSubscriptionResponse> {
    return fromJsonString(RemoveSubscriptionResponseSchema, await this.#post(HTTP_ROUTES.removeSubscription, toJsonString(RemoveSubscriptionRequestSchema, request)));
  }
  async resumeSubscription(request: ResumeSubscriptionRequest): Promise<ResumeSubscriptionResponse> {
    return fromJsonString(ResumeSubscriptionResponseSchema, await this.#post(HTTP_ROUTES.resumeSubscription, toJsonString(ResumeSubscriptionRequestSchema, request)));
  }
  async checkpointActor(request: CheckpointActorRequest): Promise<CheckpointActorResponse> {
    return fromJsonString(CheckpointActorResponseSchema, await this.#post(HTTP_ROUTES.checkpointActor, toJsonString(CheckpointActorRequestSchema, request)));
  }
  /** Invocation is not a Stream append or a durable checkpoint. */
  async invokeActor(request: InvokeActorRequest): Promise<InvokeActorResponse> {
    return fromJsonString(InvokeActorResponseSchema, await this.#post(HTTP_ROUTES.invokeActor, toJsonString(InvokeActorRequestSchema, request)));
  }

  async #post(path: string, body: string): Promise<string> {
    const response = await this.#fetcher(new URL(path, `${this.#endpoint.href.replace(/\/?$/, "/")}`), {
      method: "POST",
      headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" },
      body,
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
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maximum) {
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
