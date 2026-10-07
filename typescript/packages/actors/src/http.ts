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
import { observed, resolveObserver, type AcyclicObserver, type OperationSizes } from "./observe.js";

export interface HttpActorsOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly fetcher?: typeof fetch;
  readonly maximumResponseBytes?: number;
  readonly observer?: AcyclicObserver;
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
  readonly #observer: AcyclicObserver | undefined;

  constructor(options: HttpActorsOptions) {
    const endpoint = new URL(options.endpoint);
    const localHttp = endpoint.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname);
    if ((!localHttp && endpoint.protocol !== "https:") || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      throw new TypeError("endpoint must be HTTPS or loopback HTTP without credentials, query, or fragment");
    }
    if (!validBearerToken(options.token)) throw new TypeError("token must be a non-empty bearer token of at most 8 KiB without CR, LF, or NUL");
    this.#endpoint = endpoint;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#observer = resolveObserver(options.observer);
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }

  async createActor(request: CreateActorRequest): Promise<CreateActorResponse> {
    return fromJsonString(CreateActorResponseSchema, await this.#post("createActor", toJsonString(CreateActorRequestSchema, request)));
  }
  async updateActor(request: UpdateActorRequest): Promise<UpdateActorResponse> {
    return fromJsonString(UpdateActorResponseSchema, await this.#post("updateActor", toJsonString(UpdateActorRequestSchema, request)));
  }
  async inspectActor(request: InspectActorRequest): Promise<InspectActorResponse> {
    return fromJsonString(InspectActorResponseSchema, await this.#post("inspectActor", toJsonString(InspectActorRequestSchema, request)));
  }
  async addSubscription(request: AddSubscriptionRequest): Promise<AddSubscriptionResponse> {
    return fromJsonString(AddSubscriptionResponseSchema, await this.#post("addSubscription", toJsonString(AddSubscriptionRequestSchema, request)));
  }
  async removeSubscription(request: RemoveSubscriptionRequest): Promise<RemoveSubscriptionResponse> {
    return fromJsonString(RemoveSubscriptionResponseSchema, await this.#post("removeSubscription", toJsonString(RemoveSubscriptionRequestSchema, request)));
  }
  async resumeSubscription(request: ResumeSubscriptionRequest): Promise<ResumeSubscriptionResponse> {
    return fromJsonString(ResumeSubscriptionResponseSchema, await this.#post("resumeSubscription", toJsonString(ResumeSubscriptionRequestSchema, request)));
  }
  async checkpointActor(request: CheckpointActorRequest): Promise<CheckpointActorResponse> {
    return fromJsonString(CheckpointActorResponseSchema, await this.#post("checkpointActor", toJsonString(CheckpointActorRequestSchema, request)));
  }
  /** Invocation is not a Stream append or a durable checkpoint. */
  async invokeActor(request: InvokeActorRequest): Promise<InvokeActorResponse> {
    return fromJsonString(InvokeActorResponseSchema, await this.#post("invokeActor", toJsonString(InvokeActorRequestSchema, request)));
  }

  #post(route: keyof typeof HTTP_ROUTES, body: string, path: string = HTTP_ROUTES[route]): Promise<string> {
    return observed(this.#observer, "actors", route, sizes => this.#send(path, body, sizes));
  }

  async #send(path: string, body: string, sizes?: OperationSizes): Promise<string> {
    if (sizes) sizes.requestBytes = new TextEncoder().encode(body).byteLength;
    const response = await this.#fetcher(new URL(path, `${this.#endpoint.href.replace(/\/?$/, "/")}`), {
      method: "POST",
      redirect: "error",
      headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" },
      body,
    });
    const bytes = await boundedBytes(response, this.#maximum);
    if (sizes) sizes.responseBytes = bytes.byteLength;
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

/** Header-safe bearer token: non-blank, at most 8 KiB of UTF-8, and no CR, LF, or NUL. */
function validBearerToken(token: string): boolean {
  return token.trim().length > 0 && new TextEncoder().encode(token).byteLength <= 8192 && !/[\r\n\0]/.test(token);
}
