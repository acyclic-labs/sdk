import { fromJsonString, toJsonString } from "@bufbuild/protobuf";
import {
  CancelJobRequestSchema, CancelJobResponseSchema,
  InspectJobRequestSchema, InspectJobResponseSchema,
  InvokeVersionRequestSchema, InvokeDeploymentRequestSchema, InvokeResponseSchema,
  PublishVersionRequestSchema, PublishVersionResponseSchema,
  SelectDeploymentRequestSchema, SelectDeploymentResponseSchema,
  SubmitJobRequestSchema, SubmitJobResponseSchema,
  ErrorSchema,
} from "../generated/proto/workers/v1/workers_pb.js";
import type {
  CancelJobRequest, CancelJobResponse, InspectJobRequest, InspectJobResponse,
  InvokeVersionRequest, InvokeDeploymentRequest, InvokeResponse, PublishVersionRequest, PublishVersionResponse,
  SelectDeploymentRequest, SelectDeploymentResponse, SubmitJobRequest, SubmitJobResponse,
  ErrorCode,
} from "../generated/proto/workers/v1/workers_pb.js";
import { HTTP_ROUTES } from "./routes.js";

export interface HttpWorkersOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly fetcher?: typeof fetch;
  readonly maximumResponseBytes?: number;
}

export class WorkersTransportError extends Error {
  constructor(message: string, readonly status: number, readonly code?: ErrorCode) { super(message); }
}

/** Authenticated transport. Mutations are retried only by caller intent and key. */
export class HttpWorkersClient {
  readonly #endpoint: URL;
  readonly #token: string;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;

  constructor(options: HttpWorkersOptions) {
    const endpoint = new URL(options.endpoint);
    const localHttp = endpoint.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname);
    if ((!localHttp && endpoint.protocol !== "https:") || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      throw new TypeError("endpoint must be HTTPS or loopback HTTP without credentials, query, or fragment");
    }
    if (!options.token.trim()) throw new TypeError("token is required");
    this.#endpoint = endpoint;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }

  async publishVersion(request: PublishVersionRequest): Promise<PublishVersionResponse> {
    return fromJsonString(PublishVersionResponseSchema, await this.#post(HTTP_ROUTES.publishVersion, toJsonString(PublishVersionRequestSchema, request)));
  }
  async selectDeployment(request: SelectDeploymentRequest): Promise<SelectDeploymentResponse> {
    return fromJsonString(SelectDeploymentResponseSchema, await this.#post(HTTP_ROUTES.selectDeployment, toJsonString(SelectDeploymentRequestSchema, request)));
  }
  async submitJob(request: SubmitJobRequest): Promise<SubmitJobResponse> {
    return fromJsonString(SubmitJobResponseSchema, await this.#post(HTTP_ROUTES.submitJob, toJsonString(SubmitJobRequestSchema, request)));
  }
  async inspectJob(request: InspectJobRequest): Promise<InspectJobResponse> {
    return fromJsonString(InspectJobResponseSchema, await this.#post(HTTP_ROUTES.inspectJob, toJsonString(InspectJobRequestSchema, request)));
  }
  async cancelJob(request: CancelJobRequest): Promise<CancelJobResponse> {
    return fromJsonString(CancelJobResponseSchema, await this.#post(HTTP_ROUTES.cancelJob, toJsonString(CancelJobRequestSchema, request)));
  }
  /** Invokes exact immutable code bytes with ordinary HTTP request ambiguity. */
  async invokeVersion(request: InvokeVersionRequest): Promise<InvokeResponse> {
    if (request.versionSha256.byteLength !== 32) throw new RangeError("version digest must contain exactly 32 bytes");
    const digest = Array.from(request.versionSha256, byte => byte.toString(16).padStart(2, "0")).join("");
    const path = HTTP_ROUTES.invokeVersion.replace("{sha256hex}", digest);
    return fromJsonString(InvokeResponseSchema, await this.#post(path, toJsonString(InvokeVersionRequestSchema, request)));
  }
  /** Resolves the alias once at ingress and reports the resolved digest/revision. */
  async invokeDeployment(request: InvokeDeploymentRequest): Promise<InvokeResponse> {
    if (!/^[A-Za-z0-9._-]{1,256}$/.test(request.alias) || request.alias === "." || request.alias === "..") throw new TypeError("invalid deployment alias");
    const path = HTTP_ROUTES.invokeDeployment.replace("{alias}", encodeURIComponent(request.alias));
    return fromJsonString(InvokeResponseSchema, await this.#post(path, toJsonString(InvokeDeploymentRequestSchema, request)));
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
    catch { throw new WorkersTransportError("malformed UTF-8 response", response.status); }
    if (!response.ok) {
      try {
        const error = fromJsonString(ErrorSchema, json);
        throw new WorkersTransportError(error.message || `HTTP ${response.status}`, response.status, error.code);
      } catch (error) {
        if (error instanceof WorkersTransportError) throw error;
        throw new WorkersTransportError(json || `HTTP ${response.status}`, response.status);
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
        throw new WorkersTransportError("response exceeds configured bound", response.status);
      }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}
