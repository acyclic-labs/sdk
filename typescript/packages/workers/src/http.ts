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
import type {
  RustOwnedPublicCancelJobRequest, RustOwnedPublicCancelJobResponse,
  RustOwnedPublicInspectJobRequest, RustOwnedPublicInspectJobResponse,
  RustOwnedPublicInvokeDeploymentRequest, RustOwnedPublicInvokeResponse,
  RustOwnedPublicInvokeVersionRequest,
  RustOwnedPublicPublishVersionRequest, RustOwnedPublicPublishVersionResponse,
  RustOwnedPublicSelectDeploymentRequest, RustOwnedPublicSelectDeploymentResponse,
  RustOwnedPublicSubmitJobRequest, RustOwnedPublicSubmitJobResponse,
} from "./generated-client.js";
import { WORKERS_HANDSHAKE, WORKERS_METHODS, WORKERS_REMOTE_POLICY, interpolateRustOwnedPath, negotiateRustOwnedEndpoint, validateRustOwnedCredential, type RustOwnedMethodMetadata } from "./generated-client.js";
import { validateWorkersContentLength, validateWorkersCredential, validateWorkersEndpoint, validateWorkersInvokeDeployment, validateWorkersInvokeVersion, validateWorkersResponseChunk, validateWorkersResponseLimit } from "./wasm-runtime.js";

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
  #handshake: Promise<void> | undefined;

  constructor(options: HttpWorkersOptions) {
    const endpoint = new URL(options.endpoint);
    validateWorkersEndpoint(options.endpoint);
    validateRustOwnedCredential(WORKERS_METHODS.invokeDeployment, options.token);
    validateWorkersCredential(options.token);
    this.#endpoint = endpoint;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#maximum = options.maximumResponseBytes ?? WORKERS_REMOTE_POLICY.maximumHttpResponseBytes;
    validateWorkersResponseLimit(this.#maximum);
  }

  async publishVersion(request: RustOwnedPublicPublishVersionRequest, signal?: AbortSignal): Promise<RustOwnedPublicPublishVersionResponse> {
    return fromJsonString(PublishVersionResponseSchema, await this.#post(WORKERS_METHODS.publishVersion, request, toJsonString(PublishVersionRequestSchema, request), signal)) as unknown as RustOwnedPublicPublishVersionResponse;
  }
  async selectDeployment(request: RustOwnedPublicSelectDeploymentRequest, signal?: AbortSignal): Promise<RustOwnedPublicSelectDeploymentResponse> {
    return fromJsonString(SelectDeploymentResponseSchema, await this.#post(WORKERS_METHODS.selectDeployment, request, toJsonString(SelectDeploymentRequestSchema, request), signal)) as unknown as RustOwnedPublicSelectDeploymentResponse;
  }
  async submitJob(request: RustOwnedPublicSubmitJobRequest, signal?: AbortSignal): Promise<RustOwnedPublicSubmitJobResponse> {
    return fromJsonString(SubmitJobResponseSchema, await this.#post(WORKERS_METHODS.submitJob, request, toJsonString(SubmitJobRequestSchema, request), signal)) as unknown as RustOwnedPublicSubmitJobResponse;
  }
  async inspectJob(request: RustOwnedPublicInspectJobRequest, signal?: AbortSignal): Promise<RustOwnedPublicInspectJobResponse> {
    return fromJsonString(InspectJobResponseSchema, await this.#post(WORKERS_METHODS.inspectJob, request, toJsonString(InspectJobRequestSchema, request), signal)) as unknown as RustOwnedPublicInspectJobResponse;
  }
  async cancelJob(request: RustOwnedPublicCancelJobRequest, signal?: AbortSignal): Promise<RustOwnedPublicCancelJobResponse> {
    return fromJsonString(CancelJobResponseSchema, await this.#post(WORKERS_METHODS.cancelJob, request, toJsonString(CancelJobRequestSchema, request), signal)) as unknown as RustOwnedPublicCancelJobResponse;
  }
  /** Invokes exact immutable code bytes with ordinary HTTP request ambiguity. */
  async invokeVersion(request: RustOwnedPublicInvokeVersionRequest, signal?: AbortSignal): Promise<RustOwnedPublicInvokeResponse> {
    validateWorkersInvokeVersion(request.versionSha256, request.method);
    return fromJsonString(InvokeResponseSchema, await this.#post(WORKERS_METHODS.invokeVersion, request, toJsonString(InvokeVersionRequestSchema, request), signal)) as unknown as RustOwnedPublicInvokeResponse;
  }
  /** Resolves the alias once at ingress and reports the resolved digest/revision. */
  async invokeDeployment(request: RustOwnedPublicInvokeDeploymentRequest, signal?: AbortSignal): Promise<RustOwnedPublicInvokeResponse> {
    validateWorkersInvokeDeployment(request.alias, request.method);
    return fromJsonString(InvokeResponseSchema, await this.#post(WORKERS_METHODS.invokeDeployment, request, toJsonString(InvokeDeploymentRequestSchema, request), signal)) as unknown as RustOwnedPublicInvokeResponse;
  }

  async #post(method: RustOwnedMethodMetadata, request: unknown, body: string, signal?: AbortSignal): Promise<string> {
    const headers = { authorization: `Bearer ${this.#token}`, "content-type": "application/json" };
    await this.#ensureHandshake(headers, signal);
    const response = await this.#fetcher(new URL(interpolateRustOwnedPath(method, request), `${this.#endpoint.href.replace(/\/?$/, "/")}`), {
      method: "POST",
      headers,
      body,
      signal,
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

  async #ensureHandshake(headers: HeadersInit, signal?: AbortSignal): Promise<void> {
    if (this.#handshake !== undefined) return this.#handshake;
    const pending = negotiateRustOwnedEndpoint(this.#fetcher, this.#endpoint, headers, WORKERS_HANDSHAKE, this.#maximum, signal)
      .catch(error => { this.#handshake = undefined; throw error; });
    this.#handshake = pending;
    return pending;
  }
}

async function boundedBytes(response: Response, maximum: number): Promise<Uint8Array> {
  const contentLength = response.headers.get("content-length");
  if (contentLength !== null && /^\d+$/.test(contentLength)) {
    try { validateWorkersContentLength(contentLength, maximum); }
    catch { throw new WorkersTransportError("response exceeds configured bound", response.status); }
  }
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      try { size = validateWorkersResponseChunk(size, value.byteLength, maximum); }
      catch {
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
