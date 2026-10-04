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
import { WORKERS_METHODS, interpolateRustOwnedPath, validateRustOwnedCredential, type RustOwnedMethodMetadata } from "./generated-client.js";
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

  constructor(options: HttpWorkersOptions) {
    const endpoint = new URL(options.endpoint);
    validateWorkersEndpoint(options.endpoint);
    validateRustOwnedCredential(WORKERS_METHODS.invokeDeployment, options.token);
    validateWorkersCredential(options.token);
    this.#endpoint = endpoint;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    validateWorkersResponseLimit(this.#maximum);
  }

  async publishVersion(request: PublishVersionRequest, signal?: AbortSignal): Promise<PublishVersionResponse> {
    return fromJsonString(PublishVersionResponseSchema, await this.#post(WORKERS_METHODS.publishVersion, request, toJsonString(PublishVersionRequestSchema, request), signal));
  }
  async selectDeployment(request: SelectDeploymentRequest, signal?: AbortSignal): Promise<SelectDeploymentResponse> {
    return fromJsonString(SelectDeploymentResponseSchema, await this.#post(WORKERS_METHODS.selectDeployment, request, toJsonString(SelectDeploymentRequestSchema, request), signal));
  }
  async submitJob(request: SubmitJobRequest, signal?: AbortSignal): Promise<SubmitJobResponse> {
    return fromJsonString(SubmitJobResponseSchema, await this.#post(WORKERS_METHODS.submitJob, request, toJsonString(SubmitJobRequestSchema, request), signal));
  }
  async inspectJob(request: InspectJobRequest, signal?: AbortSignal): Promise<InspectJobResponse> {
    return fromJsonString(InspectJobResponseSchema, await this.#post(WORKERS_METHODS.inspectJob, request, toJsonString(InspectJobRequestSchema, request), signal));
  }
  async cancelJob(request: CancelJobRequest, signal?: AbortSignal): Promise<CancelJobResponse> {
    return fromJsonString(CancelJobResponseSchema, await this.#post(WORKERS_METHODS.cancelJob, request, toJsonString(CancelJobRequestSchema, request), signal));
  }
  /** Invokes exact immutable code bytes with ordinary HTTP request ambiguity. */
  async invokeVersion(request: InvokeVersionRequest, signal?: AbortSignal): Promise<InvokeResponse> {
    validateWorkersInvokeVersion(request.versionSha256, request.method);
    return fromJsonString(InvokeResponseSchema, await this.#post(WORKERS_METHODS.invokeVersion, request, toJsonString(InvokeVersionRequestSchema, request), signal));
  }
  /** Resolves the alias once at ingress and reports the resolved digest/revision. */
  async invokeDeployment(request: InvokeDeploymentRequest, signal?: AbortSignal): Promise<InvokeResponse> {
    validateWorkersInvokeDeployment(request.alias, request.method);
    return fromJsonString(InvokeResponseSchema, await this.#post(WORKERS_METHODS.invokeDeployment, request, toJsonString(InvokeDeploymentRequestSchema, request), signal));
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
