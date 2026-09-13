import { create, fromJson, toJsonString, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import {
  ContextViewSchema,
  CreateContextRequestSchema,
  GenerateRunRequestSchema,
  GenerateRunResponseSchema,
  InspectContextRequestSchema,
  InspectRunRequestSchema,
  InspectWarmRequestSchema,
  ListModelsRequestSchema,
  ListModelsResponseSchema,
  MutateContextRequestSchema,
  MutationReceiptSchema,
  ReleaseWarmRequestSchema,
  RenewWarmRequestSchema,
  RetainWarmRequestSchema,
  RunEventSchema,
  RunViewSchema,
  WarmViewSchema,
  WatchRunRequestSchema,
  type ContextView,
  type CreateContextRequest,
  type GenerateRunRequest,
  type GenerateRunResponse,
  type InspectContextRequest,
  type InspectRunRequest,
  type InspectWarmRequest,
  type ListModelsResponse,
  type MutateContextRequest,
  type MutationReceipt,
  type ReleaseWarmRequest,
  type RenewWarmRequest,
  type RetainWarmRequest,
  type RunEvent,
  type RunView,
  type WarmView,
  type WatchRunRequest,
} from "../generated/proto/inference/v1/inference_pb.js";

export * from "../generated/proto/inference/v1/inference_pb.js";

/** Complete transport-neutral customer lifecycle contract. */
export interface InferenceTransport {
  listModels(): Promise<ListModelsResponse>;
  createContext(request: CreateContextRequest): Promise<MutationReceipt>;
  inspectContext(request: InspectContextRequest): Promise<ContextView>;
  mutateContext(request: MutateContextRequest): Promise<MutationReceipt>;
  retainWarm(request: RetainWarmRequest): Promise<WarmView>;
  inspectWarm(request: InspectWarmRequest): Promise<WarmView>;
  renewWarm(request: RenewWarmRequest): Promise<WarmView>;
  releaseWarm(request: ReleaseWarmRequest): Promise<WarmView>;
  generateRun(request: GenerateRunRequest): Promise<GenerateRunResponse>;
  inspectRun(request: InspectRunRequest): Promise<RunView>;
  watchRun(request: WatchRunRequest, signal?: AbortSignal): AsyncIterable<RunEvent>;
  cancelRun(request: InspectRunRequest): Promise<RunView>;
}

/** Generated-contract client covering contexts, edits, compaction, warm state, and recoverable runs. */
export class InferenceClient {
  constructor(readonly transport: InferenceTransport) {}

  listModels(): Promise<ListModelsResponse> { return this.transport.listModels(); }
  createContext(request: CreateContextRequest): Promise<MutationReceipt> { return this.transport.createContext(request); }
  inspectContext(revision: Uint8Array): Promise<ContextView> {
    return this.transport.inspectContext(create(InspectContextRequestSchema, { revision }));
  }
  mutateContext(request: MutateContextRequest): Promise<MutationReceipt> { return this.transport.mutateContext(request); }
  retainWarm(request: RetainWarmRequest): Promise<WarmView> { return this.transport.retainWarm(request); }
  inspectWarm(commitment: Uint8Array): Promise<WarmView> {
    return this.transport.inspectWarm(create(InspectWarmRequestSchema, { commitment }));
  }
  renewWarm(request: RenewWarmRequest): Promise<WarmView> { return this.transport.renewWarm(request); }
  releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> { return this.transport.releaseWarm(request); }
  generate(request: GenerateRunRequest): Promise<GenerateRunResponse> { return this.transport.generateRun(request); }
  inspectRun(runId: Uint8Array): Promise<RunView> {
    return this.transport.inspectRun(create(InspectRunRequestSchema, { runId }));
  }
  watchRun(runId: Uint8Array, fromSequence = 0n, signal?: AbortSignal): AsyncIterable<RunEvent> {
    return this.transport.watchRun(create(WatchRunRequestSchema, { runId, fromSequence }), signal);
  }
  cancelRun(runId: Uint8Array): Promise<RunView> {
    return this.transport.cancelRun(create(InspectRunRequestSchema, { runId }));
  }
}

export type AuthorizationHeaders = () => HeadersInit | Promise<HeadersInit>;

const utf8 = new TextEncoder();

function utf8Length(value: string): number {
  return utf8.encode(value).byteLength;
}

async function readBoundedText(response: Response, maximumBytes: number, kind: string): Promise<string> {
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const chunks: string[] = [];
  let observed = 0;
  let completed = false;
  try {
    for (;;) {
      const item = await reader.read();
      if (item.done) break;
      observed += item.value.byteLength;
      if (observed > maximumBytes) {
        throw new InferenceTransportError(response.status, `${kind} exceeds configured bound`);
      }
      chunks.push(decoder.decode(item.value, { stream: true }));
    }
    chunks.push(decoder.decode());
    completed = true;
    return chunks.join("");
  } finally {
    if (!completed) await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

/** Authenticated protobuf-JSON/NDJSON transport for the public service contract. */
export class HttpInferenceTransport implements InferenceTransport {
  constructor(
    readonly endpoint: string,
    readonly authorization: AuthorizationHeaders,
    readonly fetcher: typeof fetch = fetch,
    readonly maximumEventBytes = 1024 * 1024,
  ) {
    if (!Number.isSafeInteger(maximumEventBytes) || maximumEventBytes <= 0) {
      throw new RangeError("maximumEventBytes must be a positive safe integer byte ceiling");
    }
    let parsed: URL;
    try {
      parsed = new URL(endpoint);
    } catch {
      throw new TypeError("endpoint must be an absolute HTTPS URL");
    }
    if (parsed.protocol !== "https:" || parsed.username.length > 0 || parsed.password.length > 0 || parsed.search.length > 0 || parsed.hash.length > 0) {
      throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment");
    }
  }

  /** Shared UTF-8 ceiling for requests, unary/error responses, and each stream event. */
  get maximumMessageBytes(): number { return this.maximumEventBytes; }

  listModels(): Promise<ListModelsResponse> {
    return this.#unary("models/list", ListModelsRequestSchema, create(ListModelsRequestSchema), ListModelsResponseSchema);
  }
  createContext(request: CreateContextRequest): Promise<MutationReceipt> {
    return this.#unary("contexts/create", CreateContextRequestSchema, request, MutationReceiptSchema);
  }
  inspectContext(request: InspectContextRequest): Promise<ContextView> {
    return this.#unary("contexts/inspect", InspectContextRequestSchema, request, ContextViewSchema);
  }
  mutateContext(request: MutateContextRequest): Promise<MutationReceipt> {
    return this.#unary("contexts/mutate", MutateContextRequestSchema, request, MutationReceiptSchema);
  }
  retainWarm(request: RetainWarmRequest): Promise<WarmView> {
    return this.#unary("warm/retain", RetainWarmRequestSchema, request, WarmViewSchema);
  }
  inspectWarm(request: InspectWarmRequest): Promise<WarmView> {
    return this.#unary("warm/inspect", InspectWarmRequestSchema, request, WarmViewSchema);
  }
  renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    return this.#unary("warm/renew", RenewWarmRequestSchema, request, WarmViewSchema);
  }
  releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    return this.#unary("warm/release", ReleaseWarmRequestSchema, request, WarmViewSchema);
  }
  generateRun(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    return this.#unary("runs/generate", GenerateRunRequestSchema, request, GenerateRunResponseSchema);
  }
  inspectRun(request: InspectRunRequest): Promise<RunView> {
    return this.#unary("runs/inspect", InspectRunRequestSchema, request, RunViewSchema);
  }
  cancelRun(request: InspectRunRequest): Promise<RunView> {
    return this.#unary("runs/cancel", InspectRunRequestSchema, request, RunViewSchema);
  }

  async *watchRun(request: WatchRunRequest, signal?: AbortSignal): AsyncIterable<RunEvent> {
    const response = await this.#request("runs/watch", toJsonString(WatchRunRequestSchema, request), signal);
    if (response.body === null) throw new InferenceTransportError(response.status, "run watch has no body");
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let completed = false;
    try {
      for (;;) {
        const item = await reader.read();
        if (item.done) break;
        buffer += decoder.decode(item.value, { stream: true });
        for (;;) {
          const newline = buffer.indexOf("\n");
          if (newline < 0) break;
          const line = buffer.slice(0, newline).trim();
          buffer = buffer.slice(newline + 1);
          if (utf8Length(line) > this.maximumMessageBytes) {
            throw new InferenceTransportError(response.status, "run event exceeds configured bound");
          }
          if (line.length > 0) yield fromJson(RunEventSchema, JSON.parse(line));
        }
        if (utf8Length(buffer) > this.maximumMessageBytes) {
          throw new InferenceTransportError(response.status, "run event exceeds configured bound");
        }
      }
      const final = `${buffer}${decoder.decode()}`.trim();
      if (utf8Length(final) > this.maximumMessageBytes) {
        throw new InferenceTransportError(response.status, "run event exceeds configured bound");
      }
      if (final.length > 0) yield fromJson(RunEventSchema, JSON.parse(final));
      completed = true;
    } finally {
      if (!completed) await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
  }

  async #unary<RequestSchema extends DescMessage, ResponseSchema extends DescMessage>(
    path: string,
    requestSchema: RequestSchema,
    request: MessageShape<RequestSchema>,
    responseSchema: ResponseSchema,
  ): Promise<MessageShape<ResponseSchema>> {
    const response = await this.#request(path, toJsonString(requestSchema, request));
    return fromJson(responseSchema, JSON.parse(await readBoundedText(response, this.maximumMessageBytes, "unary response")));
  }

  async #request(path: string, body: string, signal?: AbortSignal): Promise<Response> {
    if (utf8Length(body) > this.maximumMessageBytes) {
      throw new InferenceTransportError(0, "request exceeds configured bound");
    }
    const headers = new Headers(await this.authorization());
    if ((headers.get("authorization") ?? "").trim().length === 0) {
      throw new InferenceTransportError(0, "authorization header is required");
    }
    headers.set("content-type", "application/json");
    const response = await this.fetcher(`${this.endpoint.replace(/\/$/, "")}/v1/inference/${path}`, {
      method: "POST",
      headers,
      body,
      signal,
    });
    if (!response.ok) {
      throw new InferenceTransportError(
        response.status,
        await readBoundedText(response, this.maximumMessageBytes, "error response"),
      );
    }
    return response;
  }
}

export class InferenceTransportError extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}
