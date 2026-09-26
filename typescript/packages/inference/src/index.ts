import { create, fromJson, toBinary, toJsonString, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { InferenceProtocolError, validateContract } from "./contract.js";
import {
  ContextViewSchema,
  CreateEvaluationRequestSchema,
  CreateContextRequestSchema,
  EvaluationSpecSchema,
  EvaluationViewSchema,
  GenerateRunRequestSchema,
  GenerateRunResponseSchema,
  InspectContextRequestSchema,
  InspectEvaluationRequestSchema,
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
  type CreateEvaluationRequest,
  type CreateContextRequest,
  type GenerateRunRequest,
  type GenerateRunResponse,
  type EvaluationView,
  type InspectContextRequest,
  type InspectEvaluationRequest,
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
export { InferenceProtocolError } from "./contract.js";

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
  createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView>;
  inspectEvaluation(request: InspectEvaluationRequest): Promise<EvaluationView>;
}

/** Generated-contract client covering contexts, edits, compaction, warm state, and recoverable runs. */
export class InferenceClient {
  constructor(readonly transport: InferenceTransport) {}

  listModels(): Promise<ListModelsResponse> { return this.transport.listModels(); }
  async createContext(request: CreateContextRequest): Promise<MutationReceipt> {
    const receipt = await this.transport.createContext(request);
    await validateContract("mutation_receipt", MutationReceiptSchema, receipt);
    return receipt;
  }
  async inspectContext(revision: Uint8Array): Promise<ContextView> {
    requireFixed(revision, 32, "context revision");
    const view = await this.transport.inspectContext(create(InspectContextRequestSchema, { revision }));
    await validateContract("context_view", ContextViewSchema, view, revision);
    return view;
  }
  async mutateContext(request: MutateContextRequest): Promise<MutationReceipt> {
    const receipt = await this.transport.mutateContext(request);
    await validateContract("mutation_receipt", MutationReceiptSchema, receipt);
    return receipt;
  }
  async retainWarm(request: RetainWarmRequest): Promise<WarmView> {
    const view = await this.transport.retainWarm(request);
    await validateContract("warm_context", WarmViewSchema, view, request.context);
    return view;
  }
  async inspectWarm(commitment: Uint8Array): Promise<WarmView> {
    requireFixed(commitment, 32, "warm commitment");
    const view = await this.transport.inspectWarm(create(InspectWarmRequestSchema, { commitment }));
    await validateContract("warm_commitment", WarmViewSchema, view, commitment);
    return view;
  }
  async renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    const view = await this.transport.renewWarm(request);
    await validateContract("warm_commitment", WarmViewSchema, view, request.commitment);
    return view;
  }
  async releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    const view = await this.transport.releaseWarm(request);
    await validateContract("warm_commitment", WarmViewSchema, view, request.commitment);
    return view;
  }
  async generate(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    if (request.identity === undefined) throw new InferenceProtocolError("generate request identity is absent");
    requireFixed(request.identity.clientInstance, 16, "generate client instance");
    requireFixed(request.identity.requestId, 16, "generate request ID");
    const response = await this.transport.generateRun(request);
    if (response.run === undefined) throw new InferenceProtocolError("generate response omitted its run");
    await validateContract("generated_run_view", RunViewSchema, response.run, request.identity.requestId, request.context);
    return response;
  }
  async inspectRun(runId: Uint8Array): Promise<RunView> {
    requireFixed(runId, 16, "run ID");
    const view = await this.transport.inspectRun(create(InspectRunRequestSchema, { runId }));
    await validateContract("run_view", RunViewSchema, view, runId);
    return view;
  }
  async *watchRun(runId: Uint8Array, fromSequence = 0n, signal?: AbortSignal): AsyncIterable<RunEvent> {
    requireFixed(runId, 16, "run ID");
    if (fromSequence < 0n) throw new InferenceProtocolError("run cursor must be non-negative");
    let expected = fromSequence;
    let terminal = false;
    for await (const event of this.transport.watchRun(create(WatchRunRequestSchema, { runId, fromSequence }), signal)) {
      if (terminal || event.sequence !== expected) {
        throw new InferenceProtocolError("run event order or shape differs");
      }
      await validateContract("run_event", RunEventSchema, event);
      expected += 1n;
      if (event.event.case === "terminal") {
        terminal = true;
      }
      yield event;
    }
    if (!terminal) throw new InferenceProtocolError("run stream ended before terminal");
  }
  async cancelRun(runId: Uint8Array): Promise<RunView> {
    requireFixed(runId, 16, "run ID");
    const view = await this.transport.cancelRun(create(InspectRunRequestSchema, { runId }));
    await validateContract("run_view", RunViewSchema, view, runId);
    return view;
  }
  async createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    if (request.identity === undefined) throw new InferenceProtocolError("evaluation request identity is absent");
    requireFixed(request.identity.clientInstance, 16, "evaluation client instance");
    requireFixed(request.identity.requestId, 16, "evaluation request ID");
    if (request.spec === undefined) throw new InferenceProtocolError("evaluation spec is absent");
    requireFixed(request.spec.specDigest, 32, "evaluation spec digest");
    await validateContract("evaluation_spec", EvaluationSpecSchema, request.spec);
    const view = await this.transport.createEvaluation(request);
    await validateContract("evaluation_view", EvaluationViewSchema, view, request.identity.requestId,
      toBinary(EvaluationSpecSchema, request.spec));
    return view;
  }
  async inspectEvaluation(evaluationId: Uint8Array): Promise<EvaluationView> {
    requireFixed(evaluationId, 16, "evaluation ID");
    const view = await this.transport.inspectEvaluation(create(InspectEvaluationRequestSchema, { evaluationId }));
    await validateContract("evaluation_view", EvaluationViewSchema, view, evaluationId);
    return view;
  }
}

function requireFixed(value: Uint8Array, length: number, name: string): void {
  if (!(value instanceof Uint8Array) || value.byteLength !== length) {
    throw new InferenceProtocolError(`${name} must be exactly ${length} bytes`);
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
    if (parsed.protocol !== "https:" || parsed.username.length > 0 || parsed.password.length > 0 || /[?#]/.test(endpoint)) {
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
  createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    return this.#unary("evaluations/create", CreateEvaluationRequestSchema, request, EvaluationViewSchema);
  }
  inspectEvaluation(request: InspectEvaluationRequest): Promise<EvaluationView> {
    return this.#unary("evaluations/inspect", InspectEvaluationRequestSchema, request, EvaluationViewSchema);
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
          const rawLine = buffer.slice(0, newline);
          buffer = buffer.slice(newline + 1);
          if (utf8Length(rawLine) > this.maximumMessageBytes) {
            throw new InferenceTransportError(response.status, "run event exceeds configured bound");
          }
          const line = rawLine.trim();
          if (line.length > 0) yield fromJson(RunEventSchema, JSON.parse(line));
        }
        if (utf8Length(buffer) > this.maximumMessageBytes) {
          throw new InferenceTransportError(response.status, "run event exceeds configured bound");
        }
      }
      const rawFinal = `${buffer}${decoder.decode()}`;
      if (utf8Length(rawFinal) > this.maximumMessageBytes) {
        throw new InferenceTransportError(response.status, "run event exceeds configured bound");
      }
      const final = rawFinal.trim();
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

export * from "./handles.js";
