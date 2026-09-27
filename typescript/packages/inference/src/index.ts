import { create, fromJson, getOption, hasOption, toBinary, toJsonString, type DescMethod, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { InferenceProtocolError, validateContract, validateRuntimeShape, watchRunAdvance, watchRunFinish, watchRunStart } from "./contract.js";
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
import {
  EvaluationsService,
  file_inference_v1_inference,
  ModelsService,
  ContextsService,
  RunsService,
  WarmContextsService,
} from "../generated/proto/inference/v1/inference_pb.js";
import { http_path } from "../generated/proto/validation/v1/options_pb.js";

export * from "../generated/proto/inference/v1/inference_pb.js";
export { InferenceProtocolError } from "./contract.js";

/** One authenticated HTTP endpoint derived from a protobuf RPC descriptor. */
export interface InferenceHttpRoute<Method extends DescMethod = DescMethod> {
  readonly method: Method;
  readonly path: string;
  readonly input: Method["input"];
  readonly output: Method["output"];
  readonly methodKind: Method["methodKind"];
}

const inferenceServices = file_inference_v1_inference.services;

/** Reject paths that could change the /v1/inference URL root or request target. */
export function validateInferenceHttpPath(path: string): void {
  const segments = path.split("/");
  if (path.length === 0 || path !== path.trim() || path.startsWith("/") || path.endsWith("/") ||
      segments.some(segment => segment.length === 0 || segment === "." || segment === ".." || !/^[A-Za-z0-9._~-]+$/.test(segment))) {
    throw new Error("inference HTTP route path is invalid");
  }
}

/**
 * Derive every public inference endpoint from the canonical service descriptors.
 * Missing, malformed, or duplicate options stop module initialization so a
 * handwritten route cannot silently diverge from the protobuf contract.
 */
export function deriveInferenceHttpRoutes(): readonly InferenceHttpRoute[] {
  const routes: InferenceHttpRoute[] = [];
  const paths = new Map<string, string>();
  for (const service of inferenceServices) {
    for (const method of service.methods) {
      if (!hasOption(method, http_path)) {
        throw new Error(`inference RPC ${method.parent.typeName}.${method.name} has no http_path option`);
      }
      const path = getOption(method, http_path);
      try {
        validateInferenceHttpPath(path);
      } catch {
        throw new Error(`inference RPC ${method.parent.typeName}.${method.name} has an invalid http_path`);
      }
      const prior = paths.get(path);
      if (prior !== undefined) {
        throw new Error(`inference RPC route ${path} is declared by both ${prior} and ${method.parent.typeName}.${method.name}`);
      }
      paths.set(path, `${method.parent.typeName}.${method.name}`);
      routes.push({ method, path, input: method.input, output: method.output, methodKind: method.methodKind });
    }
  }
  if (routes.length === 0) throw new Error("inference protobuf declares no HTTP routes");
  return Object.freeze(routes);
}

const inferenceHttpRoutes = deriveInferenceHttpRoutes();
const routeFor = <Method extends DescMethod>(method: Method): InferenceHttpRoute<Method> => {
  const route = inferenceHttpRoutes.find(candidate =>
    candidate.method.parent.typeName === method.parent.typeName && candidate.method.name === method.name);
  if (route === undefined) throw new Error(`inference RPC descriptor ${method.parent.typeName}.${method.name} has no derived route`);
  return route as InferenceHttpRoute<Method>;
};

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
  inspectRun(request: InspectRunRequest, signal?: AbortSignal): Promise<RunView>;
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
  async inspectRun(runId: Uint8Array, signal?: AbortSignal): Promise<RunView> {
    requireFixed(runId, 16, "run ID");
    const view = await this.transport.inspectRun(create(InspectRunRequestSchema, { runId }), signal);
    await validateContract("run_view", RunViewSchema, view, runId);
    return view;
  }
  async *watchRun(runId: Uint8Array, fromSequence = 0n, signal?: AbortSignal): AsyncIterable<RunEvent> {
    requireFixed(runId, 16, "run ID");
    if (fromSequence < 0n) throw new InferenceProtocolError("run cursor must be non-negative");
    const view = await this.inspectRun(runId, signal);
    const state = await watchRunStart(toBinary(RunViewSchema, view), runId, fromSequence);
    try {
      if (state.terminal) return;
      for await (const event of this.transport.watchRun(create(WatchRunRequestSchema, { runId, fromSequence }), signal)) {
        watchRunAdvance(state, event);
        yield event;
      }
      watchRunFinish(state);
    } finally {
      state.free();
    }
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
    return this.#unary(routeFor(ModelsService.method.list), create(ModelsService.method.list.input));
  }
  createContext(request: CreateContextRequest): Promise<MutationReceipt> {
    return this.#unary(routeFor(ContextsService.method.create), request);
  }
  inspectContext(request: InspectContextRequest): Promise<ContextView> {
    return this.#unary(routeFor(ContextsService.method.inspect), request);
  }
  mutateContext(request: MutateContextRequest): Promise<MutationReceipt> {
    return this.#unary(routeFor(ContextsService.method.mutate), request);
  }
  retainWarm(request: RetainWarmRequest): Promise<WarmView> {
    return this.#unary(routeFor(WarmContextsService.method.retain), request);
  }
  inspectWarm(request: InspectWarmRequest): Promise<WarmView> {
    return this.#unary(routeFor(WarmContextsService.method.inspect), request);
  }
  renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    return this.#unary(routeFor(WarmContextsService.method.renew), request);
  }
  releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    return this.#unary(routeFor(WarmContextsService.method.release), request);
  }
  generateRun(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    return this.#unary(routeFor(RunsService.method.generate), request);
  }
  inspectRun(request: InspectRunRequest, signal?: AbortSignal): Promise<RunView> {
    return this.#unary(routeFor(RunsService.method.inspect), request, signal);
  }
  cancelRun(request: InspectRunRequest): Promise<RunView> {
    return this.#unary(routeFor(RunsService.method.cancel), request);
  }
  createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    return this.#unary(routeFor(EvaluationsService.method.create), request);
  }
  inspectEvaluation(request: InspectEvaluationRequest): Promise<EvaluationView> {
    return this.#unary(routeFor(EvaluationsService.method.inspect), request);
  }

  async *watchRun(request: WatchRunRequest, signal?: AbortSignal): AsyncIterable<RunEvent> {
    const route = routeFor(RunsService.method.watch);
    if (route.methodKind !== "server_streaming") throw new Error("inference watch route is not server streaming");
    await validateRuntimeShape(route.input, request);
    const response = await this.#request(route.path, toJsonString(route.input, request), signal);
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
          if (line.length > 0) yield fromJson(route.output, JSON.parse(line));
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
      if (final.length > 0) yield fromJson(route.output, JSON.parse(final));
      completed = true;
    } finally {
      if (!completed) await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
  }

  async #unary<Method extends DescMethod>(
    route: InferenceHttpRoute<Method>,
    request: MessageShape<Method["input"]>,
    signal?: AbortSignal,
  ): Promise<MessageShape<Method["output"]>> {
    if (route.methodKind !== "unary") throw new Error(`inference route ${route.path} is not unary`);
    await validateRuntimeShape(route.input, request);
    const response = await this.#request(route.path, toJsonString(route.input, request), signal);
    return fromJson(route.output, JSON.parse(await readBoundedText(response, this.maximumMessageBytes, "unary response")));
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
