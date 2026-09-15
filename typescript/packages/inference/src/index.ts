import { create, fromJson, toJsonString, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import {
  ContextViewSchema,
  CreateEvaluationRequestSchema,
  CreateContextRequestSchema,
  EvaluationCaseOutcome,
  EvaluationState,
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
  RunTerminal,
  WarmState,
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
    validateMutationReceipt(receipt);
    return receipt;
  }
  async inspectContext(revision: Uint8Array): Promise<ContextView> {
    requireFixed(revision, 32, "context revision");
    const view = await this.transport.inspectContext(create(InspectContextRequestSchema, { revision }));
    validateContextView(view, revision);
    return view;
  }
  async mutateContext(request: MutateContextRequest): Promise<MutationReceipt> {
    const receipt = await this.transport.mutateContext(request);
    validateMutationReceipt(receipt);
    return receipt;
  }
  async retainWarm(request: RetainWarmRequest): Promise<WarmView> {
    const view = await this.transport.retainWarm(request);
    validateWarmView(view, request.context, undefined);
    return view;
  }
  async inspectWarm(commitment: Uint8Array): Promise<WarmView> {
    requireFixed(commitment, 32, "warm commitment");
    const view = await this.transport.inspectWarm(create(InspectWarmRequestSchema, { commitment }));
    validateWarmView(view, undefined, commitment);
    return view;
  }
  async renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    const view = await this.transport.renewWarm(request);
    validateWarmView(view, undefined, request.commitment);
    return view;
  }
  async releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    const view = await this.transport.releaseWarm(request);
    validateWarmView(view, undefined, request.commitment);
    return view;
  }
  async generate(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    if (request.identity === undefined) throw new InferenceProtocolError("generate request identity is absent");
    requireFixed(request.identity.clientInstance, 16, "generate client instance");
    requireFixed(request.identity.requestId, 16, "generate request ID");
    const response = await this.transport.generateRun(request);
    if (response.run === undefined) throw new InferenceProtocolError("generate response omitted its run");
    validateRunView(response.run, request.identity.requestId, request.context);
    return response;
  }
  async inspectRun(runId: Uint8Array): Promise<RunView> {
    requireFixed(runId, 16, "run ID");
    const view = await this.transport.inspectRun(create(InspectRunRequestSchema, { runId }));
    validateRunView(view, runId);
    return view;
  }
  async *watchRun(runId: Uint8Array, fromSequence = 0n, signal?: AbortSignal): AsyncIterable<RunEvent> {
    requireFixed(runId, 16, "run ID");
    if (fromSequence < 0n) throw new InferenceProtocolError("run cursor must be non-negative");
    let expected = fromSequence;
    let terminal = false;
    for await (const event of this.transport.watchRun(create(WatchRunRequestSchema, { runId, fromSequence }), signal)) {
      if (terminal || event.sequence !== expected || event.event.case === undefined) {
        throw new InferenceProtocolError("run event order or shape differs");
      }
      expected += 1n;
      if (event.event.case === "terminal") {
        requireTerminal(event.event.value);
        terminal = true;
      }
      yield event;
    }
    if (!terminal) throw new InferenceProtocolError("run stream ended before terminal");
  }
  async cancelRun(runId: Uint8Array): Promise<RunView> {
    requireFixed(runId, 16, "run ID");
    const view = await this.transport.cancelRun(create(InspectRunRequestSchema, { runId }));
    validateRunView(view, runId);
    return view;
  }
  async createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    if (request.identity === undefined) throw new InferenceProtocolError("evaluation request identity is absent");
    requireFixed(request.identity.clientInstance, 16, "evaluation client instance");
    requireFixed(request.identity.requestId, 16, "evaluation request ID");
    if (request.spec === undefined) throw new InferenceProtocolError("evaluation spec is absent");
    requireFixed(request.spec.specDigest, 32, "evaluation spec digest");
    const view = await this.transport.createEvaluation(request);
    validateEvaluationView(view, request.identity.requestId, request.spec.specDigest);
    return view;
  }
  async inspectEvaluation(evaluationId: Uint8Array): Promise<EvaluationView> {
    requireFixed(evaluationId, 16, "evaluation ID");
    const view = await this.transport.inspectEvaluation(create(InspectEvaluationRequestSchema, { evaluationId }));
    validateEvaluationView(view, evaluationId);
    return view;
  }
}

export class InferenceProtocolError extends Error {}

function validateRunView(view: RunView, expectedRunId?: Uint8Array, expectedInput?: Uint8Array): void {
  requireFixed(view.runId, 16, "run ID");
  requireFixed(view.input, 32, "run input revision");
  if (expectedRunId !== undefined && !equalBytes(view.runId, expectedRunId)) {
    throw new InferenceProtocolError("run identity differs from the request");
  }
  if (expectedInput !== undefined && !equalBytes(view.input, expectedInput)) {
    throw new InferenceProtocolError("run input differs from the request");
  }
  if (view.model.length === 0 || view.model.length > 256) throw new InferenceProtocolError("run model is invalid");
  if (view.result !== undefined) validateRunResult(view.result);
}

function validateMutationReceipt(receipt: MutationReceipt): void {
  requireFixed(receipt.revision, 32, "mutation revision");
  requireFixed(receipt.commandDigest, 32, "mutation command digest");
  if (receipt.sequence === 0n) throw new InferenceProtocolError("mutation publication sequence is absent");
}

function validateContextView(view: ContextView, expectedRevision: Uint8Array): void {
  requireFixed(view.revision, 32, "context revision");
  if (!equalBytes(view.revision, expectedRevision)) throw new InferenceProtocolError("context revision differs from the request");
  requireFixed(view.lineage, 32, "context lineage");
  requireFixed(view.executionProfile, 32, "context execution profile");
  requireFixed(view.contentDigest, 32, "context content digest");
  if (view.parent !== undefined) requireFixed(view.parent, 32, "context parent");
  if (view.model.length === 0 || view.model.length > 256) throw new InferenceProtocolError("context model is invalid");
  if (view.provenance === undefined || view.provenance.origin.case === undefined) {
    throw new InferenceProtocolError("context provenance is absent");
  }
  const origin = view.provenance.origin;
  if (origin.case === "derived" || origin.case === "forked" || origin.case === "transferred") {
    requireFixed(origin.value.source, 32, "context provenance source");
  } else if (origin.case === "generated") {
    requireFixed(origin.value.runId, 16, "context provenance run ID");
    requireFixed(origin.value.terminalReceiptDigest, 32, "context terminal receipt digest");
  } else if (origin.case === "runInput") {
    requireFixed(origin.value.source, 32, "context provenance source");
    requireFixed(origin.value.runId, 16, "context provenance run ID");
    if (origin.value.maximumOutput === 0n) throw new InferenceProtocolError("run input output bound is zero");
  }
}

function validateWarmView(view: WarmView, expectedContext?: Uint8Array, expectedCommitment?: Uint8Array): void {
  requireFixed(view.commitment, 32, "warm commitment");
  requireFixed(view.context, 32, "warm context");
  requireFixed(view.modelProfile, 32, "warm model profile");
  requireFixed(view.latencyProfile, 32, "warm latency profile");
  requireFixed(view.evidenceDigest, 32, "warm evidence digest");
  requireFixed(view.admissionReceiptId, 32, "warm admission receipt ID");
  if (expectedContext !== undefined && !equalBytes(view.context, expectedContext)) throw new InferenceProtocolError("warm context differs from the request");
  if (expectedCommitment !== undefined && !equalBytes(view.commitment, expectedCommitment)) throw new InferenceProtocolError("warm commitment differs from the request");
  if (view.expiresAtMs === 0n || view.sequence === 0n || view.state === WarmState.UNSPECIFIED || !Object.values(WarmState).includes(view.state)) {
    throw new InferenceProtocolError("warm commitment shape differs");
  }
}

function validateRunResult(result: NonNullable<RunView["result"]>): void {
  requireTerminal(result.terminal);
  if (result.context !== undefined) requireFixed(result.context.revision, 32, "continuation revision");
  if (result.receipt !== undefined) {
    requireFixed(result.receipt.receiptId, 32, "receipt ID");
    requireFixed(result.receipt.modelProfile, 32, "receipt model profile");
    requireFixed(result.receipt.meterRevision, 32, "receipt meter revision");
    requireFixed(result.receipt.rateCardRevision, 32, "receipt rate-card revision");
    if (result.receipt.usage === undefined) throw new InferenceProtocolError("run receipt usage is absent");
  }
}

function validateEvaluationView(view: EvaluationView, expectedId: Uint8Array, expectedSpecDigest?: Uint8Array): void {
  requireFixed(view.evaluationId, 16, "evaluation ID");
  if (!equalBytes(view.evaluationId, expectedId)) throw new InferenceProtocolError("evaluation identity differs from the request");
  if (view.spec === undefined) throw new InferenceProtocolError("evaluation spec is absent");
  requireFixed(view.spec.specDigest, 32, "evaluation spec digest");
  if (expectedSpecDigest !== undefined && !equalBytes(view.spec.specDigest, expectedSpecDigest)) {
    throw new InferenceProtocolError("evaluation spec differs from the request");
  }
  if (view.sequence === 0n || view.state === EvaluationState.UNSPECIFIED || !Object.values(EvaluationState).includes(view.state)) {
    throw new InferenceProtocolError("evaluation state is invalid");
  }
  if ((view.state === EvaluationState.COMPLETED) !== (view.result !== undefined)) {
    throw new InferenceProtocolError("evaluation state is inconsistent with its result");
  }
  if (view.result !== undefined) validateEvaluationResult(view.spec, view.result);
}

function validateEvaluationResult(
  spec: NonNullable<EvaluationView["spec"]>,
  result: NonNullable<EvaluationView["result"]>,
): void {
  requireFixed(result.specDigest, 32, "evaluation result spec digest");
  requireFixed(result.resultDigest, 32, "evaluation result digest");
  if (!equalBytes(result.specDigest, spec.specDigest)) {
    throw new InferenceProtocolError("evaluation result differs from the admitted spec");
  }
  if (result.caseResults.length === 0 || BigInt(result.caseResults.length) !== spec.maximumCaseResults) {
    throw new InferenceProtocolError("evaluation result coverage is invalid");
  }
  const suite = spec.suite;
  if (suite === undefined) throw new InferenceProtocolError("evaluation suite is absent");
  const candidates = new Set(spec.candidates.map(candidate => bytesKey(candidate.digest)));
  const cases = new Set(suite.cases.map(item => bytesKey(item.caseId)));
  const metrics = new Set(spec.metrics.map(metric => metric.identity));
  const observations = new Set<string>();
  for (const item of result.caseResults) {
    requireFixed(item.candidateDigest, 32, "evaluation candidate digest");
    requireFixed(item.caseId, 16, "evaluation case ID");
    const observation = item.observation;
    if (observation === undefined) throw new InferenceProtocolError("evaluation grader observation is absent");
    requireFixed(observation.nativeOutputDigest, 32, "evaluation native output digest");
    requireFixed(observation.observationDigest, 32, "evaluation observation digest");
    requireFixed(observation.bindingDigest, 32, "evaluation observation binding digest");
    const observationKey = `${bytesKey(item.candidateDigest)}:${bytesKey(item.caseId)}`;
    if (!candidates.has(bytesKey(item.candidateDigest)) || !cases.has(bytesKey(item.caseId)) || observations.has(observationKey)
      || item.outcome === EvaluationCaseOutcome.UNSPECIFIED || !Object.values(EvaluationCaseOutcome).includes(item.outcome)) {
      throw new InferenceProtocolError("evaluation case result is invalid");
    }
    observations.add(observationKey);
    const observedMetrics = new Set<string>();
    for (const metric of item.metrics) {
      if (!metrics.has(metric.metricIdentity) || observedMetrics.has(metric.metricIdentity) || metric.value === undefined || metric.value.denominator === 0n) {
        throw new InferenceProtocolError("evaluation case metric is invalid");
      }
      observedMetrics.add(metric.metricIdentity);
    }
    if ((item.outcome === EvaluationCaseOutcome.SCORED && observedMetrics.size !== metrics.size)
      || (item.outcome !== EvaluationCaseOutcome.SCORED && observedMetrics.size !== 0)) {
      throw new InferenceProtocolError("evaluation case metric coverage is invalid");
    }
  }
  const expectedAggregates = spec.candidates.length * spec.metrics.length;
  const aggregates = new Set<string>();
  const metricAggregations = new Map(spec.metrics.map(metric => [metric.identity, metric.aggregation]));
  for (const aggregate of result.aggregates) {
    requireFixed(aggregate.candidateDigest, 32, "evaluation aggregate candidate digest");
    const candidate = bytesKey(aggregate.candidateDigest);
    const aggregation = metricAggregations.get(aggregate.metricIdentity);
    const key = `${candidate}:${aggregate.metricIdentity}`;
    if (!candidates.has(candidate) || aggregation === undefined || aggregation !== aggregate.aggregation
      || aggregates.has(key) || aggregate.value === undefined || aggregate.value.denominator === 0n) {
      throw new InferenceProtocolError("evaluation aggregate is invalid");
    }
    aggregates.add(key);
  }
  if (aggregates.size !== expectedAggregates) {
    throw new InferenceProtocolError("evaluation aggregate coverage is invalid");
  }
}

function bytesKey(value: Uint8Array): string {
  return Array.from(value, byte => byte.toString(16).padStart(2, "0")).join("");
}

function requireTerminal(value: RunTerminal): void {
  if (value === RunTerminal.UNSPECIFIED || !Object.values(RunTerminal).includes(value)) {
    throw new InferenceProtocolError("run terminal is invalid");
  }
}

function requireFixed(value: Uint8Array, length: number, name: string): void {
  if (!(value instanceof Uint8Array) || value.byteLength !== length) {
    throw new InferenceProtocolError(`${name} must be exactly ${length} bytes`);
  }
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((value, index) => value === right[index]);
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
