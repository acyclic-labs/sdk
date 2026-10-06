import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { InferenceProtocolError, loadInferenceWasm, validateContract, watchRunAdvance, watchRunFinish, watchRunStart } from "./contract.js";
import { MAXIMUM_HTTP_JSON_BYTES } from "../generated/defaults.js";
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
import { INFERENCE_REMOTE_POLICY } from "./generated-client.js";
import { INFERENCE_FIXED_WIDTHS } from "./widths.js";

export * from "../generated/proto/inference/v1/inference_pb.js";
export * from "./generated-client.js";
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
    requireFixed(revision, INFERENCE_FIXED_WIDTHS.inspectContextRevision, "context revision");
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
    if (request.idleKv !== undefined) await validateContract("retain_warm_request", RetainWarmRequestSchema, request);
    const view = await this.transport.retainWarm(request);
    await validateContract(request.idleKv === undefined ? "legacy_warm_context" : "idle_warm_context", WarmViewSchema, view, request.context, toBinary(RetainWarmRequestSchema, request));
    return view;
  }
  async inspectWarm(commitment: Uint8Array): Promise<WarmView> {
    requireFixed(commitment, INFERENCE_FIXED_WIDTHS.inspectWarmCommitment, "warm commitment");
    const view = await this.transport.inspectWarm(create(InspectWarmRequestSchema, { commitment }));
    await validateContract("warm_commitment", WarmViewSchema, view, commitment);
    return view;
  }
  async renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    requireFixed(request.commitment, INFERENCE_FIXED_WIDTHS.renewWarmCommitment, "warm commitment");
    if (request.idleTimeoutMs !== undefined) await validateContract("renew_warm_request", RenewWarmRequestSchema, request);
    const view = await this.transport.renewWarm(request);
    await validateContract(request.idleTimeoutMs === undefined ? "legacy_warm_commitment" : "idle_warm_commitment", WarmViewSchema, view, request.commitment, toBinary(RenewWarmRequestSchema, request));
    return view;
  }
  async releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    requireFixed(request.commitment, INFERENCE_FIXED_WIDTHS.releaseWarmCommitment, "warm commitment");
    const view = await this.transport.releaseWarm(request);
    await validateContract("warm_commitment", WarmViewSchema, view, request.commitment);
    return view;
  }
  async generate(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    if (request.identity === undefined) throw new InferenceProtocolError("generate request identity is absent");
    requireFixed(request.identity.clientInstance, INFERENCE_FIXED_WIDTHS.requestClientInstance, "generate client instance");
    requireFixed(request.identity.requestId, INFERENCE_FIXED_WIDTHS.requestId, "generate request ID");
    const response = await this.transport.generateRun(request);
    if (response.run === undefined) throw new InferenceProtocolError("generate response omitted its run");
    await validateContract("generated_run_view", RunViewSchema, response.run, request.identity.requestId, request.context);
    return response;
  }
  async inspectRun(runId: Uint8Array, signal?: AbortSignal): Promise<RunView> {
    requireFixed(runId, INFERENCE_FIXED_WIDTHS.inspectRunId, "run ID");
    const view = await this.transport.inspectRun(create(InspectRunRequestSchema, { runId }), signal);
    await validateContract("run_view", RunViewSchema, view, runId);
    return view;
  }
  async *watchRun(runId: Uint8Array, fromSequence = 0n, signal?: AbortSignal): AsyncIterable<RunEvent> {
    requireFixed(runId, INFERENCE_FIXED_WIDTHS.watchRunId, "run ID");
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
    requireFixed(runId, INFERENCE_FIXED_WIDTHS.cancelRunId, "run ID");
    const view = await this.transport.cancelRun(create(InspectRunRequestSchema, { runId }));
    await validateContract("run_view", RunViewSchema, view, runId);
    return view;
  }
  async createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    if (request.identity === undefined) throw new InferenceProtocolError("evaluation request identity is absent");
    requireFixed(request.identity.clientInstance, INFERENCE_FIXED_WIDTHS.requestClientInstance, "evaluation client instance");
    requireFixed(request.identity.requestId, INFERENCE_FIXED_WIDTHS.requestId, "evaluation request ID");
    if (request.spec === undefined) throw new InferenceProtocolError("evaluation spec is absent");
    requireFixed(request.spec.specDigest, INFERENCE_FIXED_WIDTHS.evaluationSpecDigest, "evaluation spec digest");
    await validateContract("evaluation_spec", EvaluationSpecSchema, request.spec);
    const view = await this.transport.createEvaluation(request);
    await validateContract("evaluation_view", EvaluationViewSchema, view, request.identity.requestId,
      toBinary(EvaluationSpecSchema, request.spec));
    return view;
  }
  async inspectEvaluation(evaluationId: Uint8Array): Promise<EvaluationView> {
    requireFixed(evaluationId, INFERENCE_FIXED_WIDTHS.inspectEvaluationId, "evaluation ID");
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

type RustInferenceClient = {
  listModels(request: Uint8Array): Promise<Uint8Array>;
  createContext(request: Uint8Array): Promise<Uint8Array>;
  inspectContext(request: Uint8Array): Promise<Uint8Array>;
  mutateContext(request: Uint8Array): Promise<Uint8Array>;
  retainWarm(request: Uint8Array): Promise<Uint8Array>;
  inspectWarm(request: Uint8Array): Promise<Uint8Array>;
  renewWarm(request: Uint8Array): Promise<Uint8Array>;
  releaseWarm(request: Uint8Array): Promise<Uint8Array>;
  generateRun(request: Uint8Array): Promise<Uint8Array>;
  inspectRun(request: Uint8Array): Promise<Uint8Array>;
  watchRun(request: Uint8Array): Promise<ReadonlyArray<Uint8Array>>;
  cancelRun(request: Uint8Array): Promise<Uint8Array>;
  createEvaluation(request: Uint8Array): Promise<Uint8Array>;
  inspectEvaluation(request: Uint8Array): Promise<Uint8Array>;
  transport(): string;
};

type RustInferenceModule = Awaited<ReturnType<typeof loadInferenceWasm>> & {
  BrowserInferenceClient: {
    connect(endpoint: string, token: string): Promise<RustInferenceClient>;
    connectWithLimit(endpoint: string, token: string, maximumResponseBytes: number): Promise<RustInferenceClient>;
  };
};

/** A legacy authorization callback accepted by the compatibility constructor. */
async function tokenFromAuthorization(value: string | AuthorizationHeaders): Promise<string> {
  if (typeof value === "string") return value;
  const headers = new Headers(await value());
  const authorization = headers.get("authorization");
  if (authorization?.startsWith("Bearer ")) return authorization.slice("Bearer ".length);
  throw new InferenceTransportError(0, "invalid bearer credential");
}

function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) return Promise.reject(new DOMException("The operation was aborted", "AbortError"));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new DOMException("The operation was aborted", "AbortError"));
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}

/** Rust-backed Inference transport. All route, handshake, encoding, limits, and streaming behavior execute in Rust. */
export class RustInferenceTransport implements InferenceTransport {
  readonly #client: Promise<RustInferenceClient>;

  constructor(
    readonly endpoint: string,
    readonly authorization: string | AuthorizationHeaders,
    _fetcher?: typeof fetch,
    readonly maximumEventBytes = MAXIMUM_HTTP_JSON_BYTES,
  ) {
    if (!Number.isSafeInteger(maximumEventBytes) || maximumEventBytes <= 0) {
      throw new RangeError("maximumEventBytes must be a positive safe integer byte ceiling");
    }
    this.#client = this.#connect();
  }

  get maximumMessageBytes(): number { return this.maximumEventBytes; }

  async #connect(): Promise<RustInferenceClient> {
    const module = await loadInferenceWasm() as RustInferenceModule;
    const token = await tokenFromAuthorization(this.authorization);
    return this.maximumEventBytes === MAXIMUM_HTTP_JSON_BYTES
      ? module.BrowserInferenceClient.connect(this.endpoint, token)
      : module.BrowserInferenceClient.connectWithLimit(this.endpoint, token, this.maximumEventBytes);
  }

  async #unary<T>(
    request: object,
    requestSchema: { typeName: string },
    responseSchema: { typeName: string },
    method: (client: RustInferenceClient, bytes: Uint8Array) => Promise<Uint8Array>,
    signal?: AbortSignal,
  ): Promise<T> {
    const client = await abortable(this.#client, signal);
    const result = await abortable(method(client, toBinary(requestSchema as never, request as never)), signal);
    return fromBinary(responseSchema as never, result) as T;
  }

  listModels(): Promise<ListModelsResponse> {
    return this.#unary(create(ListModelsRequestSchema), ListModelsRequestSchema, ListModelsResponseSchema, (client, bytes) => client.listModels(bytes));
  }
  createContext(request: CreateContextRequest): Promise<MutationReceipt> {
    return this.#unary(request, CreateContextRequestSchema, MutationReceiptSchema, (client, bytes) => client.createContext(bytes));
  }
  inspectContext(request: InspectContextRequest): Promise<ContextView> {
    return this.#unary(request, InspectContextRequestSchema, ContextViewSchema, (client, bytes) => client.inspectContext(bytes));
  }
  mutateContext(request: MutateContextRequest): Promise<MutationReceipt> {
    return this.#unary(request, MutateContextRequestSchema, MutationReceiptSchema, (client, bytes) => client.mutateContext(bytes));
  }
  retainWarm(request: RetainWarmRequest): Promise<WarmView> {
    return this.#unary(request, RetainWarmRequestSchema, WarmViewSchema, (client, bytes) => client.retainWarm(bytes));
  }
  inspectWarm(request: InspectWarmRequest): Promise<WarmView> {
    return this.#unary(request, InspectWarmRequestSchema, WarmViewSchema, (client, bytes) => client.inspectWarm(bytes));
  }
  renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    return this.#unary(request, RenewWarmRequestSchema, WarmViewSchema, (client, bytes) => client.renewWarm(bytes));
  }
  releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    return this.#unary(request, ReleaseWarmRequestSchema, WarmViewSchema, (client, bytes) => client.releaseWarm(bytes));
  }
  generateRun(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    return this.#unary(request, GenerateRunRequestSchema, GenerateRunResponseSchema, (client, bytes) => client.generateRun(bytes));
  }
  inspectRun(request: InspectRunRequest, signal?: AbortSignal): Promise<RunView> {
    return this.#unary(request, InspectRunRequestSchema, RunViewSchema, (client, bytes) => client.inspectRun(bytes), signal);
  }
  async *watchRun(request: WatchRunRequest, signal?: AbortSignal): AsyncIterable<RunEvent> {
    const client = await abortable(this.#client, signal);
    const events = await abortable(client.watchRun(toBinary(WatchRunRequestSchema, request)), signal);
    for (const event of events) {
      if (signal?.aborted) return;
      yield fromBinary(RunEventSchema, event);
    }
  }
  cancelRun(request: InspectRunRequest): Promise<RunView> {
    return this.#unary(request, InspectRunRequestSchema, RunViewSchema, (client, bytes) => client.cancelRun(bytes));
  }
  createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    return this.#unary(request, CreateEvaluationRequestSchema, EvaluationViewSchema, (client, bytes) => client.createEvaluation(bytes));
  }
  inspectEvaluation(request: InspectEvaluationRequest): Promise<EvaluationView> {
    return this.#unary(request, InspectEvaluationRequestSchema, EvaluationViewSchema, (client, bytes) => client.inspectEvaluation(bytes));
  }
}

/** Backwards-compatible name; implementation is entirely Rust-backed. */
export class HttpInferenceTransport extends RustInferenceTransport {}

export class InferenceTransportError extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}

/** The transport kinds exposed by the Rust-qualified Inference policy. */
export type InferenceTransportKind = typeof INFERENCE_REMOTE_POLICY.transport.native[number]["kind"];

/** Endpoint and credential settings for the generated remote facade. */
export interface InferenceEnvironment {
  readonly endpoint: string;
  readonly token: string;
  /** Deprecated compatibility field. Rust selects the best compatible transport. */
  readonly transport?: InferenceTransportKind;
  readonly fetcher?: typeof fetch;
  readonly maximumEventBytes?: number;
}

/**
 * Construct the Rust-qualified remote Inference facade.
 *
 * The published adapter is HTTP JSON/NDJSON in both runtimes. An explicit
 * unavailable override fails before endpoint parsing or a request is sent.
 */
export function fromEnv(environment: InferenceEnvironment): InferenceClient {
  void environment.transport;
  void INFERENCE_REMOTE_POLICY;
  return new InferenceClient(new RustInferenceTransport(
    environment.endpoint,
    environment.token,
    environment.fetcher ?? globalThis.fetch.bind(globalThis),
    environment.maximumEventBytes,
  ));
}

export * from "./handles.js";
export * from "./generated-client.js";
