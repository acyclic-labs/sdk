import { Buffer } from "node:buffer";
import { arch, platform, report } from "node:process";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ContextViewSchema,
  CreateEvaluationRequestSchema,
  CreateContextRequestSchema,
  EvaluationViewSchema,
  GenerateRunResponseSchema,
  GenerateRunRequestSchema,
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
  WatchRunRequestSchema,
  type ContextView,
  type CreateEvaluationRequest,
  type CreateContextRequest,
  type EvaluationView,
  type GenerateRunRequest,
  type GenerateRunResponse,
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
  type WatchRunRequest,
  type WarmView,
  WarmViewSchema,
} from "../generated/proto/inference/v1/inference_pb.js";
import { MAXIMUM_HTTP_JSON_BYTES } from "../generated/defaults.js";
import { INFERENCE_NATIVE_COMPANION_TARGETS, awaitWithAbort } from "./generated-client.js";
import type { InferenceTransport } from "./index.js";

export interface NativeInferenceOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumEventBytes?: number;
}

type NativeInferenceClient = {
  listModels(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  createContext(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  inspectContext(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  mutateContext(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  retainWarm(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  inspectWarm(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  renewWarm(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  releaseWarm(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  generateRun(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  inspectRun(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  watchRun(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<ReadonlyArray<Uint8Array>>;
  cancelRun(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  createEvaluation(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
  inspectEvaluation(request: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<Uint8Array>;
};

type NativeInferenceCancellation = {
  cancel(): void;
  readonly cancelled: boolean;
};

interface NativeInferenceModule {
  readonly NativeInferenceCancellation: new () => NativeInferenceCancellation;
  readonly NativeInferenceClient: {
    connect(endpoint: string, token: string, cancellation?: NativeInferenceCancellation): Promise<NativeInferenceClient>;
    connectWithLimit(endpoint: string, token: string, maximumResponseBytes: number, cancellation?: NativeInferenceCancellation): Promise<NativeInferenceClient>;
    connectWithCa(endpoint: string, token: string, caCertificatePem: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<NativeInferenceClient>;
    connectWithLimitAndCa(endpoint: string, token: string, maximumResponseBytes: number, caCertificatePem: Uint8Array, cancellation?: NativeInferenceCancellation): Promise<NativeInferenceClient>;
  };
}

class NativeCompanionUnavailable extends Error {
  constructor(message: string, options?: { readonly cause?: unknown }) {
    super(message, options);
    this.name = "NativeCompanionUnavailable";
  }
}

let bindingPromise: Promise<NativeInferenceModule> | undefined;

type NativeProcessReport = {
  readonly getReport?: () => {
    readonly header?: {
      readonly arch?: unknown;
      readonly glibcVersionRuntime?: unknown;
      readonly nodejsVersion?: unknown;
      readonly platform?: unknown;
      readonly reportVersion?: unknown;
    };
  };
};

function reportLibc(currentReport: NativeProcessReport | undefined): "gnu" | "musl" | undefined {
  const getReport = currentReport?.getReport;
  if (typeof getReport !== "function") return undefined;
  try {
    const header = getReport().header;
    if (header === undefined || typeof header !== "object") return undefined;
    // An empty or synthetic report is not enough evidence to claim musl.
    // Node's diagnostic report always identifies the runtime in its header.
    const validHeader = "arch" in header || "nodejsVersion" in header || "platform" in header || "reportVersion" in header;
    if (!validHeader) return undefined;
    return typeof header.glibcVersionRuntime === "string" ? "gnu" : "musl";
  } catch {
    return undefined;
  }
}

/** Resolve the published native companion from the host ABI and libc. */
export function nativeCompanionTarget(
  currentPlatform: string = platform,
  currentArch: string = arch,
  currentReport: NativeProcessReport | undefined = report,
): string {
  const base = `${currentPlatform}-${currentArch}`;
  if (currentPlatform !== "linux") return base;
  const libc = reportLibc(currentReport) ?? "gnu";
  return `${base}-${libc}`;
}

function isAdapterLoadError(error: unknown): boolean {
  if (error === null || typeof error !== "object") return false;
  const candidate = error as { readonly code?: unknown; readonly message?: unknown };
  if (candidate.code === "ERR_MODULE_NOT_FOUND" || candidate.code === "MODULE_NOT_FOUND" || candidate.code === "ERR_DLOPEN_FAILED" || candidate.code === "DLOPEN_FAILED") return true;
  return typeof candidate.message === "string" && (/native companion did not export/i.test(candidate.message) || /failed to load native (?:companion|module)/i.test(candidate.message));
}

async function binding(): Promise<NativeInferenceModule> {
  const target = nativeCompanionTarget();
  if (!(INFERENCE_NATIVE_COMPANION_TARGETS as readonly string[]).includes(target)) {
    throw new NativeCompanionUnavailable(`@acyclic-labs/inference has no native companion for ${target}`);
  }
  bindingPromise ??= import(`@acyclic-labs/inference-${target}`).then((module) => {
    const namespace = module as NativeInferenceModule & { readonly default?: NativeInferenceModule };
    const candidate = namespace.NativeInferenceClient === undefined ? namespace.default : namespace;
    if (candidate?.NativeInferenceClient === undefined || typeof candidate.NativeInferenceCancellation !== "function") {
      throw new NativeCompanionUnavailable("native companion did not export the Inference bridge");
    }
    return candidate;
  }).catch((error: unknown) => {
    bindingPromise = undefined;
    if (error instanceof NativeCompanionUnavailable) throw error;
    if (isAdapterLoadError(error)) throw new NativeCompanionUnavailable("failed to load native Inference companion", { cause: error });
    throw error;
  });
  return bindingPromise;
}

/** Exposes adapter availability classification to the transport fallback. */
export function isNativeInferenceUnavailable(error: unknown): boolean {
  return error instanceof NativeCompanionUnavailable;
}

type CancellationRegistration = {
  readonly handle: NativeInferenceCancellation;
  readonly dispose: () => void;
};

type SharedConnection = {
  promise: Promise<NativeInferenceClient>;
  cancellation: NativeInferenceCancellation | undefined;
  waiters: number;
  settled: boolean;
};

function registerCancellation(module: NativeInferenceModule, signal?: AbortSignal): CancellationRegistration | undefined {
  if (signal === undefined) return undefined;
  const handle = new module.NativeInferenceCancellation();
  const onAbort = () => handle.cancel();
  signal.addEventListener("abort", onAbort, { once: true });
  // Adding an abort listener after the signal was already aborted does not
  // invoke it, so explicitly mirror that state before starting native work.
  if (signal.aborted) handle.cancel();
  return {
    handle,
    dispose: () => signal.removeEventListener("abort", onAbort),
  };
}

async function withNativeCancellation<T>(
  module: NativeInferenceModule,
  signal: AbortSignal | undefined,
  operation: (cancellation?: NativeInferenceCancellation) => Promise<T>,
): Promise<T> {
  const registration = registerCancellation(module, signal);
  try {
    // Invoke the Rust operation before awaitWithAbort() observes an already-aborted
    // signal. This ensures Rust receives a cancelled handle instead of leaving
    // a native future running behind a JavaScript-only rejection.
    return await awaitWithAbort(operation(registration?.handle), signal);
  } finally {
    registration?.dispose();
  }
}

/** Inference transport backed by the Rust N-API bridge. */
export class NativeInferenceTransport implements InferenceTransport {
  #module: Promise<NativeInferenceModule> | undefined;
  #client: Promise<NativeInferenceClient> | undefined;
  #connecting: SharedConnection | undefined;
  readonly #fallback: InferenceTransport | undefined;
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate: string | undefined;
  readonly maximumEventBytes: number;

  constructor(options: NativeInferenceOptions, fallback?: InferenceTransport) {
    this.endpoint = options.endpoint;
    this.token = options.token;
    this.caCertificate = options.caCertificate;
    this.maximumEventBytes = options.maximumEventBytes ?? MAXIMUM_HTTP_JSON_BYTES;
    if (!Number.isSafeInteger(this.maximumEventBytes) || this.maximumEventBytes <= 0) {
      throw new RangeError("maximumEventBytes must be a positive safe integer byte ceiling");
    }
    this.#fallback = fallback;
  }

  get maximumMessageBytes(): number { return this.maximumEventBytes; }

  #modulePromise(): Promise<NativeInferenceModule> {
    return this.#module ??= binding().catch((error: unknown) => {
      this.#module = undefined;
      throw error;
    });
  }

  async #connect(module: NativeInferenceModule, cancellation: NativeInferenceCancellation): Promise<NativeInferenceClient> {
    const ca = this.caCertificate === undefined ? undefined : Buffer.from(this.caCertificate);
    if (ca !== undefined) {
      return this.maximumEventBytes === MAXIMUM_HTTP_JSON_BYTES
        ? module.NativeInferenceClient.connectWithCa(this.endpoint, this.token, ca, cancellation)
        : module.NativeInferenceClient.connectWithLimitAndCa(this.endpoint, this.token, this.maximumEventBytes, ca, cancellation);
    }
    return this.maximumEventBytes === MAXIMUM_HTTP_JSON_BYTES
      ? module.NativeInferenceClient.connect(this.endpoint, this.token, cancellation)
      : module.NativeInferenceClient.connectWithLimit(this.endpoint, this.token, this.maximumEventBytes, cancellation);
  }

  #startConnection(): SharedConnection {
    const connection = {} as SharedConnection;
    connection.waiters = 0;
    connection.settled = false;
    connection.cancellation = undefined;
    connection.promise = this.#modulePromise().then(async (module) => {
      // Every waiter may have aborted while the native module was loading.
      // Avoid starting a Rust future after the last caller has gone away.
      if (connection.waiters === 0) throw new DOMException("The operation was aborted", "AbortError");
      const cancellation = new module.NativeInferenceCancellation();
      connection.cancellation = cancellation;
      return this.#connect(module, cancellation);
    }).then((client) => {
      connection.settled = true;
      if (this.#connecting === connection) {
        this.#connecting = undefined;
        this.#client = Promise.resolve(client);
      }
      return client;
    }, (error: unknown) => {
      connection.settled = true;
      if (this.#connecting === connection) this.#connecting = undefined;
      throw error;
    });
    this.#connecting = connection;
    return connection;
  }

  #clientPromise(signal?: AbortSignal): Promise<NativeInferenceClient> {
    if (this.#client !== undefined) return awaitWithAbort(this.#client, signal);
    const connection = this.#connecting ?? this.#startConnection();
    connection.waiters += 1;
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      connection.waiters -= 1;
      if (connection.waiters === 0 && !connection.settled) {
        connection.cancellation?.cancel();
        if (this.#connecting === connection) this.#connecting = undefined;
      }
    };
    return awaitWithAbort(connection.promise, signal).finally(release);
  }

  async #fallbackCall<T>(operation: () => Promise<T>, fallback: (() => Promise<T>) | undefined): Promise<T> {
    try {
      return await operation();
    } catch (error) {
      if (!isNativeInferenceUnavailable(error) || fallback === undefined) throw error;
      return fallback();
    }
  }

  async #unary<T>(
    request: object,
    requestSchema: { readonly typeName: string },
    responseSchema: { readonly typeName: string },
    method: (client: NativeInferenceClient, bytes: Uint8Array, cancellation?: NativeInferenceCancellation) => Promise<Uint8Array>,
    fallback: (() => Promise<T>) | undefined,
    signal?: AbortSignal,
  ): Promise<T> {
    const bytes = Buffer.from(toBinary(requestSchema as never, request as never));
    return this.#fallbackCall(async () => {
      const module = await this.#modulePromise();
      const client = await this.#clientPromise(signal);
      const result = await withNativeCancellation(module, signal, (cancellation) => method(client, bytes, cancellation));
      return fromBinary(responseSchema as never, Uint8Array.from(result)) as T;
    }, fallback);
  }

  listModels(): Promise<ListModelsResponse> {
    return this.#unary(create(ListModelsRequestSchema), ListModelsRequestSchema, ListModelsResponseSchema, (client, bytes, cancellation) => client.listModels(bytes, cancellation), this.#fallback?.listModels === undefined ? undefined : () => this.#fallback!.listModels());
  }
  createContext(request: CreateContextRequest): Promise<MutationReceipt> {
    return this.#unary(request, CreateContextRequestSchema, MutationReceiptSchema, (client, bytes, cancellation) => client.createContext(bytes, cancellation), this.#fallback?.createContext === undefined ? undefined : () => this.#fallback!.createContext(request));
  }
  inspectContext(request: InspectContextRequest): Promise<ContextView> {
    return this.#unary(request, InspectContextRequestSchema, ContextViewSchema, (client, bytes, cancellation) => client.inspectContext(bytes, cancellation), this.#fallback?.inspectContext === undefined ? undefined : () => this.#fallback!.inspectContext(request));
  }
  mutateContext(request: MutateContextRequest): Promise<MutationReceipt> {
    return this.#unary(request, MutateContextRequestSchema, MutationReceiptSchema, (client, bytes, cancellation) => client.mutateContext(bytes, cancellation), this.#fallback?.mutateContext === undefined ? undefined : () => this.#fallback!.mutateContext(request));
  }
  retainWarm(request: RetainWarmRequest): Promise<WarmView> {
    return this.#unary(request, RetainWarmRequestSchema, WarmViewSchema, (client, bytes, cancellation) => client.retainWarm(bytes, cancellation), this.#fallback?.retainWarm === undefined ? undefined : () => this.#fallback!.retainWarm(request));
  }
  inspectWarm(request: InspectWarmRequest): Promise<WarmView> {
    return this.#unary(request, InspectWarmRequestSchema, WarmViewSchema, (client, bytes, cancellation) => client.inspectWarm(bytes, cancellation), this.#fallback?.inspectWarm === undefined ? undefined : () => this.#fallback!.inspectWarm(request));
  }
  renewWarm(request: RenewWarmRequest): Promise<WarmView> {
    return this.#unary(request, RenewWarmRequestSchema, WarmViewSchema, (client, bytes, cancellation) => client.renewWarm(bytes, cancellation), this.#fallback?.renewWarm === undefined ? undefined : () => this.#fallback!.renewWarm(request));
  }
  releaseWarm(request: ReleaseWarmRequest): Promise<WarmView> {
    return this.#unary(request, ReleaseWarmRequestSchema, WarmViewSchema, (client, bytes, cancellation) => client.releaseWarm(bytes, cancellation), this.#fallback?.releaseWarm === undefined ? undefined : () => this.#fallback!.releaseWarm(request));
  }
  generateRun(request: GenerateRunRequest): Promise<GenerateRunResponse> {
    return this.#unary(request, GenerateRunRequestSchema, GenerateRunResponseSchema, (client, bytes, cancellation) => client.generateRun(bytes, cancellation), this.#fallback?.generateRun === undefined ? undefined : () => this.#fallback!.generateRun(request));
  }
  inspectRun(request: InspectRunRequest, signal?: AbortSignal): Promise<RunView> {
    return this.#unary(request, InspectRunRequestSchema, RunViewSchema, (client, bytes, cancellation) => client.inspectRun(bytes, cancellation), this.#fallback?.inspectRun === undefined ? undefined : () => this.#fallback!.inspectRun(request, signal), signal);
  }
  async *watchRun(request: WatchRunRequest, signal?: AbortSignal): AsyncIterable<RunEvent> {
    try {
      const module = await this.#modulePromise();
      const client = await this.#clientPromise(signal);
      const events = await withNativeCancellation(module, signal, (cancellation) => client.watchRun(
        Buffer.from(toBinary(WatchRunRequestSchema, request)),
        cancellation,
      ));
      for (const event of events) {
        if (signal?.aborted) return;
        yield fromBinary(RunEventSchema, Uint8Array.from(event));
      }
    } catch (error) {
      if (!isNativeInferenceUnavailable(error) || this.#fallback === undefined) throw error;
      yield* this.#fallback.watchRun(request, signal);
    }
  }
  cancelRun(request: InspectRunRequest): Promise<RunView> {
    return this.#unary(request, InspectRunRequestSchema, RunViewSchema, (client, bytes, cancellation) => client.cancelRun(bytes, cancellation), this.#fallback?.cancelRun === undefined ? undefined : () => this.#fallback!.cancelRun(request));
  }
  createEvaluation(request: CreateEvaluationRequest): Promise<EvaluationView> {
    return this.#unary(request, CreateEvaluationRequestSchema, EvaluationViewSchema, (client, bytes, cancellation) => client.createEvaluation(bytes, cancellation), this.#fallback?.createEvaluation === undefined ? undefined : () => this.#fallback!.createEvaluation(request));
  }
  inspectEvaluation(request: InspectEvaluationRequest): Promise<EvaluationView> {
    return this.#unary(request, InspectEvaluationRequestSchema, EvaluationViewSchema, (client, bytes, cancellation) => client.inspectEvaluation(bytes, cancellation), this.#fallback?.inspectEvaluation === undefined ? undefined : () => this.#fallback!.inspectEvaluation(request));
  }
}
