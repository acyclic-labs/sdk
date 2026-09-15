import { create } from "@bufbuild/protobuf";
import {
  CompactSchema, CreateContextRequestSchema, EditSchema, EditsSchema, EmptySchema,
  GenerateRunRequestSchema, MutateContextRequestSchema, ReleaseWarmRequestSchema,
  RenewWarmRequestSchema, RequestIdentitySchema, RetainWarmRequestSchema, TransferSchema,
  TruncateSchema,
  RunTerminal,
  type ContextProvenance, type ContextView, type Edit, type Item, type ModelCapability,
  type MutateContextRequest, type MutationReceipt, type RunEvent, type RunResult, type RunView, type WarmView,
} from "../generated/proto/inference/v1/inference_pb.js";
import { InferenceProtocolError } from "./index.js";

declare const inferenceBrand: unique symbol;
export type ContextRevision = Uint8Array & { readonly [inferenceBrand]: "ContextRevision" };
export type RunId = Uint8Array & { readonly [inferenceBrand]: "RunId" };
/** The released v1 protocol uses the stable Run identity as its recovery operation identity. */
export type OperationId = RunId;
export type WarmCommitment = Uint8Array & { readonly [inferenceBrand]: "WarmCommitment" };
export type ItemId = Uint8Array & { readonly [inferenceBrand]: "ItemId" };
export type ExecutionProfile = Uint8Array & { readonly [inferenceBrand]: "ExecutionProfile" };
export function contextRevision(value: Uint8Array): ContextRevision { return brandedBytes(value, "context revision", 32) as ContextRevision; }
export function runId(value: Uint8Array): RunId { return brandedBytes(value, "run ID", 16) as RunId; }
export function operationId(value: Uint8Array): OperationId { return runId(value); }
export function warmCommitment(value: Uint8Array): WarmCommitment { return brandedBytes(value, "warm commitment", 32) as WarmCommitment; }
export function itemId(value: Uint8Array): ItemId { return brandedBytes(value, "item ID", 16) as ItemId; }
export function executionProfile(value: Uint8Array): ExecutionProfile { return brandedBytes(value, "execution profile", 32) as ExecutionProfile; }
export interface InferenceRequestIdentity { readonly clientInstance: Uint8Array; readonly requestId: Uint8Array }
export interface MutationOptions { readonly identity?: InferenceRequestIdentity }
export interface GenerateOptions extends MutationOptions { readonly maximumOutput: bigint; readonly seed?: bigint }
export interface RetentionPolicy extends MutationOptions { readonly latencyProfile: Uint8Array; readonly expiresAtMs: bigint }

export interface InferenceOperations {
  listModels(): Promise<{ readonly models: ModelCapability[] }>;
  createContext(request: Parameters<import("./index.js").InferenceTransport["createContext"]>[0]): Promise<MutationReceipt>;
  inspectContext(revision: Uint8Array): Promise<ContextView>;
  mutateContext(request: Parameters<import("./index.js").InferenceTransport["mutateContext"]>[0]): Promise<MutationReceipt>;
  retainWarm(request: Parameters<import("./index.js").InferenceTransport["retainWarm"]>[0]): Promise<WarmView>;
  inspectWarm(commitment: Uint8Array): Promise<WarmView>;
  renewWarm(request: Parameters<import("./index.js").InferenceTransport["renewWarm"]>[0]): Promise<WarmView>;
  releaseWarm(request: Parameters<import("./index.js").InferenceTransport["releaseWarm"]>[0]): Promise<WarmView>;
  generate(request: Parameters<import("./index.js").InferenceTransport["generateRun"]>[0]): Promise<{ readonly run?: RunView }>;
  inspectRun(runId: Uint8Array): Promise<RunView>;
  watchRun(runId: Uint8Array, fromSequence?: bigint, signal?: AbortSignal): AsyncIterable<RunEvent>;
  cancelRun(runId: Uint8Array): Promise<RunView>;
}

/** High-level identity-preserving entry point over the generated inference contract. */
export class Inference {
  readonly #clientInstance = randomIdentity();
  constructor(readonly client: InferenceOperations) {}
  static async fromEnv(environment: Readonly<Record<string, string | undefined>> = runtimeEnvironment()): Promise<Inference> { const endpoint = requiredEnvironment(environment, "ACYCLIC_INFERENCE_ENDPOINT"); const token = requiredEnvironment(environment, "ACYCLIC_API_KEY"); const { HttpInferenceTransport, InferenceClient } = await import("./index.js"); return new Inference(new InferenceClient(new HttpInferenceTransport(endpoint, () => ({ authorization: `Bearer ${token}` })))); }
  models(): Promise<{ readonly models: ModelCapability[] }> { return this.client.listModels(); }
  async create<ItemValue extends Item = Item>(model: string, items: readonly ItemValue[], options: MutationOptions = {}): Promise<Context<ItemValue>> { const receipt = await this.client.createContext(create(CreateContextRequestSchema, { identity: wireIdentity(options.identity ?? this.identity()), model, items: [...items] })); return new Context(this, receipt.revision as ContextRevision); }
  context<ItemValue extends Item = Item>(revision: ContextRevision): Context<ItemValue> { return new Context(this, revision); }
  async attach<ItemValue extends Item = Item>(revision: ContextRevision): Promise<Context<ItemValue>> { await this.client.inspectContext(revision); return this.context(revision); }
  run(id: RunId): Run { return new Run(this, id); }
  /** Recovers and authorization-checks the one admitted Run identified by this operation identity. */
  async recover(id: OperationId): Promise<Run> { await this.client.inspectRun(id); return this.run(id); }
  recoverRun(id: RunId): Promise<Run> { return this.recover(id); }
  identity(): InferenceRequestIdentity { return { clientInstance: this.#clientInstance.slice(), requestId: randomIdentity() }; }
}

export class Context<ItemValue extends Item = Item> {
  constructor(readonly inference: Inference, readonly revision: ContextRevision) {}
  id(): ContextRevision { return this.revision; }
  inspect(): Promise<ContextView> { return this.inference.client.inspectContext(this.revision); }
  async items(): Promise<readonly ItemValue[]> { return (await this.inspect()).items as unknown as readonly ItemValue[]; }
  async model(): Promise<string> { return (await this.inspect()).model; }
  async provenance(): Promise<ContextProvenance | undefined> { return (await this.inspect()).provenance; }
  async append<Next extends Item>(item: Next, options: MutationOptions = {}): Promise<Context<ItemValue | Next>> { return this.#mutate<ItemValue | Next>({ case: "edit", value: create(EditsSchema, { edits: [create(EditSchema, { action: { case: "append", value: item } })] }) }, options); }
  async edit(edits: readonly Edit[], options: MutationOptions = {}): Promise<Context<Item>> { return this.#mutate<Item>({ case: "edit", value: create(EditsSchema, { edits: [...edits] }) }, options); }
  async truncate(through?: ItemId, options: MutationOptions = {}): Promise<Context<ItemValue>> { return this.#mutate<ItemValue>({ case: "truncate", value: create(TruncateSchema, through ? { through } : {}) }, options); }
  async compact<Replacement extends Item>(selected: readonly ItemId[], replacement: readonly Replacement[], options: MutationOptions = {}): Promise<Context<ItemValue | Replacement>> { return this.#mutate<ItemValue | Replacement>({ case: "compact", value: create(CompactSchema, { selected: [...selected], replacement: [...replacement] }) }, options); }
  async fork(options: MutationOptions = {}): Promise<Context<ItemValue>> { return this.#mutate<ItemValue>({ case: "fork", value: create(EmptySchema) }, options); }
  async transfer(model: string, options: MutationOptions = {}): Promise<Context<ItemValue>> { return this.#mutate<ItemValue>({ case: "transfer", value: create(TransferSchema, { model }) }, options); }
  async delete(options: MutationOptions = {}): Promise<MutationReceipt> { return this.inference.client.mutateContext(create(MutateContextRequestSchema, { identity: wireIdentity(options.identity ?? this.inference.identity()), source: this.revision, action: { case: "release", value: create(EmptySchema) } })); }
  async generate(input: Item, options: GenerateOptions): Promise<Run> { const response = await this.inference.client.generate(create(GenerateRunRequestSchema, { identity: wireIdentity(options.identity ?? this.inference.identity()), context: this.revision, input, maximumOutput: options.maximumOutput, ...(options.seed === undefined ? {} : { seed: options.seed }) })); if (!response.run) throw new InferenceProtocolError("generate response omitted its run"); return new Run(this.inference, response.run.runId as RunId); }
  async retain(policy: RetentionPolicy): Promise<Warm> { const view = await this.inference.client.retainWarm(create(RetainWarmRequestSchema, { identity: wireIdentity(policy.identity ?? this.inference.identity()), context: this.revision, latencyProfile: policy.latencyProfile, expiresAtMs: policy.expiresAtMs })); return new Warm(this.inference, view.commitment as WarmCommitment); }
  async #mutate<Next extends Item>(action: MutateContextRequest["action"], options: MutationOptions): Promise<Context<Next>> { const receipt = await this.inference.client.mutateContext(create(MutateContextRequestSchema, { identity: wireIdentity(options.identity ?? this.inference.identity()), source: this.revision, action })); return new Context(this.inference, receipt.revision as ContextRevision); }
}

export class Run {
  constructor(readonly inference: Inference, readonly runId: RunId) {}
  id(): RunId { return this.runId; }
  inspect(): Promise<RunView> { return this.inference.client.inspectRun(this.runId); }
  events(options: { readonly from?: bigint; readonly signal?: AbortSignal } = {}): AsyncIterable<RunEvent> { return this.inference.client.watchRun(this.runId, options.from ?? 0n, options.signal); }
  async result(options: { readonly signal?: AbortSignal } = {}): Promise<RunOutcome> {
    let view = await this.inspect();
    if (view.result === undefined) {
      for await (const event of this.events({ from: view.lastSequence, signal: options.signal })) {
        if (event.event.case === "terminal") break;
      }
      view = await this.inspect();
    }
    if (view.result === undefined) throw new InferenceProtocolError("run observation ended without a terminal result");
    return outcome(this.inference, view.result);
  }
  cancel(): Promise<RunView> { return this.inference.client.cancelRun(this.runId); }
}

export class Warm {
  constructor(readonly inference: Inference, readonly commitment: WarmCommitment) {}
  id(): WarmCommitment { return this.commitment; }
  inspect(): Promise<WarmView> { return this.inference.client.inspectWarm(this.commitment); }
  async renew(expiresAtMs: bigint, options: MutationOptions = {}): Promise<WarmView> { return this.inference.client.renewWarm(create(RenewWarmRequestSchema, { identity: wireIdentity(options.identity ?? this.inference.identity()), commitment: this.commitment, expiresAtMs })); }
  async release(options: MutationOptions = {}): Promise<WarmView> { return this.inference.client.releaseWarm(create(ReleaseWarmRequestSchema, { identity: wireIdentity(options.identity ?? this.inference.identity()), commitment: this.commitment })); }
}

export type RunTerminalKind = "completed" | "output-limited" | "tool-call" | "refusal" | "cancelled" | "failed" | "indeterminate";
export interface RunOutcome {
  readonly terminal: RunTerminalKind;
  readonly output: Uint8Array;
  /** A new immutable continuation revision only when the service proved it valid. */
  readonly context: Context | null;
  readonly continuationValid: boolean;
  readonly partial: boolean;
  readonly receipt: RunResult["receipt"];
}
function outcome(inference: Inference, result: RunResult): RunOutcome {
  const terminal = terminalKind(result.terminal);
  const context = result.context === undefined ? null : new Context(inference, result.context.revision as ContextRevision);
  return { terminal, output: result.output, context, continuationValid: context !== null, partial: terminal === "output-limited" || terminal === "cancelled" || terminal === "failed" || terminal === "indeterminate", receipt: result.receipt };
}
function terminalKind(value: RunTerminal): RunTerminalKind {
  switch (value) {
    case RunTerminal.COMPLETED: return "completed";
    case RunTerminal.OUTPUT_LIMITED: return "output-limited";
    case RunTerminal.TOOL_CALL: return "tool-call";
    case RunTerminal.REFUSAL: return "refusal";
    case RunTerminal.CANCELLED: return "cancelled";
    case RunTerminal.FAILED: return "failed";
    case RunTerminal.INDETERMINATE: return "indeterminate";
    default: throw new InferenceProtocolError("run result has an unspecified terminal outcome");
  }
}
function brandedBytes(value: Uint8Array, name: string, length: number): Uint8Array { if (!(value instanceof Uint8Array) || value.byteLength !== length) throw new TypeError(`${name} must be exactly ${length} bytes`); return value.slice(); }
const randomIdentity = (): Uint8Array => crypto.getRandomValues(new Uint8Array(16));
const wireIdentity = (value: InferenceRequestIdentity) => create(RequestIdentitySchema, value);
const runtimeEnvironment = (): Readonly<Record<string, string | undefined>> => (globalThis as typeof globalThis & { process?: { env?: Readonly<Record<string, string | undefined>> } }).process?.env ?? {};
function requiredEnvironment(environment: Readonly<Record<string, string | undefined>>, name: string): string { const value = environment[name]; if (!value?.trim()) throw new InferenceProtocolError(`${name} is required`); return value; }
