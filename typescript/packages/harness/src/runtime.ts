import { validateComponentLabel, validateToolName, type AgentInput, type AgentLoop, type AgentOutput, type ContextBuilder, type Model, type ModelContent, type ModelContentPart, type ModelEvent, type ModelMessage, type ModelProvider, type ModelToolDefinition, type ToolDefinition, type ToolExecutor, type ToolJsonSchema, type ToolJsonValue, type ToolRef, type UserContentPart } from "./model.js";
import { DEFAULT_LIMITS, verifyFileBytes, type FileRef, type Limits, type VolumeRef } from "./conversation.js";
import { approvalBinding, interactionId, type InteractionId, type InteractionResolver, type InteractionResponse, type ResolutionReceipt } from "./interaction.js";
import { NativeContracts, type DurableBatchWire, type ExecutionPlacementWire, type MachineIdentityWire, type NativeLimitsWire, type TaskAdmissionWire, type TaskRunLimitsWire } from "./native-contracts.js";
import type { EffectId, OperationId, Scope, TaskId } from "./index.js";
import type { SelectedModelContext } from "./projection.js";
import type { ForkPreparer, ForkPublisher, ForkReport, ForkRequest, ForkSeed, ResourceRef } from "./fork.js";
import type { ProjectWorkspaceProvider } from "./project.js";
export * from "./model.js";

function compareUtf8(left: string, right: string): number {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  for (let index = 0; index < Math.min(a.length, b.length); index++) {
    if (a[index] !== b[index]) return a[index]! - b[index]!;
  }
  return a.length - b.length;
}

declare const identityBrand: unique symbol;
export type RuntimeTaskId = TaskId;
export type BatchId = string & { readonly [identityBrand]: "BatchId" };
export type GroupId = string & { readonly [identityBrand]: "GroupId" };
export type MessageId = OperationId;
export type InputKey = Readonly<{ readonly batchId: BatchId; readonly index: number }>;

export type Outcome<Output, Identity extends string = string> =
  | { readonly kind: "succeeded"; readonly value: Output }
  | { readonly kind: "failed"; readonly error: TaskFailure }
  | { readonly kind: "cancelled"; readonly receipt: CancelReceipt }
  | { readonly kind: "indeterminate"; readonly operationId: Identity };
export interface TaskFailure {
  readonly message: string;
  readonly code?: "approval_required" | "unsupported";
  readonly cause?: unknown;
}
/** A model operation may have reached its provider and must not be regenerated blindly. */
export class IndeterminateModelTurnError extends Error {
  constructor(readonly operationId: OperationId, cause?: unknown) {
    super("model dispatch outcome is indeterminate; reconcile durably or explicitly abandon the local turn", { cause });
    this.name = "IndeterminateModelTurnError";
  }
}
/** Admission may have committed remotely; retry only by the same operation ID. */
export class AdmissionUncertainError extends Error {
  constructor(readonly operationId: string, cause?: unknown) {
    super("task admission outcome is indeterminate; reconcile by operation ID", { cause });
    this.name = "AdmissionUncertainError";
  }
}
/** The durable host authoritatively finished a selected turn without an answer. */
export class TerminalModelTurnError extends Error {
  constructor(readonly operationId: OperationId, readonly outcome: "failed" | "cancelled", message: string) {
    super(message);
    this.name = "TerminalModelTurnError";
  }
}
class BatchInputError extends Error {}
class BatchProviderError extends Error {}
export interface CancelReceipt { readonly requested: boolean; readonly taskId?: RuntimeTaskId; readonly groupId?: GroupId }
export type Admission<Output> =
  | { readonly kind: "accepted"; readonly task: Task<Output> }
  | { readonly kind: "rejected"; readonly reason: Rejection }
  | { readonly kind: "indeterminate"; readonly operationId: string };
export interface Rejection { readonly code: "group_closed" | "unregistered" | "unsupported" | "invalid_input"; readonly message: string }

export interface TaskEvent<Output> {
  readonly id: string;
  readonly taskId: RuntimeTaskId;
  readonly sequence: number;
  readonly event: { readonly kind: "started" } | { readonly kind: "settled"; readonly outcome: Outcome<Output> };
}
/** A host-backed handle reads authoritative task state rather than synthesizing local events. */
export interface DurableTaskDriver<Output> {
  readonly operationId: string;
  result(): Promise<Outcome<Output>>;
  events(fromSequence: number): AsyncIterable<TaskEvent<Output>>;
  /** The persisted terminal record, including its stable ID and sequence. */
  terminalEvent(): Promise<TaskEvent<Output>>;
  cancel(): Promise<CancelReceipt>;
}

export interface TaskDefinitionOptions<Input, Output> {
  readonly input?: RuntimeSchema<Input>;
  readonly output?: RuntimeSchema<Output>;
  readonly requirements?: readonly string[];
  /** Required for resumable implementations and pinned across host reconnects. */
  readonly implementationDigest?: string;
}
const runtimeSchemaBrand: unique symbol = Symbol("harness.runtime-schema");
export type JsonSchemaValue = ToolJsonValue;
/** A schema is constructed explicitly so durable tasks cannot pin `{}` or `true`. */
export interface RuntimeSchema<Value> {
  readonly id: string;
  readonly document: Readonly<{ [key: string]: JsonSchemaValue }>;
  parse(value: unknown): Value;
  readonly [runtimeSchemaBrand]: (value: Value) => Value;
}
export function defineRuntimeSchema<Value>(id: string,
  document: Readonly<{ [key: string]: JsonSchemaValue }>, parse: (value: unknown) => Value): RuntimeSchema<Value> {
  validateComponentLabel(id, "task schema identity");
  if (document === null || typeof document !== "object" || Array.isArray(document)
    || Object.keys(document).length === 0 || typeof parse !== "function") {
    throw new TypeError("task schema must be a non-vacuous JSON Schema document and parser");
  }
  const detached = structuredClone(document);
  return Object.freeze({ id, document: freezeSchema(detached), parse, [runtimeSchemaBrand]: (value: Value) => value });
}
function freezeSchema<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freezeSchema(child);
    Object.freeze(value);
  }
  return value;
}
interface BoundModel { readonly identity: Model; readonly provider: ModelProvider }
function bindModel(identity: Model, provider: ModelProvider): BoundModel {
  for (const label of [identity.provider, identity.name, identity.revision]) {
    if (typeof label !== "string" || !label.trim()) throw new TypeError("model identity requires a provider, name, and revision");
  }
  if (typeof provider.generate !== "function" || typeof provider.reconcile !== "function") {
    throw new TypeError("model provider requires generate and reconcile");
  }
  return Object.freeze({ identity: Object.freeze({ ...identity, options: freezeSchema(structuredClone(identity.options)) }), provider });
}
export type ResumableTaskOptions<Input, Output> = TaskDefinitionOptions<Input, Output> & Readonly<{
  input: RuntimeSchema<Input>;
  output: RuntimeSchema<Output>;
  implementationDigest: string;
}>;
export type LiveTask<Input, Output> = (context: TaskContext, input: Input) => Output | Promise<Output>;
export type ResumableTransition<State, Output> =
  | { readonly kind: "continue"; readonly state: State }
  | { readonly kind: "effect"; readonly state: State; readonly operationId: OperationId; readonly effect: Readonly<{ kind: string; payload: FileRef }> }
  | { readonly kind: "wait"; readonly state: State; readonly operationId: OperationId; readonly deadline: Date }
  | { readonly kind: "finish"; readonly output: Output };
export interface ResumableTask<Input, Output, State = unknown> {
  /** Validates each restored checkpoint before it reaches a typed transition. */
  readonly state: RuntimeSchema<State>;
  initial(input: Input): State | Promise<State>;
  transition(context: TaskContext, state: State, previous?: Outcome<unknown>): Promise<ResumableTransition<State, Output>>;
}
export type TaskImplementation<Input, Output> =
  | { readonly kind: "live"; readonly handler: LiveTask<Input, Output> }
  | { readonly kind: "resumable"; readonly component: ResumableTask<Input, Output> };

/** Immutable, version-pinned typed task registration. */
export class TaskDefinition<Input, Output> {
  readonly options: TaskDefinitionOptions<Input, Output>;
  private constructor(
    readonly name: string,
    readonly revision: string,
    readonly implementation: TaskImplementation<Input, Output>,
    options: TaskDefinitionOptions<Input, Output>,
  ) {
    validateComponentLabel(name, "task name");
    validateComponentLabel(revision, "task revision");
    taskKey(name, revision);
    if ((options.implementationDigest !== undefined && !/^[0-9a-f]{64}$/.test(options.implementationDigest))
      || (implementation.kind === "resumable" && (!options.implementationDigest || !options.input || !options.output
        || !implementation.component.state))) {
      throw new TypeError("resumable tasks require input, output, and state schemas plus a pinned 32-byte implementation digest");
    }
    for (const schema of [options.input, options.output,
      implementation.kind === "resumable" ? implementation.component.state : undefined]) {
      if (schema !== undefined && (!(runtimeSchemaBrand in schema) || !schema.id.trim()
        || Object.keys(schema.document).length === 0 || typeof schema.parse !== "function")) {
        throw new TypeError("task schema requires a pinned non-vacuous definition");
      }
    }
    this.implementation = Object.freeze({ ...implementation });
    this.options = Object.freeze({ ...options, ...(options.requirements ? { requirements: Object.freeze([...options.requirements]) } : {}) });
    Object.freeze(this);
  }
  static live<Input, Output>(name: string, revision: string, handler: LiveTask<Input, Output>, options: TaskDefinitionOptions<Input, Output> = {}): TaskDefinition<Input, Output> {
    return new TaskDefinition(name, revision, { kind: "live", handler }, options);
  }
  static resumable<Input, Output, State>(name: string, revision: string, component: ResumableTask<Input, Output, State>, options: ResumableTaskOptions<Input, Output>): TaskDefinition<Input, Output> {
    if (component.state === null || typeof component.state !== "object" || !(runtimeSchemaBrand in component.state)) {
      throw new TypeError("resumable tasks require a pinned state schema");
    }
    const state = defineRuntimeSchema<unknown>(component.state.id, component.state.document,
      value => component.state.parse(value));
    const erased: ResumableTask<Input, Output> = Object.freeze({
      state,
      initial: async (input: Input): Promise<unknown> => {
        const value = await component.initial(input);
        const validated = (await NativeContracts.create()).validateToolValue(state.document, value);
        return component.state.parse(validated);
      },
      transition: async (context: TaskContext, value: unknown, previous?: Outcome<unknown>): Promise<ResumableTransition<unknown, Output>> => {
        const contracts = await NativeContracts.create();
        const validated = contracts.validateToolValue(state.document, value);
        const next = await component.transition(context, component.state.parse(validated), previous);
        if (next.kind === "finish") {
          const output = contracts.validateToolValue(options.output.document, next.output);
          return { kind: "finish", output: options.output.parse(output) };
        }
        const nextState = contracts.validateToolValue(state.document, next.state);
        const restored = component.state.parse(nextState);
        if (next.kind === "continue") return { kind: "continue", state: restored };
        const operationId = contracts.validateIdentity("operation", next.operationId);
        if (next.kind === "effect") {
          validateComponentLabel(next.effect.kind, "effect kind");
          return { kind: "effect", state: restored, operationId,
            effect: { kind: next.effect.kind, payload: contracts.validate("file_ref", next.effect.payload) } };
        }
        if (next.kind === "wait" && next.deadline instanceof Date && Number.isFinite(next.deadline.getTime())) {
          return { kind: "wait", state: restored, operationId, deadline: new Date(next.deadline.getTime()) };
        }
        throw new TypeError("resumable transition is invalid");
      },
    });
    return new TaskDefinition(name, revision, { kind: "resumable", component: erased }, options);
  }
  version(): string { return this.revision; }
}

declare const taskRefBrand: unique symbol;
/** Opaque typed child handle; task handlers and registrations stay with the owner. */
export interface TaskRef<Input, Output> {
  readonly name: string;
  readonly revision: string;
  readonly [taskRefBrand]: (input: Input) => Output;
}
const taskReferences = new WeakMap<object, object>();
const taskDefinitions = new WeakMap<object, object>();
function publicTaskHandle<Input, Output>(definition: TaskDefinition<Input, Output>): TaskRef<Input, Output> {
  let handle = taskReferences.get(definition) as TaskRef<Input, Output> | undefined;
  if (handle === undefined) {
    handle = Object.freeze({ name: definition.name, revision: definition.revision }) as TaskRef<Input, Output>;
    taskReferences.set(definition, handle);
    taskDefinitions.set(handle, definition);
  }
  return handle;
}
function registeredTaskDefinition<Input, Output>(value: TaskDefinition<Input, Output> | TaskRef<Input, Output>): TaskDefinition<Input, Output> {
  if (value instanceof TaskDefinition) return value;
  const definition = taskDefinitions.get(value) as TaskDefinition<Input, Output> | undefined;
  if (definition === undefined) throw new TypeError("task handle is not registered");
  return definition;
}

export type RunReceipt =
  | { readonly kind: "tool"; readonly step: number; readonly callId: string; readonly name: string; readonly arguments: unknown; readonly value: unknown; readonly projection: unknown }
  | { readonly kind: "model-completed"; readonly metadata: unknown };
export interface RunOutput<Content = unknown, Artifact = unknown, Receipt = RunReceipt> extends AgentOutput<Content, Artifact> { readonly taskId: RuntimeTaskId; readonly receipts: readonly Receipt[] }
export type SelectedAgentInput =
  | (AgentInput<UserContentPart> & { readonly selectedContext?: never })
  /** Exact selection already committed by the owning conversation provider. */
  | { readonly selectedContext: SelectedModelContext; readonly prompt?: never; readonly content?: never };
type RuntimeAgentInput = AgentInput<UserContentPart> & { readonly selectedContext?: SelectedModelContext };

async function validateSelectedContext(selected: SelectedModelContext, limits: Limits, contracts: NativeContracts): Promise<void> {
  if (typeof selected.selection.conversationRevision !== "bigint"
    || selected.selection.conversationRevision <= 0n
    || selected.messages.length === 0
    || selected.messages.length > limits.context_messages
    || selected.messages.length !== selected.selection.messageIds.length
    || selected.messages.at(-1)?.role !== "user") {
    throw new TypeError("selected context must end with the current user message");
  }
  const user = selected.messages.at(-1)!.content;
  const userParts: readonly ModelContentPart[] = typeof user === "string" ? [{ kind: "text", text: user }]
    : Array.isArray(user) ? user : [user as ModelContentPart];
  if (userParts.length === 0 || userParts.length > 1_024
    || userParts.some(part => (part.kind !== "text" && part.kind !== "file")
      || (part.kind === "text" && !part.text))) {
    throw new TypeError("selected user input contains an unsupported part");
  }
  for (const message of selected.messages) await validateModelContent(message.content, limits, contracts);
}

async function validateModelContent(content: ModelContent, limits: Limits, contracts: NativeContracts): Promise<void> {
  if (typeof content === "string") {
    if (new TextEncoder().encode(content).byteLength > limits.render_bytes) throw new TypeError("model text exceeds render limit");
    return;
  }
  const parts: readonly ModelContentPart[] = Array.isArray(content) ? content : [content as ModelContentPart];
  if (parts.length > limits.attachments + 1) throw new TypeError("model content exceeds attachment limit");
  for (const part of parts) {
    if (part.kind === "file") {
      const file = contracts.validate("file_ref", part.file);
      if (!["reference", "bounded_full", "native"].includes(part.policy)
        || file.descriptor.byte_length > limits.file_bytes
        || new TextEncoder().encode(file.path).byteLength > limits.path_bytes
        || (part.policy === "bounded_full" && file.descriptor.byte_length > limits.render_bytes)) {
        throw new TypeError("model file exceeds harness limits");
      }
    } else if (part.kind === "text" && new TextEncoder().encode(part.text).byteLength > limits.render_bytes) {
      throw new TypeError("model text exceeds render limit");
    } else if (part.kind === "tool_call" || part.kind === "tool_result") {
      validateToolName(part.name);
      const encoded = contracts.encodeCanonicalJson(
        part.kind === "tool_call" ? part.arguments : part.value);
      if (encoded.byteLength > limits.render_bytes) {
        throw new TypeError("model tool projection exceeds render limit");
      }
    } else if (part.kind !== "text") {
      throw new TypeError("model content contains an unsupported part");
    }
  }
}

async function boundedToolValue(value: unknown, renderLimit: number, contracts: NativeContracts): Promise<unknown> {
  const bytes = contracts.encodeCanonicalJson(value).byteLength;
  return bytes <= renderLimit ? value : { omitted: true, byteLength: bytes };
}
export interface AgentHarnessHost { connect(): Promise<AgentHarness> }
/** Exact ref-valued Rust effect status; recovered values never acquire a caller-chosen type. */
export type EffectStatus =
  | Readonly<{ state: "planned" | "dispatched" | "indeterminate" }>
  | Readonly<{ state: "succeeded"; result: FileRef }>
  | Readonly<{ state: "failed"; message: string }>;
export interface TaskMessage {
  readonly id: MessageId;
  readonly sender?: RuntimeTaskId;
  readonly recipient: RuntimeTaskId;
  readonly value: FileRef;
}
export interface HostTaskAttachment {
  readonly task: Task<unknown>;
  readonly operationId: string;
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  /** Complete immutable request retained by an independently bound state owner. */
  readonly admission?: TaskAdmissionRecord;
}
export type TaskAdmissionRecord = TaskAdmissionWire;
export interface HostBatchReplay {
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  /** Rust-canonical digest of the group, task, scope, and ordered inputs pinned at admission. */
  readonly inputDigest: readonly number[];
  readonly entries: readonly GroupEntry<unknown>[];
}
export interface BatchAdmissionRequest {
  readonly contract: "harness.batch.v2";
  readonly groupId: GroupId;
  readonly batchId: BatchId;
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  readonly parentTaskId: RuntimeTaskId | null;
  readonly policy: GroupPolicy;
  readonly members: readonly TaskAdmissionRecord[];
  /** Exact Rust-admitted manifest retained by the owner before member admission. */
  readonly canonical: DurableBatchWire;
  readonly inputDigest: readonly number[];
}
/** Durable admission authority; accepted identities must be observable through the state host. */
export interface HarnessRuntimeSpawner {
  policyIdentity(): PolicyIdentity | null;
  executionIdentity?(): ComponentIdentity | null;
  admitResumable?<Input, Output>(operationId: string, definition: TaskDefinition<Input, Output>, input: Input, harness: AgentHarness, parentTaskId?: RuntimeTaskId, admission?: TaskAdmissionRecord): Promise<Admission<unknown>>;
  /** Retains the complete manifest before admitting any member. */
  admitBatch?(request: BatchAdmissionRequest, harness: AgentHarness): Promise<HostBatchReplay>;
  reconcileAdmission?(operationId: string, harness: AgentHarness): Promise<HostTaskAttachment | null>;
  /** Loads the owner's original immutable batch without re-admitting absent members. */
  loadBatch?(batchId: BatchId, harness: AgentHarness): Promise<DurableBatchWire | null>;
  reconcileBatch?(request: BatchAdmissionRequest, harness: AgentHarness): Promise<HostBatchReplay>;
  /** Retains a batch cancellation declaration before reconciling member requests. */
  cancelBatch?(batchId: BatchId, harness: AgentHarness): Promise<BatchCancellationReport | null>;
}
/** Owner-retained task state and observation, independent of admission. */
export interface TaskChildrenPage {
  readonly revision: bigint;
  readonly entries: readonly Readonly<{ slot: string; taskId: RuntimeTaskId }>[];
  readonly nextAfter: string | null;
}
export interface HarnessRuntimeState {
  policyIdentity(): PolicyIdentity | null;
  executionIdentity?(): ComponentIdentity | null;
  attach(id: RuntimeTaskId, harness: AgentHarness): Promise<HostTaskAttachment>;
  /** Direct same-owner children, independent of conversation or fork ancestry. */
  children?(parent: RuntimeTaskId, expectedRevision: bigint | null, afterSlot: string | null,
    maximum: number, harness: AgentHarness): Promise<TaskChildrenPage>;
  reconcileEffect(taskId: RuntimeTaskId, effectId: EffectId): Promise<EffectStatus>;
  send(message: TaskMessage): Promise<{ readonly accepted: boolean; readonly messageId: MessageId }>;
  inbox(taskId: RuntimeTaskId, from?: MessageId): AsyncIterable<TaskMessage>;
  /** Stages the request, admits a ref-only interaction, and reconciles its versioned response. */
  routeInteraction?(taskId: RuntimeTaskId, id: InteractionId, interaction: Interaction & Readonly<{ id: InteractionId }>): Promise<Answer>;
  /** Commits a timer wait boundary and resumes from the same deadline after restart. */
  waitUntil?(taskId: RuntimeTaskId, operationId: string, deadline: Date): Promise<void>;
  /** Owner-journaled exact tool operation; the owner may use any tool executor. */
  executeTool?<Input, Output>(taskId: RuntimeTaskId, operationId: OperationId, tool: ToolRef<Input, Output>, input: Input, harness: AgentHarness): Promise<Outcome<unknown, OperationId>>;
}
/** Combined local host convenience; separate state and spawner bindings override it. */
export interface HarnessRuntimeHost extends HarnessRuntimeSpawner, HarnessRuntimeState {
  /** Replay or reconcile one exact selected turn through the durable execution journal. */
  executeSelectedTurn?(operationId: OperationId, selected: SelectedModelContext, harness: AgentHarness): Promise<Outcome<RunOutput, OperationId>>;
}
/** Qualified placement and the exact durable authorities that can run it. */
export interface HarnessExecutionProvider {
  identity(): ComponentIdentity;
  spawner(): HarnessRuntimeSpawner;
  state(): HarnessRuntimeState;
  qualifyTask(request: TaskAdmissionRecord): Promise<ExecutionPlacementWire>;
  qualifyBatch(request: DurableBatchWire): Promise<ExecutionPlacementWire>;
}
async function observeAdmittedTask(state: HarnessRuntimeState, taskId: RuntimeTaskId,
  harness: AgentHarness, operationId: string): Promise<HostTaskAttachment | null> {
  try { return await state.attach(taskId, harness); }
  catch (error) {
    if (error instanceof AdmissionUncertainError && error.operationId === operationId) return null;
    throw error;
  }
}
export interface InteractionHandler { route(interaction: Interaction): Promise<Answer> }
declare const policyDigestBrand: unique symbol;
export type PolicyDigest = readonly number[] & Readonly<{ readonly [policyDigestBrand]: true }>;
export interface ComponentIdentity {
  readonly name: string;
  readonly version: string;
  readonly digest: PolicyDigest;
}
/** A scoped policy retains both pinned implementations instead of claiming either one alone. */
export type PolicyIdentity = ComponentIdentity | Readonly<{ composition: readonly [PolicyIdentity, PolicyIdentity] }>;
export function policyIdentity(name: string, version: string, digest: Uint8Array): ComponentIdentity {
  validateComponentLabel(name, "policy name");
  validateComponentLabel(version, "policy version");
  if (digest.length !== 32 || !digest.some(byte => byte !== 0)) {
    throw new TypeError("policy implementation digest must be a nonzero 32-byte value");
  }
  return Object.freeze({ name, version, digest: Object.freeze([...digest]) as PolicyDigest });
}
export interface Policy {
  /** Immutable implementation revision, checked at admission and again at host dispatch. */
  identity(): ComponentIdentity;
  evaluate(invocation: Invocation, scope: EffectiveScope): Promise<PolicyDecision>;
}
interface EffectivePolicy {
  identity(): PolicyIdentity;
  evaluate(invocation: Invocation, scope: EffectiveScope): Promise<EffectivePolicyDecision>;
}
export type Interaction =
  | { readonly id: string; readonly kind: "question"; readonly prompt: string; readonly responseSchema?: ToolJsonSchema; readonly deadline?: Date }
  | { readonly id: string; readonly kind: "choice"; readonly prompt: string; readonly options: readonly { readonly id: string; readonly label: string; readonly description?: string }[]; readonly minSelections: number; readonly maxSelections: number; readonly deadline?: Date }
  | { readonly id: string; readonly kind: "form"; readonly title: string; readonly schema: ToolJsonSchema; readonly deadline?: Date }
  | { readonly id: string; readonly kind: "approval"; readonly prompt: string; readonly operationId: OperationId; readonly actionDigest: readonly number[]; readonly deadline?: Date };
export type TypedInteractionInput =
  | Omit<Extract<Interaction, { kind: "question" }>, "responseSchema">
  | Omit<Extract<Interaction, { kind: "form" }>, "schema">;
export type Answer<Value = unknown> =
  | { readonly kind: "accepted"; readonly text: string }
  | { readonly kind: "answered"; readonly value: Value }
  | { readonly kind: "approved" }
  | { readonly kind: "declined" | "cancelled" | "expired" | "denied" }
  | { readonly kind: "indeterminate"; readonly operationId: string };
export type ToolApprovalFailureKind = "declined" | "cancelled" | "expired" | "denied" | "indeterminate" | "invalid_response";
export class ToolApprovalError extends Error {
  constructor(readonly kind: ToolApprovalFailureKind, readonly operationId: OperationId) {
    super(`tool approval was not granted: ${kind}`);
  }
}
export interface Invocation { readonly kind: "tool"; readonly tool: string; readonly arguments: unknown }
export interface EffectiveScope { readonly grants: readonly string[]; readonly limits: Readonly<{ readonly concurrency?: number; readonly deadline?: Date; readonly maxSteps?: number }> }
export type PolicyDecision = { readonly kind: "allow" } | { readonly kind: "deny"; readonly reason: string } | { readonly kind: "require-approval"; readonly prompt: string };
interface PolicyApproval { readonly prompt: string; readonly policy: PolicyIdentity }
type EffectivePolicyDecision = PolicyDecision | Readonly<{ kind: "require-approvals"; approvals: readonly PolicyApproval[] }>;

export class ExecutionScope {
  readonly grants: readonly string[];
  readonly #concurrency: number | undefined;
  readonly #maxSteps: number | undefined;
  readonly #deadlineEpochMs: number | undefined;
  constructor(
    readonly modelBinding?: BoundModel,
    readonly contextBuilder?: ContextBuilder,
    readonly interactionHandler?: InteractionHandler,
    readonly policyProvider?: EffectivePolicy,
    grants: readonly string[] = [],
    limits: EffectiveScope["limits"] = {},
    readonly grantsExplicit = false,
    readonly executionProvider?: HarnessExecutionProvider,
  ) {
    if (grants.some(value => !value.trim())) throw new TypeError("capability grant is empty");
    if (limits.concurrency !== undefined && (!Number.isSafeInteger(limits.concurrency) || limits.concurrency <= 0)) throw new RangeError("scope concurrency must be a positive safe integer");
    if (limits.maxSteps !== undefined && (!Number.isSafeInteger(limits.maxSteps) || limits.maxSteps <= 0)) throw new RangeError("scope maxSteps must be a positive safe integer");
    if (limits.deadline !== undefined && !Number.isFinite(limits.deadline.getTime())) throw new RangeError("scope deadline must be valid");
    this.grants = Object.freeze([...new Set(grants)]);
    this.#concurrency = limits.concurrency;
    this.#maxSteps = limits.maxSteps;
    this.#deadlineEpochMs = limits.deadline?.getTime();
    Object.freeze(this);
  }
  get limits(): EffectiveScope["limits"] {
    return Object.freeze({
      ...(this.#concurrency === undefined ? {} : { concurrency: this.#concurrency }),
      ...(this.#maxSteps === undefined ? {} : { maxSteps: this.#maxSteps }),
      ...(this.#deadlineEpochMs === undefined ? {} : { deadline: new Date(this.#deadlineEpochMs) }),
    });
  }
  static create(): ExecutionScope { return new ExecutionScope(); }
  model(identity: Model, provider: ModelProvider): ExecutionScope { return new ExecutionScope(bindModel(identity, provider), this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  context(value: ContextBuilder): ExecutionScope { return new ExecutionScope(this.modelBinding, value, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  interactions(value: InteractionHandler): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, value, this.policyProvider, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  policy(value: Policy): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, value, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  execution(value: HarnessExecutionProvider): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit, value); }
  /** Select a subset of the parent's grants; an empty selection removes all. */
  onlyGrants(...values: readonly string[]): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, this.policyProvider, [...new Set(values)], this.limits, true, this.executionProvider); }
  grant(...values: readonly string[]): ExecutionScope { return this.onlyGrants(...this.grants, ...values); }
  withLimits(value: EffectiveScope["limits"]): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, value, this.grantsExplicit, this.executionProvider); }
}

export type GroupPolicy = { readonly kind: "collect-all" } | { readonly kind: "cancel-on-failure" };
export const GroupPolicies = Object.freeze({ collectAll: { kind: "collect-all" } as const, cancelOnFailure: { kind: "cancel-on-failure" } as const });

export class Batch<Input> {
  constructor(readonly id: BatchId, readonly inputs: readonly Input[]) { if (!id) throw new TypeError("batch id is required"); }
}
export class ExecutionQualificationError extends Error {}
export interface GroupEntry<Output> { readonly key: InputKey; readonly admission: Admission<Output>; readonly outcome?: Outcome<Output> }
/** A cancellation request is not a terminal task outcome. */
export type BatchCancellationStatus =
  | Readonly<{ kind: "requested" }>
  | Readonly<{ kind: "not-admitted"; reason: Extract<Admission<unknown>, { kind: "rejected" }>["reason"] }>
  | Readonly<{ kind: "indeterminate"; operationId: string }>
  | Readonly<{ kind: "unresolved"; message: string }>;
export interface BatchCancellationReport {
  readonly groupId: GroupId;
  readonly batchId: BatchId;
  /** The declaration is retained first; member requests are reconciled separately. */
  readonly entries: readonly Readonly<{ key: InputKey; status: BatchCancellationStatus }>[];
}
function assertBatchCancellationReport(report: BatchCancellationReport | null, groupId: GroupId,
  batchId: BatchId, count: number): asserts report is BatchCancellationReport {
  if (report === null || report.groupId !== groupId || report.batchId !== batchId
    || report.entries.length !== count || report.entries.some((entry, index) =>
      entry.key.batchId !== batchId || entry.key.index !== index)) {
    throw new Error("batch owner returned another cancellation report");
  }
}
export interface GroupOutcome<Output> { readonly groupId: GroupId; readonly complete: boolean; readonly entries: readonly GroupEntry<Output>[]; readonly cancellation: "not-needed" | "requested" | "indeterminate" }
export interface Keyed<Output> { readonly key: InputKey; readonly value: Output }
type TaskAuthorizer = <Input, Output>(definition: TaskDefinition<Input, Output>) => void;
type GroupTask<Authority extends "owner" | "scoped", Input, Output> = Authority extends "scoped"
  ? TaskRef<Input, Output> : TaskDefinition<Input, Output>;

export class Task<Output> {
  readonly #events = new ReplayQueue<TaskEvent<Output>>();
  readonly #result: Promise<Outcome<Output>>;
  readonly #driver: DurableTaskDriver<Output> | undefined;
  #terminalEvent: TaskEvent<Output> | undefined;
  #sequence = 0;
  constructor(readonly taskId: RuntimeTaskId, execution: ((signal: AbortSignal) => Promise<Output>) | DurableTaskDriver<Output>, readonly controller = new AbortController()) {
    if (typeof execution !== "function") {
      this.#driver = execution;
      this.#result = execution.result().catch(() => ({ kind: "indeterminate", operationId: execution.operationId } as const));
      return;
    }
    this.#driver = undefined;
    this.#events.push({ id: `${taskId}:0`, taskId, sequence: this.#sequence++, event: { kind: "started" } });
    this.#result = execution(controller.signal).then<Outcome<Output>, Outcome<Output>>(
      value => ({ kind: "succeeded", value }),
      error => controller.signal.aborted ? { kind: "cancelled", receipt: { requested: true, taskId } } : { kind: "failed", error: { message: error instanceof Error ? error.message : String(error), cause: error } },
    ).then(outcome => { this.#terminalEvent = { id: `${taskId}:${this.#sequence}`, taskId, sequence: this.#sequence++, event: { kind: "settled", outcome } }; this.#events.push(this.#terminalEvent); this.#events.close(); return outcome; });
  }
  static fromHost<Output>(taskId: RuntimeTaskId, driver: DurableTaskDriver<Output>): Task<Output> {
    return new Task(taskId, driver);
  }
  id(): RuntimeTaskId { return this.taskId; }
  result(): Promise<Outcome<Output>> { return this.#result; }
  async terminalEvent(): Promise<TaskEvent<Output>> {
    if (this.#driver) return this.#driver.terminalEvent();
    await this.#result;
    if (!this.#terminalEvent) throw new Error("local task lost its terminal event");
    return this.#terminalEvent;
  }
  events(fromSequence = 0): AsyncIterable<TaskEvent<Output>> {
    if (!this.#driver) return this.#events.from(fromSequence);
    return this.#hostEvents(fromSequence);
  }
  async *#hostEvents(fromSequence: number): AsyncIterable<TaskEvent<Output>> {
    let streamedTerminal: TaskEvent<Output> | undefined;
    let lastSequence = fromSequence - 1;
    let invalidEvent = false;
    try {
      for await (const event of this.#driver!.events(fromSequence)) {
        if (event.taskId !== this.taskId || !Number.isSafeInteger(event.sequence)
          || event.sequence <= lastSequence || streamedTerminal) {
          invalidEvent = true;
          throw new Error("durable host returned an invalid task event");
        }
        lastSequence = event.sequence;
        if (event.event.kind === "settled") streamedTerminal = event;
        yield event;
      }
    } catch (error) {
      if (invalidEvent) throw error;
      /* A transport failure is reconciled through the authoritative terminal record. */
    }
    const terminal = await this.#driver!.terminalEvent();
    if (terminal.taskId !== this.taskId || terminal.event.kind !== "settled"
      || !terminal.id || !Number.isSafeInteger(terminal.sequence) || terminal.sequence < 0) {
      throw new Error("durable host returned an invalid terminal event");
    }
    if (streamedTerminal && (streamedTerminal.id !== terminal.id || streamedTerminal.sequence !== terminal.sequence)) {
      throw new Error("durable host task stream disagrees with its terminal event");
    }
    if (!streamedTerminal && lastSequence >= fromSequence && terminal.sequence <= lastSequence) {
      throw new Error("durable host returned an out-of-order terminal event");
    }
    if (!streamedTerminal && terminal.sequence >= fromSequence) {
      yield terminal;
    }
  }
  async cancel(): Promise<CancelReceipt> { if (this.#driver) return this.#driver.cancel(); this.controller.abort(new Error("task cancellation requested")); return { requested: true, taskId: this.taskId }; }
}

export class TaskGroup<Output, Authority extends "owner" | "scoped" = "owner"> {
  readonly #harness: AgentHarness;
  readonly #authorizeTask: TaskAuthorizer | undefined;
  readonly #entries: GroupEntry<Output>[] = [];
  readonly #entryKeys = new Map<string, { readonly index: number; readonly registration: string }>();
  readonly #batchDigests = new Map<BatchId, string>();
  readonly #durableBatchIds = new Set<BatchId>();
  #closed = false;
  constructor(harness: AgentHarness, readonly policy: GroupPolicy, readonly id: GroupId = identity<GroupId>(), readonly parentTaskId?: RuntimeTaskId, authorizeTask?: TaskAuthorizer) {
    this.#harness = harness;
    this.#authorizeTask = authorizeTask;
  }
  async spawnMany<Input>(selected: GroupTask<Authority, Input, Output>, batch: Batch<Input>): Promise<readonly GroupEntry<Output>[]> {
    if (this.#closed) return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index }, admission: { kind: "rejected", reason: { code: "group_closed", message: "group is closed" } } }));
    let definition: TaskDefinition<Input, Output>;
    try {
      definition = registeredTaskDefinition(selected as TaskDefinition<Input, Output> | TaskRef<Input, Output>);
      this.#authorizeTask?.(definition);
    }
    catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index }, admission: { kind: "rejected", reason: { code: "unsupported", message } } }));
    }
    const registration = registrationKey(definition);
    if (definition.implementation.kind === "resumable" && (!this.#harness.spawner?.admitBatch
      || !this.#harness.state)) {
      return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index },
        admission: { kind: "rejected", reason: { code: "unsupported", message: "durable batch requires spawner and state bindings" } } }));
    }
    let request: BatchAdmissionRequest | undefined;
    try {
      this.#harness.task(definition);
      if (definition.implementation.kind === "resumable") {
        // The owner manifest and qualification providers are part of the
        // immutable admission boundary.  Check the pinned policy before
        // consulting either one so a drifted runtime cannot turn into a
        // provider-specific replay or publication error.
        this.#harness.assertPolicyIdentity();
        // Validate and canonicalize every member before consulting the owner
        // manifest. Invalid input must be rejected locally and must never be
        // turned into an unsupported-provider result (or partially admitted).
        const requestWithoutRetention = await this.#batchRequest(definition, batch);
        if (!this.#harness.spawner?.loadBatch) {
          throw new Error("durable batch requires an owner-retained manifest provider");
        }
        const retainedValue = await this.#harness.spawner.loadBatch(batch.id, this.#harness);
        this.#harness.assertPolicyIdentity();
        const retained = retainedValue === null ? null : this.#harness.contracts.validate("durable_batch_request", retainedValue);
        request = retained === null ? requestWithoutRetention
          : await this.#batchRequest(definition, batch, retained.execution);
        if (retained && !this.#harness.contracts.canonicalEqual(retained, request.canonical)) {
          throw new Error("batch identity belongs to another admission request");
        }
      }
      else await this.#bindBatchInputs(definition, batch);
    }
    catch (error) {
      if (!(error instanceof BatchInputError)) throw error;
      const message = error instanceof Error ? error.message : String(error);
      return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index },
        admission: { kind: "rejected", reason: { code: "invalid_input", message } } }));
    }
    if (request) {
      const local = this.#entries.filter(entry => entry.key.batchId === batch.id);
      if (local.length) {
        if (local.length !== batch.inputs.length || local.some(entry =>
          this.#entryKeys.get(`${batch.id}:${entry.key.index}`)?.registration !== registration)) {
          throw new Error("batch identity belongs to another admission request");
        }
        return local;
      }
      const spawner = this.#harness.spawner;
      if (!spawner?.admitBatch) throw new Error("durable batch spawner disappeared after validation");
      this.#harness.assertPolicyIdentity();
      let replay: HostBatchReplay;
      try { replay = await spawner.admitBatch(request, this.#harness); }
      catch (error) {
        if (!(error instanceof AdmissionUncertainError) || error.operationId !== batch.id) throw error;
        replay = { taskName: definition.name, revision: definition.revision,
          implementationDigest: definition.options.implementationDigest!, inputDigest: request.inputDigest,
          entries: request.members.map((member, index) => ({ key: { batchId: batch.id, index },
            admission: { kind: "indeterminate", operationId: member.operation_id } })) };
      }
      this.#harness.assertPolicyIdentity();
      return this.#acceptBatchReplay(definition, batch, request, replay);
    }
    const entries = await Promise.all(batch.inputs.map(async (input, index): Promise<GroupEntry<Output>> => {
      const key = { batchId: batch.id, index };
      const existing = this.#entryKeys.get(`${batch.id}:${index}`);
      if (existing && existing.registration !== registration) {
        return { key, admission: { kind: "rejected", reason: { code: "unsupported", message: "batch entry identity is already bound to another task definition" } } };
      }
      let admission: Admission<Output>;
      if (existing) admission = this.#entries[existing.index]!.admission;
      else if (definition.implementation.kind === "live") {
        if (this.parentTaskId) admission = { kind: "rejected", reason: { code: "unsupported", message: "durable groups cannot spawn local-only tasks" } };
        else if (this.#harness.components.execution) admission = { kind: "rejected", reason: { code: "unsupported", message: "live task closures cannot cross an execution provider" } };
        else {
          try { admission = { kind: "accepted", task: this.#harness.spawn(definition, input) }; }
          catch (error) { admission = { kind: "rejected", reason: { code: "invalid_input", message: error instanceof Error ? error.message : String(error) } }; }
        }
      } else throw new Error("durable batch bypassed immutable admission");
      return { key, admission };
    }));
    entries.forEach((entry, index) => {
      const identity = `${batch.id}:${index}`;
      const existing = this.#entryKeys.get(identity);
      if (existing && existing.registration !== registration) return;
      if (existing) this.#entries[existing.index] = entry;
      else { this.#entryKeys.set(identity, { index: this.#entries.length, registration }); this.#entries.push(entry); }
    });
    return entries;
  }
  async reconcileBatch<Input>(selected: GroupTask<Authority, Input, Output>, batch: Batch<Input>): Promise<readonly GroupEntry<Output>[]> {
    const definition = registeredTaskDefinition(selected as TaskDefinition<Input, Output> | TaskRef<Input, Output>);
    this.#authorizeTask?.(definition);
    this.#harness.task(definition);
    // Report a changed policy before provider-shape errors. Reconciliation is
    // itself a durable operation and must never run under a drifted policy.
    this.#harness.assertPolicyIdentity();
    const registration = registrationKey(definition);
    const spawner = this.#harness.spawner;
    const host = this.#harness.state;
    if (!spawner?.loadBatch || !spawner.reconcileBatch || !host) throw new Error("durable batch reconciliation requires retained owner manifest and state bindings");
    const loadedValue = await spawner.loadBatch(batch.id, this.#harness);
    if (loadedValue === null) throw new Error("retained batch is missing");
    const loaded = this.#harness.contracts.validate("durable_batch_request", loadedValue);
    const request = await this.#batchRequest(definition, batch, loaded.execution);
    if (!this.#harness.contracts.canonicalEqual(loaded, request.canonical)) {
      throw new Error("retained batch differs from its pinned task, inputs, or scope");
    }
    const local = this.#entries.filter(entry => entry.key.batchId === batch.id);
    if (local.length) {
      if (local.some(entry => this.#entryKeys.get(`${batch.id}:${entry.key.index}`)?.registration !== registration)) {
        throw new Error("batch identity belongs to another task definition");
      }
      if (local.length === batch.inputs.length && local.every(entry => entry.admission.kind !== "indeterminate"
        && entry.outcome?.kind !== "indeterminate")) return local;
    }
    this.#harness.assertPolicyIdentity();
    const recovered = await spawner.reconcileBatch(request, this.#harness);
    this.#harness.assertPolicyIdentity();
    return this.#acceptBatchReplay(definition, batch, request, recovered);
  }
  /** Reattach by the caller-retained ID after losing the original input vector. */
  async reconcileBatchId<Input>(selected: GroupTask<Authority, Input, Output>, batchId: BatchId): Promise<readonly GroupEntry<Output>[] | null> {
    const definition = registeredTaskDefinition(selected as TaskDefinition<Input, Output> | TaskRef<Input, Output>);
    this.#authorizeTask?.(definition);
    this.#harness.task(definition);
    if (definition.implementation.kind !== "resumable") throw new Error("batch task is not resumable");
    const spawner = this.#harness.spawner;
    if (!spawner?.loadBatch || !spawner.reconcileBatch || !this.#harness.state) {
      throw new Error("batch ID reconciliation requires spawner and state bindings");
    }
    this.#harness.assertPolicyIdentity();
    const loaded = await spawner.loadBatch(batchId, this.#harness);
    if (loaded === null) return null;
    const retained = this.#harness.contracts.validate("durable_batch_request", loaded);
    if (retained.batch_id !== batchId || retained.group_id !== this.id) {
      throw new Error("retained batch differs from its pinned group bindings");
    }
    const batch = new Batch<Input>(batchId, retained.inputs as readonly Input[]);
    const expected = await this.#batchRequest(definition, batch, retained.execution);
    if (!this.#harness.contracts.canonicalEqual(retained, expected.canonical)) {
      throw new Error("retained batch differs from its pinned task, inputs, or scope");
    }
    const recovered = await spawner.reconcileBatch(expected, this.#harness);
    this.#harness.assertPolicyIdentity();
    return this.#acceptBatchReplay(definition, batch, expected, recovered);
  }
  /** Reconciles a retained batch, then asks its owner to retain one
   * cancellation declaration before requesting accepted members. */
  async cancelBatchId<Input>(selected: GroupTask<Authority, Input, Output>, batchId: BatchId): Promise<BatchCancellationReport | null> {
    const admitted = await this.reconcileBatchId(selected, batchId);
    if (admitted === null) return null;
    const spawner = this.#harness.spawner;
    if (!spawner?.cancelBatch) throw new Error("durable batch cancellation is not bound");
    const report = await spawner.cancelBatch(batchId, this.#harness);
    assertBatchCancellationReport(report, this.id, batchId, admitted.length);
    return report;
  }
  async #acceptBatchReplay<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>,
    request: BatchAdmissionRequest, recovered: HostBatchReplay): Promise<readonly GroupEntry<Output>[]> {
    const registration = registrationKey(definition);
    const { inputDigest: suppliedDigest } = request;
    const canonical = this.#harness.contracts.validate("durable_batch_request", request.canonical);
    const recomputedDigest = this.#harness.contracts.digestCanonicalJson(canonical);
    const hex = Array.from(recomputedDigest, byte => byte.toString(16).padStart(2, "0")).join("");
    if (this.#batchDigests.get(batch.id) !== hex || suppliedDigest.length !== recomputedDigest.length
      || suppliedDigest.some((byte, index) => byte !== recomputedDigest[index])) {
      throw new Error("batch request changed after admission binding");
    }
    const inputDigest = request.inputDigest;
    const host = this.#harness.state;
    if (!host) throw new Error("durable batch state is not bound");
    if (inputDigest.length !== recovered.inputDigest.length
      || inputDigest.some((byte, index) => byte !== recovered.inputDigest[index])) {
      throw new Error("host batch input digest differs from the requested batch");
    }
    if (definition.name !== recovered.taskName || definition.revision !== recovered.revision
      || definition.options.implementationDigest !== recovered.implementationDigest
      || definition.implementation.kind !== "resumable") throw new Error("host returned an unregistered batch task implementation");
    const outputSchema = pinnedOutputSchema(definition);
    if (recovered.entries.length !== batch.inputs.length) throw new Error("host returned an incomplete batch");
    const entries: GroupEntry<Output>[] = [];
    const seen = new Set<number>();
    for (const entry of recovered.entries) {
      if (entry.key.batchId !== batch.id || !Number.isSafeInteger(entry.key.index)
        || entry.key.index < 0 || entry.key.index >= batch.inputs.length || seen.has(entry.key.index)) {
        throw new Error("host returned an invalid batch entry identity");
      }
      seen.add(entry.key.index);
      const identity = `${batch.id}:${entry.key.index}`;
      const prior = this.#entryKeys.get(identity);
      if (prior && prior.registration !== registration) throw new Error("batch identity belongs to another task definition");
      const operationId = this.#harness.contracts.batchMemberOperationId(this.id, batch.id, entry.key.index);
      if (entry.admission.kind === "indeterminate" && entry.admission.operationId !== operationId) {
        throw new Error("host returned an unrelated batch admission");
      }
        const outcome = entry.outcome === undefined ? undefined : await validateTaskOutcome(entry.outcome, outputSchema, this.#harness.contracts);
      let admission: Admission<Output>;
      if (entry.admission.kind === "accepted") {
        const observed = await observeAdmittedTask(host, entry.admission.task.id(), this.#harness, operationId);
        if (observed === null) admission = { kind: "indeterminate", operationId };
        else {
          if (observed.operationId !== operationId || observed.task.id() !== entry.admission.task.id()
            || observed.taskName !== definition.name || observed.revision !== definition.revision
            || observed.implementationDigest !== definition.options.implementationDigest) {
            throw new Error("spawner and state host batch bindings differ");
          }
          if (this.#harness.components.spawner || this.#harness.components.state) {
            this.#harness.attestAdmission(observed, request.members[entry.key.index]!);
          }
          admission = { kind: "accepted", task: validatedHostTask(observed.task, operationId, outputSchema, this.#harness.contracts) };
        }
      } else admission = entry.admission;
      if (outcome !== undefined && admission.kind !== "accepted") {
        throw new Error("batch outcome cannot precede an owner-observed accepted admission");
      }
      const validated: GroupEntry<Output> = { key: entry.key, admission, ...(outcome === undefined ? {} : { outcome }) };
      entries.push(validated);
    }
    entries.sort((a, b) => a.key.index - b.key.index);
    for (const entry of entries) {
      const identity = `${batch.id}:${entry.key.index}`;
      const prior = this.#entryKeys.get(identity);
      if (prior) this.#entries[prior.index] = entry;
      else { this.#entryKeys.set(identity, { index: this.#entries.length, registration }); this.#entries.push(entry); }
    }
    this.#durableBatchIds.add(batch.id);
    return entries;
  }
  async #batchRequest<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>,
    retainedExecution?: ExecutionPlacementWire | null): Promise<BatchAdmissionRequest> {
    if (definition.implementation.kind !== "resumable" || !definition.options.input
      || !definition.options.implementationDigest) throw new BatchInputError("durable batch needs a pinned resumable task");
    if (batch.inputs.length > 65_536) throw new BatchInputError("batch has too many inputs");
    let request: BatchAdmissionRequest;
    try {
      const contracts = this.#harness.contracts;
      const members = batch.inputs.map((input, index) => {
        const admitted = definition.options.input!.parse(contracts.validateToolValue(definition.options.input!.document, input));
        contracts.validateToolValue(definition.options.input!.document, admitted);
        return this.#harness.admissionRecord(contracts.batchMemberOperationId(this.id, batch.id, index),
          definition, admitted, this.parentTaskId);
      });
      const identities = durableWireIdentities(definition, contracts);
      const policy = this.#harness.durablePolicyIdentity();
      if (this.policy.kind === "cancel-on-failure" && !this.#harness.spawner?.cancelBatch) {
        throw new BatchProviderError("durable cancel-on-failure requires owner-retained batch cancellation");
      }
      const base = contracts.validate("durable_batch_request", {
        contract: "harness.batch.v2", group_id: this.id, batch_id: batch.id,
        group_policy: this.policy.kind,
        task: identities.task,
        machine: identities.machine,
        inputs: members.map(member => member.input), input_schema: definition.options.input.document,
        output_schema: definition.options.output!.document, parent: this.parentTaskId ?? null,
        grants: [...this.#harness.scope.grants].sort(), limits: nativeLimits(this.#harness.limits),
        run_limits: nativeRunLimits(this.#harness.scope.limits),
        extensions: null, policy, execution: null,
      });
      let execution = retainedExecution ?? null;
      if (retainedExecution === undefined && this.#harness.components.execution) {
        try {
          execution = contracts.validate("execution_placement",
            await this.#harness.components.execution.qualifyBatch(base));
        } catch (error) {
          throw new ExecutionQualificationError(error instanceof Error ? error.message : String(error));
        }
      }
      this.#harness.validateExecutionPlacement(execution);
      const canonical = execution === null ? base : contracts.validate("durable_batch_request",
        { ...base, execution });
      const admittedMembers = execution === null ? members
        : members.map(member => freezeSchema({ ...member, execution }));
      const body = { contract: "harness.batch.v2" as const, groupId: this.id, batchId: batch.id,
        taskName: definition.name, revision: definition.revision,
        implementationDigest: definition.options.implementationDigest,
        parentTaskId: this.parentTaskId ?? null, policy: this.policy, members: admittedMembers, canonical };
      if (contracts.encodeCanonicalJson(canonical).byteLength > this.#harness.limits.file_bytes) {
        throw new BatchInputError("batch request exceeds file limit");
      }
      const inputDigest = contracts.digestCanonicalJson(canonical);
      request = freezeSchema({ ...body, policy: { ...this.policy }, members: [...admittedMembers], inputDigest: Array.from(inputDigest) });
    }
    catch (error) {
      if (error instanceof BatchInputError || error instanceof BatchProviderError
        || error instanceof ExecutionQualificationError) throw error;
      throw new BatchInputError(error instanceof Error ? error.message : String(error));
    }
    this.#pinBatchDigest(batch.id, request.inputDigest);
    return request;
  }
  #pinBatchDigest(batchId: BatchId, digest: readonly number[] | Uint8Array): void {
    const hex = Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join("");
    const pinned = this.#batchDigests.get(batchId);
    if (pinned !== undefined && pinned !== hex) throw new Error("batch identity belongs to another admission request");
    this.#batchDigests.set(batchId, hex);
  }
  async #bindBatchInputs<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>): Promise<Uint8Array> {
    let digest: Uint8Array;
    try {
      digest = this.#harness.contracts.digestCanonicalJson({
        contract: "harness.batch.v2",
        group_id: this.id,
        batch_id: batch.id,
        task: registrationKey(definition),
        parent_task_id: this.parentTaskId ?? null,
        policy: this.policy.kind,
        grants: [...this.#harness.scope.grants].sort(),
        limits: this.#harness.scope.limits,
        inputs: batch.inputs,
      });
    }
    catch (error) { throw new BatchInputError(error instanceof Error ? error.message : String(error)); }
    this.#pinBatchDigest(batch.id, digest);
    return digest;
  }
  async *asCompleted(): AsyncIterable<GroupEntry<Output>> {
    for (const entry of this.#entries) if (entry.admission.kind !== "accepted") yield entry;
    const completed: GroupEntry<Output>[] = [];
    let next = 0;
    let remaining = 0;
    let wake: (() => void) | undefined;
    for (const entry of this.#entries) {
      if (entry.admission.kind !== "accepted") continue;
      remaining++;
      void entry.admission.task.result().then(outcome => {
        completed.push(withOutcome(entry, outcome));
        wake?.();
        wake = undefined;
      });
    }
    while (remaining > 0) {
      if (next === completed.length) await new Promise<void>(resolve => { wake = resolve; });
      while (next < completed.length) { remaining--; yield completed[next++]!; }
    }
  }
  async map<Input>(definition: GroupTask<Authority, Input, Output>, inputs: readonly Input[]): Promise<readonly Output[]> {
    if (!inputs.length) return [];
    if (registeredTaskDefinition(definition as TaskDefinition<Input, Output> | TaskRef<Input, Output>).implementation.kind === "resumable") throw new Error("durable map requires a stable Batch ID; use spawnMany and join");
    const entries = await this.spawnMany(definition, new Batch(identity<BatchId>(), inputs));
    const outcomes = await Promise.all(entries.map(entry => entry.admission.kind === "accepted" ? entry.admission.task.result().then(async outcome => { if (outcome.kind !== "succeeded" && this.policy.kind === "cancel-on-failure") await this.cancel(); return outcome; }) : undefined));
    const values: Output[] = [];
    for (const outcome of outcomes) {
      if (outcome?.kind !== "succeeded") {
        if (this.policy.kind === "cancel-on-failure") await this.cancel();
        throw new GroupError(this.id, entries.map((entry, index) => {
          const result = outcomes[index];
          return result === undefined ? entry : withOutcome(entry, result);
        }));
      }
      values.push(outcome.value);
    }
    return values;
  }
  async join(): Promise<GroupOutcome<Output>> {
    this.#closed = true;
    const observed = new Map<string, GroupEntry<Output>>();
    let cancellation: GroupOutcome<Output>["cancellation"] = "not-needed";
    for await (const entry of this.asCompleted()) {
      observed.set(`${entry.key.batchId}:${entry.key.index}`, entry);
      const failed = entry.admission.kind === "rejected"
        || entry.outcome?.kind === "failed" || entry.outcome?.kind === "cancelled";
      if (failed && this.policy.kind === "cancel-on-failure" && cancellation === "not-needed") {
        try { cancellation = (await this.cancel()).requested ? "requested" : "indeterminate"; }
        catch { cancellation = "indeterminate"; break; }
        if (cancellation === "indeterminate") break;
      }
    }
    const entries = this.#entries.map(entry => observed.get(`${entry.key.batchId}:${entry.key.index}`) ?? entry);
    const complete = cancellation !== "indeterminate" && entries.every(entry =>
      entry.admission.kind !== "indeterminate" && entry.outcome?.kind !== "indeterminate"
      && (entry.admission.kind !== "accepted" || entry.outcome !== undefined));
    return { groupId: this.id, complete, entries, cancellation };
  }
  async race(): Promise<GroupEntry<Output>> {
    const accepted = this.#entries.filter((entry): entry is GroupEntry<Output> & { admission: { kind: "accepted"; task: Task<Output> } } => entry.admission.kind === "accepted");
    if (!accepted.length) throw new GroupError(this.id, this.#entries);
    const settled = accepted.map(async entry => ({ ...entry, outcome: await entry.admission.task.result() }));
    const winner = await Promise.race(settled);
    return winner;
  }
  async firstSuccess(): Promise<Keyed<Output>> {
    const accepted = this.#entries.filter((entry): entry is GroupEntry<Output> & { admission: { kind: "accepted"; task: Task<Output> } } => entry.admission.kind === "accepted");
    if (!accepted.length) throw new GroupError(this.id, this.#entries);
    const settled = accepted.map(async entry => ({ ...entry, outcome: await entry.admission.task.result() }));
    try {
      const winner = await Promise.any(settled.map(async result => {
        const entry = await result;
        if (entry.outcome.kind !== "succeeded") throw entry;
        return { key: entry.key, value: entry.outcome.value };
      }));
      return winner;
    } catch { throw new GroupError(this.id, await Promise.all(settled)); }
  }
  async quorum(required: number, accept: (value: Output) => boolean): Promise<readonly Keyed<Output>[]> {
    if (!Number.isSafeInteger(required) || required < 0) throw new RangeError("required must be a non-negative safe integer");
    if (required === 0) return [];
    const values: Keyed<Output>[] = [];
    const observed: GroupEntry<Output>[] = [];
    for await (const entry of this.asCompleted()) {
      observed.push(entry);
      if (entry.outcome?.kind === "succeeded" && accept(entry.outcome.value)) {
        values.push({ key: entry.key, value: entry.outcome.value });
        if (values.length === required) {
          return values;
        }
      }
    }
    throw new GroupError(this.id, observed);
  }
  async reduce<Accumulator>(identityValue: Accumulator, reducer: (value: Accumulator, entry: GroupEntry<Output>) => Accumulator | Promise<Accumulator>): Promise<Accumulator> { const joined = await this.join(); let value = identityValue; for (const entry of joined.entries) value = await reducer(value, entry); return value; }
  async close(): Promise<{ readonly groupId: GroupId }> { this.#closed = true; return { groupId: this.id }; }
  async cancel(): Promise<CancelReceipt> {
    this.#closed = true;
    const spawner = this.#harness.spawner;
    if (this.#durableBatchIds.size && !spawner?.cancelBatch) {
      throw new Error("durable batch cancellation is not bound");
    }
    const reports = await Promise.all([...this.#durableBatchIds].map(async batchId => {
      const report = await spawner!.cancelBatch!(batchId, this.#harness);
      assertBatchCancellationReport(report, this.id, batchId,
        this.#entries.filter(entry => entry.key.batchId === batchId).length);
      return report;
    }));
    const liveReceipts = await Promise.all(this.#entries.map(entry =>
      !this.#durableBatchIds.has(entry.key.batchId) && entry.admission.kind === "accepted"
        ? entry.admission.task.cancel() : undefined));
    const requested = reports.every(report => report.entries.every(entry =>
      entry.status.kind === "requested" || entry.status.kind === "not-admitted"))
      && liveReceipts.every(receipt => receipt?.requested !== false);
    return { requested, groupId: this.id };
  }
}

export class GroupError<Output> extends Error { constructor(readonly groupId: GroupId, readonly entries: readonly GroupEntry<Output>[]) { super("task group did not satisfy its completion strategy"); } }

export class TaskContext {
  readonly #harness: AgentHarness;
  constructor(harness: AgentHarness, readonly signal: AbortSignal, readonly taskId?: RuntimeTaskId, readonly durable = harness.state !== undefined) {
    this.#harness = harness;
  }
  /** Narrows descendant authority and optionally selects new live-task bindings. */
  scoped(scope: ExecutionScope): TaskContext {
    if (this.durable && (scope.executionProvider || scope.modelBinding || scope.contextBuilder)) {
      throw new Error("durable provider transition requires a recorded effect");
    }
    return new TaskContext(this.#harness.scoped(scope), this.signal, this.taskId, this.durable);
  }
  task<Input, Output>(definition: TaskDefinition<Input, Output>): TaskRef<Input, Output>;
  task(name: string): TaskRef<unknown, unknown>;
  task(value: string | TaskDefinition<any, any>): TaskRef<any, any> {
    const definition = typeof value === "string" ? this.#harness.task(value) : this.#harness.task(value);
    this.#assertTask(definition);
    return publicTaskHandle(definition);
  }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>): ToolRef<Input, Output>;
  tool(name: string): ToolRef<unknown, unknown>;
  tool(value: string | ToolDefinition<any, any>): ToolRef<any, any> {
    const tool = typeof value === "string" ? this.#harness.tool(value) : this.#harness.tool(value);
    const capability = `tool:call:${tool.definition.name}`;
    if (!this.#harness.scope.grants.includes(capability)) throw new Error(`task scope lacks ${capability}`);
    return tool;
  }
  #assertTask<Input, Output>(definition: TaskDefinition<Input, Output>): void {
    this.#harness.task(definition);
    const capability = `task:spawn:${taskKey(definition.name, definition.revision)}`;
    if (!this.#harness.scope.grants.includes(capability)) throw new Error(`task scope lacks ${capability}`);
  }
  /** Resolve exact bytes through the owner's authenticated provider. A ref is not a grant. */
  async readFile(reference: FileRef): Promise<Uint8Array> {
    return this.#readBoundFile(this.#harness.content, reference);
  }
  /** Resolve immutable artifact bytes through an independently bound owner. */
  async readArtifact(reference: FileRef): Promise<Uint8Array> {
    return this.#readBoundFile(this.#harness.artifacts, reference);
  }
  /** Discover an attached owner's private tree lazily under a signed read boundary. */
  async listPrivateDirectory(volume: VolumeRef<"agent_private">, grantedPrefix: string,
    path: string, expectedGeneration: ResourceRef<"generation"> | null = null, after: string | null = null,
    maximumEntries = 256): Promise<PrivateDirectoryPage> {
    this.signal.throwIfAborted();
    const content = this.#harness.content;
    if (!content?.directory) throw new Error("private directory provider is not bound");
    const checked = this.#harness.contracts.validate("volume_ref", volume);
    if (checked.class !== "agent_private") throw new TypeError("directory discovery requires an agent-private volume");
    if (!this.#harness.scope.grants.includes(content.directoryReadCapability(checked, grantedPrefix))
      && !this.#harness.scope.grants.includes(content.volumeReadCapability(checked))) {
      throw new Error("task scope cannot discover this directory");
    }
    const page = this.#harness.contracts.validate("private_directory_page",
      await content.directory.list(checked, grantedPrefix, path, expectedGeneration, after, maximumEntries));
    if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > 4096
      || page.entries.length > maximumEntries
      || !this.#harness.contracts.canonicalEqual(page.generation.provider, checked.provider)
      || (expectedGeneration !== null && !this.#harness.contracts.canonicalEqual(page.generation, expectedGeneration))) {
      throw new TypeError("private directory provider returned an invalid page");
    }
    const entries = page.entries.map(entry => {
      if (typeof entry.name !== "string" || !entry.name || entry.name === "." || entry.name === ".."
        || entry.name.includes("/") || entry.name.includes("\\") || entry.name.includes("\0")
        || (after !== null && compareUtf8(entry.name, after) <= 0)
        || (!path && entry.name === ".system")
        || (entry.kind !== "file" && entry.kind !== "directory")) {
        throw new TypeError("private directory provider returned an invalid entry");
      }
      return { name: entry.name, kind: entry.kind };
    });
    return Object.freeze({ generation: page.generation, entries: Object.freeze(entries), hasMore: page.hasMore });
  }
  /** Resolve a named owner-private path to verified immutable bytes. */
  async readPrivatePath(volume: VolumeRef<"agent_private">, grantedPrefix: string,
    path: string, expectedGeneration: ResourceRef<"generation"> | null = null): Promise<Readonly<{ file: FileRef; bytes: Uint8Array }>> {
    this.signal.throwIfAborted();
    const content = this.#harness.content;
    if (!content?.directory) throw new Error("private directory provider is not bound");
    const checked = this.#harness.contracts.validate("volume_ref", volume);
    if (checked.class !== "agent_private") throw new TypeError("directory discovery requires an agent-private volume");
    if (!this.#harness.scope.grants.includes(content.directoryReadCapability(checked, grantedPrefix))
      && !this.#harness.scope.grants.includes(content.volumeReadCapability(checked))) {
      throw new Error("task scope cannot discover this path");
    }
    const resolved = await content.directory.readPath(checked, grantedPrefix, path, expectedGeneration);
    const file = this.#harness.contracts.validate("file_ref", resolved.file);
    if (!this.#harness.contracts.canonicalEqual(file.volume, checked) || file.path !== path
      || !(resolved.bytes instanceof Uint8Array)) {
      throw new TypeError("private path provider returned another file");
    }
    const bytes = Uint8Array.from(resolved.bytes);
    content.validate(file, this.#harness.limits);
    content.verify(file, bytes);
    this.#harness.contracts.verifyFileBytes(file, bytes);
    return { file, bytes };
  }
  async #readBoundFile(content: ContentBindings | undefined, reference: FileRef): Promise<Uint8Array> {
    this.signal.throwIfAborted();
    if (!content) throw new Error("content reader is not bound");
    const file = this.#harness.contracts.validate("file_ref", reference);
    content.validate(file, this.#harness.limits);
    const grants = this.#harness.scope.grants;
    const volumeGrant = content.volumeReadCapability(file.volume);
    const exactGrant = content.fileReadCapability(file);
    const segments = file.path.split("/");
    let allowed = grants.includes(volumeGrant) || grants.includes(exactGrant);
    if (!allowed && file.volume.class === "agent_private") {
      allowed = grants.includes(content.directoryReadCapability(file.volume, ""));
    }
    for (let length = 1; !allowed && length < segments.length; length++) {
      const prefix = segments.slice(0, length).join("/");
      allowed = grants.includes(content.directoryReadCapability(file.volume, prefix));
    }
    if (!allowed) throw new Error("task scope cannot read this file");
    const bytes = await content.read(file);
    if (!(bytes instanceof Uint8Array)) throw new TypeError("content reader returned invalid bytes");
    content.verify(file, bytes);
    await verifyFileBytes(file, bytes);
    return Uint8Array.from(bytes);
  }
  /** Stage immutable bytes under a stable operation ID before publishing their ref. */
  async stageFile(operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    return this.#stageBoundFile(this.#harness.content, operationId, path, bytes, mediaType, displayName);
  }
  /** Stage an immutable artifact independently of the conversation file store. */
  async stageArtifact(operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    return this.#stageBoundFile(this.#harness.artifacts, operationId, path, bytes, mediaType, displayName);
  }
  async #stageBoundFile(content: ContentBindings | undefined, operationId: string, path: string,
    bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    this.signal.throwIfAborted();
    if (!operationId.trim() || !(bytes instanceof Uint8Array)) throw new TypeError("stable upload identity and bytes are required");
    const payload = Uint8Array.from(bytes);
    if (payload.byteLength > this.#harness.limits.file_bytes || new TextEncoder().encode(path).byteLength > this.#harness.limits.path_bytes) {
      throw new TypeError("file exceeds task content limits");
    }
    if (!content?.writer) throw new Error("task has no owner-bound content writer");
    if (!this.#harness.scope.grants.includes(content.writer.writeCapability())) throw new Error("task scope cannot write this volume");
    const file = this.#harness.contracts.validate("file_ref",
      await content.writer.stage(operationId, path, payload, mediaType, displayName));
    content.validate(file, this.#harness.limits);
    if (!this.#harness.contracts.canonicalEqual(file.volume, content.writer.volume)
      || file.path !== path || file.display_name !== displayName || file.descriptor.media_type !== mediaType) {
      throw new TypeError("content publisher returned another file identity");
    }
    content.verify(file, payload);
    await verifyFileBytes(file, payload);
    const resident = await content.read(file);
    if (!(resident instanceof Uint8Array)) throw new TypeError("content reader returned invalid bytes");
    content.verify(file, resident);
    await verifyFileBytes(file, resident);
    return file;
  }
  call<Input, Output>(tool: ToolRef<Input, Output>, input: Input, operationId?: string, providerCallId?: string): Promise<Output> {
    if (this.durable) throw new Error("durable tools require callDurable and an explicit outcome");
    return this.#harness.call(tool, input, this.signal, this.taskId, operationId, providerCallId);
  }
  callDurable<Input, Output>(operationId: OperationId, tool: ToolRef<Input, Output>, input: Input): Promise<Outcome<Output, OperationId>> {
    if (!this.durable || !this.taskId) throw new Error("durable tool calls require an admitted task");
    return this.#harness.callDurable(operationId, tool, input, this.taskId, this.signal);
  }
  spawn<Input, Output>(task: TaskRef<Input, Output>, input: Input): Task<Output> {
    if (this.durable) throw new Error("durable descendants require admitted tasks and stable operation IDs");
    const definition = registeredTaskDefinition(task);
    this.#assertTask(definition);
    return this.#harness.spawn(definition, input);
  }
  admit<Input, Output>(task: TaskRef<Input, Output>, input: Input, operationId: string): Promise<Admission<Output>> {
    if (!this.durable || !this.taskId) throw new Error("a live task cannot claim durable descendant ownership");
    const definition = registeredTaskDefinition(task);
    this.#assertTask(definition);
    return this.#harness.admit(definition, input, operationId, this.taskId);
  }
  group<Output>(policy: GroupPolicy, id?: GroupId): TaskGroup<Output, "scoped"> {
    if (this.durable && (!this.taskId || !id)) throw new Error("durable task groups require stable group and parent identities");
    return new TaskGroup<Output, "scoped">(this.#harness, policy, id, this.durable ? this.taskId : undefined,
      definition => this.#assertTask(definition));
  }
  async interact(interaction: Interaction): Promise<Answer> {
    if (this.durable) {
      const state = this.#harness.state;
      if (!state || !this.taskId) throw new Error("durable interaction requires state and task identity");
      const id = await interactionId(interaction.id);
      if (interaction.kind === "approval") {
        if (!interaction.operationId || !interaction.actionDigest) throw new Error("durable approval requires an exact action binding");
        await approvalBinding({ operation_id: interaction.operationId, action_digest: interaction.actionDigest });
      }
      const route = state.routeInteraction;
      if (!route) throw new Error("durable interaction routing is not bound");
      return route.call(state, this.taskId, id, { ...interaction, id });
    }
    const handler = this.#harness.interactions;
    if (!handler) throw new Error("no local interaction handler is bound");
    return handler.route(interaction);
  }
  /** Admission and typed parsing are explicit; an answer never inherits a caller-chosen type. */
  async interactTyped<Value>(interaction: TypedInteractionInput,
    schema: RuntimeSchema<Value>): Promise<Answer<Value>> {
    const request: Interaction = interaction.kind === "question"
      ? { ...interaction, responseSchema: schema.document }
      : { ...interaction, schema: schema.document };
    const answer = await this.interact(request);
    if (answer.kind !== "answered") return answer;
    const admitted = this.#harness.contracts.validateToolValue(schema.document, answer.value);
    return { kind: "answered", value: schema.parse(admitted) };
  }
  async ask(prompt: string, operationId?: string): Promise<Answer> {
    if (this.durable && !operationId?.trim()) throw new Error("hosted interactions require a stable operation ID");
    return this.interact({ id: operationId ?? crypto.randomUUID(), kind: "question", prompt });
  }
  async reconcileEffect(effectId: EffectId): Promise<EffectStatus> {
    if (!this.taskId) throw new Error("effect reconciliation requires a task identity");
    const host = this.#harness.state;
    if (!host) throw new Error("no durable task state is bound");
    const status = await host.reconcileEffect(this.taskId, effectId);
    switch (status.state) {
      case "succeeded": {
        const result = this.#harness.contracts.validate("file_ref", status.result);
        return { state: "succeeded", result };
      }
      case "failed":
        if (!status.message.trim()) throw new TypeError("effect failure message is empty");
        return { state: "failed", message: status.message };
      case "planned": case "dispatched": case "indeterminate": return { state: status.state };
      default: throw new TypeError("host returned an invalid effect status");
    }
  }
  send(recipient: RuntimeTaskId, value: FileRef, messageId?: MessageId): Promise<{ readonly accepted: boolean; readonly messageId: MessageId }> {
    const host = this.#harness.state;
    if (!host) throw new Error("no durable task state is bound");
    if (this.durable && !messageId) throw new Error("durable messages require a stable message ID");
    return host.send({ id: messageId ?? identity<MessageId>(), recipient,
      value: this.#harness.contracts.validate("file_ref", value),
      ...(this.taskId === undefined ? {} : { sender: this.taskId }) });
  }
  inbox(from?: MessageId): AsyncIterable<TaskMessage> {
    if (!this.taskId) throw new Error("task inbox requires a durable task identity");
    const host = this.#harness.state;
    if (!host) throw new Error("no durable task state is bound");
    return host.inbox(this.taskId, from);
  }
  async sleepUntil(deadline: Date, operationId?: string): Promise<void> {
    if (!Number.isFinite(deadline.getTime())) throw new TypeError("timer deadline is invalid");
    if (this.durable) {
      const state = this.#harness.state;
      if (!state || !this.taskId || !operationId?.trim()) throw new Error("durable timers require state, task identity, and stable operation ID");
      const wait = state.waitUntil;
      if (!wait) throw new Error("durable timer routing is not bound");
      return wait.call(state, this.taskId, operationId, deadline);
    }
    const milliseconds = deadline.getTime() - Date.now();
    if (milliseconds <= 0) return;
    await abortableWait(milliseconds, this.signal);
  }
}
export class ToolContext extends TaskContext { constructor(harness: AgentHarness, signal: AbortSignal, readonly callId: string, taskId?: RuntimeTaskId, durable = harness.state !== undefined, readonly operationId?: string) { super(harness, signal, taskId, durable); } }

interface RegisteredTool<Input, Output> {
  readonly definition: ToolDefinition<Input, Output>;
  readonly executor?: ToolExecutor<Input, Output>;
  readonly machine?: MachineIdentityWire;
}
const publicToolHandles = new WeakMap<RegisteredTool<any, any>, ToolRef<any, any>>();
function publicToolHandle<Input, Output>(registered: RegisteredTool<Input, Output>): ToolRef<Input, Output> {
  const cached = publicToolHandles.get(registered);
  if (cached !== undefined) return cached as ToolRef<Input, Output>;
  const { name, revision, description, inputSchema, outputSchema } = registered.definition;
  const handle = Object.freeze({ definition: Object.freeze({ name, revision, description, inputSchema, outputSchema }),
    ...(registered.machine ? { machine: registered.machine } : {}) }) as ToolRef<Input, Output>;
  publicToolHandles.set(registered, handle);
  return handle;
}

/** Replaceable owner-mediated file boundary, independent of concrete storage. */
export interface ContentBindings {
  /** Exact native-contract validation before reading or publishing a ref. */
  readonly validate: (file: FileRef, limits: Limits) => void;
  /** Native digest and length check for bytes returned by the owner. */
  readonly verify: (file: FileRef, bytes: Uint8Array) => void;
  readonly read: (file: FileRef) => Promise<Uint8Array>;
  readonly fileReadCapability: (file: FileRef) => string;
  readonly volumeReadCapability: (volume: VolumeRef) => string;
  readonly directoryReadCapability: (volume: VolumeRef, prefix: string) => string;
  readonly directory?: PrivateDirectoryBindings;
  readonly writer?: Readonly<{
    readonly volume: VolumeRef;
    readonly writeCapability: () => string;
    readonly stage: (operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string) => Promise<FileRef>;
  }>;
}

export interface PrivateDirectoryPage {
  readonly generation: ResourceRef<"generation">;
  readonly entries: readonly Readonly<{ name: string; kind: "file" | "directory" }>[];
  readonly hasMore: boolean;
}

/** Owner-authenticated lazy discovery. Generation and cursor pin one page walk. */
export interface PrivateDirectoryBindings {
  list(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string,
    expectedGeneration: ResourceRef<"generation"> | null, after: string | null, maximumEntries: number): Promise<PrivateDirectoryPage>;
  readPath(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string,
    expectedGeneration: ResourceRef<"generation"> | null): Promise<Readonly<{ file: FileRef; bytes: Uint8Array }>>;
}

export type ContentReader = Pick<ContentBindings,
  "validate" | "verify" | "read" | "fileReadCapability" | "volumeReadCapability" | "directoryReadCapability" | "directory">;

/** Resolve an unseen owner volume on demand. The owner must check the exact ref
 * against a signed grant on every use; capability labels alone confer no read. */
export interface ContentMountResolver {
  mount(volume: VolumeRef): ContentReader & Readonly<{
    requireFileRead(file: FileRef): void;
    requireDirectoryPath(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string): void;
  }>
}

function pinContentReader(source: ContentReader): ContentReader {
  return Object.freeze({
    validate: source.validate.bind(source),
    verify: source.verify.bind(source),
    read: source.read.bind(source),
    fileReadCapability: source.fileReadCapability.bind(source),
    volumeReadCapability: source.volumeReadCapability.bind(source),
    directoryReadCapability: source.directoryReadCapability.bind(source),
    ...(source.directory === undefined ? {} : { directory: Object.freeze({
      list: source.directory.list.bind(source.directory),
      readPath: source.directory.readPath.bind(source.directory),
    }) }),
  });
}

/** Capture an owner's routing methods and writer identity at registration. */
function pinContentBindings(contracts: NativeContracts, source: ContentBindings): ContentBindings {
  const writer = source.writer;
  return Object.freeze({
    ...pinContentReader(source),
    ...(writer === undefined ? {} : { writer: Object.freeze({
      volume: contracts.validate("volume_ref", writer.volume),
      writeCapability: writer.writeCapability.bind(writer),
      stage: writer.stage.bind(writer),
    }) }),
  });
}

/** Route exact owner volumes without giving agents raw storage handles. */
export function composeContentBindings(contracts: NativeContracts, sources: readonly Readonly<{
  volume: VolumeRef; content: ContentBindings;
}>[], mount?: ContentMountResolver): ContentBindings {
  if (sources.length === 0 && mount === undefined) throw new TypeError("at least one content volume or mount is required");
  const volumes = new Map<string, ContentBindings>();
  let writer: ContentBindings["writer"];
  const decoder = new TextDecoder();
  const key = (volume: VolumeRef): string => {
    const checked = contracts.validate("volume_ref", volume);
    return decoder.decode(contracts.encodeCanonicalJson(checked));
  };
  for (const source of sources) {
    const id = key(source.volume);
    if (volumes.has(id)) throw new TypeError("content volume is registered twice");
    const content = pinContentBindings(contracts, source.content);
    volumes.set(id, content);
    if (content.writer !== undefined) {
      if (writer !== undefined || key(content.writer.volume) !== id) {
        throw new TypeError("content writer is ambiguous or bound to another volume");
      }
      writer = content.writer;
    }
  }
  const owner = (volume: VolumeRef, file?: FileRef): ContentReader => {
    const binding = volumes.get(key(volume));
    if (binding) return binding;
    if (!mount) throw new Error("content volume is not registered");
    const mounted = mount.mount(volume);
    if (file !== undefined) mounted.requireFileRead(file);
    return pinContentReader(mounted);
  };
  const directoryOwner = (volume: VolumeRef<"agent_private">, prefix: string, path: string): ContentReader => {
    const binding = volumes.get(key(volume));
    if (binding) return binding;
    if (!mount) throw new Error("content volume is not registered");
    const mounted = mount.mount(volume);
    mounted.requireDirectoryPath(volume, prefix, path);
    return pinContentReader(mounted);
  };
  return Object.freeze({
    validate: (file: FileRef, limits: Limits) => owner(file.volume, file).validate(file, limits),
    verify: (file: FileRef, bytes: Uint8Array) => owner(file.volume, file).verify(file, bytes),
    // Keep authorization failures on the asynchronous content boundary. A
    // lazy mount may reject before it returns a reader; callers already
    // consume `read` as a Promise and should observe that as a rejection,
    // never as an unexpected synchronous throw.
    read: async (file: FileRef) => owner(file.volume, file).read(file),
    fileReadCapability: (file: FileRef) => owner(file.volume, file).fileReadCapability(file),
    volumeReadCapability: (volume: VolumeRef) => owner(volume).volumeReadCapability(volume),
    directoryReadCapability: (volume: VolumeRef, prefix: string) => owner(volume).directoryReadCapability(volume, prefix),
    directory: Object.freeze({
      list: async (volume: VolumeRef<"agent_private">, prefix: string, path: string,
        generation: ResourceRef<"generation"> | null, after: string | null, maximum: number) => {
        const reader = directoryOwner(volume, prefix, path).directory;
        if (!reader) throw new Error("private directory provider is not bound");
        return reader.list(volume, prefix, path, generation, after, maximum);
      },
      readPath: async (volume: VolumeRef<"agent_private">, prefix: string, path: string,
        generation: ResourceRef<"generation"> | null) => {
        const reader = directoryOwner(volume, prefix, path).directory;
        if (!reader) throw new Error("private directory provider is not bound");
        return reader.readPath(volume, prefix, path, generation);
      },
    }),
    ...(writer === undefined ? {} : { writer }),
  });
}

function pinForkPreparer(contracts: NativeContracts, value: ForkPreparer): ForkPreparer {
  const snapshot = value.parentSnapshot();
  if (snapshot.parent.kind !== "conversation" || typeof snapshot.parent.id !== "string" || !snapshot.parent.id
    || /[\\/\x00-\x1f]/.test(snapshot.parent.id) || snapshot.parent.id === "." || snapshot.parent.id === ".."
    || typeof snapshot.revision !== "bigint" || snapshot.revision <= 0n) {
    throw new TypeError("fork preparer needs a bound parent conversation revision");
  }
  const pinned = Object.freeze({ parent: Object.freeze({ ...snapshot.parent }), revision: snapshot.revision });
  const assertPinned = (): void => {
    const current = value.parentSnapshot();
    if (current.revision !== pinned.revision || !contracts.canonicalEqual(current.parent, pinned.parent)) {
      throw new Error("fork preparer parent projection changed after binding");
    }
  };
  return Object.freeze({
    parentSnapshot: () => pinned,
    prepare: (request: ForkRequest) => { assertPinned(); return value.prepare(request); },
    reconcile: (request: ForkRequest) => { assertPinned(); return value.reconcile(request); },
  });
}

function pinForkPublisher(contracts: NativeContracts, value: ForkPublisher): ForkPublisher {
  const parent = value.parent();
  if (parent.kind !== "conversation" || typeof parent.id !== "string" || !parent.id
    || /[\\/\x00-\x1f]/.test(parent.id) || parent.id === "." || parent.id === "..") {
    throw new TypeError("fork publisher needs a bound parent conversation");
  }
  const pinned = Object.freeze({ ...parent });
  const assertPinned = (): void => {
    if (!contracts.canonicalEqual(value.parent(), pinned)) {
      throw new Error("fork publisher parent changed after binding");
    }
  };
  return Object.freeze({
    parent: () => pinned,
    spawnFromReport: (report: ForkReport) => { assertPinned(); return value.spawnFromReport(report); },
    reconcileSpawn: (operationId: OperationId) => { assertPinned(); return value.reconcileSpawn(operationId); },
  });
}

/** Replaceable runtime boundaries; registries remain versioned builder entries. */
export interface HarnessBindings {
  readonly model?: Readonly<{ identity: Model; provider: ModelProvider }>;
  readonly agentLoop?: AgentLoop;
  readonly context?: ContextBuilder;
  readonly interactions?: InteractionHandler;
  readonly interactionResolver?: InteractionResolver;
  readonly policy?: Policy;
  readonly host?: HarnessRuntimeHost;
  readonly state?: HarnessRuntimeState;
  readonly spawner?: HarnessRuntimeSpawner;
  readonly execution?: HarnessExecutionProvider;
  readonly content?: ContentBindings;
  readonly artifacts?: ContentBindings;
  readonly forkPreparer?: ForkPreparer;
  readonly forkPublisher?: ForkPublisher;
  readonly workspaces?: ProjectWorkspaceProvider;
  readonly grants?: readonly string[];
  readonly limits?: Partial<Limits>;
}

export class HarnessBuilder {
  constructor(readonly contracts: NativeContracts) {}
  readonly #tasks = new Map<string, TaskDefinition<any, any>>(); readonly #tools = new Map<string, RegisteredTool<any, any>>();
  readonly #toolSources = new Map<string, ToolDefinition<any, any>>();
  readonly #selectedTools = new Map<string, string>();
  #model?: BoundModel; #loop?: AgentLoop; #context?: ContextBuilder; #interactions?: InteractionHandler; #interactionResolver?: InteractionResolver; #policy?: Policy; #host?: HarnessRuntimeHost; #state?: HarnessRuntimeState; #spawner?: HarnessRuntimeSpawner; #execution?: HarnessExecutionProvider; #content?: ContentBindings; #artifacts?: ContentBindings; #forkPreparer?: ForkPreparer; #forkPublisher?: ForkPublisher; #workspaces?: ProjectWorkspaceProvider;
  readonly #grants: string[] = [];
  #limits: Limits = DEFAULT_LIMITS;
  /** Bind independent providers together without creating a second registry. */
  bindings(value: HarnessBindings): this {
    if (value.model) this.model(value.model.identity, value.model.provider);
    if (value.agentLoop) this.agentLoop(value.agentLoop);
    if (value.context) this.context(value.context);
    if (value.interactions) this.interactions(value.interactions);
    if (value.interactionResolver) this.interactionResolver(value.interactionResolver);
    if (value.policy) this.policy(value.policy);
    if (value.host) this.host(value.host);
    if (value.state) this.state(value.state);
    if (value.spawner) this.spawner(value.spawner);
    if (value.execution) this.execution(value.execution);
    if (value.content) this.content(value.content);
    if (value.artifacts) this.artifacts(value.artifacts);
    if (value.forkPreparer) this.forkPreparer(value.forkPreparer);
    if (value.forkPublisher) this.forkPublisher(value.forkPublisher);
    if (value.workspaces) this.workspaces(value.workspaces);
    if (value.grants) this.grant(...value.grants);
    if (value.limits) this.limits(value.limits);
    return this;
  }
  model(identity: Model, provider: ModelProvider): this { this.contracts.encodeCanonicalJson(identity.options); this.#model = bindModel(identity, provider); return this; }
  agentLoop(value: AgentLoop): this { this.#loop = value; return this; }
  context(value: ContextBuilder): this { this.#context = value; return this; }
  interactions(value: InteractionHandler): this { this.#interactions = value; return this; }
  interactionResolver(value: InteractionResolver): this { this.#interactionResolver = value; return this; }
  policy(value: Policy): this { this.#policy = value; return this; }
  host(value: HarnessRuntimeHost): this { this.#host = value; return this; }
  state(value: HarnessRuntimeState): this { this.#state = value; return this; }
  spawner(value: HarnessRuntimeSpawner): this { this.#spawner = value; return this; }
  execution(value: HarnessExecutionProvider): this { this.#execution = value; return this; }
  content(value: ContentBindings): this { this.#content = pinContentBindings(this.contracts, value); return this; }
  artifacts(value: ContentBindings): this { this.#artifacts = pinContentBindings(this.contracts, value); return this; }
  forkPreparer(value: ForkPreparer): this { this.#forkPreparer = pinForkPreparer(this.contracts, value); return this; }
  forkPublisher(value: ForkPublisher): this { this.#forkPublisher = pinForkPublisher(this.contracts, value); return this; }
  workspaces(value: ProjectWorkspaceProvider): this {
    const project = this.contracts.validate("volume_ref", value.project);
    const provider = this.contracts.validate("provider_ref", value.provider);
    if (project.class !== "project" || !this.contracts.canonicalEqual(project.provider, provider)) {
      throw new TypeError("workspace binding is not a project");
    }
    this.#workspaces = value;
    return this;
  }
  grant(...values: readonly string[]): this { if (values.some(value => !value.trim())) throw new TypeError("capability grant is empty"); this.#grants.push(...values); return this; }
  limits(value: Partial<Limits>): this { this.#limits = this.contracts.validate("limits", { ...DEFAULT_LIMITS, ...value }); return this; }
  task<Input, Output>(value: TaskDefinition<Input, Output>): this {
    const key = taskKey(value.name, value.revision);
    if (this.#tasks.has(key)) throw new Error(`conflicting registration for ${key}`);
    for (const schema of [value.options.input, value.options.output,
      value.implementation.kind === "resumable" ? value.implementation.component.state : undefined]) {
      if (schema !== undefined) this.contracts.encodeCanonicalJson(schema.document);
    }
    this.#tasks.set(key, value);
    return this;
  }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>, executor?: ToolExecutor<Input, Output>): this {
    if (!executor && !definition.handler) throw new TypeError("tool requires a handler or executor");
    return this.#registerTool(definition, executor);
  }
  /** Register a pinned durable-only tool; its owner host executes the machine. */
  resumableTool<Input, Output>(definition: ToolDefinition<Input, Output>, machine: MachineIdentityWire): this {
    const pinned = this.contracts.validate("machine_identity", machine);
    if (definition.handler || pinned.name !== definition.name || pinned.version !== definition.revision) {
      throw new TypeError("resumable tool machine must match a handler-free definition");
    }
    return this.#registerTool(definition, undefined, pinned);
  }
  #registerTool<Input, Output>(definition: ToolDefinition<Input, Output>, executor?: ToolExecutor<Input, Output>, machine?: MachineIdentityWire): this {
    validateToolName(definition.name);
    validateComponentLabel(definition.revision, "tool revision");
    if (typeof definition.parseInput !== "function" || typeof definition.parseOutput !== "function") {
      throw new TypeError("typed tool parsers are required");
    }
    this.contracts.encodeCanonicalJson(definition.inputSchema);
    this.contracts.encodeCanonicalJson(definition.outputSchema);
    const key = toolKey(definition.name, definition.revision);
    if (this.#tools.has(key)) throw new Error(`conflicting registration for ${key}`);
    const previouslyRegistered = [...this.#tools.values()].some(tool => tool.definition.name === definition.name);
    const pinned = Object.freeze({ ...definition,
      inputSchema: freezeSchema(structuredClone(definition.inputSchema)),
      outputSchema: freezeSchema(structuredClone(definition.outputSchema)) });
    this.#tools.set(key, Object.freeze({ definition: pinned,
      ...(executor ? { executor } : {}), ...(machine ? { machine } : {}) }));
    this.#toolSources.set(key, definition);
    if (previouslyRegistered) this.#selectedTools.delete(definition.name);
    else this.#selectedTools.set(definition.name, definition.revision);
    return this;
  }
  /** Choose exactly one revision for model-visible calls of a shared tool name. */
  selectModelTool(name: string, revision: string): this {
    if (!this.#tools.has(toolKey(name, revision))) throw new Error(`tool revision is not registered: ${name}@${revision}`);
    this.#selectedTools.set(name, revision);
    return this;
  }
  build(): AgentHarness { const components: AgentHarnessComponents = { limits: this.#limits }; if (this.#model) components.model = this.#model; if (this.#loop) components.loop = this.#loop; if (this.#context) components.context = this.#context; if (this.#interactions) components.interactions = this.#interactions; if (this.#interactionResolver) components.interactionResolver = this.#interactionResolver; if (this.#policy) components.policy = this.#policy; if (this.#host) components.host = this.#host; if (this.#execution) { if (this.#host) throw new Error("execution route conflicts with legacy durable host"); components.execution = this.#execution; components.state = this.#execution.state(); components.spawner = this.#execution.spawner(); } if (this.#state) components.state = this.#state; if (this.#spawner) components.spawner = this.#spawner; if (this.#content) components.content = this.#content; if (this.#artifacts) components.artifacts = this.#artifacts; if (this.#workspaces) {
    if (!this.#grants.includes("fork:publish") && !this.#grants.includes("project:merge")) throw new TypeError("project workspaces require fork:publish or project:merge");
    components.workspaces = this.#workspaces;
  } if (this.#forkPreparer) {
    if (!this.#grants.includes("fork:publish")) throw new TypeError("fork preparer requires fork:publish in the root scope");
    components.forkPreparer = this.#forkPreparer;
  } if (this.#forkPublisher) {
    if (!this.#grants.includes("fork:publish") || !this.#forkPreparer) throw new TypeError("fork publisher requires parent preparation authority");
    if (!this.contracts.canonicalEqual(this.#forkPublisher.parent(), this.#forkPreparer.parentSnapshot().parent)) {
      throw new TypeError("fork publisher belongs to another parent");
    }
    components.forkPublisher = this.#forkPublisher;
  } if ([...this.#tools.values()].some(tool => tool.machine) && !components.state?.executeTool) {
    throw new TypeError("resumable tools require owner-host durable tool execution");
  } validateTaskRequirements(this.#tasks, this.#tools, components, this.#grants); return new AgentHarness(this.#tasks, this.#tools, components, ExecutionScope.create().grant(...this.#grants), new Map(), this.#selectedTools, this.#toolSources, this.contracts, harnessConstruction); }
}

interface AgentHarnessComponents { model?: BoundModel; loop?: AgentLoop; context?: ContextBuilder; interactions?: InteractionHandler; interactionResolver?: InteractionResolver; policy?: Policy; host?: HarnessRuntimeHost; state?: HarnessRuntimeState; spawner?: HarnessRuntimeSpawner; execution?: HarnessExecutionProvider; content?: ContentBindings; artifacts?: ContentBindings; forkPreparer?: ForkPreparer; forkPublisher?: ForkPublisher; workspaces?: ProjectWorkspaceProvider; limits?: Limits }
const harnessConstruction = Symbol("HarnessBuilder-owned construction");
export class AgentHarness {
  readonly #tasks: ReadonlyMap<string, TaskDefinition<any, any>>; readonly #tools: ReadonlyMap<string, RegisteredTool<any, any>>;
  readonly #selectedTools: ReadonlyMap<string, string>;
  readonly #toolSources: ReadonlyMap<string, ToolDefinition<any, any>>;
  readonly #policyIdentity: PolicyIdentity | null;
  readonly components: Readonly<AgentHarnessComponents>;
  readonly #contentLimits: Limits;
  constructor(tasks: ReadonlyMap<string, TaskDefinition<any, any>>, tools: ReadonlyMap<string, RegisteredTool<any, any>>, components: AgentHarnessComponents, readonly scope: ExecutionScope, readonly running: Map<RuntimeTaskId, Task<unknown>>, selectedTools: ReadonlyMap<string, string>, toolSources: ReadonlyMap<string, ToolDefinition<any, any>>, readonly contracts: NativeContracts, construction: typeof harnessConstruction) {
    if (construction !== harnessConstruction) throw new TypeError("AgentHarness must be created through HarnessBuilder or scoped()");
    this.#tasks = new Map(tasks);
    this.#tools = new Map(tools);
    this.#selectedTools = new Map(selectedTools);
    this.#toolSources = new Map(toolSources);
    this.#contentLimits = contracts.validate("limits", components.limits ?? DEFAULT_LIMITS);
    this.components = Object.freeze({ ...components, limits: this.#contentLimits });
    this.#policyIdentity = validatePolicyIdentity((scope.policyProvider ?? components.policy)?.identity() ?? null);
    if (components.host && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(components.host.policyIdentity()))) {
      throw new Error("runtime policy identity differs from durable host policy");
    }
    if (components.state && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(components.state.policyIdentity()))) {
      throw new Error("runtime policy identity differs from durable state policy");
    }
    if (components.spawner && !components.state && !components.host) throw new Error("durable spawner requires a state binding");
    if (components.spawner && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(components.spawner.policyIdentity()))) {
      throw new Error("runtime policy identity differs from durable spawner policy");
    }
    this.#assertExecutionIdentity();
  }
  get interactions(): InteractionHandler | undefined { return this.scope.interactionHandler ?? this.components.interactions; }
  projectWorkspaces(): ProjectWorkspaceProvider {
    if (!this.scope.grants.includes("fork:publish") && !this.scope.grants.includes("project:merge")) {
      throw new Error("runtime scope lacks project authority");
    }
    if (!this.components.workspaces) throw new Error("project workspace provider is not bound");
    return this.components.workspaces;
  }
  /** Reads the authoritative version before constructing a responder reply. */
  inspectInteraction(scope: Scope, id: InteractionId) {
    const resolver = this.components.interactionResolver;
    if (!resolver) throw new Error("interaction resolver is not bound");
    return resolver.inspect(scope, id);
  }
  async resolveAnswer(operationId: OperationId, scope: Scope, id: InteractionId, expectedVersion: bigint,
    response: Exclude<InteractionResponse, { type: "approval" }>): Promise<ResolutionReceipt> {
    const resolver = this.components.interactionResolver;
    if (!resolver) throw new Error("interaction resolver is not bound");
    const receipt = this.contracts.validate("resolution_receipt",
      await resolver.resolveAnswer(operationId, scope, id, expectedVersion, response));
    if (receipt.id !== id || receipt.operation_id !== operationId || receipt.version !== expectedVersion
      || receipt.outcome.kind !== "answered") throw new Error("interaction resolver returned another answer");
    return receipt;
  }
  async resolveApproval(operationId: OperationId, scope: Scope, id: InteractionId, expectedVersion: bigint,
    approved: boolean, reason: string | null = null): Promise<ResolutionReceipt> {
    const resolver = this.components.interactionResolver;
    if (!resolver) throw new Error("interaction resolver is not bound");
    const receipt = this.contracts.validate("resolution_receipt",
      await resolver.resolveApproval(operationId, scope, id, expectedVersion, approved, reason));
    if (receipt.id !== id || receipt.operation_id !== operationId || receipt.version !== expectedVersion
      || receipt.outcome.kind !== (approved ? "approved" : "declined")) {
      throw new Error("interaction resolver returned another decision");
    }
    return receipt;
  }
  get host(): HarnessRuntimeHost | undefined { return this.components.host; }
  get state(): HarnessRuntimeState | undefined { return this.components.state ?? this.components.host; }
  get spawner(): HarnessRuntimeSpawner | undefined { return this.components.spawner ?? this.components.host; }
  get content(): ContentBindings | undefined { return this.components.content; }
  get artifacts(): ContentBindings | undefined { return this.components.artifacts; }
  get limits(): Limits { return this.#contentLimits; }
  /** Rust's durable admission pins one policy implementation, not an unrecorded composition. */
  durablePolicyIdentity(): ComponentIdentity | null {
    if (this.#policyIdentity !== null && "composition" in this.#policyIdentity) {
      throw new Error("durable batch policy composition requires a pinned owner policy");
    }
    return this.#policyIdentity;
  }
  /** Create a new view for a later parent revision without retargeting admitted tasks. */
  atParentSnapshot(preparer: ForkPreparer): AgentHarness {
    if (!this.scope.grants.includes("fork:publish")) throw new Error("runtime scope lacks fork:publish");
    const current = this.components.forkPreparer?.parentSnapshot();
    if (current) {
      const next = preparer.parentSnapshot();
      if (!this.contracts.canonicalEqual(current.parent, next.parent)) {
        throw new Error("fork snapshot belongs to another parent conversation");
      }
      if (next.revision <= current.revision) throw new Error("fork snapshot must advance the parent revision");
    } else throw new Error("parent fork control is not bound");
    return new AgentHarness(this.#tasks, this.#tools,
      { ...this.components, forkPreparer: pinForkPreparer(this.contracts, preparer) },
      this.scope, this.running, this.#selectedTools, this.#toolSources, this.contracts, harnessConstruction);
  }
  #checkedFork(request: ForkRequest): { preparer: ForkPreparer; checked: ForkRequest } {
    if (!this.scope.grants.includes("fork:publish")) throw new Error("runtime scope lacks fork:publish");
    const preparer = this.components.forkPreparer;
    if (!preparer) throw new Error("fork preparer is not bound");
    const checked = this.contracts.validate("fork_request", request);
    const snapshot = preparer.parentSnapshot();
    if (checked.parent_revision !== snapshot.revision || !this.contracts.canonicalEqual(checked.parent, snapshot.parent)) {
      throw new Error("fork request is outside the bound parent revision");
    }
    return { preparer, checked };
  }
  async prepareFork(request: ForkRequest): Promise<ForkReport> {
    const { preparer, checked } = this.#checkedFork(request);
    const report = this.contracts.validate("fork_report", await preparer.prepare(checked));
    if (!this.contracts.canonicalEqual(report.request, checked)) throw new Error("fork preparer changed the request");
    return report;
  }
  async reconcileFork(request: ForkRequest): Promise<ForkReport | null> {
    const { preparer, checked } = this.#checkedFork(request);
    const reply = await preparer.reconcile(checked);
    if (reply === null) return null;
    const report = this.contracts.validate("fork_report", reply);
    if (!this.contracts.canonicalEqual(report.request, checked)) throw new Error("reconciled fork changed the request");
    return report;
  }
  #checkedForkReport(report: ForkReport): { publisher: ForkPublisher; report: ForkReport; seed: ForkSeed; operationId: OperationId } {
    if (!this.scope.grants.includes("fork:publish")) throw new Error("runtime scope lacks fork:publish");
    const publisher = this.components.forkPublisher;
    if (!publisher) throw new Error("fork publisher is not bound");
    const checked = this.contracts.validate("fork_report", report);
    // Native validation may canonicalize UUID spelling for the detached
    // protocol value. Preserve the caller's exact idempotency key at the
    // provider boundary, matching durable admission semantics.
    const operationId = report.request.operation_id;
    const parent = this.components.forkPreparer?.parentSnapshot().parent;
    if (!parent || !this.contracts.canonicalEqual(checked.request.parent, parent)
      || !this.contracts.canonicalEqual(checked.request.parent, publisher.parent())) {
      throw new Error("fork report belongs to another parent");
    }
    return { publisher, report: checked, operationId,
      seed: this.contracts.validate("fork_seed", this.contracts.forkSeed(checked)) };
  }
  /** Parent-owned publication and child binding. A result is complete only
   * when the publication provider has durably bound the exact child seed. */
  async spawnFromReport(report: ForkReport): Promise<ForkSeed> {
    const checked = this.#checkedForkReport(report);
    const published = this.contracts.validate("fork_seed", await checked.publisher.spawnFromReport(checked.report));
    if (!this.contracts.canonicalEqual(published, checked.seed)) {
      throw new Error("fork publisher bound another child seed");
    }
    return published;
  }
  /** Reconcile an uncertain publication by the original operation; a null
   * observation is unresolved and never permission to recapture resources. */
  async reconcileSpawn(report: ForkReport): Promise<ForkSeed | null> {
    const checked = this.#checkedForkReport(report);
    const observed = await checked.publisher.reconcileSpawn(checked.operationId);
    if (observed === null) return null;
    const admitted = this.contracts.validate("fork_seed", observed);
    if (!this.contracts.canonicalEqual(admitted, checked.seed)) {
      throw new Error("reconciled publication belongs to another child seed");
    }
    return admitted;
  }
  /** Check that a durable operation still uses the policy pinned at construction. */
  assertPolicyIdentity(): void { this.#assertPolicyIdentity(); }
  /** Canonical, owner-attestable inputs and authority for one durable task. */
  admissionRecord<Input, Output>(operationId: string, definition: TaskDefinition<Input, Output>,
    input: Input, parentTaskId?: RuntimeTaskId): TaskAdmissionRecord {
    if (definition.implementation.kind !== "resumable" || !definition.options.input || !definition.options.output
      || !definition.options.implementationDigest) throw new TypeError("durable admission requires a pinned resumable task");
    const identities = durableWireIdentities(definition, this.contracts);
    const limits = this.scope.limits;
    return freezeSchema(this.contracts.validate("task_admission", {
      contract: "harness.task-admission.v2", operation_id: operationId,
      task: identities.task, machine: identities.machine,
      input, input_schema: definition.options.input.document, output_schema: definition.options.output.document,
      parent: parentTaskId ?? null, grants: [...this.scope.grants].sort(),
      limits: nativeLimits(this.#contentLimits),
      run_limits: nativeRunLimits(limits),
      policy: this.durablePolicyIdentity(), extensions: null, execution: null,
    }));
  }
  validateExecutionPlacement(value: ExecutionPlacementWire | null): void {
    const selected = this.components.execution;
    if (!selected) {
      if (value !== null) throw new Error("retained admission selected an unbound execution route");
      return;
    }
    // Qualification and replay must observe one provider composition. A
    // route that changes its state/spawner identity between calls cannot
    // safely consume the placement it just qualified.
    this.#assertExecutionIdentity();
    if (value === null) throw new Error("qualified execution route is missing");
    const checked = this.contracts.validate("execution_placement", value);
    if (!this.contracts.canonicalEqual(checked.provider, selected.identity())) {
      throw new Error("execution qualifier returned another provider");
    }
  }
  attestAdmission(observed: HostTaskAttachment, expected: TaskAdmissionRecord): void {
    if (observed.admission === undefined || !this.contracts.canonicalEqual(observed.admission, expected)) {
      throw new Error("state owner admission differs from the exact spawner request");
    }
    this.#validateObservedAdmission(observed.admission);
  }
  #validateObservedAdmission(record: TaskAdmissionRecord): void {
    this.contracts.validate("task_admission", record);
    const definition = this.#tasks.get(taskKey(record.task.name, record.task.version));
    const identities = definition ? durableWireIdentities(definition, this.contracts) : null;
    if (!definition || definition.implementation.kind !== "resumable"
      || !this.contracts.canonicalEqual(identities?.task, record.task)
      || !this.contracts.canonicalEqual(identities?.machine, record.machine)
      || !this.contracts.canonicalEqual(definition.options.input?.document, record.input_schema)
      || !this.contracts.canonicalEqual(definition.options.output?.document, record.output_schema)) {
      throw new Error("state owner retained an unregistered task machine or schema");
    }
    this.contracts.validateToolValue(record.input_schema, record.input);
    this.validateExecutionPlacement(record.execution);
    if (!samePolicyIdentity(this.#policyIdentity, validatePolicyIdentity(record.policy)) || record.extensions !== null
      || record.grants.some(grant => !this.scope.grants.includes(grant))
      || !this.contracts.canonicalEqual(record.limits, nativeLimits(this.#contentLimits))) {
      throw new Error("state owner retained widened or changed task authority");
    }
    const limits = this.scope.limits;
    if ((limits.concurrency !== undefined && (record.run_limits.concurrency === null || record.run_limits.concurrency > limits.concurrency))
      || (limits.maxSteps !== undefined && (record.run_limits.max_steps === null || record.run_limits.max_steps > limits.maxSteps))
      || (limits.deadline !== undefined && (record.run_limits.deadline_epoch_ms === null
        || record.run_limits.deadline_epoch_ms > limits.deadline.getTime()))) {
      throw new Error("state owner retained widened task limits");
    }
  }
  #assertPolicyIdentity(): void {
    this.#assertExecutionIdentity();
    const policy = this.scope.policyProvider ?? this.components.policy;
    if (!samePolicyIdentity(this.#policyIdentity, validatePolicyIdentity(policy?.identity() ?? null))) {
      throw new Error("runtime policy implementation changed after admission");
    }
    if (this.host && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(this.host.policyIdentity()))) {
      throw new Error("durable host policy implementation changed after admission");
    }
    if (this.state && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(this.state.policyIdentity()))) {
      throw new Error("durable state policy implementation changed after admission");
    }
    if (this.spawner && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(this.spawner.policyIdentity()))) {
      throw new Error("durable spawner policy implementation changed after admission");
    }
  }
  #assertExecutionIdentity(): void {
    const route = this.components.execution;
    if (!route) return;
    const selected = validatePolicyIdentity(route.identity());
    if (selected === null || "composition" in selected
      || !samePolicyIdentity(selected, validatePolicyIdentity(this.components.state?.executionIdentity?.() ?? null))
      || !samePolicyIdentity(selected, validatePolicyIdentity(this.components.spawner?.executionIdentity?.() ?? null))) {
      throw new Error("execution provider, state, and spawner routes differ");
    }
  }
  task<Input, Output>(definition: TaskDefinition<Input, Output>): TaskDefinition<Input, Output>;
  task(name: string): TaskDefinition<unknown, unknown>;
  task(value: string | TaskDefinition<any, any>): TaskDefinition<any, any> {
    if (typeof value !== "string") {
      if (this.#tasks.get(taskKey(value.name, value.revision)) !== value) {
        throw new Error("task definition is not registered or no longer active");
      }
      return value;
    }
    const exact = this.#tasks.get(value);
    if (exact) return exact;
    const matches = [...this.#tasks.values()].filter(definition => definition.name === value);
    if (matches.length > 1) throw new Error(`task revision is ambiguous: ${value}`);
    if (matches.length === 0) throw new Error(`task is not registered: ${value}`);
    return matches[0]!;
  }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>): ToolRef<Input, Output>;
  tool(name: string): ToolRef<unknown, unknown>;
  tool(value: string | ToolDefinition<any, any>): ToolRef<any, any> {
    if (typeof value !== "string") {
      const key = toolKey(value.name, value.revision);
      const exact = this.#tools.get(key);
      if (!exact || this.#toolSources.get(key) !== value) {
        throw new Error("tool definition is not registered or no longer active");
      }
      return publicToolHandle(exact);
    }
    const name = value;
    const exact = this.#tools.get(name);
    if (exact) return publicToolHandle(exact);
    const selected = this.#selectedTools.get(name);
    if (selected) return publicToolHandle(this.#tools.get(toolKey(name, selected))!);
    const matches = [...this.#tools.values()].filter(tool => tool.definition.name === name);
    if (matches.length > 1) throw new Error(`tool revision is ambiguous: ${name}`);
    if (matches.length === 0) throw new Error(`tool is not registered: ${name}`);
    return publicToolHandle(matches[0]!);
  }
  spawn<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input): Task<Output> {
    if (this.components.execution) {
      throw new Error("live task closures cannot cross an execution provider; register a resumable task and admit it with a stable operation ID");
    }
    if (this.#tasks.get(taskKey(definition.name, definition.revision)) !== definition) throw new Error("task definition is not registered or no longer active");
    const parsed = definition.options.input?.parse(input) ?? input;
    if (definition.implementation.kind === "resumable") {
      throw new Error("resumable tasks require async durable admission; use admit with a stable operation ID");
    }
    const id = identity<RuntimeTaskId>();
    const handler = definition.implementation.handler;
    const task = new Task(id, async signal => { const output = await handler(new TaskContext(this, signal, id, false), parsed); return definition.options.output?.parse(output) ?? output; });
    this.running.set(id, task as Task<unknown>);
    return task;
  }
  async admit<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input, operationId: string, parentTaskId?: RuntimeTaskId): Promise<Admission<Output>> {
    this.#assertPolicyIdentity();
    this.contracts.validateIdentity("operation", operationId);
    if (this.#tasks.get(taskKey(definition.name, definition.revision)) !== definition) return { kind: "rejected", reason: { code: "unregistered", message: "task definition is not registered or no longer active" } };
    if (definition.implementation.kind === "live") return { kind: "rejected", reason: { code: "unsupported", message: "live tasks are local-only; use spawn without a durable operation ID" } };
    const spawner = this.spawner;
    const host = this.state;
    if (!spawner?.admitResumable || !host) return { kind: "rejected", reason: { code: "unsupported", message: "resumable tasks require durable spawner and state bindings" } };
    let parsed: Input;
    try {
      const inputSchema = definition.options.input;
      if (!inputSchema) throw new Error("durable task input schema is not pinned");
      parsed = inputSchema.parse(this.contracts.validateToolValue(inputSchema.document, input));
      this.contracts.validateToolValue(inputSchema.document, parsed);
    }
    catch (error) { return { kind: "rejected", reason: { code: "invalid_input", message: error instanceof Error ? error.message : String(error) } }; }
    let expected = this.admissionRecord(operationId, definition, parsed, parentTaskId);
    if (this.components.execution) {
      if (!spawner.reconcileAdmission) throw new Error("execution route requires admission reconciliation");
      const existing = await spawner.reconcileAdmission(operationId, this);
      this.#assertPolicyIdentity();
      if (existing !== null) {
        const observed = await observeAdmittedTask(host, existing.task.id(), this, operationId);
        if (observed === null) return { kind: "indeterminate", operationId };
        if (observed.operationId !== operationId || observed.task.id() !== existing.task.id()
          || observed.taskName !== definition.name || observed.revision !== definition.revision
          || observed.implementationDigest !== definition.options.implementationDigest
          || observed.admission === undefined
          || !this.contracts.canonicalEqual({ ...observed.admission, execution: null }, expected)) {
          throw new Error("operation belongs to another admission request");
        }
        if (existing.admission === undefined) throw new Error("spawner omitted the retained admission request");
        this.attestAdmission(observed, existing.admission);
        const task = validatedHostTask(observed.task, operationId, pinnedOutputSchema(definition), this.contracts);
        this.running.set(task.id(), task as Task<unknown>);
        return { kind: "accepted", task };
      }
      const placement = this.contracts.validate("execution_placement",
        await this.components.execution.qualifyTask(expected));
      this.validateExecutionPlacement(placement);
      this.#assertPolicyIdentity();
      // Re-admit the complete envelope after adding the placement. Native
      // validation owns the canonical numeric/byte representation of nested
      // execution fields; spreading the already validated pieces directly
      // would leave provider-returned numbers mixed with canonical BigInts.
      expected = freezeSchema(this.contracts.validate("task_admission", { ...expected, execution: placement }));
    }
    let admission: Admission<unknown>;
    try { admission = await spawner.admitResumable(operationId, definition, parsed, this, parentTaskId, expected); }
    catch (error) {
      if (error instanceof AdmissionUncertainError && error.operationId === operationId) {
        return { kind: "indeterminate", operationId };
      }
      throw error;
    }
    this.#assertPolicyIdentity();
    if (admission.kind !== "accepted") return admission;
    const task = validatedHostTask(admission.task, operationId, pinnedOutputSchema(definition), this.contracts);
    const observed = await observeAdmittedTask(host, task.id(), this, operationId);
    if (observed === null) return { kind: "indeterminate", operationId };
    if (observed.operationId !== operationId || observed.task.id() !== task.id()
      || observed.taskName !== definition.name || observed.revision !== definition.revision
      || observed.implementationDigest !== definition.options.implementationDigest) {
      throw new Error("spawner and state host task bindings differ");
    }
    if (this.components.spawner || this.components.state) this.attestAdmission(observed, expected);
    const authenticated = validatedHostTask(observed.task, operationId, pinnedOutputSchema(definition), this.contracts);
    this.running.set(authenticated.id(), authenticated as Task<unknown>);
    return { kind: "accepted", task: authenticated };
  }
  async reconcileAdmission(operationId: string): Promise<Task<unknown> | null> {
    this.contracts.validateIdentity("operation", operationId);
    const spawner = this.spawner;
    const host = this.state;
    if (!spawner?.reconcileAdmission || !host) throw new Error("durable admission reconciliation is not bound");
    this.#assertPolicyIdentity();
    const attachment = await spawner.reconcileAdmission(operationId, this);
    this.#assertPolicyIdentity();
    if (attachment === null) return null;
    if (attachment.operationId !== operationId) throw new Error("host reconciled another operation identity");
    const observed = await observeAdmittedTask(host, attachment.task.id(), this, operationId);
    if (observed === null) throw new Error("admission is indeterminate: state host has not observed the accepted task");
    if (observed.operationId !== operationId || observed.task.id() !== attachment.task.id()
      || observed.taskName !== attachment.taskName || observed.revision !== attachment.revision
      || observed.implementationDigest !== attachment.implementationDigest) {
      throw new Error("spawner and state host task bindings differ");
    }
    if (this.components.spawner || this.components.state) {
      if (attachment.admission === undefined) throw new Error("spawner did not retain the full admission request");
      this.attestAdmission(observed, attachment.admission);
    }
    return this.#validatedHostAttachment(observed);
  }
  group<Output>(policy: GroupPolicy, id?: GroupId): TaskGroup<Output> { return new TaskGroup(this, policy, id); }
  scoped(scope: ExecutionScope): AgentHarness {
    if (scope.modelBinding) this.contracts.encodeCanonicalJson(scope.modelBinding.identity.options);
    const parentPolicy = this.scope.policyProvider ?? this.components.policy;
    const grants = narrowGrants(this.scope.grants, scope.grants, scope.grantsExplicit || scope.grants.length > 0);
    const limits = narrowLimits(this.scope.limits, scope.limits);
    const policy = composePolicies(parentPolicy, scope.policyProvider);
    const narrowed = new ExecutionScope(
      scope.modelBinding ?? this.scope.modelBinding,
      scope.contextBuilder ?? this.scope.contextBuilder,
      scope.interactionHandler ?? this.scope.interactionHandler,
      policy,
      grants,
      limits,
      true,
      scope.executionProvider ?? this.scope.executionProvider,
    );
    const components: AgentHarnessComponents = { ...this.components };
    delete components.forkPreparer;
    delete components.forkPublisher;
    delete components.workspaces;
    if (scope.executionProvider) {
      delete components.host;
      components.execution = scope.executionProvider;
      components.state = scope.executionProvider.state();
      components.spawner = scope.executionProvider.spawner();
    }
    return new AgentHarness(this.#tasks, this.#tools, components, narrowed, this.running, this.#selectedTools, this.#toolSources, this.contracts, harnessConstruction);
  }
  async #prepareToolCall<Input, Output>(tool: ToolRef<Input, Output>, input: Input,
    signal: AbortSignal, operationId?: string, providerCallId?: string) {
    signal.throwIfAborted();
    const registered = this.#tools.get(toolKey(tool.definition.name, tool.definition.revision));
    if (registered === undefined || publicToolHandle(registered) !== tool) {
      throw new Error("tool definition is not registered or no longer active");
    }
    const capability = `tool:call:${tool.definition.name}`;
    if (!this.scope.grants.includes(capability)) throw new Error(`task scope lacks ${capability}`);
    const contracts = this.contracts;
    const admittedInput = contracts.validateToolValue(registered.definition.inputSchema, input);
    const parsedInput = registered.definition.parseInput(admittedInput);
    const publishOutput = (value: unknown): Output => registered.definition.parseOutput(
      contracts.validateToolValue(registered.definition.outputSchema, value));
    const toolOperationId = operationId === undefined ? crypto.randomUUID()
      : contracts.validateIdentity("operation", operationId);
    const callId = providerCallId ?? toolOperationId;
    const invocation = { operationId: toolOperationId, callId, name: tool.definition.name, arguments: parsedInput };
    this.#assertPolicyIdentity();
    const policy = this.scope.policyProvider ?? this.components.policy;
    const decision = await policy?.evaluate(
      { kind: "tool", tool: invocation.name, arguments: admittedInput },
      { grants: this.scope.grants, limits: this.scope.limits },
    ) ?? { kind: "allow" as const };
    this.#assertPolicyIdentity();
    if (decision.kind === "deny") throw new Error(decision.reason);
    const approvals = policyApprovals(decision, policy?.identity() ?? null);
    return { admittedInput, parsedInput, publishOutput, callId, invocation, approvals };
  }
  async call<Input, Output>(tool: ToolRef<Input, Output>, input: Input, signal = new AbortController().signal,
    taskId?: RuntimeTaskId, operationId?: string, providerCallId?: string): Promise<Output> {
    const { admittedInput, parsedInput, publishOutput, callId, invocation, approvals } =
      await this.#prepareToolCall(tool, input, signal, operationId, providerCallId);
    const actionInstance = invocation.operationId;
    for (const [approvalIndex, request] of approvals.entries()) {
      const contracts = this.contracts;
      const actionDigest = contracts.digestCanonicalJson({
        domain: "harness:local-tool-approval:v2",
        task_id: taskId ?? null,
        action_instance: actionInstance,
        policy: request.policy,
        approval_index: approvalIndex,
        tool: {
          name: tool.definition.name, revision: tool.definition.revision,
          description: tool.definition.description,
          input_schema: tool.definition.inputSchema,
          output_schema: tool.definition.outputSchema,
        },
        invocation: { call_id: callId, name: invocation.name, arguments: admittedInput },
      });
      const approval = await approvalBinding({
        operation_id: contracts.idFromDigest(actionDigest, "operation"),
        action_digest: [...actionDigest],
      });
      const answer = await this.interactions?.route({
        id: contracts.idFromDigest(actionDigest, "interaction"), kind: "approval", prompt: request.prompt,
        operationId: approval.operation_id, actionDigest: approval.action_digest,
      });
      if (answer?.kind === "approved") continue;
      if (answer?.kind === "indeterminate" && answer.operationId !== approval.operation_id) {
        throw new TypeError("approval outcome refers to another operation");
      }
      const kind: ToolApprovalFailureKind = answer === undefined ? "indeterminate"
        : answer.kind === "accepted" || answer.kind === "answered" ? "invalid_response" : answer.kind;
      throw new ToolApprovalError(kind, approval.operation_id);
    }
    const registered = this.#tools.get(toolKey(tool.definition.name, tool.definition.revision));
    if (registered === undefined || publicToolHandle(registered) !== tool) {
      throw new Error("tool definition is not registered or no longer active");
    }
    if (registered.machine) throw new Error("resumable tool requires callDurable and its owner host");
    if (registered.definition.handler) return publishOutput(await registered.definition.handler(new ToolContext(this, signal, callId, taskId, false, invocation.operationId), parsedInput));
    if (!registered.executor) throw new Error(`tool has no executable binding: ${tool.definition.name}`);
    return publishOutput((await registered.executor.execute(invocation)).value);
  }
  async callDurable<Input, Output>(operationId: OperationId, tool: ToolRef<Input, Output>, input: Input,
    taskId: RuntimeTaskId, signal = new AbortController().signal): Promise<Outcome<Output, OperationId>> {
    if (!this.state?.executeTool || !operationId || !taskId) {
      throw new Error("durable tool calls require task state, task identity, and stable operation ID");
    }
    const { parsedInput, publishOutput } = await this.#prepareToolCall(tool, input, signal, operationId);
    // The owner host independently re-evaluates the pinned policy and routes any
    // exact-action approval through its journal; a caller-side decision grants nothing.
    const outcome = await this.state.executeTool(taskId, operationId, tool, parsedInput, this);
    return outcome.kind === "succeeded"
      ? { kind: "succeeded", value: publishOutput(outcome.value) }
      : outcome;
  }
  canReconcileSelectedTurn(): boolean { return this.host?.executeSelectedTurn !== undefined; }
  async runSelectedContext(selectedContext: SelectedModelContext, operationId?: OperationId): Promise<RunOutput> {
    if (this.host?.executeSelectedTurn !== undefined) {
      if (operationId === undefined) throw new TypeError("durable selected turns require a stable operation ID");
      await validateSelectedContext(selectedContext, this.limits, this.contracts);
      this.#assertPolicyIdentity();
      const outcome = await this.host.executeSelectedTurn(operationId, selectedContext, this);
      this.#assertPolicyIdentity();
      if (outcome.kind === "indeterminate") {
        if (outcome.operationId !== operationId) throw new Error("host returned an unrelated model operation");
        throw new IndeterminateModelTurnError(operationId);
      }
      if (outcome.kind === "failed") throw new TerminalModelTurnError(operationId, "failed", outcome.error.message);
      if (outcome.kind === "cancelled") throw new TerminalModelTurnError(operationId, "cancelled", "durable selected turn was cancelled");
      if (outcome.value === null || typeof outcome.value !== "object"
        || typeof outcome.value.text !== "string" || !Array.isArray(outcome.value.receipts)
        || typeof outcome.value.taskId !== "string" || !outcome.value.taskId) {
        throw new TypeError("host returned an invalid assistant output");
      }
      if (new TextEncoder().encode(outcome.value.text).byteLength > this.limits.file_bytes) {
        throw new TypeError("host assistant output exceeds file limit");
      }
      return structuredClone(outcome.value);
    }
    if (this.state) throw new Error("durable selected-turn execution is not bound; local replay would duplicate an owner operation");
    return this.run({ selectedContext });
  }
  async run(value: string | SelectedAgentInput): Promise<RunOutput> {
    if (typeof value !== "string" && value.selectedContext !== undefined
      && (Object.hasOwn(value, "prompt") || Object.hasOwn(value, "content"))) {
      throw new TypeError("selected context cannot be combined with another prompt or content");
    }
    const input: RuntimeAgentInput = typeof value === "string" ? { prompt: value }
      : value.selectedContext === undefined ? value as AgentInput<UserContentPart>
      : { prompt: "", selectedContext: value.selectedContext };
    if (input.selectedContext !== undefined) await validateSelectedContext(input.selectedContext, this.limits, this.contracts);
    else {
      if (typeof input.prompt !== "string" || !Array.isArray(input.content ?? [])) {
        throw new TypeError("user input shape is invalid");
      }
      if (input.content !== undefined && (input.content.length > this.limits.attachments || input.content.length > 1_024)) {
        throw new TypeError("turn attachments exceed harness limits");
      }
      if (!input.prompt && !input.content?.length) throw new TypeError("user input is empty");
      if (input.content?.some(part => (part.kind !== "text" && part.kind !== "file")
        || (part.kind === "text" && !part.text))) {
        throw new TypeError("user input contains an unsupported part");
      }
      await validateModelContent(input.content === undefined
        ? input.prompt : [{ kind: "text", text: input.prompt }, ...input.content], this.limits, this.contracts);
    }
    const receipts: RunReceipt[] = [];
    const definition = TaskDefinition.live<RuntimeAgentInput, AgentOutput>("acyclic.default-agent", "1", async (context, input) => {
      const loop = this.components.loop;
      if (loop) return loop.run(context, input);
      const model = this.scope.modelBinding ?? this.components.model;
      if (!model) throw new Error("no model or agent loop is bound");
      const contextBuilder = this.scope.contextBuilder ?? this.components.context;
      const first: ModelMessage = { role: "user", content: input.content?.length
        ? [{ kind: "text", text: input.prompt }, ...input.content]
        : input.prompt };
      const selected = input.selectedContext;
      const base = selected?.messages ?? [first];
      const messages: ModelMessage[] = [...(await contextBuilder?.build(input, base) ?? base)];
      if (messages.length === 0 || messages.length > this.limits.context_messages) {
        throw new TypeError("context builder exceeded the message limit");
      }
      for (const message of messages) {
        if (!["system", "user", "assistant", "tool"].includes(message.role)) {
          throw new TypeError("context builder returned an unsupported role");
        }
        await validateModelContent(message.content, this.limits, this.contracts);
      }
      let text = "";
      let textBytes = 0;
      const maxSteps = Math.min(this.scope.limits.maxSteps ?? this.limits.model_steps, this.limits.model_steps);
      for (let step = 0; step < maxSteps; step += 1) {
        const calls: Extract<ModelEvent, { kind: "tool_call" }>[] = [];
        const callIds = new Set<string>();
        let eventCount = 0;
        let completed = false;
        for await (const event of model.provider.generate({ model: model.identity, messages, tools: this.#modelToolDefinitions(), signal: context.signal })) {
          if (completed) throw new TypeError("model emitted an event after completion");
          if (++eventCount > this.limits.model_events_per_step) throw new TypeError("model event limit exceeded");
          if (event.kind === "content") {
            const deltaBytes = new TextEncoder().encode(event.delta).byteLength;
            if (textBytes + deltaBytes > this.limits.file_bytes) {
              throw new TypeError("assistant output exceeds file limit");
            }
            textBytes += deltaBytes;
            text += event.delta;
          }
          else if (event.kind === "tool_call") {
            validateToolName(event.name);
            if (!event.callId || new TextEncoder().encode(event.callId).byteLength > 255
              || /[\p{Cc}\\/]/u.test(event.callId) || callIds.has(event.callId)) {
              throw new TypeError("model tool call identity is invalid or repeated");
            }
            if (calls.length >= this.limits.tool_calls_per_step) throw new TypeError("model tool-call limit exceeded");
            callIds.add(event.callId);
            calls.push(event);
          }
          else if (event.kind === "completed") {
            completed = true;
            receipts.push({ kind: "model-completed", metadata: event.metadata });
          }
        }
        if (!completed) throw new Error("model attempt ended without completion");
        if (calls.length === 0) return { text };
        for (const call of calls) {
          const toolOperationId = this.contracts.idFromDigest(this.contracts.digestCanonicalJson({
            domain: "harness:tool-call:v2", task_id: context.taskId, step,
            call_id: call.callId,
          }), "operation");
          const value = await context.call(this.tool(call.name), call.arguments, toolOperationId, call.callId);
          const projection = await boundedToolValue(value, this.limits.render_bytes, this.contracts);
          receipts.push({ kind: "tool", step, callId: call.callId, name: call.name, arguments: call.arguments, value, projection });
          messages.push({ role: "assistant", content: call }, { role: "tool", content: { kind: "tool_result", callId: call.callId, name: call.name, value: projection } });
        }
      }
      throw new Error(`agent loop exceeded ${maxSteps} model steps`);
    });
    const tasks = new Map(this.#tasks); tasks.set(taskKey(definition.name, definition.revision), definition);
    const runtime = new AgentHarness(tasks, this.#tools, this.components, this.scope, this.running, this.#selectedTools, this.#toolSources, this.contracts, harnessConstruction);
    const task = runtime.spawn(definition, input);
    const outcome = await task.result();
    if (outcome.kind !== "succeeded") throw new TaskRunError(task.id(), outcome);
    return { ...outcome.value, taskId: task.id(), receipts };
  }
  attach(id: RuntimeTaskId): Promise<Task<unknown>>;
  attach<Input, Output>(definition: TaskDefinition<Input, Output>, id: RuntimeTaskId): Promise<Task<Output>>;
  async attach(value: RuntimeTaskId | TaskDefinition<any, any>, taskId?: RuntimeTaskId): Promise<Task<any>> {
    const requested = typeof value === "string" ? undefined : this.task(value);
    const id = typeof value === "string" ? value : taskId;
    if (id === undefined) throw new TypeError("task identity is required");
    const local = this.running.get(id);
    if (local) {
      if (requested) throw new Error("local task attachments cannot claim a durable definition");
      return local;
    }
    if (this.state) {
      this.#assertPolicyIdentity();
      const attachment = await this.state.attach(id, this);
      this.#assertPolicyIdentity();
      if (attachment.task.id() !== id) throw new Error("host attached a different task identity");
      return this.#validatedHostAttachment(attachment, requested);
    }
    throw new Error("task identity is not retained locally and no durable harness host is bound");
  }
  /** Read one revision-checked page of direct, owner-retained task children. */
  async children(parent: RuntimeTaskId, expectedRevision: bigint | null = null,
    afterSlot: string | null = null, maximum = 256): Promise<TaskChildrenPage> {
    this.contracts.validateIdentity("task", parent);
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 1_024
      || afterSlot !== null && (new TextEncoder().encode(afterSlot).byteLength > 255
        || [...afterSlot].some(character => /[\x00-\x1f\x7f]/u.test(character)))) {
      throw new RangeError("task child page request is invalid");
    }
    if (expectedRevision !== null && (typeof expectedRevision !== "bigint" || expectedRevision < 0n)) {
      throw new RangeError("task child revision is invalid");
    }
    const state = this.state;
    if (!state?.children) throw new Error("durable task hierarchy is not bound");
    this.#assertPolicyIdentity();
    const page = await state.children(parent, expectedRevision, afterSlot, maximum, this);
    this.#assertPolicyIdentity();
    if (typeof page.revision !== "bigint" || page.revision < 0n
      || expectedRevision !== null && page.revision !== expectedRevision
      || page.entries.length > maximum || page.nextAfter !== null
        && page.nextAfter !== page.entries.at(-1)?.slot) {
      throw new TypeError("task child page does not match its request");
    }
    let previous = afterSlot;
    const ids = new Set<RuntimeTaskId>();
    for (const entry of page.entries) {
      this.contracts.validateIdentity("task", entry.taskId);
      if (entry.slot.trim().length === 0 || new TextEncoder().encode(entry.slot).byteLength > 255
        || [...entry.slot].some(character => /[\x00-\x1f\x7f]/u.test(character))
        || previous !== null && compareUtf8(entry.slot, previous) <= 0 || ids.has(entry.taskId)) {
        throw new TypeError("task children are not in stable slot order");
      }
      ids.add(entry.taskId);
      previous = entry.slot;
    }
    return page;
  }

  #validatedHostAttachment(attachment: HostTaskAttachment, requested?: TaskDefinition<any, any>): Task<unknown> {
    const definition = this.#tasks.get(taskKey(attachment.taskName, attachment.revision));
    if (!definition || (requested !== undefined && requested !== definition)
      || definition.revision !== attachment.revision
      || definition.options.implementationDigest !== attachment.implementationDigest) {
      throw new Error("host task binding is not registered at its pinned implementation");
    }
    if (definition.implementation.kind !== "resumable") throw new Error("host task binding is not a resumable definition");
    if (this.components.spawner || this.components.state) {
      if (attachment.admission === undefined || attachment.admission.operation_id !== attachment.operationId) {
        throw new Error("state owner did not retain the task admission");
      }
      this.#validateObservedAdmission(attachment.admission);
    }
    return validatedHostTask(attachment.task, attachment.operationId, pinnedOutputSchema(definition), this.contracts);
  }

  #modelToolDefinitions(): readonly ModelToolDefinition[] {
    const names = new Set([...this.#tools.values()].map(tool => tool.definition.name));
    return [...names].map(name => {
      const selected = this.#selectedTools.get(name);
      if (!selected) throw new Error(`model-visible tool revision is ambiguous: ${name}`);
      const tool = this.#tools.get(toolKey(name, selected));
      if (!tool) throw new Error(`selected tool revision is not registered: ${name}@${selected}`);
      const { revision, description, inputSchema, outputSchema } = tool.definition;
      return { name, revision, description, inputSchema, outputSchema };
    });
  }
}
export class TaskRunError extends Error { constructor(readonly taskId: RuntimeTaskId, readonly outcome: Exclude<Outcome<unknown>, { kind: "succeeded" }>) { super(outcome.kind === "failed" ? outcome.error.message : `task ${outcome.kind}`); } }

class ReplayQueue<Value> {
  readonly #values: Value[] = []; readonly #waiters = new Set<() => void>(); #closed = false;
  push(value: Value): void { this.#values.push(value); for (const wake of this.#waiters) wake(); this.#waiters.clear(); }
  close(): void { this.#closed = true; for (const wake of this.#waiters) wake(); this.#waiters.clear(); }
  async *from(index: number): AsyncIterable<Value> { let next = index; for (;;) { while (next < this.#values.length) yield this.#values[next++]!; if (this.#closed) return; await new Promise<void>(resolve => this.#waiters.add(resolve)); } }
}
function withOutcome<Output>(entry: GroupEntry<Output>, outcome: Outcome<Output>): GroupEntry<Output> { return { ...entry, outcome }; }
function pinnedOutputSchema<Input, Output>(definition: TaskDefinition<Input, Output>): RuntimeSchema<Output> {
  const schema = definition.options.output;
  if (definition.implementation.kind !== "resumable" || !schema) throw new Error("durable task output schema is not pinned");
  return schema;
}
function validatedHostTask<Output>(original: Task<unknown>, operationId: string, schema: RuntimeSchema<Output>, contracts: NativeContracts): Task<Output> {
  return Task.fromHost<Output>(original.id(), {
    operationId,
    async result() { return validateTaskOutcome(await original.result(), schema, contracts); },
    async *events(fromSequence) {
      for await (const event of original.events(fromSequence)) {
        if (event.event.kind === "settled") yield { ...event, event: { kind: "settled" as const, outcome: await validateTaskOutcome(event.event.outcome, schema, contracts) } };
        else yield { ...event, event: { kind: "started" as const } };
      }
    },
    async terminalEvent() {
      const event = await original.terminalEvent();
      if (event.event.kind !== "settled") throw new Error("host terminal event is not settled");
      return { ...event, event: { kind: "settled" as const,
        outcome: await validateTaskOutcome(event.event.outcome, schema, contracts) } };
    },
    cancel: () => original.cancel(),
  });
}
async function validateTaskOutcome<Output>(outcome: Outcome<unknown>, schema: RuntimeSchema<Output>, contracts: NativeContracts): Promise<Outcome<Output>> {
  if (outcome.kind !== "succeeded") return outcome;
  try { return { kind: "succeeded", value: schema.parse(contracts.validateToolValue(schema.document, outcome.value)) }; }
  catch (error) { return { kind: "failed", error: { message: error instanceof Error ? error.message : String(error) } }; }
}
function identity<Value extends string>(): Value { return crypto.randomUUID() as Value; }
function taskKey(name: string, revision: string): string {
  if (!name || !revision || name.includes("@") || revision.includes("@")) {
    throw new TypeError("task name and revision must be nonempty and cannot contain @");
  }
  return `${name}@${revision}`;
}
function toolKey(name: string, revision: string): string { return taskKey(name, revision); }
function registrationKey<Input, Output>(definition: TaskDefinition<Input, Output>): string {
  return `${definition.name}@${definition.revision}#${definition.options.implementationDigest ?? "live"}`;
}
function nativeLimits(value: Limits): NativeLimitsWire {
  return {
    file_bytes: BigInt(value.file_bytes), path_bytes: BigInt(value.path_bytes),
    attachments: BigInt(value.attachments), render_bytes: BigInt(value.render_bytes),
    model_steps: BigInt(value.model_steps), model_events_per_step: BigInt(value.model_events_per_step),
    tool_calls_per_step: BigInt(value.tool_calls_per_step), context_messages: BigInt(value.context_messages),
  };
}
function nativeRunLimits(value: EffectiveScope["limits"]): TaskRunLimitsWire {
  return {
    concurrency: value.concurrency === undefined ? null : BigInt(value.concurrency),
    max_steps: value.maxSteps === undefined ? null : BigInt(value.maxSteps),
    deadline_epoch_ms: value.deadline === undefined ? null : BigInt(value.deadline.getTime()),
  };
}
function durableWireIdentities<Input, Output>(definition: TaskDefinition<Input, Output>, contracts: NativeContracts):
  { readonly task: TaskAdmissionWire["task"]; readonly machine: TaskAdmissionWire["machine"] } {
  const { implementationDigest, input, output, requirements } = definition.options;
  if (definition.implementation.kind !== "resumable" || !implementationDigest || !input || !output) {
    throw new TypeError("durable identity requires a pinned resumable definition");
  }
  const machineDigest = [...implementationDigest.matchAll(/../g)]
    .map(match => Number.parseInt(match[0]!, 16));
  const taskDigest = contracts.taskIdentityDigest(definition.name, definition.revision,
    input.document, output.document, requirements ?? [], Uint8Array.from(machineDigest));
  return {
    task: { name: definition.name, version: definition.revision, digest: taskDigest },
    machine: { name: definition.name, version: definition.revision, digest: machineDigest },
  };
}
function validateTaskRequirements(
  tasks: ReadonlyMap<string, TaskDefinition<any, any>>,
  tools: ReadonlyMap<string, RegisteredTool<any, any>>,
  components: AgentHarnessComponents,
  grants: readonly string[],
): void {
  const active = new Set<string>();
  const complete = new Set<string>();
  const visit = (name: string): void => {
    if (complete.has(name)) return;
    active.add(name);
    const definition = tasks.get(name);
    if (!definition) throw new Error(`unregistered task dependency: ${name}`);
    for (const requirement of definition.options.requirements ?? []) {
      if (requirement === "model" && components.model) continue;
      if (requirement === "context" && components.context) continue;
      if (requirement === "interactions" && components.interactions) continue;
      if (requirement === "policy" && components.policy) continue;
      if (requirement === "host" && (components.host || (components.state && components.spawner))) continue;
      if (requirement === "state" && (components.state || components.host)) continue;
      if (requirement === "spawner" && (components.spawner || components.host)) continue;
      if (requirement === "content" && components.content) continue;
      if (requirement === "artifacts" && components.artifacts) continue;
      if (requirement === "content:write" && components.content?.writer
        && grants.includes(components.content.writer.writeCapability())) continue;
      if (requirement === "artifacts:write" && components.artifacts?.writer
        && grants.includes(components.artifacts.writer.writeCapability())) continue;
      const match = /^(task|tool):([^@]+)@([^@]+)$/.exec(requirement);
      if (!match) throw new Error(`unsatisfied task requirement: ${requirement}`);
      const [, kind, target, revision] = match;
      if (kind === "tool") {
        if (!tools.has(toolKey(target!, revision!))) throw new Error(`unsatisfied tool requirement: ${requirement}`);
      } else {
        const targetKey = taskKey(target!, revision!);
        if (!tasks.has(targetKey)) throw new Error(`unsatisfied task requirement: ${requirement}`);
        if (active.has(targetKey)) throw new Error(`task dependency cycle: ${[...active, targetKey].join(" -> ")}`);
        visit(targetKey);
      }
    }
    active.delete(name);
    complete.add(name);
  };
  for (const name of tasks.keys()) visit(name);
}
function narrowGrants(parent: readonly string[], child: readonly string[], explicit: boolean): readonly string[] {
  if (!explicit) return parent;
  if (child.some(grant => !parent.includes(grant))) throw new Error("child scope cannot widen grants");
  return [...new Set(child)];
}
function narrowLimits(parent: EffectiveScope["limits"], child: EffectiveScope["limits"]): EffectiveScope["limits"] {
  const concurrency = child.concurrency ?? parent.concurrency;
  if (concurrency !== undefined && (!Number.isSafeInteger(concurrency) || concurrency <= 0)) throw new RangeError("scope concurrency must be a positive safe integer");
  if (parent.concurrency !== undefined && concurrency !== undefined && concurrency > parent.concurrency) throw new Error("child scope cannot widen concurrency");
  const maxSteps = child.maxSteps ?? parent.maxSteps;
  if (maxSteps !== undefined && (!Number.isSafeInteger(maxSteps) || maxSteps <= 0)) throw new RangeError("scope maxSteps must be a positive safe integer");
  if (parent.maxSteps !== undefined && maxSteps !== undefined && maxSteps > parent.maxSteps) throw new Error("child scope cannot widen maxSteps");
  const deadline = child.deadline ?? parent.deadline;
  if (deadline !== undefined && Number.isNaN(deadline.getTime())) throw new RangeError("scope deadline must be valid");
  if (parent.deadline !== undefined && deadline !== undefined && deadline > parent.deadline) throw new Error("child scope cannot extend deadline");
  return { ...(concurrency === undefined ? {} : { concurrency }), ...(maxSteps === undefined ? {} : { maxSteps }), ...(deadline === undefined ? {} : { deadline: new Date(deadline) }) };
}
function composePolicies(parent: EffectivePolicy | undefined, child: EffectivePolicy | undefined): EffectivePolicy | undefined {
  if (!parent || parent === child) return child ?? parent;
  if (!child) return parent;
  return {
    identity: () => Object.freeze({ composition: Object.freeze([
      validatePolicyIdentity(parent.identity())!, validatePolicyIdentity(child.identity())!,
    ] as const) }),
    async evaluate(invocation, scope) {
      const inherited = await parent.evaluate(invocation, scope);
      if (inherited.kind === "deny") return inherited;
      const narrowed = await child.evaluate(invocation, scope);
      if (narrowed.kind === "deny") return narrowed;
      const approvals = [
        ...policyApprovals(inherited, parent.identity()),
        ...policyApprovals(narrowed, child.identity()),
      ];
      return approvals.length === 0 ? { kind: "allow" } : { kind: "require-approvals", approvals };
    },
  };
}
function policyApprovals(decision: EffectivePolicyDecision, identity: PolicyIdentity | null): readonly PolicyApproval[] {
  if (decision.kind === "require-approvals") return decision.approvals;
  if (decision.kind !== "require-approval") return [];
  if (identity === null) throw new TypeError("approval policy has no pinned identity");
  return [{ prompt: decision.prompt, policy: identity }];
}
function validatePolicyIdentity(identity: PolicyIdentity | MachineIdentityWire | null, ancestors = new Set<object>()): PolicyIdentity | null {
  if (identity === null) return null;
  if (ancestors.has(identity)) throw new TypeError("policy identity contains a cycle");
  ancestors.add(identity);
  try {
    if ("composition" in identity) {
      if (Object.keys(identity).length !== 1 || !Array.isArray(identity.composition)
        || identity.composition.length !== 2) throw new TypeError("policy composition is invalid");
      const [parent, child] = identity.composition;
      if (!parent || !child) throw new TypeError("policy composition is incomplete");
      return Object.freeze({ composition: Object.freeze([
        validatePolicyIdentity(parent, ancestors)!, validatePolicyIdentity(child, ancestors)!,
      ] as const) });
    }
    if (Object.keys(identity).sort().join("\0") !== "digest\0name\0version") {
      throw new TypeError("policy identity has unexpected fields");
    }
    if (!Array.isArray(identity.digest) || identity.digest.length !== 32
      || !identity.digest.some(byte => byte !== 0)
      || identity.digest.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
      throw new TypeError("policy implementation digest must be a nonzero 32-byte value");
    }
    return policyIdentity(identity.name, identity.version, Uint8Array.from(identity.digest));
  } finally {
    ancestors.delete(identity);
  }
}
function samePolicyIdentity(left: PolicyIdentity | null, right: PolicyIdentity | null): boolean {
  if (left === null || right === null) return left === right;
  if ("composition" in left || "composition" in right) {
    return "composition" in left && "composition" in right
      && samePolicyIdentity(left.composition[0], right.composition[0])
      && samePolicyIdentity(left.composition[1], right.composition[1]);
  }
  return left.name === right.name && left.version === right.version
    && left.digest.every((byte, index) => byte === right.digest[index]);
}
async function abortableWait(milliseconds: number, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    if (signal.aborted) { reject(signal.reason); return; }
    const aborted = () => { clearTimeout(timeout); reject(signal.reason); };
    const timeout = setTimeout(() => { signal.removeEventListener("abort", aborted); resolve(); }, milliseconds);
    signal.addEventListener("abort", aborted, { once: true });
  });
}
