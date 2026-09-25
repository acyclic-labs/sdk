import { validateComponentLabel, validateToolName, type AgentInput, type AgentLoop, type AgentOutput, type ContextBuilder, type ModelContent, type ModelContentPart, type ModelEvent, type ModelMessage, type ModelProvider, type ModelToolDefinition, type ToolDefinition, type ToolExecutor, type ToolJsonSchema, type ToolJsonValue, type ToolRef, type UserContentPart } from "./model.js";
import { DEFAULT_LIMITS, verifyFileBytes, type FileRef, type Limits, type VolumeRef } from "./conversation.js";
import { approvalBinding, interactionId, type InteractionId } from "./interaction.js";
import { NativeContracts } from "./native-contracts.js";
import type { EffectId, OperationId } from "./index.js";
import type { SelectedModelContext } from "./projection.js";
export * from "./model.js";

declare const identityBrand: unique symbol;
export type RuntimeTaskId = string & { readonly [identityBrand]: "RuntimeTaskId" };
export type BatchId = string & { readonly [identityBrand]: "BatchId" };
export type GroupId = string & { readonly [identityBrand]: "GroupId" };
export type MessageId = string & { readonly [identityBrand]: "MessageId" };
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
export type ResumableTaskOptions<Input, Output> = TaskDefinitionOptions<Input, Output> & Readonly<{
  input: RuntimeSchema<Input>;
  output: RuntimeSchema<Output>;
  implementationDigest: string;
}>;
export type LiveTask<Input, Output> = (context: TaskContext, input: Input) => Output | Promise<Output>;
export type ResumableTransition<State, Output> =
  | { readonly kind: "continue"; readonly state: State }
  | { readonly kind: "finish"; readonly output: Output };
export interface ResumableTask<Input, Output, State = unknown> {
  /** Validates each restored checkpoint before it reaches a typed transition. */
  readonly state: RuntimeSchema<State>;
  initial(input: Input): State;
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
      initial: (input: Input): unknown => component.initial(input),
      transition: async (context: TaskContext, value: unknown, previous?: Outcome<unknown>): Promise<ResumableTransition<unknown, Output>> => {
        const validated = (await NativeContracts.create()).validateToolValue(state.document, value);
        return component.transition(context, component.state.parse(validated), previous);
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

async function validateSelectedContext(selected: SelectedModelContext, limits: Limits): Promise<void> {
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
  for (const message of selected.messages) await validateModelContent(message.content, limits);
}

async function validateModelContent(content: ModelContent, limits: Limits): Promise<void> {
  if (typeof content === "string") {
    if (new TextEncoder().encode(content).byteLength > limits.render_bytes) throw new TypeError("model text exceeds render limit");
    return;
  }
  const parts: readonly ModelContentPart[] = Array.isArray(content) ? content : [content as ModelContentPart];
  if (parts.length > limits.attachments + 1) throw new TypeError("model content exceeds attachment limit");
  for (const part of parts) {
    if (part.kind === "file") {
      const file = (await NativeContracts.create()).validate("file_ref", part.file);
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
      const encoded = (await NativeContracts.create()).encodeCanonicalJson(
        part.kind === "tool_call" ? part.arguments : part.value);
      if (encoded.byteLength > limits.render_bytes) {
        throw new TypeError("model tool projection exceeds render limit");
      }
    } else if (part.kind !== "text") {
      throw new TypeError("model content contains an unsupported part");
    }
  }
}

async function boundedToolValue(value: unknown, renderLimit: number): Promise<unknown> {
  const bytes = (await NativeContracts.create()).encodeCanonicalJson(value).byteLength;
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
}
export interface HostBatchReplay {
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  /** Rust-canonical digest pinned when the batch was first admitted. */
  readonly inputDigest: Uint8Array;
  readonly entries: readonly GroupEntry<unknown>[];
}
/** Durable host operations. Implementations own persistence, admission, and replay semantics. */
export interface HarnessRuntimeHost {
  /** Exact policy enforced at durable dispatch; null means no policy is installed. */
  policyIdentity(): PolicyIdentity | null;
  /** Admission, descendant ownership, state checkpoints, wait/effect boundaries, and replay are owned by the host. */
  admitResumable?<Input, Output>(operationId: string, definition: TaskDefinition<Input, Output>, input: Input, harness: AgentHarness, parentTaskId?: RuntimeTaskId): Promise<Admission<unknown>>;
  reconcileBatch?(groupId: GroupId, batchId: BatchId, inputDigest: Uint8Array, harness: AgentHarness): Promise<HostBatchReplay>;
  attach(id: RuntimeTaskId, harness: AgentHarness): Promise<HostTaskAttachment>;
  reconcileEffect(taskId: RuntimeTaskId, effectId: EffectId): Promise<EffectStatus>;
  send(message: TaskMessage): Promise<{ readonly accepted: boolean; readonly messageId: MessageId }>;
  inbox(taskId: RuntimeTaskId, from?: MessageId): AsyncIterable<TaskMessage>;
  /** Stages the request, admits a ref-only interaction, and reconciles its versioned response. */
  routeInteraction?(taskId: RuntimeTaskId, id: InteractionId, interaction: Interaction & Readonly<{ id: InteractionId }>): Promise<Answer>;
  /** Re-evaluates pinned policy and grants, then commits approval, effect intent, and outcome. */
  executeTool?<Input, Output>(taskId: RuntimeTaskId, operationId: OperationId, tool: ToolRef<Input, Output>, input: Input, harness: AgentHarness): Promise<Outcome<unknown, OperationId>>;
  /** Commits a timer wait boundary and resumes from the same deadline after restart. */
  waitUntil?(taskId: RuntimeTaskId, operationId: string, deadline: Date): Promise<void>;
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
    readonly modelProvider?: ModelProvider,
    readonly contextBuilder?: ContextBuilder,
    readonly interactionHandler?: InteractionHandler,
    readonly policyProvider?: EffectivePolicy,
    grants: readonly string[] = [],
    limits: EffectiveScope["limits"] = {},
    readonly grantsExplicit = false,
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
  model(value: ModelProvider): ExecutionScope { return new ExecutionScope(value, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit); }
  context(value: ContextBuilder): ExecutionScope { return new ExecutionScope(this.modelProvider, value, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit); }
  interactions(value: InteractionHandler): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, value, this.policyProvider, this.grants, this.limits, this.grantsExplicit); }
  policy(value: Policy): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, this.interactionHandler, value, this.grants, this.limits, this.grantsExplicit); }
  /** Select a subset of the parent's grants; an empty selection removes all. */
  onlyGrants(...values: readonly string[]): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, this.interactionHandler, this.policyProvider, [...new Set(values)], this.limits, true); }
  grant(...values: readonly string[]): ExecutionScope { return this.onlyGrants(...this.grants, ...values); }
  withLimits(value: EffectiveScope["limits"]): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, value, this.grantsExplicit); }
}

export type GroupPolicy = { readonly kind: "collect-all" } | { readonly kind: "cancel-on-failure" };
export const GroupPolicies = Object.freeze({ collectAll: { kind: "collect-all" } as const, cancelOnFailure: { kind: "cancel-on-failure" } as const });

export class Batch<Input> {
  constructor(readonly id: BatchId, readonly inputs: readonly Input[]) { if (!id) throw new TypeError("batch id is required"); }
}
export interface GroupEntry<Output> { readonly key: InputKey; readonly admission: Admission<Output>; readonly outcome?: Outcome<Output> }
export interface GroupOutcome<Output> { readonly groupId: GroupId; readonly complete: boolean; readonly entries: readonly GroupEntry<Output>[] }
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
    if (terminal.taskId !== this.taskId || terminal.event.kind !== "settled") {
      throw new Error("durable host returned an invalid terminal event");
    }
    if (streamedTerminal && (streamedTerminal.id !== terminal.id || streamedTerminal.sequence !== terminal.sequence)) {
      throw new Error("durable host task stream disagrees with its terminal event");
    }
    if (!streamedTerminal && terminal.sequence >= fromSequence) yield terminal;
  }
  async cancel(): Promise<CancelReceipt> { if (this.#driver) return this.#driver.cancel(); this.controller.abort(new Error("task cancellation requested")); return { requested: true, taskId: this.taskId }; }
}

export class TaskGroup<Output, Authority extends "owner" | "scoped" = "owner"> {
  readonly #harness: AgentHarness;
  readonly #authorizeTask: TaskAuthorizer | undefined;
  readonly #entries: GroupEntry<Output>[] = [];
  readonly #entryKeys = new Map<string, { readonly index: number; readonly registration: string }>();
  readonly #batchDigests = new Map<BatchId, string>();
  #closed = false;
  constructor(harness: AgentHarness, readonly policy: GroupPolicy, readonly id: GroupId = identity<GroupId>("group"), readonly parentTaskId?: RuntimeTaskId, authorizeTask?: TaskAuthorizer) {
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
    if (definition.implementation.kind === "resumable") await this.#bindBatchInputs(batch);
    const entries = await Promise.all(batch.inputs.map(async (input, index): Promise<GroupEntry<Output>> => {
      const key = { batchId: batch.id, index };
      const existing = this.#entryKeys.get(`${batch.id}:${index}`);
      if (existing && existing.registration !== registration) {
        return { key, admission: { kind: "rejected", reason: { code: "unsupported", message: "batch entry identity is already bound to another task definition" } } };
      }
      let admission: Admission<Output>;
      if (definition.implementation.kind === "live") {
        if (this.parentTaskId) admission = { kind: "rejected", reason: { code: "unsupported", message: "durable groups cannot spawn local-only tasks" } };
        else if (existing) admission = this.#entries[existing.index]!.admission;
        else {
          try { admission = { kind: "accepted", task: this.#harness.spawn(definition, input) }; }
          catch (error) { admission = { kind: "rejected", reason: { code: "invalid_input", message: error instanceof Error ? error.message : String(error) } }; }
        }
      } else admission = await this.#harness.admit(definition, input, `${this.id}:${registration}:${batch.id}:${index}`, this.parentTaskId);
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
    const registration = registrationKey(definition);
    const inputDigest = await this.#bindBatchInputs(batch);
    const local = this.#entries.filter(entry => entry.key.batchId === batch.id);
    if (local.length) {
      if (local.some(entry => this.#entryKeys.get(`${batch.id}:${entry.key.index}`)?.registration !== registration)) {
        throw new Error("batch identity belongs to another task definition");
      }
      if (local.length === batch.inputs.length && local.every(entry => entry.admission.kind !== "indeterminate"
        && entry.outcome?.kind !== "indeterminate")) return local;
    }
    if (!this.#harness.host?.reconcileBatch) throw new Error("durable batch reconciliation requires a host binding");
    this.#harness.assertPolicyIdentity();
    const recovered = await this.#harness.host.reconcileBatch(this.id, batch.id, inputDigest, this.#harness);
    this.#harness.assertPolicyIdentity();
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
      const operationId = `${this.id}:${registration}:${batch.id}:${entry.key.index}`;
      if (entry.admission.kind === "indeterminate" && entry.admission.operationId !== operationId) {
        throw new Error("host returned an unrelated batch admission");
      }
      const outcome = entry.outcome === undefined ? undefined : await validateTaskOutcome(entry.outcome, outputSchema);
      const validated: GroupEntry<Output> = entry.admission.kind === "accepted"
        ? { key: entry.key, admission: { kind: "accepted", task: validatedHostTask(entry.admission.task, operationId, outputSchema) }, ...(outcome === undefined ? {} : { outcome }) }
        : { key: entry.key, admission: entry.admission, ...(outcome === undefined ? {} : { outcome }) };
      entries.push(validated);
    }
    entries.sort((a, b) => a.key.index - b.key.index);
    for (const entry of entries) {
      const identity = `${batch.id}:${entry.key.index}`;
      const prior = this.#entryKeys.get(identity);
      if (prior) this.#entries[prior.index] = entry;
      else { this.#entryKeys.set(identity, { index: this.#entries.length, registration }); this.#entries.push(entry); }
    }
    return entries;
  }
  async #bindBatchInputs<Input>(batch: Batch<Input>): Promise<Uint8Array> {
    const digest = (await NativeContracts.create()).digestCanonicalJson(batch.inputs);
    const hex = Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join("");
    const pinned = this.#batchDigests.get(batch.id);
    if (pinned !== undefined && pinned !== hex) throw new Error("batch identity belongs to another input list");
    this.#batchDigests.set(batch.id, hex);
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
    const entries = await this.spawnMany(definition, new Batch(identity<BatchId>("batch"), inputs));
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
  async join(): Promise<GroupOutcome<Output>> { this.#closed = true; const entries = await Promise.all(this.#entries.map(async entry => entry.admission.kind === "accepted" ? withOutcome(entry, await entry.admission.task.result()) : entry)); return { groupId: this.id, complete: entries.every(entry => entry.admission.kind !== "indeterminate" && entry.outcome?.kind !== "indeterminate"), entries }; }
  async race(): Promise<GroupEntry<Output>> {
    this.#closed = true;
    const accepted = this.#entries.filter((entry): entry is GroupEntry<Output> & { admission: { kind: "accepted"; task: Task<Output> } } => entry.admission.kind === "accepted");
    if (!accepted.length) throw new GroupError(this.id, this.#entries);
    const settled = accepted.map(async entry => ({ ...entry, outcome: await entry.admission.task.result() }));
    const winner = await Promise.race(settled);
    await Promise.all(accepted.filter(entry => entry.key !== winner.key).map(entry => entry.admission.task.cancel()));
    return winner;
  }
  async firstSuccess(): Promise<Keyed<Output>> {
    this.#closed = true;
    const accepted = this.#entries.filter((entry): entry is GroupEntry<Output> & { admission: { kind: "accepted"; task: Task<Output> } } => entry.admission.kind === "accepted");
    if (!accepted.length) throw new GroupError(this.id, this.#entries);
    const settled = accepted.map(async entry => ({ ...entry, outcome: await entry.admission.task.result() }));
    try {
      const winner = await Promise.any(settled.map(async result => {
        const entry = await result;
        if (entry.outcome.kind !== "succeeded") throw entry;
        return { key: entry.key, value: entry.outcome.value };
      }));
      await Promise.all(accepted.filter(entry => entry.key !== winner.key).map(entry => entry.admission.task.cancel()));
      return winner;
    } catch { throw new GroupError(this.id, await Promise.all(settled)); }
  }
  async quorum(required: number, accept: (value: Output) => boolean): Promise<readonly Keyed<Output>[]> {
    if (!Number.isSafeInteger(required) || required < 0) throw new RangeError("required must be a non-negative safe integer");
    if (required === 0) return [];
    this.#closed = true;
    const values: Keyed<Output>[] = [];
    const observed: GroupEntry<Output>[] = [];
    try {
      for await (const entry of this.asCompleted()) {
        observed.push(entry);
        if (entry.outcome?.kind === "succeeded" && accept(entry.outcome.value)) {
          values.push({ key: entry.key, value: entry.outcome.value });
          if (values.length === required) {
            await this.cancel();
            return values;
          }
        }
      }
    } catch (error) {
      await this.cancel();
      throw error;
    }
    throw new GroupError(this.id, observed);
  }
  async reduce<Accumulator>(identityValue: Accumulator, reducer: (value: Accumulator, entry: GroupEntry<Output>) => Accumulator | Promise<Accumulator>): Promise<Accumulator> { const joined = await this.join(); let value = identityValue; for (const entry of joined.entries) value = await reducer(value, entry); return value; }
  async close(): Promise<{ readonly groupId: GroupId }> { this.#closed = true; return { groupId: this.id }; }
  async cancel(): Promise<CancelReceipt> { this.#closed = true; await Promise.all(this.#entries.map(entry => entry.admission.kind === "accepted" ? entry.admission.task.cancel() : undefined)); return { requested: true, groupId: this.id }; }
}

export class GroupError<Output> extends Error { constructor(readonly groupId: GroupId, readonly entries: readonly GroupEntry<Output>[]) { super("task group did not satisfy its completion strategy"); } }

export class TaskContext {
  readonly #harness: AgentHarness;
  constructor(harness: AgentHarness, readonly signal: AbortSignal, readonly taskId?: RuntimeTaskId, readonly durable = harness.host !== undefined) {
    this.#harness = harness;
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
    this.signal.throwIfAborted();
    const content = this.#harness.content;
    if (!content) throw new Error("content reader is not bound");
    const file = (await NativeContracts.create()).validate("file_ref", reference);
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
    this.signal.throwIfAborted();
    if (!operationId.trim() || !(bytes instanceof Uint8Array)) throw new TypeError("stable upload identity and bytes are required");
    const payload = Uint8Array.from(bytes);
    if (payload.byteLength > this.#harness.limits.file_bytes || new TextEncoder().encode(path).byteLength > this.#harness.limits.path_bytes) {
      throw new TypeError("file exceeds task content limits");
    }
    const content = this.#harness.content;
    if (!content?.writer) throw new Error("task has no owner-bound content writer");
    if (!this.#harness.scope.grants.includes(content.writer.writeCapability())) throw new Error("task scope cannot write this volume");
    const file = (await NativeContracts.create()).validate("file_ref",
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
      if (!this.#harness.host || !this.taskId) throw new Error("durable interaction requires a host and task identity");
      const id = await interactionId(interaction.id);
      if (interaction.kind === "approval") {
        if (!interaction.operationId || !interaction.actionDigest) throw new Error("durable approval requires an exact action binding");
        await approvalBinding({ operation_id: interaction.operationId, action_digest: interaction.actionDigest });
      }
      const route = this.#harness.host.routeInteraction;
      if (!route) throw new Error("durable interaction routing is not bound");
      return route.call(this.#harness.host, this.taskId, id, { ...interaction, id });
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
    const admitted = (await NativeContracts.create()).validateToolValue(schema.document, answer.value);
    return { kind: "answered", value: schema.parse(admitted) };
  }
  async ask(prompt: string, operationId?: string): Promise<Answer> {
    if (this.durable && !operationId?.trim()) throw new Error("hosted interactions require a stable operation ID");
    return this.interact({ id: operationId ?? crypto.randomUUID(), kind: "question", prompt });
  }
  async reconcileEffect(effectId: EffectId): Promise<EffectStatus> {
    if (!this.taskId) throw new Error("effect reconciliation requires a task identity");
    const host = this.#harness.host;
    if (!host) throw new Error("no durable harness host is bound");
    const status = await host.reconcileEffect(this.taskId, effectId);
    switch (status.state) {
      case "succeeded": {
        const result = (await NativeContracts.create()).validate("file_ref", status.result);
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
    const host = this.#harness.host;
    if (!host) throw new Error("no durable harness host is bound");
    if (this.durable && !messageId) throw new Error("durable messages require a stable message ID");
    return host.send({ id: messageId ?? identity<MessageId>("message"), recipient,
      value: this.#harness.contracts.validate("file_ref", value),
      ...(this.taskId === undefined ? {} : { sender: this.taskId }) });
  }
  inbox(from?: MessageId): AsyncIterable<TaskMessage> {
    if (!this.taskId) throw new Error("task inbox requires a durable task identity");
    const host = this.#harness.host;
    if (!host) throw new Error("no durable harness host is bound");
    return host.inbox(this.taskId, from);
  }
  async sleepUntil(deadline: Date, operationId?: string): Promise<void> {
    if (!Number.isFinite(deadline.getTime())) throw new TypeError("timer deadline is invalid");
    if (this.durable) {
      if (!this.#harness.host || !this.taskId || !operationId?.trim()) throw new Error("durable timers require a host, task identity, and stable operation ID");
      const wait = this.#harness.host.waitUntil;
      if (!wait) throw new Error("durable timer routing is not bound");
      return wait.call(this.#harness.host, this.taskId, operationId, deadline);
    }
    const milliseconds = deadline.getTime() - Date.now();
    if (milliseconds <= 0) return;
    await abortableWait(milliseconds, this.signal);
  }
}
export class ToolContext extends TaskContext { constructor(harness: AgentHarness, signal: AbortSignal, readonly callId: string, taskId?: RuntimeTaskId, durable = harness.host !== undefined, readonly operationId?: string) { super(harness, signal, taskId, durable); } }

interface RegisteredTool<Input, Output> {
  readonly definition: ToolDefinition<Input, Output>;
  readonly executor?: ToolExecutor<Input, Output>;
}
const publicToolHandles = new WeakMap<RegisteredTool<any, any>, ToolRef<any, any>>();
function publicToolHandle<Input, Output>(registered: RegisteredTool<Input, Output>): ToolRef<Input, Output> {
  const cached = publicToolHandles.get(registered);
  if (cached !== undefined) return cached as ToolRef<Input, Output>;
  const { name, revision, description, inputSchema, outputSchema } = registered.definition;
  const handle = Object.freeze({ definition: Object.freeze({ name, revision, description, inputSchema, outputSchema }) }) as ToolRef<Input, Output>;
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
  readonly writer?: Readonly<{
    readonly volume: VolumeRef;
    readonly writeCapability: () => string;
    readonly stage: (operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string) => Promise<FileRef>;
  }>;
}

/** Capture an owner's routing methods and writer identity at registration. */
function pinContentBindings(contracts: NativeContracts, source: ContentBindings): ContentBindings {
  const writer = source.writer;
  return Object.freeze({
    validate: source.validate.bind(source),
    verify: source.verify.bind(source),
    read: source.read.bind(source),
    fileReadCapability: source.fileReadCapability.bind(source),
    volumeReadCapability: source.volumeReadCapability.bind(source),
    directoryReadCapability: source.directoryReadCapability.bind(source),
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
}>[]): ContentBindings {
  if (sources.length === 0) throw new TypeError("at least one content volume is required");
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
  const owner = (volume: VolumeRef): ContentBindings => {
    const binding = volumes.get(key(volume));
    if (!binding) throw new Error("content volume is not registered");
    return binding;
  };
  return Object.freeze({
    validate: (file: FileRef, limits: Limits) => owner(file.volume).validate(file, limits),
    verify: (file: FileRef, bytes: Uint8Array) => owner(file.volume).verify(file, bytes),
    read: (file: FileRef) => owner(file.volume).read(file),
    fileReadCapability: (file: FileRef) => owner(file.volume).fileReadCapability(file),
    volumeReadCapability: (volume: VolumeRef) => owner(volume).volumeReadCapability(volume),
    directoryReadCapability: (volume: VolumeRef, prefix: string) => owner(volume).directoryReadCapability(volume, prefix),
    ...(writer === undefined ? {} : { writer }),
  });
}

export class HarnessBuilder {
  constructor(readonly contracts: NativeContracts) {}
  readonly #tasks = new Map<string, TaskDefinition<any, any>>(); readonly #tools = new Map<string, RegisteredTool<any, any>>();
  readonly #toolSources = new Map<string, ToolDefinition<any, any>>();
  readonly #selectedTools = new Map<string, string>();
  #model?: ModelProvider; #loop?: AgentLoop; #context?: ContextBuilder; #interactions?: InteractionHandler; #policy?: Policy; #host?: HarnessRuntimeHost; #content?: ContentBindings;
  readonly #grants: string[] = [];
  #limits: Limits = DEFAULT_LIMITS;
  model(value: ModelProvider): this { this.#model = value; return this; }
  agentLoop(value: AgentLoop): this { this.#loop = value; return this; }
  context(value: ContextBuilder): this { this.#context = value; return this; }
  interactions(value: InteractionHandler): this { this.#interactions = value; return this; }
  policy(value: Policy): this { this.#policy = value; return this; }
  host(value: HarnessRuntimeHost): this { this.#host = value; return this; }
  content(value: ContentBindings): this { this.#content = pinContentBindings(this.contracts, value); return this; }
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
    validateToolName(definition.name);
    validateComponentLabel(definition.revision, "tool revision");
    if (typeof definition.parseInput !== "function" || typeof definition.parseOutput !== "function") {
      throw new TypeError("typed tool parsers are required");
    }
    this.contracts.encodeCanonicalJson(definition.inputSchema);
    this.contracts.encodeCanonicalJson(definition.outputSchema);
    if (!executor && !definition.handler) throw new TypeError("tool requires a handler or executor");
    const key = toolKey(definition.name, definition.revision);
    if (this.#tools.has(key)) throw new Error(`conflicting registration for ${key}`);
    const previouslyRegistered = [...this.#tools.values()].some(tool => tool.definition.name === definition.name);
    const pinned = Object.freeze({ ...definition,
      inputSchema: freezeSchema(structuredClone(definition.inputSchema)),
      outputSchema: freezeSchema(structuredClone(definition.outputSchema)) });
    this.#tools.set(key, Object.freeze(executor ? { definition: pinned, executor } : { definition: pinned }));
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
  build(): AgentHarness { const components: AgentHarnessComponents = { limits: this.#limits }; if (this.#model) components.model = this.#model; if (this.#loop) components.loop = this.#loop; if (this.#context) components.context = this.#context; if (this.#interactions) components.interactions = this.#interactions; if (this.#policy) components.policy = this.#policy; if (this.#host) components.host = this.#host; if (this.#content) components.content = this.#content; validateTaskRequirements(this.#tasks, this.#tools, components, this.#grants); return new AgentHarness(this.#tasks, this.#tools, components, ExecutionScope.create().grant(...this.#grants), new Map(), this.#selectedTools, this.#toolSources, this.contracts, harnessConstruction); }
}

interface AgentHarnessComponents { model?: ModelProvider; loop?: AgentLoop; context?: ContextBuilder; interactions?: InteractionHandler; policy?: Policy; host?: HarnessRuntimeHost; content?: ContentBindings; limits?: Limits }
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
  }
  get interactions(): InteractionHandler | undefined { return this.scope.interactionHandler ?? this.components.interactions; }
  get host(): HarnessRuntimeHost | undefined { return this.components.host; }
  get content(): ContentBindings | undefined { return this.components.content; }
  get limits(): Limits { return this.#contentLimits; }
  /** Check that a durable operation still uses the policy pinned at construction. */
  assertPolicyIdentity(): void { this.#assertPolicyIdentity(); }
  #assertPolicyIdentity(): void {
    const policy = this.scope.policyProvider ?? this.components.policy;
    if (!samePolicyIdentity(this.#policyIdentity, validatePolicyIdentity(policy?.identity() ?? null))) {
      throw new Error("runtime policy implementation changed after admission");
    }
    if (this.host && !samePolicyIdentity(this.#policyIdentity,
      validatePolicyIdentity(this.host.policyIdentity()))) {
      throw new Error("durable host policy implementation changed after admission");
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
    if (this.#tasks.get(taskKey(definition.name, definition.revision)) !== definition) throw new Error("task definition is not registered or no longer active");
    const parsed = definition.options.input?.parse(input) ?? input;
    if (definition.implementation.kind === "resumable") {
      throw new Error("resumable tasks require async durable admission; use admit with a stable operation ID");
    }
    const id = identity<RuntimeTaskId>("task");
    const handler = definition.implementation.handler;
    const task = new Task(id, async signal => { const output = await handler(new TaskContext(this, signal, id, false), parsed); return definition.options.output?.parse(output) ?? output; });
    this.running.set(id, task as Task<unknown>);
    return task;
  }
  async admit<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input, operationId: string, parentTaskId?: RuntimeTaskId): Promise<Admission<Output>> {
    this.#assertPolicyIdentity();
    if (!operationId.trim()) throw new TypeError("stable operation ID is required");
    if (this.#tasks.get(taskKey(definition.name, definition.revision)) !== definition) return { kind: "rejected", reason: { code: "unregistered", message: "task definition is not registered or no longer active" } };
    if (definition.implementation.kind === "live") return { kind: "rejected", reason: { code: "unsupported", message: "live tasks are local-only; use spawn without a durable operation ID" } };
    if (!this.host?.admitResumable) return { kind: "rejected", reason: { code: "unsupported", message: "resumable tasks require a durable host admission binding" } };
    let parsed: Input;
    try {
      const inputSchema = definition.options.input;
      if (!inputSchema) throw new Error("durable task input schema is not pinned");
      parsed = inputSchema.parse((await NativeContracts.create()).validateToolValue(inputSchema.document, input));
    }
    catch (error) { return { kind: "rejected", reason: { code: "invalid_input", message: error instanceof Error ? error.message : String(error) } }; }
    let admission: Admission<unknown>;
    try { admission = await this.host.admitResumable(operationId, definition, parsed, this, parentTaskId); }
    catch { return { kind: "indeterminate", operationId }; }
    this.#assertPolicyIdentity();
    if (admission.kind !== "accepted") return admission;
    const task = validatedHostTask(admission.task, operationId, pinnedOutputSchema(definition));
    this.running.set(task.id(), task as Task<unknown>);
    return { kind: "accepted", task };
  }
  group<Output>(policy: GroupPolicy, id?: GroupId): TaskGroup<Output> { return new TaskGroup(this, policy, id); }
  scoped(scope: ExecutionScope): AgentHarness {
    const parentPolicy = this.scope.policyProvider ?? this.components.policy;
    const grants = narrowGrants(this.scope.grants, scope.grants, scope.grantsExplicit || scope.grants.length > 0);
    const limits = narrowLimits(this.scope.limits, scope.limits);
    const policy = composePolicies(parentPolicy, scope.policyProvider);
    const narrowed = new ExecutionScope(
      scope.modelProvider ?? this.scope.modelProvider,
      scope.contextBuilder ?? this.scope.contextBuilder,
      scope.interactionHandler ?? this.scope.interactionHandler,
      policy,
      grants,
      limits,
      true,
    );
    return new AgentHarness(this.#tasks, this.#tools, this.components, narrowed, this.running, this.#selectedTools, this.#toolSources, this.contracts, harnessConstruction);
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
    const contracts = await NativeContracts.create();
    const admittedInput = contracts.validateToolValue(registered.definition.inputSchema, input);
    const parsedInput = registered.definition.parseInput(admittedInput);
    const publishOutput = (value: unknown): Output => registered.definition.parseOutput(
      contracts.validateToolValue(registered.definition.outputSchema, value));
    const callId = providerCallId ?? operationId ?? crypto.randomUUID();
    const invocation = { callId, name: tool.definition.name, arguments: parsedInput };
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
    const actionInstance = operationId ?? crypto.randomUUID();
    for (const [approvalIndex, request] of approvals.entries()) {
      const contracts = await NativeContracts.create();
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
    if (registered.definition.handler) return publishOutput(await registered.definition.handler(new ToolContext(this, signal, callId, taskId, false, operationId), parsedInput));
    if (!registered.executor) throw new Error(`tool has no executable binding: ${tool.definition.name}`);
    return publishOutput((await registered.executor.execute(invocation)).value);
  }
  async callDurable<Input, Output>(operationId: OperationId, tool: ToolRef<Input, Output>, input: Input,
    taskId: RuntimeTaskId, signal = new AbortController().signal): Promise<Outcome<Output, OperationId>> {
    if (!this.host?.executeTool || !operationId || !taskId) {
      throw new Error("durable tool calls require a host, task identity, and stable operation ID");
    }
    const { parsedInput, publishOutput } = await this.#prepareToolCall(tool, input, signal, operationId);
    // The owner host independently re-evaluates the pinned policy and routes any
    // exact-action approval through its journal; a caller-side decision grants nothing.
    const outcome = await this.host.executeTool(taskId, operationId, tool, parsedInput, this);
    return outcome.kind === "succeeded"
      ? { kind: "succeeded", value: publishOutput(outcome.value) }
      : outcome;
  }
  async runSelectedContext(selectedContext: SelectedModelContext): Promise<RunOutput> {
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
    if (input.selectedContext !== undefined) await validateSelectedContext(input.selectedContext, this.limits);
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
        ? input.prompt : [{ kind: "text", text: input.prompt }, ...input.content], this.limits);
    }
    const receipts: RunReceipt[] = [];
    const definition = TaskDefinition.live<RuntimeAgentInput, AgentOutput>("acyclic.default-agent", "1", async (context, input) => {
      const loop = this.components.loop;
      if (loop) return loop.run(context, input);
      const model = this.scope.modelProvider ?? this.components.model;
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
        await validateModelContent(message.content, this.limits);
      }
      let text = "";
      let textBytes = 0;
      const maxSteps = Math.min(this.scope.limits.maxSteps ?? this.limits.model_steps, this.limits.model_steps);
      for (let step = 0; step < maxSteps; step += 1) {
        const calls: Extract<ModelEvent, { kind: "tool_call" }>[] = [];
        const callIds = new Set<string>();
        let eventCount = 0;
        let completed = false;
        for await (const event of model.generate({ model: { provider: "configured", name: "default", revision: "pinned", options: {} }, messages, tools: this.#modelToolDefinitions(), signal: context.signal })) {
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
        for (const [index, call] of calls.entries()) {
          const toolOperationId = `model-tool:${context.taskId}:${step}:${index}`;
          const value = await context.call(this.tool(call.name), call.arguments, toolOperationId, call.callId);
          const projection = await boundedToolValue(value, this.limits.render_bytes);
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
    if (this.host) {
      this.#assertPolicyIdentity();
      const attachment = await this.host.attach(id, this);
      this.#assertPolicyIdentity();
      if (attachment.task.id() !== id) throw new Error("host attached a different task identity");
      const definition = this.#tasks.get(taskKey(attachment.taskName, attachment.revision));
      if (!definition || (requested !== undefined && requested !== definition)
        || definition.revision !== attachment.revision
        || definition.options.implementationDigest !== attachment.implementationDigest) {
        throw new Error("host task binding is not registered at its pinned implementation");
      }
      if (definition.implementation.kind !== "resumable") throw new Error("host task binding is not a resumable definition");
      return validatedHostTask(attachment.task, attachment.operationId, pinnedOutputSchema(definition));
    }
    throw new Error("task identity is not retained locally and no durable harness host is bound");
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
function validatedHostTask<Output>(original: Task<unknown>, operationId: string, schema: RuntimeSchema<Output>): Task<Output> {
  return Task.fromHost<Output>(original.id(), {
    operationId,
    async result() { return validateTaskOutcome(await original.result(), schema); },
    async *events(fromSequence) {
      for await (const event of original.events(fromSequence)) {
        if (event.event.kind === "settled") yield { ...event, event: { kind: "settled" as const, outcome: await validateTaskOutcome(event.event.outcome, schema) } };
        else yield { ...event, event: { kind: "started" as const } };
      }
    },
    async terminalEvent() {
      const event = await original.terminalEvent();
      if (event.event.kind !== "settled") throw new Error("host terminal event is not settled");
      return { ...event, event: { kind: "settled" as const,
        outcome: await validateTaskOutcome(event.event.outcome, schema) } };
    },
    cancel: () => original.cancel(),
  });
}
async function validateTaskOutcome<Output>(outcome: Outcome<unknown>, schema: RuntimeSchema<Output>): Promise<Outcome<Output>> {
  if (outcome.kind !== "succeeded") return outcome;
  try { return { kind: "succeeded", value: schema.parse((await NativeContracts.create()).validateToolValue(schema.document, outcome.value)) }; }
  catch (error) { return { kind: "failed", error: { message: error instanceof Error ? error.message : String(error) } }; }
}
function identity<Value extends string>(prefix: string): Value { return `${prefix}:${crypto.randomUUID()}` as Value; }
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
      if (requirement === "host" && components.host) continue;
      if (requirement === "content" && components.content) continue;
      if (requirement === "content:write" && components.content?.writer
        && grants.includes(components.content.writer.writeCapability())) continue;
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
function validatePolicyIdentity(identity: PolicyIdentity | null, ancestors = new Set<object>()): PolicyIdentity | null {
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
