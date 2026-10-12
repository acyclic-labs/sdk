import { type TaskId, type OperationId, type EffectId } from "./index.js";
import { type ToolJsonValue, validateComponentLabel, type ToolRef } from "./model.js";
import { type FileRef, type Limits } from "./conversation.js";
import { type TaskContext } from "./task-context.js";
import { NativeContracts, type WasmTaskAdmissionWire, type WasmDurableBatchWire, type WasmExecutionPlacementWire, type WasmNativeLimitsWire, type WasmTaskRunLimitsWire } from "./native-contracts.js";
import { type GroupEntry, type BatchAdmissionRequest, type BatchCancellationReport } from "./task-group.js";
import { type PolicyIdentity, type ComponentIdentity, type Interaction, type Answer, type EffectiveScope } from "./execution-scope.js";
import { type AgentHarness, type RunOutput } from "./agent-harness.js";
import { type InteractionId } from "./interaction.js";
import { type SelectedModelContext } from "./projection.js";
declare const identityBrand: unique symbol;
export type BatchId = string & { readonly [identityBrand]: "BatchId" };
export type GroupId = string & { readonly [identityBrand]: "GroupId" };
export type InputKey = Readonly<{ readonly batchId: BatchId; readonly index: number }>;
export type Outcome<Output, Identity extends string = string> =
  | { readonly kind: "succeeded"; readonly value: Output }
  | { readonly kind: "failed"; readonly error: { readonly message: string; readonly cause?: unknown } }
  | { readonly kind: "cancelled"; readonly receipt: CancelReceipt }
  | { readonly kind: "indeterminate"; readonly operationId: Identity };

export class AdmissionUncertainError extends Error {
  constructor(readonly operationId: string, cause?: unknown) {
    super("task admission outcome is indeterminate; reconcile by operation ID", { cause });
    this.name = "AdmissionUncertainError";
  }
}
export interface CancelReceipt { readonly requested: boolean; readonly taskId?: TaskId; readonly groupId?: GroupId }
export type Admission<Output> =
  | { readonly kind: "accepted"; readonly task: Task<Output> }
  | { readonly kind: "rejected"; readonly reason: Rejection }
  | { readonly kind: "indeterminate"; readonly operationId: string };
export interface Rejection { readonly code: "group_closed" | "unregistered" | "unsupported" | "invalid_input"; readonly message: string }
export interface TaskEvent<Output> {
  readonly id: string;
  readonly taskId: TaskId;
  readonly sequence: number;
  readonly event: { readonly kind: "started" } | { readonly kind: "settled"; readonly outcome: Outcome<Output> };
}
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
export interface RuntimeSchema<Value> {
  readonly id: string;
  readonly document: Readonly<{ [key: string]: ToolJsonValue }>;
  parse(value: unknown): Value;
  readonly [runtimeSchemaBrand]: (value: Value) => Value;
}

export async function defineRuntimeSchema<Value>(id: string,
  document: Readonly<{ [key: string]: ToolJsonValue }>, parse: (value: unknown) => Value): Promise<RuntimeSchema<Value>> {
  await validateComponentLabel(id, "task schema identity");
  if (document === null || typeof document !== "object" || Array.isArray(document)
    || Object.keys(document).length === 0 || typeof parse !== "function") {
    throw new TypeError("task schema must be a non-vacuous JSON Schema document and parser");
  }
  const detached = structuredClone(document);
  return Object.freeze({ id, document: freezeSchema(detached), parse, [runtimeSchemaBrand]: (value: Value) => value });
}

export function freezeSchema<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freezeSchema(child);
    Object.freeze(value);
  }
  return value;
}
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

export class TaskDefinition<Input, Output> {
  readonly options: TaskDefinitionOptions<Input, Output>;
  private constructor(
    readonly name: string,
    readonly revision: string,
    readonly implementation: | { readonly kind: "live"; readonly handler: (context: TaskContext, input: Input) => Output | Promise<Output> }
  | { readonly kind: "resumable"; readonly component: ResumableTask<Input, Output> },
    options: TaskDefinitionOptions<Input, Output>,
  ) {
    componentKey(name, revision);
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
  static async live<Input, Output>(name: string, revision: string, handler: (context: TaskContext, input: Input) => Output | Promise<Output>, options: TaskDefinitionOptions<Input, Output> = {}): Promise<TaskDefinition<Input, Output>> {
    await validateComponentLabel(name, "task name");
    await validateComponentLabel(revision, "task revision");
    return new TaskDefinition(name, revision, { kind: "live", handler }, options);
  }
  static async resumable<Input, Output, State>(name: string, revision: string, component: ResumableTask<Input, Output, State>, options: TaskDefinitionOptions<Input, Output> & Readonly<{ input: RuntimeSchema<Input>; output: RuntimeSchema<Output>; implementationDigest: string }>): Promise<TaskDefinition<Input, Output>> {
    await validateComponentLabel(name, "task name");
    await validateComponentLabel(revision, "task revision");
    if (component.state === null || typeof component.state !== "object" || !(runtimeSchemaBrand in component.state)) {
      throw new TypeError("resumable tasks require a pinned state schema");
    }
    const state = await defineRuntimeSchema<unknown>(component.state.id, component.state.document,
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
          await validateComponentLabel(next.effect.kind, "effect kind");
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
}
declare const taskRefBrand: unique symbol;
export interface TaskRef<Input, Output> {
  readonly name: string;
  readonly revision: string;
  readonly [taskRefBrand]: (input: Input) => Output;
}

const taskReferences = new WeakMap<object, object>();

const taskDefinitions = new WeakMap<object, object>();

export function publicTaskHandle<Input, Output>(definition: TaskDefinition<Input, Output>): TaskRef<Input, Output> {
  let handle = taskReferences.get(definition) as TaskRef<Input, Output> | undefined;
  if (handle === undefined) {
    handle = Object.freeze({ name: definition.name, revision: definition.revision }) as TaskRef<Input, Output>;
    taskReferences.set(definition, handle);
    taskDefinitions.set(handle, definition);
  }
  return handle;
}

export function registeredTaskDefinition<Input, Output>(value: TaskDefinition<Input, Output> | TaskRef<Input, Output>): TaskDefinition<Input, Output> {
  if (value instanceof TaskDefinition) return value;
  const definition = taskDefinitions.get(value) as TaskDefinition<Input, Output> | undefined;
  if (definition === undefined) throw new TypeError("task handle is not registered");
  return definition;
}
export type EffectStatus =
  | Readonly<{ state: "planned" | "dispatched" | "indeterminate" }>
  | Readonly<{ state: "succeeded"; result: FileRef }>
  | Readonly<{ state: "failed"; message: string }>;
export interface TaskMessage {
  readonly id: OperationId;
  readonly sender?: TaskId;
  readonly recipient: TaskId;
  readonly value: FileRef;
}
export interface HostTaskAttachment {
  readonly task: Task<unknown>;
  readonly operationId: string;
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  /** Complete immutable request retained by an independently bound state owner. */
  readonly admission?: WasmTaskAdmissionWire;
}
export interface HostBatchReplay {
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  /** Rust-canonical digest of the group, task, scope, and ordered inputs pinned at admission. */
  readonly inputDigest: readonly number[];
  readonly entries: readonly GroupEntry<unknown>[];
}
export interface HarnessRuntimeSpawner {
  policyIdentity(): PolicyIdentity | null;
  executionIdentity?(): ComponentIdentity | null;
  admitResumable?<Input, Output>(operationId: string, definition: TaskDefinition<Input, Output>, input: Input, harness: AgentHarness, parentTaskId?: TaskId, admission?: WasmTaskAdmissionWire): Promise<Admission<unknown>>;
  /** Retains the complete manifest before admitting any member. */
  admitBatch?(request: BatchAdmissionRequest, harness: AgentHarness): Promise<HostBatchReplay>;
  reconcileAdmission?(operationId: string, harness: AgentHarness): Promise<HostTaskAttachment | null>;
  /** Loads the owner's original immutable batch without re-admitting absent members. */
  loadBatch?(batchId: BatchId, harness: AgentHarness): Promise<WasmDurableBatchWire | null>;
  reconcileBatch?(request: BatchAdmissionRequest, harness: AgentHarness): Promise<HostBatchReplay>;
  /** Retains a batch cancellation declaration before reconciling member requests. */
  cancelBatch?(batchId: BatchId, harness: AgentHarness): Promise<BatchCancellationReport | null>;
}
export interface TaskChildrenPage {
  readonly revision: bigint;
  readonly entries: readonly Readonly<{ slot: string; taskId: TaskId }>[];
  readonly nextAfter: string | null;
}
export interface HarnessRuntimeState {
  policyIdentity(): PolicyIdentity | null;
  executionIdentity?(): ComponentIdentity | null;
  attach(id: TaskId, harness: AgentHarness): Promise<HostTaskAttachment>;
  /** Direct same-owner children, independent of conversation or fork ancestry. */
  children?(parent: TaskId, expectedRevision: bigint | null, afterSlot: string | null,
    maximum: number, harness: AgentHarness): Promise<TaskChildrenPage>;
  reconcileEffect(taskId: TaskId, effectId: EffectId): Promise<EffectStatus>;
  send(message: TaskMessage): Promise<{ readonly accepted: boolean; readonly messageId: OperationId }>;
  inbox(taskId: TaskId, from?: OperationId): AsyncIterable<TaskMessage>;
  /** Stages the request, admits a ref-only interaction, and reconciles its versioned response. */
  routeInteraction?(taskId: TaskId, id: InteractionId, interaction: Interaction & Readonly<{ id: InteractionId }>): Promise<Answer>;
  /** Commits a timer wait boundary and resumes from the same deadline after restart. */
  waitUntil?(taskId: TaskId, operationId: string, deadline: Date): Promise<void>;
  /** Owner-journaled exact tool operation; the owner may use any tool executor. */
  executeTool?<Input, Output>(taskId: TaskId, operationId: OperationId, tool: ToolRef<Input, Output>, input: Input, harness: AgentHarness): Promise<Outcome<unknown, OperationId>>;
  /** Replay or reconcile one exact selected turn through the owner's execution journal. */
  executeSelectedTurn?(operationId: OperationId, selected: SelectedModelContext, harness: AgentHarness): Promise<Outcome<RunOutput, OperationId>>;
}
export interface HarnessExecutionProvider {
  identity(): ComponentIdentity;
  spawner(): HarnessRuntimeSpawner;
  state(): HarnessRuntimeState;
  qualifyTask(request: WasmTaskAdmissionWire): Promise<WasmExecutionPlacementWire>;
  qualifyBatch(request: WasmDurableBatchWire): Promise<WasmExecutionPlacementWire>;
}

export async function observeAdmittedTask(state: HarnessRuntimeState, taskId: TaskId,
  harness: AgentHarness, operationId: string): Promise<HostTaskAttachment | null> {
  try { return await state.attach(taskId, harness); }
  catch (error) {
    if (error instanceof AdmissionUncertainError && error.operationId === operationId) return null;
    throw error;
  }
}

export class Task<Output> {
  readonly #events: TaskEvent<Output>[] = [];
  readonly #waiters = new Set<() => void>();
  readonly #result: Promise<Outcome<Output>>;
  readonly #driver: DurableTaskDriver<Output> | undefined;
  constructor(private readonly taskId: TaskId, execution: ((signal: AbortSignal) => Promise<Output>) | DurableTaskDriver<Output>, private readonly controller = new AbortController()) {
    if (typeof execution !== "function") {
      this.#driver = execution;
      this.#result = execution.result().catch(() => ({ kind: "indeterminate", operationId: execution.operationId } as const));
      return;
    }
    this.#driver = undefined;
    this.#events.push({ id: `${taskId}:0`, taskId, sequence: 0, event: { kind: "started" } });
    this.#result = execution(controller.signal).then<Outcome<Output>, Outcome<Output>>(
      value => ({ kind: "succeeded", value }),
      error => controller.signal.aborted ? { kind: "cancelled", receipt: { requested: true, taskId } } : { kind: "failed", error: { message: error instanceof Error ? error.message : String(error), cause: error } },
    ).then(outcome => { const terminal: TaskEvent<Output> = { id: `${taskId}:1`, taskId, sequence: 1, event: { kind: "settled", outcome } }; this.#events.push(terminal); for (const wake of this.#waiters) wake(); this.#waiters.clear(); return outcome; });
  }
  id(): TaskId { return this.taskId; }
  result(): Promise<Outcome<Output>> { return this.#result; }
  async terminalEvent(): Promise<TaskEvent<Output>> {
    if (this.#driver) return this.#driver.terminalEvent();
    await this.#result;
    return this.#events[1]!;
  }
  events(fromSequence = 0): AsyncIterable<TaskEvent<Output>> {
    if (!Number.isSafeInteger(fromSequence) || fromSequence < 0) {
      throw new RangeError("task event cursor must be a nonnegative safe integer");
    }
    if (!this.#driver) return this.#localEvents(fromSequence);
    return this.#hostEvents(fromSequence);
  }
  async *#localEvents(next: number): AsyncIterable<TaskEvent<Output>> {
    for (;;) {
      while (next < this.#events.length) yield this.#events[next++]!;
      if (this.#events.at(-1)?.event.kind === "settled") return;
      await new Promise<void>(resolve => this.#waiters.add(resolve));
    }
  }
  async *#hostEvents(fromSequence: number): AsyncIterable<TaskEvent<Output>> {
    let streamedTerminal: TaskEvent<Output> | undefined;
    let nextSequence = fromSequence;
    let invalidEvent = false;
    try {
      for await (const event of this.#driver!.events(fromSequence)) {
        if (event.taskId !== this.taskId || !Number.isSafeInteger(event.sequence)
          || event.sequence !== nextSequence || streamedTerminal) {
          invalidEvent = true;
          throw new Error("durable host returned a missing or invalid task event");
        }
        nextSequence++;
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
    if (!streamedTerminal && terminal.sequence < fromSequence) {
      if (nextSequence !== fromSequence) {
        throw new Error("durable host returned a terminal event before its streamed events");
      }
      return;
    }
    if (!streamedTerminal && terminal.sequence !== nextSequence) {
      throw new Error("durable host omitted task events before its terminal event");
    }
    if (!streamedTerminal && terminal.sequence >= fromSequence) {
      yield terminal;
    }
  }
  async cancel(): Promise<CancelReceipt> { if (this.#driver) return this.#driver.cancel(); this.controller.abort(new Error("task cancellation requested")); return { requested: true, taskId: this.taskId }; }
}

export function pinnedOutputSchema<Input, Output>(definition: TaskDefinition<Input, Output>): RuntimeSchema<Output> {
  const schema = definition.options.output;
  if (definition.implementation.kind !== "resumable" || !schema) throw new Error("durable task output schema is not pinned");
  return schema;
}

export function validatedHostTask<Output>(original: Task<unknown>, operationId: string, schema: RuntimeSchema<Output>, contracts: NativeContracts): Task<Output> {
  return new Task<Output>(original.id(), {
    operationId,
    async result() { return validateTaskOutcome(await original.result(), schema, contracts); },
    async *events(fromSequence) {
      for await (const event of original.events(fromSequence)) {
        if (event.event.kind === "settled") yield { ...event, event: { kind: "settled" as const, outcome: validateTaskOutcome(event.event.outcome, schema, contracts) } };
        else yield { ...event, event: { kind: "started" as const } };
      }
    },
    async terminalEvent() {
      const event = await original.terminalEvent();
      if (event.event.kind !== "settled") throw new Error("host terminal event is not settled");
      return { ...event, event: { kind: "settled" as const,
        outcome: validateTaskOutcome(event.event.outcome, schema, contracts) } };
    },
    cancel: () => original.cancel(),
  });
}

export function validateTaskOutcome<Output>(outcome: Outcome<unknown>, schema: RuntimeSchema<Output>, contracts: NativeContracts): Outcome<Output> {
  if (outcome.kind !== "succeeded") return outcome;
  try { return { kind: "succeeded", value: schema.parse(contracts.validateToolValue(schema.document, outcome.value)) }; }
  catch (error) { return { kind: "failed", error: { message: error instanceof Error ? error.message : String(error) } }; }
}

export function componentKey(name: string, revision: string): string {
  if (!name || !revision || name.includes("@") || revision.includes("@")) {
    throw new TypeError("task name and revision must be nonempty and cannot contain @");
  }
  return `${name}@${revision}`;
}

export function durableMachineDigest<Input, Output>(definition: TaskDefinition<Input, Output>): readonly number[] {
  const { implementationDigest } = definition.options;
  if (definition.implementation.kind !== "resumable" || !implementationDigest) {
    throw new TypeError("durable identity requires a pinned resumable definition");
  }
  const bytes = [...implementationDigest.matchAll(/../g)].map(match => Number.parseInt(match[0]!, 16));
  if (bytes.length !== 32) throw new TypeError("durable machine digest must contain 32 bytes");
  return bytes;
}

export function nativeLimits(value: Limits): WasmNativeLimitsWire {
  return {
    file_bytes: BigInt(value.file_bytes), path_bytes: BigInt(value.path_bytes),
    attachments: BigInt(value.attachments), render_bytes: BigInt(value.render_bytes),
    model_steps: BigInt(value.model_steps), model_events_per_step: BigInt(value.model_events_per_step),
    tool_calls_per_step: BigInt(value.tool_calls_per_step), context_messages: BigInt(value.context_messages),
  };
}

export function nativeRunLimits(value: EffectiveScope["limits"]): WasmTaskRunLimitsWire {
  return {
    concurrency: value.concurrency === undefined ? null : BigInt(value.concurrency),
    max_steps: value.maxSteps === undefined ? null : BigInt(value.maxSteps),
    deadline_epoch_ms: value.deadline === undefined ? null : BigInt(value.deadline.getTime()),
  };
}
