import type { AgentInput, AgentLoop, AgentOutput, ContextBuilder, ModelEvent, ModelMessage, ModelProvider, ToolDefinition, ToolExecutor, ToolRef } from "./model.js";
export * from "./model.js";

declare const identityBrand: unique symbol;
export type RuntimeTaskId = string & { readonly [identityBrand]: "RuntimeTaskId" };
export type BatchId = string & { readonly [identityBrand]: "BatchId" };
export type GroupId = string & { readonly [identityBrand]: "GroupId" };
export type EffectId = string & { readonly [identityBrand]: "EffectId" };
export type MessageId = string & { readonly [identityBrand]: "MessageId" };
export type InputKey = Readonly<{ readonly batchId: BatchId; readonly index: number }>;

export type Outcome<Output> =
  | { readonly kind: "succeeded"; readonly value: Output }
  | { readonly kind: "failed"; readonly error: TaskFailure }
  | { readonly kind: "cancelled"; readonly receipt: CancelReceipt }
  | { readonly kind: "indeterminate"; readonly operationId: string };
export interface TaskFailure { readonly message: string; readonly cause?: unknown }
export interface CancelReceipt { readonly requested: boolean; readonly taskId?: RuntimeTaskId; readonly groupId?: GroupId }
export type Admission<Output> =
  | { readonly kind: "accepted"; readonly task: Task<Output> }
  | { readonly kind: "rejected"; readonly reason: Rejection }
  | { readonly kind: "indeterminate"; readonly operationId: string };
export interface Rejection { readonly code: "group_closed" | "unregistered" | "unsupported"; readonly message: string }

export interface TaskEvent<Output> {
  readonly id: string;
  readonly taskId: RuntimeTaskId;
  readonly sequence: number;
  readonly event: { readonly kind: "started" } | { readonly kind: "settled"; readonly outcome: Outcome<Output> };
}

export interface TaskDefinitionOptions<Input, Output> {
  readonly input?: RuntimeSchema<Input>;
  readonly output?: RuntimeSchema<Output>;
  readonly requirements?: readonly string[];
}
export interface RuntimeSchema<Value> { readonly id: string; parse(value: unknown): Value }
export type LiveTask<Input, Output> = (context: TaskContext, input: Input) => Output | Promise<Output>;
export interface ResumableTransition<State, Output> {
  readonly kind: "continue" | "finish";
  readonly state?: State;
  readonly output?: Output;
}
export interface ResumableTask<Input, Output, State = unknown> {
  initial(input: Input): State;
  transition(context: TaskContext, state: State, previous?: Outcome<unknown>): Promise<ResumableTransition<State, Output>>;
}

/** Immutable, version-pinned typed task registration. */
export class TaskDefinition<Input, Output> {
  private constructor(
    readonly name: string,
    readonly revision: string,
    readonly handler: LiveTask<Input, Output>,
    readonly options: TaskDefinitionOptions<Input, Output>,
  ) {
    if (!name.trim() || !revision.trim()) throw new TypeError("task name and revision are required");
  }
  static live<Input, Output>(name: string, revision: string, handler: LiveTask<Input, Output>, options: TaskDefinitionOptions<Input, Output> = {}): TaskDefinition<Input, Output> {
    return new TaskDefinition(name, revision, handler, options);
  }
  static resumable<Input, Output, State>(name: string, revision: string, component: ResumableTask<Input, Output, State>, options: TaskDefinitionOptions<Input, Output> = {}): TaskDefinition<Input, Output> {
    return new TaskDefinition(name, revision, async (context, input) => {
      let state = component.initial(input); let previous: Outcome<unknown> | undefined;
      for (;;) {
        context.signal.throwIfAborted();
        const next = await component.transition(context, state, previous);
        if (next.kind === "finish") {
          if (!("output" in next)) throw new Error("resumable finish transition omitted output");
          return next.output as Output;
        }
        if (!("state" in next)) throw new Error("resumable continue transition omitted state");
        state = next.state as State; previous = undefined;
      }
    }, options);
  }
  version(): string { return this.revision; }
}

export type RunReceipt =
  | { readonly kind: "tool"; readonly callId: string; readonly name: string; readonly value: unknown }
  | { readonly kind: "model-completed"; readonly metadata: unknown };
export interface RunOutput<Content = unknown, Artifact = unknown, Receipt = RunReceipt> extends AgentOutput<Content, Artifact> { readonly taskId: RuntimeTaskId; readonly receipts: readonly Receipt[] }
export interface AgentHarnessHost { connect(): Promise<AgentHarness> }
export type EffectOutcome<Value> =
  | { readonly kind: "completed"; readonly value: Value }
  | { readonly kind: "pending" }
  | { readonly kind: "unknown" };
export interface TaskMessage<Value = unknown> {
  readonly id: MessageId;
  readonly sender?: RuntimeTaskId;
  readonly recipient: RuntimeTaskId;
  readonly value: Value;
}
/** Durable host operations. Implementations own persistence, admission, and replay semantics. */
export interface HarnessRuntimeHost {
  attach<Output>(id: RuntimeTaskId, harness: AgentHarness): Promise<Task<Output>>;
  reconcileEffect<Value>(taskId: RuntimeTaskId, effectId: EffectId): Promise<EffectOutcome<Value>>;
  send<Value>(message: TaskMessage<Value>): Promise<{ readonly accepted: boolean; readonly messageId: MessageId }>;
  inbox<Value>(taskId: RuntimeTaskId, from?: MessageId): AsyncIterable<TaskMessage<Value>>;
}
export interface InteractionHandler { route(interaction: Interaction): Promise<Answer> }
export interface Policy { evaluate(invocation: Invocation, scope: EffectiveScope): Promise<PolicyDecision> }
export interface Interaction { readonly id: string; readonly kind: "question" | "approval"; readonly prompt: string; readonly deadline?: Date }
export type Answer = { readonly kind: "accepted"; readonly text: string } | { readonly kind: "declined" | "cancelled" | "expired" };
export interface Invocation { readonly kind: "tool"; readonly tool: string; readonly arguments: unknown }
export interface EffectiveScope { readonly grants: readonly string[]; readonly limits: Readonly<{ readonly concurrency?: number; readonly deadline?: Date }> }
export type PolicyDecision = { readonly kind: "allow" } | { readonly kind: "deny"; readonly reason: string } | { readonly kind: "require-approval"; readonly prompt: string };

export class ExecutionScope {
  constructor(
    readonly modelProvider?: ModelProvider,
    readonly contextBuilder?: ContextBuilder,
    readonly interactionHandler?: InteractionHandler,
    readonly policyProvider?: Policy,
    readonly grants: readonly string[] = [],
    readonly limits: EffectiveScope["limits"] = {},
  ) {}
  static create(): ExecutionScope { return new ExecutionScope(); }
  model(value: ModelProvider): ExecutionScope { return new ExecutionScope(value, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, this.limits); }
  context(value: ContextBuilder): ExecutionScope { return new ExecutionScope(this.modelProvider, value, this.interactionHandler, this.policyProvider, this.grants, this.limits); }
  interactions(value: InteractionHandler): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, value, this.policyProvider, this.grants, this.limits); }
  policy(value: Policy): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, this.interactionHandler, value, this.grants, this.limits); }
  grant(...values: readonly string[]): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, this.interactionHandler, this.policyProvider, [...this.grants, ...values], this.limits); }
  withLimits(value: EffectiveScope["limits"]): ExecutionScope { return new ExecutionScope(this.modelProvider, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, value); }
}

export type GroupPolicy = { readonly kind: "collect-all" } | { readonly kind: "cancel-on-failure" };
export const GroupPolicies = Object.freeze({ collectAll: { kind: "collect-all" } as const, cancelOnFailure: { kind: "cancel-on-failure" } as const });

export class Batch<Input> {
  constructor(readonly id: BatchId, readonly inputs: readonly Input[]) { if (!id) throw new TypeError("batch id is required"); }
}
export interface GroupEntry<Output> { readonly key: InputKey; readonly admission: Admission<Output>; readonly outcome?: Outcome<Output> }
export interface GroupOutcome<Output> { readonly groupId: GroupId; readonly complete: boolean; readonly entries: readonly GroupEntry<Output>[] }
export interface Keyed<Output> { readonly key: InputKey; readonly value: Output }

export class Task<Output> {
  readonly #events = new ReplayQueue<TaskEvent<Output>>();
  readonly #result: Promise<Outcome<Output>>;
  #sequence = 0;
  constructor(readonly taskId: RuntimeTaskId, execute: (signal: AbortSignal) => Promise<Output>, readonly controller = new AbortController()) {
    this.#events.push({ id: `${taskId}:0`, taskId, sequence: this.#sequence++, event: { kind: "started" } });
    this.#result = execute(controller.signal).then<Outcome<Output>, Outcome<Output>>(
      value => ({ kind: "succeeded", value }),
      error => controller.signal.aborted ? { kind: "cancelled", receipt: { requested: true, taskId } } : { kind: "failed", error: { message: error instanceof Error ? error.message : String(error), cause: error } },
    ).then(outcome => { this.#events.push({ id: `${taskId}:${this.#sequence}`, taskId, sequence: this.#sequence++, event: { kind: "settled", outcome } }); this.#events.close(); return outcome; });
  }
  id(): RuntimeTaskId { return this.taskId; }
  result(): Promise<Outcome<Output>> { return this.#result; }
  events(fromSequence = 0): AsyncIterable<TaskEvent<Output>> { return this.#events.from(fromSequence); }
  async cancel(): Promise<CancelReceipt> { this.controller.abort(new Error("task cancellation requested")); return { requested: true, taskId: this.taskId }; }
}

export class TaskGroup<Output> {
  readonly id = identity<GroupId>("group"); readonly #entries: GroupEntry<Output>[] = []; #closed = false;
  constructor(readonly harness: AgentHarness, readonly policy: GroupPolicy) {}
  async spawnMany<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>): Promise<readonly GroupEntry<Output>[]> {
    if (this.#closed) return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index }, admission: { kind: "rejected", reason: { code: "group_closed", message: "group is closed" } } }));
    const entries = batch.inputs.map((input, index): GroupEntry<Output> => { const task = this.harness.spawn(definition, input); return { key: { batchId: batch.id, index }, admission: { kind: "accepted", task } }; }); this.#entries.push(...entries); return entries;
  }
  async reconcileBatch(batch: Batch<unknown>): Promise<readonly GroupEntry<Output>[]> { return this.#entries.filter(entry => entry.key.batchId === batch.id); }
  async *asCompleted(): AsyncIterable<GroupEntry<Output>> {
    const pending = new Map(this.#entries.flatMap((entry, index) => entry.admission.kind === "accepted" ? [[index, entry] as const] : []));
    while (pending.size) { const completed = await Promise.race([...pending].map(async ([index, entry]) => ({ index, entry: withOutcome(entry, await (entry.admission as Extract<Admission<Output>, { kind: "accepted" }>).task.result()) }))); pending.delete(completed.index); yield completed.entry; }
  }
  async map<Input>(definition: TaskDefinition<Input, Output>, inputs: readonly Input[]): Promise<readonly Output[]> {
    if (!inputs.length) return [];
    const entries = await this.spawnMany(definition, new Batch(identity<BatchId>("batch"), inputs)); const outcomes = await Promise.all(entries.map(entry => entry.admission.kind === "accepted" ? entry.admission.task.result().then(async outcome => { if (outcome.kind !== "succeeded" && this.policy.kind === "cancel-on-failure") await this.cancel(); return outcome; }) : Promise.resolve<Outcome<Output>>({ kind: "failed", error: { message: entry.admission.kind === "rejected" ? entry.admission.reason.message : "admission indeterminate" } })));
    const failed = outcomes.find(outcome => outcome.kind !== "succeeded");
    if (failed) { if (this.policy.kind === "cancel-on-failure") await this.cancel(); throw new GroupError(this.id, entries.map((entry, index) => withOutcome(entry, outcomes[index]!))); }
    return outcomes.map(outcome => (outcome as Extract<Outcome<Output>, { kind: "succeeded" }>).value);
  }
  async join(): Promise<GroupOutcome<Output>> { this.#closed = true; const entries = await Promise.all(this.#entries.map(async entry => entry.admission.kind === "accepted" ? withOutcome(entry, await entry.admission.task.result()) : entry)); return { groupId: this.id, complete: entries.every(entry => entry.admission.kind !== "indeterminate" && entry.outcome?.kind !== "indeterminate"), entries }; }
  async race(): Promise<GroupEntry<Output>> {
    this.#closed = true;
    const accepted = this.#entries.filter((entry): entry is GroupEntry<Output> & { admission: { kind: "accepted"; task: Task<Output> } } => entry.admission.kind === "accepted");
    if (!accepted.length) throw new GroupError(this.id, this.#entries);
    const settled = accepted.map(async entry => ({ ...entry, outcome: await entry.admission.task.result() }));
    try {
      const winner = await Promise.any(settled.map(async result => { const entry = await result; if (entry.outcome.kind !== "succeeded") throw entry; return entry; }));
      await Promise.all(accepted.filter(entry => entry.key !== winner.key).map(entry => entry.admission.task.cancel()));
      return winner;
    } catch { throw new GroupError(this.id, await Promise.all(settled)); }
  }
  async firstSuccess(): Promise<Keyed<Output>> { const winner = await this.race(); if (winner.outcome?.kind !== "succeeded") throw new GroupError(this.id, [winner]); return { key: winner.key, value: winner.outcome.value }; }
  async quorum(required: number, accept: (value: Output) => boolean): Promise<readonly Keyed<Output>[]> { if (!Number.isSafeInteger(required) || required < 0) throw new RangeError("required must be a non-negative safe integer"); if (required === 0) return []; const joined = await this.join(); const values = joined.entries.flatMap(entry => entry.outcome?.kind === "succeeded" && accept(entry.outcome.value) ? [{ key: entry.key, value: entry.outcome.value }] : []); if (values.length < required) throw new GroupError(this.id, joined.entries); return values.slice(0, required); }
  async reduce<Accumulator>(identityValue: Accumulator, reducer: (value: Accumulator, entry: GroupEntry<Output>) => Accumulator | Promise<Accumulator>): Promise<Accumulator> { const joined = await this.join(); let value = identityValue; for (const entry of joined.entries) value = await reducer(value, entry); return value; }
  async close(): Promise<{ readonly groupId: GroupId }> { this.#closed = true; return { groupId: this.id }; }
  async cancel(): Promise<CancelReceipt> { this.#closed = true; await Promise.all(this.#entries.map(entry => entry.admission.kind === "accepted" ? entry.admission.task.cancel() : undefined)); return { requested: true, groupId: this.id }; }
}

export class GroupError<Output> extends Error { constructor(readonly groupId: GroupId, readonly entries: readonly GroupEntry<Output>[]) { super("task group did not satisfy its completion strategy"); } }

export class TaskContext {
  constructor(readonly harness: AgentHarness, readonly signal: AbortSignal, readonly taskId?: RuntimeTaskId) {}
  task<Input, Output>(name: string): TaskDefinition<Input, Output> { return this.harness.task(name); }
  tool<Input, Output>(name: string): ToolRef<Input, Output> { return this.harness.tool(name); }
  call<Input, Output>(tool: ToolRef<Input, Output>, input: Input): Promise<Output> { return this.harness.call(tool, input, this.signal, this.taskId); }
  spawn<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input): Task<Output> { return this.harness.spawn(definition, input); }
  group<Output>(policy: GroupPolicy): TaskGroup<Output> { return this.harness.group(policy); }
  async ask(prompt: string): Promise<Answer> { const handler = this.harness.interactions; if (!handler) throw new Error("no interaction handler is bound"); return handler.route({ id: crypto.randomUUID(), kind: "question", prompt }); }
  reconcileEffect<Value>(effectId: EffectId): Promise<EffectOutcome<Value>> {
    if (!this.taskId) throw new Error("effect reconciliation requires a durable task identity");
    const host = this.harness.host;
    if (!host) throw new Error("no durable harness host is bound");
    return host.reconcileEffect(this.taskId, effectId);
  }
  send<Value>(recipient: RuntimeTaskId, value: Value, messageId = identity<MessageId>("message")): Promise<{ readonly accepted: boolean; readonly messageId: MessageId }> {
    const host = this.harness.host;
    if (!host) throw new Error("no durable harness host is bound");
    return host.send({ id: messageId, recipient, value, ...(this.taskId === undefined ? {} : { sender: this.taskId }) });
  }
  inbox<Value>(from?: MessageId): AsyncIterable<TaskMessage<Value>> {
    if (!this.taskId) throw new Error("task inbox requires a durable task identity");
    const host = this.harness.host;
    if (!host) throw new Error("no durable harness host is bound");
    return host.inbox(this.taskId, from);
  }
  async sleepUntil(deadline: Date): Promise<void> { const milliseconds = deadline.getTime() - Date.now(); if (milliseconds <= 0) return; await abortableWait(milliseconds, this.signal); }
}
export class ToolContext extends TaskContext { constructor(harness: AgentHarness, signal: AbortSignal, readonly callId: string, taskId?: RuntimeTaskId) { super(harness, signal, taskId); } }

export class HarnessBuilder {
  readonly #tasks = new Map<string, TaskDefinition<any, any>>(); readonly #tools = new Map<string, ToolRef<any, any>>();
  #model?: ModelProvider; #loop?: AgentLoop; #context?: ContextBuilder; #interactions?: InteractionHandler; #policy?: Policy; #host?: HarnessRuntimeHost;
  model(value: ModelProvider): this { this.#model = value; return this; }
  agentLoop(value: AgentLoop): this { this.#loop = value; return this; }
  context(value: ContextBuilder): this { this.#context = value; return this; }
  interactions(value: InteractionHandler): this { this.#interactions = value; return this; }
  policy(value: Policy): this { this.#policy = value; return this; }
  host(value: HarnessRuntimeHost): this { this.#host = value; return this; }
  task<Input, Output>(value: TaskDefinition<Input, Output>): this { register(this.#tasks, value.name, value, value.revision); return this; }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>, executor?: ToolExecutor<Input, Output>): this { if (!executor && !definition.handler) throw new TypeError("tool requires a handler or executor"); register(this.#tools, definition.name, executor ? { definition, executor } : { definition }, definition.revision); return this; }
  build(): AgentHarness { const components: AgentHarnessComponents = {}; if (this.#model) components.model = this.#model; if (this.#loop) components.loop = this.#loop; if (this.#context) components.context = this.#context; if (this.#interactions) components.interactions = this.#interactions; if (this.#policy) components.policy = this.#policy; if (this.#host) components.host = this.#host; return new AgentHarness(this.#tasks, this.#tools, components); }
}

interface AgentHarnessComponents { model?: ModelProvider; loop?: AgentLoop; context?: ContextBuilder; interactions?: InteractionHandler; policy?: Policy; host?: HarnessRuntimeHost }
export class AgentHarness {
  readonly #tasks: ReadonlyMap<string, TaskDefinition<any, any>>; readonly #tools: ReadonlyMap<string, ToolRef<any, any>>;
  constructor(tasks: ReadonlyMap<string, TaskDefinition<any, any>>, tools: ReadonlyMap<string, ToolRef<any, any>>, readonly components: AgentHarnessComponents, readonly scope = ExecutionScope.create(), readonly running: Map<RuntimeTaskId, Task<unknown>> = new Map()) { this.#tasks = new Map(tasks); this.#tools = new Map(tools); }
  get interactions(): InteractionHandler | undefined { return this.scope.interactionHandler ?? this.components.interactions; }
  get host(): HarnessRuntimeHost | undefined { return this.components.host; }
  task<Input, Output>(name: string): TaskDefinition<Input, Output> { const value = this.#tasks.get(name); if (!value) throw new Error(`task is not registered: ${name}`); return value as TaskDefinition<Input, Output>; }
  tool<Input, Output>(name: string): ToolRef<Input, Output> { const value = this.#tools.get(name); if (!value) throw new Error(`tool is not registered: ${name}`); return value as ToolRef<Input, Output>; }
  spawn<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input): Task<Output> { if (this.#tasks.get(definition.name) !== definition) throw new Error("task definition is not registered or no longer active"); const parsed = definition.options.input?.parse(input) ?? input; const id = identity<RuntimeTaskId>("task"); const task = new Task(id, async signal => { const output = await definition.handler(new TaskContext(this, signal, id), parsed); return definition.options.output?.parse(output) ?? output; }); this.running.set(id, task as Task<unknown>); return task; }
  group<Output>(policy: GroupPolicy): TaskGroup<Output> { return new TaskGroup(this, policy); }
  scoped(scope: ExecutionScope): AgentHarness {
    const parentPolicy = this.scope.policyProvider ?? this.components.policy;
    const grants = narrowGrants(this.scope.grants, scope.grants);
    const limits = narrowLimits(this.scope.limits, scope.limits);
    const policy = composePolicies(parentPolicy, scope.policyProvider);
    const narrowed = new ExecutionScope(
      scope.modelProvider ?? this.scope.modelProvider,
      scope.contextBuilder ?? this.scope.contextBuilder,
      scope.interactionHandler ?? this.scope.interactionHandler,
      policy,
      grants,
      limits,
    );
    return new AgentHarness(this.#tasks, this.#tools, this.components, narrowed, this.running);
  }
  async call<Input, Output>(tool: ToolRef<Input, Output>, input: Input, signal = new AbortController().signal, taskId?: RuntimeTaskId): Promise<Output> { signal.throwIfAborted(); const invocation = { callId: crypto.randomUUID(), name: tool.definition.name, arguments: input }; const policy = this.scope.policyProvider ?? this.components.policy; if (policy) { const decision = await policy.evaluate({ kind: "tool", tool: invocation.name, arguments: input }, { grants: this.scope.grants, limits: this.scope.limits }); if (decision.kind === "deny") throw new Error(decision.reason); if (decision.kind === "require-approval") { const answer = await this.interactions?.route({ id: invocation.callId, kind: "approval", prompt: decision.prompt }); if (answer?.kind !== "accepted") throw new Error("tool approval was not accepted"); } } if (tool.definition.handler) return tool.definition.handler(new ToolContext(this, signal, invocation.callId, taskId), input); if (!tool.executor) throw new Error(`tool has no executable binding: ${tool.definition.name}`); return (await tool.executor.execute(invocation)).value; }
  async run(prompt: string): Promise<RunOutput> {
    const receipts: RunReceipt[] = [];
    const definition = TaskDefinition.live<AgentInput, AgentOutput>("acyclic.default-agent", "1", async (context, input) => {
      const loop = this.components.loop;
      if (loop) return loop.run(context, input);
      const model = this.scope.modelProvider ?? this.components.model;
      if (!model) throw new Error("no model or agent loop is bound");
      const contextBuilder = this.scope.contextBuilder ?? this.components.context;
      const messages: ModelMessage[] = [...(await contextBuilder?.build(input, [{ role: "user", content: input.prompt }]) ?? [{ role: "user", content: input.prompt }])];
      let text = "";
      for (let step = 0; step < 64; step += 1) {
        const calls: Extract<ModelEvent, { kind: "tool_call" }>[] = [];
        for await (const event of model.generate({ model: { provider: "configured", name: "default", revision: "pinned", options: {} }, messages, tools: [...this.#tools.values()].map(value => value.definition), signal: context.signal })) {
          if (event.kind === "content") text += event.delta;
          else if (event.kind === "tool_call") calls.push(event);
          else if (event.kind === "completed") receipts.push({ kind: "model-completed", metadata: event.metadata });
        }
        if (calls.length === 0) return { text };
        for (const call of calls) {
          const value = await context.call(this.tool(call.name), call.arguments);
          receipts.push({ kind: "tool", callId: call.callId, name: call.name, value });
          messages.push({ role: "assistant", content: call }, { role: "tool", content: { callId: call.callId, name: call.name, value } });
        }
      }
      throw new Error("agent loop exceeded 64 model steps");
    });
    const tasks = new Map(this.#tasks); tasks.set(definition.name, definition);
    const runtime = new AgentHarness(tasks, this.#tools, this.components, this.scope, this.running);
    const task = runtime.spawn(definition, { prompt });
    const outcome = await task.result();
    if (outcome.kind !== "succeeded") throw new TaskRunError(task.id(), outcome);
    return { ...outcome.value, taskId: task.id(), receipts };
  }
  async attach<Output>(id: RuntimeTaskId): Promise<Task<Output>> {
    const local = this.running.get(id);
    if (local) return local as Task<Output>;
    if (this.host) return this.host.attach(id, this);
    throw new Error("task identity is not retained locally and no durable harness host is bound");
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
function identity<Value extends string>(prefix: string): Value { return `${prefix}:${crypto.randomUUID()}` as Value; }
function register<Value>(values: Map<string, Value>, name: string, value: Value, revision: string): void { if (values.has(name)) throw new Error(`conflicting registration for ${name}`); if (!revision.trim()) throw new TypeError("registration revision is required"); values.set(name, value); }
function narrowGrants(parent: readonly string[], child: readonly string[]): readonly string[] {
  if (child.length === 0) return parent;
  if (parent.length !== 0 && child.some(grant => !parent.includes(grant))) throw new Error("child scope cannot widen grants");
  return [...new Set(child)];
}
function narrowLimits(parent: EffectiveScope["limits"], child: EffectiveScope["limits"]): EffectiveScope["limits"] {
  const concurrency = child.concurrency ?? parent.concurrency;
  if (concurrency !== undefined && (!Number.isSafeInteger(concurrency) || concurrency <= 0)) throw new RangeError("scope concurrency must be a positive safe integer");
  if (parent.concurrency !== undefined && concurrency !== undefined && concurrency > parent.concurrency) throw new Error("child scope cannot widen concurrency");
  const deadline = child.deadline ?? parent.deadline;
  if (deadline !== undefined && Number.isNaN(deadline.getTime())) throw new RangeError("scope deadline must be valid");
  if (parent.deadline !== undefined && deadline !== undefined && deadline > parent.deadline) throw new Error("child scope cannot extend deadline");
  return { ...(concurrency === undefined ? {} : { concurrency }), ...(deadline === undefined ? {} : { deadline: new Date(deadline) }) };
}
function composePolicies(parent: Policy | undefined, child: Policy | undefined): Policy | undefined {
  if (!parent || parent === child) return child ?? parent;
  if (!child) return parent;
  return {
    async evaluate(invocation, scope) {
      const inherited = await parent.evaluate(invocation, scope);
      if (inherited.kind === "deny") return inherited;
      const narrowed = await child.evaluate(invocation, scope);
      if (narrowed.kind === "deny") return narrowed;
      if (inherited.kind === "require-approval" && narrowed.kind === "require-approval") {
        return { kind: "require-approval", prompt: `${inherited.prompt}\n\n${narrowed.prompt}` };
      }
      return inherited.kind === "require-approval" ? inherited : narrowed;
    },
  };
}
async function abortableWait(milliseconds: number, signal: AbortSignal): Promise<void> { await new Promise<void>((resolve, reject) => { const timeout = setTimeout(resolve, milliseconds); signal.addEventListener("abort", () => { clearTimeout(timeout); reject(signal.reason); }, { once: true }); }); }
