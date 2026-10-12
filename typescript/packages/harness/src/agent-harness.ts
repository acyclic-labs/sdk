import { type OperationId, type TaskId, type Scope } from "./index.js";
import { type AgentInput, type ModelContent, type ToolDefinition, type ToolRef, type ToolInvocation, type RuntimeAgentInput, type ModelMessage, type ModelEvent, type ToolJsonSchema, type ModelToolDefinition } from "./model.js";
import { type SelectedModelContext, publicModelContent } from "./projection.js";
import { type TaskDefinition, Task, durableMachineDigest, nativeLimits, nativeRunLimits, freezeSchema, type HostTaskAttachment, componentKey, type Admission, observeAdmittedTask, validatedHostTask, pinnedOutputSchema, AdmissionUncertainError, type GroupId, type Outcome, type TaskChildrenPage } from "./task.js";
import { type RegisteredTool, type AgentHarnessComponents, harnessConstruction } from "./harness-builder.js";
import { type PolicyIdentity, ExecutionScope, type InteractionHandler, type ComponentIdentity, samePolicyIdentity, validatePolicyIdentity, narrowLimits, composePolicies, policyApprovals, ToolApprovalError } from "./execution-scope.js";
import { type FileRef, type Attachment } from "./conversation.js";
import { type NativeContracts, type WasmExecutionPlacementWire, type WasmTaskAdmissionWire, type WasmTaskAdmissionInput, type WasmToolJsonValue, type ModelEventAdmissionState } from "./native-contracts.js";
import { type ProjectWorkspaceProvider } from "./project.js";
import { type InteractionId, type InteractionResponse, type ResolutionReceipt } from "./interaction.js";
import { pinForkParentSnapshot } from "./content.js";
import { type ForkParentSnapshot, type ForkReport, type ForkPublisher, type ForkSeed } from "./fork.js";
import { TaskContext, ToolContext } from "./task-context.js";
import { type GroupPolicy } from "./enums.js";
import { TaskGroup } from "./task-group.js";
import { validateSelectedModelContext as validateSelectedModelContextWasm, validateUserInput as validateUserInputWasm, validateModelContent, prepareModelRequest as prepareModelRequestWasm, type ModelRequest as ModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";
import { HARNESS_CHILD_PAGE_DEFAULT } from "./child-page-contract.js";

export class IndeterminateModelTurnError extends Error {
  constructor(readonly operationId: OperationId, cause?: unknown) {
    super("model dispatch outcome is indeterminate; reconcile durably or explicitly abandon the local turn", { cause });
    this.name = "IndeterminateModelTurnError";
  }
}

export class TerminalModelTurnError extends Error {
  constructor(readonly operationId: OperationId, readonly outcome: "failed" | "cancelled", message: string) {
    super(message);
    this.name = "TerminalModelTurnError";
  }
}
export type RunReceipt =
  | { readonly kind: "tool"; readonly step: number; readonly callId: string; readonly name: string; readonly arguments: unknown; readonly value: unknown; readonly projection: unknown }
  | { readonly kind: "model-completed"; readonly metadata: unknown };
export interface RunOutput { readonly text: string; readonly attachments?: readonly Attachment[]; readonly taskId: TaskId; readonly receipts: readonly RunReceipt[] }
export type SelectedAgentInput =
  | (AgentInput & { readonly selectedContext?: never })
  /** Exact selection already committed by the owning conversation provider. */
  | { readonly selectedContext: SelectedModelContext; readonly prompt?: never; readonly content?: never };

export class AgentHarness {
  readonly #tasks: ReadonlyMap<string, TaskDefinition<never, unknown>>; readonly #tools: ReadonlyMap<string, RegisteredTool<never, unknown>>;
  readonly #selectedTools: ReadonlyMap<string, string>;
  readonly #policyIdentity: PolicyIdentity | null;
  readonly components: Readonly<AgentHarnessComponents>;
  constructor(tasks: ReadonlyMap<string, TaskDefinition<never, unknown>>, tools: ReadonlyMap<string, RegisteredTool<never, unknown>>, components: AgentHarnessComponents, readonly scope: ExecutionScope, readonly running: Map<TaskId, Task<unknown>>, selectedTools: ReadonlyMap<string, string>, readonly contracts: NativeContracts, construction: typeof harnessConstruction, policyIdentity: PolicyIdentity | null) {
    if (construction !== harnessConstruction) throw new TypeError("AgentHarness must be created through HarnessBuilder or scoped()");
    this.#tasks = tasks;
    this.#tools = tools;
    this.#selectedTools = selectedTools;
    this.components = Object.freeze({ ...components, limits: contracts.validate("limits", components.limits) });
    this.#policyIdentity = policyIdentity;
  }
  get interactions(): InteractionHandler | undefined { return this.scope.interactionHandler ?? this.components.interactions; }
  projectWorkspaces(): ProjectWorkspaceProvider {
    if (!this.scope.grants.includes("fork:publish") && !this.scope.grants.includes("project:merge")) {
      throw new Error("runtime scope lacks project authority");
    }
    if (!this.components.workspaces) throw new Error("project workspace provider is not bound");
    return this.components.workspaces;
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
  /** Rust's durable admission pins one policy implementation, not an unrecorded composition. */
  durablePolicyIdentity(): ComponentIdentity | null {
    if (this.#policyIdentity !== null && "composition" in this.#policyIdentity) {
      throw new Error("durable batch policy composition requires a pinned owner policy");
    }
    return this.#policyIdentity;
  }
  /** Create a new view for a later parent revision without retargeting admitted tasks. */
  atParentSnapshot(snapshot: ForkParentSnapshot): AgentHarness {
    if (!this.scope.grants.includes("fork:publish")) throw new Error("runtime scope lacks fork:publish");
    const current = this.components.parentSnapshot;
    if (!current) throw new Error("parent fork control is not bound");
    if (!this.contracts.canonicalEqual(current.parent, snapshot.parent)) {
      throw new Error("fork snapshot belongs to another parent conversation");
    }
    if (snapshot.revision <= current.revision) throw new Error("fork snapshot must advance the parent revision");
    return new AgentHarness(this.#tasks, this.#tools,
      { ...this.components, parentSnapshot: pinForkParentSnapshot(snapshot) },
      this.scope, this.running, this.#selectedTools, this.contracts, harnessConstruction, this.#policyIdentity);
  }
  #checkedForkReport(report: ForkReport): { publisher: ForkPublisher; report: ForkReport; seed: ForkSeed; operationId: OperationId } {
    if (!this.scope.grants.includes("fork:publish")) throw new Error("runtime scope lacks fork:publish");
    const publisher = this.components.forkPublisher;
    if (!publisher) throw new Error("fork publisher is not bound");
    const checked = this.contracts.validate("fork_report", report);
    // Keep the caller's original idempotency key across canonicalization.
    const operationId = report.request.operation_id;
    const parent = this.components.parentSnapshot?.parent;
    if (!parent || !this.contracts.canonicalEqual(checked.request.parent, parent)
      || !this.contracts.canonicalEqual(checked.request.parent, publisher.parent())) {
      throw new Error("fork report belongs to another parent");
    }
    return { publisher, report: checked, operationId,
      seed: this.contracts.validate("fork_seed", this.contracts.forkSeed(checked)) };
  }
  /** Publishes and durably binds the exact child seed. */
  async spawnFromReport(report: ForkReport): Promise<ForkSeed> {
    const checked = this.#checkedForkReport(report);
    const published = this.contracts.validate("fork_seed", await checked.publisher.spawnFromReport(checked.report));
    if (!this.contracts.canonicalEqual(published, checked.seed)) {
      throw new Error("fork publisher bound another child seed");
    }
    return published;
  }
  /** Observes the original publication; null remains unresolved. */
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
  /** Canonical, owner-attestable inputs and authority for one durable task. */
  async admissionRecord<Input, Output>(operationId: string, definition: TaskDefinition<Input, Output>,
    input: Input, parentTaskId?: TaskId, execution: WasmExecutionPlacementWire | null = null): Promise<WasmTaskAdmissionWire> {
    if (definition.implementation.kind !== "resumable" || !definition.options.input || !definition.options.output
      || !definition.options.implementationDigest) throw new TypeError("durable admission requires a pinned resumable task");
    if (execution !== null) await this.validateExecutionPlacement(execution);
    const machineDigest = durableMachineDigest(definition);
    const projection: WasmTaskAdmissionInput = {
      operation_id: operationId,
      name: definition.name,
      version: definition.revision,
      input: input as WasmToolJsonValue,
      input_schema: definition.options.input.document,
      output_schema: definition.options.output.document,
      requirements: [...definition.options.requirements ?? []],
      machine_digest: machineDigest,
      parent: parentTaskId ?? null,
      resource_proofs: [],
      grants: [...this.scope.grants].sort(),
      limits: nativeLimits(this.components.limits),
      run_limits: nativeRunLimits(this.scope.limits),
      policy: this.durablePolicyIdentity(),
      extensions: null,
      execution,
    };
    return freezeSchema(this.contracts.admitTask(projection));
  }
  async validateExecutionPlacement(value: WasmExecutionPlacementWire | null): Promise<void> {
    const selected = this.components.execution;
    if (!selected) {
      if (value !== null) throw new Error("retained admission selected an unbound execution route");
      return;
    }
    await this.#assertExecutionIdentity();
    if (value === null) throw new Error("qualified execution route is missing");
    const checked = this.contracts.validate("execution_placement", value);
    if (!this.contracts.canonicalEqual(checked.provider, selected.identity())) {
      throw new Error("execution qualifier returned another provider");
    }
  }
  async attestAdmission(observed: HostTaskAttachment, expected: WasmTaskAdmissionWire): Promise<void> {
    if (observed.admission === undefined || !this.contracts.canonicalEqual(observed.admission, expected)) {
      throw new Error("state owner admission differs from the exact spawner request");
    }
    await this.#validateObservedAdmission(observed.admission);
  }
  async #validateObservedAdmission(record: WasmTaskAdmissionWire): Promise<void> {
    this.contracts.validate("task_admission", record);
    const definition = this.#tasks.get(componentKey(record.task.name, record.task.version));
    if (!definition || definition.implementation.kind !== "resumable") throw new Error("state owner retained an unregistered task machine");
    const identities = this.contracts.taskAdmissionIdentities({
      name: definition.name, version: definition.revision,
      input_schema: definition.options.input!.document, output_schema: definition.options.output!.document,
      requirements: [...definition.options.requirements ?? []], machine_digest: durableMachineDigest(definition),
    });
    if (!this.contracts.canonicalEqual(identities.task, record.task)
      || !this.contracts.canonicalEqual(identities.machine, record.machine)
      || !this.contracts.canonicalEqual(definition.options.input?.document, record.input_schema)
      || !this.contracts.canonicalEqual(definition.options.output?.document, record.output_schema)) {
      throw new Error("state owner retained an unregistered task machine or schema");
    }
    this.contracts.validateToolValue(record.input_schema, record.input);
    await this.validateExecutionPlacement(record.execution);
    if (!samePolicyIdentity(this.#policyIdentity, await validatePolicyIdentity(record.policy)) || record.extensions !== null
      || record.grants.some(grant => !this.scope.grants.includes(grant))
      || !this.contracts.canonicalEqual(record.limits, nativeLimits(this.components.limits))) {
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
  async assertPolicyIdentity(): Promise<void> {
    await this.#assertExecutionIdentity();
    const policy = this.scope.policyProvider ?? this.components.policy;
    if (!samePolicyIdentity(this.#policyIdentity, await validatePolicyIdentity(policy?.identity() ?? null))) {
      throw new Error("runtime policy implementation changed after admission");
    }
    if (this.components.state && !samePolicyIdentity(this.#policyIdentity,
      await validatePolicyIdentity(this.components.state.policyIdentity()))) {
      throw new Error("durable state policy implementation changed after admission");
    }
    if (this.components.spawner && !samePolicyIdentity(this.#policyIdentity,
      await validatePolicyIdentity(this.components.spawner.policyIdentity()))) {
      throw new Error("durable spawner policy implementation changed after admission");
    }
  }
  async #assertExecutionIdentity(): Promise<void> {
    const route = this.components.execution;
    if (!route) return;
    const selected = await validatePolicyIdentity(route.identity());
    if (selected === null || "composition" in selected
      || !samePolicyIdentity(selected, await validatePolicyIdentity(this.components.state?.executionIdentity?.() ?? null))
      || !samePolicyIdentity(selected, await validatePolicyIdentity(this.components.spawner?.executionIdentity?.() ?? null))) {
      throw new Error("execution provider, state, and spawner routes differ");
    }
  }
  task<Input, Output>(definition: TaskDefinition<Input, Output>): TaskDefinition<Input, Output>;
  task(name: string): TaskDefinition<unknown, unknown>;
  task<Input, Output>(value: string | TaskDefinition<Input, Output>): TaskDefinition<Input, Output> {
    if (typeof value !== "string") {
      if (this.#tasks.get(componentKey(value.name, value.revision)) !== (value as unknown as TaskDefinition<never, unknown>)) {
        throw new Error("task definition is not registered or no longer active");
      }
      return value;
    }
    const exact = this.#tasks.get(value);
    if (exact) return (exact as unknown as TaskDefinition<Input, Output>);
    const matches = [...this.#tasks.values()].filter(definition => definition.name === value);
    if (matches.length > 1) throw new Error(`task revision is ambiguous: ${value}`);
    if (matches.length === 0) throw new Error(`task is not registered: ${value}`);
    return (matches[0]! as unknown as TaskDefinition<Input, Output>);
  }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>): ToolRef<Input, Output>;
  tool(name: string): ToolRef<unknown, unknown>;
  tool<Input, Output>(value: string | ToolDefinition<Input, Output>): ToolRef<Input, Output> {
    if (typeof value !== "string") {
      const key = componentKey(value.name, value.revision);
      const exact = this.#tools.get(key);
      if (!exact || exact.source !== (value as unknown as ToolDefinition<never, unknown>)) {
        throw new Error("tool definition is not registered or no longer active");
      }
      return ((exact as unknown as RegisteredTool<Input, Output>)).handle;
    }
    const name = value;
    const exact = this.#tools.get(name);
    if (exact) return ((exact as unknown as RegisteredTool<Input, Output>)).handle;
    const selected = this.#selectedTools.get(name);
    if (selected) return ((this.#tools.get(componentKey(name, selected))! as unknown as RegisteredTool<Input, Output>)).handle;
    const matches = [...this.#tools.values()].filter(tool => tool.definition.name === name);
    if (matches.length > 1) throw new Error(`tool revision is ambiguous: ${name}`);
    if (matches.length === 0) throw new Error(`tool is not registered: ${name}`);
    return ((matches[0]! as unknown as RegisteredTool<Input, Output>)).handle;
  }
  spawn<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input): Task<Output> {
    if (this.components.execution) {
      throw new Error("live task closures cannot cross an execution provider; register a resumable task and admit it with a stable operation ID");
    }
    if (this.#tasks.get(componentKey(definition.name, definition.revision)) !== (definition as unknown as TaskDefinition<never, unknown>)) throw new Error("task definition is not registered or no longer active");
    const parsed = definition.options.input?.parse(input) ?? input;
    if (definition.implementation.kind === "resumable") {
      throw new Error("resumable tasks require async durable admission; use admit with a stable operation ID");
    }
    const id = (crypto.randomUUID() as TaskId);
    const handler = definition.implementation.handler;
    const task = new Task(id, async signal => { const output = await handler(new TaskContext(this, signal, id, false), parsed); return definition.options.output?.parse(output) ?? output; });
    this.running.set(id, task as Task<unknown>);
    return task;
  }
  async admit<Input, Output>(definition: TaskDefinition<Input, Output>, input: Input, operationId: string, parentTaskId?: TaskId): Promise<Admission<Output>> {
    await this.assertPolicyIdentity();
    this.contracts.validateIdentity("operation", operationId);
    if (this.#tasks.get(componentKey(definition.name, definition.revision)) !== (definition as unknown as TaskDefinition<never, unknown>)) return { kind: "rejected", reason: { code: "unregistered", message: "task definition is not registered or no longer active" } };
    if (definition.implementation.kind === "live") return { kind: "rejected", reason: { code: "unsupported", message: "live tasks are local-only; use spawn without a durable operation ID" } };
    const spawner = this.components.spawner;
    const host = this.components.state;
    if (!spawner?.admitResumable || !host) return { kind: "rejected", reason: { code: "unsupported", message: "resumable tasks require durable spawner and state bindings" } };
    let parsed: Input;
    try {
      const inputSchema = definition.options.input;
      if (!inputSchema) throw new Error("durable task input schema is not pinned");
      parsed = inputSchema.parse(this.contracts.validateToolValue(inputSchema.document, input));
      this.contracts.validateToolValue(inputSchema.document, parsed);
    }
    catch (error) { return { kind: "rejected", reason: { code: "invalid_input", message: error instanceof Error ? error.message : String(error) } }; }
    let expected = await this.admissionRecord(operationId, definition, parsed, parentTaskId);
    if (this.components.execution) {
      if (!spawner.reconcileAdmission) throw new Error("execution route requires admission reconciliation");
      const existing = await spawner.reconcileAdmission(operationId, this);
      await this.assertPolicyIdentity();
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
        await this.attestAdmission(observed, existing.admission);
        const task = validatedHostTask<Output>(observed.task, operationId, pinnedOutputSchema(definition), this.contracts);
        this.running.set(task.id(), task as Task<unknown>);
        return { kind: "accepted", task };
      }
      const placement = this.contracts.validate("execution_placement",
        await this.components.execution.qualifyTask(expected));
      await this.validateExecutionPlacement(placement);
      await this.assertPolicyIdentity();
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
    await this.assertPolicyIdentity();
    if (admission.kind !== "accepted") return admission;
    const id = admission.task.id();
    const observed = await observeAdmittedTask(host, id, this, operationId);
    if (observed === null) return { kind: "indeterminate", operationId };
    if (observed.operationId !== operationId || observed.task.id() !== id
      || observed.taskName !== definition.name || observed.revision !== definition.revision
      || observed.implementationDigest !== definition.options.implementationDigest) {
      throw new Error("spawner and state host task bindings differ");
    }
    if (this.components.spawner || this.components.state) await this.attestAdmission(observed, expected);
    const authenticated = validatedHostTask<Output>(observed.task, operationId, pinnedOutputSchema(definition), this.contracts);
    this.running.set(authenticated.id(), authenticated as Task<unknown>);
    return { kind: "accepted", task: authenticated };
  }
  async reconcileAdmission(operationId: string): Promise<Task<unknown> | null> {
    this.contracts.validateIdentity("operation", operationId);
    const spawner = this.components.spawner;
    const host = this.components.state;
    if (!spawner?.reconcileAdmission || !host) throw new Error("durable admission reconciliation is not bound");
    await this.assertPolicyIdentity();
    const attachment = await spawner.reconcileAdmission(operationId, this);
    await this.assertPolicyIdentity();
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
      await this.attestAdmission(observed, attachment.admission);
    }
    return await this.#validatedHostAttachment(observed);
  }
  group<Output>(policy: GroupPolicy, id?: GroupId): TaskGroup<Output> { return new TaskGroup(this, policy, id); }
  async scoped(scope: ExecutionScope): Promise<AgentHarness> {
    if (scope.modelBinding) this.contracts.encodeCanonicalJson(scope.modelBinding.identity.options);
    const parentPolicy = this.scope.policyProvider ?? this.components.policy;
    const grants = scope.grantsExplicit || scope.grants.length > 0 ? scope.grants : this.scope.grants;
    if (grants.some(grant => !this.scope.grants.includes(grant))) throw new Error("child scope cannot widen grants");
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
    delete components.parentSnapshot;
    delete components.forkPublisher;
    delete components.workspaces;
    if (scope.executionProvider) {
      components.execution = scope.executionProvider;
      components.state = scope.executionProvider.state();
      components.spawner = scope.executionProvider.spawner();
    }
    const policyIdentity = await validatePolicyIdentity((narrowed.policyProvider ?? components.policy)?.identity() ?? null);
    return new AgentHarness(this.#tasks, this.#tools, components, narrowed, this.running, this.#selectedTools, this.contracts, harnessConstruction, policyIdentity);
  }
  async #prepareToolCall<Input, Output>(tool: ToolRef<Input, Output>, input: Input,
    signal: AbortSignal, operationId?: string, providerCallId?: string) {
    signal.throwIfAborted();
    const erased = this.#tools.get(componentKey(tool.definition.name, tool.definition.revision));
    if (erased === undefined) {
      throw new Error("tool definition is not registered or no longer active");
    }
    const registered = (erased as unknown as RegisteredTool<Input, Output>);
    if (registered.handle !== tool) throw new Error("tool definition is not registered or no longer active");
    const capability = `tool:call:${tool.definition.name}`;
    if (!this.scope.grants.includes(capability)) throw new Error(`task scope lacks ${capability}`);
    const contracts = this.contracts;
    const admittedInput = contracts.validateToolValue(registered.definition.inputSchema, input);
    const parsedInput = registered.definition.parseInput(admittedInput);
    const publishOutput = (value: unknown): Output => {
      contracts.validateToolResult(registered.definition, { value });
      return registered.definition.parseOutput(
        contracts.validateToolValue(registered.definition.outputSchema, value));
    };
    const toolOperationId = operationId === undefined ? crypto.randomUUID()
      : contracts.validateIdentity("operation", operationId);
    const callId = providerCallId ?? toolOperationId;
    const invocation = { operationId: toolOperationId, callId, name: tool.definition.name, arguments: parsedInput };
    contracts.validateToolInvocation(registered.definition, {
      callId,
      name: invocation.name,
      arguments: admittedInput,
    });
    await this.assertPolicyIdentity();
    const policy = this.scope.policyProvider ?? this.components.policy;
    const decision = await policy?.evaluate(
      { kind: "tool", tool: invocation.name, arguments: admittedInput },
      { grants: this.scope.grants, limits: this.scope.limits },
    ) ?? { kind: "allow" as const };
    await this.assertPolicyIdentity();
    if (decision.kind === "deny") throw new Error(decision.reason);
    const approvals = policyApprovals(decision, policy?.identity() ?? null);
    return { admittedInput, parsedInput, publishOutput, callId, invocation, approvals };
  }
  async call<Input, Output>(tool: ToolRef<Input, Output>, input: Input, signal = new AbortController().signal,
    taskId?: TaskId, operationId?: string, providerCallId?: string): Promise<Output> {
    return (await this.#invokeTool(tool, input, signal, taskId, operationId, providerCallId)).value;
  }
  async #invokeTool<Input, Output>(tool: ToolRef<Input, Output>, input: Input, signal: AbortSignal,
    taskId?: TaskId, operationId?: string, providerCallId?: string): Promise<{ value: Output; invocation: ToolInvocation<Input> }> {
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
          projection_schema: tool.definition.projectionSchema,
        },
        invocation: { call_id: callId, name: invocation.name, arguments: admittedInput },
      });
      const approval = contracts.validate("approval_binding", {
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
      const kind: ToolApprovalError["kind"] = answer === undefined ? "indeterminate"
        : answer.kind === "accepted" || answer.kind === "answered" ? "invalid_response" : answer.kind;
      throw new ToolApprovalError(kind, approval.operation_id);
    }
    const erased = this.#tools.get(componentKey(tool.definition.name, tool.definition.revision));
    if (erased === undefined) {
      throw new Error("tool definition is not registered or no longer active");
    }
    const registered = (erased as unknown as RegisteredTool<Input, Output>);
    if (registered.handle !== tool) throw new Error("tool definition is not registered or no longer active");
    if (registered.machine) throw new Error("resumable tool requires callDurable and its owner host");
    if (registered.definition.handler) return { invocation,
      value: publishOutput(await registered.definition.handler(new ToolContext(this, signal, callId, taskId, false, invocation.operationId), parsedInput)) };
    if (!registered.executor) throw new Error(`tool has no executable binding: ${tool.definition.name}`);
    return { invocation, value: publishOutput((await registered.executor.execute(invocation)).value) };
  }
  async callDurable<Input, Output>(operationId: OperationId, tool: ToolRef<Input, Output>, input: Input,
    taskId: TaskId, signal = new AbortController().signal): Promise<Outcome<Output, OperationId>> {
    if (!this.components.state?.executeTool || !operationId || !taskId) {
      throw new Error("durable tool calls require task state, task identity, and stable operation ID");
    }
    const { parsedInput, publishOutput } = await this.#prepareToolCall(tool, input, signal, operationId);
    const outcome = await this.components.state.executeTool(taskId, operationId, tool, parsedInput, this);
    return outcome.kind === "succeeded"
      ? { kind: "succeeded", value: publishOutput(outcome.value) }
      : outcome;
  }
  async runSelectedContext(selectedContext: SelectedModelContext, operationId?: OperationId): Promise<RunOutput> {
    if (this.components.state?.executeSelectedTurn !== undefined) {
      if (operationId === undefined) throw new TypeError("durable selected turns require a stable operation ID");
      validateSelectedModelContextWasm(selectedContext, this.components.limits);
      await this.assertPolicyIdentity();
      const outcome = await this.components.state.executeSelectedTurn(operationId, selectedContext, this);
      await this.assertPolicyIdentity();
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
      if (new TextEncoder().encode(outcome.value.text).byteLength > this.components.limits.file_bytes) {
        throw new TypeError("host assistant output exceeds file limit");
      }
      return structuredClone(outcome.value);
    }
    if (this.components.state) throw new Error("durable selected-turn execution is not bound; local replay would duplicate an owner operation");
    return this.run({ selectedContext });
  }
  async run(value: string | SelectedAgentInput): Promise<RunOutput> {
    if (typeof value !== "string" && value.selectedContext !== undefined
      && (Object.hasOwn(value, "prompt") || Object.hasOwn(value, "content"))) {
      throw new TypeError("selected context cannot be combined with another prompt or content");
    }
    const input: RuntimeAgentInput = typeof value === "string" ? { prompt: value }
      : value.selectedContext === undefined ? value as AgentInput
      : { prompt: "", selectedContext: value.selectedContext };
    let admittedDirectContent: ModelContent | undefined;
    if (input.selectedContext !== undefined) validateSelectedModelContextWasm(input.selectedContext, this.components.limits);
    else {
      const prompt = input.prompt;
      const content = input.content;
      if (typeof prompt !== "string" || !Array.isArray(content ?? [])) {
        throw new TypeError("user input shape is invalid");
      }
      if (content !== undefined && content.length > this.components.limits.attachments) {
        throw new TypeError("turn attachments exceed harness limits");
      }
      admittedDirectContent = content === undefined || content.length === 0 ? prompt
        : structuredClone(prompt ? [{ kind: "text" as const, text: prompt }, ...content] : content);
      validateUserInputWasm(admittedDirectContent);
      validateModelContent(admittedDirectContent, this.components.limits);
    }
    const receipts: RunReceipt[] = [];
    if (this.components.execution) throw new Error("live task closures cannot cross an execution provider");
    const id = crypto.randomUUID() as TaskId;
    const task = new Task(id, async signal => {
      const context = new TaskContext(this, signal, id, false);
      const model = this.scope.modelBinding ?? this.components.model;
      if (!model) throw new Error("no model is bound");
      const contextBuilder = this.scope.contextBuilder ?? this.components.context;
      const first: ModelMessage = { role: "user", content: admittedDirectContent ?? "" };
      const selected = input.selectedContext;
      const base = selected?.messages ?? [first];
      const messages: ModelMessage[] = [...(await contextBuilder?.build(input, base) ?? base)];
      let text = "";
      let previousAdmission: ModelEventAdmissionState = { count: 0, calls: [], completed: false, text_bytes: 0 };
      const maxSteps = Math.min(this.scope.limits.maxSteps ?? this.components.limits.model_steps, this.components.limits.model_steps);
      for (let step = 0; step < maxSteps; step += 1) {
        const calls: Extract<ModelEvent, { kind: "tool_call" }>[] = [];
        let admission: ModelEventAdmissionState = { ...previousAdmission, count: 0, calls: [], completed: false };
        const request = structuredClone({ model: model.identity, messages, tools: this.#modelToolDefinitions(), ...(this.components.modelOutputTokens === undefined ? {} : { maxOutputTokens: this.components.modelOutputTokens }) });
        const inventories = request.messages.map(message =>
          this.contracts.modelContentInventory(message.content, this.components.limits));
        // Local file grants cannot authenticate native option bindings.
        if (inventories.some(inventory => inventory.nativeConfigurations.length !== 0)) {
          throw new Error("original native option admission is unavailable in the local TypeScript runtime");
        }
        const prefix = this.components.inheritedModelPrefix;
        let bytes: Uint8Array;
        if (prefix === undefined) bytes = prepareModelRequestWasm(request, nativeLimits(this.components.limits));
        else {
          const files = new Map(prefix.files);
          const pending = new Map<string, FileRef>();
          for (const inventory of inventories) {
            for (const file of inventory.files) {
              prefix.core.verifyContentRead(prefix.scope, file);
              const key = new TextDecoder().decode(this.contracts.encodeCanonicalJson(file));
              if (!files.has(key)) pending.set(key, file);
            }
          }
          const payloads = await context.readFiles([...pending.values()]);
          [...pending.keys()].forEach((key, index) => files.set(key, payloads[index]!));
          bytes = await prefix.core.prepareInheritedModelRequest(prefix.scope, request, prefix.head, files, nativeLimits(this.components.limits));
        }
        const wire = this.contracts.decodeModelJson(bytes) as unknown as ModelRequestWire;
        const admittedRequest = {
          serializedInput: bytes,
          model: wire.model,
          messages: wire.messages.map((message) => ({
            ...message, content: publicModelContent(message.content),
          })),
          tools: wire.tools.map((tool) => ({
            name: tool.name, revision: tool.revision, description: tool.description,
            inputSchema: tool.input_schema as ToolJsonSchema, outputSchema: tool.output_schema as ToolJsonSchema,
            projectionSchema: tool.projection_schema as ToolJsonSchema,
          })),
          ...(wire.max_output_tokens == null ? {} : { maxOutputTokens: wire.max_output_tokens }),
          signal: context.signal,
        };
        for await (const event of model.provider.generate(admittedRequest)) {
          const admitted = this.contracts.admitModelEvent(event, this.components.limits, admission);
          admission = admitted.state;
          const admittedEvent = admitted.event;
          if (admittedEvent.kind === "content") {
            text += admittedEvent.delta;
          }
          else if (admittedEvent.kind === "tool_call") {
            calls.push(admittedEvent);
          }
          else if (admittedEvent.kind === "completed") {
            receipts.push({ kind: "model-completed", metadata: admittedEvent.metadata });
          }
        }
        if (!admission.completed) throw new Error("model attempt ended without completion");
        previousAdmission = admission;
        if (calls.length === 0) return { text };
        for (const call of calls) {
          const toolOperationId = this.contracts.idFromDigest(this.contracts.digestCanonicalJson({
            domain: "harness:tool-call:v2", task_id: context.taskId, step,
            call_id: call.callId,
          }), "operation");
          const reference = this.tool(call.name);
          const erased = this.#tools.get(componentKey(reference.definition.name, reference.definition.revision));
          if (!erased) throw new Error(`selected tool is not registered: ${call.name}`);
          const registered = (erased as unknown as RegisteredTool<unknown, unknown>);
          const { value, invocation } = await this.#invokeTool(reference, call.arguments,
            context.signal, context.taskId, toolOperationId, call.callId);
          const projection = registered.definition.projection.project(invocation, { value });
          this.contracts.validateToolProjection(registered.definition, projection, this.components.limits);
          const result = { kind: "tool_result" as const, callId: call.callId, name: call.name, content: projection };
          validateModelContent(result, this.components.limits);
          receipts.push({ kind: "tool", step, callId: call.callId, name: call.name, arguments: call.arguments, value, projection });
          messages.push({ role: "assistant", content: call }, { role: "tool", content: result });
        }
      }
      throw new Error(`agent loop exceeded ${maxSteps} model steps`);
    });
    this.running.set(id, task);
    const outcome = await task.result();
    if (outcome.kind !== "succeeded") throw new TaskRunError(task.id(), outcome);
    return { ...outcome.value, taskId: task.id(), receipts };
  }
  attach(id: TaskId): Promise<Task<unknown>>;
  attach<Input, Output>(definition: TaskDefinition<Input, Output>, id: TaskId): Promise<Task<Output>>;
  async attach<Input, Output>(value: TaskId | TaskDefinition<Input, Output>, taskId?: TaskId): Promise<Task<Output>> {
    const requested = typeof value === "string" ? undefined : (this.task(value) as unknown as TaskDefinition<never, unknown>);
    const id = typeof value === "string" ? value : taskId;
    if (id === undefined) throw new TypeError("task identity is required");
    const local = this.running.get(id);
    if (local) {
      if (requested) throw new Error("local task attachments cannot claim a durable definition");
      return (local as unknown as Task<Output>);
    }
    if (this.components.state) {
      await this.assertPolicyIdentity();
      const attachment = await this.components.state.attach(id, this);
      await this.assertPolicyIdentity();
      if (attachment.task.id() !== id) throw new Error("host attached a different task identity");
      return (await this.#validatedHostAttachment(attachment, requested) as unknown as Task<Output>);
    }
    throw new Error("task identity is not retained locally and no durable harness host is bound");
  }
  /** Read one revision-checked page of direct, owner-retained task children. */
  async children(parent: TaskId, expectedRevision: bigint | null = null,
    afterSlot: string | null = null, maximum = HARNESS_CHILD_PAGE_DEFAULT): Promise<TaskChildrenPage> {
    this.contracts.validateIdentity("task", parent);
    if (!Number.isSafeInteger(maximum) || maximum < 1
      || afterSlot !== null && ([...afterSlot].some(character => /[\x00-\x1f\x7f]/u.test(character)))) {
      throw new RangeError("task child page request is invalid");
    }
    if (expectedRevision !== null && (typeof expectedRevision !== "bigint" || expectedRevision < 0n)) {
      throw new RangeError("task child revision is invalid");
    }
    const state = this.components.state;
    if (!state?.children) throw new Error("durable task hierarchy is not bound");
    await this.assertPolicyIdentity();
    const page = await state.children(parent, expectedRevision, afterSlot, maximum, this);
    await this.assertPolicyIdentity();
    try {
      return this.contracts.validateTaskChildrenPage(parent, expectedRevision, afterSlot, maximum, page);
    } catch (error) {
      if (error instanceof TypeError) throw error;
      throw new TypeError("task child page is invalid", { cause: error });
    }
  }

  async #validatedHostAttachment(attachment: HostTaskAttachment, requested?: TaskDefinition<never, unknown>): Promise<Task<unknown>> {
    const definition = this.#tasks.get(componentKey(attachment.taskName, attachment.revision));
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
      await this.#validateObservedAdmission(attachment.admission);
    }
    return validatedHostTask(attachment.task, attachment.operationId, pinnedOutputSchema(definition), this.contracts);
  }

  #modelToolDefinitions(): readonly ModelToolDefinition[] {
    const names = new Set([...this.#tools.values()].map(tool => tool.definition.name));
    return [...names].map(name => {
      const selected = this.#selectedTools.get(name);
      if (!selected) throw new Error(`model-visible tool revision is ambiguous: ${name}`);
      const tool = this.#tools.get(componentKey(name, selected));
      if (!tool) throw new Error(`selected tool revision is not registered: ${name}@${selected}`);
      const { revision, description, inputSchema, outputSchema, projection } = tool.definition;
      return { name, revision, description, inputSchema, outputSchema, projectionSchema: projection.schema };
    });
  }
}

export class TaskRunError extends Error { constructor(readonly taskId: TaskId, readonly outcome: Exclude<Outcome<unknown>, { kind: "succeeded" }>) { super(outcome.kind === "failed" ? outcome.error.message : `task ${outcome.kind}`); } }
