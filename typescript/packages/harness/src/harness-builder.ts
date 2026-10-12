import { type ToolDefinition, type ToolExecutor, type ToolRef, type Model, type ModelProvider, type ContextBuilder, type ModelMessage } from "./model.js";
import { type WasmMachineIdentityWire, type NativeContracts, type DiscoveredContext } from "./native-contracts.js";
import { type TaskDefinition, type HarnessRuntimeState, type HarnessRuntimeSpawner, type HarnessExecutionProvider, componentKey, freezeSchema } from "./task.js";
import { type Limits, DEFAULT_LIMITS, type FileRef } from "./conversation.js";
import { bindModel, type InteractionHandler, type Policy, validatePolicyIdentity, samePolicyIdentity, ExecutionScope, type BoundModel } from "./execution-scope.js";
import { type InheritedModelPrefix, type ContentBindings, pinContentBindings, pinForkParentSnapshot, pinForkPublisher } from "./content.js";
import { type InteractionResolver } from "./interaction.js";
import { type ForkParentSnapshot, type ForkPublisher } from "./fork.js";
import { type ProjectWorkspaceProvider } from "./project.js";
import { AgentHarness } from "./agent-harness.js";
export interface RegisteredTool<Input, Output> {
  readonly source: ToolDefinition<Input, Output>;
  readonly definition: ToolDefinition<Input, Output>;
  readonly handle: ToolRef<Input, Output>;
  readonly executor?: ToolExecutor<Input, Output>;
  readonly machine?: WasmMachineIdentityWire;
}

export class HarnessBuilder {
  constructor(readonly contracts: NativeContracts) {}
  readonly #tasks = new Map<string, TaskDefinition<never, unknown>>(); readonly #tools = new Map<string, RegisteredTool<never, unknown>>();
  readonly #selectedTools = new Map<string, string>();
  readonly #components: AgentHarnessComponents = { limits: DEFAULT_LIMITS };
  readonly #grants: string[] = [];
  model(identity: Model, provider: ModelProvider): this { this.contracts.encodeCanonicalJson(identity.options); this.#components.model = bindModel(identity, provider); return this; }
  /** Set the caller's output token budget for stock model requests. */
  modelOutputTokens(maximum: number): this {
    if (!Number.isInteger(maximum) || maximum <= 0 || maximum > 0xffff_ffff) {
      throw new TypeError("model output token budget must be a positive u32");
    }
    this.#components.modelOutputTokens = maximum;
    return this;
  }
  inheritedModelPrefix(value: InheritedModelPrefix): this {
    const scope = structuredClone(value.scope);
    const head = this.contracts.validate("file_ref", structuredClone(value.head));
    value.core.verifyContentRead(scope, head);
    this.#components.inheritedModelPrefix = Object.freeze({ core: value.core, scope, head,
      files: new Map([...value.files].map(([key, bytes]) => [key, bytes.slice()])) });
    return this;
  }
  context(value: ContextBuilder): this { this.#components.context = value; return this; }
  /** Bind a validated immutable snapshot through the ordinary context builder. No reads occur. */
  declaredContext(snapshot: DiscoveredContext): this {
    const projected = this.contracts.projectDiscoveredContext(snapshot,
      { messages: [], metadata: {} }, "prepend", this.#components.limits);
    const sources: readonly ModelMessage[] = Object.freeze(projected.messages.map(message => {
      const content = message.content;
      if (typeof content === "string") return Object.freeze({ role: message.role, content });
      if (content && "kind" in content && content.kind === "file") {
        return Object.freeze({ role: message.role, content: Object.freeze({ ...content,
          file: this.contracts.validate("file_ref", content.file as FileRef) }) });
      }
      throw new TypeError("unsupported declared context message");
    }));
    const previous = this.#components.context;
    this.#components.context = { build: async (input, messages) => [
      ...sources, ...(previous ? await previous.build(input, messages) : messages),
    ] };
    return this;
  }
  interactions(value: InteractionHandler): this { this.#components.interactions = value; return this; }
  interactionResolver(value: InteractionResolver): this { this.#components.interactionResolver = value; return this; }
  policy(value: Policy): this { this.#components.policy = value; return this; }
  state(value: HarnessRuntimeState): this { this.#components.state = value; return this; }
  spawner(value: HarnessRuntimeSpawner): this { this.#components.spawner = value; return this; }
  execution(value: HarnessExecutionProvider): this { this.#components.execution = value; return this; }
  content(value: ContentBindings): this { this.#components.content = pinContentBindings(this.contracts, value); return this; }
  artifacts(value: ContentBindings): this { this.#components.artifacts = pinContentBindings(this.contracts, value); return this; }
  parentSnapshot(value: ForkParentSnapshot): this { this.#components.parentSnapshot = pinForkParentSnapshot(value); return this; }
  forkPublisher(value: ForkPublisher): this { this.#components.forkPublisher = pinForkPublisher(this.contracts, value); return this; }
  workspaces(value: ProjectWorkspaceProvider): this {
    const project = this.contracts.validate("volume_ref", value.project);
    const provider = this.contracts.validate("provider_ref", value.provider);
    if (project.class !== "project" || !this.contracts.canonicalEqual(project.provider, provider)) {
      throw new TypeError("workspace binding is not a project");
    }
    this.#components.workspaces = value;
    return this;
  }
  grant(...values: readonly string[]): this { if (values.some(value => !value.trim())) throw new TypeError("capability grant is empty"); this.#grants.push(...values); return this; }
  limits(value: Partial<Limits>): this { this.#components.limits = this.contracts.validate("limits", { ...DEFAULT_LIMITS, ...value }); return this; }
  task<Input, Output>(value: TaskDefinition<Input, Output>): this {
    const key = componentKey(value.name, value.revision);
    if (this.#tasks.has(key)) throw new Error(`conflicting registration for ${key}`);
    for (const schema of [value.options.input, value.options.output,
      value.implementation.kind === "resumable" ? value.implementation.component.state : undefined]) {
      if (schema !== undefined) this.contracts.encodeCanonicalJson(schema.document);
    }
    this.#tasks.set(key, (value as unknown as TaskDefinition<never, unknown>));
    return this;
  }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>, executor?: ToolExecutor<Input, Output>): this {
    if (!executor && !definition.handler) throw new TypeError("tool requires a handler or executor");
    return this.#registerTool(definition, executor);
  }
  /** Register a pinned durable-only tool; its owner host executes the machine. */
  resumableTool<Input, Output>(definition: ToolDefinition<Input, Output>, machine: WasmMachineIdentityWire): this {
    const pinned = this.contracts.validate("machine_identity", machine);
    if (definition.handler || pinned.name !== definition.name || pinned.version !== definition.revision) {
      throw new TypeError("resumable tool machine must match a handler-free definition");
    }
    return this.#registerTool(definition, undefined, pinned);
  }
  #registerTool<Input, Output>(definition: ToolDefinition<Input, Output>, executor?: ToolExecutor<Input, Output>, machine?: WasmMachineIdentityWire): this {
    const captured = { ...definition };
    const projection = captured.projection;
    const project = projection?.project;
    if (typeof captured.parseInput !== "function" || typeof captured.parseOutput !== "function") {
      throw new TypeError("typed tool parsers are required");
    }
    if (typeof project !== "function") throw new TypeError("typed tool projection is required");
    const pinned = Object.freeze({ ...captured,
      inputSchema: freezeSchema(structuredClone(captured.inputSchema)),
      outputSchema: freezeSchema(structuredClone(captured.outputSchema)),
      projection: Object.freeze({ schema: freezeSchema(structuredClone(projection.schema)), project: project.bind(projection) }) });
    this.contracts.encodeCanonicalJson(pinned.inputSchema);
    this.contracts.encodeCanonicalJson(pinned.outputSchema);
    this.contracts.encodeCanonicalJson(pinned.projection.schema);
    this.contracts.validateToolDefinition(pinned);
    const key = componentKey(pinned.name, pinned.revision);
    if (this.#tools.has(key)) throw new Error(`conflicting registration for ${key}`);
    const previouslyRegistered = [...this.#tools.values()].some(tool => tool.definition.name === pinned.name);
    const { name, revision, description, inputSchema, outputSchema } = pinned;
    const handle = Object.freeze({ definition: Object.freeze({ name, revision, description, inputSchema, outputSchema, projectionSchema: pinned.projection.schema }),
      ...(machine ? { machine } : {}) }) as ToolRef<Input, Output>;
    this.#tools.set(key, (Object.freeze({ source: definition, definition: pinned, handle,
      ...(executor ? { executor } : {}), ...(machine ? { machine } : {}) }) as unknown as RegisteredTool<never, unknown>));
    if (previouslyRegistered) this.#selectedTools.delete(pinned.name);
    else this.#selectedTools.set(pinned.name, pinned.revision);
    return this;
  }
  /** Choose exactly one revision for model-visible calls of a shared tool name. */
  selectModelTool(name: string, revision: string): this {
    if (!this.#tools.has(componentKey(name, revision))) throw new Error(`tool revision is not registered: ${name}@${revision}`);
    this.#selectedTools.set(name, revision);
    return this;
  }
  async build(): Promise<AgentHarness> {
    const components: AgentHarnessComponents = { ...this.#components };
    if (components.execution) {
      const state = components.execution.state();
      const spawner = components.execution.spawner();
      components.state = this.#components.state ?? state;
      components.spawner = this.#components.spawner ?? spawner;
    }
    if (components.workspaces && !this.#grants.includes("fork:publish") && !this.#grants.includes("project:merge")) {
      throw new TypeError("project workspaces require fork:publish or project:merge");
    }
    if (components.parentSnapshot && !this.#grants.includes("fork:publish")) {
      throw new TypeError("fork preparer requires fork:publish in the root scope");
    }
    if (components.forkPublisher) {
      if (!this.#grants.includes("fork:publish") || !components.parentSnapshot) {
        throw new TypeError("fork publisher requires parent preparation authority");
      }
      if (!this.contracts.canonicalEqual(components.forkPublisher.parent(), components.parentSnapshot.parent)) {
        throw new TypeError("fork publisher belongs to another parent");
      }
    }
    if ([...this.#tools.values()].some(tool => tool.machine) && !components.state?.executeTool) {
      throw new TypeError("resumable tools require owner-host durable tool execution");
    }
    // Snapshot provider identities before asynchronous validation.
    const policyInput = structuredClone((components.policy)?.identity() ?? null);
    const statePolicyInput = structuredClone(components.state?.policyIdentity() ?? null);
    const spawnerPolicyInput = structuredClone(components.spawner?.policyIdentity() ?? null);
    const executionInput = components.execution === undefined ? null : structuredClone(components.execution.identity());
    const stateExecutionInput = structuredClone(components.state?.executionIdentity?.() ?? null);
    const spawnerExecutionInput = structuredClone(components.spawner?.executionIdentity?.() ?? null);
    const durable = components.state !== undefined && components.spawner !== undefined;
    this.contracts.validateTaskRequirements({
      tasks: [...this.#tasks.values()].map(definition => ({
        name: definition.name,
        version: definition.revision,
        requirements: [...definition.options.requirements ?? []],
      })),
      tools: [...this.#tools.values()].map(tool => ({
        name: tool.definition.name,
        version: tool.definition.revision,
      })),
      components: {
        model: this.#grants.includes("model:generate"),
        context: this.#grants.includes("context:build"),
        interactions: (components.interactions !== undefined || durable) && this.#grants.includes("interaction:route"),
        policy: components.policy !== undefined,
        host: durable,
        state: components.state !== undefined,
        spawner: components.spawner !== undefined,
        content: components.content !== undefined,
        artifacts: components.artifacts !== undefined,
        content_write: components.content?.writer !== undefined
          && this.#grants.includes(components.content.writer.writeCapability()),
        artifacts_write: components.artifacts?.writer !== undefined
          && this.#grants.includes(components.artifacts.writer.writeCapability()),
      },
      grants: [...this.#grants],
      extensions: [],
    });
    const policy = await validatePolicyIdentity(policyInput);
    if (components.state && !samePolicyIdentity(policy, await validatePolicyIdentity(statePolicyInput))) throw new Error("runtime policy identity differs from durable state policy");
    if (components.spawner && !components.state) throw new Error("durable spawner requires a state binding");
    if (components.spawner && !samePolicyIdentity(policy, await validatePolicyIdentity(spawnerPolicyInput))) throw new Error("runtime policy identity differs from durable spawner policy");
    if (components.execution) {
      const selected = await validatePolicyIdentity(executionInput);
      if (selected === null || "composition" in selected
        || !samePolicyIdentity(selected, await validatePolicyIdentity(stateExecutionInput))
        || !samePolicyIdentity(selected, await validatePolicyIdentity(spawnerExecutionInput))) {
        throw new Error("execution provider, state, and spawner routes differ");
      }
    }
    return new AgentHarness(new Map(this.#tasks), new Map(this.#tools), components, new ExecutionScope().grant(...this.#grants), new Map(), new Map(this.#selectedTools), this.contracts, harnessConstruction, policy); }
}
export interface AgentHarnessComponents { modelOutputTokens?: number; inheritedModelPrefix?: InheritedModelPrefix; model?: BoundModel; context?: ContextBuilder; interactions?: InteractionHandler; interactionResolver?: InteractionResolver; policy?: Policy; state?: HarnessRuntimeState; spawner?: HarnessRuntimeSpawner; execution?: HarnessExecutionProvider; content?: ContentBindings; artifacts?: ContentBindings; parentSnapshot?: ForkParentSnapshot; forkPublisher?: ForkPublisher; workspaces?: ProjectWorkspaceProvider; limits: Limits }

export const harnessConstruction = Symbol("HarnessBuilder-owned construction");
