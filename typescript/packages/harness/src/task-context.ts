import { abortableWait } from "./client-requests.js";
import { type AgentHarness } from "./agent-harness.js";
import { type TaskId, type OperationId, type EffectId } from "./index.js";
import { type ExecutionScope, type Interaction, type Answer, type TypedInteractionInput } from "./execution-scope.js";
import { type TaskDefinition, type TaskRef, publicTaskHandle, componentKey, type Outcome, type Task, registeredTaskDefinition, type Admission, type GroupId, type RuntimeSchema, type EffectStatus, type TaskMessage } from "./task.js";
import { type ToolDefinition, type ToolRef } from "./model.js";
import { type FileRef, type VolumeRef } from "./conversation.js";
import { type ResourceRef } from "./fork.js";
import { HARNESS_PRIVATE_DIRECTORY_PAGE_DEFAULT, HARNESS_PRIVATE_DIRECTORY_PAGE_MAXIMUM } from "./private-directory-page-contract.js";
import { type PrivateDirectoryPage, type ContentBindings } from "./content.js";
import { type GroupPolicy } from "./enums.js";
import { TaskGroup } from "./task-group.js";

function compareUtf8(left: string, right: string): number {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  for (let index = 0; index < Math.min(a.length, b.length); index++) {
    if (a[index] !== b[index]) return a[index]! - b[index]!;
  }
  return a.length - b.length;
}

export class TaskContext {
  readonly #harness: AgentHarness;
  constructor(harness: AgentHarness, readonly signal: AbortSignal, readonly taskId?: TaskId, readonly durable = harness.components.state !== undefined) {
    this.#harness = harness;
  }
  /** Narrows descendant authority and optionally selects new live-task bindings. */
  async scoped(scope: ExecutionScope): Promise<TaskContext> {
    if (this.durable && (scope.executionProvider || scope.modelBinding || scope.contextBuilder)) {
      throw new Error("durable provider transition requires a recorded effect");
    }
    return new TaskContext(await this.#harness.scoped(scope), this.signal, this.taskId, this.durable);
  }
  task<Input, Output>(definition: TaskDefinition<Input, Output>): TaskRef<Input, Output>;
  task(name: string): TaskRef<unknown, unknown>;
  task<Input, Output>(value: string | TaskDefinition<Input, Output>): TaskRef<Input, Output> {
    const definition = this.#harness.task(value as TaskDefinition<Input, Output>) as unknown as TaskDefinition<never, unknown>;
    this.#assertTask(definition);
    return publicTaskHandle(definition) as unknown as TaskRef<Input, Output>;
  }
  tool<Input, Output>(definition: ToolDefinition<Input, Output>): ToolRef<Input, Output>;
  tool(name: string): ToolRef<unknown, unknown>;
  tool<Input, Output>(value: string | ToolDefinition<Input, Output>): ToolRef<Input, Output> {
    const tool = this.#harness.tool(value as ToolDefinition<Input, Output>);
    const capability = `tool:call:${tool.definition.name}`;
    if (!this.#harness.scope.grants.includes(capability)) throw new Error(`task scope lacks ${capability}`);
    return (tool as unknown as ToolRef<Input, Output>);
  }
  #assertTask(definition: TaskDefinition<never, unknown>): void {
    this.#harness.task(definition);
    const capability = `task:spawn:${componentKey(definition.name, definition.revision)}`;
    if (!this.#harness.scope.grants.includes(capability)) throw new Error(`task scope lacks ${capability}`);
  }
  /** Resolve exact bytes through the owner's authenticated provider. A ref is not a grant. */
  async readFile(reference: FileRef): Promise<Uint8Array> {
    return this.#readBoundFile(this.#harness.components.content, reference);
  }
  /** Resolve immutable artifact bytes through an independently bound owner. */
  async readArtifact(reference: FileRef): Promise<Uint8Array> {
    return this.#readBoundFile(this.#harness.components.artifacts, reference);
  }
  /** Discover an attached owner's private tree lazily under a signed read boundary. */
  async listPrivateDirectory(volume: VolumeRef<"agent_private">, grantedPrefix: string,
    path: string, expectedGeneration: ResourceRef<"generation"> | null = null, after: string | null = null,
    maximumEntries = HARNESS_PRIVATE_DIRECTORY_PAGE_DEFAULT): Promise<PrivateDirectoryPage> {
    this.signal.throwIfAborted();
    const content = this.#harness.components.content;
    if (!content?.directory) throw new Error("private directory provider is not bound");
    const checked = this.#harness.contracts.validate("volume_ref", volume);
    if (checked.class !== "agent_private") throw new TypeError("directory discovery requires an agent-private volume");
    if (!this.#harness.scope.grants.includes(content.directoryReadCapability(checked, grantedPrefix))
      && !this.#harness.scope.grants.includes(content.volumeReadCapability(checked))) {
      throw new Error("task scope cannot discover this directory");
    }
    if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > HARNESS_PRIVATE_DIRECTORY_PAGE_MAXIMUM) {
      throw new TypeError("private directory page limit is invalid");
    }
    const page = this.#harness.contracts.validate("private_directory_page",
      await content.directory.list(checked, grantedPrefix, path, expectedGeneration, after, maximumEntries));
    if (page.entries.length > maximumEntries
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
    const content = this.#harness.components.content;
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
    content.validate(file, this.#harness.components.limits);
    this.#harness.contracts.verifyFileBytes(file, bytes);
    return { file, bytes };
  }
  #admitBoundFile(content: ContentBindings | undefined, reference: FileRef): FileRef {
    this.signal.throwIfAborted();
    if (!content) throw new Error("content reader is not bound");
    const file = this.#harness.contracts.validate("file_ref", reference);
    content.validate(file, this.#harness.components.limits);
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
    return file;
  }
  /** Check every selected file before the first owner-mediated read. */
  async readFiles(references: readonly FileRef[]): Promise<readonly Uint8Array[]> {
    const files = references.map(file => this.#admitBoundFile(this.#harness.components.content, file));
    const bytes: Uint8Array[] = [];
    for (const file of files) bytes.push(await this.#readBoundFile(this.#harness.components.content, file));
    return bytes;
  }
  async #readBoundFile(content: ContentBindings | undefined, reference: FileRef): Promise<Uint8Array> {
    const file = this.#admitBoundFile(content, reference);
    if (!content) throw new Error("content reader is not bound");
    const bytes = await content.read(file);
    if (!(bytes instanceof Uint8Array)) throw new TypeError("content reader returned invalid bytes");
    this.#harness.contracts.verifyFileBytes(file, bytes);
    return Uint8Array.from(bytes);
  }
  /** Stage immutable bytes under a stable operation ID before publishing their ref. */
  async stageFile(operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    return this.#stageBoundFile(this.#harness.components.content, operationId, path, bytes, mediaType, displayName);
  }
  /** Stage an immutable artifact independently of the conversation file store. */
  async stageArtifact(operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    return this.#stageBoundFile(this.#harness.components.artifacts, operationId, path, bytes, mediaType, displayName);
  }
  async #stageBoundFile(content: ContentBindings | undefined, operationId: string, path: string,
    bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    this.signal.throwIfAborted();
    if (!operationId.trim() || !(bytes instanceof Uint8Array)) throw new TypeError("stable upload identity and bytes are required");
    const payload = Uint8Array.from(bytes);
    if (payload.byteLength > this.#harness.components.limits.file_bytes || new TextEncoder().encode(path).byteLength > this.#harness.components.limits.path_bytes) {
      throw new TypeError("file exceeds task content limits");
    }
    if (!content?.writer) throw new Error("task has no owner-bound content writer");
    if (!this.#harness.scope.grants.includes(content.writer.writeCapability())) throw new Error("task scope cannot write this volume");
    const file = this.#harness.contracts.validate("file_ref",
      await content.writer.stage(operationId, path, payload, mediaType, displayName));
    content.validate(file, this.#harness.components.limits);
    if (!this.#harness.contracts.canonicalEqual(file.volume, content.writer.volume)
      || file.path !== path || file.display_name !== displayName || file.descriptor.media_type !== mediaType) {
      throw new TypeError("content publisher returned another file identity");
    }
    this.#harness.contracts.verifyFileBytes(file, payload);
    const resident = await content.read(file);
    if (!(resident instanceof Uint8Array)) throw new TypeError("content reader returned invalid bytes");
    this.#harness.contracts.verifyFileBytes(file, resident);
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
    this.#assertTask((definition as unknown as TaskDefinition<never, unknown>));
    return this.#harness.spawn(definition, input);
  }
  admit<Input, Output>(task: TaskRef<Input, Output>, input: Input, operationId: string): Promise<Admission<Output>> {
    if (!this.durable || !this.taskId) throw new Error("a live task cannot claim durable descendant ownership");
    const definition = registeredTaskDefinition(task);
    this.#assertTask((definition as unknown as TaskDefinition<never, unknown>));
    return this.#harness.admit(definition, input, operationId, this.taskId);
  }
  group<Output>(policy: GroupPolicy, id?: GroupId): TaskGroup<Output, "scoped"> {
    if (this.durable && (!this.taskId || !id)) throw new Error("durable task groups require stable group and parent identities");
    return new TaskGroup<Output, "scoped">(this.#harness, policy, id, this.durable ? this.taskId : undefined,
      definition => this.#assertTask((definition as unknown as TaskDefinition<never, unknown>)));
  }
  async interact(interaction: Interaction): Promise<Answer> {
    if (this.durable) {
      const state = this.#harness.components.state;
      if (!state || !this.taskId) throw new Error("durable interaction requires state and task identity");
      const id = this.#harness.contracts.validateIdentity("interaction", interaction.id);
      if (interaction.kind === "approval") {
        if (!interaction.operationId || !interaction.actionDigest) throw new Error("durable approval requires an exact action binding");
        this.#harness.contracts.validate("approval_binding", { operation_id: interaction.operationId, action_digest: interaction.actionDigest });
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
  async reconcileEffect(effectId: EffectId): Promise<EffectStatus> {
    if (!this.taskId) throw new Error("effect reconciliation requires a task identity");
    const host = this.#harness.components.state;
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
  send(recipient: TaskId, value: FileRef, messageId?: OperationId): Promise<{ readonly accepted: boolean; readonly messageId: OperationId }> {
    const host = this.#harness.components.state;
    if (!host) throw new Error("no durable task state is bound");
    if (this.durable && !messageId) throw new Error("durable messages require a stable message ID");
    return host.send({ id: messageId ?? (crypto.randomUUID() as OperationId), recipient,
      value: this.#harness.contracts.validate("file_ref", value),
      ...(this.taskId === undefined ? {} : { sender: this.taskId }) });
  }
  inbox(from?: OperationId): AsyncIterable<TaskMessage> {
    if (!this.taskId) throw new Error("task inbox requires a durable task identity");
    const host = this.#harness.components.state;
    if (!host) throw new Error("no durable task state is bound");
    return host.inbox(this.taskId, from);
  }
  async sleepUntil(deadline: Date, operationId?: string): Promise<void> {
    if (!Number.isFinite(deadline.getTime())) throw new TypeError("timer deadline is invalid");
    if (this.durable) {
      const state = this.#harness.components.state;
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

export class ToolContext extends TaskContext { constructor(harness: AgentHarness, signal: AbortSignal, readonly callId: string, taskId?: TaskId, durable = harness.components.state !== undefined, readonly operationId?: string) { super(harness, signal, taskId, durable); } }
