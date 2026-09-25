/** Ephemeral owner-controlled conversation host; the Rust reducer owns admission. */
import { Harness, type AgentId, type Command, type Event, type OperationId, type Scope } from "./index.js";
import type {
  Attachment, ConversationMessage, ConversationMessageId, ConversationState,
  FileRef, Limits, ReferencedAttachments, VolumeRef,
} from "./conversation.js";
import { DEFAULT_LIMITS } from "./conversation.js";
import { selectModelContext } from "./projection.js";
import type { AgentHarness, ContentBindings, RunOutput } from "./runtime.js";

const encoder = new TextEncoder();
const manifestType = "application/vnd.acyclic.harness.attachments+json";

/** Dispatch may have reached the model; regenerating this operation is unsafe. */
export class IndeterminateModelTurnError extends Error {
  constructor(readonly operationId: OperationId, cause?: unknown) {
    super("model dispatch outcome is indeterminate; reconcile durably or explicitly abandon the local turn", { cause });
    this.name = "IndeterminateModelTurnError";
  }
}

export interface MemoryConversationOptions {
  readonly agent: AgentId;
  readonly conversationId?: string;
  readonly issuerId?: string;
  readonly issuerKey?: Uint8Array;
  readonly wasm?: Parameters<typeof Harness.create>[0]["wasm"];
}

/** A local default with the same ref-only admission order as a durable host. */
export class MemoryConversation {
  readonly #core: Harness;
  readonly #scope: Scope;
  readonly #volume: VolumeRef<"agent_private", "memory">;
  readonly #foreign = new Map<string, Pick<ContentBindings, "read">>();
  readonly #files = new Map<string, Readonly<{ reference: FileRef; bytes: Uint8Array }>>();
  readonly #uploads = new Map<string, Promise<FileRef>>();
  readonly #outputs = new Map<string, RunOutput>();
  #turns: Promise<void> = Promise.resolve();

  private constructor(core: Harness, scope: Scope, volume: VolumeRef<"agent_private", "memory">) {
    this.#core = core;
    this.#scope = scope;
    this.#volume = core.validateVolumeRef(volume);
  }

  static async create(options: MemoryConversationOptions): Promise<MemoryConversation> {
    const authority = { kind: "conversation" as const, id: options.conversationId ?? crypto.randomUUID() };
    const core = await Harness.create({
      authority, issuerId: options.issuerId ?? "local-harness",
      issuerKey: options.issuerKey ?? crypto.getRandomValues(new Uint8Array(32)),
      ...(options.wasm === undefined ? {} : { wasm: options.wasm }),
    });
    const volume: VolumeRef<"agent_private", "memory"> = {
      provider: { namespace: "local", family: "memory", version: "2" },
      id: "private", class: "agent_private", owner: { kind: "agent", id: options.agent },
    };
    const scope = core.issueScopeForAgent(options.agent, "conversation-owner", [
      "conversation:bind", "conversation:append", "conversation:select_context",
      core.volumeCapability(volume, "read"), core.volumeCapability(volume, "write"),
    ]);
    const host = new MemoryConversation(core, scope, volume);
    host.#apply(core.identity("operation", crypto.randomUUID()), "bind", { kind: "bind_conversation", agent: options.agent });
    return host;
  }

  get volume(): VolumeRef<"agent_private", "memory"> { return this.#volume; }
  get scope(): Scope { return structuredClone(this.#scope); }
  /** Attach one foreign owner volume lazily; its reader remains responsible for grants. */
  attachReadVolume(volume: VolumeRef, reader: Pick<ContentBindings, "read">): this {
    const key = this.#volumeKey(this.#core.validateVolumeRef(volume));
    if (key === this.#volumeKey(this.#volume) || this.#foreign.has(key)) {
      throw new TypeError("foreign content volume is already bound");
    }
    this.#foreign.set(key, Object.freeze({ read: reader.read.bind(reader) }));
    return this;
  }
  /** Owner issues one exact-file grant to an unrelated attached reader. */
  delegateFileRead(reader: AgentId, id: string, file: FileRef): Scope {
    const reference = this.#validatedFile(file);
    if (this.#volumeKey(reference.volume) !== this.#volumeKey(this.#volume) || this.#localBytes(reference) === undefined) {
      throw new TypeError("owner cannot delegate a nonresident private file");
    }
    return this.#core.delegatePrivateFileRead(this.#scope, reader, id, reference);
  }
  /** Owner issues read-only lazy discovery of a private subtree or its root. */
  delegateDirectoryRead(reader: AgentId, id: string, prefix: string): Scope {
    return this.#core.delegatePrivateDirectoryRead(this.#scope, reader, id, this.#volume, prefix);
  }
  /** Owning provider authenticates a delegated read before returning bytes. */
  async readAuthorized(scope: Scope, reference: FileRef): Promise<Uint8Array> {
    const file = this.#validatedFile(reference);
    if (this.#volumeKey(file.volume) !== this.#volumeKey(this.#volume)) throw new TypeError("file belongs to another owner");
    this.#core.verifyContentRead(scope, file);
    const bytes = this.#localBytes(file);
    if (bytes === undefined) throw new TypeError("owner has no resident file");
    this.#core.verifyFileBytes(file, bytes);
    return Uint8Array.from(bytes);
  }
  /** Mount this owner's volume read-only under an exact owner-issued scope. */
  readBindings(scope: Scope): ContentBindings {
    const admittedScope = structuredClone(scope);
    this.#core.verifyScope(admittedScope);
    return {
      validate: (file, limits) => this.#core.validateFileUnderLimits(file, limits),
      verify: (file, bytes) => this.#core.verifyFileBytes(file, bytes),
      read: file => this.readAuthorized(admittedScope, file),
      fileReadCapability: file => this.#core.fileReadCapability(file),
      volumeReadCapability: volume => this.#core.volumeCapability(volume, "read"),
      directoryReadCapability: (volume, prefix) => this.#core.directoryReadCapability(volume, prefix),
    };
  }
  /** Bind this owner's authenticated content provider to typed task/tool contexts. */
  contentBindings(): ContentBindings {
    return {
      validate: (file, limits) => {
        this.#core.validateFileUnderLimits(file, limits);
      },
      verify: (file, bytes) => this.#core.verifyFileBytes(file, bytes),
      read: file => this.read(file),
      fileReadCapability: file => this.#core.fileReadCapability(file),
      volumeReadCapability: volume => this.#core.volumeCapability(volume, "read"),
      directoryReadCapability: (volume, prefix) => this.#core.directoryReadCapability(volume, prefix),
      writer: {
        volume: this.#volume,
        writeCapability: () => this.#core.volumeCapability(this.#volume, "write"),
        stage: async (operationId, path, bytes, mediaType, displayName) => {
          let admitted = this.#uploads.get(operationId);
          if (admitted === undefined) {
            admitted = this.stage(path, bytes, mediaType, displayName);
            this.#uploads.set(operationId, admitted);
          }
          let file: FileRef;
          try { file = await admitted; }
          catch (error) {
            if (this.#uploads.get(operationId) === admitted) this.#uploads.delete(operationId);
            throw error;
          }
          const candidate = this.#core.fileDescriptor(bytes, mediaType);
          if (file.path !== path || file.display_name !== displayName
            || file.descriptor.media_type !== mediaType
            || file.descriptor.byte_length !== candidate.byte_length
            || file.descriptor.sha256.some((byte, index) => byte !== candidate.sha256[index])) {
            throw new TypeError("upload operation identity belongs to another file");
          }
          return file;
        },
      },
    };
  }
  snapshot(): ReturnType<Harness["snapshot"]> { return this.#core.snapshot(); }
  free(): void { this.#core.free(); }

  /** Store immutable bytes before any record can refer to them. */
  async stage(path: string, bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    if (path === ".system" || path.startsWith(".system/")) {
      throw new TypeError("internal storage paths are reserved");
    }
    const descriptor = this.#core.fileDescriptor(bytes, mediaType);
    const version = [...this.#core.canonicalJsonDigest([path, descriptor.sha256,
      descriptor.byte_length, descriptor.media_type, displayName])]
      .map(byte => byte.toString(16).padStart(2, "0")).join("");
    const reference = this.#validatedFile({ volume: this.#volume, path, version, descriptor, display_name: displayName });
    const key = this.#fileStorageKey(reference);
    const prior = this.#files.get(key);
    if (prior !== undefined && (this.#fileKey(prior.reference) !== this.#fileKey(reference)
      || prior.bytes.byteLength !== bytes.byteLength || prior.bytes.some((byte, i) => byte !== bytes[i]))) {
      throw new TypeError("immutable file version was reused for another file contract");
    }
    this.#files.set(key, { reference, bytes: Uint8Array.from(bytes) });
    return reference;
  }

  /** Possession of a ref is not a foreign-owner read grant. */
  async read(reference: FileRef): Promise<Uint8Array> {
    const file = this.#validatedFile(reference);
    const bytes = this.#volumeKey(file.volume) === this.#volumeKey(this.#volume)
      ? this.#localBytes(file)
      : await this.#foreign.get(this.#volumeKey(file.volume))?.read(file);
    if (bytes === undefined) throw new TypeError("content has no authorized owning-provider resolver");
    if (!(bytes instanceof Uint8Array)) throw new TypeError("content resolver returned invalid bytes");
    this.#core.verifyFileBytes(file, bytes);
    return Uint8Array.from(bytes);
  }

  #localBytes(file: FileRef): Uint8Array | undefined {
    const resident = this.#files.get(this.#fileStorageKey(file));
    return resident !== undefined && this.#fileKey(resident.reference) === this.#fileKey(file) ? resident.bytes : undefined;
  }

  /** Projection rebuilt from canonical reducer events, not a parallel mutable transcript. */
  conversation(): ConversationState {
    return this.#core.conversation();
  }

  runConversation(
    runtime: AgentHarness, operationId: OperationId, content: FileRef,
    attachments: readonly Attachment[] = [],
  ): Promise<RunOutput> {
    const run = this.#turns.then(() => this.#runConversation(runtime, operationId, content, attachments));
    this.#turns = run.then(() => {}, () => {});
    return run;
  }

  /** Explicit owner decision to close an unknown local attempt without re-dispatch. */
  abandonIndeterminateTurn(operationId: OperationId): Promise<void> {
    const run = this.#turns.then(() => this.#abandonIndeterminateTurn(operationId));
    this.#turns = run.then(() => {}, () => {});
    return run;
  }

  async #abandonIndeterminateTurn(operationId: OperationId): Promise<void> {
    const userId = this.#core.conversationMessageId(this.#core.deriveOperationId(operationId, "user"));
    const noticeId = this.#core.conversationMessageId(this.#core.deriveOperationId(operationId, "indeterminate-notice"));
    const state = this.conversation();
    const existing = state.messages.find(message => message.id === noticeId);
    if (existing !== undefined) return;
    if (!state.messages.some(message => message.id === userId && message.kind === "user")
      || state.messages.some(message => message.kind === "assistant" && message.reply_to === userId)
      || this.#outputs.has(operationId)
      || !this.#events().some(event => event.operation_id === operationId
        && event.payload.kind === "model_context_selected")) {
      throw new TypeError("only an unresolved dispatched local turn can be abandoned");
    }
    const content = await this.stage(`turns/${operationId}/indeterminate.txt`, encoder.encode(
      "The preceding model attempt has an unknown outcome and was explicitly abandoned by its owner. Do not infer an assistant answer."),
    "text/plain", "indeterminate.txt");
    const outcome = await this.stage(`turns/${operationId}/indeterminate.json`, this.#core.canonicalJsonBytes({
      state: "indeterminate", operation_id: operationId,
    }), "application/json", "indeterminate.json");
    this.#core.validateFileUnderLimits(content, DEFAULT_LIMITS);
    this.#core.validateFileUnderLimits(outcome, DEFAULT_LIMITS);
    this.#append(this.#core.deriveOperationId(operationId, "indeterminate-event"), "indeterminate", {
      id: noticeId, sequence: BigInt(state.messages.length + 1), kind: "system", content,
      attachments: { kind: "inline", items: [] }, reply_to: userId, tool_call_id: null,
      extensions: { "acyclic.turn.outcome": outcome },
    }, DEFAULT_LIMITS);
  }

  async #runConversation(
    runtime: AgentHarness, operationId: OperationId, content: FileRef,
    attachments: readonly Attachment[],
  ): Promise<RunOutput> {
    const limits = runtime.limits;
    const userContent = this.#validatedFile(content);
    this.#core.validateFileUnderLimits(userContent, limits);
    await this.read(userContent);
    const list = await this.#attachments(operationId, "user", attachments, limits);
    const userId = this.#core.conversationMessageId(this.#core.deriveOperationId(operationId, "user"));
    let state = this.conversation();
    const unresolved = [...state.messages].reverse().find(message => message.kind === "user");
    if (unresolved !== undefined && unresolved.id !== userId
      && !state.messages.some(message => (message.kind === "assistant"
        || (message.kind === "system" && Object.hasOwn(message.extensions, "acyclic.turn.outcome")))
        && message.reply_to === unresolved.id)) {
      throw new TypeError("previous conversation turn is unresolved; retry that operation first");
    }
    if (state.messages.some(message => message.kind === "system" && message.reply_to === userId
      && Object.hasOwn(message.extensions, "acyclic.turn.outcome"))) {
      throw new TypeError("conversation turn was explicitly abandoned after an indeterminate model outcome");
    }
    const existing = state.messages.find(message => message.id === userId);
    if (existing === undefined) {
      this.#append(this.#core.deriveOperationId(operationId, "user-event"), "user", {
        id: userId, sequence: BigInt(state.messages.length + 1), kind: "user", content: userContent,
        attachments: list, reply_to: null, tool_call_id: null, extensions: {},
      }, limits);
    } else if (existing.kind !== "user" || this.#fileKey(existing.content) !== this.#fileKey(userContent)
      || !this.#sameAttachments(existing.attachments, list)) {
      throw new TypeError("turn identity is bound to another user message");
    }
    const completed = this.#outputs.get(operationId);
    const assistantId = this.#core.conversationMessageId(this.#core.deriveOperationId(operationId, "assistant"));
    if (completed !== undefined && state.messages.some(message => message.id === assistantId)) {
      return structuredClone(completed);
    }
    state = this.conversation();
    let selection: { readonly conversation_revision: bigint; readonly message_ids: readonly ConversationMessageId[] } | undefined;
    for (const event of this.#events()) {
      if (event.operation_id === operationId && event.payload.kind === "model_context_selected") {
        selection = event.payload.selection;
        break;
      }
    }
    const newSelection = selection === undefined;
    if (selection === undefined) {
      const current = state.messages.findIndex(message => message.id === userId);
      const eligible = state.messages.slice(0, current + 1)
        .filter(message => ["system", "user", "assistant", "tool_call", "tool_result"].includes(message.kind));
      const suffix = eligible.slice(-limits.context_messages);
      const included = new Set(suffix.map(message => message.id));
      selection = { conversation_revision: BigInt(state.messages.length),
        message_ids: suffix.filter(message => message.kind !== "tool_result"
          || (message.reply_to !== null && included.has(message.reply_to))).map(message => message.id) };
    }
    if (selection.message_ids.at(-1) !== userId) throw new TypeError("operation is bound to another context selection");
    if (!newSelection && completed === undefined) {
      throw new IndeterminateModelTurnError(operationId);
    }
    let stableOutput = completed;
    if (stableOutput === undefined) {
      const selectedLength = Number(selection.conversation_revision);
      if (!Number.isSafeInteger(selectedLength) || selectedLength < 0
        || selectedLength > state.messages.length) {
        throw new TypeError("local conversation selection has an unavailable historical prefix");
      }
      const historical = { agent: state.agent, revision: selection.conversation_revision,
        messages: state.messages.slice(0, selectedLength) };
      const selected = await selectModelContext(historical, {
        conversationRevision: selection.conversation_revision, messageIds: selection.message_ids,
      }, { resolveFile: file => this.read(file), maxMessages: limits.context_messages,
        maxAttachments: limits.attachments, maxManifestBytes: limits.file_bytes,
        maxRenderBytes: limits.render_bytes,
        decodeManifest: (manifest, bytes, count) => this.#core.decodeAttachmentManifest(manifest, bytes, count),
        validateMessage: message => this.#core.validateConversationMessage(message, limits) });
      if (newSelection) this.#apply(operationId, "context", { kind: "select_model_context", selection });
      let output: RunOutput;
      try { output = await runtime.runSelectedContext(selected); }
      catch (error) { throw new IndeterminateModelTurnError(operationId, error); }
      if (typeof output.text !== "string") throw new TypeError("assistant output text is invalid");
      stableOutput = structuredClone(output);
      this.#outputs.set(operationId, stableOutput);
    }
    const assistant = await this.stage(`turns/${operationId}/assistant.txt`, encoder.encode(stableOutput.text), "text/plain", "assistant.txt");
    this.#core.validateFileUnderLimits(assistant, limits);
    const assistantAttachments = await this.#attachments(operationId, "assistant", stableOutput.attachments ?? [], limits);
    const finalMetadata = [...stableOutput.receipts].reverse().find(receipt => receipt.kind === "model-completed");
    const metadata = await this.stage(`turns/${operationId}/metadata.json`,
      this.#core.canonicalJsonBytes(finalMetadata?.metadata ?? {}), "application/json", "metadata.json");
    this.#core.validateFileUnderLimits(metadata, limits);
    await this.#publishToolHistory(operationId, userId, stableOutput, limits);
    state = this.conversation();
    const priorAssistant = state.messages.find(message => message.id === assistantId);
    const message: ConversationMessage = { id: assistantId, sequence: BigInt(state.messages.length + 1),
      kind: "assistant", content: assistant, attachments: assistantAttachments,
      reply_to: userId, tool_call_id: null, extensions: { "acyclic.model.metadata": metadata } };
    if (priorAssistant === undefined) this.#append(this.#core.deriveOperationId(operationId, "assistant-event"), "assistant", message, limits);
    else if (priorAssistant.kind !== "assistant" || priorAssistant.reply_to !== userId
      || this.#fileKey(priorAssistant.content) !== this.#fileKey(message.content)
      || !this.#sameAttachments(priorAssistant.attachments, message.attachments)) {
      throw new TypeError("turn identity is bound to another assistant message");
    }
    return structuredClone(stableOutput);
  }

  async runPrompt(runtime: AgentHarness, prompt: string): Promise<RunOutput> {
    const operation = this.#core.identity("operation", crypto.randomUUID());
    const content = await this.stage(`turns/${operation}/user.txt`, encoder.encode(prompt), "text/plain", "prompt.txt");
    return this.runConversation(runtime, operation, content);
  }

  async #attachments(operation: OperationId, role: string, items: readonly Attachment[], limits: Limits): Promise<ReferencedAttachments> {
    if (items.length > limits.attachments) throw new TypeError("attachments exceed harness limits");
    const checked = items.map(item => ({ file: this.#validatedFile(item.file), label: item.label }));
    for (const item of checked) { this.#core.validateFileUnderLimits(item.file, limits); await this.read(item.file); }
    if (checked.length <= 128) return { kind: "inline", items: checked };
    const manifest = await this.stage(`turns/${operation}/${role}-attachments.json`,
      this.#core.encodeAttachmentManifest(checked),
      manifestType, "attachments.json");
    this.#core.validateFileUnderLimits(manifest, limits);
    this.#core.decodeAttachmentManifest(manifest, await this.read(manifest), checked.length);
    return { kind: "manifest", manifest, item_count: checked.length };
  }

  async #publishToolHistory(operation: OperationId, userId: ConversationMessageId, output: RunOutput, limits: Limits): Promise<void> {
    const seen = new Set<string>();
    for (const [index, receipt] of output.receipts.entries()) {
      if (receipt.kind !== "tool") continue;
      const identity = `${receipt.step}:${receipt.callId}`;
      if (seen.has(identity)) throw new TypeError("tool call identity is duplicated within a model step");
      seen.add(identity);
      const callId = this.#core.conversationMessageId(this.#core.deriveOperationId(operation, `tool-call:${index}`));
      const resultId = this.#core.conversationMessageId(this.#core.deriveOperationId(operation, `tool-result:${index}`));
      const call = await this.stage(`turns/${operation}/tools/${index}-call.json`,
        this.#core.canonicalJsonBytes({ call_id: receipt.callId, name: receipt.name, arguments: receipt.arguments }),
        "application/json", "call.json");
      const result = await this.stage(`turns/${operation}/tools/${index}-result.json`,
        this.#core.canonicalJsonBytes({ value: receipt.value }), "application/json", "result.json");
      const projection = await this.stage(`turns/${operation}/tools/${index}-projection.json`,
        this.#core.canonicalJsonBytes(receipt.projection), "application/json", "projection.json");
      for (const file of [call, result, projection]) this.#core.validateFileUnderLimits(file, limits);
      let state = this.conversation();
      this.#append(this.#core.deriveOperationId(operation, `tool-call-event:${index}`), "tool-call", {
        id: callId, sequence: BigInt(state.messages.length + 1), kind: "tool_call", content: call,
        attachments: { kind: "inline", items: [] }, reply_to: userId,
        tool_call_id: receipt.callId, extensions: {},
      }, limits);
      state = this.conversation();
      this.#append(this.#core.deriveOperationId(operation, `tool-result-event:${index}`), "tool-result", {
        id: resultId, sequence: BigInt(state.messages.length + 1), kind: "tool_result", content: result,
        attachments: { kind: "inline", items: [{ file: projection, label: "model_projection" }] },
        reply_to: callId, tool_call_id: receipt.callId, extensions: {},
      }, limits);
    }
  }

  #append(operation: OperationId, label: string, message: ConversationMessage, limits: Limits): void {
    const admitted = this.#core.validateConversationMessage(message, limits);
    this.#apply(operation, label, { kind: "append_conversation_message", message: admitted });
  }

  #validatedFile(reference: FileRef): FileRef {
    return this.#core.validateFileRef(reference);
  }

  #apply<Action extends { readonly kind: string }>(operation: OperationId, label: string, action: Action): void {
    const snapshot = this.#core.snapshot();
    const command: Command<Action> = { authority: snapshot.authority,
      operation_id: operation, idempotency_key: `conversation:${operation}:${label}`,
      expected_revision: snapshot.revision, scope: this.#scope, causal_parent: null, action };
    this.#core.apply(command);
  }

  #volumeKey(volume: VolumeRef): string {
    return this.#core.volumeStorageName(volume);
  }

  #fileKey(file: FileRef): string {
    return this.#core.fileReadCapability(file);
  }

  #fileStorageKey(file: FileRef): string {
    return JSON.stringify([this.#volumeKey(file.volume), file.path, file.version]);
  }

  #sameAttachments(left: ReferencedAttachments, right: ReferencedAttachments): boolean {
    const encodedLeft = this.#core.canonicalJsonBytes(left);
    const encodedRight = this.#core.canonicalJsonBytes(right);
    return encodedLeft.byteLength === encodedRight.byteLength
      && encodedLeft.every((byte, index) => byte === encodedRight[index]);
  }

  #events(): readonly CanonicalConversationEvent[] {
    return this.#core.snapshot().events as CanonicalConversationEvent[];
  }
}

type CanonicalConversationEvent = Pick<Event, "revision"> & { readonly operation_id: string; readonly payload:
  | { readonly kind: "conversation_bound"; readonly agent: AgentId }
  | { readonly kind: "conversation_message_appended"; readonly message: ConversationMessage }
  | { readonly kind: "model_context_selected"; readonly selection: { readonly conversation_revision: bigint; readonly message_ids: readonly ConversationMessageId[] } } };
