/** Ephemeral owner-controlled conversation host; the Rust reducer owns admission. */
import { Harness, type AgentId, type Command, type OperationId, type Scope } from "./index.js";
import type { ConversationState, FileRef, Limits, VolumeRef } from "./conversation.js";
import { DEFAULT_LIMITS } from "./conversation.js";
import { memoryContentStore } from "./memory-content-store.js";
import type { ResourceRef } from "./fork.js";
import type { ContentBindings, ContentReader, PrivateDirectoryPage } from "./content.js";
import { HARNESS_PRIVATE_DIRECTORY_PAGE_DEFAULT, HARNESS_PRIVATE_DIRECTORY_PAGE_MAXIMUM } from "./private-directory-page-contract.js";

export interface MemoryConversationOptions {
  readonly agent: AgentId;
  /** Maximum staged file/path bounds; runtime admission may narrow them. */
  readonly limits?: Limits;
  /** Hard cap on immutable bytes retained by this ephemeral host. */
  readonly maxResidentBytes?: number;
  /** Hard cap on retained file versions, including zero-byte files. */
  readonly maxResidentFiles?: number;
  readonly conversationId?: string;
  readonly issuerId?: string;
  readonly issuerKey?: Uint8Array;
}

/** A local default with the same ref-only admission order as a durable host. */
export class MemoryConversation {
  readonly #core: Harness;
  readonly #scope: Scope;
  readonly #volume: VolumeRef<"agent_private", "memory">;
  readonly #maxResidentFiles: number;
  readonly #content: ReturnType<typeof memoryContentStore>;
  readonly #foreign = new Map<string, ContentReader>();
  #foreignMount: ((volume: VolumeRef) => Readonly<{ owner: MemoryConversation; scope: Scope }>) | undefined;
  readonly #uploads = new Map<string, Promise<FileRef>>();

  private constructor(core: Harness, scope: Scope, volume: VolumeRef<"agent_private", "memory">,
    maxResidentFiles: number, content: ReturnType<typeof memoryContentStore>) {
    this.#core = core;
    this.#scope = scope;
    this.#volume = core.contracts.validate("volume_ref", volume);
    this.#maxResidentFiles = maxResidentFiles;
    this.#content = content;
  }

  static async create(options: MemoryConversationOptions): Promise<MemoryConversation> {
    const authority = { kind: "conversation" as const, id: options.conversationId ?? crypto.randomUUID() };
    const core = await Harness.create({
      authority, issuerId: options.issuerId ?? "local-harness",
      issuerKey: options.issuerKey ?? crypto.getRandomValues(new Uint8Array(32)),
    });
    const maxResidentBytes = options.maxResidentBytes ?? Number.MAX_SAFE_INTEGER;
    const maxResidentFiles = options.maxResidentFiles ?? Number.MAX_SAFE_INTEGER;
    try {
      const limits = core.contracts.validate("limits", options.limits ?? DEFAULT_LIMITS);
      if (!Number.isSafeInteger(maxResidentBytes) || maxResidentBytes < 0
        || !Number.isSafeInteger(maxResidentFiles) || maxResidentFiles < 1) {
        throw new RangeError("memory content retention limits are invalid");
      }
      const volume: VolumeRef<"agent_private", "memory"> = {
        provider: { namespace: "local", family: "memory", version: "2" },
        id: crypto.randomUUID(), class: "agent_private", owner: { kind: "agent", id: options.agent },
      };
      const scope = core.issueScope("conversation-owner", [
        "conversation:bind", "conversation:append", "conversation:select_context",
        core.volumeCapability(volume, "read"), core.volumeCapability(volume, "write"),
      ], options.agent);
      const content = memoryContentStore(volume, limits.file_bytes, limits.path_bytes,
        maxResidentBytes, maxResidentFiles);
      const host = new MemoryConversation(core, scope, volume, maxResidentFiles, content);
      host.#apply(core.contracts.validateIdentity("operation", crypto.randomUUID()), "bind", { kind: "bind_conversation", agent: options.agent });
      return host;
    } catch (error) {
      core.free();
      throw error;
    }
  }

  get volume(): VolumeRef<"agent_private", "memory"> { return this.#volume; }
  get scope(): Scope { return structuredClone(this.#scope); }
  /** Attach one owner-issued reader; a bare callback or FileRef is not authority. */
  attachReadVolume(owner: MemoryConversation, scope: Scope): this {
    const key = this.#core.volumeStorageName(owner.volume);
    if (key === this.#core.volumeStorageName(this.#volume) || this.#foreign.has(key)) {
      throw new TypeError("foreign content volume is already bound");
    }
    if (scope.agent !== this.#volume.owner.id) throw new TypeError("foreign read scope belongs to another agent");
    const reader = owner.readBindings(scope);
    this.#foreign.set(key, reader);
    return this;
  }
  /** Resolve previously unseen owner volumes lazily; each read rechecks the mount's grant. */
  mountReadVolumes(resolver: { mount(volume: VolumeRef): Readonly<{ owner: MemoryConversation; scope: Scope }> }): this {
    if (this.#foreignMount !== undefined) throw new TypeError("foreign content mount is already bound");
    this.#foreignMount = resolver.mount.bind(resolver);
    return this;
  }
  /** Owner issues one exact-file grant to an unrelated attached reader. */
  delegateFileRead(reader: AgentId, id: string, file: FileRef): Scope {
    const reference = this.#core.contracts.validate("file_ref", file);
    if (this.#core.volumeStorageName(reference.volume) !== this.#core.volumeStorageName(this.#volume) || this.#localBytes(reference) === undefined) {
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
    const file = this.#core.contracts.validate("file_ref", reference);
    if (this.#core.volumeStorageName(file.volume) !== this.#core.volumeStorageName(this.#volume)) throw new TypeError("file belongs to another owner");
    this.#core.verifyContentRead(scope, file);
    const bytes = this.#localBytes(file);
    if (bytes === undefined) throw new TypeError("owner has no resident file");
    this.#core.contracts.verifyFileBytes(file, bytes);
    return Uint8Array.from(bytes);
  }
  /** Owner-authenticated, generation-pinned listing; no prior FileRef is needed. */
  listPrivateDirectory(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string,
    expectedGeneration: ResourceRef<"generation"> | null = null, after: string | null = null,
    maximumEntries = HARNESS_PRIVATE_DIRECTORY_PAGE_DEFAULT): Promise<PrivateDirectoryPage> {
    const reader = this.#reader(volume).directory;
    if (!reader) throw new TypeError("owner has no private directory reader");
    return reader.list(volume, grantedPrefix, path, expectedGeneration, after, maximumEntries);
  }
  /** Resolve a path through its owner to a verified immutable FileRef and bytes. */
  readPrivatePath(volume: VolumeRef<"agent_private">, grantedPrefix: string, path: string,
    expectedGeneration: ResourceRef<"generation"> | null = null): Promise<Readonly<{ file: FileRef; bytes: Uint8Array }>> {
    const reader = this.#reader(volume).directory;
    if (!reader) throw new TypeError("owner has no private directory reader");
    return reader.readPath(volume, grantedPrefix, path, expectedGeneration);
  }
  /** Mount this owner's volume read-only under an exact owner-issued scope. */
  readBindings(scope: Scope): ContentReader & Readonly<{ requireFileRead(file: FileRef): void }> {
    const admittedScope = structuredClone(scope);
    this.#core.verifyScope(admittedScope);
    return {
      validate: (file, limits) => this.#core.validateFileUnderLimits(file, limits),
      read: file => this.readAuthorized(admittedScope, file),
      requireFileRead: file => {
        const reference = this.#core.contracts.validate("file_ref", file);
        if (this.#core.volumeStorageName(reference.volume) !== this.#core.volumeStorageName(this.#volume)) {
          throw new TypeError("file belongs to another owner");
        }
        this.#core.verifyContentRead(admittedScope, reference);
      },
      fileReadCapability: file => this.#core.fileReadCapability(file),
      volumeReadCapability: volume => this.#core.volumeCapability(volume, "read"),
      directoryReadCapability: (volume, prefix) => this.#core.directoryReadCapability(volume, prefix),
      directory: {
        list: async (volume, prefix, path, expected, after, maximum) =>
          this.#listAuthorized(admittedScope, volume, prefix, path, expected, after, maximum),
        readPath: async (volume, prefix, path, expected) =>
          this.#readPathAuthorized(admittedScope, volume, prefix, path, expected),
      },
    };
  }
  /** Bind this owner's authenticated content provider to typed task/tool contexts. */
  contentBindings(): ContentBindings {
    return {
      ...this.readBindings(this.#scope),
      read: file => this.read(file),
      writer: {
        volume: this.#volume,
        writeCapability: () => this.#core.volumeCapability(this.#volume, "write"),
        stage: async (operationId, path, bytes, mediaType, displayName) => {
          let admitted = this.#uploads.get(operationId);
          if (admitted === undefined) {
            if (this.#uploads.size >= this.#maxResidentFiles) {
              throw new TypeError("memory upload identity retention limit exceeded");
            }
            admitted = this.stage(path, bytes, mediaType, displayName);
            this.#uploads.set(operationId, admitted);
          }
          let file: FileRef;
          try { file = await admitted; }
          catch (error) {
            if (this.#uploads.get(operationId) === admitted) this.#uploads.delete(operationId);
            throw error;
          }
          const candidate = this.#core.contracts.fileDescriptor(bytes, mediaType);
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
  free(): void {
    this.#core.free();
    this.#content.free();
    this.#uploads.clear();
    this.#foreign.clear();
    this.#foreignMount = undefined;
  }

  /** Store immutable bytes before any record can refer to them. */
  async stage(path: string, bytes: Uint8Array, mediaType: string, displayName: string): Promise<FileRef> {
    return this.#content.stage(path, bytes, mediaType, displayName, true);
  }

  /** Possession of a ref is not a foreign-owner read grant. */
  async read(reference: FileRef): Promise<Uint8Array> {
    const file = this.#core.contracts.validate("file_ref", reference);
    const bytes = await this.#reader(file.volume).read(file);
    if (!(bytes instanceof Uint8Array)) throw new TypeError("content resolver returned invalid bytes");
    this.#core.contracts.verifyFileBytes(file, bytes);
    return Uint8Array.from(bytes);
  }

  #reader(volume: VolumeRef): ContentReader {
    const key = this.#core.volumeStorageName(volume);
    if (key === this.#core.volumeStorageName(this.#volume)) return this.readBindings(this.#scope);
    const bound = this.#foreign.get(key);
    if (bound) return bound;
    const mounted = this.#foreignMount?.(volume);
    if (!mounted || this.#core.volumeStorageName(mounted.owner.volume) !== key
      || mounted.scope.agent !== this.#volume.owner.id) {
      throw new TypeError("content has no authorized owning-provider resolver");
    }
    return mounted.owner.readBindings(mounted.scope);
  }

  #checkDirectory(scope: Scope, volume: VolumeRef, grantedPrefix: string, path: string,
    expected: ResourceRef<"generation"> | null): bigint {
    if (this.#core.volumeStorageName(volume) !== this.#core.volumeStorageName(this.#volume)) {
      throw new TypeError("directory belongs to another owner");
    }
    this.#core.verifyPrivateDirectoryRead(scope, volume, grantedPrefix, path);
    const current = this.#content.generation();
    if (expected === null) return current;
    const version = expected.version;
    if (version === null || version.length > 20 || !/^(0|[1-9][0-9]*)$/.test(version)) {
      throw new TypeError("private directory generation is unavailable");
    }
    const generation = BigInt(version);
    if (generation > current || !this.#core.contracts.canonicalEqual(
      this.#core.contracts.validate("resource_ref", expected), this.#directoryGeneration(version))) {
      throw new TypeError("private directory generation is unavailable");
    }
    return generation;
  }

  #directoryGeneration(version = this.#content.generation().toString()): ResourceRef<"generation"> {
    return this.#core.contracts.validate("resource_ref", {
      kind: "generation", provider: this.#volume.provider,
      key: [...this.#core.contracts.digestCanonicalJson([this.#volume.id, version])],
      version,
    });
  }

  #listAuthorized(scope: Scope, volume: VolumeRef<"agent_private">, grantedPrefix: string,
    path: string, expected: ResourceRef<"generation"> | null, after: string | null, maximum: number): PrivateDirectoryPage {
    const generation = this.#checkDirectory(scope, volume, grantedPrefix, path, expected);
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > HARNESS_PRIVATE_DIRECTORY_PAGE_MAXIMUM) {
      throw new RangeError("private directory page limit is invalid");
    }
    if (after !== null) {
      this.#core.directoryReadCapability(volume, path ? `${path}/${after}` : after);
      if (after.includes("/")) throw new TypeError("directory cursor must name one entry");
    }
    const page = this.#content.list(path, generation, after, maximum);
    const entries = page.entries;
    return this.#core.contracts.validate("private_directory_page", {
      generation: this.#directoryGeneration(expected?.version ?? undefined), entries, hasMore: page.hasMore,
    });
  }

  async #readPathAuthorized(scope: Scope, volume: VolumeRef<"agent_private">, grantedPrefix: string,
    path: string, expected: ResourceRef<"generation"> | null): Promise<Readonly<{ file: FileRef; bytes: Uint8Array }>> {
    const generation = this.#checkDirectory(scope, volume, grantedPrefix, path, expected);
    const file = this.#content.readPath(path, generation);
    return { file, bytes: await this.readAuthorized(scope, file) };
  }

  #localBytes(file: FileRef): Uint8Array | undefined {
    return this.#content.has(file) ? this.#content.read(file) : undefined;
  }

  /** Projection rebuilt from canonical reducer events, not a parallel mutable transcript. */
  conversation(): ConversationState {
    return this.#core.conversation();
  }

  #apply<Action extends { readonly kind: string }>(operation: OperationId, label: string, action: Action): void {
    const [authority, revision] = this.#core.head();
    const command: Command<Action> = { authority,
      operation_id: operation, idempotency_key: `conversation:${operation}:${label}`,
      expected_revision: revision, scope: this.#scope, causal_parent: null, action };
    this.#core.apply(command);
  }
}
