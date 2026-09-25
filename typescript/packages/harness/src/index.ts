import { WasmReducer, type InitInput } from "../generated/wasm/acyclic_harness_wasm.js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AggregateKind as WireAggregateKind,
  ApplyResponseSchema,
  ApplyState,
  AuthoritySchema,
  CommandEnvelopeSchema,
  EventReferenceSchema,
  EventEnvelopeSchema,
  OperationIdentitySchema,
  ProtocolIdentitySchema,
  ScopeSchema,
} from "../generated/proto/harness/v2/harness_pb.js";
import { AgentHarness, HarnessBuilder, type AgentHarnessHost } from "./runtime.js";
import type { ConversationMessage, ConversationMessageId, ConversationPage, ConversationState, FileDescriptor, FileRef, Limits, VolumeRef, Attachment } from "./conversation.js";
import type { ToolJsonSchema } from "./model.js";
import type { InteractionId } from "./interaction.js";
import { NativeContracts } from "./native-contracts.js";

export * from "./cache.js";
export * from "./conversation.js";
export * from "./native-contracts.js";
export * from "./fork.js";
export * from "./project.js";
export * from "./interaction.js";
export * from "./extension.js";
export * from "./client.js";
export * from "./pagination.js";
export * from "./wire-transport.js";
export * from "./runtime.js";
export * from "./openai.js";
export * from "./projection.js";
export * from "./memory-conversation.js";

declare const brand: unique symbol;
type Id<Name extends string> = string & { readonly [brand]: Name };

export type AgentId = Id<"AgentId">;
export type ConversationId = Id<"ConversationId">;
export type SessionId = Id<"SessionId">;
export type TurnId = Id<"TurnId">;
export type TaskId = Id<"TaskId">;
export type OperationId = Id<"OperationId">;
export type EffectId = Id<"EffectId">;
export interface IdentityKindMap {
  readonly agent: AgentId;
  readonly conversation: ConversationId;
  readonly session: SessionId;
  readonly turn: TurnId;
  readonly task: TaskId;
  readonly operation: OperationId;
  readonly effect: EffectId;
  readonly interaction: InteractionId;
}
export type IdentityKind = keyof IdentityKindMap;

/** Parses and canonicalizes a branded identity through the Rust contract. */
export async function parseIdentity<Kind extends IdentityKind>(
  kind: Kind, value: string,
): Promise<IdentityKindMap[Kind]> {
  return (await NativeContracts.create()).validateIdentity(kind, value);
}
export type AggregateKind = "agent" | "conversation" | "session" | "turn" | "task";

export interface Authority<Kind extends AggregateKind = AggregateKind> {
  readonly kind: Kind;
  readonly id: string;
}

export interface Scope {
  readonly id: string;
  readonly capabilities: readonly string[];
  readonly issuer: string;
  readonly agent: AgentId | null;
  readonly parent_proof: readonly number[] | null;
  readonly proof: readonly number[];
}

/** Durable non-bearer admission evidence; a signed command scope is never stored in an event. */
export interface RecordedScope {
  readonly id: string;
  readonly capabilities: readonly string[];
  readonly issuer: string;
  readonly agent: AgentId | null;
}

export type AuthorityLevel = "runtime" | "agent" | "conversation" | "session" | "turn" | "task" | "invocation";

export interface PolicyLayer {
  readonly level: AuthorityLevel;
  readonly policy: {
    readonly grants: readonly string[];
    readonly denies: readonly string[];
  };
}

export interface Command<Action = unknown> {
  readonly authority: Authority;
  readonly operation_id: OperationId;
  readonly idempotency_key: string;
  readonly expected_revision: bigint;
  readonly scope: Scope;
  readonly causal_parent: EventReference | null;
  readonly action: Action;
}

export interface EventReference {
  readonly authority: Authority;
  readonly revision: bigint;
}

export interface Event<Payload = unknown> {
  readonly authority: Authority;
  readonly revision: bigint;
  readonly operationId: OperationId;
  readonly intentDigest: Uint8Array;
  readonly scope: RecordedScope;
  readonly attestation: Uint8Array;
  readonly causalParent?: EventReference;
  readonly payload: Payload;
}

export interface Snapshot {
  readonly format_version: number;
  readonly authority: Authority;
  readonly revision: bigint;
  readonly events: readonly unknown[];
  readonly state_digest: readonly number[];
}

export type ApplyResult<Event = unknown> =
  | { readonly result: "applied"; readonly event: Event; readonly eventWire: Uint8Array }
  | { readonly result: "replayed"; readonly event: Event; readonly eventWire: Uint8Array };

/** A parser may establish a payload type only after the durable apply result is known. */
export function decodeEventPayload<Payload>(event: Event<unknown>, parse: (value: unknown) => Payload): Event<Payload> {
  return { ...event, payload: parse(event.payload) };
}

export interface HarnessOptions {
  readonly authority: Authority;
  readonly issuerId: string;
  readonly issuerKey: Uint8Array;
  readonly wasm?: InitInput;
  readonly schemas?: readonly ExtensionSchema[];
}

export interface ProtocolIdentityValue {
  readonly version: string;
  readonly descriptor_digest: string;
}

export interface ExtensionSchema {
  readonly name: string;
  readonly version: number;
  readonly schema: ToolJsonSchema;
  readonly implementation_digest: readonly number[];
  readonly fork_policy: "inherit" | "reset" | "reject";
}

/** Synchronous typed facade over the canonical Rust reducer hosted in WASM. */
export class Harness {
  readonly #core: WasmReducer;
  readonly #contracts: NativeContracts;
  readonly #protocol: ProtocolIdentityValue;

  private constructor(core: WasmReducer, contracts: NativeContracts) {
    this.#core = core;
    this.#contracts = contracts;
    this.#protocol = core.protocolIdentity() as ProtocolIdentityValue;
  }

  /** Starts a typed runtime with already initialized Rust contract admission. */
  static builder(contracts: NativeContracts): HarnessBuilder { return new HarnessBuilder(contracts); }

  /** Connects to a durable runtime host through its explicit typed capability. */
  static connect(host: AgentHarnessHost): Promise<AgentHarness> { return host.connect(); }

  /** Initializes the shared Rust core and creates an empty aggregate. */
  static async create(options: HarnessOptions): Promise<Harness> {
    const contracts = await NativeContracts.create(options.wasm);
    const key = options.issuerKey;
    if (key.byteLength !== 32) throw new Error("issuerKey must contain exactly 32 bytes");
    return new Harness(
      new WasmReducer(options.authority, options.issuerId, key, options.schemas ?? []),
      contracts,
    );
  }

  /** Restores durable state while checking its expected aggregate audience. */
  static async restore(snapshot: Snapshot, options: HarnessOptions): Promise<Harness> {
    if (
      snapshot.authority.kind !== options.authority.kind ||
      snapshot.authority.id !== options.authority.id
    ) {
      throw new Error("snapshot authority does not match expected authority");
    }
    const contracts = await NativeContracts.create(options.wasm);
    if (options.issuerKey.byteLength !== 32) {
      throw new Error("issuerKey must contain exactly 32 bytes");
    }
    return new Harness(
      WasmReducer.restore(
        snapshot,
        options.issuerId,
        options.issuerKey,
        options.schemas ?? [],
      ),
      contracts,
    );
  }

  /** Returns the exact wire protocol identity compiled into the native/WASM reducer. */
  protocolIdentity(): { readonly version: string; readonly descriptorDigest: string } {
    return {
      version: this.#protocol.version,
      descriptorDigest: this.#protocol.descriptor_digest,
    };
  }

  /** Issues an explicit root grant from this host. */
  issueScope(id: string, capabilities: readonly string[]): Scope {
    return this.#core.issueScope(id, capabilities) as Scope;
  }

  /** Issues a signed, owner-bound scope for private file writes. */
  issueScopeForAgent(agent: AgentId, id: string, capabilities: readonly string[]): Scope {
    return this.#core.issueScopeForAgent(agent, id, capabilities) as Scope;
  }

  /** Resolves runtime-to-invocation policy layers; grants intersect and deny wins. */
  issueScopeWithPolicies(id: string, layers: readonly PolicyLayer[]): Scope {
    return this.#core.issueScopeWithPolicies(id, layers) as Scope;
  }

  /** Resolves hierarchy policy for one signed acting-agent identity. */
  issueScopeWithPoliciesForAgent(agent: AgentId, id: string, layers: readonly PolicyLayer[]): Scope {
    return this.#core.issueScopeWithPoliciesForAgent(agent, id, layers) as Scope;
  }

  /** Creates a child grant that cannot widen its parent. */
  attenuate(parent: Scope, id: string, capabilities: readonly string[]): Scope {
    return this.#core.attenuate(parent, id, capabilities) as Scope;
  }

  /** Shares one pinned private file without granting a writable directory or fork lineage. */
  delegatePrivateFileRead(owner: Scope, reader: AgentId, id: string, file: FileRef): Scope {
    return this.#core.delegatePrivateFileRead(owner, reader, id, file) as Scope;
  }

  /** Grants read-only, lazy discovery of one private directory without fork ancestry. */
  delegatePrivateDirectoryRead(
    owner: Scope, reader: AgentId, id: string, volume: VolumeRef, prefix: string,
  ): Scope {
    return this.#core.delegatePrivateDirectoryRead(owner, reader, id, volume, prefix) as Scope;
  }

  /** Uses the Rust contract's canonical volume-capability encoding. */
  volumeCapability(volume: VolumeRef, operation: "read" | "write"): string {
    return this.#core.volumeCapability(volume, operation);
  }

  /** Uses the Rust contract's canonical exact-file capability encoding. */
  fileReadCapability(file: FileRef): string {
    return this.#core.fileReadCapability(file);
  }

  /** Uses the Rust contract's segment-bounded private-directory capability. */
  directoryReadCapability(volume: VolumeRef, prefix: string): string {
    return this.#core.directoryReadCapability(volume, prefix);
  }

  /** Proves that this issuer signed a matching read grant for one pinned file. */
  verifyContentRead(scope: Scope, file: FileRef): void {
    this.#core.verifyContentRead(scope, file);
  }

  /** Authenticates one issuer-signed scope without widening its capabilities. */
  verifyScope(scope: Scope): void {
    this.#core.verifyScope(scope);
  }

  /** Proves a signed write grant and the original agent-private owner. */
  verifyContentWrite(scope: Scope, volume: VolumeRef): void {
    this.#core.verifyContentWrite(scope, volume);
  }

  /** Computes the canonical provider-neutral physical namespace for a volume. */
  volumeStorageName(volume: VolumeRef): string {
    return this.#core.volumeStorageName(volume);
  }

  /** Returns a detached, immutable volume admitted by the Rust contract. */
  validateVolumeRef<Class extends VolumeRef["class"], Family extends string>(
    volume: VolumeRef<Class, Family>,
  ): VolumeRef<Class, Family> {
    return this.#contracts.validate("volume_ref", volume);
  }

  /** Returns a detached, immutable file reference admitted by the Rust contract. */
  validateFileRef(file: FileRef): FileRef {
    return this.#contracts.validate("file_ref", file);
  }

  /** Constructs a content descriptor in Rust; JS owns only the byte transport. */
  fileDescriptor(bytes: Uint8Array, mediaType: string): FileDescriptor {
    return this.#contracts.fileDescriptor(bytes, mediaType);
  }

  /** Applies Rust's checked file/path limits before admission. */
  validateFileUnderLimits(file: FileRef, limits: Limits): void {
    this.#core.validateFileUnderLimits(file, limits);
  }

  /** Applies the native Rust ref-only message and configured-limit contracts. */
  validateConversationMessage(message: ConversationMessage, limits: Limits): ConversationMessage {
    return this.#contracts.validate("conversation_message", message, limits);
  }

  /** Brands an exact message identity using this reducer's initialized Rust module. */
  conversationMessageId(value: string): ConversationMessageId {
    return this.#contracts.validateConversationMessageId(value);
  }

  /** Stable child identity derived by Rust from an admitted operation and role. */
  deriveOperationId(operation: OperationId, label: string): OperationId {
    return this.#contracts.deriveOperationId(operation, label);
  }

  /** Brands a canonical UUID using this reducer's initialized Rust module. */
  identity<Kind extends IdentityKind>(kind: Kind, value: string): IdentityKindMap[Kind] {
    return this.#contracts.validateIdentity(kind, value);
  }

  /** Canonical Rust JSON bytes for ref-valued local artifacts and manifests. */
  canonicalJsonBytes(value: unknown): Uint8Array {
    return this.#contracts.encodeCanonicalJson(value);
  }

  /** Exact BLAKE3 of those canonical bytes, without a TypeScript encoder. */
  canonicalJsonDigest(value: unknown): Uint8Array {
    return this.#contracts.digestCanonicalJson(value);
  }

  /** Checks resident bytes against the Rust SHA-256 and length descriptor. */
  verifyFileBytes(file: FileRef, bytes: Uint8Array): void {
    this.#contracts.verifyFileBytes(file, bytes);
  }

  /** Resolves a pinned manifest with Rust's canonical list validation. */
  decodeAttachmentManifest(manifest: FileRef, bytes: Uint8Array, itemCount: number): readonly Attachment[] {
    return this.#contracts.decodeAttachmentManifest(manifest, bytes, itemCount);
  }

  /** Produces the exact typed Rust serde bytes required for a complete manifest. */
  encodeAttachmentManifest(items: readonly Attachment[]): Uint8Array {
    return this.#contracts.encodeAttachmentManifest(items);
  }

  /** Reads one bounded immutable page from the Rust reducer. */
  conversationPage(afterSequence: bigint, limit = 1_024): ConversationPage {
    if (typeof afterSequence !== "bigint" || afterSequence < 0n
      || !Number.isSafeInteger(limit) || limit <= 0 || limit > 1_024) {
      throw new TypeError("conversation page cursor or limit is invalid");
    }
    return this.#contracts.conversationPage(this.#core, afterSequence, limit);
  }

  /** Hydrates complete history through bounded Rust pages, never one giant JSON response. */
  conversation(): ConversationState {
    const messages: ConversationMessage[] = [];
    let cursor = 0n;
    let expectedRevision: bigint | undefined;
    let expectedTotal: bigint | undefined;
    let expectedAgent: AgentId | null | undefined;
    while (true) {
      const page = this.conversationPage(cursor);
      if (expectedRevision === undefined) {
        expectedRevision = page.event_revision;
        expectedTotal = page.total_messages;
        expectedAgent = page.agent;
      } else if (page.event_revision !== expectedRevision || page.total_messages !== expectedTotal
        || page.agent !== expectedAgent) {
        throw new TypeError("conversation changed while hydrating pages");
      }
      if (page.messages.length > 1_024 || cursor + BigInt(page.messages.length) > page.total_messages) {
        throw new TypeError("conversation page exceeds its declared history");
      }
      for (const message of page.messages) {
        if (message.sequence !== cursor + 1n) {
          throw new TypeError("conversation page sequence is not contiguous");
        }
        cursor = message.sequence;
        messages.push(message);
      }
      if (page.next_sequence === null) {
        if (cursor !== page.total_messages) throw new TypeError("conversation page ended before its declared tail");
        return { agent: expectedAgent ?? null, messages };
      }
      if (page.next_sequence !== cursor || page.messages.length === 0) {
        throw new TypeError("conversation page cursor did not advance");
      }
    }
  }

  /** Applies a deterministic command and returns its canonical event. */
  apply<Action extends { readonly kind: string }>(
    command: Command<Action>,
  ): ApplyResult<Event<unknown>> {
    const encoded = toBinary(
      CommandEnvelopeSchema,
      create(CommandEnvelopeSchema, {
        protocol: create(ProtocolIdentitySchema, {
          version: this.#protocol.version,
          descriptorDigest: this.#protocol.descriptor_digest,
        }),
        authority: encodeAuthority(command.authority),
        operation: create(OperationIdentitySchema, {
          operationId: command.operation_id,
          idempotencyKey: command.idempotency_key,
        }),
        expectedRevision: command.expected_revision,
        scope: create(ScopeSchema, {
          id: command.scope.id,
          capabilities: [...command.scope.capabilities],
          issuer: command.scope.issuer,
          agentId: command.scope.agent ?? "",
          parentProof: new Uint8Array(command.scope.parent_proof ?? []),
          proof: new Uint8Array(command.scope.proof),
        }),
        causalParent: command.causal_parent === null ? undefined : encodeReference(command.causal_parent),
        actionType: command.action.kind,
        canonicalActionJson: this.#contracts.encodeCanonicalJson(command.action),
      }),
    );
    const response = fromBinary(ApplyResponseSchema, this.#core.applyWire(encoded));
    if (response.event === undefined || response.state === ApplyState.UNSPECIFIED) {
      throw new Error("Rust core returned an invalid apply response");
    }
    const eventWire = toBinary(EventEnvelopeSchema, response.event);
    const payload: unknown = this.#contracts.decodeCanonicalJson(response.event.canonicalPayloadJson);
    const event: Event<unknown> = {
      authority: decodeAuthority(response.event.authority),
      revision: response.event.revision,
      operationId: this.#contracts.validateIdentity("operation", response.event.operationId),
      intentDigest: response.event.intentDigest,
      scope: decodeRecordedScope(response.event.scope, this.#contracts),
      attestation: response.event.attestation,
      ...(response.event.causalParent === undefined
        ? {}
        : { causalParent: decodeReference(response.event.causalParent) }),
      payload,
    };
    return response.state === ApplyState.APPLIED
      ? { result: "applied", event, eventWire }
      : { result: "replayed", event, eventWire };
  }

  /** Returns a versioned integrity-checked snapshot. */
  snapshot(): Snapshot {
    return this.#core.snapshot() as Snapshot;
  }

  /** Releases the underlying WASM reducer immediately. */
  free(): void {
    this.#core.free();
  }
}

function encodeAuthority(authority: Authority) {
  const kinds: Record<AggregateKind, WireAggregateKind> = {
    agent: WireAggregateKind.AGENT,
    conversation: WireAggregateKind.CONVERSATION,
    session: WireAggregateKind.SESSION,
    turn: WireAggregateKind.TURN,
    task: WireAggregateKind.TASK,
  };
  return create(AuthoritySchema, { kind: kinds[authority.kind], id: authority.id });
}

function decodeAuthority(authority: { kind: WireAggregateKind; id: string } | undefined): Authority {
  if (authority === undefined) throw new Error("event authority is missing");
  const kinds: Partial<Record<WireAggregateKind, AggregateKind>> = {
    [WireAggregateKind.AGENT]: "agent",
    [WireAggregateKind.CONVERSATION]: "conversation",
    [WireAggregateKind.SESSION]: "session",
    [WireAggregateKind.TURN]: "turn",
    [WireAggregateKind.TASK]: "task",
  };
  const kind = kinds[authority.kind];
  if (kind === undefined) throw new Error("event authority kind is invalid");
  return { kind, id: authority.id };
}

function encodeReference(reference: EventReference) {
  if (typeof reference.revision !== "bigint" || reference.revision < 0n) {
    throw new TypeError("event reference revision must be a non-negative bigint");
  }
  return create(EventReferenceSchema, { authority: encodeAuthority(reference.authority), revision: reference.revision });
}

function decodeReference(reference: {
  authority?: { kind: WireAggregateKind; id: string } | undefined;
  revision: bigint;
}): EventReference {
  return { authority: decodeAuthority(reference.authority), revision: reference.revision };
}

function decodeRecordedScope(scope: {
  id: string;
  capabilities: string[];
  issuer: string;
  agentId: string;
} | undefined, contracts: NativeContracts): RecordedScope {
  if (scope === undefined) throw new Error("event scope is missing");
  return {
    id: scope.id,
    capabilities: scope.capabilities,
    issuer: scope.issuer,
    agent: scope.agentId === "" ? null : contracts.validateIdentity("agent", scope.agentId),
  };
}
