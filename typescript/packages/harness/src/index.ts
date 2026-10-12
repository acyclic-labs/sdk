import { WasmReducer } from "../generated/wasm/acyclic_harness_wasm.js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ApplyResponseSchema,
  ApplyState,
  AuthoritySchema,
  CommandEnvelopeSchema,
  EventReferenceSchema,
  EventEnvelopeSchema,
  OperationIdentitySchema,
  ScopeSchema,
} from "../generated/proto/harness/v2/harness_pb.js";
import {
  ProtocolIdentitySchema,
} from "../generated/proto/protocol/v1/protocol_pb.js";
import { HarnessBuilder } from "./runtime.js";
import type { ConversationMessage, ConversationMessageId, ConversationPage, ConversationState, FileRef, Limits, VolumeRef } from "./conversation.js";
import type { ToolJsonSchema } from "./model.js";
import type { InteractionId } from "./interaction.js";
import type { ForkHistoryPolicy, ForkRequest } from "./fork.js";
import { NativeContracts } from "./native-contracts.js";
import { aggregateKindToWire, decodeAggregateKind } from "./enums.js";
import type { AggregateKind, AuthorityLevel } from "./enums.js";
import { HARNESS_CONVERSATION_PAGE_DEFAULT, HARNESS_CONVERSATION_PAGE_MAXIMUM } from "./conversation-page-contract.js";

export * from "./cache.js";
export * from "./enums.js";
export * from "./conversation.js";
export * from "./native-contracts.js";
export { createBrowserMcpHttpProvider } from "./mcp-http.js";
export { WasmMcpHttpTransport, WasmMcpHttpInitialization } from "../generated/wasm/acyclic_harness_wasm.js";
export type { SessionLimitsWire } from "./limits-contract.js";
export * from "./fork.js";
export * from "./project.js";
export * from "./interaction.js";
export * from "./extension.js";
export * from "./client.js";
export * from "./client-views.js";
export * from "./client-demand.js";
export { RequestScheduler, RequestCapacityError, type RequestLimits } from "./client-requests.js";
export * from "./client-transport.js";
export type { ClientSnapshot } from "./client-snapshot.js";
export * from "./pagination.js";
export * from "./wire-transport.js";
export { performanceObserver, type AcyclicObserver, type OperationEvent } from "./observe.js";
export * from "./runtime.js";
export * from "./openai.js";
export * from "./projection.js";
export * from "./memory-conversation.js";
export * from "./browser-history.js";

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
  readonly group: import("./runtime.js").GroupId;
  readonly batch: import("./runtime.js").BatchId;
  readonly operation: OperationId;
  readonly effect: EffectId;
  readonly interaction: InteractionId;
}
export type IdentityKind = keyof IdentityKindMap;

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
  /** Opaque Rust projection; authenticated together with the event cache. */
  readonly projection: unknown;
  readonly state_digest: readonly number[];
  readonly attestation: readonly number[];
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

  get contracts(): NativeContracts { return this.#contracts; }

  private constructor(core: WasmReducer, contracts: NativeContracts) {
    this.#core = core;
    this.#contracts = contracts;
    this.#protocol = core.protocolIdentity() as ProtocolIdentityValue;
  }

  /** Starts a typed runtime with already initialized Rust contract admission. */
  static builder(contracts: NativeContracts): HarnessBuilder { return new HarnessBuilder(contracts); }

  /** Initializes the shared Rust core and creates an empty aggregate. */
  static async create(options: HarnessOptions): Promise<Harness> {
    const contracts = await NativeContracts.create();
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
    const contracts = await NativeContracts.create();
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

  /** Authenticates lazy owner-private discovery under a signed subtree grant. */
  verifyPrivateDirectoryRead(scope: Scope, volume: VolumeRef, grantedPrefix: string, path: string): void {
    this.#core.verifyPrivateDirectoryRead(scope, volume, grantedPrefix, path);
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

  /** Selects the exact logical parent boundary before any fork preparation.
   * The default inherits logical history; fresh history is explicitly selected.
   * Providers, child volumes, grants and finite bounds remain caller supplied. */
  prepareForkRequest(request: ForkRequest, policy: ForkHistoryPolicy | null = null): ForkRequest {
    const prepared = this.#core.prepareForkRequest(request, policy);
    const summary = prepared.preparation.summary;
    // Public Limits use exact Numbers; native counters retain BigInt.
    const numericLimit = (value: bigint): number => {
      if (typeof value !== "bigint" || value <= 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new RangeError("native Summary limit is not an exact positive integer");
      }
      return Number(value);
    };
    return { ...prepared, preparation: { ...prepared.preparation, summary: summary == null ? null : {
      ...summary, limits: {
        file_bytes: numericLimit(summary.limits.file_bytes),
        path_bytes: numericLimit(summary.limits.path_bytes),
        attachments: numericLimit(summary.limits.attachments),
        render_bytes: numericLimit(summary.limits.render_bytes),
        model_steps: numericLimit(summary.limits.model_steps),
        model_events_per_step: numericLimit(summary.limits.model_events_per_step),
        tool_calls_per_step: numericLimit(summary.limits.tool_calls_per_step),
        context_messages: numericLimit(summary.limits.context_messages),
      },
    } } } as ForkRequest;
  }

  /** Applies Rust's checked file/path limits before admission. */
  validateFileUnderLimits(file: FileRef, limits: Limits): void {
    this.#core.validateFileUnderLimits(file, limits);
  }

  /** Reads one bounded immutable page from the Rust reducer. */
  conversationPage(afterSequence: bigint, limit = HARNESS_CONVERSATION_PAGE_DEFAULT): ConversationPage {
    if (typeof afterSequence !== "bigint" || afterSequence < 0n
      || !Number.isSafeInteger(limit) || limit <= 0 || limit > HARNESS_CONVERSATION_PAGE_MAXIMUM) {
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
      if (page.messages.length > HARNESS_CONVERSATION_PAGE_MAXIMUM
        || cursor + BigInt(page.messages.length) > page.total_messages) {
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

  /** Read the authoritative command position without serializing history. */
  head(): readonly [authority: Authority, revision: bigint] {
    return this.#core.head() as readonly [Authority, bigint];
  }

  /** Reads one admitted selection without serializing the retained event history. */
  contextSelectionForOperation(operationId: OperationId): Readonly<{
    conversation_revision: bigint;
    message_ids: readonly ConversationMessageId[];
    checkpoint?: FileRef;
  }> | null {
    return this.#core.contextSelectionForOperation(operationId);
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
  return create(AuthoritySchema, { kind: aggregateKindToWire[authority.kind], id: authority.id });
}

function decodeAuthority(authority: { kind: number; id: string } | undefined): Authority {
  if (authority === undefined) throw new Error("event authority is missing");
  return { kind: decodeAggregateKind(authority.kind), id: authority.id };
}

function encodeReference(reference: EventReference) {
  if (typeof reference.revision !== "bigint" || reference.revision < 0n) {
    throw new TypeError("event reference revision must be a non-negative bigint");
  }
  return create(EventReferenceSchema, { authority: encodeAuthority(reference.authority), revision: reference.revision });
}

function decodeReference(reference: {
  authority?: { kind: number; id: string } | undefined;
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
