import initWasm, { WasmReducer, type InitInput } from "../generated/wasm/acyclic_harness_wasm.js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AggregateKind as WireAggregateKind,
  ApplyResponseSchema,
  ApplyState,
  CommandEnvelopeSchema,
} from "../generated/proto/harness/v1/harness_pb.js";

export * from "./cache.js";
export * from "./client.js";
export * from "./pagination.js";
export * from "./wire-transport.js";
export * from "./runtime.js";
export * from "./openai.js";

declare const brand: unique symbol;
type Id<Name extends string> = string & { readonly [brand]: Name };

export type AgentId = Id<"AgentId">;
export type ConversationId = Id<"ConversationId">;
export type SessionId = Id<"SessionId">;
export type TurnId = Id<"TurnId">;
export type TaskId = Id<"TaskId">;
export type OperationId = Id<"OperationId">;
export type EffectId = Id<"EffectId">;
export type AggregateKind = "agent" | "conversation" | "session" | "turn" | "task";

export interface Authority {
  readonly kind: AggregateKind;
  readonly id: string;
}

export interface Scope {
  readonly id: string;
  readonly capabilities: readonly string[];
  readonly issuer: string;
  readonly parent_proof: readonly number[] | null;
  readonly proof: readonly number[];
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
  readonly causal_parent: unknown | null;
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
  readonly scope: Scope;
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
  | { readonly result: "applied"; readonly event: Event }
  | { readonly result: "replayed"; readonly event: Event };

export interface HarnessOptions {
  readonly authority: Authority;
  readonly issuerId: string;
  readonly issuerKey: Uint8Array;
  readonly wasm?: InitInput;
  readonly schemas?: readonly ExtensionSchema[];
}

interface ProtocolIdentityValue {
  readonly version: string;
  readonly descriptor_digest: string;
}

export interface ExtensionSchema {
  readonly name: string;
  readonly version: number;
  readonly schema: unknown;
}

/** Synchronous typed facade over the canonical Rust reducer hosted in WASM. */
export class Harness {
  readonly #core: WasmReducer;
  readonly #protocol: ProtocolIdentityValue;

  private constructor(core: WasmReducer) {
    this.#core = core;
    this.#protocol = core.protocolIdentity() as ProtocolIdentityValue;
  }

  /** Initializes the shared Rust core and creates an empty aggregate. */
  static async create(options: HarnessOptions): Promise<Harness> {
    if (options.wasm === undefined) await initWasm();
    else await initWasm({ module_or_path: options.wasm });
    const key = options.issuerKey;
    if (key.byteLength !== 32) throw new Error("issuerKey must contain exactly 32 bytes");
    return new Harness(
      new WasmReducer(options.authority, options.issuerId, key, options.schemas ?? []),
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
    if (options.wasm === undefined) await initWasm();
    else await initWasm({ module_or_path: options.wasm });
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
    );
  }

  /** Issues an explicit root grant from this host. */
  issueScope(id: string, capabilities: readonly string[]): Scope {
    return this.#core.issueScope(id, capabilities) as Scope;
  }

  /** Resolves runtime-to-invocation policy layers; grants intersect and deny wins. */
  issueScopeWithPolicies(id: string, layers: readonly PolicyLayer[]): Scope {
    return this.#core.issueScopeWithPolicies(id, layers) as Scope;
  }

  /** Creates a child grant that cannot widen its parent. */
  attenuate(parent: Scope, id: string, capabilities: readonly string[]): Scope {
    return this.#core.attenuate(parent, id, capabilities) as Scope;
  }

  /** Applies a deterministic command and returns its canonical event. */
  apply<Payload = unknown, Action extends { readonly kind: string } = { readonly kind: string }>(
    command: Command<Action>,
  ): ApplyResult<Event<Payload>> {
    const encoded = toBinary(
      CommandEnvelopeSchema,
      create(CommandEnvelopeSchema, {
        protocol: {
          version: this.#protocol.version,
          descriptorDigest: this.#protocol.descriptor_digest,
        },
        authority: encodeAuthority(command.authority),
        operation: {
          operationId: command.operation_id,
          idempotencyKey: command.idempotency_key,
        },
        expectedRevision: command.expected_revision,
        scope: {
          id: command.scope.id,
          capabilities: [...command.scope.capabilities],
          issuer: command.scope.issuer,
          parentProof: new Uint8Array(command.scope.parent_proof ?? []),
          proof: new Uint8Array(command.scope.proof),
        },
        causalParent: command.causal_parent === null ? undefined : encodeReference(command.causal_parent as EventReference),
        actionType: command.action.kind,
        canonicalActionJson: new TextEncoder().encode(canonicalJson(command.action)),
      }),
    );
    const response = fromBinary(ApplyResponseSchema, this.#core.applyWire(encoded));
    if (response.event === undefined || response.state === ApplyState.UNSPECIFIED) {
      throw new Error("Rust core returned an invalid apply response");
    }
    const event: Event<Payload> = {
      authority: decodeAuthority(response.event.authority),
      revision: response.event.revision,
      operationId: response.event.operationId as OperationId,
      intentDigest: response.event.intentDigest,
      scope: decodeScope(response.event.scope),
      ...(response.event.causalParent === undefined
        ? {}
        : { causalParent: decodeReference(response.event.causalParent) }),
      payload: JSON.parse(new TextDecoder().decode(response.event.canonicalPayloadJson)) as Payload,
    };
    return response.state === ApplyState.APPLIED
      ? { result: "applied", event }
      : { result: "replayed", event };
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
  return { kind: kinds[authority.kind], id: authority.id };
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
  return { authority: encodeAuthority(reference.authority), revision: reference.revision };
}

function decodeReference(reference: {
  authority?: { kind: WireAggregateKind; id: string } | undefined;
  revision: bigint;
}): EventReference {
  return { authority: decodeAuthority(reference.authority), revision: reference.revision };
}

function decodeScope(scope: {
  id: string;
  capabilities: string[];
  issuer: string;
  parentProof: Uint8Array;
  proof: Uint8Array;
} | undefined): Scope {
  if (scope === undefined) throw new Error("event scope is missing");
  return {
    id: scope.id,
    capabilities: scope.capabilities,
    issuer: scope.issuer,
    parent_proof: scope.parentProof.length === 0 ? null : [...scope.parentProof],
    proof: [...scope.proof],
  };
}

function canonicalJson(value: unknown): string {
  return canonical(value, new Set<object>());
}

function canonical(value: unknown, ancestors: Set<object>): string {
  if (typeof value === "number" && (!Number.isFinite(value) || Object.is(value, -0))) {
    throw new TypeError("non-finite numbers and negative zero are not canonical JSON");
  }
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
    return JSON.stringify(value);
  }
  if (typeof value !== "object") throw new TypeError("value is not canonical JSON");
  if (ancestors.has(value)) throw new TypeError("cyclic values are not canonical JSON");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return `[${value.map(child => canonical(child, ancestors)).join(",")}]`;
    }
    const prototype = Object.getPrototypeOf(value) as object | null;
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("only plain objects are canonical JSON");
    }
    return `{${Object.entries(value)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([key, child]) => `${JSON.stringify(key)}:${canonical(child, ancestors)}`)
      .join(",")}}`;
  } finally {
    ancestors.delete(value);
  }
}
