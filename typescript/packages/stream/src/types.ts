import type {
  AppendReceipt as WireAppendReceipt, AppendRequest as WireAppendRequest,
  CommittedAppend as WireCommittedAppend, CommittedFork as WireCommittedFork,
  CommittedMutation as WireCommittedMutation, CommittedEnvelope as WireCommittedEnvelope,
  CommitResponse as WireCommitResponse, AppendResponse as WireAppendResponse,
  IdempotencyObservation as WireIdempotencyObservation,
  CommitRequest as WireCommitRequest, CommitCondition as WireCommitCondition,
  CommitMutation as WireCommitMutation, TailCondition as WireTailCondition,
  AbsentCondition as WireAbsentCondition, AppendMutation as WireAppendMutation,
  ForkMutation as WireForkMutation,
  CommitConflict as WireCommitConflict, TailCommitConflict as WireTailCommitConflict,
  ExistsCommitConflict as WireExistsCommitConflict,
  ForkReceipt as WireForkReceipt,
  ForkRequest as WireForkRequest, ReadRequest as WireReadRequest,
  Record as WireRecord, TailConflict as WireTailConflict,
  CreateTokenRequest as WireCreateTokenRequest, TokenGrant as WireTokenGrant,
} from "../generated/proto/stream/v2/stream_pb.js";
import { validateIdempotencyKey as validateIdempotencyKeyRust } from "../generated/wasm/acyclic_stream_wasm.js";
import type { TokenOperation } from "./token-operations.js";
import type { RustOwnedTransportKind } from "./generated-client.js";
export { TOKEN_OPERATIONS } from "./token-operations.js";
export type { TokenOperation } from "./token-operations.js";

type PublicWire<Wire, Overrides extends object = object, Omitted extends keyof Wire = never> =
  Readonly<Omit<Wire, "$typeName" | "$unknown" | keyof Overrides | Omitted> & Overrides>;
type AssertNever<Value extends never> = Value;
type CommittedKind = Exclude<WireCommittedMutation["mutation"]["case"], undefined>;
type _CommittedKinds = AssertNever<Exclude<CommittedKind, "append" | "fork"> | Exclude<"append" | "fork", CommittedKind>>;
type AppendOutcomeKind = Exclude<WireAppendResponse["outcome"]["case"], undefined>;
type _AppendOutcomes = AssertNever<Exclude<AppendOutcomeKind, "committed" | "conflict"> | Exclude<"committed" | "conflict", AppendOutcomeKind>>;
type CommitOutcomeKind = Exclude<WireCommitResponse["outcome"]["case"], undefined>;
type _CommitOutcomes = AssertNever<Exclude<CommitOutcomeKind, "committed" | "conflict"> | Exclude<"committed" | "conflict", CommitOutcomeKind>>;
type IdempotencyKind = Exclude<WireIdempotencyObservation["outcome"]["case"], undefined>;
type _IdempotencyKinds = AssertNever<Exclude<IdempotencyKind, "append" | "fork" | "commit"> | Exclude<"append" | "fork" | "commit", IdempotencyKind>>;
type ProviderConditionKind = Exclude<WireCommitCondition["condition"]["case"], undefined>;
type _ProviderConditionKinds = AssertNever<Exclude<ProviderConditionKind, "tail" | "absent"> | Exclude<"tail" | "absent", ProviderConditionKind>>;
type ProviderMutationKind = Exclude<WireCommitMutation["mutation"]["case"], undefined>;
type _ProviderMutationKinds = AssertNever<Exclude<ProviderMutationKind, "append" | "fork"> | Exclude<"append" | "fork", ProviderMutationKind>>;
type CommitConflictKind = Exclude<WireCommitConflict["conflict"]["case"], undefined>;
type _CommitConflictKinds = AssertNever<Exclude<CommitConflictKind, "tail" | "exists"> | Exclude<"tail" | "exists", CommitConflictKind>>;

declare const streamIdentityBrand: unique symbol;
/** Exact unsigned 64-bit protocol position. */
export type Sequence = WireRecord["sequence"];
/** Opaque stable 32-byte commit identity. */
export type CommitId = Uint8Array & { readonly [streamIdentityBrand]: "CommitId" };
/** Opaque non-empty caller retry identity, bounded by the canonical Stream contract. */
export type IdempotencyKey = Uint8Array & { readonly [streamIdentityBrand]: "IdempotencyKey" };

export function commitId(value: Uint8Array): CommitId {
  if (!(value instanceof Uint8Array) || value.byteLength !== 32) throw new RangeError("commit ID must contain exactly 32 bytes");
  return value.slice() as CommitId;
}
export function idempotencyKey(value: Uint8Array): IdempotencyKey {
  if (!(value instanceof Uint8Array)) throw new TypeError("idempotency key must be bytes");
  const error = validateIdempotencyKeyRust(value);
  if (error) throw new RangeError("idempotency key is outside the Rust Stream bounds");
  return value.slice() as IdempotencyKey;
}
export type Record<Value = Uint8Array> = PublicWire<WireRecord, { readonly value: Value; readonly commitId: CommitId }>;
export type EncodedRecord = Record<Uint8Array>;
export type AppendResult =
  | (PublicWire<WireAppendReceipt, { readonly commitId: CommitId }> & { readonly ok: true })
  | (PublicWire<WireTailConflict> & { readonly ok: false; readonly code: "tail_conflict" });
export type AppendOptions = PublicWire<WireAppendRequest, { readonly idempotencyKey?: IdempotencyKey }, "path" | "records">;
export type ForkOptions = PublicWire<WireForkRequest, { readonly idempotencyKey?: IdempotencyKey }, "source" | "destination">;
export type ReadOptions = Readonly<Pick<WireReadRequest, "from" | "limit">>;
export interface FollowOptions { readonly from: Sequence; readonly signal?: AbortSignal }
export interface StreamChild { readonly path: string }
export type ChildrenPageRequest = Readonly<{ parent?: string; limit: number }> & (
  | Readonly<{ after?: undefined; hierarchyVersion?: CommitId }>
  | Readonly<{ after: string; hierarchyVersion: CommitId }>
);
export interface ChildrenPage { readonly hierarchyVersion: CommitId; readonly children: readonly StreamChild[]; readonly nextAfter?: string }
export type ForkReceipt = PublicWire<WireForkReceipt, { readonly commitId: CommitId }>;

export type CommitCondition =
  | { readonly stream: import("./client.js").Stream<unknown>; readonly ifTail: Sequence }
  | { readonly path: string; readonly ifAbsent: true };
export type CommitMutation =
  | { readonly append: { readonly stream: import("./client.js").Stream<unknown>; readonly values: readonly unknown[] } }
  | { readonly fork: { readonly source: import("./client.js").Stream<unknown>; readonly destination: string; readonly atTail: Sequence; readonly values?: readonly unknown[] } };
export type CommittedMutation =
  | (PublicWire<WireCommittedAppend, { readonly records: readonly Record<unknown>[] }> & { readonly type: Extract<CommittedKind, "append"> })
  | (PublicWire<WireCommittedFork, { readonly records: readonly Record<unknown>[] }> & { readonly type: Extract<CommittedKind, "fork"> });
export type CommittedEnvelope = PublicWire<WireCommittedEnvelope, { readonly commitId: CommitId; readonly mutations: readonly CommittedMutation[] }>;
export type CommitConflict =
  | PublicWire<WireTailCommitConflict, {
      readonly expectedTail: WireTailCommitConflict["expected"];
      readonly actualTail?: WireTailCommitConflict["actual"];
    }, "expected" | "actual">
  | (PublicWire<WireExistsCommitConflict, { readonly expectedAbsent: true }> & { readonly actual: Extract<CommitConflictKind, "exists"> });
export type CommitResult =
  | { readonly ok: true; readonly commitId: CommitId; readonly tails: Readonly<{ readonly [path: string]: Sequence }>; readonly forks: readonly { readonly path: string; readonly tail: Sequence }[] }
  | { readonly ok: false; readonly code: "conflict"; readonly conflicts: readonly CommitConflict[] };
export interface CommitRequest { readonly conditions: readonly CommitCondition[]; readonly mutations: readonly CommitMutation[] }
export interface CommitOptions { readonly idempotencyKey: IdempotencyKey; readonly deadlineUnixMillis?: bigint }
export type IdempotencyOutcome =
  | { readonly type: Extract<IdempotencyKind, "append">; readonly outcome: AppendResult }
  | { readonly type: Extract<IdempotencyKind, "fork">; readonly receipt: ForkReceipt }
  | { readonly type: Extract<IdempotencyKind, "commit">; readonly outcome: CommitResult };
export type IdempotencyObservation = PublicWire<WireIdempotencyObservation, { readonly idempotencyKey: IdempotencyKey; readonly outcome: IdempotencyOutcome }>;
export type TokenGrant = PublicWire<WireTokenGrant, { readonly operations: readonly TokenOperation[] }>;
export type CreateTokenRequest = PublicWire<WireCreateTokenRequest, { readonly allow: readonly TokenGrant[] }>;
export interface AccessToken { readonly token: string; readonly expiresAt: Date }

export interface ProviderCommitRequest {
  readonly conditions: readonly ({ readonly path: string; readonly ifTail: Sequence } | { readonly path: string; readonly ifAbsent: true })[];
  readonly mutations: readonly (
    | { readonly append: { readonly path: string; readonly values: readonly Uint8Array[] } }
    | { readonly fork: { readonly source: string; readonly destination: string; readonly atTail: Sequence; readonly values: readonly Uint8Array[] } }
  )[];
}
export interface StreamProvider {
  inspectIdempotency(idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<IdempotencyObservation | undefined>;
  tail(path: string, signal?: AbortSignal): Promise<Sequence>;
  append(path: string, values: readonly Uint8Array[], options?: AppendOptions, signal?: AbortSignal): Promise<AppendResult>;
  fork(source: string, destination: string, options?: ForkOptions, signal?: AbortSignal): Promise<ForkReceipt>;
  read(path: string, options: ReadOptions, signal?: AbortSignal): AsyncIterable<EncodedRecord>;
  follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord>;
  childrenPage(request: ChildrenPageRequest, signal?: AbortSignal): Promise<ChildrenPage>;
  commit(request: ProviderCommitRequest, options: CommitOptions, signal?: AbortSignal): Promise<CommitResult>;
  readCommit(commitId: CommitId, signal?: AbortSignal): Promise<CommittedEnvelope>;
  createToken?(request: CreateTokenRequest, signal?: AbortSignal): Promise<AccessToken>;
}

export interface StreamEnvironment {
  readonly endpoint: string;
  readonly token: string;
  /** Optional Rust-qualified transport override; omission selects the best installed transport. */
  readonly transport?: RustOwnedTransportKind;
}
export class StreamError extends Error {
  constructor(readonly code: string, message: string, readonly status?: number) { super(message); }
}
