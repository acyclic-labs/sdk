import type {
  AppendReceipt as WireAppendReceipt, AppendRequest as WireAppendRequest,
  CommittedAppend as WireCommittedAppend, CommittedFork as WireCommittedFork,
  CommittedTrim as WireCommittedTrim, CommittedDelete as WireCommittedDelete,
  CommittedMutation as WireCommittedMutation, CommittedEnvelope as WireCommittedEnvelope,
  CommitResponse as WireCommitResponse, AppendResponse as WireAppendResponse,
  IdempotencyObservation as WireIdempotencyObservation,
  CommitRequest as WireCommitRequest, CommitCondition as WireCommitCondition,
  CommitMutation as WireCommitMutation, TailCondition as WireTailCondition,
  AbsentCondition as WireAbsentCondition, AppendMutation as WireAppendMutation,
  ForkMutation as WireForkMutation, TrimMutation as WireTrimMutation,
  DeleteMutation as WireDeleteMutation,
  CommitConflict as WireCommitConflict, TailCommitConflict as WireTailCommitConflict,
  ExistsCommitConflict as WireExistsCommitConflict, RetiredCommitConflict as WireRetiredCommitConflict,
  DeleteReceipt as WireDeleteReceipt, ForkReceipt as WireForkReceipt,
  ForkRequest as WireForkRequest, ReadRequest as WireReadRequest,
  Record as WireRecord, TailConflict as WireTailConflict,
  TrimReceipt as WireTrimReceipt,
  CreateTokenRequest as WireCreateTokenRequest, TokenGrant as WireTokenGrant,
} from "../generated/proto/stream/v2/stream_pb.js";
import { StreamLimit } from "../generated/proto/stream/v2/stream_pb.js";

type PublicWire<Wire, Overrides extends object = object, Omitted extends keyof Wire = never> =
  Readonly<Omit<Wire, "$typeName" | "$unknown" | keyof Overrides | Omitted> & Overrides>;
type AssertNever<Value extends never> = Value;
type CommittedKind = Exclude<WireCommittedMutation["mutation"]["case"], undefined>;
type _CommittedKinds = AssertNever<Exclude<CommittedKind, "append" | "fork" | "trim" | "delete"> | Exclude<"append" | "fork" | "trim" | "delete", CommittedKind>>;
type AppendOutcomeKind = Exclude<WireAppendResponse["outcome"]["case"], undefined>;
type _AppendOutcomes = AssertNever<Exclude<AppendOutcomeKind, "committed" | "conflict"> | Exclude<"committed" | "conflict", AppendOutcomeKind>>;
type CommitOutcomeKind = Exclude<WireCommitResponse["outcome"]["case"], undefined>;
type _CommitOutcomes = AssertNever<Exclude<CommitOutcomeKind, "committed" | "conflict"> | Exclude<"committed" | "conflict", CommitOutcomeKind>>;
type IdempotencyKind = Exclude<WireIdempotencyObservation["outcome"]["case"], undefined>;
type _IdempotencyKinds = AssertNever<Exclude<IdempotencyKind, "append" | "fork" | "trim" | "delete" | "commit"> | Exclude<"append" | "fork" | "trim" | "delete" | "commit", IdempotencyKind>>;
type ProviderConditionKind = Exclude<WireCommitCondition["condition"]["case"], undefined>;
type _ProviderConditionKinds = AssertNever<Exclude<ProviderConditionKind, "tail" | "absent"> | Exclude<"tail" | "absent", ProviderConditionKind>>;
type ProviderMutationKind = Exclude<WireCommitMutation["mutation"]["case"], undefined>;
type _ProviderMutationKinds = AssertNever<Exclude<ProviderMutationKind, "append" | "fork" | "trim" | "delete"> | Exclude<"append" | "fork" | "trim" | "delete", ProviderMutationKind>>;
type CommitConflictKind = Exclude<WireCommitConflict["conflict"]["case"], undefined>;
type _CommitConflictKinds = AssertNever<Exclude<CommitConflictKind, "tail" | "exists" | "retired"> | Exclude<"tail" | "exists" | "retired", CommitConflictKind>>;

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
  if (!(value instanceof Uint8Array) || value.byteLength < 1 || value.byteLength > StreamLimit.MAX_IDEMPOTENCY_KEY_BYTES) throw new RangeError(`idempotency key must contain 1..${StreamLimit.MAX_IDEMPOTENCY_KEY_BYTES} bytes`);
  return value.slice() as IdempotencyKey;
}
/** Rust-compatible UTF-8 path ordering for pages and cursors. */
export function compareStreamPaths(left: string, right: string): number {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  for (let index = 0; index < Math.min(leftBytes.length, rightBytes.length); index += 1) {
    if (leftBytes[index] !== rightBytes[index]) return leftBytes[index]! - rightBytes[index]!;
  }
  return leftBytes.length - rightBytes.length;
}

export type Record<Value = Uint8Array> = PublicWire<WireRecord, { readonly value: Value; readonly commitId: CommitId }>;
export type EncodedRecord = Record<Uint8Array>;
export type AppendResult =
  | (PublicWire<WireAppendReceipt, { readonly commitId: CommitId }> & { readonly ok: true })
  | (PublicWire<WireTailConflict> & { readonly ok: false; readonly code: "tail_conflict" });
export type AppendOptions = PublicWire<WireAppendRequest, { readonly idempotencyKey?: IdempotencyKey }, "path" | "records">;
export type ForkOptions = PublicWire<WireForkRequest, { readonly idempotencyKey?: IdempotencyKey }, "source" | "destination">;
export type ReadOptions = Readonly<Pick<WireReadRequest, "from" | "limit">>;
/** Atomic retained replay window for one stream. */
export interface StreamBounds { readonly trimPoint: Sequence; readonly tail: Sequence }
export interface FollowOptions { readonly from: Sequence; readonly signal?: AbortSignal }
export interface StreamChild { readonly path: string }
export type ChildrenPageRequest = Readonly<{ parent?: string; limit: number }> & (
  | Readonly<{ after?: undefined; hierarchyVersion?: CommitId }>
  | Readonly<{ after: string; hierarchyVersion: CommitId }>
);
export interface ChildrenPage { readonly hierarchyVersion: CommitId; readonly children: readonly StreamChild[]; readonly nextAfter?: string }
export type ForkReceipt = PublicWire<WireForkReceipt, { readonly commitId: CommitId }>;
export type TrimReceipt = PublicWire<WireTrimReceipt, { readonly commitId: CommitId }>;
export type DeleteReceipt = PublicWire<WireDeleteReceipt, { readonly commitId: CommitId }>;

export type CommitCondition =
  | { readonly stream: import("./client.js").Stream<unknown>; readonly ifTail: Sequence }
  | { readonly path: string; readonly ifAbsent: true };
export type CommitMutation =
  | { readonly append: { readonly stream: import("./client.js").Stream<unknown>; readonly values: readonly unknown[] } }
  | { readonly fork: { readonly source: import("./client.js").Stream<unknown>; readonly destination: string; readonly atTail: Sequence; readonly values?: readonly unknown[] } }
  | { readonly trim: { readonly stream: import("./client.js").Stream<unknown>; readonly before: Sequence } }
  | { readonly delete: { readonly stream: import("./client.js").Stream<unknown> } };
export type CommittedMutation =
  | (PublicWire<WireCommittedAppend, { readonly records: readonly Record<unknown>[] }> & { readonly type: Extract<CommittedKind, "append"> })
  | (PublicWire<WireCommittedFork, { readonly records: readonly Record<unknown>[] }> & { readonly type: Extract<CommittedKind, "fork"> })
  | (PublicWire<WireCommittedTrim> & { readonly type: Extract<CommittedKind, "trim"> })
  | (PublicWire<WireCommittedDelete> & { readonly type: Extract<CommittedKind, "delete"> });
export type CommittedEnvelope = PublicWire<WireCommittedEnvelope, { readonly commitId: CommitId; readonly mutations: readonly CommittedMutation[] }>;
export type CommitConflict =
  | PublicWire<WireTailCommitConflict, {
      readonly expectedTail: WireTailCommitConflict["expected"];
      readonly actualTail?: WireTailCommitConflict["actual"];
    }, "expected" | "actual">
  | (PublicWire<WireExistsCommitConflict, { readonly expectedAbsent: true }> & { readonly actual: Extract<CommitConflictKind, "exists"> })
  | (PublicWire<WireRetiredCommitConflict, { readonly expectedAbsent: true }> & { readonly actual: Extract<CommitConflictKind, "retired"> });
export type CommitResult =
  | { readonly ok: true; readonly commitId: CommitId; readonly tails: Readonly<{ readonly [path: string]: Sequence }>; readonly forks: readonly { readonly path: string; readonly tail: Sequence }[] }
  | { readonly ok: false; readonly code: "conflict"; readonly conflicts: readonly CommitConflict[] };
export interface CommitRequest { readonly conditions: readonly CommitCondition[]; readonly mutations: readonly CommitMutation[] }
export interface CommitOptions { readonly idempotencyKey: IdempotencyKey; readonly deadlineUnixMillis?: bigint }
export type IdempotencyOutcome =
  | { readonly type: Extract<IdempotencyKind, "append">; readonly outcome: AppendResult }
  | { readonly type: Extract<IdempotencyKind, "fork">; readonly receipt: ForkReceipt }
  | { readonly type: Extract<IdempotencyKind, "trim">; readonly receipt: TrimReceipt }
  | { readonly type: Extract<IdempotencyKind, "delete">; readonly receipt: DeleteReceipt }
  | { readonly type: Extract<IdempotencyKind, "commit">; readonly outcome: CommitResult };
export type IdempotencyObservation = PublicWire<WireIdempotencyObservation, { readonly idempotencyKey: IdempotencyKey; readonly outcome: IdempotencyOutcome }>;
export const TOKEN_OPERATIONS = ["list", "read", "follow", "append", "fork", "create", "trim", "delete", "commit"] as const;
export type TokenOperation = (typeof TOKEN_OPERATIONS)[number];
export type TokenGrant = PublicWire<WireTokenGrant, { readonly operations: readonly TokenOperation[] }>;
export type CreateTokenRequest = PublicWire<WireCreateTokenRequest, { readonly allow: readonly TokenGrant[] }>;
export interface AccessToken { readonly token: string; readonly expiresAt: Date }

export interface ProviderCommitRequest {
  readonly conditions: readonly ({ readonly path: string; readonly ifTail: Sequence } | { readonly path: string; readonly ifAbsent: true })[];
  readonly mutations: readonly (
    | { readonly append: { readonly path: string; readonly values: readonly Uint8Array[] } }
    | { readonly fork: { readonly source: string; readonly destination: string; readonly atTail: Sequence; readonly values: readonly Uint8Array[] } }
    | { readonly trim: { readonly path: string; readonly before: Sequence } }
    | { readonly delete: { readonly path: string } }
  )[];
}
export interface StreamProvider {
  inspectIdempotency(idempotencyKey: IdempotencyKey): Promise<IdempotencyObservation | undefined>;
  tail(path: string): Promise<Sequence>;
  bounds(path: string): Promise<StreamBounds>;
  append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult>;
  fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt>;
  trim(path: string, before: Sequence, idempotencyKey?: IdempotencyKey): Promise<TrimReceipt>;
  delete(path: string, idempotencyKey?: IdempotencyKey): Promise<DeleteReceipt>;
  read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord>;
  follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord>;
  childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage>;
  commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult>;
  readCommit(commitId: CommitId): Promise<CommittedEnvelope>;
  createToken?(request: CreateTokenRequest): Promise<AccessToken>;
}

export interface StreamEnvironment { readonly endpoint: string; readonly token: string }
export class StreamError extends Error {
  constructor(readonly code: string, message: string, readonly status?: number) { super(message); }
}
