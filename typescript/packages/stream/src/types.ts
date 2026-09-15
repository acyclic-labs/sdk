declare const streamIdentityBrand: unique symbol;
/** Exact unsigned 64-bit protocol position. */
export type Sequence = bigint;
/** Opaque stable 32-byte commit identity. */
export type CommitId = Uint8Array & { readonly [streamIdentityBrand]: "CommitId" };
/** Opaque non-empty caller retry identity, bounded to 256 bytes. */
export type IdempotencyKey = Uint8Array & { readonly [streamIdentityBrand]: "IdempotencyKey" };

export function commitId(value: Uint8Array): CommitId {
  if (!(value instanceof Uint8Array) || value.byteLength !== 32) throw new RangeError("commit ID must contain exactly 32 bytes");
  return value.slice() as CommitId;
}
export function idempotencyKey(value: Uint8Array): IdempotencyKey {
  if (!(value instanceof Uint8Array) || value.byteLength < 1 || value.byteLength > 256) throw new RangeError("idempotency key must contain 1..256 bytes");
  return value.slice() as IdempotencyKey;
}

export interface Record<Value = Uint8Array> {
  readonly sequence: Sequence;
  readonly value: Value;
  readonly commitId: CommitId;
}
export interface EncodedRecord extends Record<Uint8Array> {}
export type AppendResult =
  | { readonly ok: true; readonly start: Sequence; readonly end: Sequence; readonly tail: Sequence; readonly commitId: CommitId }
  | { readonly ok: false; readonly code: "tail_conflict"; readonly actualTail: Sequence };
export interface AppendOptions { readonly ifTail?: Sequence; readonly idempotencyKey?: IdempotencyKey }
export interface ForkOptions { readonly atTail?: Sequence; readonly idempotencyKey?: IdempotencyKey }
export interface ReadOptions { readonly from: Sequence; readonly limit: number }
export interface FollowOptions { readonly from: Sequence; readonly signal?: AbortSignal }
export interface ForkReceipt { readonly source: string; readonly destination: string; readonly forkedAt: Sequence; readonly tail: Sequence; readonly commitId: CommitId }
export interface TrimReceipt { readonly path: string; readonly trimPoint: Sequence; readonly commitId: CommitId }
export interface DeleteReceipt { readonly path: string; readonly commitId: CommitId }

export type CommitCondition =
  | { readonly stream: import("./client.js").Stream<unknown>; readonly ifTail: Sequence }
  | { readonly path: string; readonly ifAbsent: true };
export type CommitMutation =
  | { readonly append: { readonly stream: import("./client.js").Stream<unknown>; readonly values: readonly unknown[] } }
  | { readonly fork: { readonly source: import("./client.js").Stream<unknown>; readonly destination: string; readonly atTail: Sequence } }
  | { readonly trim: { readonly stream: import("./client.js").Stream<unknown>; readonly before: Sequence } }
  | { readonly delete: { readonly stream: import("./client.js").Stream<unknown> } };
export type CommittedMutation =
  | { readonly type: "append"; readonly path: string; readonly start: Sequence; readonly end: Sequence; readonly tail: Sequence; readonly records: readonly Record<unknown>[] }
  | { readonly type: "fork"; readonly source: string; readonly destination: string; readonly forkedAt: Sequence; readonly tail: Sequence }
  | { readonly type: "trim"; readonly path: string; readonly trimPoint: Sequence }
  | { readonly type: "delete"; readonly path: string };
export interface CommittedEnvelope { readonly commitId: CommitId; readonly mutations: readonly CommittedMutation[] }
export type CommitConflict =
  | { readonly path: string; readonly expectedTail: Sequence; readonly actualTail: Sequence }
  | { readonly path: string; readonly expectedAbsent: true; readonly actual: "exists" | "retired" };
export type CommitResult =
  | { readonly ok: true; readonly commitId: CommitId; readonly tails: Readonly<{ readonly [path: string]: Sequence }>; readonly forks: readonly { readonly path: string; readonly tail: Sequence }[] }
  | { readonly ok: false; readonly code: "conflict"; readonly conflicts: readonly CommitConflict[] };
export interface CommitRequest { readonly conditions: readonly CommitCondition[]; readonly mutations: readonly CommitMutation[] }
export interface CommitOptions { readonly idempotencyKey: IdempotencyKey }
export type IdempotencyOutcome =
  | { readonly type: "append"; readonly outcome: AppendResult }
  | { readonly type: "fork"; readonly receipt: ForkReceipt }
  | { readonly type: "trim"; readonly receipt: TrimReceipt }
  | { readonly type: "delete"; readonly receipt: DeleteReceipt }
  | { readonly type: "commit"; readonly outcome: CommitResult };
export interface IdempotencyObservation { readonly idempotencyKey: IdempotencyKey; readonly requestDigest: Uint8Array; readonly outcome: IdempotencyOutcome }
export type TokenOperation = "list" | "read" | "follow" | "append" | "fork" | "create" | "trim" | "delete" | "commit";
export interface TokenGrant { readonly path: string; readonly subtree?: boolean; readonly operations: readonly TokenOperation[] }
export interface CreateTokenRequest { readonly expiresIn: string; readonly allow: readonly TokenGrant[] }
export interface AccessToken { readonly token: string; readonly expiresAt: Date }

export interface ProviderCommitRequest {
  readonly conditions: readonly ({ readonly path: string; readonly ifTail: Sequence } | { readonly path: string; readonly ifAbsent: true })[];
  readonly mutations: readonly (
    | { readonly append: { readonly path: string; readonly values: readonly Uint8Array[] } }
    | { readonly fork: { readonly source: string; readonly destination: string; readonly atTail: Sequence } }
    | { readonly trim: { readonly path: string; readonly before: Sequence } }
    | { readonly delete: { readonly path: string } }
  )[];
}
export interface StreamProvider {
  inspectIdempotency(idempotencyKey: IdempotencyKey): Promise<IdempotencyObservation | undefined>;
  tail(path: string): Promise<Sequence>;
  append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult>;
  fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt>;
  trim(path: string, before: Sequence, idempotencyKey?: IdempotencyKey): Promise<TrimReceipt>;
  delete(path: string, idempotencyKey?: IdempotencyKey): Promise<DeleteReceipt>;
  read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord>;
  follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord>;
  children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }>;
  commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult>;
  readCommit(commitId: CommitId): Promise<CommittedEnvelope>;
  createToken?(request: CreateTokenRequest): Promise<AccessToken>;
}

export interface StreamEnvironment { readonly endpoint: string; readonly token: string }
export class StreamError extends Error {
  constructor(readonly code: string, message: string, readonly status?: number) { super(message); }
}
