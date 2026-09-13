/** Provider-independent hierarchical Stream v2 facade. */

export type Sequence = bigint;
export type CommitId = Uint8Array;
export type IdempotencyKey = Uint8Array;

export interface Record<T = Uint8Array> {
  readonly sequence: Sequence;
  readonly value: T;
  readonly commitId: CommitId;
}

export interface AppendReceipt {
  readonly start: Sequence;
  readonly end: Sequence;
  readonly tail: Sequence;
  readonly commitId: CommitId;
}

export type AppendOutcome =
  | { readonly ok: true; readonly receipt: AppendReceipt }
  | { readonly ok: false; readonly code: "tail_conflict"; readonly actualTail: Sequence };

export interface AppendOptions {
  readonly ifTail?: Sequence;
  readonly idempotencyKey?: IdempotencyKey;
}

export interface ForkOptions {
  readonly atTail?: Sequence;
  readonly idempotencyKey?: IdempotencyKey;
}

export interface ForkReceipt {
  readonly source: string;
  readonly destination: string;
  readonly forkedAt: Sequence;
  readonly tail: Sequence;
  readonly commitId: CommitId;
}

export interface TrimReceipt {
  readonly path: string;
  readonly trimPoint: Sequence;
  readonly commitId: CommitId;
}

export interface DeleteReceipt {
  readonly path: string;
  readonly commitId: CommitId;
}

export type CommitCondition =
  | { readonly path: string; readonly ifTail: Sequence }
  | { readonly path: string; readonly ifAbsent: true };

export type CommitMutation<T = Uint8Array> =
  | { readonly append: { readonly path: string; readonly values: readonly T[] } }
  | {
      readonly fork: {
        readonly source: string;
        readonly destination: string;
        readonly atTail: Sequence;
      };
    }
  | { readonly trim: { readonly path: string; readonly before: Sequence } }
  | { readonly delete: { readonly path: string } };

export type CommittedMutation<T = Uint8Array> =
  | {
      readonly type: "append";
      readonly path: string;
      readonly start: Sequence;
      readonly end: Sequence;
      readonly tail: Sequence;
      readonly records: readonly Record<T>[];
    }
  | {
      readonly type: "fork";
      readonly source: string;
      readonly destination: string;
      readonly forkedAt: Sequence;
      readonly tail: Sequence;
    }
  | { readonly type: "trim"; readonly path: string; readonly trimPoint: Sequence }
  | { readonly type: "delete"; readonly path: string };

export interface CommittedEnvelope<T = Uint8Array> {
  readonly commitId: CommitId;
  readonly mutations: readonly CommittedMutation<T>[];
}

export type CommitConflict =
  | {
      readonly path: string;
      readonly expectedTail: Sequence;
      readonly actualTail: Sequence;
    }
  | { readonly path: string; readonly expectedAbsent: true; readonly actual: "exists" | "retired" };

export type CommitOutcome<T = Uint8Array> =
  | { readonly ok: true; readonly envelope: CommittedEnvelope<T> }
  | { readonly ok: false; readonly code: "conflict"; readonly conflicts: readonly CommitConflict[] };

export type IdempotencyOutcome<T = Uint8Array> =
  | { readonly type: "append"; readonly outcome: AppendOutcome }
  | { readonly type: "fork"; readonly receipt: ForkReceipt }
  | { readonly type: "trim"; readonly receipt: TrimReceipt }
  | { readonly type: "delete"; readonly receipt: DeleteReceipt }
  | { readonly type: "commit"; readonly outcome: CommitOutcome<T> };

export interface IdempotencyObservation<T = Uint8Array> {
  readonly idempotencyKey: IdempotencyKey;
  readonly requestDigest: Uint8Array;
  readonly outcome: IdempotencyOutcome<T>;
}

/** One authenticated account provider. Transport, placement, and retries stay internal. */
export interface StreamProvider<T = Uint8Array> {
  inspectIdempotency(idempotencyKey: IdempotencyKey): Promise<IdempotencyObservation<T> | undefined>;
  tail(path: string): Promise<Sequence>;
  append(path: string, values: readonly T[], options?: AppendOptions): Promise<AppendOutcome>;
  fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt>;
  trim(path: string, before: Sequence, idempotencyKey?: IdempotencyKey): Promise<TrimReceipt>;
  delete(path: string, idempotencyKey?: IdempotencyKey): Promise<DeleteReceipt>;
  read(path: string, from: Sequence, limit: number): AsyncIterable<Record<T>>;
  follow(path: string, from: Sequence): AsyncIterable<Record<T>>;
  children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }>;
  commit(
    conditions: readonly CommitCondition[],
    mutations: readonly CommitMutation<T>[],
    idempotencyKey: IdempotencyKey,
  ): Promise<CommitOutcome<T>>;
  readCommit(commitId: CommitId): Promise<CommittedEnvelope<T>>;
}

/** Minimal provider-bound client. */
export class StreamClient<T = Uint8Array> {
  readonly #provider: StreamProvider<T>;

  constructor(provider: StreamProvider<T>) {
    this.#provider = provider;
  }

  stream(path: string): Stream<T> {
    return new Stream(this.#provider, path);
  }

  inspectIdempotency(idempotencyKey: IdempotencyKey): Promise<IdempotencyObservation<T> | undefined> {
    return this.#provider.inspectIdempotency(idempotencyKey);
  }

  children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }> {
    return this.#provider.children(parent, limit);
  }

  commit(
    conditions: readonly CommitCondition[],
    mutations: readonly CommitMutation<T>[],
    idempotencyKey: IdempotencyKey,
  ): Promise<CommitOutcome<T>> {
    return this.#provider.commit(conditions, mutations, idempotencyKey);
  }

  readCommit(commitId: CommitId): Promise<CommittedEnvelope<T>> {
    return this.#provider.readCommit(commitId);
  }
}

/** Handle for one permanent Stream path. */
export class Stream<T = Uint8Array> {
  readonly #provider: StreamProvider<T>;
  readonly path: string;

  constructor(provider: StreamProvider<T>, path: string) {
    this.#provider = provider;
    this.path = path;
  }

  tail(): Promise<Sequence> {
    return this.#provider.tail(this.path);
  }

  append(value: T, options?: AppendOptions): Promise<AppendOutcome> {
    return this.#provider.append(this.path, [value], options);
  }

  appendBatch(values: readonly T[], options?: AppendOptions): Promise<AppendOutcome> {
    return this.#provider.append(this.path, values, options);
  }

  async fork(destination: string, options?: ForkOptions): Promise<{ readonly stream: Stream<T>; readonly receipt: ForkReceipt }> {
    const receipt = await this.#provider.fork(this.path, destination, options);
    return { stream: new Stream(this.#provider, destination), receipt };
  }

  trim(before: Sequence, idempotencyKey?: IdempotencyKey): Promise<TrimReceipt> {
    return this.#provider.trim(this.path, before, idempotencyKey);
  }

  delete(idempotencyKey?: IdempotencyKey): Promise<DeleteReceipt> {
    return this.#provider.delete(this.path, idempotencyKey);
  }

  read(from: Sequence, limit: number): AsyncIterable<Record<T>> {
    return this.#provider.read(this.path, from, limit);
  }

  follow(from: Sequence): AsyncIterable<Record<T>> {
    return this.#provider.follow(this.path, from);
  }
}
