import { pathValue, sequence } from "./client.js";
import type {
  AccessToken, AppendOptions, AppendResult, CommitConflict, CommittedEnvelope, CommittedMutation,
  CommitId, CommitOptions, CommitResult, CreateTokenRequest, DeleteReceipt, EncodedRecord,
  FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation,
  IdempotencyOutcome, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider, TrimReceipt,
} from "./types.js";
import { StreamError } from "./types.js";

interface MemoryPath { records: EncodedRecord[]; trimPoint: Sequence; retired: boolean }

/** Deterministic, bounded local implementation. */
export class MemoryStreamProvider implements StreamProvider {
  readonly #paths = new Map<string, MemoryPath>();
  readonly #commits = new Map<CommitId, CommittedEnvelope>();
  readonly #idempotency = new Map<IdempotencyKey, { digest: string; outcome: IdempotencyOutcome }>();
  readonly #followers = new Map<string, Set<() => void>>();
  #nextCommit = 1;

  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const value = this.#idempotency.get(key);
    return value === undefined ? undefined : { idempotencyKey: key, requestDigest: value.digest, outcome: structuredClone(value.outcome) };
  }
  async tail(streamPath: string): Promise<Sequence> { return this.#active(streamPath).records.length; }
  async append(streamPath: string, values: readonly Uint8Array[], options: AppendOptions = {}): Promise<AppendResult> {
    return this.#replay(options.idempotencyKey, "append", { streamPath, values, ifTail: options.ifTail }, () => {
      pathValue(streamPath);
      if (!this.#paths.has(streamPath) && options.ifTail !== undefined && options.ifTail !== 0) {
        return { ok: false, code: "tail_conflict", actualTail: 0 };
      }
      const state = this.#appendable(streamPath);
      const tail = state.records.length;
      if (options.ifTail !== undefined && options.ifTail !== tail) return { ok: false, code: "tail_conflict", actualTail: tail };
      if (tail + values.length > Number.MAX_SAFE_INTEGER) throw new StreamError("sequence_exhausted", "safe-integer sequence ceiling reached");
      const commitId = this.#id();
      const start = tail;
      const records = values.map((value, index) => ({ sequence: start + index, value: value.slice(), commitId }));
      state.records.push(...records);
      const result = { ok: true, start, end: start + values.length, tail: state.records.length, commitId } as const;
      this.#commits.set(commitId, { commitId, mutations: [{ type: "append", path: streamPath, start, end: result.end, tail: result.tail, records }] });
      this.#notify(streamPath);
      return result;
    });
  }
  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    return this.#replay(options.idempotencyKey, "fork", { source, destination, atTail: options.atTail }, () => {
      pathValue(destination);
      if (this.#paths.has(destination)) throw new StreamError("destination_exists", "fork destination already exists or is retired");
      const parent = this.#active(source);
      const forkedAt = options.atTail ?? parent.records.length;
      sequence(forkedAt);
      if (forkedAt < parent.trimPoint || forkedAt > parent.records.length) throw new StreamError("prefix_not_retained", "requested prefix is not retained");
      const commitId = this.#id();
      this.#paths.set(destination, { records: parent.records.slice(0, forkedAt), trimPoint: parent.trimPoint, retired: false });
      const receipt = { source, destination, forkedAt, tail: forkedAt, commitId };
      this.#commits.set(commitId, { commitId, mutations: [{ type: "fork", source, destination, forkedAt, tail: forkedAt }] });
      return receipt;
    });
  }
  async trim(streamPath: string, before: Sequence, key?: IdempotencyKey): Promise<TrimReceipt> {
    return this.#replay(key, "trim", { streamPath, before }, () => {
      const state = this.#active(streamPath);
      sequence(before);
      if (before > state.records.length) throw new StreamError("invalid_trim", "trim point is beyond the tail");
      state.trimPoint = Math.max(state.trimPoint, before);
      const receipt = { path: streamPath, trimPoint: state.trimPoint, commitId: this.#id() };
      this.#commits.set(receipt.commitId, { commitId: receipt.commitId, mutations: [{ type: "trim", path: streamPath, trimPoint: receipt.trimPoint }] });
      return receipt;
    });
  }
  async delete(streamPath: string, key?: IdempotencyKey): Promise<DeleteReceipt> {
    return this.#replay(key, "delete", { streamPath }, () => {
      const state = this.#active(streamPath);
      state.retired = true;
      const receipt = { path: streamPath, commitId: this.#id() };
      this.#commits.set(receipt.commitId, { commitId: receipt.commitId, mutations: [{ type: "delete", path: streamPath }] });
      this.#notify(streamPath);
      return receipt;
    });
  }
  async *read(streamPath: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const state = this.#active(streamPath);
    if (options.from < state.trimPoint) throw new StreamError("cursor_trimmed", "requested sequence is no longer retained");
    for (const item of state.records.slice(options.from, options.from + options.limit)) yield clone(item);
  }
  async *follow(streamPath: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    let next = options.from;
    for (;;) {
      if (options.signal?.aborted) return;
      const state = this.#active(streamPath);
      if (next < state.trimPoint) throw new StreamError("cursor_trimmed", "requested sequence is no longer retained");
      if (next < state.records.length) { yield clone(state.records[next]!); next += 1; continue; }
      await this.#wait(streamPath, options.signal);
    }
  }
  async *children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }> {
    const prefix = parent === undefined ? "" : `${parent}/`;
    const found = new Set<string>();
    for (const [candidate, state] of [...this.#paths].sort(([left], [right]) => left.localeCompare(right))) {
      if (state.retired || !candidate.startsWith(prefix)) continue;
      const child = candidate.slice(prefix.length).split("/")[0];
      if (!child) continue;
      found.add(prefix + child);
      if (found.size === limit) break;
    }
    for (const child of found) yield { path: child };
  }
  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    return this.#replay(options.idempotencyKey, "commit", request, () => {
      const conflicts: CommitConflict[] = [];
      for (const condition of request.conditions) {
        const state = this.#paths.get(condition.path);
        if ("ifTail" in condition) {
          const actualTail = state === undefined || state.retired ? 0 : state.records.length;
          if (actualTail !== condition.ifTail) conflicts.push({ path: condition.path, expectedTail: condition.ifTail, actualTail });
        } else if (state !== undefined) conflicts.push({ path: condition.path, expectedAbsent: true, actual: state.retired ? "retired" : "exists" });
      }
      if (conflicts.length) return { ok: false, code: "conflict", conflicts };
      const commitId = this.#id();
      const mutations: CommittedMutation[] = [];
      const tails: { [path: string]: Sequence } = {};
      const forks: { path: string; tail: Sequence }[] = [];
      const before = new Map([...this.#paths].map(([path, state]) => [path, { records: state.records.map(clone), trimPoint: state.trimPoint, retired: state.retired }]));
      const changed = new Set<string>();
      try { for (const mutation of request.mutations) {
        if ("append" in mutation) {
          const state = this.#appendable(mutation.append.path);
          const start = state.records.length;
          if (start + mutation.append.values.length > Number.MAX_SAFE_INTEGER) throw new StreamError("sequence_exhausted", "safe-integer sequence ceiling reached");
          const records = mutation.append.values.map((value, index) => ({ sequence: start + index, value: value.slice(), commitId }));
          state.records.push(...records);
          tails[mutation.append.path] = state.records.length;
          mutations.push({ type: "append", path: mutation.append.path, start, end: state.records.length, tail: state.records.length, records });
          changed.add(mutation.append.path);
        } else if ("fork" in mutation) {
          pathValue(mutation.fork.destination);
          sequence(mutation.fork.atTail);
          if (this.#paths.has(mutation.fork.destination)) throw new StreamError("destination_exists", "fork destination already exists or is retired");
          const source = this.#active(mutation.fork.source);
          if (mutation.fork.atTail < source.trimPoint || mutation.fork.atTail > source.records.length) throw new StreamError("prefix_not_retained", "requested prefix is not retained");
          this.#paths.set(mutation.fork.destination, { records: source.records.slice(0, mutation.fork.atTail), trimPoint: source.trimPoint, retired: false });
          forks.push({ path: mutation.fork.destination, tail: mutation.fork.atTail });
          mutations.push({ type: "fork", source: mutation.fork.source, destination: mutation.fork.destination, forkedAt: mutation.fork.atTail, tail: mutation.fork.atTail });
        } else if ("trim" in mutation) {
          const state = this.#active(mutation.trim.path);
          sequence(mutation.trim.before);
          if (mutation.trim.before > state.records.length) throw new StreamError("invalid_trim", "trim point is beyond the tail");
          state.trimPoint = Math.max(state.trimPoint, mutation.trim.before);
          mutations.push({ type: "trim", path: mutation.trim.path, trimPoint: state.trimPoint });
        } else {
          this.#active(mutation.delete.path).retired = true;
          mutations.push({ type: "delete", path: mutation.delete.path });
          changed.add(mutation.delete.path);
        }
      } } catch (error) { this.#paths.clear(); for (const [path, state] of before) this.#paths.set(path, state); this.#nextCommit -= 1; throw error; }
      this.#commits.set(commitId, { commitId, mutations });
      for (const path of changed) this.#notify(path);
      return { ok: true, commitId, tails, forks };
    });
  }
  async readCommit(commitId: CommitId): Promise<CommittedEnvelope> {
    const value = this.#commits.get(commitId);
    if (value === undefined) throw new StreamError("commit_not_found", "commit is unavailable");
    return structuredClone(value);
  }
  async createToken(request: CreateTokenRequest): Promise<AccessToken> {
    if (!/^\d+[smhd]$/.test(request.expiresIn) || request.allow.length === 0) throw new StreamError("invalid_token", "token expiry and grants are required");
    return { token: `memory.${crypto.randomUUID()}`, expiresAt: new Date(Date.now() + duration(request.expiresIn)) };
  }
  #active(streamPath: string): MemoryPath {
    pathValue(streamPath);
    const value = this.#paths.get(streamPath);
    if (value === undefined || value.retired) throw new StreamError("stream_not_found", "Stream path does not exist");
    return value;
  }
  #appendable(streamPath: string): MemoryPath {
    pathValue(streamPath);
    const value = this.#paths.get(streamPath);
    if (value?.retired) throw new StreamError("stream_retired", "Stream path is permanently retired");
    if (value) return value;
    const created = { records: [], trimPoint: 0, retired: false };
    this.#paths.set(streamPath, created);
    return created;
  }
  #id(): CommitId { return `commit_${this.#nextCommit++}`; }
  #replay<Result>(key: IdempotencyKey | undefined, type: IdempotencyOutcome["type"], intent: unknown, execute: () => Result): Result {
    if (key === undefined) return execute();
    if (key === "") throw new StreamError("invalid_idempotency_key", "idempotency key is empty");
    const digest = canonical(intent);
    const previous = this.#idempotency.get(key);
    if (previous) {
      if (previous.digest !== digest || previous.outcome.type !== type) throw new StreamError("idempotency_mismatch", "idempotency key is bound to another request");
      return structuredClone("outcome" in previous.outcome ? previous.outcome.outcome : previous.outcome.receipt) as Result;
    }
    const result = execute();
    const outcome = (type === "append" || type === "commit" ? { type, outcome: result } : { type, receipt: result }) as IdempotencyOutcome;
    this.#idempotency.set(key, { digest, outcome: structuredClone(outcome) });
    return result;
  }
  #notify(streamPath: string): void {
    for (const wake of this.#followers.get(streamPath) ?? []) wake();
    this.#followers.delete(streamPath);
  }
  #wait(streamPath: string, signal?: AbortSignal): Promise<void> {
    return new Promise(resolve => {
      const wakes = this.#followers.get(streamPath) ?? new Set();
      const wake = () => {
        signal?.removeEventListener("abort", wake);
        wakes.delete(wake);
        if (wakes.size === 0) this.#followers.delete(streamPath);
        resolve();
      };
      wakes.add(wake);
      this.#followers.set(streamPath, wakes);
      if (signal?.aborted) wake();
      else signal?.addEventListener("abort", wake, { once: true });
    });
  }
}

function clone(item: EncodedRecord): EncodedRecord { return { ...item, value: item.value.slice() }; }
function canonical(value: unknown): string {
  if (value instanceof Uint8Array) return `{"$bytes":${JSON.stringify(base64(value))}}`;
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value).sort(([left], [right]) => left.localeCompare(right)).map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
function base64(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);
  return btoa(binary);
}
function duration(value: string): number {
  return Number(value.slice(0, -1)) * ({ s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[value.at(-1)!] ?? 0);
}
