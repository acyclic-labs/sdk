import { pathValue, sequence } from "./client.js";
import type {
  AccessToken, AppendOptions, AppendResult, CommitConflict, CommittedEnvelope, CommittedMutation,
  CommitId, CommitOptions, CommitResult, CreateTokenRequest, DeleteReceipt, EncodedRecord,
  FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation,
  IdempotencyOutcome, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider, TrimReceipt,
} from "./types.js";
import { StreamError, commitId as validateCommitId, idempotencyKey as validateIdempotencyKey } from "./types.js";

interface MemoryPath { records: EncodedRecord[]; trimPoint: Sequence; retired: boolean }

/** Deterministic, bounded local implementation. */
export class MemoryStreamProvider implements StreamProvider {
  readonly #paths = new Map<string, MemoryPath>();
  readonly #commits = new Map<string, CommittedEnvelope>();
  readonly #idempotency = new Map<string, { key: IdempotencyKey; digest: string; outcome: IdempotencyOutcome }>();
  readonly #followers = new Map<string, Set<() => void>>();
  #mutationTail: Promise<void> = Promise.resolve();
  #nextCommit = 1n;

  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const value = this.#idempotency.get(bytesKey(validateIdempotencyKey(key)));
    return value === undefined ? undefined : { idempotencyKey: value.key.slice() as IdempotencyKey, requestDigest: await sha256(new TextEncoder().encode(value.digest)), outcome: structuredClone(value.outcome) };
  }
  async tail(streamPath: string): Promise<Sequence> { return BigInt(this.#active(streamPath).records.length); }
  async append(streamPath: string, values: readonly Uint8Array[], options: AppendOptions = {}): Promise<AppendResult> {
    return this.#replay(options.idempotencyKey, "append", { streamPath, values, ifTail: options.ifTail }, async digest => {
      pathValue(streamPath);
      if (!this.#paths.has(streamPath) && options.ifTail !== undefined && options.ifTail !== 0n) {
        return { ok: false, code: "tail_conflict", actualTail: 0n };
      }
      const state = this.#appendable(streamPath);
      const tail = BigInt(state.records.length);
      if (options.ifTail !== undefined && options.ifTail !== tail) return { ok: false, code: "tail_conflict", actualTail: tail };
      const commitId = await this.#id(digest);
      const start = tail;
      const records = values.map((value, index) => ({ sequence: start + BigInt(index), value: value.slice(), commitId: commitId.slice() as CommitId }));
      state.records.push(...records);
      const result = { ok: true, start, end: start + BigInt(values.length), tail: BigInt(state.records.length), commitId: commitId.slice() as CommitId } as const;
      this.#commits.set(bytesKey(commitId), { commitId: commitId.slice() as CommitId, mutations: [{ type: "append", path: streamPath, start, end: result.end, tail: result.tail, records }] });
      this.#notify(streamPath);
      return result;
    });
  }
  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    return this.#replay(options.idempotencyKey, "fork", { source, destination, atTail: options.atTail }, async digest => {
      pathValue(destination);
      if (this.#paths.has(destination)) throw new StreamError("destination_exists", "fork destination already exists or is retired");
      const parent = this.#active(source);
      const forkedAt = options.atTail ?? BigInt(parent.records.length);
      sequence(forkedAt);
      if (forkedAt < parent.trimPoint || forkedAt > BigInt(parent.records.length)) throw new StreamError("prefix_not_retained", "requested prefix is not retained");
      const commitId = await this.#id(digest);
      this.#paths.set(destination, { records: parent.records.slice(0, index(forkedAt)), trimPoint: parent.trimPoint, retired: false });
      const receipt = { source, destination, forkedAt, tail: forkedAt, commitId: commitId.slice() as CommitId };
      this.#commits.set(bytesKey(commitId), { commitId: commitId.slice() as CommitId, mutations: [{ type: "fork", source, destination, forkedAt, tail: forkedAt, records: [] }] });
      return receipt;
    });
  }
  async trim(streamPath: string, before: Sequence, key?: IdempotencyKey): Promise<TrimReceipt> {
    return this.#replay(key, "trim", { streamPath, before }, async digest => {
      const state = this.#active(streamPath);
      sequence(before);
      if (before > BigInt(state.records.length)) throw new StreamError("invalid_trim", "trim point is beyond the tail");
      if (before > state.trimPoint) state.trimPoint = before;
      const receipt = { path: streamPath, trimPoint: state.trimPoint, commitId: await this.#id(digest) };
      this.#commits.set(bytesKey(receipt.commitId), { commitId: receipt.commitId.slice() as CommitId, mutations: [{ type: "trim", path: streamPath, trimPoint: receipt.trimPoint }] });
      return receipt;
    });
  }
  async delete(streamPath: string, key?: IdempotencyKey): Promise<DeleteReceipt> {
    return this.#replay(key, "delete", { streamPath }, async digest => {
      const state = this.#active(streamPath);
      state.retired = true;
      const receipt = { path: streamPath, commitId: await this.#id(digest) };
      this.#commits.set(bytesKey(receipt.commitId), { commitId: receipt.commitId.slice() as CommitId, mutations: [{ type: "delete", path: streamPath }] });
      this.#notify(streamPath);
      return receipt;
    });
  }
  async *read(streamPath: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const state = this.#active(streamPath);
    if (options.from < state.trimPoint) throw new StreamError("cursor_trimmed", "requested sequence is no longer retained");
    const from = index(options.from);
    for (const item of state.records.slice(from, from + options.limit)) yield clone(item);
  }
  async *follow(streamPath: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    let next = options.from;
    for (;;) {
      if (options.signal?.aborted) return;
      const state = this.#active(streamPath);
      if (next < state.trimPoint) throw new StreamError("cursor_trimmed", "requested sequence is no longer retained");
      if (next < BigInt(state.records.length)) { yield clone(state.records[index(next)]!); next += 1n; continue; }
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
    return this.#replay(options.idempotencyKey, "commit", request, async digest => {
      const conflicts: CommitConflict[] = [];
      for (const condition of request.conditions) {
        const state = this.#paths.get(condition.path);
        if ("ifTail" in condition) {
          const actualTail = state === undefined || state.retired ? 0n : BigInt(state.records.length);
          if (actualTail !== condition.ifTail) conflicts.push({ path: condition.path, expectedTail: condition.ifTail, actualTail });
        } else if (state !== undefined) conflicts.push({ path: condition.path, expectedAbsent: true, actual: state.retired ? "retired" : "exists" });
      }
      if (conflicts.length) return { ok: false, code: "conflict", conflicts };
      const commitId = await this.#id(digest);
      const mutations: CommittedMutation[] = [];
      const tails: { [path: string]: Sequence } = {};
      const forks: { path: string; tail: Sequence }[] = [];
      const before = new Map([...this.#paths].map(([path, state]) => [path, { records: state.records.map(clone), trimPoint: state.trimPoint, retired: state.retired }]));
      const changed = new Set<string>();
      try { for (const mutation of request.mutations) {
        if ("append" in mutation) {
          const state = this.#appendable(mutation.append.path);
          const start = BigInt(state.records.length);
          const records = mutation.append.values.map((value, itemIndex) => ({ sequence: start + BigInt(itemIndex), value: value.slice(), commitId: commitId.slice() as CommitId }));
          state.records.push(...records);
          tails[mutation.append.path] = BigInt(state.records.length);
          mutations.push({ type: "append", path: mutation.append.path, start, end: BigInt(state.records.length), tail: BigInt(state.records.length), records });
          changed.add(mutation.append.path);
        } else if ("fork" in mutation) {
          pathValue(mutation.fork.destination);
          sequence(mutation.fork.atTail);
          if (this.#paths.has(mutation.fork.destination)) throw new StreamError("destination_exists", "fork destination already exists or is retired");
          const source = this.#active(mutation.fork.source);
          if (mutation.fork.atTail < source.trimPoint || mutation.fork.atTail > BigInt(source.records.length)) throw new StreamError("prefix_not_retained", "requested prefix is not retained");
          const appended = mutation.fork.values.map((value, itemIndex) => ({ sequence: mutation.fork.atTail + BigInt(itemIndex), value: value.slice(), commitId: commitId.slice() as CommitId }));
          const state = { records: [...source.records.slice(0, index(mutation.fork.atTail)), ...appended], trimPoint: source.trimPoint, retired: false };
          this.#paths.set(mutation.fork.destination, state);
          const tail = BigInt(state.records.length);
          forks.push({ path: mutation.fork.destination, tail });
          mutations.push({ type: "fork", source: mutation.fork.source, destination: mutation.fork.destination, forkedAt: mutation.fork.atTail, tail, records: appended });
          changed.add(mutation.fork.destination);
        } else if ("trim" in mutation) {
          const state = this.#active(mutation.trim.path);
          sequence(mutation.trim.before);
          if (mutation.trim.before > BigInt(state.records.length)) throw new StreamError("invalid_trim", "trim point is beyond the tail");
          if (mutation.trim.before > state.trimPoint) state.trimPoint = mutation.trim.before;
          mutations.push({ type: "trim", path: mutation.trim.path, trimPoint: state.trimPoint });
        } else {
          this.#active(mutation.delete.path).retired = true;
          mutations.push({ type: "delete", path: mutation.delete.path });
          changed.add(mutation.delete.path);
        }
      } } catch (error) { this.#paths.clear(); for (const [path, state] of before) this.#paths.set(path, state); this.#nextCommit -= 1n; throw error; }
      this.#commits.set(bytesKey(commitId), { commitId: commitId.slice() as CommitId, mutations });
      for (const path of changed) this.#notify(path);
      return { ok: true, commitId, tails, forks };
    });
  }
  async readCommit(commitId: CommitId): Promise<CommittedEnvelope> {
    const value = this.#commits.get(bytesKey(validateCommitId(commitId)));
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
    const created: MemoryPath = { records: [], trimPoint: 0n, retired: false };
    this.#paths.set(streamPath, created);
    return created;
  }
  async #id(digest: string): Promise<CommitId> {
    if (this.#nextCommit > 0xffff_ffff_ffff_ffffn) throw new StreamError("identity_exhausted", "commit identity space is exhausted");
    const decision = new Uint8Array(8);
    new DataView(decision.buffer).setBigUint64(0, this.#nextCommit++, true);
    const domain = new TextEncoder().encode("acyclic.stream.commit.v1\0");
    return validateCommitId(await sha256(join(domain, decision, await sha256(new TextEncoder().encode(digest)))));
  }
  async #replay<Result>(key: IdempotencyKey | undefined, type: IdempotencyOutcome["type"], intent: unknown, execute: (digest: string) => Result | Promise<Result>): Promise<Result> {
    const prior = this.#mutationTail;
    let release!: () => void;
    this.#mutationTail = new Promise<void>(resolve => { release = resolve; });
    await prior;
    try {
      const digest = canonical(intent);
      if (key === undefined) return await execute(digest);
      if (!(key instanceof Uint8Array) || key.byteLength < 1 || key.byteLength > 256) throw new StreamError("invalid_idempotency_key", "idempotency key must contain 1..256 bytes");
      const identity = bytesKey(key);
      const previous = this.#idempotency.get(identity);
      if (previous) {
        if (previous.digest !== digest || previous.outcome.type !== type) throw new StreamError("idempotency_mismatch", "idempotency key is bound to another request");
        return structuredClone("outcome" in previous.outcome ? previous.outcome.outcome : previous.outcome.receipt) as Result;
      }
      const result = await execute(digest);
      const outcome = (type === "append" || type === "commit" ? { type, outcome: result } : { type, receipt: result }) as IdempotencyOutcome;
      this.#idempotency.set(identity, { key: key.slice() as IdempotencyKey, digest, outcome: structuredClone(outcome) });
      return result;
    } finally { release(); }
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

function clone(item: EncodedRecord): EncodedRecord { return { ...item, value: item.value.slice(), commitId: item.commitId.slice() as CommitId }; }
function canonical(value: unknown): string {
  if (value instanceof Uint8Array) return `{"$bytes":${JSON.stringify(base64(value))}}`;
  if (typeof value === "bigint") return `{"$uint64":${JSON.stringify(value.toString())}}`;
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value).sort(([left], [right]) => left.localeCompare(right)).map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
function index(value: Sequence): number {
  sequence(value);
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new StreamError("sequence_unavailable", "the in-memory provider cannot index beyond JavaScript's exact array range");
  return Number(value);
}
function bytesKey(value: Uint8Array): string { return base64(value); }
async function sha256(value: Uint8Array): Promise<Uint8Array> {
  const copied = new Uint8Array(value.byteLength);
  copied.set(value);
  return new Uint8Array(await crypto.subtle.digest("SHA-256", copied.buffer));
}
function join(...values: readonly Uint8Array[]): Uint8Array {
  const result = new Uint8Array(values.reduce((length, value) => length + value.byteLength, 0));
  let offset = 0;
  for (const value of values) { result.set(value, offset); offset += value.byteLength; }
  return result;
}
function base64(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);
  return btoa(binary);
}
function duration(value: string): number {
  return Number(value.slice(0, -1)) * ({ s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[value.at(-1)!] ?? 0);
}
