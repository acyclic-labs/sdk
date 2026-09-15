import { pathValue, sequence } from "./client.js";
import type { AccessToken, AppendOptions, AppendResult, CommitConflict, CommittedEnvelope, CommitId, CommitOptions, CommitResult, CreateTokenRequest, DeleteReceipt, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, StreamProvider, TrimReceipt } from "./types.js";
import { StreamError } from "./types.js";

export interface HttpStreamProviderOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Authenticated JSON transport. Mutation retries are deliberately the caller's decision. */
export class HttpStreamProvider implements StreamProvider {
  readonly #endpoint: string;
  readonly #token: string;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;
  constructor(options: HttpStreamProviderOptions) {
    const endpoint = new URL(options.endpoint);
    if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment");
    if (!options.token.trim()) throw new TypeError("token is required");
    this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? fetch;
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }
  inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> { return this.#request("idempotency/inspect", { idempotencyKey: key }, value => value === null ? undefined : idempotency(value)); }
  tail(path: string): Promise<number> { return this.#request("tail", { path }, value => sequence(integer(value, "tail"))); }
  append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> { return this.#request("append", { path, values: values.map(base64), options }, appendResult); }
  fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> { return this.#request("fork", { source, destination, options }, forkReceipt); }
  trim(path: string, before: number, idempotencyKey?: IdempotencyKey): Promise<TrimReceipt> { return this.#request("trim", { path, before, idempotencyKey }, trimReceipt); }
  delete(path: string, idempotencyKey?: IdempotencyKey): Promise<DeleteReceipt> { return this.#request("delete", { path, idempotencyKey }, deleteReceipt); }
  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> { for (const item of await this.#read(path, options)) yield item; }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    let next = options.from;
    while (!options.signal?.aborted) {
      const records: EncodedRecord[] = [];
      for (const item of await this.#read(path, { from: next, limit: 256 }, options.signal)) records.push(item);
      for (const item of records) { yield item; next = item.sequence + 1; }
      if (!records.length) await delay(250, options.signal);
    }
  }
  async *children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }> { for (const item of await this.#request("children", { parent, limit }, value => array(value, child))) yield item; }
  commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> { return this.#request("commit", { request: providerJson(request), options }, commitResult); }
  readCommit(commitId: CommitId): Promise<CommittedEnvelope> { return this.#request("commits/read", { commitId }, envelope); }
  createToken(request: CreateTokenRequest): Promise<AccessToken> { return this.#request("tokens/create", request, token); }
  #read(path: string, options: ReadOptions, signal?: AbortSignal): Promise<readonly EncodedRecord[]> { return this.#request("read", { path, ...options }, value => array(value, encodedRecord), signal); }
  async #request<Result>(route: string, body: unknown, project: Decoder<Result>, signal?: AbortSignal): Promise<Result> {
    const response = await this.#fetcher(new URL(`v1/stream/${route}`, this.#endpoint), { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: JSON.stringify(body), ...(signal === undefined ? {} : { signal }) });
    let text: string;
    try { text = await boundedText(response, this.#maximum); }
    catch (error) { if (error instanceof StreamError) throw error; throw new StreamError("invalid_response", `invalid ${route} response encoding: ${error instanceof Error ? error.message : String(error)}`, response.status); }
    if (!response.ok) throw new StreamError("transport", text || `HTTP ${response.status}`, response.status);
    try { return project(JSON.parse(text)); } catch (error) { throw new StreamError("invalid_response", `invalid ${route} response: ${error instanceof Error ? error.message : String(error)}`, response.status); }
  }
}

function base64(value: Uint8Array): string { let binary = ""; for (const byte of value) binary += String.fromCharCode(byte); return btoa(binary); }
function unbase64(value: string): Uint8Array { return Uint8Array.from(atob(value), character => character.charCodeAt(0)); }
const decoder = new TextDecoder("utf-8", { fatal: true });
async function boundedText(response: Response, maximum: number): Promise<string> {
  const reader = response.body?.getReader();
  if (reader === undefined) return "";
  const chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maximum) { await reader.cancel(); throw new StreamError("response_too_large", "response exceeds configured bound", response.status); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return decoder.decode(bytes);
}
async function delay(milliseconds: number, signal?: AbortSignal): Promise<void> { if (signal?.aborted) return; await new Promise<void>(resolve => { const finish = () => { clearTimeout(timeout); signal?.removeEventListener("abort", finish); resolve(); }; const timeout = setTimeout(finish, milliseconds); signal?.addEventListener("abort", finish, { once: true }); }); }
function providerJson(request: ProviderCommitRequest): unknown { return { conditions: request.conditions, mutations: request.mutations.map(mutation => "append" in mutation ? { append: { ...mutation.append, values: mutation.append.values.map(base64) } } : mutation) }; }
type Decoder<Value> = (value: unknown) => Value;
function object(value: unknown): Record<string, unknown> { if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("expected object"); return value as Record<string, unknown>; }
function text(value: unknown, name: string): string { if (typeof value !== "string" || !value) throw new TypeError(`${name} must be a non-empty string`); return value; }
function integer(value: unknown, name: string): number { if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError(`${name} must be a non-negative safe integer`); return value as number; }
function bool(value: unknown, name: string): boolean { if (typeof value !== "boolean") throw new TypeError(`${name} must be boolean`); return value; }
function array<Value>(value: unknown, item: Decoder<Value>): readonly Value[] { if (!Array.isArray(value)) throw new TypeError("expected array"); return value.map(item); }
function streamPath(value: unknown, name: string): string { const path = text(value, name); pathValue(path); return path; }
function child(value: unknown) { const item = object(value); return { path: streamPath(item.path, "path") }; }
function encodedRecord(value: unknown): EncodedRecord { const item = object(value); const encoded = text(item.value, "record.value"); let bytesValue: Uint8Array; try { bytesValue = unbase64(encoded); } catch { throw new TypeError("record.value must be base64"); } return { sequence: sequence(integer(item.sequence, "record.sequence")), value: bytesValue, commitId: text(item.commitId, "record.commitId") }; }
function appendResult(value: unknown): AppendResult { const item = object(value); const ok = bool(item.ok, "ok"); if (!ok) { if (item.code !== "tail_conflict") throw new TypeError("append conflict code is invalid"); return { ok, code: item.code, actualTail: integer(item.actualTail, "actualTail") }; } const start = integer(item.start, "start"); const end = integer(item.end, "end"); const tail = integer(item.tail, "tail"); if (start > end || end > tail) throw new TypeError("append positions must satisfy start <= end <= tail"); return { ok, start, end, tail, commitId: text(item.commitId, "commitId") }; }
function forkReceipt(value: unknown): ForkReceipt { const item = object(value); const forkedAt = integer(item.forkedAt, "forkedAt"); const tail = integer(item.tail, "tail"); if (forkedAt > tail) throw new TypeError("forkedAt must not exceed tail"); return { source: streamPath(item.source, "source"), destination: streamPath(item.destination, "destination"), forkedAt, tail, commitId: text(item.commitId, "commitId") }; }
function trimReceipt(value: unknown): TrimReceipt { const item = object(value); return { path: streamPath(item.path, "path"), trimPoint: integer(item.trimPoint, "trimPoint"), commitId: text(item.commitId, "commitId") }; }
function deleteReceipt(value: unknown): DeleteReceipt { const item = object(value); return { path: streamPath(item.path, "path"), commitId: text(item.commitId, "commitId") }; }
function conflict(value: unknown): CommitConflict { const item = object(value); const path = streamPath(item.path, "conflict.path"); if (item.expectedAbsent === true) { if (item.actual !== "exists" && item.actual !== "retired") throw new TypeError("conflict actual state is invalid"); return { path, expectedAbsent: true, actual: item.actual }; } return { path, expectedTail: integer(item.expectedTail, "expectedTail"), actualTail: integer(item.actualTail, "actualTail") }; }
function commitResult(value: unknown): CommitResult { const item = object(value); const ok = bool(item.ok, "ok"); if (!ok) { if (item.code !== "conflict") throw new TypeError("commit conflict code is invalid"); return { ok, code: item.code, conflicts: array(item.conflicts, conflict) }; } const tailsValue = object(item.tails); const tails: Record<string, number> = {}; for (const [path, tail] of Object.entries(tailsValue)) { pathValue(path); tails[path] = integer(tail, `tail ${path}`); } return { ok, commitId: text(item.commitId, "commitId"), tails, forks: array(item.forks, value => { const fork = object(value); return { path: streamPath(fork.path, "fork.path"), tail: integer(fork.tail, "fork.tail") }; }) }; }
function mutation(value: unknown): CommittedEnvelope["mutations"][number] { const item = object(value); switch (item.type) { case "append": { const path = streamPath(item.path, "path"); const start = integer(item.start, "start"); const end = integer(item.end, "end"); const tail = integer(item.tail, "tail"); if (start > end || end > tail) throw new TypeError("append positions must satisfy start <= end <= tail"); return { type: item.type, path, start, end, tail, records: array(item.records, encodedRecord) }; } case "fork": { const forkedAt = integer(item.forkedAt, "forkedAt"); const tail = integer(item.tail, "tail"); if (forkedAt > tail) throw new TypeError("forkedAt must not exceed tail"); return { type: item.type, source: streamPath(item.source, "source"), destination: streamPath(item.destination, "destination"), forkedAt, tail }; } case "trim": return { type: item.type, path: streamPath(item.path, "path"), trimPoint: integer(item.trimPoint, "trimPoint") }; case "delete": return { type: item.type, path: streamPath(item.path, "path") }; default: throw new TypeError("commit mutation type is invalid"); } }
function envelope(value: unknown): CommittedEnvelope { const item = object(value); return { commitId: text(item.commitId, "commitId"), mutations: array(item.mutations, mutation) }; }
function idempotency(value: unknown): IdempotencyObservation { const item = object(value); const outcome = object(item.outcome); let parsed: IdempotencyObservation["outcome"]; switch (outcome.type) { case "append": parsed = { type: outcome.type, outcome: appendResult(outcome.outcome) }; break; case "fork": parsed = { type: outcome.type, receipt: forkReceipt(outcome.receipt) }; break; case "trim": parsed = { type: outcome.type, receipt: trimReceipt(outcome.receipt) }; break; case "delete": parsed = { type: outcome.type, receipt: deleteReceipt(outcome.receipt) }; break; case "commit": parsed = { type: outcome.type, outcome: commitResult(outcome.outcome) }; break; default: throw new TypeError("idempotency outcome type is invalid"); } return { idempotencyKey: text(item.idempotencyKey, "idempotencyKey"), requestDigest: text(item.requestDigest, "requestDigest"), outcome: parsed }; }
function token(value: unknown): AccessToken { const item = object(value); const expiresAt = new Date(text(item.expiresAt, "expiresAt")); if (Number.isNaN(expiresAt.getTime())) throw new TypeError("expiresAt must be an ISO timestamp"); return { token: text(item.token, "token"), expiresAt }; }
