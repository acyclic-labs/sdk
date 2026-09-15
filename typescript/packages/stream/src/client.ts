import { HttpStreamProvider } from "./http.js";
import type {
  AccessToken, AppendOptions, AppendResult, CommitId, CommittedEnvelope, CommitOptions,
  CommitRequest, CommitResult, CreateTokenRequest, DeleteReceipt, FollowOptions, ForkOptions,
  IdempotencyKey, IdempotencyObservation, ReadOptions, Record, Sequence, StreamEnvironment,
  StreamProvider, TrimReceipt,
} from "./types.js";
import { StreamError } from "./types.js";

export interface Codec<Value> {
  encode(value: Value): Uint8Array;
  decode(value: Uint8Array): Value;
}
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [name: string]: JsonValue };
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
export const bytesCodec: Codec<Uint8Array> = Object.freeze({
  encode: (value: Uint8Array) => value.slice(),
  decode: (value: Uint8Array) => value.slice(),
});
export function jsonCodec<Value extends JsonValue>(): Codec<Value> {
  return Object.freeze({
    encode(value: Value) {
      assertJson(value);
      return encoder.encode(JSON.stringify(value));
    },
    decode(value: Uint8Array) {
      const decoded: unknown = JSON.parse(decoder.decode(value));
      assertJson(decoded);
      return decoded as Value;
    },
  });
}

/** Account client and documented entry point. */
export class StreamClient {
  readonly tokens: { create(request: CreateTokenRequest): Promise<AccessToken> };
  constructor(readonly provider: StreamProvider) {
    this.tokens = { create: request => {
      if (provider.createToken === undefined) throw new StreamError("unsupported", "provider does not support token creation");
      return provider.createToken(request);
    } };
  }
  json<Value extends JsonValue>(path: string): Stream<Value> { return new Stream(this.provider, path, jsonCodec<Value>()); }
  bytes(path: string): Stream<Uint8Array> { return new Stream(this.provider, path, bytesCodec); }
  inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> { return this.provider.inspectIdempotency(key); }
  children(parent: string | undefined, options: { readonly limit: number } | number): AsyncIterable<{ readonly path: string }> {
    const limit = typeof options === "number" ? options : options.limit;
    positiveInteger(limit, "limit");
    return this.provider.children(parent, limit);
  }
  commit(request: CommitRequest, options: CommitOptions): Promise<CommitResult> {
    const conditions = request.conditions.map(condition => {
      if ("stream" in condition) {
        sameProvider(this.provider, condition.stream);
        return { path: condition.stream.path, ifTail: sequence(condition.ifTail) };
      }
      pathValue(condition.path);
      return condition;
    });
    const mutations = request.mutations.map(mutation => {
      if ("append" in mutation) {
        sameProvider(this.provider, mutation.append.stream);
        return { append: { path: mutation.append.stream.path, values: mutation.append.values.map(value => mutation.append.stream.encode(value)) } };
      }
      if ("fork" in mutation) {
        sameProvider(this.provider, mutation.fork.source);
        pathValue(mutation.fork.destination);
        return { fork: { source: mutation.fork.source.path, destination: mutation.fork.destination, atTail: sequence(mutation.fork.atTail) } };
      }
      if ("trim" in mutation) {
        sameProvider(this.provider, mutation.trim.stream);
        return { trim: { path: mutation.trim.stream.path, before: sequence(mutation.trim.before) } };
      }
      sameProvider(this.provider, mutation.delete.stream);
      return { delete: { path: mutation.delete.stream.path } };
    });
    return this.provider.commit({ conditions, mutations }, options);
  }
  readCommit(commitId: CommitId): Promise<CommittedEnvelope> { return this.provider.readCommit(commitId); }
}

/** Handle for one permanent Stream path. */
export class Stream<Value = Uint8Array> {
  static fromEnv(environment?: Partial<StreamEnvironment>): StreamClient {
    return new StreamClient(new HttpStreamProvider({
      endpoint: environment?.endpoint ?? environmentValue("ACYCLIC_STREAM_ENDPOINT"),
      token: environment?.token ?? environmentValue("ACYCLIC_API_KEY"),
    }));
  }
  constructor(readonly provider: StreamProvider, readonly path: string, readonly codec: Codec<Value>) {
    pathValue(path);
  }
  encode(value: Value): Uint8Array { return this.codec.encode(value); }
  tail(): Promise<Sequence> { return this.provider.tail(this.path); }
  append(value: Value, options?: AppendOptions): Promise<AppendResult> { return this.appendBatch([value], options); }
  appendBatch(values: readonly Value[], options?: AppendOptions): Promise<AppendResult> {
    if (values.length === 0) throw new RangeError("appendBatch requires at least one value");
    if (options?.ifTail !== undefined) sequence(options.ifTail);
    return this.provider.append(this.path, values.map(value => this.codec.encode(value)), options);
  }
  async fork(destination: string, options?: ForkOptions): Promise<{ readonly stream: Stream<Value>; readonly tail: Sequence; readonly forkedAt: Sequence; readonly commitId: CommitId }> {
    if (options?.atTail !== undefined) sequence(options.atTail);
    const value = await this.provider.fork(this.path, destination, options);
    return { stream: new Stream(this.provider, destination, this.codec), tail: value.tail, forkedAt: value.forkedAt, commitId: value.commitId };
  }
  trim(before: Sequence, idempotencyKey?: IdempotencyKey): Promise<TrimReceipt> { return this.provider.trim(this.path, sequence(before), idempotencyKey); }
  delete(idempotencyKey?: IdempotencyKey): Promise<DeleteReceipt> { return this.provider.delete(this.path, idempotencyKey); }
  async *read(options: ReadOptions): AsyncIterable<Record<Value>> {
    sequence(options.from);
    positiveInteger(options.limit, "limit");
    for await (const item of this.provider.read(this.path, options)) yield { ...item, value: this.codec.decode(item.value) };
  }
  async *follow(options: FollowOptions): AsyncIterable<Record<Value>> {
    sequence(options.from);
    for await (const item of this.provider.follow(this.path, options)) yield { ...item, value: this.codec.decode(item.value) };
  }
}

function sameProvider(provider: StreamProvider, stream: Stream<unknown>): void {
  if (stream.provider !== provider) throw new StreamError("provider_mismatch", "coordinated commit streams must use one provider");
}
export function pathValue(value: string): void {
  if (!value || value.startsWith("/") || value.endsWith("/") || value.includes("//") || value.split("/").some(part => part === "." || part === "..")) {
    throw new StreamError("invalid_path", "path must contain canonical non-empty segments");
  }
}
export function sequence(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("sequence must be a non-negative safe integer");
  return value;
}
export function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be a positive safe integer`);
}
function environmentValue(name: string): string {
  const runtime = globalThis as typeof globalThis & { process?: { env?: Readonly<{ [key: string]: string | undefined }> } };
  const value = runtime.process?.env?.[name];
  if (!value?.trim()) throw new StreamError("configuration", `${name} is required`);
  return value;
}
function assertJson(value: unknown, seen = new Set<object>()): asserts value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Stream JSON numbers must be finite");
    return;
  }
  if (typeof value !== "object") throw new TypeError("Stream value is not JSON-safe");
  if (seen.has(value)) throw new TypeError("Stream JSON values cannot contain cycles");
  seen.add(value);
  if (Array.isArray(value)) validateJsonArray(value, seen);
  else validateJsonRecord(value, seen);
  seen.delete(value);
}
function validateJsonArray(value: readonly unknown[], seen: Set<object>): void {
  rejectJsonOverrides(value);
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const names = Object.keys(descriptors).filter(name => name !== "length");
  if (names.length !== value.length) throw new TypeError("Stream JSON arrays must be dense and contain no extra properties");
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = descriptors[index];
    if (!descriptor || !descriptor.enumerable || !("value" in descriptor)) throw new TypeError("Stream JSON values must not contain accessors");
    assertJson(descriptor.value, seen);
  }
}
function validateJsonRecord(value: object, seen: Set<object>): void {
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) throw new TypeError("Stream JSON objects must be plain records");
  rejectJsonOverrides(value);
  for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(value))) {
    if (!descriptor.enumerable) throw new TypeError("Stream JSON objects must not contain hidden properties");
    if (!("value" in descriptor)) throw new TypeError("Stream JSON values must not contain accessors");
    assertJson(descriptor.value, seen);
  }
}
function rejectJsonOverrides(value: object): void {
  if (Object.getOwnPropertySymbols(value).length !== 0) throw new TypeError("Stream JSON values must not contain symbol properties");
  for (let owner: object | null = value; owner !== null; owner = Object.getPrototypeOf(owner) as object | null) {
    const descriptor = Object.getOwnPropertyDescriptor(owner, "toJSON");
    if (!descriptor) continue;
    if (!("value" in descriptor) || typeof descriptor.value === "function") throw new TypeError("Stream JSON objects must not define toJSON");
    break;
  }
}
