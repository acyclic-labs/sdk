import { DefaultStreamProvider } from "./default.js";
import { MemoryStreamProvider } from "./memory.js";
import { StreamLimit } from "../generated/proto/stream/v2/stream_pb.js";
import type {
  AccessToken, AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommitId, CommittedEnvelope, CommitOptions,
  CommitRequest, CommitResult, CreateTokenRequest, FollowOptions, ForkOptions,
  IdempotencyKey, IdempotencyObservation, ReadOptions, Record, Sequence, StreamEnvironment,
  StreamProvider,
} from "./types.js";
import { StreamError, compareStreamPaths } from "./types.js";
import { ensureStreamWasm, normalizeWireCommit, validatePathValue, validateSequenceValue, validateWireAppend } from "./contract.js";

export interface Codec<Value> {
  encode(value: Value): Uint8Array;
  decode(value: Uint8Array): Value;
}
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [name: string]: JsonValue };
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
export const bytesCodec: Codec<Uint8Array> = Object.freeze({
  encode: (value: Uint8Array) => Uint8Array.from(value),
  decode: (value: Uint8Array) => Uint8Array.from(value),
});
export function jsonCodec(): Codec<JsonValue>;
export function jsonCodec<Value extends JsonValue>(parse: (value: JsonValue) => Value): Codec<Value>;
export function jsonCodec<Value extends JsonValue>(parse?: (value: JsonValue) => Value): Codec<JsonValue | Value> {
  return Object.freeze({
    encode(value: JsonValue | Value) {
      assertJson(value);
      return encoder.encode(JSON.stringify(value));
    },
    decode(value: Uint8Array) {
      const decoded: unknown = JSON.parse(decoder.decode(value));
      assertJson(decoded);
      return parse === undefined ? decoded : parse(decoded);
    },
  });
}

/** Account client and documented entry point. */
export class StreamClient {
  readonly tokens: { create(request: CreateTokenRequest): Promise<AccessToken> };
  /** Creates a deterministic process-local client for tests and examples. */
  static memory(): StreamClient { return new StreamClient(new MemoryStreamProvider()); }
  constructor(readonly provider: StreamProvider) {
    this.tokens = { create: async request => {
      if (provider.createToken === undefined) throw new StreamError("unsupported", "provider does not support token creation");
      return provider.createToken(structuredClone(request));
    } };
  }
  json(path: string): Stream<JsonValue>;
  json<Value extends JsonValue>(path: string, parse: (value: JsonValue) => Value): Stream<Value>;
  json(path: string, parse?: (value: JsonValue) => JsonValue): Stream<JsonValue> {
    return new Stream(this.provider, path, parse === undefined ? jsonCodec() : jsonCodec(parse));
  }
  bytes(path: string): Stream<Uint8Array> { return new Stream(this.provider, path, bytesCodec); }
  inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    return this.provider.inspectIdempotency(key.slice() as IdempotencyKey);
  }
  children(parent: string | undefined, options: { readonly limit: number } | number): AsyncIterable<{ readonly path: string }> {
    const limit = typeof options === "number" ? options : options.limit;
    if (parent !== undefined) pathValue(parent);
    positiveInteger(limit, "limit");
    if (limit > StreamLimit.MAX_ITEMS) throw new RangeError(`child page limit exceeds ${StreamLimit.MAX_ITEMS}`);
    return this.childrenAll(parent, limit);
  }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    let authored: ChildrenPageRequest;
    if (request.after === undefined) {
      authored = request.hierarchyVersion === undefined
        ? { ...(request.parent === undefined ? {} : { parent: request.parent }), limit: request.limit }
        : { ...(request.parent === undefined ? {} : { parent: request.parent }), hierarchyVersion: request.hierarchyVersion.slice() as CommitId, limit: request.limit };
    } else {
      if (request.hierarchyVersion === undefined) {
        throw new StreamError("invalid_cursor", "child continuation must name a direct child and its hierarchy revision");
      }
      authored = {
        ...(request.parent === undefined ? {} : { parent: request.parent }),
        after: request.after,
        hierarchyVersion: request.hierarchyVersion.slice() as CommitId,
        limit: request.limit,
      };
    }
    if (authored.parent !== undefined) pathValue(authored.parent);
    if (authored.after !== undefined) {
      pathValue(authored.after);
      if (authored.hierarchyVersion === undefined || directParent(authored.after) !== (authored.parent ?? "")) {
        throw new StreamError("invalid_cursor", "child continuation must name a direct child and its hierarchy revision");
      }
    }
    if (authored.hierarchyVersion !== undefined && authored.hierarchyVersion.byteLength !== 32) {
      throw new StreamError("invalid_cursor", "hierarchy version must be a commit identity");
    }
    positiveInteger(authored.limit, "limit");
    const page = await this.provider.childrenPage(authored);
    if (page.hierarchyVersion.byteLength !== 32) throw new StreamError("invalid_page", "provider returned an invalid hierarchy version");
    const expectedVersion = authored.hierarchyVersion;
    if (expectedVersion !== undefined &&
        page.hierarchyVersion.some((byte, index) => byte !== expectedVersion[index])) {
      throw new StreamError("hierarchy_changed", "provider changed hierarchy version during pagination");
    }
    if (page.children.length > authored.limit || (page.nextAfter !== undefined && page.nextAfter !== page.children.at(-1)?.path)) {
      throw new StreamError("invalid_page", "provider returned an invalid child continuation");
    }
    let previous = authored.after;
    for (const child of page.children) {
      pathValue(child.path);
      if (directParent(child.path) !== (authored.parent ?? "") ||
          (previous !== undefined && compareStreamPaths(previous, child.path) >= 0)) {
        throw new StreamError("invalid_page", "provider returned non-direct or unordered children");
      }
      previous = child.path;
    }
    if (page.nextAfter !== undefined && page.children.length === 0) {
      throw new StreamError("invalid_page", "provider returned an empty continuation page");
    }
    return page;
  }
  /** Traverses all direct children, failing rather than silently skipping a concurrent hierarchy change. */
  async *childrenAll(parent?: string, limit = StreamLimit.MAX_ITEMS): AsyncIterable<{ readonly path: string }> {
    let request: ChildrenPageRequest = { ...(parent === undefined ? {} : { parent }), limit };
    for (;;) {
      const page = await this.childrenPage(request);
      for (const child of page.children) yield child;
      if (page.nextAfter === undefined) return;
      request = { ...(parent === undefined ? {} : { parent }), limit,
        after: page.nextAfter, hierarchyVersion: page.hierarchyVersion };
    }
  }
  async commit(request: CommitRequest, options: CommitOptions): Promise<CommitResult> {
    const authoredOptions = structuredClone(options);
    const conditions = request.conditions.map(condition => {
      if ("stream" in condition) {
        sameProvider(this.provider, condition.stream);
        return { path: condition.stream.path, ifTail: sequence(condition.ifTail) };
      }
      pathValue(condition.path);
      if (condition.ifAbsent !== true) throw new StreamError("invalid_argument", "absence condition must be true");
      return { path: condition.path, ifAbsent: true as const };
    });
    const mutations = request.mutations.map(mutation => {
      if ("append" in mutation) {
        sameProvider(this.provider, mutation.append.stream);
        return { append: { path: mutation.append.stream.path, values: mutation.append.values.map(value => mutation.append.stream.encode(value)) } };
      }
      if ("fork" in mutation) {
        sameProvider(this.provider, mutation.fork.source);
        pathValue(mutation.fork.destination);
        const source = mutation.fork.source;
        return { fork: { source: source.path, destination: mutation.fork.destination, atTail: sequence(mutation.fork.atTail), values: (mutation.fork.values ?? []).map(value => source.encode(value)) } };
      }
      throw new StreamError("invalid_argument", "commit mutation is invalid");
    });
    return this.provider.commit(await normalizeWireCommit({ conditions, mutations }, authoredOptions), authoredOptions);
  }
  readCommit(commitId: CommitId): Promise<CommittedEnvelope> { return this.provider.readCommit(commitId.slice() as CommitId); }
}

/** Handle for one permanent Stream path. */
export class Stream<Value = Uint8Array> {
  static fromEnv(environment?: Partial<StreamEnvironment>): StreamClient {
    return new StreamClient(new DefaultStreamProvider({
      endpoint: environment?.endpoint ?? environmentValue("ACYCLIC_STREAM_ENDPOINT"),
      token: environment?.token ?? environmentValue("ACYCLIC_API_KEY"),
      ...(environment?.caCertificate === undefined ? {} : { caCertificate: environment.caCertificate.slice() }),
    }));
  }
  constructor(readonly provider: StreamProvider, readonly path: string, readonly codec: Codec<Value>) {
    pathValue(path);
  }
  encode(value: Value): Uint8Array { return this.codec.encode(value); }
  tail(): Promise<Sequence> { return this.provider.tail(this.path); }
  append(value: Value, options?: AppendOptions): Promise<AppendResult> { return this.appendBatch([value], options); }
  appendBatch(values: readonly Value[], options?: AppendOptions): Promise<AppendResult> {
    if (values.length < 1 || values.length > StreamLimit.MAX_ITEMS) {
      return Promise.reject(new StreamError("limit_exceeded", `append requires 1..${StreamLimit.MAX_ITEMS} records`));
    }
    if (options?.ifTail !== undefined) sequence(options.ifTail);
    const authored = options === undefined ? undefined : structuredClone(options);
    return this.provider.append(this.path, values.map(value => this.codec.encode(value).slice()), authored);
  }
  async fork(destination: string, options?: ForkOptions): Promise<{ readonly stream: Stream<Value>; readonly tail: Sequence; readonly forkedAt: Sequence; readonly commitId: CommitId }> {
    pathValue(destination);
    if (options?.atTail !== undefined) sequence(options.atTail);
    const authored = options === undefined ? undefined : structuredClone(options);
    const value = await this.provider.fork(this.path, destination, authored);
    return { stream: new Stream(this.provider, destination, this.codec), tail: value.tail, forkedAt: value.forkedAt, commitId: value.commitId };
  }
  read(options: ReadOptions): AsyncIterable<Record<Value>> {
    const authored = { from: options.from, limit: options.limit };
    return this.readAfterCapture(authored);
  }
  private async *readAfterCapture(options: ReadOptions): AsyncIterable<Record<Value>> {
    sequence(options.from);
    positiveInteger(options.limit, "limit");
    for await (const item of this.provider.read(this.path, options)) yield { ...item, value: this.codec.decode(item.value) };
  }
  follow(options: FollowOptions): AsyncIterable<Record<Value>> {
    const authored = {
      from: options.from,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    };
    return this.followAfterCapture(authored);
  }
  private async *followAfterCapture(options: FollowOptions): AsyncIterable<Record<Value>> {
    sequence(options.from);
    for await (const item of this.provider.follow(this.path, options)) yield { ...item, value: this.codec.decode(item.value) };
  }
}

function sameProvider(provider: StreamProvider, stream: Stream<unknown>): void {
  if (stream.provider !== provider) throw new StreamError("provider_mismatch", "coordinated commit streams must use one provider");
}
export function pathValue(value: string): void {
  validatePathValue(value);
}
function directParent(path: string): string { const at = path.lastIndexOf("/"); return at < 0 ? "" : path.slice(0, at); }
export function sequence(value: bigint): bigint {
  return validateSequenceValue(value);
}
export function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1 || value > StreamLimit.MAX_ITEMS) throw new RangeError(`${name} must be between 1 and ${StreamLimit.MAX_ITEMS}`);
}
/** Synchronous compatibility helper; provider append admission is Rust-owned. */
export function validateRecords(values: readonly Uint8Array[]): void {
  if (values.length < 1 || values.length > StreamLimit.MAX_ITEMS) throw new RangeError(`append requires 1..${StreamLimit.MAX_ITEMS} records`);
  for (const value of values) {
    if (!(value instanceof Uint8Array) || value.byteLength > StreamLimit.MAX_RECORD_BYTES) throw new RangeError(`record must contain at most ${StreamLimit.MAX_RECORD_BYTES} bytes`);
  }
}
export async function validateAppend(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<void> {
  // Validate path before optional fields so the canonical path error wins.
  await ensureStreamWasm();
  validatePathValue(path);
  if (options?.ifTail !== undefined) sequence(options.ifTail);
  await validateWireAppend(path, values, options);
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
