import { HttpStreamProvider } from "./http.js";
import { MemoryStreamProvider } from "./memory.js";
import { StreamLimit } from "../generated/proto/stream/v2/stream_pb.js";
import type {
  AccessToken, AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommitId, CommittedEnvelope, CommitOptions,
  CommitRequest, CommitResult, CreateTokenRequest, FollowOptions, ForkOptions,
  IdempotencyKey, IdempotencyObservation, ReadOptions, Record, Sequence, StreamEnvironment,
  StreamProvider,
} from "./types.js";
import { StreamError } from "./types.js";
import { isRustOwnedTransportUnavailable, selectRustOwnedTransport, STREAM_REMOTE_POLICY } from "./generated-client.js";
import { ensureStreamWasm, normalizeWireCommit, projectChildrenPage, validateChildrenPageRequest, validatePathValue, validateReadRequest, validateSequenceValue, validateRecordBatch, validateWireAppend, validateWireRequest } from "./contract.js";

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
  readonly tokens: { create(request: CreateTokenRequest, signal?: AbortSignal): Promise<AccessToken> };
  /** Creates a deterministic process-local client for tests and examples. */
  static memory(): StreamClient { return new StreamClient(new MemoryStreamProvider()); }
  /** Selects the Rust-qualified default transport for this runtime. */
  static fromEnv(environment?: Partial<StreamEnvironment>): Promise<StreamClient> {
    return createStreamClientFromEnv(environment);
  }
  constructor(readonly provider: StreamProvider) {
    this.tokens = { create: async (request, signal) => {
      if (provider.createToken === undefined) throw new StreamError("unsupported", "provider does not support token creation");
      return provider.createToken(request, signal);
    } };
  }
  json(path: string): Stream<JsonValue>;
  json<Value extends JsonValue>(path: string, parse: (value: JsonValue) => Value): Stream<Value>;
  json(path: string, parse?: (value: JsonValue) => JsonValue): Stream<JsonValue> {
    return new Stream(this.provider, path, parse === undefined ? jsonCodec() : jsonCodec(parse));
  }
  bytes(path: string): Stream<Uint8Array> { return new Stream(this.provider, path, bytesCodec); }
  inspectIdempotency(key: IdempotencyKey, signal?: AbortSignal): Promise<IdempotencyObservation | undefined> { return this.provider.inspectIdempotency(key, signal); }
  children(parent: string | undefined, options: { readonly limit: number } | number, signal?: AbortSignal): AsyncIterable<{ readonly path: string }> {
    const limit = typeof options === "number" ? options : options.limit;
    if (parent !== undefined) pathValue(parent);
    validateChildrenPageRequest({ ...(parent === undefined ? {} : { parent }), limit });
    return this.childrenAll(parent, limit, signal);
  }
  async childrenPage(request: ChildrenPageRequest, signal?: AbortSignal): Promise<ChildrenPage> {
    const authored = {
      kind: "children_page" as const,
      limit: request.limit,
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
    };
    await validateWireRequest(authored);
    const page = await projectChildrenPage(request, await this.provider.childrenPage(request, signal));
    return page;
  }
  /** Traverses all direct children, failing rather than silently skipping a concurrent hierarchy change. */
  async *childrenAll(parent?: string, limit = StreamLimit.MAX_ITEMS, signal?: AbortSignal): AsyncIterable<{ readonly path: string }> {
    let request: ChildrenPageRequest = { ...(parent === undefined ? {} : { parent }), limit };
    for (;;) {
      const page = await this.childrenPage(request, signal);
      for (const child of page.children) yield child;
      if (page.nextAfter === undefined) return;
      request = { ...(parent === undefined ? {} : { parent }), limit,
        after: page.nextAfter, hierarchyVersion: page.hierarchyVersion };
    }
  }
  async commit(request: CommitRequest, options: CommitOptions, signal?: AbortSignal): Promise<CommitResult> {
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
        const source = mutation.fork.source;
        return { fork: { source: source.path, destination: mutation.fork.destination, atTail: sequence(mutation.fork.atTail), values: (mutation.fork.values ?? []).map(value => source.encode(value)) } };
      }
      throw new StreamError("invalid_argument", "commit mutation is invalid");
    });
    return this.provider.commit(await normalizeWireCommit({ conditions, mutations }, options), options, signal);
  }
  readCommit(commitId: CommitId, signal?: AbortSignal): Promise<CommittedEnvelope> { return this.provider.readCommit(commitId, signal); }
}

/** Handle for one permanent Stream path. */
export class Stream<Value = Uint8Array> {
  static fromEnv(environment?: Partial<StreamEnvironment>): Promise<StreamClient> {
    return createStreamClientFromEnv(environment);
  }
  constructor(readonly provider: StreamProvider, readonly path: string, readonly codec: Codec<Value>) {
    pathValue(path);
  }
  encode(value: Value): Uint8Array { return this.codec.encode(value); }
  tail(signal?: AbortSignal): Promise<Sequence> { return this.provider.tail(this.path, signal); }
  append(value: Value, options?: AppendOptions, signal?: AbortSignal): Promise<AppendResult> { return this.appendBatch([value], options, signal); }
  appendBatch(values: readonly Value[], options?: AppendOptions, signal?: AbortSignal): Promise<AppendResult> {
    if (options?.ifTail !== undefined) sequence(options.ifTail);
    const records = values.map(value => this.codec.encode(value));
    try { validateRecordBatch(records); }
    catch (error) { return Promise.reject(error); }
    return this.provider.append(this.path, records, options, signal);
  }
  async fork(destination: string, options?: ForkOptions, signal?: AbortSignal): Promise<{ readonly stream: Stream<Value>; readonly tail: Sequence; readonly forkedAt: Sequence; readonly commitId: CommitId }> {
    pathValue(destination);
    if (options?.atTail !== undefined) sequence(options.atTail);
    const value = await this.provider.fork(this.path, destination, options, signal);
    return { stream: new Stream(this.provider, destination, this.codec), tail: value.tail, forkedAt: value.forkedAt, commitId: value.commitId };
  }
  async *read(options: ReadOptions, signal?: AbortSignal): AsyncIterable<Record<Value>> {
    sequence(options.from);
    validateReadRequest(this.path, options.from, options.limit);
    for await (const item of this.provider.read(this.path, options, signal)) yield { ...item, value: this.codec.decode(item.value) };
  }
  async *follow(options: FollowOptions): AsyncIterable<Record<Value>> {
    sequence(options.from);
    for await (const item of this.provider.follow(this.path, options)) yield { ...item, value: this.codec.decode(item.value) };
  }
}

async function createStreamClientFromEnv(environment?: Partial<StreamEnvironment>): Promise<StreamClient> {
  const endpoint = environment?.endpoint ?? environmentValue("ACYCLIC_STREAM_ENDPOINT");
  const token = environment?.token ?? environmentValue("ACYCLIC_API_KEY");
  const runtime = isNativeRuntime() ? "native" : "browser";
  const requested = environment?.transport;
  const selected = selectRustOwnedTransport(STREAM_REMOTE_POLICY, runtime, requested);
  if (selected === "http") {
    if (typeof globalThis.fetch !== "function") throw new StreamError("unavailable", "Stream HTTP transport requires fetch in this runtime");
    return new StreamClient(new HttpStreamProvider({ endpoint, token }));
  }
  if (selected === "grpc") {
    if (runtime !== "native") throw new StreamError("unsupported", "Stream gRPC transport requires a native Node or Bun runtime");
    try {
      // Keep the native companion out of browser bundles. The Rust policy has
      // already selected gRPC here; the adapter owns the N-API capability.
      const nativeModule = "./native.js";
      const { NativeStreamProvider } = await import(nativeModule);
      try {
        return new StreamClient(await NativeStreamProvider.connect({ endpoints: [endpoint], token }));
      } catch (error) {
        // Source checkouts may omit the optional platform companion. The
        // generated Node gRPC adapter is the same full transport contract and
        // remains the best available native implementation in that case.
        if (!isMissingNativeCompanion(error)) throw error;
        const { GrpcStreamProvider } = await import("./grpc.js");
        return new StreamClient(new GrpcStreamProvider({ endpoint, token }));
      }
    } catch (error) {
      // The default Rust policy prefers native gRPC, but an installation may
      // omit both optional native companions.  In that case the Rust policy's
      // next option is HTTP; explicit gRPC requests still fail clearly.
      if (requested === undefined && isRustOwnedTransportUnavailable(error)) {
        if (typeof globalThis.fetch !== "function") throw new StreamError("unavailable", "Stream native transport is unavailable and HTTP fallback requires fetch in this runtime");
        return new StreamClient(new HttpStreamProvider({ endpoint, token }));
      }
      const reason = error instanceof Error ? `: ${error.message}` : "";
      throw new StreamError("unavailable", `Stream native transport is unavailable in this runtime${reason}`);
    }
  }
  throw new StreamError("unsupported", `Selected Rust-qualified Stream transport is unavailable in the ${runtime} runtime`);
}

function isNativeRuntime(): boolean {
  const runtime = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof runtime.process?.versions?.node === "string" || typeof runtime.process?.versions?.bun === "string";
}

function isMissingNativeCompanion(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const code = (error as { readonly code?: unknown }).code;
  const message = (error as { readonly message?: unknown }).message;
  return code === "ERR_MODULE_NOT_FOUND" || (typeof message === "string" && message.includes("has no native companion"));
}

function sameProvider(provider: StreamProvider, stream: Stream<unknown>): void {
  if (stream.provider !== provider) throw new StreamError("provider_mismatch", "coordinated commit streams must use one provider");
}
export function pathValue(value: string): void {
  validatePathValue(value);
}
export function sequence(value: bigint): bigint {
  return validateSequenceValue(value);
}
/** Synchronous compatibility helper; provider append admission is Rust-owned. */
export function validateRecords(values: readonly Uint8Array[]): void {
  validateRecordBatch(values);
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
