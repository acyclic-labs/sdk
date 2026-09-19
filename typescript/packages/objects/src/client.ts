import { HttpObjectsProvider } from "./http.js";
import { MemoryObjectsProvider } from "./memory.js";
import type {
  BucketRef, ByteRange, Condition, IdempotencyKey, ListPage, ObjectMetadata, ObjectsProvider,
  ObjectVersion, ReadTarget, SnapshotRef, StoredObject, UploadedPart, VersionId, MultipartProvider, MultipartUpload,
} from "./index.js";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [name: string]: JsonValue };
export interface Codec<Value> { readonly mediaType: string; encode(value: Value): Uint8Array; decode(bytes: Uint8Array): Value }
export const bytesCodec: Codec<Uint8Array> = Object.freeze({ mediaType: "application/octet-stream", encode: (value: Uint8Array) => value, decode: (value: Uint8Array) => value });
export function jsonCodec<Value extends JsonValue>(): Codec<Value> {
  const encoder = new TextEncoder(); const decoder = new TextDecoder();
  return Object.freeze({
    mediaType: "application/json",
    encode(value: Value) { assertJson(value); return encoder.encode(JSON.stringify(value)); },
    decode(value: Uint8Array) { const decoded: unknown = JSON.parse(decoder.decode(value)); assertJson(decoded); return decoded as Value; },
  });
}

export interface PutOptions { readonly metadata?: Partial<ObjectMetadata>; readonly condition?: Condition; readonly idempotencyKey?: IdempotencyKey }
export interface GetOptions { readonly versionId?: VersionId; readonly range?: Omit<ByteRange, "total"> }
export interface DeleteOptions { readonly versionId?: VersionId; readonly condition?: Condition; readonly idempotencyKey?: IdempotencyKey }
export interface ListOptions { readonly prefix?: string; readonly delimiter?: string; readonly versions?: boolean; readonly pageSize?: number; readonly continuation?: string }
export interface StoredValue<Value> { readonly version: ObjectVersion; readonly value: Value; readonly contentRange?: ByteRange }
export interface ObjectsEnvironment { readonly endpoint: string; readonly token: string }
export interface MultipartOptions { readonly metadata?: Partial<ObjectMetadata>; readonly idempotencyKey?: IdempotencyKey }
export interface CompleteMultipartOptions { readonly condition?: Condition; readonly idempotencyKey?: IdempotencyKey }

const metadata = (value: Partial<ObjectMetadata> | undefined, mediaType: string): ObjectMetadata => ({
  contentType: value?.contentType ?? mediaType,
  contentEncoding: value?.contentEncoding ?? "",
  cacheControl: value?.cacheControl ?? "",
  contentDisposition: value?.contentDisposition ?? "",
  contentLanguage: value?.contentLanguage ?? "",
  expiresUnixSeconds: value?.expiresUnixSeconds,
  user: value?.user ?? new Map(),
});

export class Objects {
  /** Creates a deterministic process-local client for tests and examples. */
  static memory(): Objects { return new Objects(new MemoryObjectsProvider()); }
  constructor(readonly provider: ObjectsProvider) {}
  static fromEnv(environment?: Partial<ObjectsEnvironment>): Objects { return new Objects(new HttpObjectsProvider({ endpoint: environment?.endpoint ?? environmentValue("ACYCLIC_OBJECTS_ENDPOINT"), token: environment?.token ?? environmentValue("ACYCLIC_OBJECTS_TOKEN") })); }
  async createBucket(name: string, options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<Bucket> { return new Bucket(this.provider, await this.provider.createBucket(name, options.idempotencyKey)); }
  async bucket(reference: BucketRef): Promise<Bucket> { return new Bucket(this.provider, await this.provider.headBucket(reference)); }
  snapshot(reference: SnapshotRef): Snapshot { return new Snapshot(this.provider, reference); }
}

export class Bucket {
  readonly target: ReadTarget;
  constructor(readonly provider: ObjectsProvider, readonly reference: BucketRef) { this.target = { kind: "bucket", bucket: reference }; }
  async put<Value>(key: string, value: Value, codec: Codec<Value>, options: PutOptions = {}): Promise<ObjectVersion> { return this.provider.put(this.reference, key, codec.encode(value), metadata(options.metadata, codec.mediaType), options.condition, options.idempotencyKey); }
  async get<Value>(key: string, codec: Codec<Value>, options: GetOptions = {}): Promise<StoredValue<Value>> { const stored = await this.provider.get(this.target, key, options.versionId, options.range); return project(stored, codec); }
  head(key: string, options: Pick<GetOptions, "versionId"> = {}): Promise<ObjectVersion> { return this.provider.head(this.target, key, options.versionId); }
  delete(key: string, options: DeleteOptions = {}) { return this.provider.delete(this.reference, key, options.versionId, options.condition, options.idempotencyKey); }
  listPage(options: ListOptions = {}): Promise<ListPage> { return this.provider.list(this.target, options.prefix ?? "", options.delimiter, options.versions ?? false, options.pageSize ?? 1000, options.continuation); }
  async *pages(options: Omit<ListOptions, "continuation"> = {}): AsyncIterable<ListPage> { let continuation: string | undefined; do { const page = await this.listPage({ ...options, ...(continuation ? { continuation } : {}) }); yield page; continuation = page.continuation; } while (continuation); }
  async *list(options: Omit<ListOptions, "continuation"> = {}): AsyncIterable<ListPage["entries"][number]> { if (options.delimiter !== undefined) throw new TypeError("list() is entry-only; use pages() to retain common prefixes"); for await (const page of this.pages(options)) yield* page.entries; }
  async snapshot(options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<Snapshot> { return new Snapshot(this.provider, await this.provider.snapshot(this.reference, options.idempotencyKey)); }
  async fork(destination: string, options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<Bucket> { return new Bucket(this.provider, await this.provider.fork(this.target, destination, options.idempotencyKey)); }
  async createMultipart(key: string, options: MultipartOptions = {}): Promise<Multipart> { const provider = multipartProvider(this.provider); return new Multipart(provider, await provider.createMultipart(this.reference, key, metadata(options.metadata, "application/octet-stream"), options.idempotencyKey)); }
  deleteBucket(options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<boolean> { return this.provider.deleteBucket(this.reference, options.idempotencyKey); }
}

export class Snapshot {
  readonly target: ReadTarget;
  constructor(readonly provider: ObjectsProvider, readonly reference: SnapshotRef) { this.target = { kind: "snapshot", snapshot: reference }; }
  async get<Value>(key: string, codec: Codec<Value>, options: GetOptions = {}): Promise<StoredValue<Value>> { return project(await this.provider.get(this.target, key, options.versionId, options.range), codec); }
  head(key: string, options: Pick<GetOptions, "versionId"> = {}): Promise<ObjectVersion> { return this.provider.head(this.target, key, options.versionId); }
  listPage(options: ListOptions = {}): Promise<ListPage> { return this.provider.list(this.target, options.prefix ?? "", options.delimiter, options.versions ?? false, options.pageSize ?? 1000, options.continuation); }
  async fork(destination: string, options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<Bucket> { return new Bucket(this.provider, await this.provider.fork(this.target, destination, options.idempotencyKey)); }
  destroy(options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<boolean> { return this.provider.destroySnapshot(this.reference, options.idempotencyKey); }
}

export class Multipart {
  constructor(readonly provider: MultipartProvider, readonly upload: MultipartUpload) {}
  uploadPart(partNumber: number, body: Uint8Array, options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<UploadedPart> { return this.provider.uploadPart(this.upload, partNumber, body, options.idempotencyKey); }
  listParts(): Promise<readonly UploadedPart[]> { return this.provider.listParts(this.upload); }
  complete(parts: readonly UploadedPart[], options: CompleteMultipartOptions = {}): Promise<ObjectVersion> { return this.provider.completeMultipart(this.upload, parts, options.condition, options.idempotencyKey); }
  abort(options: { readonly idempotencyKey?: IdempotencyKey } = {}): Promise<boolean> { return this.provider.abortMultipart(this.upload, options.idempotencyKey); }
}

function project<Value>(stored: StoredObject, codec: Codec<Value>): StoredValue<Value> { return { version: stored.version, value: codec.decode(stored.body), ...(stored.contentRange ? { contentRange: stored.contentRange } : {}) }; }
function assertJson(value: unknown, seen = new Set<object>()): asserts value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") { if (!Number.isFinite(value)) throw new TypeError("JSON numbers must be finite"); return; }
  if (typeof value !== "object") throw new TypeError("value is not JSON-safe");
  if (seen.has(value)) throw new TypeError("JSON values cannot contain cycles");
  seen.add(value);
  if (Array.isArray(value)) validateJsonArray(value, seen);
  else validateJsonRecord(value, seen);
  seen.delete(value);
}
function validateJsonArray(value: readonly unknown[], seen: Set<object>): void {
  rejectJsonOverrides(value);
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const names = Object.keys(descriptors).filter(name => name !== "length");
  if (names.length !== value.length) throw new TypeError("JSON arrays must be dense and contain no extra properties");
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = descriptors[index];
    if (!descriptor || !descriptor.enumerable || !("value" in descriptor)) throw new TypeError("JSON values must not contain accessors");
    assertJson(descriptor.value, seen);
  }
}
function validateJsonRecord(value: object, seen: Set<object>): void {
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) throw new TypeError("JSON objects must be plain records");
  rejectJsonOverrides(value);
  for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(value))) {
    if (!descriptor.enumerable) throw new TypeError("JSON objects must not contain hidden properties");
    if (!("value" in descriptor)) throw new TypeError("JSON values must not contain accessors");
    assertJson(descriptor.value, seen);
  }
}
function rejectJsonOverrides(value: object): void {
  if (Object.getOwnPropertySymbols(value).length !== 0) throw new TypeError("JSON values must not contain symbol properties");
  for (let owner: object | null = value; owner !== null; owner = Object.getPrototypeOf(owner) as object | null) {
    const descriptor = Object.getOwnPropertyDescriptor(owner, "toJSON");
    if (!descriptor) continue;
    if (!("value" in descriptor) || typeof descriptor.value === "function") throw new TypeError("JSON objects must not define toJSON");
    break;
  }
}
function multipartProvider(provider: ObjectsProvider): ObjectsProvider & MultipartProvider { const value = provider as ObjectsProvider & Partial<MultipartProvider>; if (value.createMultipart === undefined || value.uploadPart === undefined || value.listParts === undefined || value.completeMultipart === undefined || value.abortMultipart === undefined) throw new TypeError("provider does not support multipart uploads"); return value as ObjectsProvider & MultipartProvider; }
function environmentValue(name: string): string { const runtime = globalThis as typeof globalThis & { process?: { env?: Readonly<Record<string, string | undefined>> } }; const value = runtime.process?.env?.[name]; if (!value?.trim()) throw new TypeError(`${name} is required`); return value; }
