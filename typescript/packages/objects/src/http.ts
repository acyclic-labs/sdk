import type { BucketId, BucketRef, ByteRange, Condition, ETag, HeadOptions, IdempotencyKey, ListPage, MultipartProvider, MultipartUpload, ObjectMetadata, ObjectsProvider, ObjectVersion, ReadTarget, SnapshotId, SnapshotRef, StoredObject, UploadedPart, UploadId, VersionId } from "./index.js";

export interface HttpObjectsOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Bounded managed Objects transport; mutation retries remain caller-controlled by idempotency key. */
export class HttpObjectsProvider implements ObjectsProvider, MultipartProvider {
  readonly #endpoint: string; readonly #token: string; readonly #fetcher: typeof fetch; readonly #maximum: number;
  constructor(options: HttpObjectsOptions) { const endpoint = new URL(options.endpoint); if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment"); if (!options.token.trim()) throw new TypeError("token is required"); const maximum = options.maximumResponseBytes ?? 16 * 1024 * 1024; if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new RangeError("maximumResponseBytes must be a positive safe integer"); this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`; this.#token = options.token; this.#fetcher = options.fetcher ?? fetch; this.#maximum = maximum; }
  createBucket(name: string, idempotencyKey?: IdempotencyKey) { return this.#call("buckets/create", { name, idempotencyKey }, bucket); }
  headBucket(value: BucketRef) { return this.#call("buckets/head", { bucket: value }, bucket); }
  deleteBucket(value: BucketRef, idempotencyKey?: IdempotencyKey) { return this.#call("buckets/delete", { bucket: value, idempotencyKey }, bool); }
  put(value: BucketRef, objectKey: string, body: Uint8Array, metadata: ObjectMetadata, condition?: Condition, idempotencyKey?: IdempotencyKey) { return this.#call("objects/put", { bucket: value, objectKey, body, metadata, condition, idempotencyKey }, version); }
  head(target: ReadTarget, objectKey: string, options: HeadOptions = {}) { return this.#call("objects/head", { target, objectKey, ...options }, version); }
  get(target: ReadTarget, objectKey: string, versionId?: VersionId, range?: Omit<ByteRange, "total">) { return this.#call("objects/get", { target, objectKey, versionId, range }, stored); }
  delete(value: BucketRef, objectKey: string, versionId?: VersionId, condition?: Condition, idempotencyKey?: IdempotencyKey) { return this.#call("objects/delete", { bucket: value, objectKey, versionId, condition, idempotencyKey }, deleted); }
  list(target: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean, pageSize: number, continuation?: string) { return this.#call("objects/list", { target, prefix, delimiter, versions, pageSize, continuation }, page); }
  snapshot(value: BucketRef, idempotencyKey?: IdempotencyKey) { return this.#call("snapshots/create", { bucket: value, idempotencyKey }, snapshot); }
  destroySnapshot(value: SnapshotRef, idempotencyKey?: IdempotencyKey) { return this.#call("snapshots/destroy", { snapshot: value, idempotencyKey }, bool); }
  fork(source: ReadTarget, destinationName: string, idempotencyKey?: IdempotencyKey) { return this.#call("snapshots/fork", { source, destinationName, idempotencyKey }, bucket); }
  createMultipart(value: BucketRef, objectKey: string, metadata: ObjectMetadata, condition?: Condition, idempotencyKey?: IdempotencyKey) { return this.#call("multipart/create", { bucket: value, objectKey, metadata, condition, idempotencyKey }, multipart); }
  uploadPart(upload: MultipartUpload, partNumber: number, body: Uint8Array, idempotencyKey?: IdempotencyKey) { return this.#call("multipart/upload-part", { upload, partNumber, body, idempotencyKey }, part); }
  listParts(upload: MultipartUpload) { return this.#call("multipart/list-parts", { upload }, value => array(value, part)); }
  completeMultipart(upload: MultipartUpload, parts: readonly UploadedPart[], idempotencyKey?: IdempotencyKey) { return this.#call("multipart/complete", { upload, parts, idempotencyKey }, version); }
  abortMultipart(upload: MultipartUpload, idempotencyKey?: IdempotencyKey) { return this.#call("multipart/abort", { upload, idempotencyKey }, bool); }
  async #call<Value>(route: string, request: unknown, project: Decoder<Value>): Promise<Value> { const response = await this.#fetcher(new URL(`v1/objects/${route}`, this.#endpoint), { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: encode(request) }); const bytes = await boundedBytes(response, this.#maximum); if (!response.ok) throw new ObjectsTransportError(`HTTP ${response.status}`, response.status); try { return project(decode(new TextDecoder().decode(bytes))); } catch { throw new ObjectsTransportError(`invalid ${route} response`, response.status); } }
}
export class ObjectsTransportError extends Error { constructor(message: string, readonly status: number) { super(message); } }
async function boundedBytes(response: Response, maximum: number): Promise<Uint8Array> {
  const reader = response.body?.getReader();
  if (reader === undefined) return new Uint8Array();
  const chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maximum) { await reader.cancel().catch(() => undefined); throw new ObjectsTransportError("response exceeds configured bound", response.status); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}
function encode(value: unknown): string { return JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item instanceof Uint8Array ? { $bytes: bytes(item) } : item instanceof Map ? { $map: [...item] } : item); }
function decode(value: string): unknown { return JSON.parse(value, (_key, item: unknown) => { if (item !== null && typeof item === "object" && "$bigint" in item && typeof item.$bigint === "string") return BigInt(item.$bigint); if (item !== null && typeof item === "object" && "$bytes" in item && typeof item.$bytes === "string") return fromBytes(item.$bytes); if (item !== null && typeof item === "object" && "$map" in item && Array.isArray(item.$map)) return new Map(item.$map as readonly (readonly [unknown, unknown])[]); return item; }); }
function bytes(value: Uint8Array): string { let binary = ""; for (const byte of value) binary += String.fromCharCode(byte); return btoa(binary); }
function fromBytes(value: string): Uint8Array { return Uint8Array.from(atob(value), character => character.charCodeAt(0)); }
type Decoder<Value> = (value: unknown) => Value;
function record(value: unknown): Record<string, unknown> { if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("expected object"); return value as Record<string, unknown>; }
function text(value: unknown, name: string): string { if (typeof value !== "string" || !value) throw new TypeError(`${name} must be a non-empty string`); return value; }
function bool(value: unknown): boolean { if (typeof value !== "boolean") throw new TypeError("expected boolean"); return value; }
function integer(value: unknown, name: string): number { if (!Number.isSafeInteger(value)) throw new TypeError(`${name} must be a safe integer`); return value as number; }
function array<Value>(value: unknown, item: Decoder<Value>): readonly Value[] { if (!Array.isArray(value)) throw new TypeError("expected array"); return value.map(item); }
function bucket(value: unknown): BucketRef { const item = record(value); return { bucketId: text(item.bucketId, "bucketId") as BucketId, name: text(item.name, "name") }; }
function metadata(value: unknown): ObjectMetadata { const item = record(value); if (!(item.user instanceof Map) || [...item.user].some(([key, entry]) => typeof key !== "string" || typeof entry !== "string")) throw new TypeError("metadata.user must be a string map"); return { contentType: optionalText(item.contentType, "contentType"), contentEncoding: optionalText(item.contentEncoding, "contentEncoding"), cacheControl: optionalText(item.cacheControl, "cacheControl"), contentDisposition: optionalText(item.contentDisposition, "contentDisposition"), contentLanguage: optionalText(item.contentLanguage, "contentLanguage"), expiresUnixSeconds: item.expiresUnixSeconds === undefined ? undefined : bigint(item.expiresUnixSeconds, "expiresUnixSeconds"), user: item.user }; }
function optionalText(value: unknown, name: string): string { if (value === undefined) return ""; if (typeof value !== "string") throw new TypeError(`${name} must be a string`); return value; }
function bigint(value: unknown, name: string): bigint { if (typeof value !== "bigint") throw new TypeError(`${name} must be a bigint`); return value; }
function version(value: unknown): ObjectVersion { const item = record(value); return { versionId: text(item.versionId, "versionId") as VersionId, etag: text(item.etag, "etag") as ETag, size: bigint(item.size, "size"), deleteMarker: bool(item.deleteMarker), metadata: metadata(item.metadata) }; }
function snapshot(value: unknown): SnapshotRef { const item = record(value); return { snapshotId: text(item.snapshotId, "snapshotId") as SnapshotId, sourceBucketId: text(item.sourceBucketId, "sourceBucketId") as BucketId }; }
function stored(value: unknown): StoredObject { const item = record(value); if (!(item.body instanceof Uint8Array)) throw new TypeError("body must be bytes"); const contentRange = item.contentRange === undefined ? undefined : range(item.contentRange); return { version: version(item.version), body: item.body, ...(contentRange === undefined ? {} : { contentRange }) }; }
function range(value: unknown): ByteRange { const item = record(value); const start = integer(item.start, "range.start"); const endExclusive = integer(item.endExclusive, "range.endExclusive"); const total = integer(item.total, "range.total"); if (start < 0 || endExclusive <= start || endExclusive > total) throw new TypeError("content range is invalid"); return { start, endExclusive, total }; }
function deleted(value: unknown): { readonly existed: boolean; readonly marker?: ObjectVersion } { const item = record(value); return { existed: bool(item.existed), ...(item.marker === undefined ? {} : { marker: version(item.marker) }) }; }
function page(value: unknown): ListPage { const item = record(value); return { entries: array(item.entries, entry => { const value = record(entry); return { objectKey: text(value.objectKey, "objectKey"), version: version(value.version) }; }), commonPrefixes: array(item.commonPrefixes, value => text(value, "commonPrefix")), continuation: item.continuation === undefined ? undefined : text(item.continuation, "continuation") }; }
function multipart(value: unknown): MultipartUpload { const item = record(value); return { uploadId: text(item.uploadId, "uploadId") as UploadId, bucket: bucket(item.bucket), objectKey: text(item.objectKey, "objectKey"), metadata: metadata(item.metadata) }; }
function part(value: unknown): UploadedPart { const item = record(value); return { partNumber: integer(item.partNumber, "partNumber"), etag: text(item.etag, "etag") as ETag, size: integer(item.size, "size") }; }
