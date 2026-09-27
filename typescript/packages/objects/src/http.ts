import type { BucketRef, ByteRange, Condition, HeadOptions, IdempotencyKey, ListPage, MultipartProvider, MultipartUpload, ObjectMetadata, ObjectsProvider, ObjectVersion, ReadTarget, SnapshotRef, StoredObject, UploadedPart, VersionId } from "./index.js";
import type { HttpResponseFor, HttpRoute } from "./http-contract.js";
import { decodeHttpError, decode_http_response, encode_http_request } from "../generated/wasm/acyclic_objects_wasm.js";
import { ObjectError } from "./memory.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";

export interface HttpObjectsOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Bounded managed Objects transport; mutation retries remain caller-controlled by idempotency key. */
export class HttpObjectsProvider implements ObjectsProvider, MultipartProvider {
  readonly #endpoint: string; readonly #token: string; readonly #fetcher: typeof fetch; readonly #maximum: number;
  constructor(options: HttpObjectsOptions) { const endpoint = new URL(options.endpoint); if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment"); if (!options.token.trim()) throw new TypeError("token is required"); const maximum = options.maximumResponseBytes ?? 16 * 1024 * 1024; if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new RangeError("maximumResponseBytes must be a positive safe integer"); this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`; this.#token = options.token; this.#fetcher = options.fetcher ?? fetch; this.#maximum = maximum; }
  createBucket(name: string, idempotencyKey?: IdempotencyKey) { return this.#call("buckets/create", { name, idempotencyKey }); }
  headBucket(value: BucketRef) { return this.#call("buckets/head", { bucket: value }); }
  deleteBucket(value: BucketRef, idempotencyKey?: IdempotencyKey) { return this.#call("buckets/delete", { bucket: value, idempotencyKey }); }
  put(value: BucketRef, objectKey: string, body: Uint8Array, metadata: ObjectMetadata, condition?: Condition, idempotencyKey?: IdempotencyKey) { return this.#call("objects/put", { bucket: value, objectKey, body, metadata, condition, idempotencyKey }); }
  head(target: ReadTarget, objectKey: string, options: HeadOptions = {}) { return this.#call("objects/head", { target, objectKey, ...options }); }
  get(target: ReadTarget, objectKey: string, versionId?: VersionId, range?: Omit<ByteRange, "total">) { return this.#call("objects/get", { target, objectKey, versionId, range }); }
  delete(value: BucketRef, objectKey: string, versionId?: VersionId, condition?: Condition, idempotencyKey?: IdempotencyKey) { return this.#call("objects/delete", { bucket: value, objectKey, versionId, condition, idempotencyKey }); }
  list(target: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean, pageSize: number, continuation?: string) { return this.#call("objects/list", { target, prefix, delimiter, versions, pageSize, continuation }); }
  snapshot(value: BucketRef, idempotencyKey?: IdempotencyKey) { return this.#call("snapshots/create", { bucket: value, idempotencyKey }); }
  destroySnapshot(value: SnapshotRef, idempotencyKey?: IdempotencyKey) { return this.#call("snapshots/destroy", { snapshot: value, idempotencyKey }); }
  fork(source: ReadTarget, destinationName: string, idempotencyKey?: IdempotencyKey) { return this.#call("snapshots/fork", { source, destinationName, idempotencyKey }); }
  createMultipart(value: BucketRef, objectKey: string, metadata: ObjectMetadata, condition?: Condition, idempotencyKey?: IdempotencyKey) { return this.#call("multipart/create", { bucket: value, objectKey, metadata, condition, idempotencyKey }); }
  uploadPart(upload: MultipartUpload, partNumber: number, body: Uint8Array, idempotencyKey?: IdempotencyKey) { return this.#call("multipart/upload-part", { upload, partNumber, body, idempotencyKey }); }
  listParts(upload: MultipartUpload) { return this.#call("multipart/list-parts", { upload }); }
  completeMultipart(upload: MultipartUpload, parts: readonly UploadedPart[], idempotencyKey?: IdempotencyKey) { return this.#call("multipart/complete", { upload, parts, idempotencyKey }); }
  abortMultipart(upload: MultipartUpload, idempotencyKey?: IdempotencyKey) { return this.#call("multipart/abort", { upload, idempotencyKey }); }
  async #call<Route extends HttpRoute>(route: Route, request: unknown): Promise<HttpResponseFor<Route>> {
    await ensureObjectsWasm();
    const response = await this.#fetcher(new URL(`v1/objects/${route}`, this.#endpoint), { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: encode_http_request(request) });
    const bytes = await boundedBytes(response, this.#maximum);
    let text: string;
    try { text = new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
    catch { throw new ObjectsTransportError(`invalid ${route} response: malformed UTF-8`, response.status); }
    if (!response.ok) throw hostedError(route, text, response.status);
    try {
      return decode_http_response(route, text);
    } catch (error) {
      throw new ObjectsTransportError(`invalid ${route} response: ${error instanceof Error ? error.message : String(error)}`, response.status);
    }
  }
}
export class ObjectsTransportError extends Error { constructor(message: string, readonly status: number) { super(message); } }
function hostedError(route: HttpRoute, text: string, status: number): ObjectError | ObjectsTransportError {
  const fallback = text || `HTTP ${status}`;
  const detail = decodeHttpError(route, text);
  return detail === undefined
    ? new ObjectsTransportError(fallback, status)
    : new ObjectError(detail.code, detail.message ?? fallback);
}
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
