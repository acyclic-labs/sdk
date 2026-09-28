import { create, toBinary } from "@bufbuild/protobuf";
import {
  AbortMultipartRequestSchema, CreateBucketRequestSchema, CreateMultipartRequestSchema,
  CreateSnapshotRequestSchema, DeleteBucketRequestSchema, DeleteObjectRequestSchema,
  DestroySnapshotRequestSchema, ForkBucketRequestSchema, ForkSnapshotRequestSchema,
  GetObjectRequestSchema, HeadBucketRequestSchema, HeadObjectRequestSchema, ListObjectsRequestSchema,
  ListPartsRequestSchema, ListingMode, ObjectMetadataSchema, PreconditionsSchema,
  PutObjectHeaderSchema, CompleteMultipartRequestSchema, UploadPartHeaderSchema,
} from "../generated/proto/objects/v1/objects_pb.js";
import type { MemoryResponseFor, MemoryResponseOperation, ObjectsErrorCode as WasmObjectsErrorCode } from "../generated/wasm/acyclic_objects_wasm.js";
import { is_objects_error_code, projectMemoryResponse, WasmMemoryObjects } from "../generated/wasm/acyclic_objects_wasm.js";
import { ensureObjectsWasm, objectsLimits } from "./wasm-runtime.js";
import type { BucketRef, ByteRange, Condition, HeadOptions, IdempotencyKey, ListPage, MultipartProvider, MultipartUpload, ObjectMetadata, ObjectsProvider, ObjectVersion, ReadTarget, SnapshotRef, StoredObject, UploadedPart, VersionId, UploadId } from "./index.js";

/** Objects provider backed by the canonical Rust reference implementation. */
export class MemoryObjectsProvider implements ObjectsProvider, MultipartProvider {
  readonly #inner: Promise<WasmMemoryObjects>;
  constructor(maximumBytes = 64n * 1024n * 1024n) { this.#inner = ensureObjectsWasm().then(() => new WasmMemoryObjects(maximumBytes)); }
  async createBucket(name: string, key?: IdempotencyKey): Promise<BucketRef> { const b = await this.#call("create_bucket", CreateBucketRequestSchema, { name, mutation: mutation(key) }); return this.#project("create_bucket", b); }
  async headBucket(bucket: BucketRef): Promise<BucketRef> { const b = await this.#call("head_bucket", HeadBucketRequestSchema, { bucket: wireBucket(bucket) }); return this.#project("head_bucket", b); }
  async deleteBucket(bucket: BucketRef, key?: IdempotencyKey): Promise<boolean> { const b = await this.#call("delete_bucket", DeleteBucketRequestSchema, { bucket: wireBucket(bucket), mutation: mutation(key) }); return (await this.#project("delete_bucket", b)).existed; }
  async put(bucket: BucketRef, objectKey: string, body: Uint8Array, metadata: ObjectMetadata, condition?: Condition, key?: IdempotencyKey): Promise<ObjectVersion> { const b = await this.#call("put", PutObjectHeaderSchema, { bucket: wireBucket(bucket), objectKey, metadata: wireMetadata(metadata), preconditions: preconditions(condition), mutation: mutation(key) }, body); return this.#project("put", b); }
  async head(target: ReadTarget, objectKey: string, options: HeadOptions = {}): Promise<ObjectVersion> { const b = await this.#call("head", HeadObjectRequestSchema, { target: wireTarget(target), objectKey, versionId: options.versionId ?? "", ifMatch: options.ifMatch ?? "", ifNoneMatch: options.ifNoneMatch ?? "" }); return this.#project("head", b); }
  async get(target: ReadTarget, objectKey: string, versionId?: VersionId, range?: Omit<ByteRange, "total">): Promise<StoredObject> {
    if (range && (!Number.isSafeInteger(range.start) || !Number.isSafeInteger(range.endExclusive) || range.start < 0 || range.endExclusive <= range.start)) throw new ObjectError("invalid_range", "range is outside the object");
    const request = create(GetObjectRequestSchema, { target: wireTarget(target), objectKey, versionId: versionId ?? "", rangeStart: BigInt(range?.start ?? 0), rangeEndInclusive: range ? BigInt(range.endExclusive - 1) : undefined, ifMatch: "", ifNoneMatch: "", rangeRequested: range !== undefined });
    try {
      const result = await (await this.#inner).get(toBinary(GetObjectRequestSchema, request));
      try {
        const version = await this.#project("get_version", result.version);
        const body = result.body;
        if (range && version.size > BigInt(Number.MAX_SAFE_INTEGER)) {
          throw new ObjectError("invalid_range", "object size exceeds the exact JavaScript range");
        }
        return range ? { version, body, contentRange: { start: range.start, endExclusive: range.endExclusive, total: Number(version.size) } } : { version, body };
      } finally { result.free(); }
    } catch (error) { if (error instanceof ObjectError) throw error; throw objectError(error); }
  }
  async delete(bucket: BucketRef, objectKey: string, versionId?: VersionId, condition?: Condition, key?: IdempotencyKey): Promise<{ readonly existed: boolean; readonly marker?: ObjectVersion }> { const b = await this.#call("delete", DeleteObjectRequestSchema, { bucket: wireBucket(bucket), objectKey, versionId: versionId ?? "", preconditions: preconditions(condition), mutation: mutation(key) }); return this.#project("delete", b); }
  async list(target: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean, pageSize: number, continuation?: string): Promise<ListPage> { const { listPageEntries } = await objectsLimits(); if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > listPageEntries) throw new ObjectError("invalid_page_size", `page size must be 1..${listPageEntries}`); const b = await this.#call("list", ListObjectsRequestSchema, { target: wireTarget(target), prefix, delimiter: delimiter ?? "", mode: versions ? ListingMode.VERSIONS : ListingMode.CURRENT, pageSize, continuationToken: continuation ?? "" }); return this.#project("list", b); }
  async snapshot(bucket: BucketRef, key?: IdempotencyKey): Promise<SnapshotRef> { const b = await this.#call("snapshot", CreateSnapshotRequestSchema, { bucket: wireBucket(bucket), mutation: mutation(key) }); return this.#project("snapshot", b); }
  async destroySnapshot(snapshot: SnapshotRef, key?: IdempotencyKey): Promise<boolean> { const b = await this.#call("destroy_snapshot", DestroySnapshotRequestSchema, { snapshot: wireSnapshot(snapshot), mutation: mutation(key) }); return (await this.#project("destroy_snapshot", b)).existed; }
  async fork(source: ReadTarget, destinationName: string, key?: IdempotencyKey): Promise<BucketRef> { const schema = source.kind === "bucket" ? ForkBucketRequestSchema : ForkSnapshotRequestSchema; const operation = source.kind === "bucket" ? "fork_bucket" : "fork_snapshot"; const request = source.kind === "bucket" ? { source: wireBucket(source.bucket), destinationName, mutation: mutation(key) } : { snapshot: wireSnapshot(source.snapshot), destinationName, mutation: mutation(key) }; const b = await this.#call(operation, schema, request); return this.#project(operation, b); }
  async createMultipart(bucket: BucketRef, objectKey: string, metadata: ObjectMetadata, condition?: Condition, key?: IdempotencyKey): Promise<MultipartUpload> { const retained = { ...metadata, user: new Map(metadata.user) }; const b = await this.#call("create_multipart", CreateMultipartRequestSchema, { bucket: wireBucket(bucket), objectKey, metadata: wireMetadata(retained), preconditions: preconditions(condition), mutation: mutation(key) }); const upload = await this.#project("create_multipart", b); return { uploadId: trustedString<UploadId>(upload.uploadId), bucket: { ...bucket }, objectKey, metadata: retained }; }
  async uploadPart(upload: MultipartUpload, partNumber: number, body: Uint8Array, key?: IdempotencyKey): Promise<UploadedPart> { const { multipartParts } = await objectsLimits(); if (!Number.isSafeInteger(partNumber) || partNumber < 1 || partNumber > multipartParts) throw new ObjectError("invalid_part", `part number must be 1..${multipartParts}`); const b = await this.#call("upload_part", UploadPartHeaderSchema, { bucket: wireBucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId, partNumber, mutation: mutation(key) }, body); return this.#project("upload_part", b); }
  async listParts(upload: MultipartUpload): Promise<readonly UploadedPart[]> { const b = await this.#call("list_parts", ListPartsRequestSchema, { bucket: wireBucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId }); return this.#project("list_parts", b); }
  async completeMultipart(upload: MultipartUpload, parts: readonly UploadedPart[], key?: IdempotencyKey): Promise<ObjectVersion> { const b = await this.#call("complete_multipart", CompleteMultipartRequestSchema, { bucket: wireBucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId, parts: parts.map(part => ({ partNumber: part.partNumber, etag: part.etag, size: part.size })), mutation: mutation(key) }); return this.#project("complete_multipart", b); }
  async abortMultipart(upload: MultipartUpload, key?: IdempotencyKey): Promise<boolean> { const b = await this.#call("abort_multipart", AbortMultipartRequestSchema, { bucket: wireBucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId, mutation: mutation(key) }); return (await this.#project("abort_multipart", b)).existed; }
  async #project<Operation extends MemoryResponseOperation>(operation: Operation, response: Uint8Array): Promise<MemoryResponseFor<Operation>> {
    try { return projectMemoryResponse(operation, response); }
    catch (error) {
      throw objectError(error);
    }
  }
  async #call<Schema extends Parameters<typeof create>[0]>(operation: string, schema: Schema, value: Parameters<typeof create<Schema>>[1], body?: Uint8Array): Promise<Uint8Array> { try { return await (await this.#inner).dispatch(operation, toBinary(schema, create(schema, value)), body); } catch (error) { throw objectError(error); } }
}

export type ObjectErrorCode = WasmObjectsErrorCode;

const isKnownObjectErrorCode = (value: string): value is ObjectErrorCode => is_objects_error_code(value);

export class ObjectError extends Error { constructor(readonly code: ObjectErrorCode, message: string) { super(message); } }
const wireBucket = (value: BucketRef) => ({ bucketId: value.bucketId, name: value.name });
const wireSnapshot = (value: SnapshotRef) => ({ snapshotId: value.snapshotId, sourceBucketId: value.sourceBucketId });
const wireTarget = (value: ReadTarget) => value.kind === "bucket" ? { target: { case: "bucket" as const, value: wireBucket(value.bucket) } } : { target: { case: "snapshot" as const, value: value.snapshot } };
const wireMetadata = (value: ObjectMetadata) => create(ObjectMetadataSchema, { contentType: value.contentType, user: Object.fromEntries(value.user), contentEncoding: value.contentEncoding, cacheControl: value.cacheControl, contentDisposition: value.contentDisposition, contentLanguage: value.contentLanguage, expiresUnixSeconds: value.expiresUnixSeconds });
const preconditions = (value: Condition | undefined) => value === undefined ? undefined : create(PreconditionsSchema, { condition: value.kind === "ifAbsent" ? { case: "ifAbsent" as const, value: true } : value.kind === "ifMatch" ? { case: "ifMatch" as const, value: value.etag } : { case: "ifVersion" as const, value: value.versionId } });
const mutation = (value: IdempotencyKey | undefined) => value === undefined ? undefined : { idempotencyKey: value };
const trustedString = <Brand extends string>(value: string): Brand => value as Brand;
function objectError(error: unknown): ObjectError {
  let message: string;
  let rawCode: unknown;
  try {
    message = error instanceof Error ? error.message : String(error);
  } catch {
    message = "Objects operation failed";
  }
  try {
    rawCode = typeof error === "object" && error !== null ? (error as { readonly code?: unknown }).code : undefined;
  } catch {
    rawCode = undefined;
  }
  const code = typeof rawCode === "string" && isKnownObjectErrorCode(rawCode) ? rawCode : "invalid_argument";
  return new ObjectError(code, message);
}
