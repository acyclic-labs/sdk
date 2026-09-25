import { create, fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import {
  AbortMultipartRequestSchema, AbortMultipartResponseSchema, BucketRefSchema, BucketSchema,
  CompleteMultipartRequestSchema, CreateBucketRequestSchema, CreateMultipartRequestSchema,
  CreateSnapshotRequestSchema, DeleteBucketRequestSchema, DeleteBucketResponseSchema,
  DeleteObjectRequestSchema, DeleteObjectResponseSchema, DestroySnapshotRequestSchema,
  DestroySnapshotResponseSchema, ForkBucketRequestSchema, ForkSnapshotRequestSchema,
  GetObjectRequestSchema, HeadBucketRequestSchema, HeadObjectRequestSchema, HeadObjectResponseSchema,
  ListObjectsRequestSchema, ListObjectsResponseSchema, ListPartsRequestSchema, ListPartsResponseSchema,
  ListingMode, MutationIdentitySchema, ObjectMetadataSchema, ObjectVersionSchema,
  PreconditionsSchema, PutObjectHeaderSchema, ReadTargetSchema, SnapshotRefSchema, SnapshotSchema,
  UploadedPartSchema, UploadPartHeaderSchema, MultipartUploadSchema,
} from "../generated/proto/objects/v1/objects_pb.js";
import type {
  BucketId, BucketRef, ByteRange, Condition, ETag, IdempotencyKey, ListPage, MultipartProvider,
  MultipartUpload, ObjectMetadata, ObjectsProvider, ObjectVersion, ReadTarget, SnapshotId,
  SnapshotRef, StoredObject, UploadedPart, UploadId, VersionId,
} from "./index.js";

type WireBucket = MessageShape<typeof BucketRefSchema>;
type WireSnapshot = MessageShape<typeof SnapshotRefSchema>;
type WireMetadata = MessageShape<typeof ObjectMetadataSchema>;
type WireVersion = MessageShape<typeof ObjectVersionSchema>;
type WasmModule = typeof import("../generated/wasm/acyclic_objects_wasm.js");
type Binding = InstanceType<WasmModule["MemoryObjectsBinding"]>;
let wasmModule: Promise<WasmModule> | undefined;
async function loadWasm(): Promise<WasmModule> {
  wasmModule ??= (async () => {
    const module = await import("../generated/wasm/acyclic_objects_wasm.js");
    const node = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node;
    if (node === undefined) await module.default();
    else {
      const fsPath: string = "node:fs/promises";
      const { readFile } = await import(fsPath) as { readFile(url: URL): Promise<Uint8Array> };
      await module.default(await readFile(new URL("../generated/wasm/acyclic_objects_wasm_bg.wasm", import.meta.url)));
    }
    return module;
  })().catch((error: unknown) => { wasmModule = undefined; throw error; });
  return wasmModule;
}

export class ObjectError extends Error {
  constructor(readonly code: "not_found" | "precondition_failed" | "idempotency_mismatch" | "bucket_not_empty" |
    "bucket_exists" | "invalid_name" | "invalid_key" | "invalid_range" | "invalid_page_size" |
    "invalid_part" | "invalid_continuation" | "invalid_response" | "capacity_exhausted", message: string) { super(message); }
}
function asObjectError(error: unknown): never {
  if (error instanceof ObjectError) throw error;
  if (error instanceof Error) {
    const code = (error as Error & { code?: ObjectError["code"] }).code;
    if (code !== undefined) throw new ObjectError(code, error.message);
  }
  throw error;
}
function encode<S extends DescMessage>(schema: S, value: MessageShape<S>): Uint8Array { return toBinary(schema, value); }
function decode<S extends DescMessage>(schema: S, bytes: Uint8Array): MessageShape<S> {
  try { return fromBinary(schema, bytes); }
  catch { throw new ObjectError("invalid_response", "provider returned malformed protobuf bytes"); }
}
function bucket(value: BucketRef): WireBucket { return create(BucketRefSchema, { bucketId: value.bucketId, name: value.name }); }
function bucketRef(value: WireBucket | undefined): BucketRef {
  if (!value) throw new ObjectError("invalid_response", "bucket response has no identity");
  return { bucketId: value.bucketId as BucketId, name: value.name };
}
function snapshot(value: SnapshotRef): WireSnapshot { return create(SnapshotRefSchema, { snapshotId: value.snapshotId, sourceBucketId: value.sourceBucketId }); }
function snapshotRef(value: WireSnapshot | undefined): SnapshotRef {
  if (!value) throw new ObjectError("invalid_response", "snapshot response has no identity");
  return { snapshotId: value.snapshotId as SnapshotId, sourceBucketId: value.sourceBucketId as BucketId };
}
function target(value: ReadTarget): MessageShape<typeof ReadTargetSchema> {
  return create(ReadTargetSchema, { target: value.kind === "bucket" ? { case: "bucket", value: bucket(value.bucket) } : { case: "snapshot", value: snapshot(value.snapshot) } });
}
function metadata(value: ObjectMetadata): WireMetadata {
  return create(ObjectMetadataSchema, {
    contentType: value.contentType, contentEncoding: value.contentEncoding, cacheControl: value.cacheControl,
    contentDisposition: value.contentDisposition, contentLanguage: value.contentLanguage,
    expiresUnixSeconds: value.expiresUnixSeconds, user: Object.fromEntries(value.user),
  });
}
function objectMetadata(value: WireMetadata | undefined): ObjectMetadata {
  if (!value) throw new ObjectError("invalid_response", "object metadata is missing");
  return {
    contentType: value.contentType, contentEncoding: value.contentEncoding, cacheControl: value.cacheControl,
    contentDisposition: value.contentDisposition, contentLanguage: value.contentLanguage,
    expiresUnixSeconds: value.expiresUnixSeconds, user: new Map(Object.entries(value.user)),
  };
}
function precondition(value: Condition | undefined): MessageShape<typeof PreconditionsSchema> | undefined {
  if (!value) return undefined;
  const condition = value.kind === "ifAbsent" ? { case: "ifAbsent" as const, value: true } :
    value.kind === "ifMatch" ? { case: "ifMatch" as const, value: value.etag } :
      { case: "ifVersion" as const, value: value.versionId };
  return create(PreconditionsSchema, { condition });
}
function mutation(value: IdempotencyKey | undefined): MessageShape<typeof MutationIdentitySchema> | undefined {
  return value === undefined ? undefined : create(MutationIdentitySchema, { idempotencyKey: value });
}
function objectVersion(value: WireVersion | undefined): ObjectVersion {
  if (!value) throw new ObjectError("invalid_response", "object version is missing");
  return { versionId: value.versionId as VersionId, etag: value.etag as ETag, size: value.size,
    deleteMarker: value.deleteMarker, metadata: objectMetadata(value.metadata) };
}
function uploadedPart(value: MessageShape<typeof UploadedPartSchema>): UploadedPart {
  const size = Number(value.size);
  if (!Number.isSafeInteger(size)) throw new ObjectError("invalid_part", "part size exceeds the exact JavaScript range");
  return { partNumber: value.partNumber, etag: value.etag as ETag, size };
}
function part(value: UploadedPart): MessageShape<typeof UploadedPartSchema> {
  validatePartNumber(value.partNumber);
  if (!Number.isSafeInteger(value.size) || value.size < 0) throw new ObjectError("invalid_part", "part size is invalid");
  return create(UploadedPartSchema, { partNumber: value.partNumber, etag: value.etag, size: BigInt(value.size) });
}
function validatePartNumber(value: number): void {
  if (!Number.isInteger(value) || value < 1 || value > 10_000) throw new ObjectError("invalid_part", "part number must be 1..10000");
}

/** Canonical Rust process-local provider for tests, browsers, and offline agents. */
export class MemoryObjectsProvider implements ObjectsProvider, MultipartProvider {
  #binding: Promise<Binding> | undefined;
  #ready(): Promise<Binding> {
    this.#binding ??= loadWasm().then((module) => new module.MemoryObjectsBinding())
      .catch((error: unknown) => { this.#binding = undefined; throw error; });
    return this.#binding;
  }
  async createBucket(name: string, key?: IdempotencyKey): Promise<BucketRef> {
    const request = encode(CreateBucketRequestSchema, create(CreateBucketRequestSchema, { name, mutation: mutation(key) }));
    try { return bucketRef(decode(BucketSchema, await (await this.#ready()).create_bucket(request)).bucket); } catch (error) { return asObjectError(error); }
  }
  async headBucket(value: BucketRef): Promise<BucketRef> {
    const request = encode(HeadBucketRequestSchema, create(HeadBucketRequestSchema, { bucket: bucket(value) }));
    try { return bucketRef(decode(BucketSchema, await (await this.#ready()).head_bucket(request)).bucket); } catch (error) { return asObjectError(error); }
  }
  async deleteBucket(value: BucketRef, key?: IdempotencyKey): Promise<boolean> {
    const request = encode(DeleteBucketRequestSchema, create(DeleteBucketRequestSchema, { bucket: bucket(value), mutation: mutation(key) }));
    try { return decode(DeleteBucketResponseSchema, await (await this.#ready()).delete_bucket(request)).existed; } catch (error) { return asObjectError(error); }
  }
  async put(value: BucketRef, objectKey: string, body: Uint8Array, meta: ObjectMetadata, condition?: Condition, key?: IdempotencyKey): Promise<ObjectVersion> {
    const header = encode(PutObjectHeaderSchema, create(PutObjectHeaderSchema, { bucket: bucket(value), objectKey, metadata: metadata(meta), preconditions: precondition(condition), mutation: mutation(key) }));
    const ownedBody = Uint8Array.from(body);
    try { return objectVersion(decode(ObjectVersionSchema, await (await this.#ready()).put(header, ownedBody))); } catch (error) { return asObjectError(error); }
  }
  async head(value: ReadTarget, objectKey: string, versionId?: VersionId): Promise<ObjectVersion> {
    const request = encode(HeadObjectRequestSchema, create(HeadObjectRequestSchema, { target: target(value), objectKey, versionId: versionId ?? "" }));
    try { return objectVersion(decode(HeadObjectResponseSchema, await (await this.#ready()).head(request)).version); } catch (error) { return asObjectError(error); }
  }
  async get(value: ReadTarget, objectKey: string, versionId?: VersionId, range?: Omit<ByteRange, "total">): Promise<StoredObject> {
    if (range && (!Number.isSafeInteger(range.start) || !Number.isSafeInteger(range.endExclusive) || range.start < 0 || range.endExclusive <= range.start)) throw new ObjectError("invalid_range", "range is invalid");
    const request = encode(GetObjectRequestSchema, create(GetObjectRequestSchema, {
      target: target(value), objectKey, versionId: versionId ?? "", rangeRequested: range !== undefined,
      rangeStart: BigInt(range?.start ?? 0), rangeEndInclusive: range ? BigInt(range.endExclusive - 1) : undefined,
    }));
    try {
      const frames = await (await this.#ready()).get(request);
      if (!Array.isArray(frames) || frames.length !== 2 || !(frames[0] instanceof Uint8Array) || !(frames[1] instanceof Uint8Array)) throw new ObjectError("invalid_response", "object response is malformed");
      const version = objectVersion(decode(ObjectVersionSchema, frames[0]));
      const body = Uint8Array.from(frames[1]);
      return range ? { version, body, contentRange: { start: range.start, endExclusive: range.start + body.byteLength, total: Number(version.size) } } : { version, body };
    } catch (error) { return asObjectError(error); }
  }
  async delete(value: BucketRef, objectKey: string, versionId?: VersionId, condition?: Condition, key?: IdempotencyKey): Promise<{ readonly existed: boolean; readonly marker?: ObjectVersion }> {
    const request = encode(DeleteObjectRequestSchema, create(DeleteObjectRequestSchema, { bucket: bucket(value), objectKey, versionId: versionId ?? "", preconditions: precondition(condition), mutation: mutation(key) }));
    try { const result = decode(DeleteObjectResponseSchema, await (await this.#ready()).delete(request)); return { existed: result.existed, ...(result.version ? { marker: objectVersion(result.version) } : {}) }; } catch (error) { return asObjectError(error); }
  }
  async list(value: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean, pageSize: number, continuation?: string): Promise<ListPage> {
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1_000) throw new ObjectError("invalid_page_size", "page size must be 1..1000");
    const request = encode(ListObjectsRequestSchema, create(ListObjectsRequestSchema, { target: target(value), prefix, delimiter: delimiter ?? "", mode: versions ? ListingMode.VERSIONS : ListingMode.CURRENT, pageSize, continuationToken: continuation ?? "" }));
    try { const result = decode(ListObjectsResponseSchema, await (await this.#ready()).list(request)); return {
      entries: result.entries.map((entry) => ({ objectKey: entry.objectKey, version: objectVersion(entry.version) })),
      commonPrefixes: result.commonPrefixes, continuation: result.continuationToken || undefined,
    }; } catch (error) { return asObjectError(error); }
  }
  async snapshot(value: BucketRef, key?: IdempotencyKey): Promise<SnapshotRef> {
    const request = encode(CreateSnapshotRequestSchema, create(CreateSnapshotRequestSchema, { bucket: bucket(value), mutation: mutation(key) }));
    try { return snapshotRef(decode(SnapshotSchema, await (await this.#ready()).snapshot(request)).snapshot); } catch (error) { return asObjectError(error); }
  }
  async destroySnapshot(value: SnapshotRef, key?: IdempotencyKey): Promise<boolean> {
    const request = encode(DestroySnapshotRequestSchema, create(DestroySnapshotRequestSchema, { snapshot: snapshot(value), mutation: mutation(key) }));
    try { return decode(DestroySnapshotResponseSchema, await (await this.#ready()).destroy_snapshot(request)).existed; } catch (error) { return asObjectError(error); }
  }
  async fork(value: ReadTarget, destinationName: string, key?: IdempotencyKey): Promise<BucketRef> {
    const request = value.kind === "bucket" ? encode(ForkBucketRequestSchema, create(ForkBucketRequestSchema, { source: bucket(value.bucket), destinationName, mutation: mutation(key) })) :
      encode(ForkSnapshotRequestSchema, create(ForkSnapshotRequestSchema, { snapshot: snapshot(value.snapshot), destinationName, mutation: mutation(key) }));
    try { const raw = value.kind === "bucket" ? await (await this.#ready()).fork_bucket(request) : await (await this.#ready()).fork_snapshot(request);
      return bucketRef(decode(BucketSchema, raw).bucket); } catch (error) { return asObjectError(error); }
  }
  async createMultipart(value: BucketRef, objectKey: string, meta: ObjectMetadata, condition?: Condition, key?: IdempotencyKey): Promise<MultipartUpload> {
    const request = encode(CreateMultipartRequestSchema, create(CreateMultipartRequestSchema, { bucket: bucket(value), objectKey, metadata: metadata(meta), preconditions: precondition(condition), mutation: mutation(key) }));
    try { const result = decode(MultipartUploadSchema, await (await this.#ready()).create_multipart(request)); return { uploadId: result.uploadId as UploadId, bucket: value, objectKey, metadata: { ...meta, user: new Map(meta.user) } }; } catch (error) { return asObjectError(error); }
  }
  async uploadPart(upload: MultipartUpload, partNumber: number, body: Uint8Array, key?: IdempotencyKey): Promise<UploadedPart> {
    validatePartNumber(partNumber);
    const header = encode(UploadPartHeaderSchema, create(UploadPartHeaderSchema, { bucket: bucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId, partNumber, mutation: mutation(key) }));
    const ownedBody = Uint8Array.from(body);
    try { return uploadedPart(decode(UploadedPartSchema, await (await this.#ready()).upload_part(header, ownedBody))); } catch (error) { return asObjectError(error); }
  }
  async listParts(upload: MultipartUpload): Promise<readonly UploadedPart[]> {
    const request = encode(ListPartsRequestSchema, create(ListPartsRequestSchema, { bucket: bucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId }));
    try { return decode(ListPartsResponseSchema, await (await this.#ready()).list_parts(request)).parts.map(uploadedPart); } catch (error) { return asObjectError(error); }
  }
  async completeMultipart(upload: MultipartUpload, parts: readonly UploadedPart[], key?: IdempotencyKey): Promise<ObjectVersion> {
    const request = encode(CompleteMultipartRequestSchema, create(CompleteMultipartRequestSchema, { bucket: bucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId, parts: parts.map(part), mutation: mutation(key) }));
    try { return objectVersion(decode(ObjectVersionSchema, await (await this.#ready()).complete_multipart(request))); } catch (error) { return asObjectError(error); }
  }
  async abortMultipart(upload: MultipartUpload, key?: IdempotencyKey): Promise<boolean> {
    const request = encode(AbortMultipartRequestSchema, create(AbortMultipartRequestSchema, { bucket: bucket(upload.bucket), objectKey: upload.objectKey, uploadId: upload.uploadId, mutation: mutation(key) }));
    try { return decode(AbortMultipartResponseSchema, await (await this.#ready()).abort_multipart(request)).existed; } catch (error) { return asObjectError(error); }
  }
}
