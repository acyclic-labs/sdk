/** Public contracts for the permanently versioned Objects service.
 *
 * Providers carry protocol semantics; the exported handles add typed codecs and stable object,
 * bucket, snapshot, and multipart identities without duplicating those semantics.
 */

declare const objectBrand: unique symbol;
export type BucketId = string & { readonly [objectBrand]: "BucketId" };
export type SnapshotId = string & { readonly [objectBrand]: "SnapshotId" };
export type VersionId = string & { readonly [objectBrand]: "VersionId" };
export type ETag = string & { readonly [objectBrand]: "ETag" };
export type UploadId = string & { readonly [objectBrand]: "UploadId" };
export type IdempotencyKey = string & { readonly [objectBrand]: "IdempotencyKey" };
export function idempotencyKey(value: string): IdempotencyKey {
  if (!value.trim()) throw new TypeError("idempotency key is required");
  return value as IdempotencyKey;
}

/** Exact bucket identity. Reusing a name creates a different identity. */
export interface BucketRef { readonly bucketId: BucketId; readonly name: string }

/** Exact immutable whole-bucket snapshot identity. */
export interface SnapshotRef { readonly snapshotId: SnapshotId; readonly sourceBucketId: BucketId }

/** Immutable metadata attached to one object version. */
export interface ObjectMetadata {
  readonly contentType: string;
  readonly contentEncoding: string;
  readonly cacheControl: string;
  readonly contentDisposition: string;
  readonly contentLanguage: string;
  readonly expiresUnixSeconds: bigint | undefined;
  readonly user: ReadonlyMap<string, string>;
}

/** Opaque immutable object-version descriptor. */
export interface ObjectVersion {
  readonly versionId: VersionId;
  readonly etag: ETag;
  readonly size: bigint;
  readonly deleteMarker: boolean;
  readonly metadata: ObjectMetadata;
}

/** Exactly one current-version write condition. */
export type Condition =
  | { readonly kind: "ifAbsent" }
  | { readonly kind: "ifMatch"; readonly etag: ETag }
  | { readonly kind: "ifVersion"; readonly versionId: VersionId };

/** A current bucket or immutable snapshot read target. */
export type ReadTarget =
  | { readonly kind: "bucket"; readonly bucket: BucketRef }
  | { readonly kind: "snapshot"; readonly snapshot: SnapshotRef };

/** Buffered object returned by a transport adapter. */
export interface StoredObject { readonly version: ObjectVersion; readonly body: Uint8Array; readonly contentRange?: ByteRange }
/** Buffered transport offsets, each runtime-enforced as an exact non-negative safe integer. */
export interface ByteRange { readonly start: number; readonly endExclusive: number; readonly total: number }

/** Stable listing page. Continuations remain bound to the original captured view. */
export interface ListPage {
  readonly entries: readonly { readonly objectKey: string; readonly version: ObjectVersion }[];
  readonly commonPrefixes: readonly string[];
  readonly continuation: string | undefined;
}

/** Transport-neutral public provider contract.
 *
 * Hosted and embedded adapters must implement these exact permanent-version semantics.
 */
export interface ObjectsProvider {
  createBucket(name: string, idempotencyKey?: IdempotencyKey): Promise<BucketRef>;
  headBucket(bucket: BucketRef): Promise<BucketRef>;
  deleteBucket(bucket: BucketRef, idempotencyKey?: IdempotencyKey): Promise<boolean>;
  put(bucket: BucketRef, objectKey: string, body: Uint8Array, metadata: ObjectMetadata,
    condition?: Condition, idempotencyKey?: IdempotencyKey): Promise<ObjectVersion>;
  head(target: ReadTarget, objectKey: string, versionId?: VersionId): Promise<ObjectVersion>;
  get(target: ReadTarget, objectKey: string, versionId?: VersionId, range?: Omit<ByteRange, "total">): Promise<StoredObject>;
  delete(bucket: BucketRef, objectKey: string, versionId?: VersionId, condition?: Condition,
    idempotencyKey?: IdempotencyKey): Promise<{ readonly existed: boolean; readonly marker?: ObjectVersion }>;
  list(target: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean,
    pageSize: number, continuation?: string): Promise<ListPage>;
  snapshot(bucket: BucketRef, idempotencyKey?: IdempotencyKey): Promise<SnapshotRef>;
  destroySnapshot(snapshot: SnapshotRef, idempotencyKey?: IdempotencyKey): Promise<boolean>;
  fork(source: ReadTarget, destinationName: string, idempotencyKey?: IdempotencyKey): Promise<BucketRef>;
}

export interface MultipartUpload { readonly uploadId: UploadId; readonly bucket: BucketRef; readonly objectKey: string; readonly metadata: ObjectMetadata }
export interface UploadedPart { readonly partNumber: number; readonly etag: ETag; readonly size: number }
export interface MultipartProvider {
  createMultipart(bucket: BucketRef, objectKey: string, metadata: ObjectMetadata, idempotencyKey?: IdempotencyKey): Promise<MultipartUpload>;
  uploadPart(upload: MultipartUpload, partNumber: number, body: Uint8Array, idempotencyKey?: IdempotencyKey): Promise<UploadedPart>;
  listParts(upload: MultipartUpload): Promise<readonly UploadedPart[]>;
  completeMultipart(upload: MultipartUpload, parts: readonly UploadedPart[], condition?: Condition, idempotencyKey?: IdempotencyKey): Promise<ObjectVersion>;
  abortMultipart(upload: MultipartUpload, idempotencyKey?: IdempotencyKey): Promise<boolean>;
}

export * from "./client.js";
export * from "./memory.js";
export * from "./http.js";
