/** Public contracts for the permanently versioned Objects service.
 *
 * Providers carry protocol semantics; the exported handles add typed codecs and stable object,
 * bucket, snapshot, and multipart identities without duplicating those semantics.
 */

import type {
  BucketRef as WireBucketRef, ObjectMetadata as WireObjectMetadata,
  GetObjectResponse as WireGetObjectResponse, ListEntry as WireListEntry,
  ListObjectsResponse as WireListObjectsResponse, MultipartUpload as WireMultipartUpload,
  ObjectVersion as WireObjectVersion, Preconditions as WirePreconditions,
  ReadTarget as WireReadTarget, SnapshotRef as WireSnapshotRef,
  UploadedPart as WireUploadedPart,
} from "../generated/proto/objects/v1/objects_pb.js";
import { ObjectsLimit } from "../generated/proto/objects/v1/objects_pb.js";

type PublicWire<Wire, Overrides extends object, Omitted extends keyof Wire = never> =
  Readonly<Omit<Wire, "$typeName" | "$unknown" | keyof Overrides | Omitted> & Overrides>;
type AssertNever<Value extends never> = Value;

declare const objectBrand: unique symbol;
export type BucketId = string & { readonly [objectBrand]: "BucketId" };
export type SnapshotId = string & { readonly [objectBrand]: "SnapshotId" };
export type VersionId = string & { readonly [objectBrand]: "VersionId" };
export type ETag = string & { readonly [objectBrand]: "ETag" };
export type UploadId = string & { readonly [objectBrand]: "UploadId" };
export type IdempotencyKey = string & { readonly [objectBrand]: "IdempotencyKey" };
export function idempotencyKey(value: string): IdempotencyKey {
  if (value.length === 0) throw new TypeError("idempotency key is required");
  if (new TextEncoder().encode(value).byteLength > ObjectsLimit.MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new RangeError(`idempotency key must contain at most ${ObjectsLimit.MAX_IDEMPOTENCY_KEY_BYTES} UTF-8 bytes`);
  }
  return value as IdempotencyKey;
}

/** Exact bucket identity. Reusing a name creates a different identity. */
export type BucketRef = PublicWire<WireBucketRef, { readonly bucketId: BucketId }>;

/** Exact immutable whole-bucket snapshot identity. */
export type SnapshotRef = PublicWire<WireSnapshotRef, { readonly snapshotId: SnapshotId; readonly sourceBucketId: BucketId }>;

/** Immutable metadata attached to one object version. */
export type ObjectMetadata = PublicWire<WireObjectMetadata, {
  readonly expiresUnixSeconds: bigint | undefined;
  readonly user: ReadonlyMap<string, string>;
}>;

/** Opaque immutable object-version descriptor. */
export type ObjectVersion = PublicWire<WireObjectVersion, {
  readonly versionId: VersionId;
  readonly etag: ETag;
  readonly metadata: ObjectMetadata;
  /** Server-authored creation time retained as a natural JavaScript date. */
  readonly createdAt: Date | undefined;
  /** Exact server-authored creation time in Unix nanoseconds. */
  readonly createdAtUnixNanos: bigint | undefined;
}, "createdAt">;

/** Exactly one current-version write condition. */
type ConditionKind = Exclude<WirePreconditions["condition"]["case"], undefined>;
type _ConditionKinds = AssertNever<Exclude<ConditionKind, "ifAbsent" | "ifMatch" | "ifVersion">>;
export type Condition =
  | { readonly kind: Extract<ConditionKind, "ifAbsent"> }
  | { readonly kind: Extract<ConditionKind, "ifMatch">; readonly etag: ETag }
  | { readonly kind: Extract<ConditionKind, "ifVersion">; readonly versionId: VersionId };

/** A current bucket or immutable snapshot read target. */
type ReadTargetKind = Exclude<WireReadTarget["target"]["case"], undefined>;
type _ReadTargetKinds = AssertNever<Exclude<ReadTargetKind, "bucket" | "snapshot">>;
export type ReadTarget =
  | { readonly kind: Extract<ReadTargetKind, "bucket">; readonly bucket: BucketRef }
  | { readonly kind: Extract<ReadTargetKind, "snapshot">; readonly snapshot: SnapshotRef };

/** Metadata-only read conditions; body ranges and limits do not apply. */
export interface HeadOptions {
  readonly versionId?: VersionId;
  readonly ifMatch?: ETag;
  readonly ifNoneMatch?: ETag;
}

/**
 * Buffered object returned by a transport adapter.
 *
 * The body type is the bytes arm of the generated GetObject response. `version` and
 * `contentRange` are combined here by the SDK after consuming the response stream;
 * they do not form one protobuf message. `contentRange` therefore remains an
 * intentional TypeScript-only projection of a ranged read.
 */
type WireObjectBody = Extract<WireGetObjectResponse["frame"], { readonly case: "body" }>["value"];
export type StoredObject = Readonly<{
  readonly version: ObjectVersion;
  readonly body: WireObjectBody;
  readonly contentRange?: ByteRange;
}>;

/** Buffered transport offsets, each runtime-enforced as an exact non-negative safe integer. */
export interface ByteRange { readonly start: number; readonly endExclusive: number; readonly total: number }

/** Stable listing page. Continuations remain bound to the original captured view. */
type PublicListEntry = PublicWire<WireListEntry, { readonly version: ObjectVersion }>;
export type ListPage = PublicWire<WireListObjectsResponse, {
  readonly entries: readonly PublicListEntry[];
  readonly commonPrefixes: readonly string[];
  readonly continuation: string | undefined;
}, "continuationToken">;

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
  head(target: ReadTarget, objectKey: string, options?: HeadOptions): Promise<ObjectVersion>;
  get(target: ReadTarget, objectKey: string, versionId?: VersionId, range?: Omit<ByteRange, "total">): Promise<StoredObject>;
  delete(bucket: BucketRef, objectKey: string, versionId?: VersionId, condition?: Condition,
    idempotencyKey?: IdempotencyKey): Promise<{ readonly existed: boolean; readonly marker?: ObjectVersion }>;
  list(target: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean,
    pageSize: number, continuation?: string): Promise<ListPage>;
  snapshot(bucket: BucketRef, idempotencyKey?: IdempotencyKey): Promise<SnapshotRef>;
  destroySnapshot(snapshot: SnapshotRef, idempotencyKey?: IdempotencyKey): Promise<boolean>;
  fork(source: ReadTarget, destinationName: string, idempotencyKey?: IdempotencyKey): Promise<BucketRef>;
}

export type MultipartUpload = PublicWire<WireMultipartUpload, {
  readonly uploadId: UploadId;
  readonly bucket: BucketRef;
  readonly objectKey: string;
  readonly metadata: ObjectMetadata;
}>;
export type UploadedPart = PublicWire<WireUploadedPart, { readonly etag: ETag }>;
type _PublicWireExposesNoProtobufInternals = AssertNever<Extract<
  keyof BucketRef | keyof SnapshotRef | keyof ObjectMetadata | keyof ObjectVersion | keyof UploadedPart,
  "$typeName" | "$unknown"
>>;
export interface MultipartProvider {
  createMultipart(bucket: BucketRef, objectKey: string, metadata: ObjectMetadata, condition?: Condition, idempotencyKey?: IdempotencyKey): Promise<MultipartUpload>;
  uploadPart(upload: MultipartUpload, partNumber: number, body: Uint8Array, idempotencyKey?: IdempotencyKey): Promise<UploadedPart>;
  listParts(upload: MultipartUpload): Promise<readonly UploadedPart[]>;
  completeMultipart(upload: MultipartUpload, parts: readonly UploadedPart[], idempotencyKey?: IdempotencyKey): Promise<ObjectVersion>;
  abortMultipart(upload: MultipartUpload, idempotencyKey?: IdempotencyKey): Promise<boolean>;
}

export * from "./client.js";
export * from "./memory.js";
export * from "./http.js";
export type { HttpResponseFor, HttpRoute } from "./http-contract.js";
