/** Public data shapes for the permanently versioned Objects protocol.
 *
 * The supported high-level client is the Rust gRPC SDK. TypeScript consumers can use these
 * transport-neutral shapes with the generated protobuf bindings without inheriting a second
 * semantic implementation.
 */

/** Exact bucket identity. Reusing a name creates a different identity. */
export interface BucketRef { readonly bucketId: string; readonly name: string }

/** Exact immutable whole-bucket snapshot identity. */
export interface SnapshotRef { readonly snapshotId: string; readonly sourceBucketId: string }

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
  readonly versionId: string;
  readonly etag: string;
  readonly size: bigint;
  readonly deleteMarker: boolean;
  readonly metadata: ObjectMetadata;
}

/** Exactly one current-version write condition. */
export type Condition =
  | { readonly kind: "ifAbsent" }
  | { readonly kind: "ifMatch"; readonly etag: string }
  | { readonly kind: "ifVersion"; readonly versionId: string };

/** A current bucket or immutable snapshot read target. */
export type ReadTarget =
  | { readonly kind: "bucket"; readonly bucket: BucketRef }
  | { readonly kind: "snapshot"; readonly snapshot: SnapshotRef };

/** Buffered object returned by a transport adapter. */
export interface StoredObject { readonly version: ObjectVersion; readonly body: Uint8Array }

/** Stable listing page. Continuations remain bound to the original captured view. */
export interface ListPage {
  readonly entries: readonly { readonly objectKey: string; readonly version: ObjectVersion }[];
  readonly commonPrefixes: readonly string[];
  readonly continuation: string | undefined;
}

/** Transport-neutral public provider contract.
 *
 * Hosted and embedded adapters must implement these exact permanent-version semantics. Multipart
 * streaming remains on the generated protocol because a buffered browser interface would encode
 * the wrong memory and backpressure contract.
 */
export interface ObjectsProvider {
  createBucket(name: string, idempotencyKey?: string): Promise<BucketRef>;
  headBucket(bucket: BucketRef): Promise<BucketRef>;
  deleteBucket(bucket: BucketRef, idempotencyKey?: string): Promise<boolean>;
  put(bucket: BucketRef, objectKey: string, body: Uint8Array, metadata: ObjectMetadata,
    condition?: Condition, idempotencyKey?: string): Promise<ObjectVersion>;
  get(target: ReadTarget, objectKey: string, versionId?: string): Promise<StoredObject>;
  delete(bucket: BucketRef, objectKey: string, versionId?: string, condition?: Condition,
    idempotencyKey?: string): Promise<{ readonly existed: boolean; readonly marker?: ObjectVersion }>;
  list(target: ReadTarget, prefix: string, delimiter: string | undefined, versions: boolean,
    pageSize: number, continuation?: string): Promise<ListPage>;
  snapshot(bucket: BucketRef, idempotencyKey?: string): Promise<SnapshotRef>;
  destroySnapshot(snapshot: SnapshotRef, idempotencyKey?: string): Promise<boolean>;
  fork(source: ReadTarget, destinationName: string, idempotencyKey?: string): Promise<BucketRef>;
}
