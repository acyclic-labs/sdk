/** Owner-authenticated, content-addressed logical Objects for Harness. */
import { create } from "@bufbuild/protobuf";
import {
  type Attachment, type ContentBindings, type FileRef, type Harness,
  type Limits, type ProviderRef, type Scope, type VolumeClass, type VolumeRef,
} from "./index.js";
import {
  GetObjectRequestSchema, HeadBucketRequestSchema, PutObjectHeaderSchema,
  ErrorCode, ObjectsV2Error, type Bucket, type ObjectsV2Provider,
} from "@acyclic-labs/objects/v2";
import { makeRustOwnedObjectKey, type PublicGetObjectRequest } from "@acyclic-labs/objects";

const encoder = new TextEncoder();
const maximumObjectBytes = 5 * 1_024 ** 3;
const maximumObjectKeyBytes = 1_024;

export interface ObjectContentOptions {
  readonly objects: ObjectsV2Provider;
  readonly bucket: Bucket;
  readonly volume: ObjectVolumeRef;
  readonly expectedProvider: ProviderRef<"objects">;
  /** Harness issuer for the bucket's owning provider. */
  readonly authority: Harness;
  /** Owner-bound authority anchor, supplied by the owning provider, not the reader. */
  readonly ownerScope: Scope;
  /** Signed owner or attached-reader scope; capability strings alone do not authorize bytes. */
  readonly scope: Scope;
  readonly maximumBytes: number;
}

/** An Objects adapter cannot accidentally be configured with a Filesystem or Stream volume. */
export type ObjectVolumeRef = VolumeRef<VolumeClass, "objects">;

/** One bucket/volume binding; the original owner is the only private writer. */
export class ObjectContentStore {
  readonly #namespace: string;
  readonly #writable: boolean;
  readonly #objects: Pick<ObjectsV2Provider, "headBucket" | "put" | "get">;

  private constructor(readonly options: ObjectContentOptions, namespace: string, writable: boolean,
    objects: Pick<ObjectsV2Provider, "headBucket" | "put" | "get">) {
    this.#namespace = namespace;
    this.#writable = writable;
    this.#objects = objects;
  }

  static async create(options: ObjectContentOptions): Promise<ObjectContentStore> {
    const authority = options.authority;
    const volume = authority.validateVolumeRef(options.volume);
    const ownerScope = freezeScope(options.ownerScope);
    const scope = freezeScope(options.scope);
    const bucket = Object.freeze({ ...options.bucket, bucket: options.bucket.bucket === undefined
      ? undefined : Object.freeze({ ...options.bucket.bucket }) });
    const expectedProvider = Object.freeze({ ...options.expectedProvider });
    const maximumBytes = options.maximumBytes;
    const owner = options.objects;
    const objects = Object.freeze({
      headBucket: owner.headBucket.bind(owner), put: owner.put.bind(owner),
      get: owner.get.bind(owner),
    });
    authority.verifyScope(ownerScope);
    authority.verifyScope(scope);
    const ownerWrite = authority.volumeCapability(volume, "write");
    if (!ownerScope.capabilities.includes(ownerWrite)
      || (volume.owner.kind === "agent" && ownerScope.agent !== volume.owner.id)) {
      throw new Error("Objects binding lacks its owner-issued write authority");
    }
    if (volume.provider.family !== "objects"
      || volume.provider.namespace !== expectedProvider.namespace
      || volume.provider.family !== expectedProvider.family
      || volume.provider.version !== expectedProvider.version
      || volume.id !== bucket.bucket?.name
      || !bucket.bucket?.name || !Number.isSafeInteger(maximumBytes)
      || maximumBytes <= 0 || maximumBytes > maximumObjectBytes) {
      throw new TypeError("Objects content binding is invalid");
    }
    const resolved = await objects.headBucket(create(HeadBucketRequestSchema, { bucket: bucket.bucket }));
    if (resolved.bucket?.name !== bucket.bucket.name) {
      throw new Error("Objects provider resolved another bucket");
    }
    const writeGrant = authority.volumeCapability(volume, "write");
    const writable = scope.capabilities.includes(writeGrant);
    if (writable) authority.verifyContentWrite(scope, volume);
    return new ObjectContentStore(Object.freeze({ ...options, bucket, expectedProvider,
      maximumBytes, ownerScope, scope, volume, authority }),
      authority.volumeStorageName(volume), writable, objects);
  }

  /** Bind a reader and, only for its original owner, a publisher to typed tasks. */
  bindings(): ContentBindings {
    const base: ContentBindings = {
      validate: (file, limits) => { this.#validate(file, limits); },
      verify: (file, bytes) => this.options.authority.verifyFileBytes(file, bytes),
      read: file => this.read(file),
      fileReadCapability: file => this.options.authority.fileReadCapability(file),
      volumeReadCapability: volume => this.options.authority.volumeCapability(volume, "read"),
      directoryReadCapability: (volume, prefix) => this.options.authority.directoryReadCapability(volume, prefix),
      ...(this.#writable ? { writer: {
        volume: this.options.volume,
        writeCapability: () => this.options.authority.volumeCapability(this.options.volume, "write"),
        stage: (operationId: string, path: string, bytes: Uint8Array, mediaType: string, displayName: string) =>
          this.stage(operationId, path, bytes, mediaType, displayName),
      } } : {}),
    };
    return Object.freeze(base);
  }

  async stage(operationId: string, path: string, bytes: Uint8Array,
    mediaType: string, displayName: string): Promise<FileRef> {
    if (!this.#writable) throw new Error("Objects writer is not bound to the original owner");
    this.options.authority.verifyContentWrite(this.options.scope, this.options.volume);
    if (path === ".system" || path.startsWith(".system/")) {
      throw new TypeError("internal storage paths are reserved");
    }
    if (!operationId.trim() || !(bytes instanceof Uint8Array)
      || bytes.byteLength > this.options.maximumBytes) {
      throw new TypeError("Objects upload identity or size is invalid");
    }
    const descriptor = this.options.authority.fileDescriptor(bytes, mediaType);
    const content = this.#identity(descriptor, displayName);
    const reference = this.#validate({ volume: this.options.volume, path, version: content,
      descriptor, display_name: displayName });
    const objectKey = this.#key(path, content);
    await this.#pinUpload(operationId, reference);
    const header = create(PutObjectHeaderSchema, {
      bucket: this.options.bucket.bucket,
      objectKey,
      metadata: { contentType: mediaType, user: { "harness-display-name": displayName } },
      preconditions: { condition: { case: "ifAbsent", value: true } },
      mutation: { idempotencyKey: `harness-upload:${this.#namespace}:${operationId}` },
    });
    try {
      const published = await this.#objects.put(header, Uint8Array.from(bytes));
      if (published.size !== BigInt(bytes.byteLength)
        || published.metadata?.contentType !== mediaType
        || published.metadata?.user["harness-display-name"] !== displayName) {
        throw new Error("Objects publication returned inconsistent metadata");
      }
    } catch (error) {
      // Another operation may already have placed this exact content. Verify
      // the selected current bytes below; a retry receipt alone is not residency.
      if (!(error instanceof ObjectsV2Error) || error.code !== ErrorCode.PRECONDITION_FAILED) throw error;
    }
    await this.#readContent(reference);
    return reference;
  }

  async read(reference: FileRef): Promise<Uint8Array> {
    const file = this.#validate(reference);
    this.options.authority.verifyContentRead(this.options.scope, file);
    return this.#readContent(file);
  }

  async #readContent(file: FileRef): Promise<Uint8Array> {
    if (this.options.authority.volumeStorageName(file.volume) !== this.#namespace
      || file.descriptor.byte_length > this.options.maximumBytes) {
      throw new Error("file is outside this Objects binding");
    }
    if (file.version !== this.#identity(file.descriptor, file.display_name)) {
      throw new Error("Objects file content identity does not match its descriptor");
    }
    const request: PublicGetObjectRequest = {
      ...create(GetObjectRequestSchema, { bucket: this.options.bucket.bucket, objectKey: this.#key(file.path, file.version) }),
      objectKey: makeRustOwnedObjectKey(this.#key(file.path, file.version)),
    };
    const result = await this.#objects.get(request, BigInt(file.descriptor.byte_length));
    const info = result.header.object;
    if (info?.size !== BigInt(file.descriptor.byte_length)
      || info.metadata?.contentType !== file.descriptor.media_type
      || info.metadata?.user["harness-display-name"] !== file.display_name
      || result.header.contentRange !== undefined || !(result.body instanceof Uint8Array)) {
      throw new Error("Objects returned another file length, metadata or a partial body");
    }
    this.options.authority.verifyFileBytes(file, result.body);
    return Uint8Array.from(result.body);
  }

  /** Load a pinned list; a supplied resolver may route foreign refs to their owning provider. */
  async loadManifest(reference: FileRef, itemCount: number,
    resolve: (file: FileRef) => Promise<Uint8Array> = file => this.read(file)): Promise<readonly Attachment[]> {
    const items = this.options.authority.decodeAttachmentManifest(reference, await this.read(reference), itemCount);
    for (const item of items) this.options.authority.verifyFileBytes(item.file, await resolve(item.file));
    return items;
  }

  #identity(descriptor: FileRef["descriptor"], displayName: string): string {
    // Same Rust canonical JSON+BLAKE3 tuple as the native Harness adapter.
    return Array.from(this.options.authority.canonicalJsonDigest([descriptor, displayName]),
      byte => byte.toString(16).padStart(2, "0")).join("");
  }

  async #pinUpload(operationId: string, file: FileRef): Promise<void> {
    const intent = this.options.authority.canonicalJsonDigest([file.path, file.descriptor, file.display_name]);
    const operation = Array.from(this.options.authority.canonicalJsonDigest(["harness-upload-intent-v1", operationId]),
      byte => byte.toString(16).padStart(2, "0")).join("");
    const objectKey = `${this.#namespace}/.system/uploads/${operation}`;
    const contentType = "application/vnd.acyclic.harness.upload-intent-v1";
    try {
      await this.#objects.put(create(PutObjectHeaderSchema, {
        bucket: this.options.bucket.bucket, objectKey, metadata: { contentType },
        preconditions: { condition: { case: "ifAbsent", value: true } },
        mutation: { idempotencyKey: `harness-upload-intent:${this.#namespace}:${operationId}` },
      }), intent);
    } catch (error) {
      if (!(error instanceof ObjectsV2Error) || error.code !== ErrorCode.PRECONDITION_FAILED) throw error;
    }
    const pinnedRequest: PublicGetObjectRequest = {
      ...create(GetObjectRequestSchema, { bucket: this.options.bucket.bucket, objectKey }),
      objectKey: makeRustOwnedObjectKey(objectKey),
    };
    const pinned = await this.#objects.get(pinnedRequest, 32n);
    if (pinned.header.contentRange !== undefined || pinned.header.object?.size !== 32n
      || pinned.header.object.metadata?.contentType !== contentType
      || pinned.body.byteLength !== intent.byteLength
      || !pinned.body.every((byte, index) => byte === intent[index])) {
      throw new Error("upload identity belongs to different content");
    }
  }

  #key(path: string, content: string): string {
    if (!/^[0-9a-f]{64}$/.test(content)) throw new TypeError("Objects content identity is invalid");
    const key = `${this.#namespace}/${path}/@content/${content}`;
    if (encoder.encode(key).byteLength > maximumObjectKeyBytes) {
      throw new TypeError("Objects content key exceeds provider limit");
    }
    return key;
  }

  #validate(reference: FileRef, limits?: Limits): FileRef {
    const file = this.options.authority.validateFileRef(reference);
    if (limits !== undefined) this.options.authority.validateFileUnderLimits(file, limits);
    return file;
  }
}

function freezeScope(value: Scope): Scope {
  const scope = structuredClone(value);
  return Object.freeze({ ...scope,
    capabilities: Object.freeze([...scope.capabilities]),
    parent_proof: scope.parent_proof == null ? null : Object.freeze([...scope.parent_proof]),
    proof: Object.freeze([...scope.proof]),
  });
}
