/** Owner-authenticated, exact-version Objects content for Harness. */
import {
  type Attachment, type ContentBindings, type FileRef, type Harness,
  type Limits, type ProviderRef, type Scope, type VolumeClass, type VolumeRef,
} from "@acyclic-labs/harness";
import {
  idempotencyKey, type BucketRef, type ObjectMetadata,
  type ObjectsProvider, type VersionId,
} from "@acyclic-labs/objects";

const encoder = new TextEncoder();
const maximumObjectBytes = 5 * 1_024 ** 4;
const maximumObjectKeyBytes = 1_024;

export interface ObjectContentOptions {
  readonly objects: ObjectsProvider;
  readonly bucket: BucketRef;
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
  readonly #objects: Pick<ObjectsProvider, "headBucket" | "put" | "head" | "get">;

  private constructor(readonly options: ObjectContentOptions, namespace: string, writable: boolean,
    objects: Pick<ObjectsProvider, "headBucket" | "put" | "head" | "get">) {
    this.#namespace = namespace;
    this.#writable = writable;
    this.#objects = objects;
  }

  static async create(options: ObjectContentOptions): Promise<ObjectContentStore> {
    const authority = options.authority;
    const volume = authority.validateVolumeRef(options.volume);
    const ownerScope = freezeScope(options.ownerScope);
    const scope = freezeScope(options.scope);
    const bucket = Object.freeze({ ...options.bucket });
    const expectedProvider = Object.freeze({ ...options.expectedProvider });
    const maximumBytes = options.maximumBytes;
    const owner = options.objects;
    const objects = Object.freeze({
      headBucket: owner.headBucket.bind(owner), put: owner.put.bind(owner),
      head: owner.head.bind(owner), get: owner.get.bind(owner),
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
      || volume.id !== bucket.bucketId
      || !bucket.name || !Number.isSafeInteger(maximumBytes)
      || maximumBytes <= 0 || maximumBytes > maximumObjectBytes) {
      throw new TypeError("Objects content binding is invalid");
    }
    const resolved = await objects.headBucket(bucket);
    if (resolved.bucketId !== bucket.bucketId || resolved.name !== bucket.name) {
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
    this.#validate({ volume: this.options.volume, path, version: "staged", descriptor,
      display_name: displayName });
    const objectKey = this.#key(path);
    const retry = idempotencyKey(`harness-upload:${this.#namespace}:${operationId}`);
    const metadata: ObjectMetadata = {
      contentType: mediaType, contentEncoding: "", cacheControl: "",
      contentDisposition: "", contentLanguage: "", expiresUnixSeconds: undefined,
      user: new Map([["harness-display-name", displayName]]),
    };
    const version = await this.#objects.put(this.options.bucket, objectKey,
      Uint8Array.from(bytes), metadata, undefined, retry);
    if (version.deleteMarker || version.size !== BigInt(bytes.byteLength)
      || version.metadata.contentType !== mediaType
      || version.metadata.user.get("harness-display-name") !== displayName) {
      throw new Error("Objects publication returned inconsistent metadata");
    }
    const reference = this.#validate({ volume: this.options.volume, path, version: version.versionId,
      descriptor, display_name: displayName });
    const published = await this.#objects.get({ kind: "bucket", bucket: this.options.bucket },
      objectKey, version.versionId);
    if (published.version.versionId !== version.versionId || published.version.deleteMarker
      || published.version.size !== BigInt(bytes.byteLength)
      || published.version.metadata.contentType !== mediaType
      || published.version.metadata.user.get("harness-display-name") !== displayName
      || published.contentRange !== undefined || !(published.body instanceof Uint8Array)) {
      throw new Error("Objects publication did not resolve to the full staged version");
    }
    this.options.authority.verifyFileBytes(reference, published.body);
    return reference;
  }

  async read(reference: FileRef): Promise<Uint8Array> {
    const file = this.#validate(reference);
    if (this.options.authority.volumeStorageName(file.volume) !== this.#namespace
      || file.descriptor.byte_length > this.options.maximumBytes) {
      throw new Error("file is outside this Objects binding");
    }
    this.options.authority.verifyContentRead(this.options.scope, file);
    const objectKey = this.#key(file.path);
    const target = { kind: "bucket" as const, bucket: this.options.bucket };
    const version = await this.#objects.head(target, objectKey, { versionId: file.version as VersionId });
    if (version.deleteMarker || version.versionId !== file.version
      || version.size !== BigInt(file.descriptor.byte_length)
      || version.metadata.contentType !== file.descriptor.media_type
      || version.metadata.user.get("harness-display-name") !== file.display_name) {
      throw new Error("Objects returned another file version or media type");
    }
    const result = await this.#objects.get(target, objectKey, file.version as VersionId);
    if (result.version.versionId !== file.version || result.version.deleteMarker
      || result.version.size !== BigInt(file.descriptor.byte_length)
      || result.version.metadata.contentType !== file.descriptor.media_type
      || result.version.metadata.user.get("harness-display-name") !== file.display_name
      || result.contentRange !== undefined
      || !(result.body instanceof Uint8Array)) {
      throw new Error("Objects returned another file version or a partial body");
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

  #key(path: string): string {
    const key = `${this.#namespace}/${path}`;
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
