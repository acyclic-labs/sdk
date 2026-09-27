import type * as NativeBinding from "../generated/native/binding.js";
import type * as WasmBinding from "../generated/wasm/acyclic_fs_wasm.js";
import {
  DEFAULT_OBJECT_CACHE_OPTIONS as GENERATED_OBJECT_CACHE_OPTIONS,
  DEFAULT_VOLUME_LIMITS as GENERATED_VOLUME_LIMITS,
} from "../generated/defaults.js";

/**
 * The workspace and generation DTOs are emitted from the Rust WASM boundary.
 * Keep the customer-facing contracts readonly while deriving their field
 * names, optionality, and literal unions from that generated source.
 */
type ReadonlyDeep<T> = T extends (...args: never[]) => unknown
  ? T
  : T extends Uint8Array
    ? Uint8Array
  : T extends readonly (infer Value)[]
    ? readonly ReadonlyDeep<Value>[]
    : T extends object
      ? { readonly [Key in keyof T]: ReadonlyDeep<T[Key]> }
      : T;

/**
 * Project one generated browser DTO into the public API shape.  The WASM
 * boundary reports work counters as bigint so it cannot lose precision while
 * crossing the Rust boundary; the public adapters intentionally expose the
 * bounded counters as numbers after checking their range.  Keeping this
 * projection here makes every result DTO inherit its fields and discriminants
 * from the generated Rust contract instead of re-declaring them in TypeScript.
 */
type PublicWasm<T> = T extends WasmBinding.BrowserWorkCounters
  ? WorkCounters
  : T extends Uint8Array
    ? Uint8Array
    : T extends readonly (infer Value)[]
      ? readonly PublicWasm<Value>[]
      : T extends object
        ? { readonly [Key in keyof T]: PublicWasm<T[Key]> }
        : T;

export type FsProfile = "portable" | "posix" | "windows" | "browser";

export interface EngineCapabilities {
  readonly version: string;
  readonly platform: string;
  readonly architecture: string;
  readonly authority: "memory" | "indexeddb" | "local" | "remote";
  readonly immutableObjects: "memory" | "indexeddb" | "opfs" | "local" | "remote";
  readonly nativeMount: "none" | "linux-fuse" | "macos-nfs" | "windows-projfs";
  readonly writableNativeMount: boolean;
  readonly nativeWatch: boolean;
  readonly nativeWatchBackend:
    | "none"
    | "linux-inotify"
    | "macos-fsevents"
    | "windows-read-directory-changes"
    | "unsupported";
  readonly nativeWatchPersistentRestart: boolean;
  readonly nativeWatchRootIdentityFencing: boolean;
  readonly providerProcessIoObservable: boolean;
}

export interface FsEngine {
  readonly capabilities: EngineCapabilities;
  createWorkspace(name: string): Promise<FsWorkspace>;
  openWorkspace(name: string): Promise<FsWorkspace>;
  close(): void | Promise<void>;
}

/** Customer-side engine with the complete immutable-object, Volume, Checkout, and speculation surface. */
export interface FsVolumeEngine extends FsEngine {
  objectCacheStats(): ObjectCacheStats;
  clearObjectCache(): void;
  createSpeculation(volumeId: Uint8Array, generationId: Uint8Array, options: SpeculationOptions): Speculation;
  createVolume(options: VolumeOptions): Promise<FsVolume>;
  createVolumeWithId(volumeId: Uint8Array, options: VolumeOptions): Promise<FsVolume>;
  openVolume(volumeId: Uint8Array): Promise<FsVolume>;
  exportObject(objectId: Uint8Array, maximumBytes: bigint): Promise<FileReadResult>;
  importObject(objectId: Uint8Array, bytes: Uint8Array): Promise<MutationResult>;
  exportGenerationBatch(manifest: GenerationExportManifest, cursor: bigint, maximumObjects: number, maximumObjectBytes: bigint): Promise<GenerationTransferBatch>;
  importGenerationBatch(manifest: GenerationExportManifest, cursor: bigint, objects: readonly Uint8Array[], maximumObjects: number): Promise<GenerationTransferCursor>;
  restoreVolume(manifest: GenerationExportManifest, operationId: Uint8Array): Promise<FsVolume>;
}

export interface HostedFsOptions {
  readonly endpoint: string;
  readonly bearerToken: string;
  readonly maximumResponseBytes?: number;
  readonly fetch?: typeof globalThis.fetch;
}

export interface HostedFsCapabilities extends EngineCapabilities {
  readonly profiles: readonly FsProfile[];
  readonly maximumRequestBytes: bigint;
  readonly maximumResponseBytes: bigint;
  readonly maximumTransactionMutations: number;
  readonly maximumPageItems: number;
  readonly nativeMountCredentials: boolean;
  readonly s3Credentials: boolean;
  readonly sourceReconciliation: boolean;
}

declare const filesystemIdentity: unique symbol;
export type S3Bucket = string & { readonly [filesystemIdentity]: "S3Bucket" };
export type S3Region = string & { readonly [filesystemIdentity]: "S3Region" };
export type S3AccessKeyId = string & { readonly [filesystemIdentity]: "S3AccessKeyId" };
export type S3SecretAccessKey = string & { readonly [filesystemIdentity]: "S3SecretAccessKey" };
export type S3SessionToken = string & { readonly [filesystemIdentity]: "S3SessionToken" };

/** Scoped, expiring coordinates for the workspace's S3-compatible view. */
export interface S3Access {
  readonly endpoint: string;
  readonly expiresAtUnixSeconds: bigint;
  readonly bucket: S3Bucket;
  readonly region: S3Region;
  readonly accessKeyId: S3AccessKeyId;
  readonly secretAccessKey: S3SecretAccessKey;
  readonly sessionToken: S3SessionToken;
}

/** Hosted capability whose additional operation is backed by the canonical filesystem protocol. */
export interface HostedFsWorkspace extends FsWorkspace {
  s3Access(
    writable: boolean,
    expiresAfterSeconds: bigint,
    idempotencyKey?: Uint8Array,
  ): Promise<S3Access>;
  fork(destination: string): Promise<HostedFsWorkspace>;
  forkAt(destination: string, generation: FsGeneration): Promise<HostedFsWorkspace>;
  sourceState(): Promise<SourceResult>;
  reconcileSource(idempotencyKey?: Uint8Array): Promise<SourceResult>;
  rescanSource(idempotencyKey?: Uint8Array): Promise<SourceResult>;
  seal(idempotencyKey?: Uint8Array): Promise<FsGeneration>;
}

export interface HostedFsEngine extends FsEngine {
  readonly capabilities: HostedFsCapabilities;
  createWorkspace(name: string): Promise<HostedFsWorkspace>;
  openWorkspace(name: string): Promise<HostedFsWorkspace>;
}

export type WorkspaceCommitStatus = WasmBinding.BrowserWorkspaceCommit["status"];
export type WorkspaceCommit = ReadonlyDeep<WasmBinding.BrowserWorkspaceCommit>;
export type WorkspaceFileKind = WasmBinding.BrowserWorkspaceDirectoryEntry["kind"];
export type WorkspaceNameEncoding = WasmBinding.BrowserWorkspaceName["encoding"];
export type WorkspaceName = ReadonlyDeep<WasmBinding.BrowserWorkspaceName>;
export type WorkspaceMetadata = ReadonlyDeep<WasmBinding.BrowserWorkspaceMetadata>;
export type WorkspaceStat = ReadonlyDeep<WasmBinding.BrowserWorkspaceStat>;
export type WorkspaceDirectoryEntry = ReadonlyDeep<WasmBinding.BrowserWorkspaceDirectoryEntry>;
export type WorkspaceDirectoryPage = ReadonlyDeep<WasmBinding.BrowserWorkspaceDirectoryPage>;
export type WorkspaceExtentKind = WasmBinding.BrowserWorkspaceExtentSpan["kind"];
export type WorkspaceExtentSpan = ReadonlyDeep<WasmBinding.BrowserWorkspaceExtentSpan>;
export type WorkspaceExtentPlan = ReadonlyDeep<WasmBinding.BrowserWorkspaceExtentPlan>;

/** Small customer-facing handle over one independently versioned filesystem. */
export interface FsWorkspace {
  readonly name: string;
  readonly id: Uint8Array;
  head(): Promise<Uint8Array>;
  /** Observe the current immutable head for pinned reads and directory pagination. */
  sync(): Promise<FsGeneration>;
  checkpoint(label: string): Promise<FsGeneration>;
  pin(identity: string): Promise<FsGeneration>;
  delete(idempotencyKey?: Uint8Array): Promise<WorkspaceDeleteStatus>;
  read(path: string, maximumBytes: bigint): Promise<Uint8Array>;
  readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
  stat(path: string): Promise<WorkspaceStat>;
  readSymbolicLink(path: string): Promise<Uint8Array>;
  planExtents(path: string, offset: bigint, length: bigint, maximumSpans: number): Promise<WorkspaceExtentPlan>;
  write(path: string, bytes: Uint8Array): Promise<WorkspaceCommit>;
  remove(path: string): Promise<WorkspaceCommit>;
  fork(destination: string, idempotencyKey?: Uint8Array): Promise<FsWorkspace>;
  forkAt(destination: string, generation: FsGeneration): Promise<FsWorkspace>;
  beginTransaction(idempotencyKey?: Uint8Array): Promise<FsTransaction>;
  liveRebase(options: WorkspaceRebaseOptions, idempotencyKey?: Uint8Array): Promise<WorkspaceRebaseResult>;
  diff(from: FsGeneration, to: FsGeneration, maximumChanges: number): Promise<FsChangeSet>;
  joinInto(target: FsWorkspace, options: JoinOptions): Promise<FsJoinPlan>;
}

export type WorkspaceDeleteStatus =
  | "deleted"
  | "already-deleted"
  | "conflict"
  | "idempotency-conflict";

/** One exact immutable complete filesystem state. */
export interface FsGeneration {
  readonly id: Uint8Array;
  readonly workspaceId: Uint8Array;
  read(path: string, maximumBytes: bigint): Promise<Uint8Array>;
  readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
  stat(path: string): Promise<WorkspaceStat>;
  listDirectory(path: string, after: WorkspaceName | undefined, maximumEntries: number): Promise<WorkspaceDirectoryPage>;
  readSymbolicLink(path: string): Promise<Uint8Array>;
  planExtents(path: string, offset: bigint, length: bigint, maximumSpans: number): Promise<WorkspaceExtentPlan>;
  pin(identity: string): Promise<FsGeneration>;
}

/** One immutable semantic delta between exact generations. */
export interface FsChangeSet {
  readonly from: FsGeneration;
  readonly to: FsGeneration;
  changes(): GenerationDiff;
  compose(next: FsChangeSet, maximumChanges: number): Promise<FsChangeSet>;
}

export type JoinHistory = "merge" | "rebase" | "squash" | "cherry-pick";

export interface JoinOptions {
  readonly history: JoinHistory;
  readonly maximumGenerations: number;
  readonly maximumChanges: number;
  readonly maximumConflicts: number;
}

export type JoinStatus = WasmBinding.BrowserJoinResult["status"];

export interface JoinResult {
  readonly status: JoinStatus;
  readonly generationId: Uint8Array | undefined;
  readonly conflicts: readonly MergeConflict[];
  readonly truncated: boolean;
}

export interface WorkspaceRebaseOptions {
  readonly maximumGenerations: number;
  readonly maximumChanges: number;
  readonly maximumConflicts: number;
}

export type WorkspaceRebaseStatus = WasmBinding.BrowserWorkspaceRebaseResult["status"];

export interface WorkspaceRebaseResult {
  readonly status: WorkspaceRebaseStatus;
  readonly generationId: Uint8Array | undefined;
  readonly conflicts: readonly MergeConflict[];
  readonly truncated: boolean;
}

/** Immutable side-effect-free plan; only apply may publish one target generation. */
export interface FsJoinPlan {
  readonly targetHead: Uint8Array;
  readonly commonAncestor: Uint8Array;
  apply(ifTarget: Uint8Array, idempotencyKey?: Uint8Array): Promise<JoinResult>;
  close(): Promise<void>;
}

/** Native join plan with declarative, generation-fenced conflict selection. */
export interface ResolvableFsJoinPlan extends FsJoinPlan {
  applySides(
    ifTarget: Uint8Array,
    selections: readonly MergeConflictSelection[],
    idempotencyKey?: Uint8Array,
  ): Promise<JoinResult>;
}

export type TransactionConflictRegionKind =
  | "file-record" | "metadata" | "file-length" | "content-range"
  | "sparse-seek" | "directory-name" | "directory-range";
export type TransactionDependencyUse =
  | "observation" | "mutation" | "observation-and-mutation";
export interface TransactionConflict {
  readonly region: TransactionConflictRegionKind;
  readonly fileId: Uint8Array | undefined;
  readonly directoryId: Uint8Array | undefined;
  readonly offset: bigint | undefined;
  readonly length: bigint | undefined;
  readonly sparseTarget: "data" | "hole" | undefined;
  readonly name: WorkspaceName | undefined;
  readonly maximumEntries: number | undefined;
  readonly usage: TransactionDependencyUse;
  readonly expected: Uint8Array | undefined;
  readonly actual: Uint8Array | undefined;
}
export interface TransactionRebaseResult {
  readonly status: "rebased" | "conflicted";
  readonly generationId: Uint8Array | undefined;
  readonly conflicts: readonly TransactionConflict[];
  readonly truncated: boolean;
}

/** One sparse candidate published atomically as a single immutable generation. */
export interface FsTransaction {
  createDirAll(path: string): Promise<void>;
  createDirectory(path: string): Promise<void>;
  createSymbolicLink(path: string, target: Uint8Array): Promise<void>;
  write(path: string, bytes: Uint8Array): Promise<void>;
  remove(path: string): Promise<void>;
  copy(source: string, destination: string): Promise<void>;
  rename(source: string, destination: string): Promise<void>;
  hardLink(source: string, destination: string): Promise<void>;
  writeRange(path: string, offset: bigint, bytes: Uint8Array): Promise<void>;
  resize(path: string, logicalBytes: bigint): Promise<void>;
  zeroRange(
    path: string,
    offset: bigint,
    length: bigint,
    allocated: boolean,
    extend: boolean,
  ): Promise<void>;
  preallocate(path: string, offset: bigint, length: bigint, keepSize: boolean): Promise<void>;
  cloneRange(
    source: string,
    sourceOffset: bigint,
    destination: string,
    destinationOffset: bigint,
    length: bigint,
  ): Promise<void>;
  rebase(maximumConflicts: number): Promise<TransactionRebaseResult>;
  commit(): Promise<WorkspaceCommit>;
  close(): Promise<void>;
}

export type StorageTier = "process-memory" | "node-local" | "shared-cache" | "durable-origin";
export type ResidencyReason =
  | "directory-successor"
  | "sequential-range"
  | "metadata-successor"
  | "consumer-hint";

export interface SpeculationOptions {
  readonly residency: {
    readonly maximumActiveOperations: number;
    readonly maximumActiveBytes: bigint;
    readonly outcomeWindow: number;
    readonly trafficWindow: number;
    readonly speculativeCostBasisPoints: number;
    readonly minimumUsefulnessSamples: number;
    readonly minimumUsefulnessBasisPoints: number;
  };
  readonly promotion: {
    readonly maximumActiveOperations: number;
    readonly maximumActiveBytes: bigint;
    readonly maximumActiveCostUnits: bigint;
    readonly maximumResidencyFacts: number;
    readonly maximumDestinations: number;
    readonly maximumAcceptedTiers: number;
    readonly outcomeWindow: number;
    readonly minimumUsefulnessSamples: number;
    readonly minimumUsefulnessBasisPoints: number;
  };
}

export interface ResidencyObservation {
  readonly operationId: Uint8Array;
  readonly volumeId: Uint8Array;
  readonly generationId: Uint8Array;
  readonly foregroundBytes: bigint;
  readonly objectId: Uint8Array;
  readonly maximumBytes: bigint;
  readonly reason: ResidencyReason;
}

export interface ObjectResidency {
  readonly objectId: Uint8Array;
  readonly locationId: Uint8Array;
  readonly tier: StorageTier;
  readonly sourcePriority: number;
}

export interface PromotionDestination {
  readonly locationId: Uint8Array;
  readonly tier: StorageTier;
  readonly writable: boolean;
  readonly maximumObjectBytes: bigint;
  readonly priority: number;
  readonly costUnitsPerByte: bigint;
}

export interface PromotionRequest {
  readonly operationId: Uint8Array;
  readonly acceptedTiers: readonly StorageTier[];
  readonly residency: readonly ObjectResidency[];
  readonly destinations: readonly PromotionDestination[];
}

export interface SpeculationPreemption {
  readonly residencyOperationIds: readonly Uint8Array[];
  readonly promotionOperationIds: readonly Uint8Array[];
}

export interface SpeculationMetrics {
  readonly residency: Readonly<Record<string, bigint>>;
  readonly promotion: Readonly<Record<string, bigint>>;
}

export interface Speculation {
  observe(observation: ResidencyObservation): Promise<{ readonly status: string; readonly rejection?: string }>;
  executeResidency(operationId: Uint8Array): Promise<{ readonly objectBytes: bigint; readonly work: WorkCounters }>;
  finishResidency(operationId: Uint8Array, useful: boolean): Promise<void>;
  planPromotion(request: PromotionRequest): Promise<{
    readonly status: string;
    readonly rejection?: string;
    readonly operationId?: Uint8Array;
    readonly objectId?: Uint8Array;
    readonly sourceLocationId?: Uint8Array;
    readonly destinationLocationId?: Uint8Array;
    readonly estimatedCostUnits?: bigint;
  }>;
  finishPromotion(operationId: Uint8Array, useful: boolean): Promise<void>;
  preemptForForeground(bytes: bigint): Promise<SpeculationPreemption>;
  replaceGeneration(generationId: Uint8Array): Promise<SpeculationPreemption>;
  metrics(): Promise<SpeculationMetrics>;
  cancel(): void;
}

export interface VolumeOptions {
  readonly profile: FsProfile;
  readonly concurrency: "exclusive-writer" | "optimistic" | "serialized-authority";
  readonly lifecycle: "ephemeral" | "durable";
  readonly caseSensitivity: "sensitive" | "profile-folded";
  readonly unicode: "preserve" | "require-nfc";
  readonly symbolicLinks: boolean;
  readonly hardLinks: boolean;
  readonly sparseFiles: boolean;
  readonly limits: VolumeLimits;
}

export interface VolumeLimits {
  readonly maximumPathBytes: number;
  readonly maximumComponentBytes: number;
  readonly maximumPathDepth: number;
  readonly maximumObjectBytes: bigint;
  readonly maximumMutationsPerBatch: number;
  readonly maximumPathsPerBatch: number;
  readonly maximumCheckoutDependencies: number;
  readonly maximumDirectoryPageEntries: number;
  readonly maximumPageHeight: number;
  readonly maximumReadBytes: bigint;
  readonly maximumFilesPerGeneration: bigint;
  readonly maximumObjectsPerGeneration: bigint;
  readonly maximumGenerationBytes: bigint;
}

export const DEFAULT_VOLUME_LIMITS: VolumeLimits = GENERATED_VOLUME_LIMITS;

export function portableVolumeOptions(
  lifecycle: VolumeOptions["lifecycle"],
): VolumeOptions {
  return {
    profile: "portable",
    concurrency: "optimistic",
    lifecycle,
    caseSensitivity: "sensitive",
    unicode: "preserve",
    symbolicLinks: true,
    hardLinks: true,
    sparseFiles: true,
    limits: DEFAULT_VOLUME_LIMITS,
  };
}

export interface CheckoutOptions {
  readonly access: "read-only" | "read-write";
  readonly consistency: "pinned" | "tracking-safe" | "live" | "manual";
  readonly mutationMode: "none" | "private-cow" | "direct-live";
}

/** Bounded customer-side counters projected from the generated Rust DTO. */
export type WorkCounters = Readonly<{
  [Key in keyof WasmBinding.BrowserWorkCounters]: number;
}>;

export type LookupResult = PublicWasm<WasmBinding.BrowserLookupResult>;

export type FileReadResult = PublicWasm<WasmBinding.BrowserFileReadResult>;

/** One immutable file handle resolved against a pinned checkout generation. */
export interface ResolvedFile {
  readonly kind: string;
  readonly logicalBytes: bigint;
  readonly metadataCanonicalBytes: Uint8Array;
  readRange(offset: bigint, length: bigint): Promise<FileReadResult>;
  readSymbolicLink(): Promise<FileReadResult>;
}

/** Original-order handles and the shared namespace-resolution work receipt. */
export interface ResolvedFilesResult {
  readonly files: readonly (ResolvedFile | undefined)[];
  readonly work: WorkCounters;
}

export type ExtentSeekTarget = "data" | "hole";

export interface ExtentSeekResult {
  readonly offset: bigint | undefined;
  readonly work: WorkCounters;
}

export type FileExtentSpan =
  | {
      readonly kind: "hole" | "allocated-zero";
      readonly offset: bigint;
      readonly length: bigint;
      readonly sourceEnd: bigint;
    }
  | {
      readonly kind: "content";
      readonly offset: bigint;
      readonly length: bigint;
      readonly sourceEnd: bigint;
      readonly objectId: Uint8Array;
      readonly objectOffset: bigint;
    };

/** Exact sparse representation without reading file bodies. */
export type FileExtentPlan =
  | { readonly kind: "inline"; readonly work: WorkCounters }
  | {
      readonly kind: "sparse";
      readonly spans: readonly FileExtentSpan[];
      readonly retainedAllocationBytes: bigint;
      readonly work: WorkCounters;
    };

export type DirectoryEntry = PublicWasm<WasmBinding.BrowserDirectoryEntryResult>;

export type DirectoryPage = PublicWasm<WasmBinding.BrowserDirectoryPageResult>;

export type MutationResult = PublicWasm<WasmBinding.BrowserMutationResult>;

/** Immutable content-addressed candidate built without publishing authority. */
export type CheckpointResult = PublicWasm<WasmBinding.BrowserCheckpointResult>;

export interface MaterializeOptions {
  readonly destination: string;
  readonly maximumDirectoryEntries: number;
  readonly maximumExtentSpans: number;
  readonly transferBytes: bigint;
}

export interface MaterializationResult {
  readonly files: bigint;
  readonly directories: bigint;
  readonly symbolicLinks: bigint;
  readonly specialFiles: bigint;
  readonly logicalFileBytes: bigint;
  readonly writtenBytes: bigint;
  readonly work: WorkCounters;
}

export interface CaptureResult {
  readonly examinedPaths: bigint;
  readonly changedPaths: bigint;
  readonly stagedFileBytes: bigint;
  readonly work: WorkCounters;
}

/** Capture result whose watcher interval is now safe to acknowledge durably. */
export interface WatchCaptureResult extends CaptureResult {
  readonly epoch: bigint;
  readonly firstSequence: bigint;
  readonly nextSequence: bigint;
}

/** Watcher-bound authenticated baseline and concurrent post-baseline interval. */
export interface WatchReconcileResult {
  readonly epoch: bigint;
  readonly baseline: CaptureResult;
  readonly postBaseline: NativeWatchBatch;
}

export interface NativePathComponent {
  readonly encoding: "utf8" | "posix-bytes" | "windows-utf16le";
  readonly bytes: Uint8Array;
}

export interface NativeNamespacePath {
  readonly components: readonly NativePathComponent[];
}

export type NativeWatchChange =
  | { readonly kind: "created" | "modified" | "arrived" | "metadata" | "removed"; readonly path: NativeNamespacePath }
  | { readonly kind: "renamed"; readonly from: NativeNamespacePath; readonly to: NativeNamespacePath };

export type NativeWatchBatch =
  | {
      readonly status: "changes";
      readonly epoch: bigint;
      readonly firstSequence: bigint;
      readonly nextSequence: bigint;
      readonly changes: readonly NativeWatchChange[];
      readonly work: WorkCounters;
    }
  | {
      readonly status: "rescan-required";
      readonly epoch: bigint;
      readonly reason:
        | "initial-snapshot-required"
        | "queue-overflow"
        | "native-rescan-required"
        | "backend-error"
        | "unrepresentable-path"
        | "ambiguous-rename"
        | "root-changed";
      readonly work: WorkCounters;
    };

export interface NativeWatcher {
  reconcile(maximumPaths: number, maximumExtentSpans: number): Promise<WatchReconcileResult>;
  pollCapture(
    maximumChanges: number,
    maximumPaths: number,
    maximumExtentSpans: number,
  ): Promise<WatchCaptureResult>;
}

/** Complete authenticated closure descriptor. Object identities are kind-tag + digest. */
export type GenerationExportManifest = PublicWasm<WasmBinding.BrowserExportManifestResult>;

/** One manifest-ordered, resumable immutable-object transfer page. */
export type GenerationTransferBatch = PublicWasm<WasmBinding.GenerationTransferBatchResult>;

/** Cursor after an idempotently imported manifest-aligned page. */
export type GenerationTransferCursor = PublicWasm<WasmBinding.GenerationTransferCursorResult>;

export type CommitResult = PublicWasm<WasmBinding.BrowserCommitResult>;

export type RebaseResult = PublicWasm<WasmBinding.BrowserRebaseResult>;

/** Complete path-independent file record in a generation diff. */
export type FileRecordSnapshot = PublicWasm<WasmBinding.BrowserFileRecordResult>;

export type FileRecordReadResult = PublicWasm<WasmBinding.BrowserFileRecordReadResult>;

export type BatchLookupEntry = PublicWasm<WasmBinding.BrowserBatchLookupEntryResult>;

export type BatchLookupResult = PublicWasm<WasmBinding.BrowserBatchLookupResult>;

export type DirectoryRecordEntry = PublicWasm<WasmBinding.BrowserDirectoryRecordEntryResult>;

export type DirectoryRecordPage = PublicWasm<WasmBinding.BrowserDirectoryRecordPageResult>;

export type FileRecordChange = PublicWasm<WasmBinding.BrowserFileRecordChangeResult>;

export type TreeEntrySnapshot = PublicWasm<WasmBinding.BrowserTreeEntryResult>;

export type DirectoryBindingChange = PublicWasm<WasmBinding.BrowserBindingChangeResult>;

export type GenerationDiff = PublicWasm<WasmBinding.BrowserGenerationDiffResult>;

export type MergeConflict =
  | { readonly kind: "file"; readonly fileId: Uint8Array }
  | {
      readonly kind: "binding";
      readonly directoryId: Uint8Array;
      readonly name: NativePathComponent;
    };

export type MergeConflictSelection = MergeConflict & {
  readonly side: "base" | "ours" | "theirs";
};

export type MergePreparationResult =
  | {
      readonly status: "prepared";
      readonly generationId: Uint8Array;
      readonly conflicts: readonly [];
      readonly truncated: false;
      readonly work: WorkCounters;
    }
  | {
      readonly status: "conflicted";
      readonly generationId: undefined;
      readonly conflicts: readonly MergeConflict[];
      readonly truncated: boolean;
      readonly work: WorkCounters;
    };

export type LiveMutationResult = PublicWasm<WasmBinding.BrowserLiveMutationResult>;

export type LiveTransactionResult = PublicWasm<WasmBinding.BrowserLiveTransactionResult>;

export type NamedAttributeClass = "posix-xattr" | "windows-stream" | "mac-resource-fork";

export type MetadataResult = PublicWasm<WasmBinding.BrowserMetadataResult>;

export type StatResult = PublicWasm<WasmBinding.BrowserStatResult>;

export type NamedAttributeResult = PublicWasm<WasmBinding.BrowserNamedAttributeResult>;

export type NamedAttributePage = PublicWasm<WasmBinding.BrowserNamedAttributePageResult>;

export type NamedAttributeName = PublicWasm<WasmBinding.BrowserNamedAttributeNameResult>;

export type NamedAttributeWriteMode = "upsert" | "create" | "replace";
export type EmptySpecialKind = "fifo" | "socket" | "mount-boundary";
export type DeviceKind = "character-device" | "block-device";

export type TransactionOperation = ReadonlyDeep<WasmBinding.TransactionOperation>;

export type TransactionResult = PublicWasm<WasmBinding.BrowserTransactionResult>;

export interface FsCheckout {
  /** Exact bounded work used to acquire this checkout handle. */
  readonly acquisitionWork: WorkCounters;
  applyTransaction(operations: readonly TransactionOperation[]): Promise<TransactionResult>;
  /**
   * Builds an immutable content-addressed candidate only. Implementations MUST NOT publish it or
   * change the checkout's pending mutation state, so callers may safely checkpoint independently.
   */
  checkpoint(): Promise<CheckpointResult>;
  refreshHead(): Promise<CheckpointResult>;
  refreshLive(): Promise<CheckpointResult>;
  exportManifest(): Promise<GenerationExportManifest>;
  prepareMerge(
    theirs: Uint8Array,
    maximumChanges: number,
    maximumConflicts: number,
  ): Promise<MergePreparationResult>;
  lookupNoFollow(path: string): Promise<LookupResult>;
  lookupBatchNoFollow(paths: readonly string[]): Promise<BatchLookupResult>;
  statNoFollow(path: string): Promise<StatResult>;
  readFileRecordById(fileId: Uint8Array): Promise<FileRecordReadResult>;
  readMetadata(path: string): Promise<MetadataResult>;
  readMetadataById(fileId: Uint8Array): Promise<MetadataResult>;
  setMetadata(path: string, canonicalBytes: Uint8Array): Promise<MutationResult>;
  setMetadataById(fileId: Uint8Array, canonicalBytes: Uint8Array): Promise<MutationResult>;
  setAttributes(path: string, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined): Promise<MutationResult>;
  setAttributesById(fileId: Uint8Array, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined): Promise<MutationResult>;
  readNamedAttribute(
    path: string,
    attributeClass: NamedAttributeClass,
    name: Uint8Array,
  ): Promise<NamedAttributeResult>;
  listNamedAttributes(
    path: string,
    after: NamedAttributeName | undefined,
    maximumEntries: number,
  ): Promise<NamedAttributePage>;
  writeNamedAttribute(
    path: string,
    attributeClass: NamedAttributeClass,
    name: Uint8Array,
    bytes: Uint8Array,
    mode: NamedAttributeWriteMode,
  ): Promise<MutationResult>;
  removeNamedAttribute(
    path: string,
    attributeClass: NamedAttributeClass,
    name: Uint8Array,
  ): Promise<MutationResult>;
  resolveFiles(paths: readonly string[]): Promise<ResolvedFilesResult>;
  readFileRange(path: string, offset: bigint, length: bigint): Promise<FileReadResult>;
  readFileRangeById(fileId: Uint8Array, offset: bigint, length: bigint): Promise<FileReadResult>;
  planFileExtents(
    path: string,
    offset: bigint,
    length: bigint,
    maximumSpans: number,
  ): Promise<FileExtentPlan>;
  planFileExtentsById(
    fileId: Uint8Array,
    offset: bigint,
    length: bigint,
    maximumSpans: number,
  ): Promise<FileExtentPlan>;
  seekFileExtent(path: string, offset: bigint, target: ExtentSeekTarget): Promise<ExtentSeekResult>;
  seekFileExtentById(fileId: Uint8Array, offset: bigint, target: ExtentSeekTarget): Promise<ExtentSeekResult>;
  readSymbolicLink(path: string): Promise<FileReadResult>;
  readReparsePoint(path: string): Promise<FileReadResult>;
  listDirectory(
    path: string,
    after: string | undefined,
    maximumEntries: number,
  ): Promise<DirectoryPage>;
  listDirectoryRecords(
    path: string,
    after: string | undefined,
    maximumEntries: number,
  ): Promise<DirectoryRecordPage>;
  createFile(path: string, bytes: Uint8Array): Promise<MutationResult>;
  createDirectory(path: string): Promise<MutationResult>;
  createSymbolicLink(path: string, target: Uint8Array): Promise<MutationResult>;
  createSpecial(path: string, kind: EmptySpecialKind): Promise<MutationResult>;
  createDevice(path: string, kind: DeviceKind, major: number, minor: number): Promise<MutationResult>;
  createReparsePoint(path: string, payload: Uint8Array): Promise<MutationResult>;
  writeFile(path: string, offset: bigint, bytes: Uint8Array): Promise<MutationResult>;
  writeFileById(fileId: Uint8Array, offset: bigint, bytes: Uint8Array): Promise<MutationResult>;
  remove(path: string, expectedFileId: Uint8Array | undefined): Promise<MutationResult>;
  rename(source: string, destination: string, replace: boolean): Promise<MutationResult>;
  hardLink(source: string, destination: string): Promise<MutationResult>;
  resizeFile(path: string, logicalBytes: bigint): Promise<MutationResult>;
  resizeFileById(fileId: Uint8Array, logicalBytes: bigint): Promise<MutationResult>;
  zeroFileRange(
    path: string,
    offset: bigint,
    length: bigint,
    allocated: boolean,
    extend: boolean,
  ): Promise<MutationResult>;
  zeroFileRangeById(
    fileId: Uint8Array,
    offset: bigint,
    length: bigint,
    allocated: boolean,
    extend: boolean,
  ): Promise<MutationResult>;
  preallocateFile(
    path: string,
    offset: bigint,
    length: bigint,
    keepSize: boolean,
  ): Promise<MutationResult>;
  preallocateFileById(
    fileId: Uint8Array,
    offset: bigint,
    length: bigint,
    keepSize: boolean,
  ): Promise<MutationResult>;
  cloneFileRange(
    source: string,
    sourceOffset: bigint,
    destination: string,
    destinationOffset: bigint,
    length: bigint,
  ): Promise<MutationResult>;
  cloneFileRangeById(
    sourceFileId: Uint8Array,
    sourceOffset: bigint,
    destinationFileId: Uint8Array,
    destinationOffset: bigint,
    length: bigint,
  ): Promise<MutationResult>;
  commit(operationId: Uint8Array): Promise<CommitResult>;
  mutateLive(
    operations: readonly TransactionOperation[],
    operationId: Uint8Array,
    maximumAttempts: number,
    maximumConflicts: number,
  ): Promise<LiveTransactionResult>;
  resumeLive(
    operationId: Uint8Array,
    maximumAttempts: number,
    maximumConflicts: number,
  ): Promise<LiveMutationResult>;
  rebaseHead(maximumConflicts: number): Promise<RebaseResult>;
  discard(): Promise<MutationResult>;
  mount?(destination: string, writable: boolean): NativeMount;
  materialize?(options: MaterializeOptions): Promise<MaterializationResult>;
  capture?(
    sourceRoot: string,
    paths: readonly string[],
    maximumPaths: number,
    maximumExtentSpans: number,
  ): Promise<CaptureResult>;
  captureBaseline?(
    sourceRoot: string,
    maximumPaths: number,
    maximumExtentSpans: number,
  ): Promise<CaptureResult>;
  watch?(
    sourceRoot: string,
    maximumQueuedChanges: number,
    recursive: boolean,
  ): NativeWatcher;
  cancel?(): void;
}

export interface NativeMount {
  readonly id: Uint8Array;
  readonly destination: string;
  /**
   * Waits until the mount reflects every change to its checkout made so far,
   * including changes made through the checkout rather than the mount. Those
   * reach the mount on their own shortly after; call this to read one through
   * the mount immediately.
   */
  revalidate(): void;
  stop(): boolean;
}

export interface FsVolume {
  readonly id: Uint8Array;
  /** Exact bounded work used to create, restore, or open this volume handle. */
  readonly acquisitionWork: WorkCounters;
  diffGenerations(
    before: Uint8Array,
    after: Uint8Array,
    maximumChanges: number,
  ): Promise<GenerationDiff>;
  checkout(options: CheckoutOptions): Promise<FsCheckout>;
}

export interface BrowserFsOptions {
  readonly databaseName: string;
  readonly maximumObjectBytes: number;
  readonly objectAcceleration: "indexeddb" | "opfs";
  readonly objectCache: ObjectCacheOptions;
}

export interface MemoryFsOptions {
  readonly maximumObjectBytes: number;
  readonly maximumMemoryBytes: number;
  readonly objectCache: ObjectCacheOptions;
}

export interface NativeFsOptions {
  readonly root: string;
  readonly objectCache: ObjectCacheOptions;
}

export interface ObjectCacheOptions {
  readonly maximumEntries: number;
  readonly maximumBytes: number;
  readonly maximumInFlight: number;
  readonly maximumWaitersPerObject: number;
}

const generatedCacheBytes = Number(GENERATED_OBJECT_CACHE_OPTIONS.maximumBytes);
if (!Number.isSafeInteger(generatedCacheBytes)) {
  throw new RangeError("Rust object cache default exceeds JavaScript's safe integer range");
}

export const DEFAULT_OBJECT_CACHE_OPTIONS: ObjectCacheOptions = Object.freeze({
  ...GENERATED_OBJECT_CACHE_OPTIONS,
  maximumBytes: generatedCacheBytes,
});

export interface ObjectCacheStats {
  readonly hits: bigint;
  readonly decodedHits: bigint;
  readonly misses: bigint;
  readonly coalescedReads: bigint;
  readonly evictions: bigint;
  readonly residentEntries: bigint;
  readonly residentBytes: bigint;
  readonly residentCanonicalObjects: bigint;
  readonly residentCanonicalBytes: bigint;
  readonly residentDecodedPages: bigint;
  readonly residentDecodedBytes: bigint;
  readonly inFlight: bigint;
}

export interface NativeFsEngine extends FsVolumeEngine {
  createWorkspace(name: string): Promise<NativeFsWorkspace>;
  openWorkspace(name: string): Promise<NativeFsWorkspace>;
  attachDirectory(
    name: string,
    path: string,
    options: NativeSourceOptions,
  ): Promise<NativeFsWorkspace>;
  cancel(): void;
  readonly cancelled: boolean;
}

export type NativeWorkspaceMountPublication = "close-and-sync" | "per-mutation" | "manual";

export interface NativeWorkspaceMountOptions {
  readonly writable: boolean;
  readonly subdirectory: string;
  readonly publication: NativeWorkspaceMountPublication;
}

export interface NativeWorkspaceMount {
  readonly path: string;
  sync(): Promise<void>;
  unmount(): Promise<boolean>;
}

export interface NativeFsWorkspace extends FsWorkspace {
  joinInto(target: FsWorkspace, options: JoinOptions): Promise<ResolvableFsJoinPlan>;
  mount(destination: string, options: NativeWorkspaceMountOptions): Promise<NativeWorkspaceMount>;
  sourceState(): Promise<SourceResult>;
  reconcileSource(): Promise<SourceResult>;
  rescanSource(): Promise<SourceResult>;
  seal(): Promise<FsGeneration>;
}

export interface NativeSourceOptions {
  readonly mode: "pinned" | "tracking";
  readonly maximumPaths: number;
  readonly maximumExtentSpans: number;
  readonly maximumQueuedChanges: number;
  /** Canonical absolute prefixes omitted from capture and deletion inference. */
  readonly excludedPaths?: readonly string[];
}

export type SourceStatus =
  | "none"
  | "clean"
  | "pending-capture"
  | "needs-rescan"
  | "conflict"
  | "sealed";

export type SourceInvalidationReason =
  | "initial-snapshot-required"
  | "queue-overflow"
  | "native-rescan-required"
  | "backend-error"
  | "unrepresentable-path"
  | "ambiguous-rename"
  | "root-changed";

export interface SourceResult {
  readonly status: SourceStatus;
  readonly reason: SourceInvalidationReason | undefined;
  readonly generationId: Uint8Array | undefined;
}

/*
 * Keep the module-level factory surface tied to wasm-bindgen's generated
 * exports. The only refinements here are the serde option/result types that
 * this package validates at its adapter boundary.
 */
type GeneratedWasmFactories = Pick<
  typeof WasmBinding,
  | "default"
  | "encodeMergePlanJson"
  | "decodeMergePlanJson"
  | "encodeMergeCandidateJson"
  | "decodeMergeCandidateJson"
  | "encodeMultiRootPlanJson"
  | "decodeMultiRootPlanJson"
  | "encodeMultiRootCandidateJson"
  | "decodeMultiRootCandidateJson"
  | "encodePublicationJson"
  | "decodePublicationJson"
>;

export type WasmBindings = GeneratedWasmFactories & {
  openBrowserFs(options: BrowserFsOptions): Promise<WasmRawFs>;
  openMemoryFs(options: MemoryFsOptions): WasmRawFs;
  readonly BrowserWorkspaceContextRegistry: typeof WasmBinding.BrowserWorkspaceContextRegistry;
};

export interface RawWorkspaceContextRegistry {
  registerRoot(contextId: Uint8Array, rootsWire: Uint8Array): Promise<Uint8Array>;
  registerChild(
    contextId: Uint8Array,
    parentContextId: Uint8Array,
    rootsWire: Uint8Array,
  ): Promise<Uint8Array>;
  adoptRoot(contextId: Uint8Array, rootWire: Uint8Array): Promise<Uint8Array>;
  removeRoot(contextId: Uint8Array, rootId: Uint8Array): Promise<Uint8Array>;
  resolve(contextId: Uint8Array): Promise<Uint8Array>;
  setActive(contextId: Uint8Array, active: boolean): Promise<Uint8Array>;
  setWorkspace(
    contextId: Uint8Array,
    rootId: Uint8Array,
    workspaceId: Uint8Array,
    workspaceName: string,
    parentWorkspaceId?: Uint8Array,
  ): Promise<Uint8Array>;
  discardSubtree(
    parentContextId: Uint8Array,
    childContextId: Uint8Array,
    maximum: number,
  ): Promise<Uint8Array>;
}

export type WasmRawWorkspaceContextRegistry = RawWorkspaceContextRegistry;

/*
 * wasm-bindgen owns the class and method inventory. Rust Tsify DTOs provide
 * generated declarations for the workspace and generation result family;
 * older serde values remain erased until their own DTO migration. These
 * helpers inherit concrete signatures directly from the generated binding and
 * replace only the remaining erased values whose wire shapes are in contract.
 */
type WasmWithArgs<Base, Args extends object> = Omit<Base, keyof Args> & {
  [Key in keyof Args & keyof Base]: Base[Key] extends (...args: infer _Args) => infer Result
    ? (...args: Args[Key] extends readonly unknown[] ? Args[Key] : never) => Result
    : never;
};
type WasmWithAsyncResults<Base, Results extends object> = Omit<Base, keyof Results> & {
  [Key in keyof Results & keyof Base]: Base[Key] extends (...args: infer Args) => unknown
    ? (...args: Args) => Promise<Results[Key]>
    : never;
};
type WasmWithSyncResults<Base, Results extends object> = Omit<Base, keyof Results> & {
  [Key in keyof Results & keyof Base]: Base[Key] extends (...args: infer Args) => unknown
    ? (...args: Args) => Results[Key]
    : never;
};
type WasmWithProperties<Base, Properties extends object> = Omit<Base, keyof Properties> & Properties;
type WasmAssertKnownKeys<Base, Overrides extends object> =
  Exclude<keyof Overrides, keyof Base> extends never
    ? unknown
    : { readonly __unknownGeneratedWasmKeys__: Exclude<keyof Overrides, keyof Base> };
type WasmTypedClass<
  Base,
  Args extends object = {},
  AsyncResults extends object = {},
  SyncResults extends object = {},
  Properties extends object = {},
> = WasmWithProperties<
  WasmWithSyncResults<WasmWithAsyncResults<WasmWithArgs<Base, Args>, AsyncResults>, SyncResults>,
  Properties
> & WasmAssertKnownKeys<Base, Args>
  & WasmAssertKnownKeys<Base, AsyncResults>
  & WasmAssertKnownKeys<Base, SyncResults>
  & WasmAssertKnownKeys<Base, Properties>;

export type WasmRawMergeConflict = WasmBinding.MergeConflictResult;
// Join and live-rebase share the same result envelope. Keeping the generated
// Rust DTO union here lets the operation adapters accept each native status
// union without recreating a handwritten overlay.
export type WasmRawJoinResult =
  | WasmBinding.BrowserJoinResult
  | WasmBinding.BrowserWorkspaceRebaseResult;

export interface WasmRawObjectCacheStats {
  readonly hits: string;
  readonly decodedHits: string;
  readonly misses: string;
  readonly coalescedReads: string;
  readonly evictions: string;
  readonly residentEntries: string;
  readonly residentBytes: string;
  readonly residentCanonicalObjects: string;
  readonly residentCanonicalBytes: string;
  readonly residentDecodedPages: string;
  readonly residentDecodedBytes: string;
  readonly inFlight: string;
}

export interface WasmImportManifest {
  readonly manifestBytes: Uint8Array;
  readonly objects: readonly Uint8Array[];
}

export type WasmRawExportManifest = WasmBinding.BrowserExportManifestResult;
export type WasmRawFileRecordSnapshot = WasmBinding.BrowserFileRecordResult;
export type WasmRawGenerationDiff = WasmBinding.BrowserGenerationDiffResult;
export type WasmRawCommitResult = WasmBinding.BrowserCommitResult;
export type WasmRawLiveMutationResult = WasmBinding.BrowserLiveMutationResult;
export type WasmRawLiveTransactionResult = WasmBinding.BrowserLiveTransactionResult;
export type WasmRawFileExtentPlan = WasmBinding.BrowserExtentPlanResult;
export type WasmRawExtentSeekResult = WasmBinding.BrowserExtentSeekResult;

export type WasmRawGeneration = WasmTypedClass<WasmBinding.BrowserGeneration, {
  listDirectory: [path: string, after: WorkspaceName | undefined, maximumEntries: number];
}, {
  listDirectory: WasmBinding.BrowserWorkspaceDirectoryPage;
  pin: WasmRawGeneration;
  planExtents: WasmBinding.BrowserWorkspaceExtentPlan;
  stat: WasmBinding.BrowserWorkspaceStat;
}>;

export interface RawGenerationChangeSet<Diff> {
  readonly from: WasmRawGeneration;
  readonly to: WasmRawGeneration;
  changes(): Diff;
  compose(next: RawGenerationChangeSet<Diff>, maximumChanges: number): Promise<RawGenerationChangeSet<Diff>>;
}

export type WasmRawChangeSet = WasmTypedClass<WasmBinding.BrowserChangeSet, {}, {
  compose: WasmRawChangeSet;
}, {
  changes: WasmBinding.BrowserGenerationDiffResult;
}, {
  readonly from: WasmRawGeneration;
  readonly to: WasmRawGeneration;
}>;

export type WasmRawJoinPlan = WasmTypedClass<WasmBinding.BrowserJoinPlan, {}, {
  apply: WasmBinding.BrowserJoinResult;
}>;

export type WasmRawWorkspace = WasmTypedClass<WasmBinding.BrowserWorkspace, {
  joinInto: [target: WasmRawWorkspace, options: JoinOptions];
}, {
  beginTransaction: WasmRawTransaction;
  checkpoint: WasmRawGeneration;
  diff: WasmRawChangeSet;
  fork: WasmRawWorkspace;
  forkAt: WasmRawWorkspace;
  joinInto: WasmRawJoinPlan;
  liveRebase: WasmBinding.BrowserWorkspaceRebaseResult;
  pin: WasmRawGeneration;
  planExtents: WasmBinding.BrowserWorkspaceExtentPlan;
  remove: WasmBinding.BrowserWorkspaceCommit;
  stat: WasmBinding.BrowserWorkspaceStat;
  sync: WasmRawGeneration;
  write: WasmBinding.BrowserWorkspaceCommit;
}>;

export interface RawWorkspaceTransaction<Commit = unknown> {
  createDirAll(path: string): Promise<void>;
  createDirectory(path: string): Promise<void>;
  createSymbolicLink(path: string, target: Uint8Array): Promise<void>;
  write(path: string, bytes: Uint8Array): Promise<void>;
  remove(path: string): Promise<void>;
  copy(source: string, destination: string): Promise<void>;
  rename(source: string, destination: string): Promise<void>;
  hardLink(source: string, destination: string): Promise<void>;
  writeRange(path: string, offset: bigint, bytes: Uint8Array): Promise<void>;
  resize(path: string, logicalBytes: bigint): Promise<void>;
  zeroRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<void>;
  preallocate(path: string, offset: bigint, length: bigint, keepSize: boolean): Promise<void>;
  cloneRange(source: string, sourceOffset: bigint, destination: string, destinationOffset: bigint, length: bigint): Promise<void>;
  rebase(maximumConflicts: number): Promise<TransactionRebaseResult>;
  commit(): Promise<Commit>;
}

export type WasmRawTransaction = WasmTypedClass<WasmBinding.BrowserTransaction, {}, {
  commit: WorkspaceCommit;
  rebase: TransactionRebaseResult;
}>;

export type WasmRawSpeculation = WasmTypedClass<WasmBinding.BrowserSpeculation, {
  observe: [observation: ResidencyObservation];
  planPromotion: [request: PromotionRequest];
}, {
  executeResidency: WasmBinding.BrowserResidencyExecution;
}, {
  observe: WasmBinding.BrowserAdmissionResult;
  metrics: WasmBinding.BrowserSpeculationMetrics;
  planPromotion: WasmBinding.BrowserPromotionAdmission;
  preemptForForeground: WasmBinding.BrowserSpeculationPreemption;
  replaceGeneration: WasmBinding.BrowserSpeculationPreemption;
}>;

export type WasmRawVolume = WasmTypedClass<WasmBinding.BrowserVolume, {
  checkout: [options: CheckoutOptions];
}, {
  checkout: WasmRawCheckout;
  diffGenerations: WasmRawGenerationDiff;
}, {}, {
  readonly acquisitionWork: WasmBinding.BrowserWorkCounters;
}>;

export type WasmRawCheckout = WasmTypedClass<
  WasmBinding.BrowserCheckout,
  {
    createDevice: [path: string, kind: DeviceKind, major: number, minor: number];
    createSpecial: [path: string, kind: EmptySpecialKind];
    listDirectory: [path: string, after: string | undefined, maximumEntries: number];
    listDirectoryRecords: [path: string, after: string | undefined, maximumEntries: number];
    listNamedAttributes: [path: string, afterClass: NamedAttributeClass | undefined, afterName: Uint8Array | undefined, maximumEntries: number];
    remove: [path: string, expectedFileId: Uint8Array | undefined];
    seekFileExtent: [path: string, offset: bigint, target: ExtentSeekTarget];
    seekFileExtentById: [fileId: Uint8Array, offset: bigint, target: ExtentSeekTarget];
    setAttributes: [path: string, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined];
    setAttributesById: [fileId: Uint8Array, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined];
    writeNamedAttribute: [path: string, attributeClass: NamedAttributeClass, name: Uint8Array, bytes: Uint8Array, mode: NamedAttributeWriteMode];
  },
  {
    applyTransaction: WasmBinding.BrowserTransactionResult;
    checkpoint: WasmBinding.BrowserCheckpointResult;
    cloneFileRange: WasmBinding.BrowserMutationResult;
    cloneFileRangeById: WasmBinding.BrowserMutationResult;
    commit: WasmRawCommitResult;
    createDevice: WasmBinding.BrowserMutationResult;
    createDirectory: WasmBinding.BrowserMutationResult;
    createFile: WasmBinding.BrowserMutationResult;
    createReparsePoint: WasmBinding.BrowserMutationResult;
    createSpecial: WasmBinding.BrowserMutationResult;
    createSymbolicLink: WasmBinding.BrowserMutationResult;
    discard: WasmBinding.BrowserMutationResult;
    exportManifest: WasmRawExportManifest;
    hardLink: WasmBinding.BrowserMutationResult;
    listDirectory: WasmBinding.BrowserDirectoryPageResult;
    listDirectoryRecords: WasmBinding.BrowserDirectoryRecordPageResult;
    listNamedAttributes: WasmBinding.BrowserNamedAttributePageResult;
    lookupBatchNoFollow: WasmBinding.BrowserBatchLookupResult;
    lookupNoFollow: WasmBinding.BrowserLookupResult;
    mutateLive: WasmRawLiveTransactionResult;
    planFileExtents: WasmBinding.BrowserExtentPlanResult;
    planFileExtentsById: WasmBinding.BrowserExtentPlanResult;
    preallocateFile: WasmBinding.BrowserMutationResult;
    preallocateFileById: WasmBinding.BrowserMutationResult;
    prepareMerge: WasmBinding.BrowserMergePreparationResult;
    readFileRange: WasmBinding.BrowserFileReadResult;
    readFileRangeById: WasmBinding.BrowserFileReadResult;
    readFileRecordById: WasmBinding.BrowserFileRecordReadResult;
    readMetadata: WasmBinding.BrowserMetadataResult;
    readMetadataById: WasmBinding.BrowserMetadataResult;
    readNamedAttribute: WasmBinding.BrowserNamedAttributeResult;
    readReparsePoint: WasmBinding.BrowserFileReadResult;
    readSymbolicLink: WasmBinding.BrowserFileReadResult;
    rebaseHead: WasmBinding.BrowserRebaseResult;
    refreshHead: WasmBinding.BrowserCheckpointResult;
    refreshLive: WasmBinding.BrowserCheckpointResult;
    remove: WasmBinding.BrowserMutationResult;
    removeNamedAttribute: WasmBinding.BrowserMutationResult;
    rename: WasmBinding.BrowserMutationResult;
    resizeFile: WasmBinding.BrowserMutationResult;
    resizeFileById: WasmBinding.BrowserMutationResult;
    resolveFiles: WasmRawResolvedFiles;
    resumeLive: WasmBinding.BrowserLiveMutationResult;
    seekFileExtent: WasmBinding.BrowserExtentSeekResult;
    seekFileExtentById: WasmBinding.BrowserExtentSeekResult;
    setAttributes: WasmBinding.BrowserMutationResult;
    setAttributesById: WasmBinding.BrowserMutationResult;
    setMetadata: WasmBinding.BrowserMutationResult;
    setMetadataById: WasmBinding.BrowserMutationResult;
    statNoFollow: WasmBinding.BrowserStatResult;
    writeFile: WasmBinding.BrowserMutationResult;
    writeFileById: WasmBinding.BrowserMutationResult;
    writeNamedAttribute: WasmBinding.BrowserMutationResult;
    zeroFileRange: WasmBinding.BrowserMutationResult;
    zeroFileRangeById: WasmBinding.BrowserMutationResult;
  },
  {},
  { readonly acquisitionWork: WasmBinding.BrowserWorkCounters }
>;

export type WasmRawFs = WasmTypedClass<WasmBinding.BrowserFs, {
  createSpeculation: [volumeId: Uint8Array, generationId: Uint8Array, options: SpeculationOptions];
  createVolume: [options: VolumeOptions];
  createVolumeWithId: [volumeId: Uint8Array, options: VolumeOptions];
  exportGenerationBatch: [manifest: WasmImportManifest, cursor: bigint, maximumObjects: number, maximumObjectBytes: bigint];
  importGenerationBatch: [manifest: WasmImportManifest, cursor: bigint, objects: readonly Uint8Array[], maximumObjects: number];
  restoreVolume: [manifest: WasmImportManifest, operationId: Uint8Array];
}, {
  createWorkspace: WasmRawWorkspace;
  openWorkspace: WasmRawWorkspace;
  createVolume: WasmRawVolume;
  createVolumeWithId: WasmRawVolume;
  openVolume: WasmRawVolume;
  exportObject: WasmBinding.BrowserFileReadResult;
  importObject: WasmBinding.BrowserMutationResult;
  exportGenerationBatch: WasmBinding.GenerationTransferBatchResult;
  importGenerationBatch: WasmBinding.GenerationTransferCursorResult;
  restoreVolume: WasmRawVolume;
}, {
  createSpeculation: WasmRawSpeculation;
  objectCacheStats: WasmRawObjectCacheStats;
}, {
  readonly capabilities: EngineCapabilities;
}>;
export type WasmRawResolvedFile = WasmTypedClass<WasmBinding.BrowserResolvedFile, {}, {
  readRange: WasmBinding.BrowserFileReadResult;
  readSymbolicLink: WasmBinding.BrowserFileReadResult;
}>;

export type WasmRawResolvedFiles = WasmWithProperties<WasmBinding.BrowserResolvedFiles, {
  readonly work: WasmBinding.BrowserWorkCounters;
}> & { take(index: number): WasmRawResolvedFile | undefined };

// Native companion declarations are generated by NAPI-RS from the canonical
// Rust binding. Keep the adapter's historical Raw aliases, but make every one
// of them resolve to the generated declaration instead of maintaining a second
// handwritten ABI mirror here.
type NativeBoundary<T> =
  T extends Uint8Array ? Uint8Array :
  T extends Promise<infer Value> ? Promise<NativeBoundary<Value>> :
  T extends readonly (infer Value)[] ? readonly NativeBoundary<Value>[] :
  T extends (...args: infer Args) => infer Result
    ? ((...args: { [Key in keyof Args]: NativeBoundary<Args[Key]> }) => NativeBoundary<Result>) &
      { [Key in keyof T]: NativeBoundary<T[Key]> } :
  T extends object ? { [Key in keyof T]: NativeBoundary<T[Key]> } :
  T;

export type NativeBindings = NativeBoundary<typeof NativeBinding>;
export type NativeRawWorkspaceContextRegistry = NativeBoundary<NativeBinding.NativeWorkspaceContextRegistry>;
export type NativeRawWorkspaceLineageRecord = NativeBoundary<NativeBinding.NativeWorkspaceLineageRecord>;
export type NativeRawWorkspaceGraph = NativeBoundary<NativeBinding.NativeWorkspaceGraph>;
export type NativeRawOperationWindowLease = NativeBoundary<NativeBinding.NativeOperationWindowLease>;
export type NativeRawOperationWindowPhase = NativeBoundary<NativeBinding.NativeOperationWindowPhase>;
export type NativeRawOperationWindowClose = NativeBoundary<NativeBinding.NativeOperationWindowClose>;
export type NativeRawWorkspaceOperationWindowClose = NativeBoundary<NativeBinding.NativeWorkspaceOperationFinish>;
export type NativeRawOperationWindowCoordinator = NativeBoundary<NativeBinding.NativeOperationWindowCoordinator>;
export type NativeRawGitCompatRepository = NativeBoundary<NativeBinding.NativeGitCompatRepository>;
export type NativeRawObjectCacheOptions = NativeBoundary<NativeBinding.NativeObjectCacheOptions>;
export type NativeRawLookup = NativeBoundary<NativeBinding.NativeLookup>;
export type NativeRawMutation = NativeBoundary<NativeBinding.NativeMutationResult>;
export type NativeRawWatchChange = NativeBoundary<NativeBinding.NativeWatchChange>;
export type NativeRawWatchBatch = NativeBoundary<NativeBinding.NativeWatchBatch>;
export type NativeRawWatcher = NativeBoundary<NativeBinding.NativeWatcher>;
export type NativeRawCheckout = NativeBoundary<NativeBinding.NativeCheckout>;
export type NativeRawResolvedFile = NativeBoundary<NativeBinding.NativeResolvedFile>;
export type NativeRawResolvedFiles = NativeBoundary<NativeBinding.NativeResolvedFiles>;
export type NativeRawTransactionOperation = NativeBoundary<NativeBinding.NativeTransactionOperation>;
export type NativeRawCheckpointResult = NativeBoundary<NativeBinding.NativeCheckpointResult>;
export type NativeRawVolume = NativeBoundary<NativeBinding.NativeVolume>;
export type NativeRawGenerationDiff = NativeBoundary<NativeBinding.NativeGenerationDiff>;
export type NativeRawMergeConflict = NativeBoundary<NativeBinding.NativeMergeConflict>;
export type NativeRawMergePreparation = NativeBoundary<NativeBinding.NativeMergePreparation>;
export type NativeRawFs = NativeBoundary<NativeBinding.NativeFs>;
export type NativeRawWorkspaceCommit = NativeBoundary<NativeBinding.NativeWorkspaceCommit>;
export type NativeRawWorkspace = NativeBoundary<NativeBinding.NativeWorkspace>;
export type NativeRawChangeSet = NativeBoundary<NativeBinding.NativeChangeSet>;
export type NativeRawJoinPlan = NativeBoundary<NativeBinding.NativeJoinPlan>;
export type NativeRawJoinResult = NativeBoundary<NativeBinding.NativeJoinResult>;
export type NativeRawSourceResult = NativeBoundary<NativeBinding.NativeSourceResult>;
export type NativeRawGeneration = NativeBoundary<NativeBinding.NativeGeneration>;
export type NativeRawWorkspaceMount = NativeBoundary<NativeBinding.NativeWorkspaceMount>;
export type NativeRawWorkspaceTransaction = NativeBoundary<NativeBinding.NativeWorkspaceTransaction>;
export type NativeRawSpeculation = NativeBoundary<NativeBinding.NativeSpeculation>;
export type NativeRawExportManifest = NativeBoundary<NativeBinding.NativeExportManifest>;
