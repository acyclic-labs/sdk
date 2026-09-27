import type * as NativeBinding from "../generated/native/binding.js";
import type * as WasmBinding from "../generated/wasm/acyclic_fs_wasm.js";

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

export type WorkspaceCommitStatus =
  | "committed"
  | "already-committed"
  | "conflict"
  | "fenced"
  | "idempotency-conflict";

export interface WorkspaceCommit {
  readonly status: WorkspaceCommitStatus;
  readonly generationId: Uint8Array | undefined;
}

export type WorkspaceFileKind =
  | "regular" | "directory" | "symbolic-link" | "fifo" | "socket"
  | "character-device" | "block-device" | "reparse-point" | "mount-boundary";
export type WorkspaceNameEncoding = "utf8" | "posix-bytes" | "windows-utf16le";
export interface WorkspaceName { readonly encoding: WorkspaceNameEncoding; readonly bytes: Uint8Array; }
export interface WorkspaceMetadata {
  readonly posixMode: number | undefined; readonly posixUid: number | undefined;
  readonly posixGid: number | undefined; readonly posixFlags: bigint | undefined;
  readonly windowsAttributes: number | undefined; readonly createdNs: bigint | undefined;
  readonly modifiedNs: bigint | undefined; readonly accessedNs: bigint | undefined;
  readonly changedNs: bigint | undefined; readonly hasNamedAttributes: boolean;
  readonly hasAcl: boolean; readonly hasSecurityDescriptor: boolean;
}
export interface WorkspaceStat {
  readonly fileId: Uint8Array; readonly kind: WorkspaceFileKind;
  readonly linkCount: bigint; readonly logicalBytes: bigint | undefined;
  readonly metadata: WorkspaceMetadata;
}
export interface WorkspaceDirectoryEntry {
  readonly name: WorkspaceName; readonly fileId: Uint8Array; readonly kind: WorkspaceFileKind;
}
export interface WorkspaceDirectoryPage {
  readonly entries: readonly WorkspaceDirectoryEntry[]; readonly hasMore: boolean;
}
export type WorkspaceExtentKind = "hole" | "allocated-zero" | "content";
export interface WorkspaceExtentSpan {
  readonly offset: bigint; readonly length: bigint; readonly sourceEnd: bigint;
  readonly kind: WorkspaceExtentKind;
}
export interface WorkspaceExtentPlan { readonly spans: readonly WorkspaceExtentSpan[]; }

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

export type JoinStatus =
  | "applied"
  | "already-applied"
  | "no-changes"
  | "stale-target"
  | "conflicted"
  | "fenced"
  | "idempotency-conflict";

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

export type WorkspaceRebaseStatus =
  | "rebased"
  | "already-rebased"
  | "current"
  | "stale"
  | "conflicted"
  | "fenced"
  | "idempotency-conflict";

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

export const DEFAULT_VOLUME_LIMITS: VolumeLimits = Object.freeze({
  maximumPathBytes: 32 * 1024,
  maximumComponentBytes: 255,
  maximumPathDepth: 1024,
  maximumObjectBytes: 64n * 1024n * 1024n,
  maximumMutationsPerBatch: 2048,
  maximumPathsPerBatch: 65_536,
  maximumCheckoutDependencies: 262_144,
  maximumDirectoryPageEntries: 1024,
  maximumPageHeight: 64,
  maximumReadBytes: 16n * 1024n * 1024n,
  maximumFilesPerGeneration: 16n * 1024n * 1024n,
  maximumObjectsPerGeneration: 64n * 1024n * 1024n,
  maximumGenerationBytes: 1024n * 1024n * 1024n * 1024n,
});

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

export interface WorkCounters {
  readonly authorityRecordsRead: number;
  readonly authorityRecordsAppended: number;
  readonly authorityBytesRead: number;
  readonly authorityBytesWritten: number;
  readonly objectProbes: number;
  readonly backendReadOperations: number;
  readonly backendWriteOperations: number;
  readonly durabilityOperations: number;
  readonly pageReads: number;
  readonly pageWrites: number;
  readonly objectBytesRead: number;
  readonly objectBytesWritten: number;
  readonly bytesHashed: number;
  readonly bytesCopied: number;
  readonly bytesEncoded: number;
  readonly sourceBytesRead: number;
  readonly outputBytes: number;
  readonly itemsExamined: number;
  readonly itemsReturned: number;
  readonly allocationOperations: number;
  readonly peakAllocationBytes: number;
  readonly materializations: number;
}

export interface LookupResult {
  readonly exists: boolean;
  readonly fileId: Uint8Array | undefined;
  readonly fileKind: string | undefined;
  readonly resolvedComponents: number;
  readonly work: WorkCounters;
}

export interface FileReadResult {
  readonly bytes: Uint8Array;
  readonly work: WorkCounters;
}

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

export interface DirectoryEntry {
  readonly name: Uint8Array;
  readonly fileId: Uint8Array;
  readonly fileKind: string;
}

export interface DirectoryPage {
  readonly entries: readonly DirectoryEntry[];
  readonly hasMore: boolean;
  readonly work: WorkCounters;
}

export interface MutationResult {
  readonly fileId: Uint8Array | undefined;
  readonly work: WorkCounters;
}

/** Immutable content-addressed candidate built without publishing authority. */
export interface CheckpointResult {
  readonly generationId: Uint8Array;
  readonly work: WorkCounters;
}

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
  | { readonly kind: "created" | "modified" | "metadata" | "removed"; readonly path: NativeNamespacePath }
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
export interface GenerationExportManifest {
  readonly manifestBytes: Uint8Array;
  readonly objects: readonly Uint8Array[];
  readonly work: WorkCounters;
}

/** One manifest-ordered, resumable immutable-object transfer page. */
export interface GenerationTransferBatch {
  readonly firstObject: bigint;
  readonly nextObject: bigint | undefined;
  readonly objects: readonly Uint8Array[];
  readonly work: WorkCounters;
}

/** Cursor after an idempotently imported manifest-aligned page. */
export interface GenerationTransferCursor {
  readonly nextObject: bigint;
  readonly work: WorkCounters;
}

export interface CommitResult {
  readonly status:
    | "committed"
    | "already-committed"
    | "conflict"
    | "fenced"
    | "idempotency-conflict";
  readonly generationId: Uint8Array | undefined;
  readonly epoch: bigint | undefined;
  readonly sequence: bigint | undefined;
  readonly committedFingerprint: Uint8Array | undefined;
  readonly work: WorkCounters;
}

export interface RebaseResult {
  readonly status: "safe" | "conflicted";
  readonly generationId: Uint8Array | undefined;
  readonly conflictCount: number;
  readonly truncated: boolean;
  readonly work: WorkCounters;
}

/** Complete path-independent file record in a generation diff. */
export interface FileRecordSnapshot {
  readonly fileId: Uint8Array;
  readonly fileKind: string;
  readonly linkCount: bigint;
  readonly metadataObject: Uint8Array;
  readonly payloadKind: string;
  readonly logicalBytes: bigint | undefined;
  readonly payloadObject: Uint8Array | undefined;
  readonly inlineBytes: Uint8Array | undefined;
  readonly deviceMajor: number | undefined;
  readonly deviceMinor: number | undefined;
}

export interface FileRecordReadResult {
  readonly record: FileRecordSnapshot;
  readonly work: WorkCounters;
}

export interface BatchLookupEntry {
  readonly exists: boolean;
  readonly fileId: Uint8Array | undefined;
  readonly fileKind: string | undefined;
  readonly resolvedComponents: number;
}

export interface BatchLookupResult {
  readonly entries: readonly BatchLookupEntry[];
  readonly retainedAllocationBytes: bigint;
  readonly work: WorkCounters;
}

export interface DirectoryRecordEntry {
  readonly name: Uint8Array;
  readonly record: FileRecordSnapshot;
  readonly metadataCanonicalBytes: Uint8Array;
}

export interface DirectoryRecordPage {
  readonly entries: readonly DirectoryRecordEntry[];
  readonly hasMore: boolean;
  readonly work: WorkCounters;
}

export interface FileRecordChange {
  readonly fileId: Uint8Array;
  readonly before: FileRecordSnapshot | undefined;
  readonly after: FileRecordSnapshot | undefined;
}

export interface TreeEntrySnapshot {
  readonly name: NativePathComponent;
  readonly fileId: Uint8Array;
  readonly fileKind: string;
}

export interface DirectoryBindingChange {
  readonly directoryId: Uint8Array;
  readonly name: NativePathComponent;
  readonly before: TreeEntrySnapshot | undefined;
  readonly after: TreeEntrySnapshot | undefined;
}

export interface GenerationDiff {
  readonly files: readonly FileRecordChange[];
  readonly bindings: readonly DirectoryBindingChange[];
  readonly truncated: boolean;
  readonly work: WorkCounters;
}

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

export interface LiveMutationResult {
  readonly status:
    | "committed"
    | "already-committed"
    | "conflicted"
    | "retry-limit"
    | "fenced"
    | "idempotency-conflict";
  readonly generationId: Uint8Array | undefined;
  readonly epoch: bigint | undefined;
  readonly sequence: bigint | undefined;
  readonly conflictCount: number;
  readonly truncated: boolean;
  readonly committedFingerprint: Uint8Array | undefined;
  readonly work: WorkCounters;
}

export interface LiveTransactionResult extends LiveMutationResult {
  readonly createdFileIds: readonly (Uint8Array | undefined)[];
}

export type NamedAttributeClass = "posix-xattr" | "windows-stream" | "mac-resource-fork";

export interface MetadataResult {
  readonly canonicalBytes: Uint8Array;
  readonly work: WorkCounters;
}

export interface StatResult {
  readonly exists: boolean;
  readonly record: FileRecordSnapshot | undefined;
  readonly metadataCanonicalBytes: Uint8Array | undefined;
  readonly work: WorkCounters;
}

export interface NamedAttributeResult {
  readonly exists: boolean;
  readonly bytes: Uint8Array | undefined;
  readonly work: WorkCounters;
}

export interface NamedAttributePage {
  readonly entries: readonly NamedAttributeName[];
  readonly hasMore: boolean;
  readonly work: WorkCounters;
}

export interface NamedAttributeName {
  readonly attributeClass: NamedAttributeClass;
  readonly name: Uint8Array;
}

export type NamedAttributeWriteMode = "upsert" | "create" | "replace";
export type EmptySpecialKind = "fifo" | "socket" | "mount-boundary";
export type DeviceKind = "character-device" | "block-device";

export type TransactionOperation =
  | { readonly kind: "create-file"; readonly path: string; readonly bytes: Uint8Array }
  | { readonly kind: "create-directory"; readonly path: string }
  | { readonly kind: "create-symbolic-link"; readonly path: string; readonly target: Uint8Array }
  | { readonly kind: "create-special"; readonly path: string; readonly fileKind: EmptySpecialKind }
  | { readonly kind: "create-device"; readonly path: string; readonly fileKind: DeviceKind; readonly major: number; readonly minor: number }
  | { readonly kind: "create-reparse-point"; readonly path: string; readonly payload: Uint8Array }
  | { readonly kind: "remove"; readonly path: string; readonly expectedFileId: Uint8Array | undefined }
  | { readonly kind: "rename"; readonly source: string; readonly destination: string; readonly replace: boolean }
  | { readonly kind: "hard-link"; readonly source: string; readonly destination: string }
  | { readonly kind: "write"; readonly path: string; readonly offset: bigint; readonly bytes: Uint8Array }
  | { readonly kind: "set-metadata"; readonly path: string; readonly canonicalBytes: Uint8Array }
  | { readonly kind: "resize"; readonly path: string; readonly logicalBytes: bigint }
  | { readonly kind: "zero-range"; readonly path: string; readonly offset: bigint; readonly length: bigint; readonly allocated: boolean; readonly extend: boolean }
  | { readonly kind: "preallocate"; readonly path: string; readonly offset: bigint; readonly length: bigint; readonly keepSize: boolean }
  | { readonly kind: "clone-range"; readonly source: string; readonly sourceOffset: bigint; readonly destination: string; readonly destinationOffset: bigint; readonly length: bigint };

export interface TransactionResult {
  readonly createdFileIds: readonly (Uint8Array | undefined)[];
  readonly work: WorkCounters;
}

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

export const DEFAULT_OBJECT_CACHE_OPTIONS: ObjectCacheOptions = Object.freeze({
  maximumEntries: 4096,
  maximumBytes: 256 * 1024 * 1024,
  maximumInFlight: 1024,
  maximumWaitersPerObject: 1024,
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

export interface WasmBindings {
  default(
    moduleOrPath?:
      | { readonly module_or_path: WebAssembly.Module | RequestInfo | URL | Response | BufferSource }
      | WebAssembly.Module
      | RequestInfo
      | URL
      | Response
      | BufferSource,
  ): Promise<unknown>;
  openBrowserFs(options: BrowserFsOptions): Promise<WasmRawFs>;
  openMemoryFs(options: MemoryFsOptions): Promise<WasmRawFs>;
  readonly BrowserWorkspaceContextRegistry: {
    new(): WasmRawWorkspaceContextRegistry;
  };
  encodeMergePlanJson(valueJson: string): string;
  decodeMergePlanJson(valueJson: string): string;
  encodeMergeCandidateJson(valueJson: string): string;
  decodeMergeCandidateJson(valueJson: string): string;
  encodeMultiRootPlanJson(valueJson: string): string;
  decodeMultiRootPlanJson(valueJson: string): string;
  encodeMultiRootCandidateJson(valueJson: string): string;
  decodeMultiRootCandidateJson(valueJson: string): string;
  encodePublicationJson(valueJson: string): string;
  decodePublicationJson(valueJson: string): string;
}

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
 * wasm-bindgen owns the class and method inventory. Its generated declaration
 * intentionally uses `any` for serde values because Rust values cross the
 * boundary as JavaScript objects. These helpers replace only erased result
 * values (and the few serde arguments whose wire shapes are in this contract),
 * while inheriting concrete signatures directly from the generated binding.
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

export interface WasmRawMergeConflict {
  readonly kind: string;
  readonly fileId: Uint8Array | undefined;
  readonly directoryId: Uint8Array | undefined;
  readonly name: NativePathComponent | undefined;
}

export interface WasmRawJoinResult {
  readonly status: string;
  readonly generationId: Uint8Array | undefined;
  readonly conflicts: readonly WasmRawMergeConflict[];
  readonly truncated: boolean;
}

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

export interface WasmRawExportManifest {
  readonly manifestBytes: Uint8Array;
  readonly objects: readonly Uint8Array[];
  readonly work: WorkCounters;
}

export interface WasmRawFileRecordSnapshot {
  readonly fileId: Uint8Array;
  readonly fileKind: string;
  readonly linkCount: string;
  readonly metadataObject: Uint8Array;
  readonly payloadKind: string;
  readonly logicalBytes: string | undefined;
  readonly payloadObject: Uint8Array | undefined;
  readonly inlineBytes: Uint8Array | undefined;
  readonly deviceMajor: number | undefined;
  readonly deviceMinor: number | undefined;
}

export interface WasmRawGenerationDiff {
  readonly files: readonly {
    readonly fileId: Uint8Array;
    readonly before: WasmRawFileRecordSnapshot | undefined;
    readonly after: WasmRawFileRecordSnapshot | undefined;
  }[];
  readonly bindings: readonly DirectoryBindingChange[];
  readonly truncated: boolean;
  readonly work: WorkCounters;
}

export interface WasmRawCommitResult {
  readonly status: CommitResult["status"];
  readonly generationId: Uint8Array | undefined;
  readonly epoch: string | undefined;
  readonly sequence: string | undefined;
  readonly committedFingerprint: Uint8Array | undefined;
  readonly work: WorkCounters;
}

export interface WasmRawLiveMutationResult {
  readonly status: LiveMutationResult["status"];
  readonly generationId: Uint8Array | undefined;
  readonly epoch: string | undefined;
  readonly sequence: string | undefined;
  readonly conflictCount: number;
  readonly truncated: boolean;
  readonly committedFingerprint: Uint8Array | undefined;
  readonly work: WorkCounters;
}

export interface WasmRawLiveTransactionResult extends WasmRawLiveMutationResult {
  readonly createdFileIds: readonly (Uint8Array | undefined)[];
}

export interface WasmRawFileExtentPlan {
  readonly kind: "inline" | "sparse";
  readonly spans?: readonly {
    readonly kind: "hole" | "allocated-zero" | "content";
    readonly offset: string;
    readonly length: string;
    readonly sourceEnd: string;
    readonly objectId?: Uint8Array;
    readonly objectOffset?: string;
  }[];
  readonly retainedAllocationBytes?: string;
  readonly work: WorkCounters;
}

export interface WasmRawExtentSeekResult {
  readonly offset: string | undefined;
  readonly work: WorkCounters;
}

export type WasmRawGeneration = WasmTypedClass<WasmBinding.BrowserGeneration, {
  listDirectory: [path: string, after: WorkspaceName | undefined, maximumEntries: number];
}, {
  listDirectory: WorkspaceDirectoryPage;
  pin: WasmRawGeneration;
  planExtents: WorkspaceExtentPlan;
  stat: WorkspaceStat;
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
  changes: WasmRawGenerationDiff;
}, {
  readonly from: WasmRawGeneration;
  readonly to: WasmRawGeneration;
}>;

export type WasmRawJoinPlan = WasmTypedClass<WasmBinding.BrowserJoinPlan, {}, {
  apply: WasmRawJoinResult;
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
  liveRebase: WasmRawJoinResult;
  pin: WasmRawGeneration;
  planExtents: WorkspaceExtentPlan;
  remove: WorkspaceCommit;
  stat: WorkspaceStat;
  sync: WasmRawGeneration;
  write: WorkspaceCommit;
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
  executeResidency: { readonly objectBytes: string; readonly work: WorkCounters };
}, {
  observe: { readonly status: string; readonly rejection?: string };
  metrics: Record<string, Record<string, string>>;
  planPromotion: {
    readonly status: string;
    readonly rejection?: string;
    readonly operationId?: Uint8Array;
    readonly objectId?: Uint8Array;
    readonly sourceLocationId?: Uint8Array;
    readonly destinationLocationId?: Uint8Array;
    readonly estimatedCostUnits?: string;
  };
  preemptForForeground: SpeculationPreemption;
  replaceGeneration: SpeculationPreemption;
}>;

export type WasmRawVolume = WasmTypedClass<WasmBinding.BrowserVolume, {
  checkout: [options: CheckoutOptions];
}, {
  checkout: WasmRawCheckout;
  diffGenerations: WasmRawGenerationDiff;
}, {}, {
  readonly acquisitionWork: WorkCounters;
}>;

export type WasmRawCheckout = WasmTypedClass<
  WasmBinding.BrowserCheckout,
  {
    applyTransaction: [operations: readonly TransactionOperation[]];
    createDevice: [path: string, kind: DeviceKind, major: number, minor: number];
    createSpecial: [path: string, kind: EmptySpecialKind];
    listDirectory: [path: string, after: string | undefined, maximumEntries: number];
    listDirectoryRecords: [path: string, after: string | undefined, maximumEntries: number];
    listNamedAttributes: [path: string, afterClass: NamedAttributeClass | undefined, afterName: Uint8Array | undefined, maximumEntries: number];
    lookupBatchNoFollow: [paths: readonly string[]];
    mutateLive: [operations: readonly TransactionOperation[], operationId: Uint8Array, maximumAttempts: number, maximumConflicts: number];
    resolveFiles: [paths: readonly string[]];
    remove: [path: string, expectedFileId: Uint8Array | undefined];
    seekFileExtent: [path: string, offset: bigint, target: ExtentSeekTarget];
    seekFileExtentById: [fileId: Uint8Array, offset: bigint, target: ExtentSeekTarget];
    setAttributes: [path: string, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined];
    setAttributesById: [fileId: Uint8Array, canonicalBytes: Uint8Array, logicalBytes: bigint | undefined];
    writeNamedAttribute: [path: string, attributeClass: NamedAttributeClass, name: Uint8Array, bytes: Uint8Array, mode: NamedAttributeWriteMode];
  },
  {
    applyTransaction: TransactionResult;
    checkpoint: CheckpointResult;
    cloneFileRange: MutationResult;
    cloneFileRangeById: MutationResult;
    commit: WasmRawCommitResult;
    createDevice: MutationResult;
    createDirectory: MutationResult;
    createFile: MutationResult;
    createReparsePoint: MutationResult;
    createSpecial: MutationResult;
    createSymbolicLink: MutationResult;
    discard: MutationResult;
    exportManifest: WasmRawExportManifest;
    hardLink: MutationResult;
    listDirectory: DirectoryPage;
    listDirectoryRecords: DirectoryRecordPage;
    listNamedAttributes: NamedAttributePage;
    lookupBatchNoFollow: BatchLookupResult;
    lookupNoFollow: LookupResult;
    mutateLive: WasmRawLiveTransactionResult;
    planFileExtents: WasmRawFileExtentPlan;
    planFileExtentsById: WasmRawFileExtentPlan;
    preallocateFile: MutationResult;
    preallocateFileById: MutationResult;
    prepareMerge: MergePreparationResult;
    readFileRange: FileReadResult;
    readFileRangeById: FileReadResult;
    readFileRecordById: FileRecordReadResult;
    readMetadata: MetadataResult;
    readMetadataById: MetadataResult;
    readNamedAttribute: NamedAttributeResult;
    readReparsePoint: FileReadResult;
    readSymbolicLink: FileReadResult;
    rebaseHead: RebaseResult;
    refreshHead: CheckpointResult;
    refreshLive: CheckpointResult;
    remove: MutationResult;
    removeNamedAttribute: MutationResult;
    rename: MutationResult;
    resizeFile: MutationResult;
    resizeFileById: MutationResult;
    resolveFiles: WasmRawResolvedFiles;
    resumeLive: WasmRawLiveMutationResult;
    seekFileExtent: WasmRawExtentSeekResult;
    seekFileExtentById: WasmRawExtentSeekResult;
    setAttributes: MutationResult;
    setAttributesById: MutationResult;
    setMetadata: MutationResult;
    setMetadataById: MutationResult;
    statNoFollow: StatResult;
    writeFile: MutationResult;
    writeFileById: MutationResult;
    writeNamedAttribute: MutationResult;
    zeroFileRange: MutationResult;
    zeroFileRangeById: MutationResult;
  },
  {},
  { readonly acquisitionWork: WorkCounters }
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
  exportObject: FileReadResult;
  importObject: MutationResult;
  exportGenerationBatch: {
    readonly firstObject: string;
    readonly nextObject: string | undefined;
    readonly objects: readonly Uint8Array[];
    readonly work: WorkCounters;
  };
  importGenerationBatch: { readonly nextObject: string; readonly work: WorkCounters };
  restoreVolume: WasmRawVolume;
}, {
  createSpeculation: WasmRawSpeculation;
  objectCacheStats: WasmRawObjectCacheStats;
}, {
  readonly capabilities: EngineCapabilities;
}>;
export type WasmRawResolvedFile = WasmTypedClass<WasmBinding.BrowserResolvedFile, {}, {
  readRange: FileReadResult;
  readSymbolicLink: FileReadResult;
}>;

export type WasmRawResolvedFiles = WasmWithProperties<WasmBinding.BrowserResolvedFiles, {
  readonly work: WorkCounters;
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
