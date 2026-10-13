/* tslint:disable */
/* eslint-disable */
/**
 * Complete namespace entry kind.
 */
export type FileKind = "regular" | "directory" | "symbolic-link" | "fifo" | "socket" | "character-device" | "block-device" | "reparse-point" | "mount-boundary";

/**
 * Exact result of advancing a retained transaction to the current head.
 */
export interface BrowserTransactionRebase {
    status: BrowserTransactionRebaseStatus;
    generationId: Uint8Array | undefined;
    conflicts: BrowserTransactionConflict[];
    truncated: boolean;
}

/**
 * One observation or mutation dependency that prevents a safe rebase.
 */
export interface BrowserTransactionConflict {
    region: BrowserTransactionConflictRegion;
    fileId: Uint8Array | undefined;
    directoryId: Uint8Array | undefined;
    offset: bigint | undefined;
    length: bigint | undefined;
    sparseTarget: BrowserTransactionSparseSeek | undefined;
    name: BrowserWorkspaceName | undefined;
    maximumEntries: number | undefined;
    usage: BrowserTransactionDependencyUse;
    expected: Uint8Array | undefined;
    actual: Uint8Array | undefined;
}

/**
 *Auto-generated discriminant enum variants
 */
export type FilePayloadKind = "inline-regular" | "regular" | "directory" | "symbolic-link" | "empty" | "device" | "reparse-point";

export interface BrowserAdmissionResult {
    status: "admitted" | "rejected";
    rejection: string | undefined;
}

export interface BrowserBatchLookupEntryResult {
    exists: boolean;
    fileId: Uint8Array | undefined;
    fileKind: FileKind | undefined;
    resolvedComponents: number;
}

export interface BrowserBatchLookupResult {
    entries: BrowserBatchLookupEntryResult[];
    retainedAllocationBytes: bigint;
    work: BrowserWorkCounters;
}

export interface BrowserBindingChangeResult {
    directoryId: Uint8Array;
    name: NameComponentResult;
    before: BrowserTreeEntryResult | undefined;
    after: BrowserTreeEntryResult | undefined;
}

export interface BrowserCheckoutOptions {
    access: "read-only" | "read-write";
    consistency: "pinned" | "tracking-safe" | "live" | "manual";
    mutationMode: "none" | "private-cow" | "direct-live";
}

export interface BrowserCheckpointResult {
    generationId: Uint8Array;
    work: BrowserWorkCounters;
}

export interface BrowserCommitResult {
    status: "committed" | "already-committed" | "conflict" | "fenced" | "idempotency-conflict";
    generationId: Uint8Array | undefined;
    epoch: bigint | undefined;
    sequence: bigint | undefined;
    committedFingerprint: Uint8Array | undefined;
    work: BrowserWorkCounters;
}

export interface BrowserDirectoryEntryResult {
    name: Uint8Array;
    fileId: Uint8Array;
    fileKind: FileKind;
}

export interface BrowserDirectoryPageResult {
    entries: BrowserDirectoryEntryResult[];
    hasMore: boolean;
    work: BrowserWorkCounters;
}

export interface BrowserDirectoryRecordEntryResult {
    name: Uint8Array;
    record: BrowserFileRecordResult;
    metadataCanonicalBytes: Uint8Array;
}

export interface BrowserDirectoryRecordPageResult {
    entries: BrowserDirectoryRecordEntryResult[];
    hasMore: boolean;
    work: BrowserWorkCounters;
}

export interface BrowserExportManifestResult {
    manifestBytes: Uint8Array;
    objects: Uint8Array[];
    work: BrowserWorkCounters;
}

export interface BrowserExtentSeekResult {
    offset: bigint | undefined;
    work: BrowserWorkCounters;
}

export interface BrowserExtentSpanResult {
    kind: "hole" | "allocated-zero" | "content";
    offset: bigint;
    length: bigint;
    sourceEnd: bigint;
    objectId: Uint8Array | undefined;
    objectOffset: bigint | undefined;
}

export interface BrowserFileReadResult {
    bytes: Uint8Array;
    work: BrowserWorkCounters;
}

export interface BrowserFileRecordChangeResult {
    fileId: Uint8Array;
    before: BrowserFileRecordResult | undefined;
    after: BrowserFileRecordResult | undefined;
}

export interface BrowserFileRecordReadResult {
    record: BrowserFileRecordResult;
    work: BrowserWorkCounters;
}

export interface BrowserFileRecordResult {
    fileId: Uint8Array;
    fileKind: FileKind;
    linkCount: bigint;
    metadataObject: Uint8Array;
    payloadKind: FilePayloadKind;
    logicalBytes: bigint | undefined;
    payloadObject: Uint8Array | undefined;
    inlineBytes: Uint8Array | undefined;
    deviceMajor: number | undefined;
    deviceMinor: number | undefined;
}

export interface BrowserForkOptions {
    paths?: string[];
    idempotencyKey?: Uint8Array;
}

export interface BrowserGenerationDiffResult {
    files: BrowserFileRecordChangeResult[];
    bindings: BrowserBindingChangeResult[];
    truncated: boolean;
    work: BrowserWorkCounters;
}

export interface BrowserJoinOptions {
    history: string;
    maximumGenerations: number;
    maximumChanges: number;
    maximumConflicts: number;
    sourceGeneration?: Uint8Array;
    targetGeneration?: Uint8Array;
}

export interface BrowserJoinResult {
    status: "applied" | "already-applied" | "no-changes" | "stale-target" | "conflicted" | "fenced" | "idempotency-conflict";
    generationId: Uint8Array | undefined;
    conflicts: MergeConflictResult[];
    truncated: boolean;
}

export interface BrowserLiveMutationResult {
    status: "committed" | "already-committed" | "conflicted" | "retry-limit" | "fenced" | "idempotency-conflict";
    generationId: Uint8Array | undefined;
    epoch: bigint | undefined;
    sequence: bigint | undefined;
    conflictCount: number;
    truncated: boolean;
    committedFingerprint: Uint8Array | undefined;
    work: BrowserWorkCounters;
}

export interface BrowserLiveTransactionResult {
    status: "committed" | "already-committed" | "conflicted" | "retry-limit" | "fenced" | "idempotency-conflict";
    generationId: Uint8Array | undefined;
    epoch: bigint | undefined;
    sequence: bigint | undefined;
    conflictCount: number;
    truncated: boolean;
    committedFingerprint: Uint8Array | undefined;
    work: BrowserWorkCounters;
    createdFileIds: (Uint8Array | undefined)[];
}

export interface BrowserLookupResult {
    exists: boolean;
    fileId: Uint8Array | undefined;
    fileKind: FileKind | undefined;
    resolvedComponents: number;
    work: BrowserWorkCounters;
}

export interface BrowserMergePreparationResult {
    status: "prepared" | "conflicted";
    generationId: Uint8Array | undefined;
    conflicts: MergeConflictResult[];
    truncated: boolean;
    work: BrowserWorkCounters;
}

export interface BrowserMetadataResult {
    canonicalBytes: Uint8Array;
    work: BrowserWorkCounters;
}

export interface BrowserMutationResult {
    fileId: Uint8Array | undefined;
    work: BrowserWorkCounters;
}

export interface BrowserNamedAttributeNameResult {
    attributeClass: "posix-xattr" | "windows-stream" | "mac-resource-fork";
    name: Uint8Array;
}

export interface BrowserNamedAttributePageResult {
    entries: BrowserNamedAttributeNameResult[];
    hasMore: boolean;
    work: BrowserWorkCounters;
}

export interface BrowserNamedAttributeResult {
    exists: boolean;
    bytes: Uint8Array | undefined;
    work: BrowserWorkCounters;
}

export interface BrowserOperationReconcileOptions {
    maximumGenerations: number;
    maximumChanges: number;
    maximumConflicts: number;
}

export interface BrowserOperationWindowLease {
    workspaceId: Uint8Array;
    leaseId: Uint8Array;
    pinnedParent: Uint8Array;
    expiresAtMillis: bigint;
}

export interface BrowserPromotionAdmission {
    status: "satisfied" | "planned" | "rejected";
    rejection: string | undefined;
    operationId: Uint8Array | undefined;
    objectId: Uint8Array | undefined;
    sourceLocationId: Uint8Array | undefined;
    destinationLocationId: Uint8Array | undefined;
    estimatedCostUnits: bigint | undefined;
}

export interface BrowserPromotionMetrics {
    candidates: bigint;
    satisfied: bigint;
    planned: bigint;
    active: bigint;
    activeBytes: bigint;
    activeCostUnits: bigint;
    useful: bigint;
    wasted: bigint;
    rejected: bigint;
}

export interface BrowserRebaseResult {
    status: "safe" | "conflicted";
    generationId: Uint8Array | undefined;
    conflictCount: number;
    truncated: boolean;
    work: BrowserWorkCounters;
}

export interface BrowserResidencyExecution {
    objectBytes: bigint;
    work: BrowserWorkCounters;
}

export interface BrowserResidencyMetrics {
    candidates: bigint;
    admitted: bigint;
    active: bigint;
    activeBytes: bigint;
    useful: bigint;
    wasted: bigint;
    rejectedFence: bigint;
    rejectedDuplicate: bigint;
    rejectedCapacity: bigint;
    rejectedCost: bigint;
    rejectedUsefulness: bigint;
}

export interface BrowserSpeculationMetrics {
    residency: BrowserResidencyMetrics;
    promotion: BrowserPromotionMetrics;
}

export interface BrowserSpeculationPreemption {
    residencyOperationIds: Uint8Array[];
    promotionOperationIds: Uint8Array[];
}

export interface BrowserStatResult {
    exists: boolean;
    record: BrowserFileRecordResult | undefined;
    metadataCanonicalBytes: Uint8Array | undefined;
    work: BrowserWorkCounters;
}

export interface BrowserTransactionResult {
    createdFileIds: (Uint8Array | undefined)[];
    work: BrowserWorkCounters;
}

export interface BrowserTreeEntryResult {
    name: NameComponentResult;
    fileId: Uint8Array;
    fileKind: FileKind;
}

export interface BrowserVolumeLimits {
    maximumPathBytes: number;
    maximumComponentBytes: number;
    maximumPathDepth: number;
    maximumObjectBytes: bigint;
    maximumMutationsPerBatch: number;
    maximumPathsPerBatch: number;
    maximumCheckoutDependencies: number;
    maximumDirectoryPageEntries: number;
    maximumPageHeight: number;
    maximumReadBytes: bigint;
    maximumFilesPerGeneration: bigint;
    maximumObjectsPerGeneration: bigint;
    maximumGenerationBytes: bigint;
}

export interface BrowserVolumeOptions {
    profile: "portable" | "posix" | "windows" | "browser";
    concurrency: "exclusive-writer" | "optimistic" | "serialized-authority";
    lifecycle: "ephemeral" | "durable";
    caseSensitivity: "sensitive" | "profile-folded";
    unicode: "preserve" | "require-nfc";
    symbolicLinks: boolean;
    hardLinks: boolean;
    sparseFiles: boolean;
    limits: BrowserVolumeLimits;
}

export interface BrowserWorkCounters {
    authorityRecordsRead: bigint;
    authorityRecordsAppended: bigint;
    authorityBytesRead: bigint;
    authorityBytesWritten: bigint;
    objectProbes: bigint;
    backendReadOperations: bigint;
    backendWriteOperations: bigint;
    durabilityOperations: bigint;
    pageReads: bigint;
    pageWrites: bigint;
    objectBytesRead: bigint;
    objectBytesWritten: bigint;
    bytesHashed: bigint;
    bytesCopied: bigint;
    bytesEncoded: bigint;
    sourceBytesRead: bigint;
    sourcePathComponents: bigint;
    sourceEntriesVisited: bigint;
    outputBytes: bigint;
    itemsExamined: bigint;
    itemsReturned: bigint;
    allocationOperations: bigint;
    peakAllocationBytes: bigint;
    materializations: bigint;
}

export interface BrowserWorkspaceCommit {
    status: BrowserWorkspaceCommitStatus;
    generationId: Uint8Array | undefined;
}

export interface BrowserWorkspaceDirectoryEntry {
    name: BrowserWorkspaceName;
    fileId: Uint8Array;
    kind: FileKind;
}

export interface BrowserWorkspaceDirectoryPage {
    entries: BrowserWorkspaceDirectoryEntry[];
    hasMore: boolean;
}

export interface BrowserWorkspaceExtentPlan {
    spans: BrowserWorkspaceExtentSpan[];
}

export interface BrowserWorkspaceExtentSpan {
    offset: bigint;
    length: bigint;
    sourceEnd: bigint;
    kind: "hole" | "allocated-zero" | "content";
}

export interface BrowserWorkspaceMetadata {
    posixMode: number | undefined;
    posixUid: number | undefined;
    posixGid: number | undefined;
    posixFlags: bigint | undefined;
    windowsAttributes: number | undefined;
    createdNs: bigint | undefined;
    modifiedNs: bigint | undefined;
    accessedNs: bigint | undefined;
    changedNs: bigint | undefined;
    hasNamedAttributes: boolean;
    hasAcl: boolean;
    hasSecurityDescriptor: boolean;
}

export interface BrowserWorkspaceName {
    encoding: "utf8" | "posix-bytes" | "windows-utf16le";
    bytes: Uint8Array;
}

export interface BrowserWorkspaceRebaseResult {
    status: "rebased" | "already-rebased" | "current" | "stale" | "conflicted" | "fenced" | "idempotency-conflict";
    generationId: Uint8Array | undefined;
    conflicts: MergeConflictResult[];
    truncated: boolean;
}

export interface BrowserWorkspaceStat {
    fileId: Uint8Array;
    kind: FileKind;
    linkCount: bigint;
    logicalBytes: bigint | undefined;
    metadata: BrowserWorkspaceMetadata;
}

export interface GenerationTransferBatchResult {
    firstObject: bigint;
    nextObject: bigint | undefined;
    objects: Uint8Array[];
    work: BrowserWorkCounters;
}

export interface GenerationTransferCursorResult {
    nextObject: bigint;
    work: BrowserWorkCounters;
}

export interface MergeConflictResult {
    kind: "file" | "binding";
    fileId: Uint8Array | undefined;
    directoryId: Uint8Array | undefined;
    name: NameComponentResult | undefined;
}

export interface NameComponentResult {
    encoding: "utf8" | "posix-bytes" | "windows-utf16le";
    bytes: Uint8Array;
}

export type BrowserExtentPlanResult = { kind: "inline"; work: BrowserWorkCounters } | { kind: "sparse"; spans: BrowserExtentSpanResult[]; retainedAllocationBytes: bigint; work: BrowserWorkCounters };

export type BrowserOperationWindowClose = { kind: "still-active"; remaining: number } | { kind: "already-closed" } | { kind: "reconcile"; ticket: Uint8Array; pinnedParent: Uint8Array; pendingParent: Uint8Array | undefined };

export type BrowserOperationWindowPhase = { kind: "idle" } | { kind: "active"; pinnedParent: Uint8Array; pendingParent: Uint8Array | undefined; activeLeaseCount: number } | { kind: "reconciling"; ticket: Uint8Array; pinnedParent: Uint8Array; pendingParent: Uint8Array | undefined };

export type BrowserPathBatch = string[];

export type BrowserTransactionConflictRegion = "file-record" | "metadata" | "file-length" | "content-range" | "sparse-seek" | "directory-name" | "directory-range";

export type BrowserTransactionDependencyUse = "observation" | "mutation" | "observation-and-mutation";

export type BrowserTransactionRebaseStatus = "rebased" | "conflicted";

export type BrowserTransactionSparseSeek = "data" | "hole";

export type BrowserWorkspaceCommitStatus = "committed" | "already-committed" | "conflict" | "fenced" | "idempotency-conflict";

export type BrowserWorkspaceOperationClose = { kind: "still-active"; remaining: number } | { kind: "already-closed" } | { kind: "reconciled"; rebase: BrowserWorkspaceRebaseResult };

export type TransactionOperation = { kind: "create-file"; path: string; bytes: Uint8Array } | { kind: "create-directory"; path: string } | { kind: "create-symbolic-link"; path: string; target: Uint8Array } | { kind: "create-special"; path: string; fileKind: "fifo" | "socket" | "mount-boundary" } | { kind: "create-device"; path: string; fileKind: "character-device" | "block-device"; major: number; minor: number } | { kind: "create-reparse-point"; path: string; payload: Uint8Array } | { kind: "remove"; path: string; expectedFileId: Uint8Array | undefined } | { kind: "rename"; source: string; destination: string; replace: boolean } | { kind: "hard-link"; source: string; destination: string } | { kind: "write"; path: string; offset: bigint; bytes: Uint8Array } | { kind: "set-metadata"; path: string; canonicalBytes: Uint8Array } | { kind: "resize"; path: string; logicalBytes: bigint } | { kind: "zero-range"; path: string; offset: bigint; length: bigint; allocated: boolean; extend: boolean } | { kind: "preallocate"; path: string; offset: bigint; length: bigint; keepSize: boolean } | { kind: "clone-range"; source: string; sourceOffset: bigint; destination: string; destinationOffset: bigint; length: bigint };


/**
 * One immutable semantic delta between exact generations.
 */
export class BrowserChangeSet {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    changes(): BrowserGenerationDiffResult;
    compose(next: BrowserChangeSet, maximum_changes: number): Promise<BrowserChangeSet>;
    readonly from: BrowserGeneration;
    readonly to: BrowserGeneration;
}

/**
 * One immutable-generation checkout with optional private COW mutations.
 */
export class BrowserCheckout {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    applyTransaction(operations: TransactionOperation[]): Promise<BrowserTransactionResult>;
    checkpoint(): Promise<BrowserCheckpointResult>;
    cloneFileRange(source: string, source_offset: bigint, destination: string, destination_offset: bigint, length: bigint): Promise<BrowserMutationResult>;
    cloneFileRangeById(source_file_id: Uint8Array, source_offset: bigint, destination_file_id: Uint8Array, destination_offset: bigint, length: bigint): Promise<BrowserMutationResult>;
    commit(operation_id: Uint8Array, lease: BrowserOperationWindowLease | undefined): Promise<BrowserCommitResult>;
    createDevice(path: string, kind: string, major: number, minor: number): Promise<BrowserMutationResult>;
    createDirectory(path: string): Promise<BrowserMutationResult>;
    createFile(path: string, bytes: Uint8Array): Promise<BrowserMutationResult>;
    createReparsePoint(path: string, payload: Uint8Array): Promise<BrowserMutationResult>;
    createSpecial(path: string, kind: string): Promise<BrowserMutationResult>;
    createSymbolicLink(path: string, target: Uint8Array): Promise<BrowserMutationResult>;
    discard(): Promise<BrowserMutationResult>;
    exportManifest(): Promise<BrowserExportManifestResult>;
    hardLink(source: string, destination: string): Promise<BrowserMutationResult>;
    listDirectory(path: string, after: string | null | undefined, maximum_entries: number): Promise<BrowserDirectoryPageResult>;
    listDirectoryRecords(path: string, after: string | null | undefined, maximum_entries: number): Promise<BrowserDirectoryRecordPageResult>;
    listNamedAttributes(path: string, after_class: string | null | undefined, after_name: Uint8Array | null | undefined, maximum_entries: number): Promise<BrowserNamedAttributePageResult>;
    lookupBatchNoFollow(paths: BrowserPathBatch): Promise<BrowserBatchLookupResult>;
    lookupNoFollow(path: string): Promise<BrowserLookupResult>;
    mutateLive(operations: TransactionOperation[], operation_id: Uint8Array, maximum_attempts: number, maximum_conflicts: number): Promise<BrowserLiveTransactionResult>;
    planFileExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserExtentPlanResult>;
    planFileExtentsById(file_id: Uint8Array, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserExtentPlanResult>;
    preallocateFile(path: string, offset: bigint, length: bigint, keep_size: boolean): Promise<BrowserMutationResult>;
    preallocateFileById(file_id: Uint8Array, offset: bigint, length: bigint, keep_size: boolean): Promise<BrowserMutationResult>;
    prepareMerge(theirs: Uint8Array, maximum_changes: number, maximum_conflicts: number): Promise<BrowserMergePreparationResult>;
    readFileRange(path: string, offset: bigint, length: bigint): Promise<BrowserFileReadResult>;
    readFileRangeById(file_id: Uint8Array, offset: bigint, length: bigint): Promise<BrowserFileReadResult>;
    readFileRecordById(file_id: Uint8Array): Promise<BrowserFileRecordReadResult>;
    readMetadata(path: string): Promise<BrowserMetadataResult>;
    readMetadataById(file_id: Uint8Array): Promise<BrowserMetadataResult>;
    readNamedAttribute(path: string, attribute_class: string, name: Uint8Array): Promise<BrowserNamedAttributeResult>;
    readReparsePoint(path: string): Promise<BrowserFileReadResult>;
    readSymbolicLink(path: string): Promise<BrowserFileReadResult>;
    rebaseHead(maximum_conflicts: number): Promise<BrowserRebaseResult>;
    refreshHead(): Promise<BrowserCheckpointResult>;
    refreshLive(): Promise<BrowserCheckpointResult>;
    remove(path: string, expected_file_id?: Uint8Array | null): Promise<BrowserMutationResult>;
    removeNamedAttribute(path: string, attribute_class: string, name: Uint8Array): Promise<BrowserMutationResult>;
    rename(source: string, destination: string, replace: boolean): Promise<BrowserMutationResult>;
    resizeFile(path: string, logical_bytes: bigint): Promise<BrowserMutationResult>;
    resizeFileById(file_id: Uint8Array, logical_bytes: bigint): Promise<BrowserMutationResult>;
    resolveFiles(paths: BrowserPathBatch): Promise<BrowserResolvedFiles>;
    resumeLive(operation_id: Uint8Array, maximum_attempts: number, maximum_conflicts: number): Promise<BrowserLiveMutationResult>;
    seekFileExtent(path: string, offset: bigint, target: string): Promise<BrowserExtentSeekResult>;
    seekFileExtentById(file_id: Uint8Array, offset: bigint, target: string): Promise<BrowserExtentSeekResult>;
    setAttributes(path: string, canonical_bytes: Uint8Array, logical_bytes?: bigint | null): Promise<BrowserMutationResult>;
    setAttributesById(file_id: Uint8Array, canonical_bytes: Uint8Array, logical_bytes?: bigint | null): Promise<BrowserMutationResult>;
    setMetadata(path: string, canonical_bytes: Uint8Array): Promise<BrowserMutationResult>;
    setMetadataById(file_id: Uint8Array, canonical_bytes: Uint8Array): Promise<BrowserMutationResult>;
    statNoFollow(path: string): Promise<BrowserStatResult>;
    writeFile(path: string, offset: bigint, bytes: Uint8Array): Promise<BrowserMutationResult>;
    writeFileById(file_id: Uint8Array, offset: bigint, bytes: Uint8Array): Promise<BrowserMutationResult>;
    writeNamedAttribute(path: string, attribute_class: string, name: Uint8Array, bytes: Uint8Array, mode: string): Promise<BrowserMutationResult>;
    zeroFileRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<BrowserMutationResult>;
    zeroFileRangeById(file_id: Uint8Array, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<BrowserMutationResult>;
    readonly acquisitionWork: BrowserWorkCounters;
}

/**
 * Browser-safe handle backed by the canonical Rust engine.
 */
export class BrowserFs {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    clearObjectCache(): void;
    close(): void;
    createSpeculation(volume_id: Uint8Array, generation_id: Uint8Array, options: any): BrowserSpeculation;
    createVolume(options: any): Promise<BrowserVolume>;
    createVolumeWithId(volume_id: Uint8Array, options: any): Promise<BrowserVolume>;
    createWorkspace(name: string): Promise<BrowserWorkspace>;
    exportGenerationBatch(manifest: any, cursor: bigint, maximum_objects: number, maximum_object_bytes: bigint): Promise<GenerationTransferBatchResult>;
    exportObject(object_id: Uint8Array, maximum_bytes: bigint): Promise<BrowserFileReadResult>;
    importGenerationBatch(manifest: any, cursor: bigint, objects: any, maximum_objects: number): Promise<GenerationTransferCursorResult>;
    importObject(object_id: Uint8Array, bytes: Uint8Array): Promise<BrowserMutationResult>;
    objectCacheStats(): any;
    openVolume(volume_id: Uint8Array): Promise<BrowserVolume>;
    openWorkspace(name: string): Promise<BrowserWorkspace>;
    operationWindows(): BrowserOperationWindowCoordinator;
    restoreVolume(manifest: any, operation_id: Uint8Array): Promise<BrowserVolume>;
    readonly capabilities: any;
}

/**
 * One exact immutable browser workspace generation.
 */
export class BrowserGeneration {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    listDirectory(path: string, after: any | null | undefined, maximum_entries: number): Promise<BrowserWorkspaceDirectoryPage>;
    pin(identity: string): Promise<BrowserGeneration>;
    planExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserWorkspaceExtentPlan>;
    read(path: string, maximum_bytes: bigint): Promise<Uint8Array>;
    readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
    readSymbolicLink(path: string): Promise<Uint8Array>;
    stat(path: string): Promise<BrowserWorkspaceStat>;
    readonly id: Uint8Array;
    readonly workspaceId: Uint8Array;
}

/**
 * One immutable, side-effect-free workspace join plan.
 */
export class BrowserJoinPlan {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    apply(if_target: Uint8Array, idempotency_key?: Uint8Array | null): Promise<BrowserJoinResult>;
    readonly commonAncestor: Uint8Array;
    readonly targetHead: Uint8Array;
}

/**
 * Durable leases sharing the filesystem's exact `IndexedDB` authority.
 */
export class BrowserOperationWindowCoordinator {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    begin(workspace_id: Uint8Array, parent: Uint8Array, owner: string, now_millis: bigint, expires_at_millis: bigint, lease_id?: Uint8Array | null): Promise<BrowserOperationWindowLease>;
    finish(lease: BrowserOperationWindowLease, now_millis: bigint): Promise<BrowserOperationWindowClose>;
    finishWorkspace(workspace: BrowserWorkspace, lease: BrowserOperationWindowLease, now_millis: bigint, options: BrowserOperationReconcileOptions): Promise<BrowserWorkspaceOperationClose>;
    inspect(workspace_id: Uint8Array): Promise<BrowserOperationWindowPhase>;
    observeParent(workspace_id: Uint8Array, parent: Uint8Array): Promise<boolean>;
    recoverWorkspace(workspace: BrowserWorkspace, now_millis: bigint, options: BrowserOperationReconcileOptions): Promise<BrowserWorkspaceRebaseResult | undefined>;
    renew(lease: BrowserOperationWindowLease, now_millis: bigint, expires_at_millis: bigint): Promise<BrowserOperationWindowLease>;
}

/**
 * One immutable file resolved against a pinned checkout generation.
 */
export class BrowserResolvedFile {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    readRange(offset: bigint, length: bigint): Promise<BrowserFileReadResult>;
    readSymbolicLink(): Promise<BrowserFileReadResult>;
    readonly kind: FileKind;
    readonly logicalBytes: bigint;
    readonly metadataCanonicalBytes: Uint8Array;
}

/**
 * One original-order resolved path batch and its shared work receipt.
 */
export class BrowserResolvedFiles {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    take(index: number): BrowserResolvedFile | undefined;
    readonly length: number;
    readonly work: BrowserWorkCounters;
}

/**
 * Browser owner of one volume generation's residency and promotion engines.
 */
export class BrowserSpeculation {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    executeResidency(operation_id: Uint8Array): Promise<BrowserResidencyExecution>;
    finishPromotion(operation_id: Uint8Array, useful: boolean): void;
    finishResidency(operation_id: Uint8Array, useful: boolean): void;
    metrics(): BrowserSpeculationMetrics;
    observe(observation: any): BrowserAdmissionResult;
    planPromotion(request: any): BrowserPromotionAdmission;
    preemptForForeground(bytes: bigint): BrowserSpeculationPreemption;
    replaceGeneration(generation_id: Uint8Array): BrowserSpeculationPreemption;
}

/**
 * One sparse atomic browser workspace transaction.
 */
export class BrowserTransaction {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cloneRange(source: string, source_offset: bigint, destination: string, destination_offset: bigint, length: bigint): Promise<void>;
    commit(lease: BrowserOperationWindowLease | undefined): Promise<BrowserWorkspaceCommit>;
    copy(source: string, destination: string): Promise<void>;
    createDirAll(path: string): Promise<void>;
    createDirectory(path: string): Promise<void>;
    createSymbolicLink(path: string, target: Uint8Array): Promise<void>;
    hardLink(source: string, destination: string): Promise<void>;
    preallocate(path: string, offset: bigint, length: bigint, keep_size: boolean): Promise<void>;
    rebase(maximum_conflicts: number): Promise<BrowserTransactionRebase>;
    remove(path: string): Promise<void>;
    rename(source: string, destination: string): Promise<void>;
    resize(path: string, logical_bytes: bigint): Promise<void>;
    write(path: string, bytes: Uint8Array): Promise<void>;
    writeRange(path: string, offset: bigint, bytes: Uint8Array): Promise<void>;
    zeroRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<void>;
}

/**
 * One independently configured browser volume.
 */
export class BrowserVolume {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    checkout(options: any): Promise<BrowserCheckout>;
    diffGenerations(before: Uint8Array, after: Uint8Array, maximum_changes: number): Promise<BrowserGenerationDiffResult>;
    readonly acquisitionWork: BrowserWorkCounters;
    readonly id: Uint8Array;
}

/**
 * One named customer workspace.
 */
export class BrowserWorkspace {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    beginTransaction(idempotency_key?: Uint8Array | null): Promise<BrowserTransaction>;
    checkpoint(label: string): Promise<BrowserGeneration>;
    delete(idempotency_key?: Uint8Array | null): Promise<string>;
    diff(from: BrowserGeneration, to: BrowserGeneration, maximum_changes: number): Promise<BrowserChangeSet>;
    fork(destination: string, idempotency_key?: Uint8Array | null): Promise<BrowserWorkspace>;
    forkAt(destination: string, generation: BrowserGeneration, options: BrowserForkOptions): Promise<BrowserWorkspace>;
    generation(id: Uint8Array): Promise<BrowserGeneration>;
    head(): Promise<Uint8Array>;
    joinInto(target: BrowserWorkspace, options: any): Promise<BrowserJoinPlan>;
    liveRebase(idempotency_key: Uint8Array | null | undefined, maximum_generations: number, maximum_changes: number, maximum_conflicts: number): Promise<BrowserWorkspaceRebaseResult>;
    pin(identity: string): Promise<BrowserGeneration>;
    planExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserWorkspaceExtentPlan>;
    read(path: string, maximum_bytes: bigint): Promise<Uint8Array>;
    readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
    readSymbolicLink(path: string): Promise<Uint8Array>;
    remove(path: string): Promise<BrowserWorkspaceCommit>;
    stat(path: string): Promise<BrowserWorkspaceStat>;
    sync(): Promise<BrowserGeneration>;
    write(path: string, bytes: Uint8Array): Promise<BrowserWorkspaceCommit>;
    readonly id: Uint8Array;
    readonly name: string;
}

/**
 * Browser-safe recursive multi-root context registry.
 */
export class BrowserWorkspaceContextRegistry {
    free(): void;
    [Symbol.dispose](): void;
    adoptRoot(context_id: Uint8Array, root_wire: Uint8Array): Promise<Uint8Array>;
    discardSubtree(parent_context_id: Uint8Array, child_context_id: Uint8Array, maximum: number): Promise<Uint8Array>;
    constructor();
    registerChild(context_id: Uint8Array, parent_context_id: Uint8Array, roots_wire: Uint8Array): Promise<Uint8Array>;
    registerRoot(context_id: Uint8Array, roots_wire: Uint8Array): Promise<Uint8Array>;
    removeRoot(context_id: Uint8Array, root_id: Uint8Array): Promise<Uint8Array>;
    resolve(context_id: Uint8Array): Promise<Uint8Array>;
    setActive(context_id: Uint8Array, active: boolean): Promise<Uint8Array>;
    setWorkspace(context_id: Uint8Array, root_id: Uint8Array, workspace_id: Uint8Array, workspace_name: string, parent_workspace_id?: Uint8Array | null): Promise<Uint8Array>;
}

/**
 * Decodes a versioned merge-candidate envelope to its canonical payload.
 */
export function decodeMergeCandidateJson(value_json: string): string;

/**
 * Decodes a versioned merge-plan envelope to its canonical payload.
 */
export function decodeMergePlanJson(value_json: string): string;

/**
 * Decodes a multi-root candidate envelope to its canonical payload.
 */
export function decodeMultiRootCandidateJson(value_json: string): string;

/**
 * Decodes a versioned multi-root plan envelope to its canonical payload.
 */
export function decodeMultiRootPlanJson(value_json: string): string;

/**
 * Decodes a versioned publication envelope to its canonical payload.
 */
export function decodePublicationJson(value_json: string): string;

/**
 * Encodes a merge-candidate payload in the versioned compatibility envelope.
 */
export function encodeMergeCandidateJson(value_json: string): string;

/**
 * Encodes a merge-plan payload in the versioned compatibility envelope.
 */
export function encodeMergePlanJson(value_json: string): string;

/**
 * Encodes a multi-root candidate payload in the compatibility envelope.
 */
export function encodeMultiRootCandidateJson(value_json: string): string;

/**
 * Encodes a multi-root plan payload in the versioned compatibility envelope.
 */
export function encodeMultiRootPlanJson(value_json: string): string;

/**
 * Encodes a publication payload in the versioned compatibility envelope.
 */
export function encodePublicationJson(value_json: string): string;

/**
 * Opens transactional browser storage with explicit `IndexedDB` or OPFS immutable objects.
 */
export function openBrowserFs(options: any): Promise<BrowserFs>;

/**
 * Opens the deterministic process-local reference backend.
 */
export function openMemoryFs(options: any): BrowserFs;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_browserchangeset_free: (a: number, b: number) => void;
    readonly __wbg_browsercheckout_free: (a: number, b: number) => void;
    readonly __wbg_browserfs_free: (a: number, b: number) => void;
    readonly __wbg_browsergeneration_free: (a: number, b: number) => void;
    readonly __wbg_browserjoinplan_free: (a: number, b: number) => void;
    readonly __wbg_browseroperationwindowcoordinator_free: (a: number, b: number) => void;
    readonly __wbg_browserresolvedfile_free: (a: number, b: number) => void;
    readonly __wbg_browserresolvedfiles_free: (a: number, b: number) => void;
    readonly __wbg_browserspeculation_free: (a: number, b: number) => void;
    readonly __wbg_browsertransaction_free: (a: number, b: number) => void;
    readonly __wbg_browservolume_free: (a: number, b: number) => void;
    readonly __wbg_browserworkspace_free: (a: number, b: number) => void;
    readonly __wbg_browserworkspacecontextregistry_free: (a: number, b: number) => void;
    readonly browserchangeset_changes: (a: number) => [number, number, number];
    readonly browserchangeset_compose: (a: number, b: number, c: any) => any;
    readonly browserchangeset_from: (a: number) => number;
    readonly browserchangeset_to: (a: number) => number;
    readonly browsercheckout_acquisitionWork: (a: number) => [number, number, number];
    readonly browsercheckout_applyTransaction: (a: number, b: any) => any;
    readonly browsercheckout_checkpoint: (a: number) => any;
    readonly browsercheckout_cloneFileRange: (a: number, b: number, c: number, d: bigint, e: number, f: number, g: bigint, h: bigint) => any;
    readonly browsercheckout_cloneFileRangeById: (a: number, b: number, c: number, d: bigint, e: number, f: number, g: bigint, h: bigint) => any;
    readonly browsercheckout_commit: (a: number, b: number, c: number, d: number) => any;
    readonly browsercheckout_createDevice: (a: number, b: number, c: number, d: number, e: number, f: any, g: any) => any;
    readonly browsercheckout_createDirectory: (a: number, b: number, c: number) => any;
    readonly browsercheckout_createFile: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_createReparsePoint: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_createSpecial: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_createSymbolicLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_discard: (a: number) => any;
    readonly browsercheckout_exportManifest: (a: number) => any;
    readonly browsercheckout_hardLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_listDirectory: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly browsercheckout_listDirectoryRecords: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly browsercheckout_listNamedAttributes: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: any) => any;
    readonly browsercheckout_lookupBatchNoFollow: (a: number, b: any) => any;
    readonly browsercheckout_lookupNoFollow: (a: number, b: number, c: number) => any;
    readonly browsercheckout_mutateLive: (a: number, b: any, c: number, d: number, e: any, f: any) => any;
    readonly browsercheckout_planFileExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: any) => any;
    readonly browsercheckout_planFileExtentsById: (a: number, b: number, c: number, d: bigint, e: bigint, f: any) => any;
    readonly browsercheckout_preallocateFile: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsercheckout_preallocateFileById: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsercheckout_prepareMerge: (a: number, b: number, c: number, d: any, e: any) => any;
    readonly browsercheckout_readFileRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsercheckout_readFileRangeById: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsercheckout_readFileRecordById: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readMetadata: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readMetadataById: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readNamedAttribute: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly browsercheckout_readReparsePoint: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browsercheckout_rebaseHead: (a: number, b: any) => any;
    readonly browsercheckout_refreshHead: (a: number) => any;
    readonly browsercheckout_refreshLive: (a: number) => any;
    readonly browsercheckout_remove: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_removeNamedAttribute: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly browsercheckout_rename: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browsercheckout_resizeFile: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsercheckout_resizeFileById: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsercheckout_resolveFiles: (a: number, b: any) => any;
    readonly browsercheckout_resumeLive: (a: number, b: number, c: number, d: any, e: any) => any;
    readonly browsercheckout_seekFileExtent: (a: number, b: number, c: number, d: bigint, e: number, f: number) => any;
    readonly browsercheckout_seekFileExtentById: (a: number, b: number, c: number, d: bigint, e: number, f: number) => any;
    readonly browsercheckout_setAttributes: (a: number, b: number, c: number, d: number, e: number, f: number, g: bigint) => any;
    readonly browsercheckout_setAttributesById: (a: number, b: number, c: number, d: number, e: number, f: number, g: bigint) => any;
    readonly browsercheckout_setMetadata: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_setMetadataById: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_statNoFollow: (a: number, b: number, c: number) => any;
    readonly browsercheckout_writeFile: (a: number, b: number, c: number, d: bigint, e: number, f: number) => any;
    readonly browsercheckout_writeFileById: (a: number, b: number, c: number, d: bigint, e: number, f: number) => any;
    readonly browsercheckout_writeNamedAttribute: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number) => any;
    readonly browsercheckout_zeroFileRange: (a: number, b: number, c: number, d: bigint, e: bigint, f: number, g: number) => any;
    readonly browsercheckout_zeroFileRangeById: (a: number, b: number, c: number, d: bigint, e: bigint, f: number, g: number) => any;
    readonly browserfs_capabilities: (a: number) => [number, number, number];
    readonly browserfs_clearObjectCache: (a: number) => [number, number];
    readonly browserfs_close: (a: number) => void;
    readonly browserfs_createSpeculation: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly browserfs_createVolume: (a: number, b: any) => any;
    readonly browserfs_createVolumeWithId: (a: number, b: number, c: number, d: any) => any;
    readonly browserfs_createWorkspace: (a: number, b: number, c: number) => any;
    readonly browserfs_exportGenerationBatch: (a: number, b: any, c: bigint, d: any, e: bigint) => any;
    readonly browserfs_exportObject: (a: number, b: number, c: number, d: bigint) => any;
    readonly browserfs_importGenerationBatch: (a: number, b: any, c: bigint, d: any, e: any) => any;
    readonly browserfs_importObject: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserfs_objectCacheStats: (a: number) => [number, number, number];
    readonly browserfs_openVolume: (a: number, b: number, c: number) => any;
    readonly browserfs_openWorkspace: (a: number, b: number, c: number) => any;
    readonly browserfs_operationWindows: (a: number) => [number, number, number];
    readonly browserfs_restoreVolume: (a: number, b: any, c: number, d: number) => any;
    readonly browsergeneration_id: (a: number) => [number, number];
    readonly browsergeneration_listDirectory: (a: number, b: number, c: number, d: number, e: any) => any;
    readonly browsergeneration_pin: (a: number, b: number, c: number) => any;
    readonly browsergeneration_planExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: any) => any;
    readonly browsergeneration_read: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsergeneration_readRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsergeneration_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browsergeneration_stat: (a: number, b: number, c: number) => any;
    readonly browsergeneration_workspaceId: (a: number) => [number, number];
    readonly browserjoinplan_apply: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserjoinplan_commonAncestor: (a: number) => [number, number];
    readonly browserjoinplan_targetHead: (a: number) => [number, number];
    readonly browseroperationwindowcoordinator_begin: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: bigint, i: bigint, j: number, k: number) => any;
    readonly browseroperationwindowcoordinator_finish: (a: number, b: any, c: bigint) => any;
    readonly browseroperationwindowcoordinator_finishWorkspace: (a: number, b: number, c: any, d: bigint, e: any) => any;
    readonly browseroperationwindowcoordinator_inspect: (a: number, b: number, c: number) => any;
    readonly browseroperationwindowcoordinator_observeParent: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browseroperationwindowcoordinator_recoverWorkspace: (a: number, b: number, c: bigint, d: any) => any;
    readonly browseroperationwindowcoordinator_renew: (a: number, b: any, c: bigint, d: bigint) => any;
    readonly browserresolvedfile_kind: (a: number) => [number, number, number];
    readonly browserresolvedfile_logicalBytes: (a: number) => bigint;
    readonly browserresolvedfile_metadataCanonicalBytes: (a: number) => [number, number, number, number];
    readonly browserresolvedfile_readRange: (a: number, b: bigint, c: bigint) => any;
    readonly browserresolvedfile_readSymbolicLink: (a: number) => any;
    readonly browserresolvedfiles_length: (a: number) => number;
    readonly browserresolvedfiles_take: (a: number, b: number) => [number, number, number];
    readonly browserresolvedfiles_work: (a: number) => [number, number, number];
    readonly browserspeculation_cancel: (a: number) => void;
    readonly browserspeculation_executeResidency: (a: number, b: number, c: number) => any;
    readonly browserspeculation_finishPromotion: (a: number, b: number, c: number, d: number) => [number, number];
    readonly browserspeculation_finishResidency: (a: number, b: number, c: number, d: number) => [number, number];
    readonly browserspeculation_metrics: (a: number) => [number, number, number];
    readonly browserspeculation_observe: (a: number, b: any) => [number, number, number];
    readonly browserspeculation_planPromotion: (a: number, b: any) => [number, number, number];
    readonly browserspeculation_preemptForForeground: (a: number, b: bigint) => [number, number, number];
    readonly browserspeculation_replaceGeneration: (a: number, b: number, c: number) => [number, number, number];
    readonly browsertransaction_cloneRange: (a: number, b: number, c: number, d: bigint, e: number, f: number, g: bigint, h: bigint) => any;
    readonly browsertransaction_commit: (a: number, b: number) => any;
    readonly browsertransaction_copy: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_createDirAll: (a: number, b: number, c: number) => any;
    readonly browsertransaction_createDirectory: (a: number, b: number, c: number) => any;
    readonly browsertransaction_createSymbolicLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_hardLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_preallocate: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsertransaction_rebase: (a: number, b: any) => any;
    readonly browsertransaction_remove: (a: number, b: number, c: number) => any;
    readonly browsertransaction_rename: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_resize: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsertransaction_write: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_writeRange: (a: number, b: number, c: number, d: bigint, e: number, f: number) => any;
    readonly browsertransaction_zeroRange: (a: number, b: number, c: number, d: bigint, e: bigint, f: number, g: number) => any;
    readonly browservolume_acquisitionWork: (a: number) => [number, number, number];
    readonly browservolume_checkout: (a: number, b: any) => any;
    readonly browservolume_diffGenerations: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly browservolume_id: (a: number) => [number, number];
    readonly browserworkspace_beginTransaction: (a: number, b: number, c: number) => any;
    readonly browserworkspace_checkpoint: (a: number, b: number, c: number) => any;
    readonly browserworkspace_delete: (a: number, b: number, c: number) => any;
    readonly browserworkspace_diff: (a: number, b: number, c: number, d: any) => any;
    readonly browserworkspace_fork: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspace_forkAt: (a: number, b: number, c: number, d: number, e: any) => any;
    readonly browserworkspace_generation: (a: number, b: number, c: number) => any;
    readonly browserworkspace_head: (a: number) => any;
    readonly browserworkspace_id: (a: number) => [number, number];
    readonly browserworkspace_joinInto: (a: number, b: number, c: any) => any;
    readonly browserworkspace_liveRebase: (a: number, b: number, c: number, d: any, e: any, f: any) => any;
    readonly browserworkspace_name: (a: number) => [number, number];
    readonly browserworkspace_pin: (a: number, b: number, c: number) => any;
    readonly browserworkspace_planExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: any) => any;
    readonly browserworkspace_read: (a: number, b: number, c: number, d: bigint) => any;
    readonly browserworkspace_readRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browserworkspace_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browserworkspace_remove: (a: number, b: number, c: number) => any;
    readonly browserworkspace_stat: (a: number, b: number, c: number) => any;
    readonly browserworkspace_sync: (a: number) => any;
    readonly browserworkspace_write: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspacecontextregistry_adoptRoot: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspacecontextregistry_discardSubtree: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly browserworkspacecontextregistry_new: () => number;
    readonly browserworkspacecontextregistry_registerChild: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly browserworkspacecontextregistry_registerRoot: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspacecontextregistry_removeRoot: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspacecontextregistry_resolve: (a: number, b: number, c: number) => any;
    readonly browserworkspacecontextregistry_setActive: (a: number, b: number, c: number, d: number) => any;
    readonly browserworkspacecontextregistry_setWorkspace: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number) => any;
    readonly decodeMergeCandidateJson: (a: number, b: number) => [number, number, number, number];
    readonly decodeMergePlanJson: (a: number, b: number) => [number, number, number, number];
    readonly decodeMultiRootCandidateJson: (a: number, b: number) => [number, number, number, number];
    readonly decodeMultiRootPlanJson: (a: number, b: number) => [number, number, number, number];
    readonly decodePublicationJson: (a: number, b: number) => [number, number, number, number];
    readonly encodeMergeCandidateJson: (a: number, b: number) => [number, number, number, number];
    readonly encodeMergePlanJson: (a: number, b: number) => [number, number, number, number];
    readonly encodeMultiRootCandidateJson: (a: number, b: number) => [number, number, number, number];
    readonly encodeMultiRootPlanJson: (a: number, b: number) => [number, number, number, number];
    readonly encodePublicationJson: (a: number, b: number) => [number, number, number, number];
    readonly openBrowserFs: (a: any) => any;
    readonly openMemoryFs: (a: any) => [number, number, number];
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___wasm_bindgen_2db2d17d2c533688___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_2db2d17d2c533688___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___web_sys_197818af2a22eac3___features__gen_IdbVersionChangeEvent__IdbVersionChangeEvent__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_2db2d17d2c533688___JsValue___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___js_sys_a5471da919551fc6___Function_fn_wasm_bindgen_2db2d17d2c533688___JsValue_____wasm_bindgen_2db2d17d2c533688___sys__Undefined___js_sys_a5471da919551fc6___Function_fn_wasm_bindgen_2db2d17d2c533688___JsValue_____wasm_bindgen_2db2d17d2c533688___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___web_sys_197818af2a22eac3___features__gen_Event__Event______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke_______true_: (a: number, b: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
