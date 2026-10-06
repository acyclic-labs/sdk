/* tslint:disable */
/* eslint-disable */
/**
 * The `ReadableStreamType` enum.
 *
 * *This API requires the following crate features to be activated: `ReadableStreamType`*
 */

type ReadableStreamType = "bytes";
export interface BrowserAdmissionResult {
    status: "admitted" | "rejected";
    rejection: string | undefined;
}

export interface BrowserBatchLookupEntryResult {
    exists: boolean;
    fileId: Uint8Array | undefined;
    fileKind: string | undefined;
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
    fileKind: string;
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
    fileKind: string;
    linkCount: bigint;
    metadataObject: Uint8Array;
    payloadKind: string;
    logicalBytes: bigint | undefined;
    payloadObject: Uint8Array | undefined;
    inlineBytes: Uint8Array | undefined;
    deviceMajor: number | undefined;
    deviceMinor: number | undefined;
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
    fileKind: string | undefined;
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
    fileKind: string;
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
    status: "committed" | "already-committed" | "conflict" | "fenced" | "idempotency-conflict";
    generationId: Uint8Array | undefined;
}

export interface BrowserWorkspaceDirectoryEntry {
    name: BrowserWorkspaceName;
    fileId: Uint8Array;
    kind: "regular" | "directory" | "symbolic-link" | "fifo" | "socket" | "character-device" | "block-device" | "reparse-point" | "mount-boundary";
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
    kind: "regular" | "directory" | "symbolic-link" | "fifo" | "socket" | "character-device" | "block-device" | "reparse-point" | "mount-boundary";
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

export type BrowserPathBatch = string[];

export type StreamErrorCode = "invalid_path" | "invalid_argument" | "limit_exceeded" | "not_found" | "already_exists" | "prefix_not_retained" | "out_of_range" | "idempotency_mismatch" | "capacity" | "access_denied" | "unavailable" | "hierarchy_changed" | "deadline_elapsed" | "unsupported";

export type TransactionOperation = { kind: "create-file"; path: string; bytes: Uint8Array } | { kind: "create-directory"; path: string } | { kind: "create-symbolic-link"; path: string; target: Uint8Array } | { kind: "create-special"; path: string; fileKind: "fifo" | "socket" | "mount-boundary" } | { kind: "create-device"; path: string; fileKind: "character-device" | "block-device"; major: number; minor: number } | { kind: "create-reparse-point"; path: string; payload: Uint8Array } | { kind: "remove"; path: string; expectedFileId: Uint8Array | undefined } | { kind: "rename"; source: string; destination: string; replace: boolean } | { kind: "hard-link"; source: string; destination: string } | { kind: "write"; path: string; offset: bigint; bytes: Uint8Array } | { kind: "set-metadata"; path: string; canonicalBytes: Uint8Array } | { kind: "resize"; path: string; logicalBytes: bigint } | { kind: "zero-range"; path: string; offset: bigint; length: bigint; allocated: boolean; extend: boolean } | { kind: "preallocate"; path: string; offset: bigint; length: bigint; keepSize: boolean } | { kind: "clone-range"; source: string; sourceOffset: bigint; destination: string; destinationOffset: bigint; length: bigint };


/**
 * One immutable semantic delta between exact generations.
 */
export class BrowserChangeSet {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Stable path-independent records and namespace binding changes.
     */
    changes(): BrowserGenerationDiffResult;
    /**
     * Composes contiguous immutable deltas by diffing their outer endpoints.
     */
    compose(next: BrowserChangeSet, maximum_changes: number): Promise<BrowserChangeSet>;
    /**
     * Exact immutable base endpoint.
     */
    readonly from: BrowserGeneration;
    /**
     * Exact immutable resulting endpoint.
     */
    readonly to: BrowserGeneration;
}

/**
 * One immutable-generation checkout with optional private COW mutations.
 */
export class BrowserCheckout {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies one ordered sparse mutation batch atomically within this volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operations, rejected semantics,
     * cancellation, storage, or bounded-work failure.
     */
    applyTransaction(operations: TransactionOperation[]): Promise<BrowserTransactionResult>;
    /**
     * Builds an immutable candidate generation without publishing authority.
     *
     * # Errors
     *
     * Returns a JavaScript error for invalid checkout state, corruption,
     * cancellation, storage failure, or bounded-work exhaustion.
     */
    checkpoint(): Promise<BrowserCheckpointResult>;
    /**
     * Clones one logical range by immutable extent reference.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/ranges,
     * cancellation, storage, or bounded work.
     */
    cloneFileRange(source: string, source_offset: bigint, destination: string, destination_offset: bigint, length: bigint): Promise<BrowserMutationResult>;
    /**
     * Clones one logical range between stable file identities.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identities/ranges, absence,
     * invalid kinds, storage, cancellation, or bounded work.
     */
    cloneFileRangeById(source_file_id: Uint8Array, source_offset: bigint, destination_file_id: Uint8Array, destination_offset: bigint, length: bigint): Promise<BrowserMutationResult>;
    /**
     * Checkpoints and conditionally publishes this private overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operation identity, clean
     * or read-only checkout, closure failure, cancellation, or bounded work.
     */
    commit(operation_id: Uint8Array): Promise<BrowserCommitResult>;
    /**
     * Creates an exact POSIX character or block device identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/kinds, unsupported
     * profile semantics, storage, cancellation, or bounded work.
     */
    createDevice(path: string, kind: string, major: number, minor: number): Promise<BrowserMutationResult>;
    /**
     * Creates one empty directory in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, allocation, or bounded work.
     */
    createDirectory(path: string): Promise<BrowserMutationResult>;
    /**
     * Creates one regular file in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, allocation, or bounded work.
     */
    createFile(path: string, bytes: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Creates an opaque exact Windows reparse-point payload.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, unsupported profile
     * semantics, excessive payload, storage, cancellation, or bounded work.
     */
    createReparsePoint(path: string, payload: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Creates an exact empty special namespace entry.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/kinds, unsupported
     * profile semantics, storage, cancellation, or bounded work.
     */
    createSpecial(path: string, kind: string): Promise<BrowserMutationResult>;
    /**
     * Creates one symbolic link with exact opaque target bytes.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * excessive targets, cancellation, storage, or bounded work.
     */
    createSymbolicLink(path: string, target: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Discards the private overlay and returns to its immutable base.
     *
     * # Errors
     *
     * Returns a JavaScript error for cancellation, corruption, storage, or work bounds.
     */
    discard(): Promise<BrowserMutationResult>;
    /**
     * Builds a deterministic complete manifest for resumable transfer.
     *
     * # Errors
     *
     * Returns a JavaScript error for checkpoint, closure, authentication,
     * cancellation, storage, serialization, or bounded work.
     */
    exportManifest(): Promise<BrowserExportManifestResult>;
    /**
     * Creates one hard link within the volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, invalid kinds,
     * conflicts, cancellation, storage, or bounded work.
     */
    hardLink(source: string, destination: string): Promise<BrowserMutationResult>;
    /**
     * Returns one bounded ordered directory page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/cursors,
     * non-directories, corruption, cancellation, or bounded work.
     */
    listDirectory(path: string, after: string | null | undefined, maximum_entries: number): Promise<BrowserDirectoryPageResult>;
    /**
     * Returns one bounded directory page with records and metadata fetched in batches.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/cursors,
     * non-directories, corruption, cancellation, or bounded work.
     */
    listDirectoryRecords(path: string, after: string | null | undefined, maximum_entries: number): Promise<BrowserDirectoryRecordPageResult>;
    /**
     * Returns one bounded ordered named-attribute page.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, cursor, path, storage, cancellation, or work failure.
     */
    listNamedAttributes(path: string, after_class: string | null | undefined, after_name: Uint8Array | null | undefined, maximum_entries: number): Promise<BrowserNamedAttributePageResult>;
    /**
     * Resolves a bounded path batch with shared authenticated frontiers.
     *
     * # Errors
     *
     * Returns a JavaScript error for non-array/excessive/malformed paths,
     * storage, cancellation, authentication, or bounded-work failure.
     */
    lookupBatchNoFollow(paths: BrowserPathBatch): Promise<BrowserBatchLookupResult>;
    /**
     * Resolves one canonical absolute path without following links.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths or authenticated
     * storage, cancellation, and bounded-work failures.
     */
    lookupNoFollow(path: string): Promise<BrowserLookupResult>;
    /**
     * Applies and publishes one direct-live transaction with bounded safe retries.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operations or identity,
     * wrong checkout mode, unresolved work, cancellation, storage, rebase,
     * or bounded-work failure.
     */
    mutateLive(operations: TransactionOperation[], operation_id: Uint8Array, maximum_attempts: number, maximum_conflicts: number): Promise<BrowserLiveTransactionResult>;
    /**
     * Plans one bounded sparse range without reading file content blobs.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed path/range, invalid bounds,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    planFileExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserExtentPlanResult>;
    /**
     * Plans one bounded sparse range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, invalid bounds,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    planFileExtentsById(file_id: Uint8Array, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserExtentPlanResult>;
    /**
     * Allocates sparse holes without replacing existing content.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed values, unsupported
     * keep-size physical allocation, cancellation, storage, or bounded work.
     */
    preallocateFile(path: string, offset: bigint, length: bigint, keep_size: boolean): Promise<BrowserMutationResult>;
    /**
     * Allocates sparse holes by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * unsupported allocation, storage, cancellation, or bounded work.
     */
    preallocateFileById(file_id: Uint8Array, offset: bigint, length: bigint, keep_size: boolean): Promise<BrowserMutationResult>;
    /**
     * Prepares a bounded two-parent merge against the current authority head.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, invalid checkout
     * state, non-head parent, corruption, cancellation, or bounded work.
     */
    prepareMerge(theirs: Uint8Array, maximum_changes: number, maximum_conflicts: number): Promise<BrowserMergePreparationResult>;
    /**
     * Reads one exact logical regular-file range.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, invalid ranges,
     * non-regular files, corruption, cancellation, or bounded work.
     */
    readFileRange(path: string, offset: bigint, length: bigint): Promise<BrowserFileReadResult>;
    /**
     * Reads one exact logical range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    readFileRangeById(file_id: Uint8Array, offset: bigint, length: bigint): Promise<BrowserFileReadResult>;
    /**
     * Reads one complete candidate file record by stable identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, authenticated
     * absence, storage, cancellation, or bounded-work failure.
     */
    readFileRecordById(file_id: Uint8Array): Promise<BrowserFileRecordReadResult>;
    /**
     * Reads complete canonical metadata bytes for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for path, storage, codec, cancellation, or work failure.
     */
    readMetadata(path: string): Promise<BrowserMetadataResult>;
    /**
     * Reads complete canonical metadata by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, absence, storage,
     * authentication, cancellation, encoding, or bounded work.
     */
    readMetadataById(file_id: Uint8Array): Promise<BrowserMetadataResult>;
    /**
     * Reads one exact named attribute value.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, path, storage, cancellation, or work failure.
     */
    readNamedAttribute(path: string, attribute_class: string, name: Uint8Array): Promise<BrowserNamedAttributeResult>;
    /**
     * Reads one opaque Windows reparse-point payload without interpreting it.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, wrong file kind,
     * storage, cancellation, authentication, or bounded work.
     */
    readReparsePoint(path: string): Promise<BrowserFileReadResult>;
    /**
     * Reads one symbolic link's exact opaque target bytes without following it.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, non-links,
     * corruption, cancellation, storage, or bounded work.
     */
    readSymbolicLink(path: string): Promise<BrowserFileReadResult>;
    /**
     * Safely advances to head and sparsely replays private mutations.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported consistency, invalid
     * bounds, corruption, cancellation, storage, replay, or bounded work.
     */
    rebaseHead(maximum_conflicts: number): Promise<BrowserRebaseResult>;
    /**
     * Explicitly advances a clean manual checkout to the authority head.
     *
     * # Errors
     *
     * Returns a JavaScript error for dirty state, storage, cancellation,
     * authentication, or bounded-work failure.
     */
    refreshHead(): Promise<BrowserCheckpointResult>;
    /**
     * Explicitly performs observation-safe synchronization for a live checkout.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported mode, conflicts, storage,
     * cancellation, authentication, or bounded-work failure.
     */
    refreshLive(): Promise<BrowserCheckpointResult>;
    /**
     * Removes one namespace binding.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/identities,
     * conflicts, cancellation, storage, or bounded work.
     */
    remove(path: string, expected_file_id?: Uint8Array | null): Promise<BrowserMutationResult>;
    /**
     * Removes one exact named attribute.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, mutation, cancellation, or work failure.
     */
    removeNamedAttribute(path: string, attribute_class: string, name: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Atomically renames one binding within the volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, or bounded work.
     */
    rename(source: string, destination: string, replace: boolean): Promise<BrowserMutationResult>;
    /**
     * Changes one regular file's logical length.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed values, invalid kinds,
     * cancellation, storage, or bounded work.
     */
    resizeFile(path: string, logical_bytes: bigint): Promise<BrowserMutationResult>;
    /**
     * Changes logical length by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/length, absence,
     * invalid kind, storage, cancellation, or bounded work.
     */
    resizeFileById(file_id: Uint8Array, logical_bytes: bigint): Promise<BrowserMutationResult>;
    /**
     * Resolves an ordered path batch once into immutable generation-bound handles.
     *
     * # Errors
     *
     * Returns a JavaScript error for non-pinned checkouts, malformed paths,
     * corruption, cancellation, or bounded work.
     */
    resolveFiles(paths: BrowserPathBatch): Promise<BrowserResolvedFiles>;
    /**
     * Resumes an unresolved direct-live transaction with the same operation identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, wrong checkout
     * mode, absent staged work, cancellation, storage, rebase, or bounds.
     */
    resumeLive(operation_id: Uint8Array, maximum_attempts: number, maximum_conflicts: number): Promise<BrowserLiveMutationResult>;
    /**
     * Finds the next sparse data or hole boundary without reading file bodies.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed path/offset/target,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    seekFileExtent(path: string, offset: bigint, target: string): Promise<BrowserExtentSeekResult>;
    /**
     * Finds the next sparse boundary by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/offset/target,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    seekFileExtentById(file_id: Uint8Array, offset: bigint, target: string): Promise<BrowserExtentSeekResult>;
    /**
     * Atomically replaces metadata and optional logical size for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed metadata/path, non-regular
     * resize, storage, cancellation, mutation, or bounded-work failure.
     */
    setAttributes(path: string, canonical_bytes: Uint8Array, logical_bytes?: bigint | null): Promise<BrowserMutationResult>;
    /**
     * Atomically replaces metadata and optional logical size by file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/metadata,
     * non-regular resize, storage, cancellation, or bounded work.
     */
    setAttributesById(file_id: Uint8Array, canonical_bytes: Uint8Array, logical_bytes?: bigint | null): Promise<BrowserMutationResult>;
    /**
     * Atomically replaces complete canonical metadata for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for path, codec, mutation, cancellation, or work failure.
     */
    setMetadata(path: string, canonical_bytes: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Replaces complete canonical metadata by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/metadata, absence,
     * storage, cancellation, or bounded work.
     */
    setMetadataById(file_id: Uint8Array, canonical_bytes: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Returns one complete no-follow file record and canonical metadata.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, storage,
     * authentication, cancellation, encoding, or bounded-work failure.
     */
    statNoFollow(path: string): Promise<BrowserStatResult>;
    /**
     * Replaces one logical file range in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/offsets,
     * non-regular files, cancellation, storage, or bounded work.
     */
    writeFile(path: string, offset: bigint, bytes: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Replaces one logical range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/offset,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    writeFileById(file_id: Uint8Array, offset: bigint, bytes: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Inserts or replaces one exact named attribute.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, mode, mutation, cancellation, or work failure.
     */
    writeNamedAttribute(path: string, attribute_class: string, name: Uint8Array, bytes: Uint8Array, mode: string): Promise<BrowserMutationResult>;
    /**
     * Punches a hole or records physically allocated zeros.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed ranges/kinds,
     * cancellation, storage, or bounded work.
     */
    zeroFileRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<BrowserMutationResult>;
    /**
     * Punches a hole or records allocated zero by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * invalid kind, storage, cancellation, or bounded work.
     */
    zeroFileRangeById(file_id: Uint8Array, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<BrowserMutationResult>;
    /**
     * Returns exact bounded work used to acquire this checkout handle.
     */
    readonly acquisitionWork: BrowserWorkCounters;
}

/**
 * Authenticated Rust-owned Filesystem grpc-web client for browser WASM.
 *
 * This client uses the generated FilesystemService contract directly. It
 * retains negotiated bounds for every later request and exposes streamed
 * exports through Rust futures; dropping a stream cancels its fetch.
 */
export class BrowserFilesystemClient {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Sends a typed cancellation request encoded by the Rust contract.
     */
    cancel(request: Uint8Array): Promise<Uint8Array>;
    /**
     * Returns negotiated capability limits to JavaScript.
     */
    capabilities(): any;
    /**
     * Connects from JavaScript using the Rust-owned authenticated handshake.
     */
    static connect(endpoint: string, bearer_token: string, maximum_request_bytes: bigint, maximum_response_bytes: bigint): Promise<BrowserFilesystemClient>;
    /**
     * Collects a typed export stream into encoded chunks.
     */
    export(request: Uint8Array): Promise<Array<any>>;
}

/**
 * Browser-safe handle backed by the canonical Rust engine.
 */
export class BrowserFs {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Discards resident acceleration without changing persistent state.
     *
     * # Errors
     *
     * Returns a JavaScript error after close or poisoned cache state.
     */
    clearObjectCache(): void;
    /**
     * Releases browser handles. Durable state remains in the selected browser stores.
     */
    close(): void;
    /**
     * Creates both generation-fenced speculation engines over this
     * browser filesystem's authenticated object backend and shared cache.
     *
     * # Errors
     *
     * Returns a JavaScript error after close, for malformed identities or
     * options, or when either engine rejects its hard policy.
     */
    createSpeculation(volume_id: Uint8Array, generation_id: Uint8Array, options: any): BrowserSpeculation;
    /**
     * Creates one independently configured volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported durability, storage,
     * authentication, cancellation, or bounded-work failure.
     */
    createVolume(options: any): Promise<BrowserVolume>;
    /**
     * Idempotently creates one caller-selected volume identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, incompatible
     * existing configuration, unsupported semantics, or storage failure.
     */
    createVolumeWithId(volume_id: Uint8Array, options: any): Promise<BrowserVolume>;
    /**
     * Creates or idempotently reopens one named workspace.
     *
     * # Errors
     *
     * Returns closed-engine, invalid-name, authority, or storage failures.
     */
    createWorkspace(name: string): Promise<BrowserWorkspace>;
    /**
     * Exports one bounded manifest-ordered immutable-object page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifests, invalid cursors,
     * cancellation, storage, allocation, or bounded-work failures.
     */
    exportGenerationBatch(manifest: any, cursor: bigint, maximum_objects: number, maximum_object_bytes: bigint): Promise<GenerationTransferBatchResult>;
    /**
     * Exports one exact authenticated immutable object for resumable transfer.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, absence,
     * corruption, cancellation, storage, or bounded work.
     */
    exportObject(object_id: Uint8Array, maximum_bytes: bigint): Promise<BrowserFileReadResult>;
    /**
     * Idempotently imports one manifest-aligned immutable-object page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifests, cursor/body
     * bounds, cancellation, storage, or bounded-work failures.
     */
    importGenerationBatch(manifest: any, cursor: bigint, objects: any, maximum_objects: number): Promise<GenerationTransferCursorResult>;
    /**
     * Idempotently imports one immutable object under its authenticated identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, digest mismatch,
     * cancellation, storage, or bounded work.
     */
    importObject(object_id: Uint8Array, bytes: Uint8Array): Promise<BrowserMutationResult>;
    /**
     * Exact process-local immutable-object accelerator telemetry.
     *
     * # Errors
     *
     * Returns a JavaScript error after close, poisoned cache state, or
     * serialization failure.
     */
    objectCacheStats(): any;
    /**
     * Opens one previously created or restored persistent volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, authenticated
     * absence, storage corruption, cancellation, or bounded work.
     */
    openVolume(volume_id: Uint8Array): Promise<BrowserVolume>;
    /**
     * Opens one existing named workspace.
     *
     * # Errors
     *
     * Returns closed-engine, invalid-name, absence, authority, or storage failures.
     */
    openWorkspace(name: string): Promise<BrowserWorkspace>;
    /**
     * Restores authority only after authenticating a complete imported closure.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifest, incomplete or
     * corrupt closure, conflicting authority, cancellation, or bounded work.
     */
    restoreVolume(manifest: any, operation_id: Uint8Array): Promise<BrowserVolume>;
    /**
     * Exact backend facts selected during open.
     *
     * # Errors
     *
     * Returns a JavaScript error when capability serialization fails.
     */
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
    /**
     * Retains this exact generation under one opaque identity.
     */
    pin(identity: string): Promise<BrowserGeneration>;
    planExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserWorkspaceExtentPlan>;
    /**
     * Reads one complete file from this exact immutable state.
     */
    read(path: string, maximum_bytes: bigint): Promise<Uint8Array>;
    readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
    readSymbolicLink(path: string): Promise<Uint8Array>;
    stat(path: string): Promise<BrowserWorkspaceStat>;
    /**
     * Content-addressed generation identity.
     */
    readonly id: Uint8Array;
    /**
     * Owning opaque workspace identity.
     */
    readonly workspaceId: Uint8Array;
}

/**
 * Browser-safe Git-shaped compatibility history over the canonical Rust state machine.
 */
export class BrowserGitCompatRepository {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Validates and canonicalizes a Rust Git output before JS projection.
     */
    canonicalizeOutputJson(value_json: string): string;
    /**
     * Validates and canonicalizes a durable pending transition before JS
     * projection.
     */
    canonicalizePendingTransitionJson(value_json: string): string;
    /**
     * Parses and executes a Git-shaped argv command using the Rust source of truth.
     */
    executeArgvJson(argv: string[], workspace_generation: Uint8Array, default_author: string, now_seconds: bigint): Promise<string>;
    /**
     * Executes one typed command encoded with the public serde contract.
     */
    executeJson(command_json: string, workspace_generation: Uint8Array): Promise<string>;
    /**
     * Executes a natural JavaScript Git command through the Rust
     * projection shared with the native binding.
     */
    executePublicJson(command_json: string, workspace_generation: Uint8Array): Promise<string>;
    /**
     * Creates process-local compatibility state for one SDK workspace.
     */
    constructor(workspace_id: Uint8Array);
}

/**
 * Authenticated Rust-owned Harness gRPC-Web client for browser WASM.
 */
export class BrowserHarnessClient {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(request: Uint8Array): Promise<Uint8Array>;
    capabilities(): any;
    static connect(endpoint: string, bearer_token: string): Promise<BrowserHarnessClient>;
    static connectWithLimits(endpoint: string, bearer_token: string, maximum_request_bytes: bigint, maximum_response_bytes: bigint): Promise<BrowserHarnessClient>;
    observe(request: Uint8Array): Promise<Uint8Array>;
    replay(request: Uint8Array): Promise<Array<any>>;
    submit(request: Uint8Array): Promise<Uint8Array>;
}

/**
 * One immutable, side-effect-free workspace join plan.
 */
export class BrowserJoinPlan {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies this immutable plan through one exact target-head CAS.
     */
    apply(if_target: Uint8Array, idempotency_key?: Uint8Array | null): Promise<BrowserJoinResult>;
    /**
     * Exact discovered common ancestor.
     */
    readonly commonAncestor: Uint8Array;
    /**
     * Target generation observed while planning.
     */
    readonly targetHead: Uint8Array;
}

/**
 * One immutable file resolved against a pinned checkout generation.
 */
export class BrowserResolvedFile {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Reads one exact logical range without another namespace lookup.
     */
    readRange(offset: bigint, length: bigint): Promise<BrowserFileReadResult>;
    /**
     * Reads opaque symbolic-link target bytes without another namespace lookup.
     */
    readSymbolicLink(): Promise<BrowserFileReadResult>;
    /**
     * Terminal file kind authenticated by the pinned generation.
     */
    readonly kind: string;
    /**
     * Logical content length authenticated by the pinned generation.
     */
    readonly logicalBytes: bigint;
    /**
     * Complete canonical metadata authenticated by the pinned generation.
     */
    readonly metadataCanonicalBytes: Uint8Array;
}

/**
 * One original-order resolved path batch and its shared work receipt.
 */
export class BrowserResolvedFiles {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Transfers one generation-bound handle to JavaScript. Each index may be taken once.
     */
    take(index: number): BrowserResolvedFile | undefined;
    /**
     * Number of original-order results.
     */
    readonly length: number;
    /**
     * Exact work receipt for the shared namespace traversal.
     */
    readonly work: BrowserWorkCounters;
}

/**
 * Browser owner of one volume generation's residency and promotion engines.
 */
export class BrowserSpeculation {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Cooperatively cancels future residency execution from this owner.
     */
    cancel(): void;
    /**
     * Executes one admitted residency prediction through the browser's
     * authenticated object backend and shared cache.
     *
     * # Errors
     *
     * Returns a JavaScript error for an inactive operation, storage or
     * authentication failure, bounded-work exhaustion, or cancellation.
     */
    executeResidency(operation_id: Uint8Array): Promise<BrowserResidencyExecution>;
    /**
     * Records terminal usefulness for one promotion operation.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed or inactive operation identities.
     */
    finishPromotion(operation_id: Uint8Array, useful: boolean): void;
    /**
     * Records terminal usefulness for one residency operation.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed or inactive operation identities.
     */
    finishResidency(operation_id: Uint8Array, useful: boolean): void;
    /**
     * Returns exact payload-free metrics for both engines.
     *
     * # Errors
     *
     * Returns a JavaScript error if metrics cannot be serialized.
     */
    metrics(): BrowserSpeculationMetrics;
    /**
     * Records foreground demand and admits one authenticated successor.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed input or a failed bounded transition.
     */
    observe(observation: any): BrowserAdmissionResult;
    /**
     * Plans one bounded promotion from exact caller-observed location facts.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed facts, unsupported tiers,
     * inactive residency, or a failed bounded transition.
     */
    planPromotion(request: any): BrowserPromotionAdmission;
    /**
     * Atomically preempts both engines before recording foreground bytes.
     *
     * # Errors
     *
     * Returns a JavaScript error if exact bounded accounting fails.
     */
    preemptForForeground(bytes: bigint): BrowserSpeculationPreemption;
    /**
     * Atomically fences both engines onto a new immutable generation.
     *
     * # Errors
     *
     * Returns a JavaScript error for a malformed identity or failed transition.
     */
    replaceGeneration(generation_id: Uint8Array): BrowserSpeculationPreemption;
}

/**
 * One sparse atomic browser workspace transaction.
 */
export class BrowserTransaction {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Clones one immutable range without reading file bytes.
     */
    cloneRange(source: string, source_offset: bigint, destination: string, destination_offset: bigint, length: bigint): Promise<void>;
    /**
     * Publishes the complete candidate through one idempotent head CAS.
     */
    commit(): Promise<any>;
    /**
     * Clones one complete regular file without copying its body.
     */
    copy(source: string, destination: string): Promise<void>;
    /**
     * Creates every absent directory on one canonical path.
     */
    createDirAll(path: string): Promise<void>;
    /**
     * Creates exactly one empty directory.
     */
    createDirectory(path: string): Promise<void>;
    /**
     * Creates one symbolic link with an opaque target.
     */
    createSymbolicLink(path: string, target: Uint8Array): Promise<void>;
    /**
     * Creates one same-workspace hard link.
     */
    hardLink(source: string, destination: string): Promise<void>;
    /**
     * Preallocates one sparse range without replacing content.
     */
    preallocate(path: string, offset: bigint, length: bigint, keep_size: boolean): Promise<void>;
    /**
     * Safely advances this retained candidate and sparsely replays its work.
     */
    rebase(maximum_conflicts: number): Promise<any>;
    /**
     * Removes one existing namespace binding inside this transaction.
     */
    remove(path: string): Promise<void>;
    /**
     * Atomically renames one namespace binding inside this transaction.
     */
    rename(source: string, destination: string): Promise<void>;
    /**
     * Changes one regular file's logical length.
     */
    resize(path: string, logical_bytes: bigint): Promise<void>;
    /**
     * Creates or replaces one complete file inside this transaction.
     */
    write(path: string, bytes: Uint8Array): Promise<void>;
    /**
     * Replaces one sparse regular-file range.
     */
    writeRange(path: string, offset: bigint, bytes: Uint8Array): Promise<void>;
    /**
     * Punches a hole or installs allocated zeros over one exact range.
     */
    zeroRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<void>;
}

/**
 * One independently configured browser volume.
 */
export class BrowserVolume {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Opens the volume head with explicit access and consistency semantics.
     *
     * # Errors
     *
     * Returns a JavaScript error for an invalid mode or authenticated
     * storage, cancellation, and bounded-work failures.
     */
    checkout(options: any): Promise<BrowserCheckout>;
    /**
     * Computes one bounded Merkle-aware semantic generation diff.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identities, corrupt
     * storage, cancellation, allocation, or bounded work.
     */
    diffGenerations(before: Uint8Array, after: Uint8Array, maximum_changes: number): Promise<BrowserGenerationDiffResult>;
    /**
     * Returns exact bounded work used to acquire this volume handle.
     */
    readonly acquisitionWork: BrowserWorkCounters;
    /**
     * Returns the canonical 16-byte volume identity.
     */
    readonly id: Uint8Array;
}

/**
 * One named customer workspace.
 */
export class BrowserWorkspace {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Begins one sparse atomic transaction at the current workspace head.
     */
    beginTransaction(idempotency_key?: Uint8Array | null): Promise<BrowserTransaction>;
    /**
     * Retains the current generation under one human-readable label.
     */
    checkpoint(label: string): Promise<BrowserGeneration>;
    /**
     * Terminally removes this mutable workspace head.
     */
    delete(idempotency_key?: Uint8Array | null): Promise<string>;
    /**
     * Computes one immutable bounded semantic delta between exact generations.
     */
    diff(from: BrowserGeneration, to: BrowserGeneration, maximum_changes: number): Promise<BrowserChangeSet>;
    /**
     * Forks the current generation into an independent named workspace.
     */
    fork(destination: string, idempotency_key?: Uint8Array | null): Promise<BrowserWorkspace>;
    /**
     * Creates an independent workspace at one caller-selected exact generation.
     */
    forkAt(destination: string, generation: BrowserGeneration, idempotency_key?: Uint8Array | null): Promise<BrowserWorkspace>;
    /**
     * Current immutable generation identity.
     */
    head(): Promise<Uint8Array>;
    /**
     * Builds one immutable side-effect-free plan for joining this workspace into a target.
     */
    joinInto(target: BrowserWorkspace, options: any): Promise<BrowserJoinPlan>;
    /**
     * Advances this fork onto its source workspace's current generation.
     */
    liveRebase(idempotency_key: Uint8Array | null | undefined, maximum_generations: number, maximum_changes: number, maximum_conflicts: number): Promise<BrowserWorkspaceRebaseResult>;
    /**
     * Retains the current generation under one opaque stable identity.
     */
    pin(identity: string): Promise<BrowserGeneration>;
    planExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<BrowserWorkspaceExtentPlan>;
    /**
     * Reads one complete regular file under a byte bound.
     */
    read(path: string, maximum_bytes: bigint): Promise<Uint8Array>;
    readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
    readSymbolicLink(path: string): Promise<Uint8Array>;
    /**
     * Removes one existing path atomically.
     */
    remove(path: string): Promise<BrowserWorkspaceCommit>;
    stat(path: string): Promise<BrowserWorkspaceStat>;
    /**
     * Synchronizes prior operations and returns the exact immutable head.
     */
    sync(): Promise<BrowserGeneration>;
    /**
     * Atomically creates or replaces one complete file.
     */
    write(path: string, bytes: Uint8Array): Promise<BrowserWorkspaceCommit>;
    /**
     * Stable opaque workspace identity.
     */
    readonly id: Uint8Array;
    /**
     * Canonical workspace name.
     */
    readonly name: string;
}

/**
 * Browser-safe recursive multi-root context registry.
 */
export class BrowserWorkspaceContextRegistry {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Adopts one parent-authorized root without enumerating its contents.
     */
    adoptRoot(context_id: Uint8Array, root_wire: Uint8Array): Promise<Uint8Array>;
    /**
     * Recursively tombstones a direct-child subtree.
     */
    discardSubtree(parent_context_id: Uint8Array, child_context_id: Uint8Array, maximum: number): Promise<Uint8Array>;
    /**
     * Creates an in-process registry. Host persistence can be injected by
     * the JavaScript embedding when it is available.
     */
    constructor();
    /**
     * Registers an exact direct child from the shared serde contract.
     */
    registerChild(context_id: Uint8Array, parent_context_id: Uint8Array, roots_wire: Uint8Array): Promise<Uint8Array>;
    /**
     * Registers one root context from the shared serde contract.
     */
    registerRoot(context_id: Uint8Array, roots_wire: Uint8Array): Promise<Uint8Array>;
    /**
     * Releases one root after callers have settled its filesystem changes.
     */
    removeRoot(context_id: Uint8Array, root_id: Uint8Array): Promise<Uint8Array>;
    /**
     * Resolves one context as stable JSON.
     */
    resolve(context_id: Uint8Array): Promise<Uint8Array>;
    /**
     * Freezes or resumes one durable context.
     */
    setActive(context_id: Uint8Array, active: boolean): Promise<Uint8Array>;
    /**
     * Advances one root binding after a compatibility branch switch.
     */
    setWorkspace(context_id: Uint8Array, root_id: Uint8Array, workspace_id: Uint8Array, workspace_name: string, parent_workspace_id?: Uint8Array | null): Promise<Uint8Array>;
}

/**
 * Rust-owned state machine for the polling form of hosted HTTP follow.
 * The JavaScript boundary supplies only fetch and timer primitives.
 */
export class HttpFollowCursor {
    free(): void;
    [Symbol.dispose](): void;
    acceptRead(response_json: string): void;
    acceptTail(response_json: string): void;
    close(): void;
    isClosed(): boolean;
    constructor(input: Uint8Array);
    pollDelayMillis(): number;
    readRequest(): Uint8Array;
    shouldPoll(): boolean;
    tailRequest(): Uint8Array;
}

declare class IntoUnderlyingByteSource {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    pull(controller: ReadableByteStreamController): Promise<any>;
    start(controller: ReadableByteStreamController): void;
    readonly autoAllocateChunkSize: number;
    readonly type: ReadableStreamType;
}
export type { IntoUnderlyingByteSource };

declare class IntoUnderlyingSink {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    abort(reason: any): Promise<any>;
    close(): Promise<any>;
    write(chunk: any): Promise<any>;
}
export type { IntoUnderlyingSink };

declare class IntoUnderlyingSource {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    pull(controller: ReadableStreamDefaultController): Promise<any>;
}
export type { IntoUnderlyingSource };

/**
 * One Rust-backed live follow cursor.
 *
 * `next` releases the state lock before awaiting the stream, so `close` can
 * always signal a pending call and promptly release its cursor.
 */
export class WasmFollow {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Cancels the cursor and wakes any pending `next` call.
     */
    close(): void;
    /**
     * Waits for one record. Returns `null` after close or stream termination.
     */
    next(): Promise<Uint8Array | null>;
}

/**
 * Stateful browser provider backed by the canonical Rust memory provider.
 *
 * Unary operations use `dispatch(operation, request_bytes)` and return the
 * corresponding protobuf response bytes. `read` and `children` return arrays
 * of encoded stream response messages because protobuf streams have no single
 * finite response envelope.
 */
export class WasmMemoryStream {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
     */
    children(input: Uint8Array): Promise<Uint8Array[]>;
    /**
     * Executes one finite unary operation over canonical protobuf bytes.
     */
    dispatch(operation: string, input: Uint8Array): Promise<Uint8Array>;
    constructor();
    /**
     * Opens a live follow cursor backed by the canonical provider.
     */
    open_follow(input: Uint8Array): Promise<WasmFollow>;
    /**
     * Reads one bounded page, returning encoded `ReadResponse` messages.
     */
    read(input: Uint8Array): Promise<Uint8Array[]>;
}

/**
 * Type-only bridge for the complete Rust-owned Stream error-code contract.
 */
export function __streamErrorCodeContract(value: StreamErrorCode): StreamErrorCode;

/**
 * Advances the cumulative byte count for a hosted response. The caller may
 * read chunks natively, but Rust owns overflow and configured-bound policy.
 */
export function consumeHttpResponseBytes(total: bigint, chunk: bigint, maximum: bigint): bigint;

/**
 * Validate and project one hosted HTTP JSON success response into the public
 * JavaScript shape. Rust owns the scalar widths and tagged response schema:
 * decimal uint64 strings become `bigint`, base64 bytes become `Uint8Array`,
 * and token timestamps become `Date` values before the value crosses the
 * browser boundary.
 */
export function decodeHttpResponse(route: string, response_json: string): unknown;

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
 * Returns the canonical default cumulative hosted response bound.
 */
export function defaultHttpResponseBytes(): bigint;

/**
 * Encode one protobuf request into the hosted Stream HTTP JSON shape.
 *
 * Protobuf remains the only request contract crossing from TypeScript into
 * Rust.  Rust owns the conversion of uint64 values and opaque bytes to the
 * decimal and base64 spellings required by the hosted API, keeping the HTTP
 * adapter from maintaining a second scalar conversion table.
 */
export function encodeHttpRequest(route: string, input: Uint8Array): string;

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
 * Return whether a code can be emitted by this WASM adapter.
 *
 * Keeping this validator beside the Rust error mapping prevents the TypeScript adapter from
 * maintaining a second, potentially stale list of base Stream error codes.
 */
export function is_stream_error_code(value: string): boolean;

/**
 * Validates one hosted read page and returns its canonical follow cursor.
 */
export function nextHttpFollowCursor(response_json: string, from: bigint): bigint;

/**
 * Normalize and encode canonical protobuf bytes for one commit request.
 *
 * The returned bytes use the same deterministic ordering as the in-memory
 * provider. Validation failures are thrown as stable error codes.
 */
export function normalizeCommitRequest(input: Uint8Array): Uint8Array;

/**
 * Opens transactional browser storage with explicit `IndexedDB` or OPFS immutable objects.
 *
 * # Errors
 *
 * Returns a JavaScript error for invalid options or unavailable required storage.
 */
export function openBrowserFs(options: any): Promise<BrowserFs>;

/**
 * Opens the deterministic process-local reference backend.
 *
 * # Errors
 *
 * Returns a JavaScript error when the memory options are invalid.
 */
export function openMemoryFs(options: any): BrowserFs;

/**
 * Validates and projects one gRPC read response. Rust owns protobuf decoding,
 * record bounds, commit identity width, and request-relative contiguity.
 */
export function projectGrpcReadResponse(input: Uint8Array, expected: bigint): unknown;

export function projectHostedSourceState(state: number, reason: number, has_generation: boolean): any;

/**
 * Decode one unary memory-provider response from canonical protobuf bytes
 * into the public JavaScript result shape. Rust owns the response oneofs,
 * scalar widths, copied byte buffers, and camelCase projection at this
 * boundary; TypeScript keeps only request adaptation and cursor lifecycle.
 */
export function projectMemoryResponse(operation: string, input: Uint8Array): unknown;

/**
 * Project a hosted HTTP error code onto the public Stream error vocabulary.
 *
 * The hosted API may report either the Rust-owned wire code or a public alias.
 * Unknown values and a commit-only alias on another route return no value.
 */
export function publicHttpErrorCode(raw: string, route: string): string | undefined;

/**
 * Validate canonical protobuf bytes for one append request.
 *
 * The empty string means that the request passed the same domain validators as
 * the in-memory provider. Otherwise this returns one stable error code.
 */
export function validateAppendRequest(input: Uint8Array): string;

/**
 * Validates the bearer credential shared by the native and browser Stream
 * clients. The empty string means success; failures use a stable Rust-owned
 * invalid-argument boundary consumed by generated facades.
 */
export function validateBearerToken(token: string): string;

/**
 * Validates request-relative child-page semantics through the canonical Rust
 * provider rules before a public page reaches a TypeScript caller.
 */
export function validateChildrenPageResponse(request: Uint8Array, response: Uint8Array): void;

/**
 * Validates request-relative gRPC identities through the canonical wire
 * model. The adapter supplies only the expected identity bytes.
 */
export function validateGrpcResponseIdentity(operation: string, input: Uint8Array, expected: Uint8Array): void;

export function validateHostedAdvertisedLimits(maximum_transaction_mutations: number, maximum_page_items: number): void;

export function validateHostedCredentialExpiry(expires_at_unix_seconds: string, now_unix_seconds: bigint): void;

export function validateHostedGenerationBounds(maximum_generations: number, maximum_changes: number, maximum_conflicts: number, maximum_page_items: number): void;

export function validateHostedGenerationContinuity(left_generation_id: Uint8Array, right_generation_id: Uint8Array): void;

export function validateHostedGenerationIdentity(generation_id: Uint8Array, owner_workspace_id: Uint8Array, expected_workspace_id: Uint8Array): void;

export function validateHostedPageBound(value: number, maximum: number): void;

export function validateHostedResponseBytes(maximum_response_bytes: number, minimum_handshake_response_bytes: number): void;

export function validateHostedSourceState(state: number, reason: number, has_generation: boolean): void;

export function validateHostedTransactionBounds(mutation_count: number, maximum_mutations: number, maximum_conflicts: number, maximum_page_items: number): void;

/**
 * Validates the endpoint policy shared by native and browser HTTP clients.
 * HTTPS is required for hosted endpoints; HTTP is allowed only for loopback
 * fixture servers. The return value is empty for a valid endpoint.
 */
export function validateHttpEndpoint(endpoint: string): string;

/**
 * Validate a hosted read page against the request cursor captured by the
 * caller. Rust owns record shape and cursor contiguity; the HTTP adapter only
 * supplies the response text and its request-relative starting position.
 */
export function validateHttpReadResponse(response_json: string, from: bigint): void;

/**
 * Validate one hosted HTTP JSON success response using the same path, width,
 * identity, and tagged-union rules as the canonical Stream domain.
 *
 * The HTTP adapter keeps its intentionally simple JSON representation (u64
 * values are decimal strings and opaque bytes are base64). This entry point
 * validates that representation using the same Rust projection used by
 * `decodeHttpResponse`, without crossing a second scalar schema boundary.
 */
export function validateHttpResponse(route: string, response_json: string): void;

/**
 * Validate one caller retry identity through the canonical Stream model.
 */
export function validateIdempotencyKey(input: Uint8Array): string;

/**
 * Validate one canonical Stream path using the same parser used by every
 * provider and wire decoder.
 *
 * The empty string means success; failures use the stable Stream error code
 * consumed by the TypeScript adapter.
 */
export function validatePath(path: string): string;

/**
 * Validate one remote bearer credential using the shared Rust policy.
 */
export function validateRemoteWebCredential(token: string): void;

/**
 * Validate one remote endpoint using the shared Rust policy.
 */
export function validateRemoteWebEndpoint(endpoint: string): void;

/**
 * Validate a wire handshake and return its canonical Rust-owned capabilities.
 */
export function validateRemoteWebFilesystemHandshake(response: Uint8Array): Uint8Array;

/**
 * Validate one remote gRPC endpoint using the shared Rust policy.
 */
export function validateRemoteWebGrpcEndpoint(endpoint: string): void;

/**
 * Validate one canonical protobuf request at the browser boundary.
 *
 * `kind` is deliberately a small closed set so callers cannot accidentally
 * select a different validator after adding a new wire message. The empty
 * string means success; failures use the same stable codes as the append and
 * commit entry points.
 */
export function validateRequest(kind: string, input: Uint8Array): string;

/**
 * Validate one JavaScript representation of a canonical Stream sequence.
 *
 * JavaScript passes the decimal spelling of its `bigint`; Rust owns the
 * unsigned 64-bit range accepted by every Stream wire field.
 */
export function validateSequence(value: string): string;

/**
 * Validates the Rust Actors invoke request admission rules.
 */
export function validate_actors_invoke(actor_id: string, method: string): void;

/**
 * Validate the opaque commit identity used by Stream responses and requests.
 * The empty string means success; malformed identities use the canonical
 * invalid-argument boundary consumed by generated facades.
 */
export function validate_commit_id(input: Uint8Array): string;

/**
 * Validates the optional native TLS CA certificate before it reaches the
 * platform gRPC adapter.
 */
export function validate_remote_web_ca_certificate(certificate: string): void;

/**
 * Checks an HTTP content-length without first narrowing it through a JS number.
 */
export function validate_remote_web_content_length(content_length: string, maximum: bigint): void;

/**
 * Validates one bearer credential according to the shared Rust policy.
 */
export function validate_remote_web_credential(token: string): void;

/**
 * Validates the HTTPS or loopback-HTTP endpoint shared by Actors and Workers.
 */
export function validate_remote_web_endpoint(endpoint: string): void;

/**
 * Validates the HTTPS endpoint required by native gRPC transports.
 */
export function validate_remote_web_grpc_endpoint(endpoint: string): void;

/**
 * Validates the configured native gRPC message bound.
 */
export function validate_remote_web_message_limit(maximum: bigint): void;

/**
 * Advances a cumulative response byte count under the caller's configured bound.
 */
export function validate_remote_web_response_chunk(observed: bigint, chunk: bigint, maximum: bigint): bigint;

/**
 * Validates the configured cumulative response bound.
 */
export function validate_remote_web_response_limit(maximum: bigint): void;

/**
 * Validates the Rust Workers invoke-deployment path and request admission rules.
 */
export function validate_workers_invoke_deployment(alias: string, method: string): void;

/**
 * Validates the Rust Workers invoke-version path and request admission rules.
 */
export function validate_workers_invoke_version(version_sha256: Uint8Array, method: string): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_browserchangeset_free: (a: number, b: number) => void;
    readonly __wbg_browsercheckout_free: (a: number, b: number) => void;
    readonly __wbg_browserfs_free: (a: number, b: number) => void;
    readonly __wbg_browsergeneration_free: (a: number, b: number) => void;
    readonly __wbg_browsergitcompatrepository_free: (a: number, b: number) => void;
    readonly __wbg_browserjoinplan_free: (a: number, b: number) => void;
    readonly __wbg_browserresolvedfile_free: (a: number, b: number) => void;
    readonly __wbg_browserresolvedfiles_free: (a: number, b: number) => void;
    readonly __wbg_browserspeculation_free: (a: number, b: number) => void;
    readonly __wbg_browsertransaction_free: (a: number, b: number) => void;
    readonly __wbg_browservolume_free: (a: number, b: number) => void;
    readonly __wbg_browserworkspace_free: (a: number, b: number) => void;
    readonly __wbg_browserworkspacecontextregistry_free: (a: number, b: number) => void;
    readonly browserchangeset_changes: (a: number) => [number, number, number];
    readonly browserchangeset_compose: (a: number, b: number, c: number) => any;
    readonly browserchangeset_from: (a: number) => number;
    readonly browserchangeset_to: (a: number) => number;
    readonly browsercheckout_acquisitionWork: (a: number) => any;
    readonly browsercheckout_applyTransaction: (a: number, b: any) => any;
    readonly browsercheckout_checkpoint: (a: number) => any;
    readonly browsercheckout_cloneFileRange: (a: number, b: number, c: number, d: bigint, e: number, f: number, g: bigint, h: bigint) => any;
    readonly browsercheckout_cloneFileRangeById: (a: number, b: number, c: number, d: bigint, e: number, f: number, g: bigint, h: bigint) => any;
    readonly browsercheckout_commit: (a: number, b: number, c: number) => any;
    readonly browsercheckout_createDevice: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly browsercheckout_createDirectory: (a: number, b: number, c: number) => any;
    readonly browsercheckout_createFile: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_createReparsePoint: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_createSpecial: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_createSymbolicLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_discard: (a: number) => any;
    readonly browsercheckout_exportManifest: (a: number) => any;
    readonly browsercheckout_hardLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_listDirectory: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browsercheckout_listDirectoryRecords: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browsercheckout_listNamedAttributes: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => any;
    readonly browsercheckout_lookupBatchNoFollow: (a: number, b: any) => any;
    readonly browsercheckout_lookupNoFollow: (a: number, b: number, c: number) => any;
    readonly browsercheckout_mutateLive: (a: number, b: any, c: number, d: number, e: number, f: number) => any;
    readonly browsercheckout_planFileExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsercheckout_planFileExtentsById: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsercheckout_preallocateFile: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsercheckout_preallocateFileById: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsercheckout_prepareMerge: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_readFileRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsercheckout_readFileRangeById: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsercheckout_readFileRecordById: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readMetadata: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readMetadataById: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readNamedAttribute: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly browsercheckout_readReparsePoint: (a: number, b: number, c: number) => any;
    readonly browsercheckout_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browsercheckout_rebaseHead: (a: number, b: number) => any;
    readonly browsercheckout_refreshHead: (a: number) => any;
    readonly browsercheckout_refreshLive: (a: number) => any;
    readonly browsercheckout_remove: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsercheckout_removeNamedAttribute: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly browsercheckout_rename: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browsercheckout_resizeFile: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsercheckout_resizeFileById: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsercheckout_resolveFiles: (a: number, b: any) => any;
    readonly browsercheckout_resumeLive: (a: number, b: number, c: number, d: number, e: number) => any;
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
    readonly browserfs_exportGenerationBatch: (a: number, b: any, c: bigint, d: number, e: bigint) => any;
    readonly browserfs_exportObject: (a: number, b: number, c: number, d: bigint) => any;
    readonly browserfs_importGenerationBatch: (a: number, b: any, c: bigint, d: any, e: number) => any;
    readonly browserfs_importObject: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserfs_objectCacheStats: (a: number) => [number, number, number];
    readonly browserfs_openVolume: (a: number, b: number, c: number) => any;
    readonly browserfs_openWorkspace: (a: number, b: number, c: number) => any;
    readonly browserfs_restoreVolume: (a: number, b: any, c: number, d: number) => any;
    readonly browsergeneration_id: (a: number) => [number, number];
    readonly browsergeneration_listDirectory: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsergeneration_pin: (a: number, b: number, c: number) => any;
    readonly browsergeneration_planExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsergeneration_read: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsergeneration_readRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsergeneration_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browsergeneration_stat: (a: number, b: number, c: number) => any;
    readonly browsergeneration_workspaceId: (a: number) => [number, number];
    readonly browsergitcompatrepository_canonicalizeOutputJson: (a: number, b: number, c: number) => [number, number, number, number];
    readonly browsergitcompatrepository_canonicalizePendingTransitionJson: (a: number, b: number, c: number) => [number, number, number, number];
    readonly browsergitcompatrepository_executeArgvJson: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: bigint) => any;
    readonly browsergitcompatrepository_executeJson: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsergitcompatrepository_executePublicJson: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsergitcompatrepository_new: (a: number, b: number) => [number, number, number];
    readonly browserjoinplan_apply: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserjoinplan_commonAncestor: (a: number) => [number, number];
    readonly browserjoinplan_targetHead: (a: number) => [number, number];
    readonly browserresolvedfile_kind: (a: number) => [number, number];
    readonly browserresolvedfile_logicalBytes: (a: number) => bigint;
    readonly browserresolvedfile_metadataCanonicalBytes: (a: number) => [number, number, number, number];
    readonly browserresolvedfile_readRange: (a: number, b: bigint, c: bigint) => any;
    readonly browserresolvedfile_readSymbolicLink: (a: number) => any;
    readonly browserresolvedfiles_length: (a: number) => number;
    readonly browserresolvedfiles_take: (a: number, b: number) => [number, number, number];
    readonly browserresolvedfiles_work: (a: number) => any;
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
    readonly browsertransaction_commit: (a: number) => any;
    readonly browsertransaction_copy: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_createDirAll: (a: number, b: number, c: number) => any;
    readonly browsertransaction_createDirectory: (a: number, b: number, c: number) => any;
    readonly browsertransaction_createSymbolicLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_hardLink: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_preallocate: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsertransaction_rebase: (a: number, b: number) => any;
    readonly browsertransaction_remove: (a: number, b: number, c: number) => any;
    readonly browsertransaction_rename: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_resize: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsertransaction_write: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsertransaction_writeRange: (a: number, b: number, c: number, d: bigint, e: number, f: number) => any;
    readonly browsertransaction_zeroRange: (a: number, b: number, c: number, d: bigint, e: bigint, f: number, g: number) => any;
    readonly browservolume_acquisitionWork: (a: number) => any;
    readonly browservolume_checkout: (a: number, b: any) => any;
    readonly browservolume_diffGenerations: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browservolume_id: (a: number) => [number, number];
    readonly browserworkspace_beginTransaction: (a: number, b: number, c: number) => any;
    readonly browserworkspace_checkpoint: (a: number, b: number, c: number) => any;
    readonly browserworkspace_delete: (a: number, b: number, c: number) => any;
    readonly browserworkspace_diff: (a: number, b: number, c: number, d: number) => any;
    readonly browserworkspace_fork: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspace_forkAt: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browserworkspace_head: (a: number) => any;
    readonly browserworkspace_id: (a: number) => [number, number];
    readonly browserworkspace_joinInto: (a: number, b: number, c: any) => any;
    readonly browserworkspace_liveRebase: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browserworkspace_name: (a: number) => [number, number];
    readonly browserworkspace_pin: (a: number, b: number, c: number) => any;
    readonly browserworkspace_planExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browserworkspace_read: (a: number, b: number, c: number, d: bigint) => any;
    readonly browserworkspace_readRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browserworkspace_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browserworkspace_remove: (a: number, b: number, c: number) => any;
    readonly browserworkspace_stat: (a: number, b: number, c: number) => any;
    readonly browserworkspace_sync: (a: number) => any;
    readonly browserworkspace_write: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspacecontextregistry_adoptRoot: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserworkspacecontextregistry_discardSubtree: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
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
    readonly projectHostedSourceState: (a: number, b: number, c: number) => [number, number, number];
    readonly validateHostedAdvertisedLimits: (a: number, b: number) => [number, number];
    readonly validateHostedCredentialExpiry: (a: number, b: number, c: bigint) => [number, number];
    readonly validateHostedGenerationBounds: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateHostedGenerationContinuity: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateHostedGenerationIdentity: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number];
    readonly validateHostedPageBound: (a: number, b: number) => [number, number];
    readonly validateHostedResponseBytes: (a: number, b: number) => [number, number];
    readonly validateHostedSourceState: (a: number, b: number, c: number) => [number, number];
    readonly validateHostedTransactionBounds: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateRemoteWebCredential: (a: number, b: number) => [number, number];
    readonly validateRemoteWebEndpoint: (a: number, b: number) => [number, number];
    readonly validateRemoteWebGrpcEndpoint: (a: number, b: number) => [number, number];
    readonly __wbg_browserfilesystemclient_free: (a: number, b: number) => void;
    readonly __wbg_browserharnessclient_free: (a: number, b: number) => void;
    readonly browserfilesystemclient_cancel: (a: number, b: number, c: number) => any;
    readonly browserfilesystemclient_capabilities: (a: number) => [number, number, number];
    readonly browserfilesystemclient_connect: (a: number, b: number, c: number, d: number, e: bigint, f: bigint) => any;
    readonly browserfilesystemclient_export: (a: number, b: number, c: number) => any;
    readonly browserharnessclient_cancel: (a: number, b: number, c: number) => any;
    readonly browserharnessclient_capabilities: (a: number) => [number, number, number];
    readonly browserharnessclient_connect: (a: number, b: number, c: number, d: number) => any;
    readonly browserharnessclient_connectWithLimits: (a: number, b: number, c: number, d: number, e: bigint, f: bigint) => any;
    readonly browserharnessclient_observe: (a: number, b: number, c: number) => any;
    readonly browserharnessclient_replay: (a: number, b: number, c: number) => any;
    readonly browserharnessclient_submit: (a: number, b: number, c: number) => any;
    readonly validateRemoteWebFilesystemHandshake: (a: number, b: number) => [number, number, number, number];
    readonly validate_actors_invoke: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validate_remote_web_ca_certificate: (a: number, b: number) => [number, number];
    readonly validate_remote_web_content_length: (a: number, b: number, c: bigint) => [number, number];
    readonly validate_remote_web_credential: (a: number, b: number) => [number, number];
    readonly validate_remote_web_endpoint: (a: number, b: number) => [number, number];
    readonly validate_remote_web_grpc_endpoint: (a: number, b: number) => [number, number];
    readonly validate_remote_web_message_limit: (a: bigint) => [number, number];
    readonly validate_remote_web_response_chunk: (a: bigint, b: bigint, c: bigint) => [bigint, number, number];
    readonly validate_remote_web_response_limit: (a: bigint) => [number, number];
    readonly validate_workers_invoke_deployment: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validate_workers_invoke_version: (a: number, b: number, c: number, d: number) => [number, number];
    readonly __wbg_intounderlyingbytesource_free: (a: number, b: number) => void;
    readonly __wbg_intounderlyingsink_free: (a: number, b: number) => void;
    readonly __wbg_intounderlyingsource_free: (a: number, b: number) => void;
    readonly intounderlyingbytesource_autoAllocateChunkSize: (a: number) => number;
    readonly intounderlyingbytesource_cancel: (a: number) => void;
    readonly intounderlyingbytesource_pull: (a: number, b: any) => any;
    readonly intounderlyingbytesource_start: (a: number, b: any) => void;
    readonly intounderlyingbytesource_type: (a: number) => number;
    readonly intounderlyingsink_abort: (a: number, b: any) => any;
    readonly intounderlyingsink_close: (a: number) => any;
    readonly intounderlyingsink_write: (a: number, b: any) => any;
    readonly intounderlyingsource_cancel: (a: number) => void;
    readonly intounderlyingsource_pull: (a: number, b: any) => any;
    readonly __streamErrorCodeContract: (a: any) => any;
    readonly __wbg_httpfollowcursor_free: (a: number, b: number) => void;
    readonly __wbg_wasmfollow_free: (a: number, b: number) => void;
    readonly __wbg_wasmmemorystream_free: (a: number, b: number) => void;
    readonly consumeHttpResponseBytes: (a: bigint, b: bigint, c: bigint) => [bigint, number, number];
    readonly decodeHttpResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly encodeHttpRequest: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly httpfollowcursor_acceptRead: (a: number, b: number, c: number) => [number, number];
    readonly httpfollowcursor_acceptTail: (a: number, b: number, c: number) => [number, number];
    readonly httpfollowcursor_close: (a: number) => void;
    readonly httpfollowcursor_isClosed: (a: number) => number;
    readonly httpfollowcursor_new: (a: number, b: number) => [number, number, number];
    readonly httpfollowcursor_pollDelayMillis: (a: number) => number;
    readonly httpfollowcursor_readRequest: (a: number) => [number, number, number, number];
    readonly httpfollowcursor_shouldPoll: (a: number) => number;
    readonly httpfollowcursor_tailRequest: (a: number) => [number, number, number, number];
    readonly is_stream_error_code: (a: number, b: number) => number;
    readonly nextHttpFollowCursor: (a: number, b: number, c: bigint) => [bigint, number, number];
    readonly normalizeCommitRequest: (a: number, b: number) => [number, number, number, number];
    readonly projectGrpcReadResponse: (a: number, b: number, c: bigint) => [number, number, number];
    readonly projectMemoryResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly publicHttpErrorCode: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateAppendRequest: (a: number, b: number) => [number, number];
    readonly validateBearerToken: (a: number, b: number) => [number, number];
    readonly validateChildrenPageResponse: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateGrpcResponseIdentity: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number];
    readonly validateHttpEndpoint: (a: number, b: number) => [number, number];
    readonly validateHttpReadResponse: (a: number, b: number, c: bigint) => [number, number];
    readonly validateHttpResponse: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateIdempotencyKey: (a: number, b: number) => [number, number];
    readonly validatePath: (a: number, b: number) => [number, number];
    readonly validateRequest: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateSequence: (a: number, b: number) => [number, number];
    readonly validate_commit_id: (a: number, b: number) => [number, number];
    readonly wasmfollow_close: (a: number) => void;
    readonly wasmfollow_next: (a: number) => any;
    readonly wasmmemorystream_children: (a: number, b: number, c: number) => any;
    readonly wasmmemorystream_dispatch: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmmemorystream_new: () => number;
    readonly wasmmemorystream_open_follow: (a: number, b: number, c: number) => any;
    readonly wasmmemorystream_read: (a: number, b: number, c: number) => any;
    readonly defaultHttpResponseBytes: () => bigint;
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_IdbVersionChangeEvent__IdbVersionChangeEvent__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsValue___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_Event__Event______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true__1_: (a: number, b: number) => void;
    readonly wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true_: (a: number, b: number) => void;
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
