/* tslint:disable */
/* eslint-disable */

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
    changes(): any;
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
    applyTransaction(operations: any): Promise<any>;
    /**
     * Builds an immutable candidate generation without publishing authority.
     *
     * # Errors
     *
     * Returns a JavaScript error for invalid checkout state, corruption,
     * cancellation, storage failure, or bounded-work exhaustion.
     */
    checkpoint(): Promise<any>;
    /**
     * Clones one logical range by immutable extent reference.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/ranges,
     * cancellation, storage, or bounded work.
     */
    cloneFileRange(source: string, source_offset: bigint, destination: string, destination_offset: bigint, length: bigint): Promise<any>;
    /**
     * Clones one logical range between stable file identities.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identities/ranges, absence,
     * invalid kinds, storage, cancellation, or bounded work.
     */
    cloneFileRangeById(source_file_id: Uint8Array, source_offset: bigint, destination_file_id: Uint8Array, destination_offset: bigint, length: bigint): Promise<any>;
    /**
     * Checkpoints and conditionally publishes this private overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operation identity, clean
     * or read-only checkout, closure failure, cancellation, or bounded work.
     */
    commit(operation_id: Uint8Array): Promise<any>;
    /**
     * Creates an exact POSIX character or block device identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/kinds, unsupported
     * profile semantics, storage, cancellation, or bounded work.
     */
    createDevice(path: string, kind: string, major: number, minor: number): Promise<any>;
    /**
     * Creates one empty directory in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, allocation, or bounded work.
     */
    createDirectory(path: string): Promise<any>;
    /**
     * Creates one regular file in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, allocation, or bounded work.
     */
    createFile(path: string, bytes: Uint8Array): Promise<any>;
    /**
     * Creates an opaque exact Windows reparse-point payload.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, unsupported profile
     * semantics, excessive payload, storage, cancellation, or bounded work.
     */
    createReparsePoint(path: string, payload: Uint8Array): Promise<any>;
    /**
     * Creates an exact empty special namespace entry.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/kinds, unsupported
     * profile semantics, storage, cancellation, or bounded work.
     */
    createSpecial(path: string, kind: string): Promise<any>;
    /**
     * Creates one symbolic link with exact opaque target bytes.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * excessive targets, cancellation, storage, or bounded work.
     */
    createSymbolicLink(path: string, target: Uint8Array): Promise<any>;
    /**
     * Discards the private overlay and returns to its immutable base.
     *
     * # Errors
     *
     * Returns a JavaScript error for cancellation, corruption, storage, or work bounds.
     */
    discard(): Promise<any>;
    /**
     * Builds a deterministic complete manifest for resumable transfer.
     *
     * # Errors
     *
     * Returns a JavaScript error for checkpoint, closure, authentication,
     * cancellation, storage, serialization, or bounded work.
     */
    exportManifest(): Promise<any>;
    /**
     * Creates one hard link within the volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, invalid kinds,
     * conflicts, cancellation, storage, or bounded work.
     */
    hardLink(source: string, destination: string): Promise<any>;
    /**
     * Returns one bounded ordered directory page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/cursors,
     * non-directories, corruption, cancellation, or bounded work.
     */
    listDirectory(path: string, after: string | null | undefined, maximum_entries: number): Promise<any>;
    /**
     * Returns one bounded directory page with records and metadata fetched in batches.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/cursors,
     * non-directories, corruption, cancellation, or bounded work.
     */
    listDirectoryRecords(path: string, after: string | null | undefined, maximum_entries: number): Promise<any>;
    /**
     * Returns one bounded ordered named-attribute page.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, cursor, path, storage, cancellation, or work failure.
     */
    listNamedAttributes(path: string, after_class: string | null | undefined, after_name: Uint8Array | null | undefined, maximum_entries: number): Promise<any>;
    /**
     * Resolves a bounded path batch with shared authenticated frontiers.
     *
     * # Errors
     *
     * Returns a JavaScript error for non-array/excessive/malformed paths,
     * storage, cancellation, authentication, or bounded-work failure.
     */
    lookupBatchNoFollow(paths: any): Promise<any>;
    /**
     * Resolves one canonical absolute path without following links.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths or authenticated
     * storage, cancellation, and bounded-work failures.
     */
    lookupNoFollow(path: string): Promise<any>;
    /**
     * Applies and publishes one direct-live transaction with bounded safe retries.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operations or identity,
     * wrong checkout mode, unresolved work, cancellation, storage, rebase,
     * or bounded-work failure.
     */
    mutateLive(operations: any, operation_id: Uint8Array, maximum_attempts: number, maximum_conflicts: number): Promise<any>;
    /**
     * Plans one bounded sparse range without reading file content blobs.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed path/range, invalid bounds,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    planFileExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<any>;
    /**
     * Plans one bounded sparse range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, invalid bounds,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    planFileExtentsById(file_id: Uint8Array, offset: bigint, length: bigint, maximum_spans: number): Promise<any>;
    /**
     * Allocates sparse holes without replacing existing content.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed values, unsupported
     * keep-size physical allocation, cancellation, storage, or bounded work.
     */
    preallocateFile(path: string, offset: bigint, length: bigint, keep_size: boolean): Promise<any>;
    /**
     * Allocates sparse holes by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * unsupported allocation, storage, cancellation, or bounded work.
     */
    preallocateFileById(file_id: Uint8Array, offset: bigint, length: bigint, keep_size: boolean): Promise<any>;
    /**
     * Prepares a bounded two-parent merge against the current authority head.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, invalid checkout
     * state, non-head parent, corruption, cancellation, or bounded work.
     */
    prepareMerge(theirs: Uint8Array, maximum_changes: number, maximum_conflicts: number): Promise<any>;
    /**
     * Reads one exact logical regular-file range.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, invalid ranges,
     * non-regular files, corruption, cancellation, or bounded work.
     */
    readFileRange(path: string, offset: bigint, length: bigint): Promise<any>;
    /**
     * Reads one exact logical range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    readFileRangeById(file_id: Uint8Array, offset: bigint, length: bigint): Promise<any>;
    /**
     * Reads one complete candidate file record by stable identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, authenticated
     * absence, storage, cancellation, or bounded-work failure.
     */
    readFileRecordById(file_id: Uint8Array): Promise<any>;
    /**
     * Reads complete canonical metadata bytes for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for path, storage, codec, cancellation, or work failure.
     */
    readMetadata(path: string): Promise<any>;
    /**
     * Reads complete canonical metadata by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, absence, storage,
     * authentication, cancellation, encoding, or bounded work.
     */
    readMetadataById(file_id: Uint8Array): Promise<any>;
    /**
     * Reads one exact named attribute value.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, path, storage, cancellation, or work failure.
     */
    readNamedAttribute(path: string, attribute_class: string, name: Uint8Array): Promise<any>;
    /**
     * Reads one opaque Windows reparse-point payload without interpreting it.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, wrong file kind,
     * storage, cancellation, authentication, or bounded work.
     */
    readReparsePoint(path: string): Promise<any>;
    /**
     * Reads one symbolic link's exact opaque target bytes without following it.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, non-links,
     * corruption, cancellation, storage, or bounded work.
     */
    readSymbolicLink(path: string): Promise<any>;
    /**
     * Safely advances to head and sparsely replays private mutations.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported consistency, invalid
     * bounds, corruption, cancellation, storage, replay, or bounded work.
     */
    rebaseHead(maximum_conflicts: number): Promise<any>;
    /**
     * Explicitly advances a clean manual checkout to the authority head.
     *
     * # Errors
     *
     * Returns a JavaScript error for dirty state, storage, cancellation,
     * authentication, or bounded-work failure.
     */
    refreshHead(): Promise<any>;
    /**
     * Explicitly performs observation-safe synchronization for a live checkout.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported mode, conflicts, storage,
     * cancellation, authentication, or bounded-work failure.
     */
    refreshLive(): Promise<any>;
    /**
     * Removes one namespace binding.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/identities,
     * conflicts, cancellation, storage, or bounded work.
     */
    remove(path: string, expected_file_id?: Uint8Array | null): Promise<any>;
    /**
     * Removes one exact named attribute.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, mutation, cancellation, or work failure.
     */
    removeNamedAttribute(path: string, attribute_class: string, name: Uint8Array): Promise<any>;
    /**
     * Atomically renames one binding within the volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, or bounded work.
     */
    rename(source: string, destination: string, replace: boolean): Promise<any>;
    /**
     * Changes one regular file's logical length.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed values, invalid kinds,
     * cancellation, storage, or bounded work.
     */
    resizeFile(path: string, logical_bytes: bigint): Promise<any>;
    /**
     * Changes logical length by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/length, absence,
     * invalid kind, storage, cancellation, or bounded work.
     */
    resizeFileById(file_id: Uint8Array, logical_bytes: bigint): Promise<any>;
    /**
     * Resumes an unresolved direct-live transaction with the same operation identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, wrong checkout
     * mode, absent staged work, cancellation, storage, rebase, or bounds.
     */
    resumeLive(operation_id: Uint8Array, maximum_attempts: number, maximum_conflicts: number): Promise<any>;
    /**
     * Finds the next sparse data or hole boundary without reading file bodies.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed path/offset/target,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    seekFileExtent(path: string, offset: bigint, target: string): Promise<any>;
    /**
     * Finds the next sparse boundary by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/offset/target,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    seekFileExtentById(file_id: Uint8Array, offset: bigint, target: string): Promise<any>;
    /**
     * Atomically replaces metadata and optional logical size for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed metadata/path, non-regular
     * resize, storage, cancellation, mutation, or bounded-work failure.
     */
    setAttributes(path: string, canonical_bytes: Uint8Array, logical_bytes?: bigint | null): Promise<any>;
    /**
     * Atomically replaces metadata and optional logical size by file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/metadata,
     * non-regular resize, storage, cancellation, or bounded work.
     */
    setAttributesById(file_id: Uint8Array, canonical_bytes: Uint8Array, logical_bytes?: bigint | null): Promise<any>;
    /**
     * Atomically replaces complete canonical metadata for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for path, codec, mutation, cancellation, or work failure.
     */
    setMetadata(path: string, canonical_bytes: Uint8Array): Promise<any>;
    /**
     * Replaces complete canonical metadata by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/metadata, absence,
     * storage, cancellation, or bounded work.
     */
    setMetadataById(file_id: Uint8Array, canonical_bytes: Uint8Array): Promise<any>;
    /**
     * Returns one complete no-follow file record and canonical metadata.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, storage,
     * authentication, cancellation, encoding, or bounded-work failure.
     */
    statNoFollow(path: string): Promise<any>;
    /**
     * Replaces one logical file range in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/offsets,
     * non-regular files, cancellation, storage, or bounded work.
     */
    writeFile(path: string, offset: bigint, bytes: Uint8Array): Promise<any>;
    /**
     * Replaces one logical range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/offset,
     * non-regular kind, storage, cancellation, or bounded work.
     */
    writeFileById(file_id: Uint8Array, offset: bigint, bytes: Uint8Array): Promise<any>;
    /**
     * Inserts or replaces one exact named attribute.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, mode, mutation, cancellation, or work failure.
     */
    writeNamedAttribute(path: string, attribute_class: string, name: Uint8Array, bytes: Uint8Array, mode: string): Promise<any>;
    /**
     * Punches a hole or records physically allocated zeros.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed ranges/kinds,
     * cancellation, storage, or bounded work.
     */
    zeroFileRange(path: string, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<any>;
    /**
     * Punches a hole or records allocated zero by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * invalid kind, storage, cancellation, or bounded work.
     */
    zeroFileRangeById(file_id: Uint8Array, offset: bigint, length: bigint, allocated: boolean, extend: boolean): Promise<any>;
    /**
     * Returns exact bounded work used to acquire this checkout handle.
     *
     * # Errors
     *
     * Returns a JavaScript error when the bounded work receipt cannot be
     * serialized for JavaScript.
     */
    readonly acquisitionWork: any;
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
     * Releases browser handles. Durable state remains in `IndexedDB`.
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
    exportGenerationBatch(manifest: any, cursor: bigint, maximum_objects: number, maximum_object_bytes: bigint): Promise<any>;
    /**
     * Exports one exact authenticated immutable object for resumable transfer.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, absence,
     * corruption, cancellation, storage, or bounded work.
     */
    exportObject(object_id: Uint8Array, maximum_bytes: bigint): Promise<any>;
    /**
     * Idempotently imports one manifest-aligned immutable-object page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifests, cursor/body
     * bounds, cancellation, storage, or bounded-work failures.
     */
    importGenerationBatch(manifest: any, cursor: bigint, objects: any, maximum_objects: number): Promise<any>;
    /**
     * Idempotently imports one immutable object under its authenticated identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, digest mismatch,
     * cancellation, storage, or bounded work.
     */
    importObject(object_id: Uint8Array, bytes: Uint8Array): Promise<any>;
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
    restoreVolume(manifest: any): Promise<BrowserVolume>;
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
    listDirectory(path: string, after: any | null | undefined, maximum_entries: number): Promise<any>;
    /**
     * Retains this exact generation under one opaque identity.
     */
    pin(identity: string): Promise<BrowserGeneration>;
    planExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<any>;
    /**
     * Reads one complete file from this exact immutable state.
     */
    read(path: string, maximum_bytes: bigint): Promise<Uint8Array>;
    readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
    readSymbolicLink(path: string): Promise<Uint8Array>;
    stat(path: string): Promise<any>;
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
 * One immutable, side-effect-free workspace join plan.
 */
export class BrowserJoinPlan {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies this immutable plan through one exact target-head CAS.
     */
    apply(if_target: Uint8Array, idempotency_key?: Uint8Array | null): Promise<any>;
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
    executeResidency(operation_id: Uint8Array): Promise<any>;
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
    metrics(): any;
    /**
     * Records foreground demand and admits one authenticated successor.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed input or a failed bounded transition.
     */
    observe(observation: any): any;
    /**
     * Plans one bounded promotion from exact caller-observed location facts.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed facts, unsupported tiers,
     * inactive residency, or a failed bounded transition.
     */
    planPromotion(request: any): any;
    /**
     * Atomically preempts both engines before recording foreground bytes.
     *
     * # Errors
     *
     * Returns a JavaScript error if exact bounded accounting fails.
     */
    preemptForForeground(bytes: bigint): any;
    /**
     * Atomically fences both engines onto a new immutable generation.
     *
     * # Errors
     *
     * Returns a JavaScript error for a malformed identity or failed transition.
     */
    replaceGeneration(generation_id: Uint8Array): any;
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
    diffGenerations(before: Uint8Array, after: Uint8Array, maximum_changes: number): Promise<any>;
    /**
     * Returns exact bounded work used to acquire this volume handle.
     *
     * # Errors
     *
     * Returns a JavaScript error when the bounded work receipt cannot be
     * serialized for JavaScript.
     */
    readonly acquisitionWork: any;
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
    fork(destination: string): Promise<BrowserWorkspace>;
    /**
     * Creates an independent workspace at one caller-selected exact generation.
     */
    forkAt(destination: string, generation: BrowserGeneration): Promise<BrowserWorkspace>;
    /**
     * Current immutable generation identity.
     */
    head(): Promise<Uint8Array>;
    /**
     * Builds one immutable side-effect-free plan for joining this workspace into a target.
     */
    joinInto(target: BrowserWorkspace, options: any): Promise<BrowserJoinPlan>;
    listDirectory(path: string, after: any | null | undefined, maximum_entries: number): Promise<any>;
    /**
     * Advances this fork onto its source workspace's current generation.
     */
    liveRebase(idempotency_key: Uint8Array | null | undefined, maximum_generations: number, maximum_changes: number, maximum_conflicts: number): Promise<any>;
    /**
     * Retains the current generation under one opaque stable identity.
     */
    pin(identity: string): Promise<BrowserGeneration>;
    planExtents(path: string, offset: bigint, length: bigint, maximum_spans: number): Promise<any>;
    /**
     * Reads one complete regular file under a byte bound.
     */
    read(path: string, maximum_bytes: bigint): Promise<Uint8Array>;
    readRange(path: string, offset: bigint, length: bigint): Promise<Uint8Array>;
    readSymbolicLink(path: string): Promise<Uint8Array>;
    /**
     * Removes one existing path atomically.
     */
    remove(path: string): Promise<any>;
    stat(path: string): Promise<any>;
    /**
     * Synchronizes prior operations and returns the exact immutable head.
     */
    sync(): Promise<BrowserGeneration>;
    /**
     * Atomically creates or replaces one complete file.
     */
    write(path: string, bytes: Uint8Array): Promise<any>;
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
 * Opens transactional `IndexedDB` correctness storage with optional OPFS acceleration.
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

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_browserchangeset_free: (a: number, b: number) => void;
    readonly __wbg_browsercheckout_free: (a: number, b: number) => void;
    readonly __wbg_browserfs_free: (a: number, b: number) => void;
    readonly __wbg_browsergeneration_free: (a: number, b: number) => void;
    readonly __wbg_browserjoinplan_free: (a: number, b: number) => void;
    readonly __wbg_browserspeculation_free: (a: number, b: number) => void;
    readonly __wbg_browsertransaction_free: (a: number, b: number) => void;
    readonly __wbg_browservolume_free: (a: number, b: number) => void;
    readonly __wbg_browserworkspace_free: (a: number, b: number) => void;
    readonly browserchangeset_changes: (a: number) => [number, number, number];
    readonly browserchangeset_compose: (a: number, b: number, c: number) => any;
    readonly browserchangeset_from: (a: number) => number;
    readonly browserchangeset_to: (a: number) => number;
    readonly browsercheckout_acquisitionWork: (a: number) => [number, number, number];
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
    readonly browserfs_restoreVolume: (a: number, b: any) => any;
    readonly browsergeneration_id: (a: number) => [number, number];
    readonly browsergeneration_listDirectory: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browsergeneration_pin: (a: number, b: number, c: number) => any;
    readonly browsergeneration_planExtents: (a: number, b: number, c: number, d: bigint, e: bigint, f: number) => any;
    readonly browsergeneration_read: (a: number, b: number, c: number, d: bigint) => any;
    readonly browsergeneration_readRange: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly browsergeneration_readSymbolicLink: (a: number, b: number, c: number) => any;
    readonly browsergeneration_stat: (a: number, b: number, c: number) => any;
    readonly browsergeneration_workspaceId: (a: number) => [number, number];
    readonly browserjoinplan_apply: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly browserjoinplan_commonAncestor: (a: number) => [number, number];
    readonly browserjoinplan_targetHead: (a: number) => [number, number];
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
    readonly browservolume_acquisitionWork: (a: number) => [number, number, number];
    readonly browservolume_checkout: (a: number, b: any) => any;
    readonly browservolume_diffGenerations: (a: number, b: number, c: number, d: number, e: number, f: number) => any;
    readonly browservolume_id: (a: number) => [number, number];
    readonly browserworkspace_beginTransaction: (a: number, b: number, c: number) => any;
    readonly browserworkspace_checkpoint: (a: number, b: number, c: number) => any;
    readonly browserworkspace_delete: (a: number, b: number, c: number) => any;
    readonly browserworkspace_diff: (a: number, b: number, c: number, d: number) => any;
    readonly browserworkspace_fork: (a: number, b: number, c: number) => any;
    readonly browserworkspace_forkAt: (a: number, b: number, c: number, d: number) => any;
    readonly browserworkspace_head: (a: number) => any;
    readonly browserworkspace_id: (a: number) => [number, number];
    readonly browserworkspace_joinInto: (a: number, b: number, c: any) => any;
    readonly browserworkspace_listDirectory: (a: number, b: number, c: number, d: number, e: number) => any;
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
    readonly openBrowserFs: (a: any) => any;
    readonly openMemoryFs: (a: any) => [number, number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h99011fa81a70a26c: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h6351411ee5760e0b: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h0732115cd49c6da8: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h826571d35b52986c: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h7721336414c3989a: (a: number, b: number) => void;
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
