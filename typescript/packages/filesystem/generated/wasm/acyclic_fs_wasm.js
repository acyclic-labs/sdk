/* @ts-self-types="./acyclic_fs_wasm.d.ts" */

/**
 * One immutable semantic delta between exact generations.
 */
export class BrowserChangeSet {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserChangeSet.prototype);
        obj.__wbg_ptr = ptr;
        BrowserChangeSetFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserChangeSetFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserchangeset_free(ptr, 0);
    }
    /**
     * Stable path-independent records and namespace binding changes.
     * @returns {BrowserGenerationDiffResult}
     */
    changes() {
        const ret = wasm.browserchangeset_changes(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Composes contiguous immutable deltas by diffing their outer endpoints.
     * @param {BrowserChangeSet} next
     * @param {number} maximum_changes
     * @returns {Promise<BrowserChangeSet>}
     */
    compose(next, maximum_changes) {
        _assertClass(next, BrowserChangeSet);
        const ret = wasm.browserchangeset_compose(this.__wbg_ptr, next.__wbg_ptr, maximum_changes);
        return ret;
    }
    /**
     * Exact immutable base endpoint.
     * @returns {BrowserGeneration}
     */
    get from() {
        const ret = wasm.browserchangeset_from(this.__wbg_ptr);
        return BrowserGeneration.__wrap(ret);
    }
    /**
     * Exact immutable resulting endpoint.
     * @returns {BrowserGeneration}
     */
    get to() {
        const ret = wasm.browserchangeset_to(this.__wbg_ptr);
        return BrowserGeneration.__wrap(ret);
    }
}
if (Symbol.dispose) BrowserChangeSet.prototype[Symbol.dispose] = BrowserChangeSet.prototype.free;

/**
 * One immutable-generation checkout with optional private COW mutations.
 */
export class BrowserCheckout {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserCheckout.prototype);
        obj.__wbg_ptr = ptr;
        BrowserCheckoutFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserCheckoutFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browsercheckout_free(ptr, 0);
    }
    /**
     * Returns exact bounded work used to acquire this checkout handle.
     * @returns {BrowserWorkCounters}
     */
    get acquisitionWork() {
        const ret = wasm.browsercheckout_acquisitionWork(this.__wbg_ptr);
        return ret;
    }
    /**
     * Applies one ordered sparse mutation batch atomically within this volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operations, rejected semantics,
     * cancellation, storage, or bounded-work failure.
     * @param {TransactionOperation[]} operations
     * @returns {Promise<BrowserTransactionResult>}
     */
    applyTransaction(operations) {
        const ret = wasm.browsercheckout_applyTransaction(this.__wbg_ptr, operations);
        return ret;
    }
    /**
     * Builds an immutable candidate generation without publishing authority.
     *
     * # Errors
     *
     * Returns a JavaScript error for invalid checkout state, corruption,
     * cancellation, storage failure, or bounded-work exhaustion.
     * @returns {Promise<BrowserCheckpointResult>}
     */
    checkpoint() {
        const ret = wasm.browsercheckout_checkpoint(this.__wbg_ptr);
        return ret;
    }
    /**
     * Clones one logical range by immutable extent reference.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/ranges,
     * cancellation, storage, or bounded work.
     * @param {string} source
     * @param {bigint} source_offset
     * @param {string} destination
     * @param {bigint} destination_offset
     * @param {bigint} length
     * @returns {Promise<BrowserMutationResult>}
     */
    cloneFileRange(source, source_offset, destination, destination_offset, length) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_cloneFileRange(this.__wbg_ptr, ptr0, len0, source_offset, ptr1, len1, destination_offset, length);
        return ret;
    }
    /**
     * Clones one logical range between stable file identities.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identities/ranges, absence,
     * invalid kinds, storage, cancellation, or bounded work.
     * @param {Uint8Array} source_file_id
     * @param {bigint} source_offset
     * @param {Uint8Array} destination_file_id
     * @param {bigint} destination_offset
     * @param {bigint} length
     * @returns {Promise<BrowserMutationResult>}
     */
    cloneFileRangeById(source_file_id, source_offset, destination_file_id, destination_offset, length) {
        const ptr0 = passArray8ToWasm0(source_file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(destination_file_id, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_cloneFileRangeById(this.__wbg_ptr, ptr0, len0, source_offset, ptr1, len1, destination_offset, length);
        return ret;
    }
    /**
     * Checkpoints and conditionally publishes this private overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operation identity, clean
     * or read-only checkout, closure failure, cancellation, or bounded work.
     * @param {Uint8Array} operation_id
     * @returns {Promise<BrowserCommitResult>}
     */
    commit(operation_id) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_commit(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Creates an exact POSIX character or block device identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/kinds, unsupported
     * profile semantics, storage, cancellation, or bounded work.
     * @param {string} path
     * @param {string} kind
     * @param {number} major
     * @param {number} minor
     * @returns {Promise<BrowserMutationResult>}
     */
    createDevice(path, kind, major, minor) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(kind, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_createDevice(this.__wbg_ptr, ptr0, len0, ptr1, len1, major, minor);
        return ret;
    }
    /**
     * Creates one empty directory in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, allocation, or bounded work.
     * @param {string} path
     * @returns {Promise<BrowserMutationResult>}
     */
    createDirectory(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_createDirectory(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Creates one regular file in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, allocation, or bounded work.
     * @param {string} path
     * @param {Uint8Array} bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    createFile(path, bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_createFile(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates an opaque exact Windows reparse-point payload.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, unsupported profile
     * semantics, excessive payload, storage, cancellation, or bounded work.
     * @param {string} path
     * @param {Uint8Array} payload
     * @returns {Promise<BrowserMutationResult>}
     */
    createReparsePoint(path, payload) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(payload, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_createReparsePoint(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates an exact empty special namespace entry.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/kinds, unsupported
     * profile semantics, storage, cancellation, or bounded work.
     * @param {string} path
     * @param {string} kind
     * @returns {Promise<BrowserMutationResult>}
     */
    createSpecial(path, kind) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(kind, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_createSpecial(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates one symbolic link with exact opaque target bytes.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * excessive targets, cancellation, storage, or bounded work.
     * @param {string} path
     * @param {Uint8Array} target
     * @returns {Promise<BrowserMutationResult>}
     */
    createSymbolicLink(path, target) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(target, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_createSymbolicLink(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Discards the private overlay and returns to its immutable base.
     *
     * # Errors
     *
     * Returns a JavaScript error for cancellation, corruption, storage, or work bounds.
     * @returns {Promise<BrowserMutationResult>}
     */
    discard() {
        const ret = wasm.browsercheckout_discard(this.__wbg_ptr);
        return ret;
    }
    /**
     * Builds a deterministic complete manifest for resumable transfer.
     *
     * # Errors
     *
     * Returns a JavaScript error for checkpoint, closure, authentication,
     * cancellation, storage, serialization, or bounded work.
     * @returns {Promise<BrowserExportManifestResult>}
     */
    exportManifest() {
        const ret = wasm.browsercheckout_exportManifest(this.__wbg_ptr);
        return ret;
    }
    /**
     * Creates one hard link within the volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, invalid kinds,
     * conflicts, cancellation, storage, or bounded work.
     * @param {string} source
     * @param {string} destination
     * @returns {Promise<BrowserMutationResult>}
     */
    hardLink(source, destination) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_hardLink(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Returns one bounded ordered directory page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/cursors,
     * non-directories, corruption, cancellation, or bounded work.
     * @param {string} path
     * @param {string | null | undefined} after
     * @param {number} maximum_entries
     * @returns {Promise<BrowserDirectoryPageResult>}
     */
    listDirectory(path, after, maximum_entries) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(after) ? 0 : passStringToWasm0(after, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_listDirectory(this.__wbg_ptr, ptr0, len0, ptr1, len1, maximum_entries);
        return ret;
    }
    /**
     * Returns one bounded directory page with records and metadata fetched in batches.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/cursors,
     * non-directories, corruption, cancellation, or bounded work.
     * @param {string} path
     * @param {string | null | undefined} after
     * @param {number} maximum_entries
     * @returns {Promise<BrowserDirectoryRecordPageResult>}
     */
    listDirectoryRecords(path, after, maximum_entries) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(after) ? 0 : passStringToWasm0(after, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_listDirectoryRecords(this.__wbg_ptr, ptr0, len0, ptr1, len1, maximum_entries);
        return ret;
    }
    /**
     * Returns one bounded ordered named-attribute page.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, cursor, path, storage, cancellation, or work failure.
     * @param {string} path
     * @param {string | null | undefined} after_class
     * @param {Uint8Array | null | undefined} after_name
     * @param {number} maximum_entries
     * @returns {Promise<BrowserNamedAttributePageResult>}
     */
    listNamedAttributes(path, after_class, after_name, maximum_entries) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(after_class) ? 0 : passStringToWasm0(after_class, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(after_name) ? 0 : passArray8ToWasm0(after_name, wasm.__wbindgen_malloc);
        var len2 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_listNamedAttributes(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, maximum_entries);
        return ret;
    }
    /**
     * Resolves a bounded path batch with shared authenticated frontiers.
     *
     * # Errors
     *
     * Returns a JavaScript error for non-array/excessive/malformed paths,
     * storage, cancellation, authentication, or bounded-work failure.
     * @param {BrowserPathBatch} paths
     * @returns {Promise<BrowserBatchLookupResult>}
     */
    lookupBatchNoFollow(paths) {
        const ret = wasm.browsercheckout_lookupBatchNoFollow(this.__wbg_ptr, paths);
        return ret;
    }
    /**
     * Resolves one canonical absolute path without following links.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths or authenticated
     * storage, cancellation, and bounded-work failures.
     * @param {string} path
     * @returns {Promise<BrowserLookupResult>}
     */
    lookupNoFollow(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_lookupNoFollow(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Applies and publishes one direct-live transaction with bounded safe retries.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed operations or identity,
     * wrong checkout mode, unresolved work, cancellation, storage, rebase,
     * or bounded-work failure.
     * @param {TransactionOperation[]} operations
     * @param {Uint8Array} operation_id
     * @param {number} maximum_attempts
     * @param {number} maximum_conflicts
     * @returns {Promise<BrowserLiveTransactionResult>}
     */
    mutateLive(operations, operation_id, maximum_attempts, maximum_conflicts) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_mutateLive(this.__wbg_ptr, operations, ptr0, len0, maximum_attempts, maximum_conflicts);
        return ret;
    }
    /**
     * Plans one bounded sparse range without reading file content blobs.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed path/range, invalid bounds,
     * non-regular kind, storage, cancellation, or bounded work.
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {number} maximum_spans
     * @returns {Promise<BrowserExtentPlanResult>}
     */
    planFileExtents(path, offset, length, maximum_spans) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_planFileExtents(this.__wbg_ptr, ptr0, len0, offset, length, maximum_spans);
        return ret;
    }
    /**
     * Plans one bounded sparse range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, invalid bounds,
     * non-regular kind, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} offset
     * @param {bigint} length
     * @param {number} maximum_spans
     * @returns {Promise<BrowserExtentPlanResult>}
     */
    planFileExtentsById(file_id, offset, length, maximum_spans) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_planFileExtentsById(this.__wbg_ptr, ptr0, len0, offset, length, maximum_spans);
        return ret;
    }
    /**
     * Allocates sparse holes without replacing existing content.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed values, unsupported
     * keep-size physical allocation, cancellation, storage, or bounded work.
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {boolean} keep_size
     * @returns {Promise<BrowserMutationResult>}
     */
    preallocateFile(path, offset, length, keep_size) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_preallocateFile(this.__wbg_ptr, ptr0, len0, offset, length, keep_size);
        return ret;
    }
    /**
     * Allocates sparse holes by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * unsupported allocation, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} offset
     * @param {bigint} length
     * @param {boolean} keep_size
     * @returns {Promise<BrowserMutationResult>}
     */
    preallocateFileById(file_id, offset, length, keep_size) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_preallocateFileById(this.__wbg_ptr, ptr0, len0, offset, length, keep_size);
        return ret;
    }
    /**
     * Prepares a bounded two-parent merge against the current authority head.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, invalid checkout
     * state, non-head parent, corruption, cancellation, or bounded work.
     * @param {Uint8Array} theirs
     * @param {number} maximum_changes
     * @param {number} maximum_conflicts
     * @returns {Promise<BrowserMergePreparationResult>}
     */
    prepareMerge(theirs, maximum_changes, maximum_conflicts) {
        const ptr0 = passArray8ToWasm0(theirs, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_prepareMerge(this.__wbg_ptr, ptr0, len0, maximum_changes, maximum_conflicts);
        return ret;
    }
    /**
     * Reads one exact logical regular-file range.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, invalid ranges,
     * non-regular files, corruption, cancellation, or bounded work.
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @returns {Promise<BrowserFileReadResult>}
     */
    readFileRange(path, offset, length) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readFileRange(this.__wbg_ptr, ptr0, len0, offset, length);
        return ret;
    }
    /**
     * Reads one exact logical range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * non-regular kind, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} offset
     * @param {bigint} length
     * @returns {Promise<BrowserFileReadResult>}
     */
    readFileRangeById(file_id, offset, length) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readFileRangeById(this.__wbg_ptr, ptr0, len0, offset, length);
        return ret;
    }
    /**
     * Reads one complete candidate file record by stable identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, authenticated
     * absence, storage, cancellation, or bounded-work failure.
     * @param {Uint8Array} file_id
     * @returns {Promise<BrowserFileRecordReadResult>}
     */
    readFileRecordById(file_id) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readFileRecordById(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Reads complete canonical metadata bytes for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for path, storage, codec, cancellation, or work failure.
     * @param {string} path
     * @returns {Promise<BrowserMetadataResult>}
     */
    readMetadata(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readMetadata(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Reads complete canonical metadata by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, absence, storage,
     * authentication, cancellation, encoding, or bounded work.
     * @param {Uint8Array} file_id
     * @returns {Promise<BrowserMetadataResult>}
     */
    readMetadataById(file_id) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readMetadataById(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Reads one exact named attribute value.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, path, storage, cancellation, or work failure.
     * @param {string} path
     * @param {string} attribute_class
     * @param {Uint8Array} name
     * @returns {Promise<BrowserNamedAttributeResult>}
     */
    readNamedAttribute(path, attribute_class, name) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(attribute_class, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passArray8ToWasm0(name, wasm.__wbindgen_malloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readNamedAttribute(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
        return ret;
    }
    /**
     * Reads one opaque Windows reparse-point payload without interpreting it.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, wrong file kind,
     * storage, cancellation, authentication, or bounded work.
     * @param {string} path
     * @returns {Promise<BrowserFileReadResult>}
     */
    readReparsePoint(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readReparsePoint(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Reads one symbolic link's exact opaque target bytes without following it.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, non-links,
     * corruption, cancellation, storage, or bounded work.
     * @param {string} path
     * @returns {Promise<BrowserFileReadResult>}
     */
    readSymbolicLink(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_readSymbolicLink(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Safely advances to head and sparsely replays private mutations.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported consistency, invalid
     * bounds, corruption, cancellation, storage, replay, or bounded work.
     * @param {number} maximum_conflicts
     * @returns {Promise<BrowserRebaseResult>}
     */
    rebaseHead(maximum_conflicts) {
        const ret = wasm.browsercheckout_rebaseHead(this.__wbg_ptr, maximum_conflicts);
        return ret;
    }
    /**
     * Explicitly advances a clean manual checkout to the authority head.
     *
     * # Errors
     *
     * Returns a JavaScript error for dirty state, storage, cancellation,
     * authentication, or bounded-work failure.
     * @returns {Promise<BrowserCheckpointResult>}
     */
    refreshHead() {
        const ret = wasm.browsercheckout_refreshHead(this.__wbg_ptr);
        return ret;
    }
    /**
     * Explicitly performs observation-safe synchronization for a live checkout.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported mode, conflicts, storage,
     * cancellation, authentication, or bounded-work failure.
     * @returns {Promise<BrowserCheckpointResult>}
     */
    refreshLive() {
        const ret = wasm.browsercheckout_refreshLive(this.__wbg_ptr);
        return ret;
    }
    /**
     * Removes one namespace binding.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/identities,
     * conflicts, cancellation, storage, or bounded work.
     * @param {string} path
     * @param {Uint8Array | null} [expected_file_id]
     * @returns {Promise<BrowserMutationResult>}
     */
    remove(path, expected_file_id) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(expected_file_id) ? 0 : passArray8ToWasm0(expected_file_id, wasm.__wbindgen_malloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_remove(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Removes one exact named attribute.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, mutation, cancellation, or work failure.
     * @param {string} path
     * @param {string} attribute_class
     * @param {Uint8Array} name
     * @returns {Promise<BrowserMutationResult>}
     */
    removeNamedAttribute(path, attribute_class, name) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(attribute_class, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passArray8ToWasm0(name, wasm.__wbindgen_malloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_removeNamedAttribute(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
        return ret;
    }
    /**
     * Atomically renames one binding within the volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, conflicts,
     * cancellation, storage, or bounded work.
     * @param {string} source
     * @param {string} destination
     * @param {boolean} replace
     * @returns {Promise<BrowserMutationResult>}
     */
    rename(source, destination, replace) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_rename(this.__wbg_ptr, ptr0, len0, ptr1, len1, replace);
        return ret;
    }
    /**
     * Changes one regular file's logical length.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed values, invalid kinds,
     * cancellation, storage, or bounded work.
     * @param {string} path
     * @param {bigint} logical_bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    resizeFile(path, logical_bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_resizeFile(this.__wbg_ptr, ptr0, len0, logical_bytes);
        return ret;
    }
    /**
     * Changes logical length by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/length, absence,
     * invalid kind, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} logical_bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    resizeFileById(file_id, logical_bytes) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_resizeFileById(this.__wbg_ptr, ptr0, len0, logical_bytes);
        return ret;
    }
    /**
     * Resolves an ordered path batch once into immutable generation-bound handles.
     *
     * # Errors
     *
     * Returns a JavaScript error for non-pinned checkouts, malformed paths,
     * corruption, cancellation, or bounded work.
     * @param {BrowserPathBatch} paths
     * @returns {Promise<BrowserResolvedFiles>}
     */
    resolveFiles(paths) {
        const ret = wasm.browsercheckout_resolveFiles(this.__wbg_ptr, paths);
        return ret;
    }
    /**
     * Resumes an unresolved direct-live transaction with the same operation identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, wrong checkout
     * mode, absent staged work, cancellation, storage, rebase, or bounds.
     * @param {Uint8Array} operation_id
     * @param {number} maximum_attempts
     * @param {number} maximum_conflicts
     * @returns {Promise<BrowserLiveMutationResult>}
     */
    resumeLive(operation_id, maximum_attempts, maximum_conflicts) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_resumeLive(this.__wbg_ptr, ptr0, len0, maximum_attempts, maximum_conflicts);
        return ret;
    }
    /**
     * Finds the next sparse data or hole boundary without reading file bodies.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed path/offset/target,
     * non-regular kind, storage, cancellation, or bounded work.
     * @param {string} path
     * @param {bigint} offset
     * @param {string} target
     * @returns {Promise<BrowserExtentSeekResult>}
     */
    seekFileExtent(path, offset, target) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(target, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_seekFileExtent(this.__wbg_ptr, ptr0, len0, offset, ptr1, len1);
        return ret;
    }
    /**
     * Finds the next sparse boundary by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/offset/target,
     * non-regular kind, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} offset
     * @param {string} target
     * @returns {Promise<BrowserExtentSeekResult>}
     */
    seekFileExtentById(file_id, offset, target) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(target, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_seekFileExtentById(this.__wbg_ptr, ptr0, len0, offset, ptr1, len1);
        return ret;
    }
    /**
     * Atomically replaces metadata and optional logical size for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed metadata/path, non-regular
     * resize, storage, cancellation, mutation, or bounded-work failure.
     * @param {string} path
     * @param {Uint8Array} canonical_bytes
     * @param {bigint | null} [logical_bytes]
     * @returns {Promise<BrowserMutationResult>}
     */
    setAttributes(path, canonical_bytes, logical_bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(canonical_bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_setAttributes(this.__wbg_ptr, ptr0, len0, ptr1, len1, !isLikeNone(logical_bytes), isLikeNone(logical_bytes) ? BigInt(0) : logical_bytes);
        return ret;
    }
    /**
     * Atomically replaces metadata and optional logical size by file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/metadata,
     * non-regular resize, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {Uint8Array} canonical_bytes
     * @param {bigint | null} [logical_bytes]
     * @returns {Promise<BrowserMutationResult>}
     */
    setAttributesById(file_id, canonical_bytes, logical_bytes) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(canonical_bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_setAttributesById(this.__wbg_ptr, ptr0, len0, ptr1, len1, !isLikeNone(logical_bytes), isLikeNone(logical_bytes) ? BigInt(0) : logical_bytes);
        return ret;
    }
    /**
     * Atomically replaces complete canonical metadata for one path.
     *
     * # Errors
     *
     * Returns a JavaScript error for path, codec, mutation, cancellation, or work failure.
     * @param {string} path
     * @param {Uint8Array} canonical_bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    setMetadata(path, canonical_bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(canonical_bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_setMetadata(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Replaces complete canonical metadata by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/metadata, absence,
     * storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {Uint8Array} canonical_bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    setMetadataById(file_id, canonical_bytes) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(canonical_bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_setMetadataById(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Returns one complete no-follow file record and canonical metadata.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths, storage,
     * authentication, cancellation, encoding, or bounded-work failure.
     * @param {string} path
     * @returns {Promise<BrowserStatResult>}
     */
    statNoFollow(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_statNoFollow(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Replaces one logical file range in the private COW overlay.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed paths/offsets,
     * non-regular files, cancellation, storage, or bounded work.
     * @param {string} path
     * @param {bigint} offset
     * @param {Uint8Array} bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    writeFile(path, offset, bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_writeFile(this.__wbg_ptr, ptr0, len0, offset, ptr1, len1);
        return ret;
    }
    /**
     * Replaces one logical range by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/offset,
     * non-regular kind, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} offset
     * @param {Uint8Array} bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    writeFileById(file_id, offset, bytes) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_writeFileById(this.__wbg_ptr, ptr0, len0, offset, ptr1, len1);
        return ret;
    }
    /**
     * Inserts or replaces one exact named attribute.
     *
     * # Errors
     *
     * Returns a JavaScript error for class, name, mode, mutation, cancellation, or work failure.
     * @param {string} path
     * @param {string} attribute_class
     * @param {Uint8Array} name
     * @param {Uint8Array} bytes
     * @param {string} mode
     * @returns {Promise<BrowserMutationResult>}
     */
    writeNamedAttribute(path, attribute_class, name, bytes, mode) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(attribute_class, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passArray8ToWasm0(name, wasm.__wbindgen_malloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len3 = WASM_VECTOR_LEN;
        const ptr4 = passStringToWasm0(mode, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len4 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_writeNamedAttribute(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3, ptr4, len4);
        return ret;
    }
    /**
     * Punches a hole or records physically allocated zeros.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed ranges/kinds,
     * cancellation, storage, or bounded work.
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {boolean} allocated
     * @param {boolean} extend
     * @returns {Promise<BrowserMutationResult>}
     */
    zeroFileRange(path, offset, length, allocated, extend) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_zeroFileRange(this.__wbg_ptr, ptr0, len0, offset, length, allocated, extend);
        return ret;
    }
    /**
     * Punches a hole or records allocated zero by stable file identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity/range, absence,
     * invalid kind, storage, cancellation, or bounded work.
     * @param {Uint8Array} file_id
     * @param {bigint} offset
     * @param {bigint} length
     * @param {boolean} allocated
     * @param {boolean} extend
     * @returns {Promise<BrowserMutationResult>}
     */
    zeroFileRangeById(file_id, offset, length, allocated, extend) {
        const ptr0 = passArray8ToWasm0(file_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsercheckout_zeroFileRangeById(this.__wbg_ptr, ptr0, len0, offset, length, allocated, extend);
        return ret;
    }
}
if (Symbol.dispose) BrowserCheckout.prototype[Symbol.dispose] = BrowserCheckout.prototype.free;

/**
 * Authenticated Rust-owned Filesystem grpc-web client for browser WASM.
 *
 * This client uses the generated FilesystemService contract directly. It
 * retains negotiated bounds for every later request and exposes streamed
 * exports through Rust futures; dropping a stream cancels its fetch.
 */
export class BrowserFilesystemClient {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserFilesystemClient.prototype);
        obj.__wbg_ptr = ptr;
        BrowserFilesystemClientFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserFilesystemClientFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserfilesystemclient_free(ptr, 0);
    }
    /**
     * Sends a typed cancellation request encoded by the Rust contract.
     * @param {Uint8Array} request
     * @returns {Promise<Uint8Array>}
     */
    cancel(request) {
        const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfilesystemclient_cancel(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Returns negotiated capability limits to JavaScript.
     * @returns {any}
     */
    capabilities() {
        const ret = wasm.browserfilesystemclient_capabilities(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Connects from JavaScript using the Rust-owned authenticated handshake.
     * @param {string} endpoint
     * @param {string} bearer_token
     * @param {bigint} maximum_request_bytes
     * @param {bigint} maximum_response_bytes
     * @returns {Promise<BrowserFilesystemClient>}
     */
    static connect(endpoint, bearer_token, maximum_request_bytes, maximum_response_bytes) {
        const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(bearer_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserfilesystemclient_connect(ptr0, len0, ptr1, len1, maximum_request_bytes, maximum_response_bytes);
        return ret;
    }
    /**
     * Collects a typed export stream into encoded chunks.
     * @param {Uint8Array} request
     * @returns {Promise<Array<any>>}
     */
    export(request) {
        const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfilesystemclient_export(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
}
if (Symbol.dispose) BrowserFilesystemClient.prototype[Symbol.dispose] = BrowserFilesystemClient.prototype.free;

/**
 * Browser-safe handle backed by the canonical Rust engine.
 */
export class BrowserFs {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserFs.prototype);
        obj.__wbg_ptr = ptr;
        BrowserFsFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserFsFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserfs_free(ptr, 0);
    }
    /**
     * Exact backend facts selected during open.
     *
     * # Errors
     *
     * Returns a JavaScript error when capability serialization fails.
     * @returns {any}
     */
    get capabilities() {
        const ret = wasm.browserfs_capabilities(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Discards resident acceleration without changing persistent state.
     *
     * # Errors
     *
     * Returns a JavaScript error after close or poisoned cache state.
     */
    clearObjectCache() {
        const ret = wasm.browserfs_clearObjectCache(this.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Releases browser handles. Durable state remains in the selected browser stores.
     */
    close() {
        wasm.browserfs_close(this.__wbg_ptr);
    }
    /**
     * Creates both generation-fenced speculation engines over this
     * browser filesystem's authenticated object backend and shared cache.
     *
     * # Errors
     *
     * Returns a JavaScript error after close, for malformed identities or
     * options, or when either engine rejects its hard policy.
     * @param {Uint8Array} volume_id
     * @param {Uint8Array} generation_id
     * @param {any} options
     * @returns {BrowserSpeculation}
     */
    createSpeculation(volume_id, generation_id, options) {
        const ptr0 = passArray8ToWasm0(volume_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(generation_id, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_createSpeculation(this.__wbg_ptr, ptr0, len0, ptr1, len1, options);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return BrowserSpeculation.__wrap(ret[0]);
    }
    /**
     * Creates one independently configured volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for unsupported durability, storage,
     * authentication, cancellation, or bounded-work failure.
     * @param {any} options
     * @returns {Promise<BrowserVolume>}
     */
    createVolume(options) {
        const ret = wasm.browserfs_createVolume(this.__wbg_ptr, options);
        return ret;
    }
    /**
     * Idempotently creates one caller-selected volume identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, incompatible
     * existing configuration, unsupported semantics, or storage failure.
     * @param {Uint8Array} volume_id
     * @param {any} options
     * @returns {Promise<BrowserVolume>}
     */
    createVolumeWithId(volume_id, options) {
        const ptr0 = passArray8ToWasm0(volume_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_createVolumeWithId(this.__wbg_ptr, ptr0, len0, options);
        return ret;
    }
    /**
     * Creates or idempotently reopens one named workspace.
     *
     * # Errors
     *
     * Returns closed-engine, invalid-name, authority, or storage failures.
     * @param {string} name
     * @returns {Promise<BrowserWorkspace>}
     */
    createWorkspace(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_createWorkspace(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Exports one bounded manifest-ordered immutable-object page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifests, invalid cursors,
     * cancellation, storage, allocation, or bounded-work failures.
     * @param {any} manifest
     * @param {bigint} cursor
     * @param {number} maximum_objects
     * @param {bigint} maximum_object_bytes
     * @returns {Promise<GenerationTransferBatchResult>}
     */
    exportGenerationBatch(manifest, cursor, maximum_objects, maximum_object_bytes) {
        const ret = wasm.browserfs_exportGenerationBatch(this.__wbg_ptr, manifest, cursor, maximum_objects, maximum_object_bytes);
        return ret;
    }
    /**
     * Exports one exact authenticated immutable object for resumable transfer.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, absence,
     * corruption, cancellation, storage, or bounded work.
     * @param {Uint8Array} object_id
     * @param {bigint} maximum_bytes
     * @returns {Promise<BrowserFileReadResult>}
     */
    exportObject(object_id, maximum_bytes) {
        const ptr0 = passArray8ToWasm0(object_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_exportObject(this.__wbg_ptr, ptr0, len0, maximum_bytes);
        return ret;
    }
    /**
     * Idempotently imports one manifest-aligned immutable-object page.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifests, cursor/body
     * bounds, cancellation, storage, or bounded-work failures.
     * @param {any} manifest
     * @param {bigint} cursor
     * @param {any} objects
     * @param {number} maximum_objects
     * @returns {Promise<GenerationTransferCursorResult>}
     */
    importGenerationBatch(manifest, cursor, objects, maximum_objects) {
        const ret = wasm.browserfs_importGenerationBatch(this.__wbg_ptr, manifest, cursor, objects, maximum_objects);
        return ret;
    }
    /**
     * Idempotently imports one immutable object under its authenticated identity.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, digest mismatch,
     * cancellation, storage, or bounded work.
     * @param {Uint8Array} object_id
     * @param {Uint8Array} bytes
     * @returns {Promise<BrowserMutationResult>}
     */
    importObject(object_id, bytes) {
        const ptr0 = passArray8ToWasm0(object_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_importObject(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Exact process-local immutable-object accelerator telemetry.
     *
     * # Errors
     *
     * Returns a JavaScript error after close, poisoned cache state, or
     * serialization failure.
     * @returns {any}
     */
    objectCacheStats() {
        const ret = wasm.browserfs_objectCacheStats(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Opens one previously created or restored persistent volume.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identity, authenticated
     * absence, storage corruption, cancellation, or bounded work.
     * @param {Uint8Array} volume_id
     * @returns {Promise<BrowserVolume>}
     */
    openVolume(volume_id) {
        const ptr0 = passArray8ToWasm0(volume_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_openVolume(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Opens one existing named workspace.
     *
     * # Errors
     *
     * Returns closed-engine, invalid-name, absence, authority, or storage failures.
     * @param {string} name
     * @returns {Promise<BrowserWorkspace>}
     */
    openWorkspace(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_openWorkspace(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Restores authority only after authenticating a complete imported closure.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed manifest, incomplete or
     * corrupt closure, conflicting authority, cancellation, or bounded work.
     * @param {any} manifest
     * @param {Uint8Array} operation_id
     * @returns {Promise<BrowserVolume>}
     */
    restoreVolume(manifest, operation_id) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserfs_restoreVolume(this.__wbg_ptr, manifest, ptr0, len0);
        return ret;
    }
}
if (Symbol.dispose) BrowserFs.prototype[Symbol.dispose] = BrowserFs.prototype.free;

/**
 * One exact immutable browser workspace generation.
 */
export class BrowserGeneration {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserGeneration.prototype);
        obj.__wbg_ptr = ptr;
        BrowserGenerationFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserGenerationFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browsergeneration_free(ptr, 0);
    }
    /**
     * Content-addressed generation identity.
     * @returns {Uint8Array}
     */
    get id() {
        const ret = wasm.browsergeneration_id(this.__wbg_ptr);
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
    /**
     * @param {string} path
     * @param {any | null | undefined} after
     * @param {number} maximum_entries
     * @returns {Promise<BrowserWorkspaceDirectoryPage>}
     */
    listDirectory(path, after, maximum_entries) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_listDirectory(this.__wbg_ptr, ptr0, len0, isLikeNone(after) ? 0 : addToExternrefTable0(after), maximum_entries);
        return ret;
    }
    /**
     * Retains this exact generation under one opaque identity.
     * @param {string} identity
     * @returns {Promise<BrowserGeneration>}
     */
    pin(identity) {
        const ptr0 = passStringToWasm0(identity, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_pin(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {number} maximum_spans
     * @returns {Promise<BrowserWorkspaceExtentPlan>}
     */
    planExtents(path, offset, length, maximum_spans) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_planExtents(this.__wbg_ptr, ptr0, len0, offset, length, maximum_spans);
        return ret;
    }
    /**
     * Reads one complete file from this exact immutable state.
     * @param {string} path
     * @param {bigint} maximum_bytes
     * @returns {Promise<Uint8Array>}
     */
    read(path, maximum_bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_read(this.__wbg_ptr, ptr0, len0, maximum_bytes);
        return ret;
    }
    /**
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @returns {Promise<Uint8Array>}
     */
    readRange(path, offset, length) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_readRange(this.__wbg_ptr, ptr0, len0, offset, length);
        return ret;
    }
    /**
     * @param {string} path
     * @returns {Promise<Uint8Array>}
     */
    readSymbolicLink(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_readSymbolicLink(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @param {string} path
     * @returns {Promise<BrowserWorkspaceStat>}
     */
    stat(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergeneration_stat(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Owning opaque workspace identity.
     * @returns {Uint8Array}
     */
    get workspaceId() {
        const ret = wasm.browsergeneration_workspaceId(this.__wbg_ptr);
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
}
if (Symbol.dispose) BrowserGeneration.prototype[Symbol.dispose] = BrowserGeneration.prototype.free;

/**
 * Browser-safe Git-shaped compatibility history over the canonical Rust state machine.
 */
export class BrowserGitCompatRepository {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserGitCompatRepositoryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browsergitcompatrepository_free(ptr, 0);
    }
    /**
     * Validates and canonicalizes a Rust Git output before JS projection.
     * @param {string} value_json
     * @returns {string}
     */
    canonicalizeOutputJson(value_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.browsergitcompatrepository_canonicalizeOutputJson(this.__wbg_ptr, ptr0, len0);
            var ptr2 = ret[0];
            var len2 = ret[1];
            if (ret[3]) {
                ptr2 = 0; len2 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Validates and canonicalizes a durable pending transition before JS
     * projection.
     * @param {string} value_json
     * @returns {string}
     */
    canonicalizePendingTransitionJson(value_json) {
        let deferred3_0;
        let deferred3_1;
        try {
            const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.browsergitcompatrepository_canonicalizePendingTransitionJson(this.__wbg_ptr, ptr0, len0);
            var ptr2 = ret[0];
            var len2 = ret[1];
            if (ret[3]) {
                ptr2 = 0; len2 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Parses and executes a Git-shaped argv command using the Rust source of truth.
     * @param {string[]} argv
     * @param {Uint8Array} workspace_generation
     * @param {string} default_author
     * @param {bigint} now_seconds
     * @returns {Promise<string>}
     */
    executeArgvJson(argv, workspace_generation, default_author, now_seconds) {
        const ptr0 = passArrayJsValueToWasm0(argv, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(workspace_generation, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(default_author, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.browsergitcompatrepository_executeArgvJson(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, now_seconds);
        return ret;
    }
    /**
     * Executes one typed command encoded with the public serde contract.
     * @param {string} command_json
     * @param {Uint8Array} workspace_generation
     * @returns {Promise<string>}
     */
    executeJson(command_json, workspace_generation) {
        const ptr0 = passStringToWasm0(command_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(workspace_generation, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsergitcompatrepository_executeJson(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Executes a natural JavaScript Git command through the Rust
     * projection shared with the native binding.
     * @param {string} command_json
     * @param {Uint8Array} workspace_generation
     * @returns {Promise<string>}
     */
    executePublicJson(command_json, workspace_generation) {
        const ptr0 = passStringToWasm0(command_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(workspace_generation, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsergitcompatrepository_executePublicJson(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates process-local compatibility state for one SDK workspace.
     * @param {Uint8Array} workspace_id
     */
    constructor(workspace_id) {
        const ptr0 = passArray8ToWasm0(workspace_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsergitcompatrepository_new(ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        this.__wbg_ptr = ret[0] >>> 0;
        BrowserGitCompatRepositoryFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
}
if (Symbol.dispose) BrowserGitCompatRepository.prototype[Symbol.dispose] = BrowserGitCompatRepository.prototype.free;

/**
 * Authenticated Rust-owned Harness gRPC-Web client for browser WASM.
 *
 * Harness has no HTTP/JSON projection. Browser consumers use the same
 * generated protobuf service through gRPC-Web, with all protocol checks
 * and operation-control semantics retained in Rust.
 */
export class BrowserHarnessClient {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserHarnessClient.prototype);
        obj.__wbg_ptr = ptr;
        BrowserHarnessClientFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserHarnessClientFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserharnessclient_free(ptr, 0);
    }
    /**
     * Cancels an encoded Rust Harness operation request.
     * @param {Uint8Array} request
     * @returns {Promise<Uint8Array>}
     */
    cancel(request) {
        const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserharnessclient_cancel(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Returns the negotiated Harness protocol identity.
     * @returns {any}
     */
    capabilities() {
        const ret = wasm.browserharnessclient_capabilities(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Connects from JavaScript using the Rust-owned authenticated
     * handshake and gRPC-Web adapter with Rust-owned safe bounds.
     * @param {string} endpoint
     * @param {string} bearer_token
     * @returns {Promise<BrowserHarnessClient>}
     */
    static connect(endpoint, bearer_token) {
        const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(bearer_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserharnessclient_connect(ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Connects with explicit request and response bounds.
     * @param {string} endpoint
     * @param {string} bearer_token
     * @param {bigint} maximum_request_bytes
     * @param {bigint} maximum_response_bytes
     * @returns {Promise<BrowserHarnessClient>}
     */
    static connectWithLimits(endpoint, bearer_token, maximum_request_bytes, maximum_response_bytes) {
        const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(bearer_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserharnessclient_connectWithLimits(ptr0, len0, ptr1, len1, maximum_request_bytes, maximum_response_bytes);
        return ret;
    }
    /**
     * Observes an encoded Rust Harness operation request.
     * @param {Uint8Array} request
     * @returns {Promise<Uint8Array>}
     */
    observe(request) {
        const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserharnessclient_observe(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Collects a bounded replay page into encoded deliveries. The Rust
     * API remains streaming for callers that need a live follow stream.
     * @param {Uint8Array} request
     * @returns {Promise<Array<any>>}
     */
    replay(request) {
        const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserharnessclient_replay(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Submits an encoded Rust Harness command and returns its encoded
     * admission. The protobuf bytes preserve the generated wire types.
     * @param {Uint8Array} request
     * @returns {Promise<Uint8Array>}
     */
    submit(request) {
        const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserharnessclient_submit(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
}
if (Symbol.dispose) BrowserHarnessClient.prototype[Symbol.dispose] = BrowserHarnessClient.prototype.free;

/**
 * One immutable, side-effect-free workspace join plan.
 */
export class BrowserJoinPlan {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserJoinPlan.prototype);
        obj.__wbg_ptr = ptr;
        BrowserJoinPlanFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserJoinPlanFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserjoinplan_free(ptr, 0);
    }
    /**
     * Applies this immutable plan through one exact target-head CAS.
     * @param {Uint8Array} if_target
     * @param {Uint8Array | null} [idempotency_key]
     * @returns {Promise<BrowserJoinResult>}
     */
    apply(if_target, idempotency_key) {
        const ptr0 = passArray8ToWasm0(if_target, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(idempotency_key) ? 0 : passArray8ToWasm0(idempotency_key, wasm.__wbindgen_malloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserjoinplan_apply(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Exact discovered common ancestor.
     * @returns {Uint8Array}
     */
    get commonAncestor() {
        const ret = wasm.browserjoinplan_commonAncestor(this.__wbg_ptr);
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
    /**
     * Target generation observed while planning.
     * @returns {Uint8Array}
     */
    get targetHead() {
        const ret = wasm.browserjoinplan_targetHead(this.__wbg_ptr);
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
}
if (Symbol.dispose) BrowserJoinPlan.prototype[Symbol.dispose] = BrowserJoinPlan.prototype.free;

/**
 * One immutable file resolved against a pinned checkout generation.
 */
export class BrowserResolvedFile {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserResolvedFile.prototype);
        obj.__wbg_ptr = ptr;
        BrowserResolvedFileFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserResolvedFileFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserresolvedfile_free(ptr, 0);
    }
    /**
     * Terminal file kind authenticated by the pinned generation.
     * @returns {string}
     */
    get kind() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.browserresolvedfile_kind(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Logical content length authenticated by the pinned generation.
     * @returns {bigint}
     */
    get logicalBytes() {
        const ret = wasm.browserresolvedfile_logicalBytes(this.__wbg_ptr);
        return BigInt.asUintN(64, ret);
    }
    /**
     * Complete canonical metadata authenticated by the pinned generation.
     * @returns {Uint8Array}
     */
    get metadataCanonicalBytes() {
        const ret = wasm.browserresolvedfile_metadataCanonicalBytes(this.__wbg_ptr);
        if (ret[3]) {
            throw takeFromExternrefTable0(ret[2]);
        }
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
    /**
     * Reads one exact logical range without another namespace lookup.
     * @param {bigint} offset
     * @param {bigint} length
     * @returns {Promise<BrowserFileReadResult>}
     */
    readRange(offset, length) {
        const ret = wasm.browserresolvedfile_readRange(this.__wbg_ptr, offset, length);
        return ret;
    }
    /**
     * Reads opaque symbolic-link target bytes without another namespace lookup.
     * @returns {Promise<BrowserFileReadResult>}
     */
    readSymbolicLink() {
        const ret = wasm.browserresolvedfile_readSymbolicLink(this.__wbg_ptr);
        return ret;
    }
}
if (Symbol.dispose) BrowserResolvedFile.prototype[Symbol.dispose] = BrowserResolvedFile.prototype.free;

/**
 * One original-order resolved path batch and its shared work receipt.
 */
export class BrowserResolvedFiles {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserResolvedFiles.prototype);
        obj.__wbg_ptr = ptr;
        BrowserResolvedFilesFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserResolvedFilesFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserresolvedfiles_free(ptr, 0);
    }
    /**
     * Number of original-order results.
     * @returns {number}
     */
    get length() {
        const ret = wasm.browserresolvedfiles_length(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Transfers one generation-bound handle to JavaScript. Each index may be taken once.
     * @param {number} index
     * @returns {BrowserResolvedFile | undefined}
     */
    take(index) {
        const ret = wasm.browserresolvedfiles_take(this.__wbg_ptr, index);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] === 0 ? undefined : BrowserResolvedFile.__wrap(ret[0]);
    }
    /**
     * Exact work receipt for the shared namespace traversal.
     * @returns {BrowserWorkCounters}
     */
    get work() {
        const ret = wasm.browserresolvedfiles_work(this.__wbg_ptr);
        return ret;
    }
}
if (Symbol.dispose) BrowserResolvedFiles.prototype[Symbol.dispose] = BrowserResolvedFiles.prototype.free;

/**
 * Browser owner of one volume generation's residency and promotion engines.
 */
export class BrowserSpeculation {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserSpeculation.prototype);
        obj.__wbg_ptr = ptr;
        BrowserSpeculationFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserSpeculationFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserspeculation_free(ptr, 0);
    }
    /**
     * Cooperatively cancels future residency execution from this owner.
     */
    cancel() {
        wasm.browserspeculation_cancel(this.__wbg_ptr);
    }
    /**
     * Executes one admitted residency prediction through the browser's
     * authenticated object backend and shared cache.
     *
     * # Errors
     *
     * Returns a JavaScript error for an inactive operation, storage or
     * authentication failure, bounded-work exhaustion, or cancellation.
     * @param {Uint8Array} operation_id
     * @returns {Promise<BrowserResidencyExecution>}
     */
    executeResidency(operation_id) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserspeculation_executeResidency(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Records terminal usefulness for one promotion operation.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed or inactive operation identities.
     * @param {Uint8Array} operation_id
     * @param {boolean} useful
     */
    finishPromotion(operation_id, useful) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserspeculation_finishPromotion(this.__wbg_ptr, ptr0, len0, useful);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Records terminal usefulness for one residency operation.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed or inactive operation identities.
     * @param {Uint8Array} operation_id
     * @param {boolean} useful
     */
    finishResidency(operation_id, useful) {
        const ptr0 = passArray8ToWasm0(operation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserspeculation_finishResidency(this.__wbg_ptr, ptr0, len0, useful);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Returns exact payload-free metrics for both engines.
     *
     * # Errors
     *
     * Returns a JavaScript error if metrics cannot be serialized.
     * @returns {BrowserSpeculationMetrics}
     */
    metrics() {
        const ret = wasm.browserspeculation_metrics(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Records foreground demand and admits one authenticated successor.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed input or a failed bounded transition.
     * @param {any} observation
     * @returns {BrowserAdmissionResult}
     */
    observe(observation) {
        const ret = wasm.browserspeculation_observe(this.__wbg_ptr, observation);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Plans one bounded promotion from exact caller-observed location facts.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed facts, unsupported tiers,
     * inactive residency, or a failed bounded transition.
     * @param {any} request
     * @returns {BrowserPromotionAdmission}
     */
    planPromotion(request) {
        const ret = wasm.browserspeculation_planPromotion(this.__wbg_ptr, request);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Atomically preempts both engines before recording foreground bytes.
     *
     * # Errors
     *
     * Returns a JavaScript error if exact bounded accounting fails.
     * @param {bigint} bytes
     * @returns {BrowserSpeculationPreemption}
     */
    preemptForForeground(bytes) {
        const ret = wasm.browserspeculation_preemptForForeground(this.__wbg_ptr, bytes);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Atomically fences both engines onto a new immutable generation.
     *
     * # Errors
     *
     * Returns a JavaScript error for a malformed identity or failed transition.
     * @param {Uint8Array} generation_id
     * @returns {BrowserSpeculationPreemption}
     */
    replaceGeneration(generation_id) {
        const ptr0 = passArray8ToWasm0(generation_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserspeculation_replaceGeneration(this.__wbg_ptr, ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
}
if (Symbol.dispose) BrowserSpeculation.prototype[Symbol.dispose] = BrowserSpeculation.prototype.free;

/**
 * One sparse atomic browser workspace transaction.
 */
export class BrowserTransaction {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserTransaction.prototype);
        obj.__wbg_ptr = ptr;
        BrowserTransactionFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserTransactionFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browsertransaction_free(ptr, 0);
    }
    /**
     * Clones one immutable range without reading file bytes.
     * @param {string} source
     * @param {bigint} source_offset
     * @param {string} destination
     * @param {bigint} destination_offset
     * @param {bigint} length
     * @returns {Promise<void>}
     */
    cloneRange(source, source_offset, destination, destination_offset, length) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_cloneRange(this.__wbg_ptr, ptr0, len0, source_offset, ptr1, len1, destination_offset, length);
        return ret;
    }
    /**
     * Publishes the complete candidate through one idempotent head CAS.
     * @returns {Promise<any>}
     */
    commit() {
        const ret = wasm.browsertransaction_commit(this.__wbg_ptr);
        return ret;
    }
    /**
     * Clones one complete regular file without copying its body.
     * @param {string} source
     * @param {string} destination
     * @returns {Promise<void>}
     */
    copy(source, destination) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_copy(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates every absent directory on one canonical path.
     * @param {string} path
     * @returns {Promise<void>}
     */
    createDirAll(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_createDirAll(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Creates exactly one empty directory.
     * @param {string} path
     * @returns {Promise<void>}
     */
    createDirectory(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_createDirectory(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Creates one symbolic link with an opaque target.
     * @param {string} path
     * @param {Uint8Array} target
     * @returns {Promise<void>}
     */
    createSymbolicLink(path, target) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(target, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_createSymbolicLink(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates one same-workspace hard link.
     * @param {string} source
     * @param {string} destination
     * @returns {Promise<void>}
     */
    hardLink(source, destination) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_hardLink(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Preallocates one sparse range without replacing content.
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {boolean} keep_size
     * @returns {Promise<void>}
     */
    preallocate(path, offset, length, keep_size) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_preallocate(this.__wbg_ptr, ptr0, len0, offset, length, keep_size);
        return ret;
    }
    /**
     * Safely advances this retained candidate and sparsely replays its work.
     * @param {number} maximum_conflicts
     * @returns {Promise<any>}
     */
    rebase(maximum_conflicts) {
        const ret = wasm.browsertransaction_rebase(this.__wbg_ptr, maximum_conflicts);
        return ret;
    }
    /**
     * Removes one existing namespace binding inside this transaction.
     * @param {string} path
     * @returns {Promise<void>}
     */
    remove(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_remove(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Atomically renames one namespace binding inside this transaction.
     * @param {string} source
     * @param {string} destination
     * @returns {Promise<void>}
     */
    rename(source, destination) {
        const ptr0 = passStringToWasm0(source, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_rename(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Changes one regular file's logical length.
     * @param {string} path
     * @param {bigint} logical_bytes
     * @returns {Promise<void>}
     */
    resize(path, logical_bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_resize(this.__wbg_ptr, ptr0, len0, logical_bytes);
        return ret;
    }
    /**
     * Creates or replaces one complete file inside this transaction.
     * @param {string} path
     * @param {Uint8Array} bytes
     * @returns {Promise<void>}
     */
    write(path, bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_write(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Replaces one sparse regular-file range.
     * @param {string} path
     * @param {bigint} offset
     * @param {Uint8Array} bytes
     * @returns {Promise<void>}
     */
    writeRange(path, offset, bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_writeRange(this.__wbg_ptr, ptr0, len0, offset, ptr1, len1);
        return ret;
    }
    /**
     * Punches a hole or installs allocated zeros over one exact range.
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {boolean} allocated
     * @param {boolean} extend
     * @returns {Promise<void>}
     */
    zeroRange(path, offset, length, allocated, extend) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browsertransaction_zeroRange(this.__wbg_ptr, ptr0, len0, offset, length, allocated, extend);
        return ret;
    }
}
if (Symbol.dispose) BrowserTransaction.prototype[Symbol.dispose] = BrowserTransaction.prototype.free;

/**
 * One independently configured browser volume.
 */
export class BrowserVolume {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserVolume.prototype);
        obj.__wbg_ptr = ptr;
        BrowserVolumeFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserVolumeFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browservolume_free(ptr, 0);
    }
    /**
     * Returns exact bounded work used to acquire this volume handle.
     * @returns {BrowserWorkCounters}
     */
    get acquisitionWork() {
        const ret = wasm.browservolume_acquisitionWork(this.__wbg_ptr);
        return ret;
    }
    /**
     * Opens the volume head with explicit access and consistency semantics.
     *
     * # Errors
     *
     * Returns a JavaScript error for an invalid mode or authenticated
     * storage, cancellation, and bounded-work failures.
     * @param {any} options
     * @returns {Promise<BrowserCheckout>}
     */
    checkout(options) {
        const ret = wasm.browservolume_checkout(this.__wbg_ptr, options);
        return ret;
    }
    /**
     * Computes one bounded Merkle-aware semantic generation diff.
     *
     * # Errors
     *
     * Returns a JavaScript error for malformed identities, corrupt
     * storage, cancellation, allocation, or bounded work.
     * @param {Uint8Array} before
     * @param {Uint8Array} after
     * @param {number} maximum_changes
     * @returns {Promise<BrowserGenerationDiffResult>}
     */
    diffGenerations(before, after, maximum_changes) {
        const ptr0 = passArray8ToWasm0(before, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(after, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browservolume_diffGenerations(this.__wbg_ptr, ptr0, len0, ptr1, len1, maximum_changes);
        return ret;
    }
    /**
     * Returns the canonical 16-byte volume identity.
     * @returns {Uint8Array}
     */
    get id() {
        const ret = wasm.browservolume_id(this.__wbg_ptr);
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
}
if (Symbol.dispose) BrowserVolume.prototype[Symbol.dispose] = BrowserVolume.prototype.free;

/**
 * One named customer workspace.
 */
export class BrowserWorkspace {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrowserWorkspace.prototype);
        obj.__wbg_ptr = ptr;
        BrowserWorkspaceFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserWorkspaceFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserworkspace_free(ptr, 0);
    }
    /**
     * Begins one sparse atomic transaction at the current workspace head.
     * @param {Uint8Array | null} [idempotency_key]
     * @returns {Promise<BrowserTransaction>}
     */
    beginTransaction(idempotency_key) {
        var ptr0 = isLikeNone(idempotency_key) ? 0 : passArray8ToWasm0(idempotency_key, wasm.__wbindgen_malloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_beginTransaction(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Retains the current generation under one human-readable label.
     * @param {string} label
     * @returns {Promise<BrowserGeneration>}
     */
    checkpoint(label) {
        const ptr0 = passStringToWasm0(label, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_checkpoint(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Terminally removes this mutable workspace head.
     * @param {Uint8Array | null} [idempotency_key]
     * @returns {Promise<string>}
     */
    delete(idempotency_key) {
        var ptr0 = isLikeNone(idempotency_key) ? 0 : passArray8ToWasm0(idempotency_key, wasm.__wbindgen_malloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_delete(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Computes one immutable bounded semantic delta between exact generations.
     * @param {BrowserGeneration} from
     * @param {BrowserGeneration} to
     * @param {number} maximum_changes
     * @returns {Promise<BrowserChangeSet>}
     */
    diff(from, to, maximum_changes) {
        _assertClass(from, BrowserGeneration);
        _assertClass(to, BrowserGeneration);
        const ret = wasm.browserworkspace_diff(this.__wbg_ptr, from.__wbg_ptr, to.__wbg_ptr, maximum_changes);
        return ret;
    }
    /**
     * Forks the current generation into an independent named workspace.
     * @param {string} destination
     * @param {Uint8Array | null} [idempotency_key]
     * @returns {Promise<BrowserWorkspace>}
     */
    fork(destination, idempotency_key) {
        const ptr0 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(idempotency_key) ? 0 : passArray8ToWasm0(idempotency_key, wasm.__wbindgen_malloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_fork(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Creates an independent workspace at one caller-selected exact generation.
     * @param {string} destination
     * @param {BrowserGeneration} generation
     * @param {Uint8Array | null} [idempotency_key]
     * @returns {Promise<BrowserWorkspace>}
     */
    forkAt(destination, generation, idempotency_key) {
        const ptr0 = passStringToWasm0(destination, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        _assertClass(generation, BrowserGeneration);
        var ptr1 = isLikeNone(idempotency_key) ? 0 : passArray8ToWasm0(idempotency_key, wasm.__wbindgen_malloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_forkAt(this.__wbg_ptr, ptr0, len0, generation.__wbg_ptr, ptr1, len1);
        return ret;
    }
    /**
     * Current immutable generation identity.
     * @returns {Promise<Uint8Array>}
     */
    head() {
        const ret = wasm.browserworkspace_head(this.__wbg_ptr);
        return ret;
    }
    /**
     * Stable opaque workspace identity.
     * @returns {Uint8Array}
     */
    get id() {
        const ret = wasm.browserworkspace_id(this.__wbg_ptr);
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
    /**
     * Builds one immutable side-effect-free plan for joining this workspace into a target.
     * @param {BrowserWorkspace} target
     * @param {any} options
     * @returns {Promise<BrowserJoinPlan>}
     */
    joinInto(target, options) {
        _assertClass(target, BrowserWorkspace);
        const ret = wasm.browserworkspace_joinInto(this.__wbg_ptr, target.__wbg_ptr, options);
        return ret;
    }
    /**
     * Advances this fork onto its source workspace's current generation.
     * @param {Uint8Array | null | undefined} idempotency_key
     * @param {number} maximum_generations
     * @param {number} maximum_changes
     * @param {number} maximum_conflicts
     * @returns {Promise<BrowserWorkspaceRebaseResult>}
     */
    liveRebase(idempotency_key, maximum_generations, maximum_changes, maximum_conflicts) {
        var ptr0 = isLikeNone(idempotency_key) ? 0 : passArray8ToWasm0(idempotency_key, wasm.__wbindgen_malloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_liveRebase(this.__wbg_ptr, ptr0, len0, maximum_generations, maximum_changes, maximum_conflicts);
        return ret;
    }
    /**
     * Canonical workspace name.
     * @returns {string}
     */
    get name() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.browserworkspace_name(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Retains the current generation under one opaque stable identity.
     * @param {string} identity
     * @returns {Promise<BrowserGeneration>}
     */
    pin(identity) {
        const ptr0 = passStringToWasm0(identity, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_pin(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @param {number} maximum_spans
     * @returns {Promise<BrowserWorkspaceExtentPlan>}
     */
    planExtents(path, offset, length, maximum_spans) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_planExtents(this.__wbg_ptr, ptr0, len0, offset, length, maximum_spans);
        return ret;
    }
    /**
     * Reads one complete regular file under a byte bound.
     * @param {string} path
     * @param {bigint} maximum_bytes
     * @returns {Promise<Uint8Array>}
     */
    read(path, maximum_bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_read(this.__wbg_ptr, ptr0, len0, maximum_bytes);
        return ret;
    }
    /**
     * @param {string} path
     * @param {bigint} offset
     * @param {bigint} length
     * @returns {Promise<Uint8Array>}
     */
    readRange(path, offset, length) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_readRange(this.__wbg_ptr, ptr0, len0, offset, length);
        return ret;
    }
    /**
     * @param {string} path
     * @returns {Promise<Uint8Array>}
     */
    readSymbolicLink(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_readSymbolicLink(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Removes one existing path atomically.
     * @param {string} path
     * @returns {Promise<BrowserWorkspaceCommit>}
     */
    remove(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_remove(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * @param {string} path
     * @returns {Promise<BrowserWorkspaceStat>}
     */
    stat(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_stat(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Synchronizes prior operations and returns the exact immutable head.
     * @returns {Promise<BrowserGeneration>}
     */
    sync() {
        const ret = wasm.browserworkspace_sync(this.__wbg_ptr);
        return ret;
    }
    /**
     * Atomically creates or replaces one complete file.
     * @param {string} path
     * @param {Uint8Array} bytes
     * @returns {Promise<BrowserWorkspaceCommit>}
     */
    write(path, bytes) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspace_write(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
}
if (Symbol.dispose) BrowserWorkspace.prototype[Symbol.dispose] = BrowserWorkspace.prototype.free;

/**
 * Browser-safe recursive multi-root context registry.
 */
export class BrowserWorkspaceContextRegistry {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrowserWorkspaceContextRegistryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_browserworkspacecontextregistry_free(ptr, 0);
    }
    /**
     * Adopts one parent-authorized root without enumerating its contents.
     * @param {Uint8Array} context_id
     * @param {Uint8Array} root_wire
     * @returns {Promise<Uint8Array>}
     */
    adoptRoot(context_id, root_wire) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(root_wire, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_adoptRoot(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Recursively tombstones a direct-child subtree.
     * @param {Uint8Array} parent_context_id
     * @param {Uint8Array} child_context_id
     * @param {number} maximum
     * @returns {Promise<Uint8Array>}
     */
    discardSubtree(parent_context_id, child_context_id, maximum) {
        const ptr0 = passArray8ToWasm0(parent_context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(child_context_id, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_discardSubtree(this.__wbg_ptr, ptr0, len0, ptr1, len1, maximum);
        return ret;
    }
    /**
     * Creates an in-process registry. Host persistence can be injected by
     * the JavaScript embedding when it is available.
     */
    constructor() {
        const ret = wasm.browserworkspacecontextregistry_new();
        this.__wbg_ptr = ret >>> 0;
        BrowserWorkspaceContextRegistryFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Registers an exact direct child from the shared serde contract.
     * @param {Uint8Array} context_id
     * @param {Uint8Array} parent_context_id
     * @param {Uint8Array} roots_wire
     * @returns {Promise<Uint8Array>}
     */
    registerChild(context_id, parent_context_id, roots_wire) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(parent_context_id, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passArray8ToWasm0(roots_wire, wasm.__wbindgen_malloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_registerChild(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2);
        return ret;
    }
    /**
     * Registers one root context from the shared serde contract.
     * @param {Uint8Array} context_id
     * @param {Uint8Array} roots_wire
     * @returns {Promise<Uint8Array>}
     */
    registerRoot(context_id, roots_wire) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(roots_wire, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_registerRoot(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Releases one root after callers have settled its filesystem changes.
     * @param {Uint8Array} context_id
     * @param {Uint8Array} root_id
     * @returns {Promise<Uint8Array>}
     */
    removeRoot(context_id, root_id) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(root_id, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_removeRoot(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Resolves one context as stable JSON.
     * @param {Uint8Array} context_id
     * @returns {Promise<Uint8Array>}
     */
    resolve(context_id) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_resolve(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Freezes or resumes one durable context.
     * @param {Uint8Array} context_id
     * @param {boolean} active
     * @returns {Promise<Uint8Array>}
     */
    setActive(context_id, active) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_setActive(this.__wbg_ptr, ptr0, len0, active);
        return ret;
    }
    /**
     * Advances one root binding after a compatibility branch switch.
     * @param {Uint8Array} context_id
     * @param {Uint8Array} root_id
     * @param {Uint8Array} workspace_id
     * @param {string} workspace_name
     * @param {Uint8Array | null} [parent_workspace_id]
     * @returns {Promise<Uint8Array>}
     */
    setWorkspace(context_id, root_id, workspace_id, workspace_name, parent_workspace_id) {
        const ptr0 = passArray8ToWasm0(context_id, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(root_id, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passArray8ToWasm0(workspace_id, wasm.__wbindgen_malloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(workspace_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len3 = WASM_VECTOR_LEN;
        var ptr4 = isLikeNone(parent_workspace_id) ? 0 : passArray8ToWasm0(parent_workspace_id, wasm.__wbindgen_malloc);
        var len4 = WASM_VECTOR_LEN;
        const ret = wasm.browserworkspacecontextregistry_setWorkspace(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3, ptr4, len4);
        return ret;
    }
}
if (Symbol.dispose) BrowserWorkspaceContextRegistry.prototype[Symbol.dispose] = BrowserWorkspaceContextRegistry.prototype.free;

/**
 * Rust-owned state machine for the polling form of hosted HTTP follow.
 * The JavaScript boundary supplies only fetch and timer primitives.
 */
export class HttpFollowCursor {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        HttpFollowCursorFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_httpfollowcursor_free(ptr, 0);
    }
    /**
     * @param {string} response_json
     */
    acceptRead(response_json) {
        const ptr0 = passStringToWasm0(response_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.httpfollowcursor_acceptRead(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * @param {string} response_json
     */
    acceptTail(response_json) {
        const ptr0 = passStringToWasm0(response_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.httpfollowcursor_acceptTail(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    close() {
        wasm.httpfollowcursor_close(this.__wbg_ptr);
    }
    /**
     * @returns {boolean}
     */
    isClosed() {
        const ret = wasm.httpfollowcursor_isClosed(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @param {Uint8Array} input
     */
    constructor(input) {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.httpfollowcursor_new(ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        this.__wbg_ptr = ret[0] >>> 0;
        HttpFollowCursorFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * @returns {number}
     */
    pollDelayMillis() {
        const ret = wasm.httpfollowcursor_pollDelayMillis(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * @returns {Uint8Array}
     */
    readRequest() {
        const ret = wasm.httpfollowcursor_readRequest(this.__wbg_ptr);
        if (ret[3]) {
            throw takeFromExternrefTable0(ret[2]);
        }
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
    /**
     * @returns {boolean}
     */
    shouldPoll() {
        const ret = wasm.httpfollowcursor_shouldPoll(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {Uint8Array}
     */
    tailRequest() {
        const ret = wasm.httpfollowcursor_tailRequest(this.__wbg_ptr);
        if (ret[3]) {
            throw takeFromExternrefTable0(ret[2]);
        }
        var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        return v1;
    }
}
if (Symbol.dispose) HttpFollowCursor.prototype[Symbol.dispose] = HttpFollowCursor.prototype.free;

class IntoUnderlyingByteSource {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        IntoUnderlyingByteSourceFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_intounderlyingbytesource_free(ptr, 0);
    }
    /**
     * @returns {number}
     */
    get autoAllocateChunkSize() {
        const ret = wasm.intounderlyingbytesource_autoAllocateChunkSize(this.__wbg_ptr);
        return ret >>> 0;
    }
    cancel() {
        const ptr = this.__destroy_into_raw();
        wasm.intounderlyingbytesource_cancel(ptr);
    }
    /**
     * @param {ReadableByteStreamController} controller
     * @returns {Promise<any>}
     */
    pull(controller) {
        const ret = wasm.intounderlyingbytesource_pull(this.__wbg_ptr, controller);
        return ret;
    }
    /**
     * @param {ReadableByteStreamController} controller
     */
    start(controller) {
        wasm.intounderlyingbytesource_start(this.__wbg_ptr, controller);
    }
    /**
     * @returns {ReadableStreamType}
     */
    get type() {
        const ret = wasm.intounderlyingbytesource_type(this.__wbg_ptr);
        return __wbindgen_enum_ReadableStreamType[ret];
    }
}
if (Symbol.dispose) IntoUnderlyingByteSource.prototype[Symbol.dispose] = IntoUnderlyingByteSource.prototype.free;

class IntoUnderlyingSink {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        IntoUnderlyingSinkFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_intounderlyingsink_free(ptr, 0);
    }
    /**
     * @param {any} reason
     * @returns {Promise<any>}
     */
    abort(reason) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.intounderlyingsink_abort(ptr, reason);
        return ret;
    }
    /**
     * @returns {Promise<any>}
     */
    close() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.intounderlyingsink_close(ptr);
        return ret;
    }
    /**
     * @param {any} chunk
     * @returns {Promise<any>}
     */
    write(chunk) {
        const ret = wasm.intounderlyingsink_write(this.__wbg_ptr, chunk);
        return ret;
    }
}
if (Symbol.dispose) IntoUnderlyingSink.prototype[Symbol.dispose] = IntoUnderlyingSink.prototype.free;

class IntoUnderlyingSource {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        IntoUnderlyingSourceFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_intounderlyingsource_free(ptr, 0);
    }
    cancel() {
        const ptr = this.__destroy_into_raw();
        wasm.intounderlyingsource_cancel(ptr);
    }
    /**
     * @param {ReadableStreamDefaultController} controller
     * @returns {Promise<any>}
     */
    pull(controller) {
        const ret = wasm.intounderlyingsource_pull(this.__wbg_ptr, controller);
        return ret;
    }
}
if (Symbol.dispose) IntoUnderlyingSource.prototype[Symbol.dispose] = IntoUnderlyingSource.prototype.free;

/**
 * One Rust-backed live follow cursor.
 *
 * `next` releases the state lock before awaiting the stream, so `close` can
 * always signal a pending call and promptly release its cursor.
 */
export class WasmFollow {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmFollow.prototype);
        obj.__wbg_ptr = ptr;
        WasmFollowFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmFollowFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmfollow_free(ptr, 0);
    }
    /**
     * Cancels the cursor and wakes any pending `next` call.
     */
    close() {
        wasm.wasmfollow_close(this.__wbg_ptr);
    }
    /**
     * Waits for one record. Returns `null` after close or stream termination.
     * @returns {Uint8Array | null}
     */
    next() {
        const ret = wasm.wasmfollow_next(this.__wbg_ptr);
        return ret;
    }
}
if (Symbol.dispose) WasmFollow.prototype[Symbol.dispose] = WasmFollow.prototype.free;

/**
 * Stateful browser provider backed by the canonical Rust memory provider.
 *
 * Unary operations use `dispatch(operation, request_bytes)` and return the
 * corresponding protobuf response bytes. `read` and `children` return arrays
 * of encoded stream response messages because protobuf streams have no single
 * finite response envelope.
 */
export class WasmMemoryStream {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmMemoryStreamFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmmemorystream_free(ptr, 0);
    }
    /**
     * Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
     * @param {Uint8Array} input
     * @returns {Uint8Array[]}
     */
    children(input) {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmmemorystream_children(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Executes one finite unary operation over canonical protobuf bytes.
     * @param {string} operation
     * @param {Uint8Array} input
     * @returns {Promise<Uint8Array>}
     */
    dispatch(operation, input) {
        const ptr0 = passStringToWasm0(operation, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmmemorystream_dispatch(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    constructor() {
        const ret = wasm.wasmmemorystream_new();
        this.__wbg_ptr = ret >>> 0;
        WasmMemoryStreamFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Opens a live follow cursor backed by the canonical provider.
     * @param {Uint8Array} input
     * @returns {Promise<WasmFollow>}
     */
    open_follow(input) {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmmemorystream_open_follow(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Reads one bounded page, returning encoded `ReadResponse` messages.
     * @param {Uint8Array} input
     * @returns {Uint8Array[]}
     */
    read(input) {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmmemorystream_read(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
}
if (Symbol.dispose) WasmMemoryStream.prototype[Symbol.dispose] = WasmMemoryStream.prototype.free;

/**
 * Type-only bridge for the complete Rust-owned Stream error-code contract.
 * @param {StreamErrorCode} value
 * @returns {StreamErrorCode}
 */
export function __streamErrorCodeContract(value) {
    const ret = wasm.__streamErrorCodeContract(value);
    return ret;
}

/**
 * Advances the cumulative byte count for a hosted response. The caller may
 * read chunks natively, but Rust owns overflow and configured-bound policy.
 * @param {bigint} total
 * @param {bigint} chunk
 * @param {bigint} maximum
 * @returns {bigint}
 */
export function consumeHttpResponseBytes(total, chunk, maximum) {
    const ret = wasm.consumeHttpResponseBytes(total, chunk, maximum);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return BigInt.asUintN(64, ret[0]);
}

/**
 * Validate and project one hosted HTTP JSON success response into the public
 * JavaScript shape. Rust owns the scalar widths and tagged response schema:
 * decimal uint64 strings become `bigint`, base64 bytes become `Uint8Array`,
 * and token timestamps become `Date` values before the value crosses the
 * browser boundary.
 * @param {string} route
 * @param {string} response_json
 * @returns {unknown}
 */
export function decodeHttpResponse(route, response_json) {
    const ptr0 = passStringToWasm0(route, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(response_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.decodeHttpResponse(ptr0, len0, ptr1, len1);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
}

/**
 * Decodes a versioned merge-candidate envelope to its canonical payload.
 * @param {string} value_json
 * @returns {string}
 */
export function decodeMergeCandidateJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.decodeMergeCandidateJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Decodes a versioned merge-plan envelope to its canonical payload.
 * @param {string} value_json
 * @returns {string}
 */
export function decodeMergePlanJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.decodeMergePlanJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Decodes a multi-root candidate envelope to its canonical payload.
 * @param {string} value_json
 * @returns {string}
 */
export function decodeMultiRootCandidateJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.decodeMultiRootCandidateJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Decodes a versioned multi-root plan envelope to its canonical payload.
 * @param {string} value_json
 * @returns {string}
 */
export function decodeMultiRootPlanJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.decodeMultiRootPlanJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Decodes a versioned publication envelope to its canonical payload.
 * @param {string} value_json
 * @returns {string}
 */
export function decodePublicationJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.decodePublicationJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Returns the canonical default cumulative hosted response bound.
 * @returns {bigint}
 */
export function defaultHttpResponseBytes() {
    const ret = wasm.defaultHttpResponseBytes();
    return BigInt.asUintN(64, ret);
}

/**
 * Encode one protobuf request into the hosted Stream HTTP JSON shape.
 *
 * Protobuf remains the only request contract crossing from TypeScript into
 * Rust.  Rust owns the conversion of uint64 values and opaque bytes to the
 * decimal and base64 spellings required by the hosted API, keeping the HTTP
 * adapter from maintaining a second scalar conversion table.
 * @param {string} route
 * @param {Uint8Array} input
 * @returns {string}
 */
export function encodeHttpRequest(route, input) {
    let deferred4_0;
    let deferred4_1;
    try {
        const ptr0 = passStringToWasm0(route, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.encodeHttpRequest(ptr0, len0, ptr1, len1);
        var ptr3 = ret[0];
        var len3 = ret[1];
        if (ret[3]) {
            ptr3 = 0; len3 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred4_0 = ptr3;
        deferred4_1 = len3;
        return getStringFromWasm0(ptr3, len3);
    } finally {
        wasm.__wbindgen_free(deferred4_0, deferred4_1, 1);
    }
}

/**
 * Encodes a merge-candidate payload in the versioned compatibility envelope.
 * @param {string} value_json
 * @returns {string}
 */
export function encodeMergeCandidateJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.encodeMergeCandidateJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Encodes a merge-plan payload in the versioned compatibility envelope.
 * @param {string} value_json
 * @returns {string}
 */
export function encodeMergePlanJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.encodeMergePlanJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Encodes a multi-root candidate payload in the compatibility envelope.
 * @param {string} value_json
 * @returns {string}
 */
export function encodeMultiRootCandidateJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.encodeMultiRootCandidateJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Encodes a multi-root plan payload in the versioned compatibility envelope.
 * @param {string} value_json
 * @returns {string}
 */
export function encodeMultiRootPlanJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.encodeMultiRootPlanJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Encodes a publication payload in the versioned compatibility envelope.
 * @param {string} value_json
 * @returns {string}
 */
export function encodePublicationJson(value_json) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(value_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.encodePublicationJson(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Return whether a code can be emitted by this WASM adapter.
 *
 * Keeping this validator beside the Rust error mapping prevents the TypeScript adapter from
 * maintaining a second, potentially stale list of base Stream error codes.
 * @param {string} value
 * @returns {boolean}
 */
export function is_stream_error_code(value) {
    const ptr0 = passStringToWasm0(value, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.is_stream_error_code(ptr0, len0);
    return ret !== 0;
}

/**
 * Validates one hosted read page and returns its canonical follow cursor.
 * @param {string} response_json
 * @param {bigint} from
 * @returns {bigint}
 */
export function nextHttpFollowCursor(response_json, from) {
    const ptr0 = passStringToWasm0(response_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.nextHttpFollowCursor(ptr0, len0, from);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return BigInt.asUintN(64, ret[0]);
}

/**
 * Normalize and encode canonical protobuf bytes for one commit request.
 *
 * The returned bytes use the same deterministic ordering as the in-memory
 * provider. Validation failures are thrown as stable error codes.
 * @param {Uint8Array} input
 * @returns {Uint8Array}
 */
export function normalizeCommitRequest(input) {
    const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.normalizeCommitRequest(ptr0, len0);
    if (ret[3]) {
        throw takeFromExternrefTable0(ret[2]);
    }
    var v2 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v2;
}

/**
 * Opens transactional browser storage with explicit `IndexedDB` or OPFS immutable objects.
 *
 * # Errors
 *
 * Returns a JavaScript error for invalid options or unavailable required storage.
 * @param {any} options
 * @returns {Promise<BrowserFs>}
 */
export function openBrowserFs(options) {
    const ret = wasm.openBrowserFs(options);
    return ret;
}

/**
 * Opens the deterministic process-local reference backend.
 *
 * # Errors
 *
 * Returns a JavaScript error when the memory options are invalid.
 * @param {any} options
 * @returns {BrowserFs}
 */
export function openMemoryFs(options) {
    const ret = wasm.openMemoryFs(options);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return BrowserFs.__wrap(ret[0]);
}

/**
 * Validates and projects one gRPC read response. Rust owns protobuf decoding,
 * record bounds, commit identity width, and request-relative contiguity.
 * @param {Uint8Array} input
 * @param {bigint} expected
 * @returns {unknown}
 */
export function projectGrpcReadResponse(input, expected) {
    const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.projectGrpcReadResponse(ptr0, len0, expected);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
}

/**
 * Decode one unary memory-provider response from canonical protobuf bytes
 * into the public JavaScript result shape. Rust owns the response oneofs,
 * scalar widths, copied byte buffers, and camelCase projection at this
 * boundary; TypeScript keeps only request adaptation and cursor lifecycle.
 * @param {string} operation
 * @param {Uint8Array} input
 * @returns {unknown}
 */
export function projectMemoryResponse(operation, input) {
    const ptr0 = passStringToWasm0(operation, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.projectMemoryResponse(ptr0, len0, ptr1, len1);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
}

/**
 * Project a hosted HTTP error code onto the public Stream error vocabulary.
 *
 * The hosted API may report either the Rust-owned wire code or a public alias.
 * Unknown values and a commit-only alias on another route return no value.
 * @param {string} raw
 * @param {string} route
 * @returns {string | undefined}
 */
export function publicHttpErrorCode(raw, route) {
    const ptr0 = passStringToWasm0(raw, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(route, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.publicHttpErrorCode(ptr0, len0, ptr1, len1);
    let v3;
    if (ret[0] !== 0) {
        v3 = getStringFromWasm0(ret[0], ret[1]).slice();
        wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    }
    return v3;
}

/**
 * Validate canonical protobuf bytes for one append request.
 *
 * The empty string means that the request passed the same domain validators as
 * the in-memory provider. Otherwise this returns one stable error code.
 * @param {Uint8Array} input
 * @returns {string}
 */
export function validateAppendRequest(input) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validateAppendRequest(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validates the bearer credential shared by the native and browser Stream
 * clients. The empty string means success; failures use a stable Rust-owned
 * invalid-argument boundary consumed by generated facades.
 * @param {string} token
 * @returns {string}
 */
export function validateBearerToken(token) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passStringToWasm0(token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validateBearerToken(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validates request-relative child-page semantics through the canonical Rust
 * provider rules before a public page reaches a TypeScript caller.
 * @param {Uint8Array} request
 * @param {Uint8Array} response
 */
export function validateChildrenPageResponse(request, response) {
    const ptr0 = passArray8ToWasm0(request, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(response, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.validateChildrenPageResponse(ptr0, len0, ptr1, len1);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates request-relative gRPC identities through the canonical wire
 * model. The adapter supplies only the expected identity bytes.
 * @param {string} operation
 * @param {Uint8Array} input
 * @param {Uint8Array} expected
 */
export function validateGrpcResponseIdentity(operation, input, expected) {
    const ptr0 = passStringToWasm0(operation, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ptr2 = passArray8ToWasm0(expected, wasm.__wbindgen_malloc);
    const len2 = WASM_VECTOR_LEN;
    const ret = wasm.validateGrpcResponseIdentity(ptr0, len0, ptr1, len1, ptr2, len2);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * @param {number} maximum_generations
 * @param {number} maximum_changes
 * @param {number} maximum_conflicts
 * @param {number} maximum_page_items
 */
export function validateHostedGenerationBounds(maximum_generations, maximum_changes, maximum_conflicts, maximum_page_items) {
    const ret = wasm.validateHostedGenerationBounds(maximum_generations, maximum_changes, maximum_conflicts, maximum_page_items);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * @param {Uint8Array} generation_id
 * @param {Uint8Array} owner_workspace_id
 * @param {Uint8Array} expected_workspace_id
 */
export function validateHostedGenerationIdentity(generation_id, owner_workspace_id, expected_workspace_id) {
    const ptr0 = passArray8ToWasm0(generation_id, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passArray8ToWasm0(owner_workspace_id, wasm.__wbindgen_malloc);
    const len1 = WASM_VECTOR_LEN;
    const ptr2 = passArray8ToWasm0(expected_workspace_id, wasm.__wbindgen_malloc);
    const len2 = WASM_VECTOR_LEN;
    const ret = wasm.validateHostedGenerationIdentity(ptr0, len0, ptr1, len1, ptr2, len2);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * @param {number} value
 * @param {number} maximum
 */
export function validateHostedPageBound(value, maximum) {
    const ret = wasm.validateHostedPageBound(value, maximum);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * @param {number} state
 * @param {number} reason
 * @param {boolean} has_generation
 */
export function validateHostedSourceState(state, reason, has_generation) {
    const ret = wasm.validateHostedSourceState(state, reason, has_generation);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * @param {number} mutation_count
 * @param {number} maximum_mutations
 * @param {number} maximum_conflicts
 * @param {number} maximum_page_items
 */
export function validateHostedTransactionBounds(mutation_count, maximum_mutations, maximum_conflicts, maximum_page_items) {
    const ret = wasm.validateHostedTransactionBounds(mutation_count, maximum_mutations, maximum_conflicts, maximum_page_items);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates the endpoint policy shared by native and browser HTTP clients.
 * HTTPS is required for hosted endpoints; HTTP is allowed only for loopback
 * fixture servers. The return value is empty for a valid endpoint.
 * @param {string} endpoint
 * @returns {string}
 */
export function validateHttpEndpoint(endpoint) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validateHttpEndpoint(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validate a hosted read page against the request cursor captured by the
 * caller. Rust owns record shape and cursor contiguity; the HTTP adapter only
 * supplies the response text and its request-relative starting position.
 * @param {string} response_json
 * @param {bigint} from
 */
export function validateHttpReadResponse(response_json, from) {
    const ptr0 = passStringToWasm0(response_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validateHttpReadResponse(ptr0, len0, from);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validate one hosted HTTP JSON success response using the same path, width,
 * identity, and tagged-union rules as the canonical Stream domain.
 *
 * The HTTP adapter keeps its intentionally simple JSON representation (u64
 * values are decimal strings and opaque bytes are base64). This entry point
 * validates that representation using the same Rust projection used by
 * `decodeHttpResponse`, without crossing a second scalar schema boundary.
 * @param {string} route
 * @param {string} response_json
 */
export function validateHttpResponse(route, response_json) {
    const ptr0 = passStringToWasm0(route, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(response_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.validateHttpResponse(ptr0, len0, ptr1, len1);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validate one caller retry identity through the canonical Stream model.
 * @param {Uint8Array} input
 * @returns {string}
 */
export function validateIdempotencyKey(input) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validateIdempotencyKey(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validate one canonical Stream path using the same parser used by every
 * provider and wire decoder.
 *
 * The empty string means success; failures use the stable Stream error code
 * consumed by the TypeScript adapter.
 * @param {string} path
 * @returns {string}
 */
export function validatePath(path) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validatePath(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validate one remote endpoint using the shared Rust policy.
 * @param {string} endpoint
 */
export function validateRemoteWebEndpoint(endpoint) {
    const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validateRemoteWebEndpoint(ptr0, len0);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates an encoded Filesystem handshake response and returns the
 * canonical negotiated capabilities. Keeping decoding and admission in
 * this Rust boundary prevents hosted TypeScript clients from re-owning
 * protocol identity or capability validation.
 * @param {Uint8Array} response
 * @returns {Uint8Array}
 */
export function validateRemoteWebFilesystemHandshake(response) {
    const ptr0 = passArray8ToWasm0(response, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validateRemoteWebFilesystemHandshake(ptr0, len0);
    if (ret[3]) {
        throw takeFromExternrefTable0(ret[2]);
    }
    var v2 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
    wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
    return v2;
}

/**
 * Validate one remote gRPC endpoint using the shared Rust policy.
 * @param {string} endpoint
 */
export function validateRemoteWebGrpcEndpoint(endpoint) {
    const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validateRemoteWebGrpcEndpoint(ptr0, len0);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validate one canonical protobuf request at the browser boundary.
 *
 * `kind` is deliberately a small closed set so callers cannot accidentally
 * select a different validator after adding a new wire message. The empty
 * string means success; failures use the same stable codes as the append and
 * commit entry points.
 * @param {string} kind
 * @param {Uint8Array} input
 * @returns {string}
 */
export function validateRequest(kind, input) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passStringToWasm0(kind, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.validateRequest(ptr0, len0, ptr1, len1);
        deferred3_0 = ret[0];
        deferred3_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

/**
 * Validate one JavaScript representation of a canonical Stream sequence.
 *
 * JavaScript passes the decimal spelling of its `bigint`; Rust owns the
 * unsigned 64-bit range accepted by every Stream wire field.
 * @param {string} value
 * @returns {string}
 */
export function validateSequence(value) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passStringToWasm0(value, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validateSequence(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validates the Rust Actors invoke request admission rules.
 * @param {string} actor_id
 * @param {string} method
 */
export function validate_actors_invoke(actor_id, method) {
    const ptr0 = passStringToWasm0(actor_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(method, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.validate_actors_invoke(ptr0, len0, ptr1, len1);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validate the opaque commit identity used by Stream responses and requests.
 * The empty string means success; malformed identities use the canonical
 * invalid-argument boundary consumed by generated facades.
 * @param {Uint8Array} input
 * @returns {string}
 */
export function validate_commit_id(input) {
    let deferred2_0;
    let deferred2_1;
    try {
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.validate_commit_id(ptr0, len0);
        deferred2_0 = ret[0];
        deferred2_1 = ret[1];
        return getStringFromWasm0(ret[0], ret[1]);
    } finally {
        wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
    }
}

/**
 * Validates the optional native TLS CA certificate before it reaches the
 * platform gRPC adapter.
 * @param {string} certificate
 */
export function validate_remote_web_ca_certificate(certificate) {
    const ptr0 = passStringToWasm0(certificate, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validate_remote_web_ca_certificate(ptr0, len0);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Checks an HTTP content-length without first narrowing it through a JS number.
 * @param {string} content_length
 * @param {bigint} maximum
 */
export function validate_remote_web_content_length(content_length, maximum) {
    const ptr0 = passStringToWasm0(content_length, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validate_remote_web_content_length(ptr0, len0, maximum);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validate one remote bearer credential using the shared Rust policy.
 * @param {string} token
 */
export function validate_remote_web_credential(token) {
    const ptr0 = passStringToWasm0(token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validate_remote_web_credential(ptr0, len0);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates the HTTPS or loopback-HTTP endpoint shared by Actors and Workers.
 * @param {string} endpoint
 */
export function validate_remote_web_endpoint(endpoint) {
    const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validate_remote_web_endpoint(ptr0, len0);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates the HTTPS endpoint required by native gRPC transports.
 * @param {string} endpoint
 */
export function validate_remote_web_grpc_endpoint(endpoint) {
    const ptr0 = passStringToWasm0(endpoint, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.validate_remote_web_grpc_endpoint(ptr0, len0);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates the configured native gRPC message bound.
 * @param {bigint} maximum
 */
export function validate_remote_web_message_limit(maximum) {
    const ret = wasm.validate_remote_web_message_limit(maximum);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Advances a cumulative response byte count under the caller's configured bound.
 * @param {bigint} observed
 * @param {bigint} chunk
 * @param {bigint} maximum
 * @returns {bigint}
 */
export function validate_remote_web_response_chunk(observed, chunk, maximum) {
    const ret = wasm.validate_remote_web_response_chunk(observed, chunk, maximum);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return BigInt.asUintN(64, ret[0]);
}

/**
 * Validates the configured cumulative response bound.
 * @param {bigint} maximum
 */
export function validate_remote_web_response_limit(maximum) {
    const ret = wasm.validate_remote_web_response_limit(maximum);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates the Rust Workers invoke-deployment path and request admission rules.
 * @param {string} alias
 * @param {string} method
 */
export function validate_workers_invoke_deployment(alias, method) {
    const ptr0 = passStringToWasm0(alias, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(method, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.validate_workers_invoke_deployment(ptr0, len0, ptr1, len1);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

/**
 * Validates the Rust Workers invoke-version path and request admission rules.
 * @param {Uint8Array} version_sha256
 * @param {string} method
 */
export function validate_workers_invoke_version(version_sha256, method) {
    const ptr0 = passArray8ToWasm0(version_sha256, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ptr1 = passStringToWasm0(method, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    const ret = wasm.validate_workers_invoke_version(ptr0, len0, ptr1, len1);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg_Error_2e59b1b37a9a34c3: function(arg0, arg1) {
            const ret = Error(getStringFromWasm0(arg0, arg1));
            return ret;
        },
        __wbg_Number_e6ffdb596c888833: function(arg0) {
            const ret = Number(arg0);
            return ret;
        },
        __wbg_String_8564e559799eccda: function(arg0, arg1) {
            const ret = String(arg1);
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_Window_70131fc0c91e4b3c: function(arg0) {
            const ret = arg0.Window;
            return ret;
        },
        __wbg_WorkerGlobalScope_601c48015b8cc78e: function(arg0) {
            const ret = arg0.WorkerGlobalScope;
            return ret;
        },
        __wbg___wbindgen_bigint_get_as_i64_2c5082002e4826e2: function(arg0, arg1) {
            const v = arg1;
            const ret = typeof(v) === 'bigint' ? v : undefined;
            getDataViewMemory0().setBigInt64(arg0 + 8 * 1, isLikeNone(ret) ? BigInt(0) : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_boolean_get_a86c216575a75c30: function(arg0) {
            const v = arg0;
            const ret = typeof(v) === 'boolean' ? v : undefined;
            return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
        },
        __wbg___wbindgen_debug_string_dd5d2d07ce9e6c57: function(arg0, arg1) {
            const ret = debugString(arg1);
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_in_4bd7a57e54337366: function(arg0, arg1) {
            const ret = arg0 in arg1;
            return ret;
        },
        __wbg___wbindgen_is_bigint_6c98f7e945dacdde: function(arg0) {
            const ret = typeof(arg0) === 'bigint';
            return ret;
        },
        __wbg___wbindgen_is_function_49868bde5eb1e745: function(arg0) {
            const ret = typeof(arg0) === 'function';
            return ret;
        },
        __wbg___wbindgen_is_null_344c8750a8525473: function(arg0) {
            const ret = arg0 === null;
            return ret;
        },
        __wbg___wbindgen_is_object_40c5a80572e8f9d3: function(arg0) {
            const val = arg0;
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbg___wbindgen_is_string_b29b5c5a8065ba1a: function(arg0) {
            const ret = typeof(arg0) === 'string';
            return ret;
        },
        __wbg___wbindgen_is_undefined_c0cca72b82b86f4d: function(arg0) {
            const ret = arg0 === undefined;
            return ret;
        },
        __wbg___wbindgen_jsval_eq_7d430e744a913d26: function(arg0, arg1) {
            const ret = arg0 === arg1;
            return ret;
        },
        __wbg___wbindgen_jsval_loose_eq_3a72ae764d46d944: function(arg0, arg1) {
            const ret = arg0 == arg1;
            return ret;
        },
        __wbg___wbindgen_number_get_7579aab02a8a620c: function(arg0, arg1) {
            const obj = arg1;
            const ret = typeof(obj) === 'number' ? obj : undefined;
            getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_string_get_914df97fcfa788f2: function(arg0, arg1) {
            const obj = arg1;
            const ret = typeof(obj) === 'string' ? obj : undefined;
            var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_throw_81fc77679af83bc6: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbg__wbg_cb_unref_3c3b4f651835fbcb: function(arg0) {
            arg0._wbg_cb_unref();
        },
        __wbg_abort_5ee4083ce26e0b01: function(arg0) {
            arg0.abort();
        },
        __wbg_abort_7a67cb8f9383baa1: function(arg0, arg1) {
            arg0.abort(arg1);
        },
        __wbg_abort_cfab06d16d2d33a6: function() { return handleError(function (arg0) {
            arg0.abort();
        }, arguments); },
        __wbg_add_92106df461c103cf: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.add(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_append_c015600138ae60bb: function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
            arg0.append(getStringFromWasm0(arg1, arg2), getStringFromWasm0(arg3, arg4));
        }, arguments); },
        __wbg_arrayBuffer_7bba74066875530e: function(arg0) {
            const ret = arg0.arrayBuffer();
            return ret;
        },
        __wbg_body_9a25d64338506fbe: function(arg0) {
            const ret = arg0.body;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_bound_88db1072ea68e901: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            const ret = IDBKeyRange.bound(arg0, arg1, arg2 !== 0, arg3 !== 0);
            return ret;
        }, arguments); },
        __wbg_browserchangeset_new: function(arg0) {
            const ret = BrowserChangeSet.__wrap(arg0);
            return ret;
        },
        __wbg_browsercheckout_new: function(arg0) {
            const ret = BrowserCheckout.__wrap(arg0);
            return ret;
        },
        __wbg_browserfilesystemclient_new: function(arg0) {
            const ret = BrowserFilesystemClient.__wrap(arg0);
            return ret;
        },
        __wbg_browserfs_new: function(arg0) {
            const ret = BrowserFs.__wrap(arg0);
            return ret;
        },
        __wbg_browsergeneration_new: function(arg0) {
            const ret = BrowserGeneration.__wrap(arg0);
            return ret;
        },
        __wbg_browserharnessclient_new: function(arg0) {
            const ret = BrowserHarnessClient.__wrap(arg0);
            return ret;
        },
        __wbg_browserjoinplan_new: function(arg0) {
            const ret = BrowserJoinPlan.__wrap(arg0);
            return ret;
        },
        __wbg_browserresolvedfiles_new: function(arg0) {
            const ret = BrowserResolvedFiles.__wrap(arg0);
            return ret;
        },
        __wbg_browsertransaction_new: function(arg0) {
            const ret = BrowserTransaction.__wrap(arg0);
            return ret;
        },
        __wbg_browservolume_new: function(arg0) {
            const ret = BrowserVolume.__wrap(arg0);
            return ret;
        },
        __wbg_browserworkspace_new: function(arg0) {
            const ret = BrowserWorkspace.__wrap(arg0);
            return ret;
        },
        __wbg_buffer_a77cc90da4bdb503: function(arg0) {
            const ret = arg0.buffer;
            return ret;
        },
        __wbg_byobRequest_41018d4f78e8364a: function(arg0) {
            const ret = arg0.byobRequest;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_byteLength_f5c761c4472de41b: function(arg0) {
            const ret = arg0.byteLength;
            return ret;
        },
        __wbg_byteOffset_7cd0c9f7e573845c: function(arg0) {
            const ret = arg0.byteOffset;
            return ret;
        },
        __wbg_call_7f2987183bb62793: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.call(arg1);
            return ret;
        }, arguments); },
        __wbg_call_d578befcc3145dee: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.call(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_cancel_8314206ca10b9605: function(arg0) {
            const ret = arg0.cancel();
            return ret;
        },
        __wbg_catch_32d296b856e661d9: function(arg0, arg1) {
            const ret = arg0.catch(arg1);
            return ret;
        },
        __wbg_clearTimeout_fa0abacb8606b0a7: function(arg0) {
            const ret = clearTimeout(arg0);
            return ret;
        },
        __wbg_close_37e34297940956fd: function(arg0) {
            const ret = arg0.close();
            return ret;
        },
        __wbg_close_9998ebb2f4515c42: function() { return handleError(function (arg0) {
            arg0.close();
        }, arguments); },
        __wbg_close_a7c33e895baeb773: function() { return handleError(function (arg0) {
            arg0.close();
        }, arguments); },
        __wbg_commit_e9c1332714c53826: function() { return handleError(function (arg0) {
            arg0.commit();
        }, arguments); },
        __wbg_createObjectStore_11c03f9eac3c3672: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.createObjectStore(getStringFromWasm0(arg1, arg2));
            return ret;
        }, arguments); },
        __wbg_createWritable_d5314165379c13be: function(arg0) {
            const ret = arg0.createWritable();
            return ret;
        },
        __wbg_create_afd4db08c8ba3078: function(arg0) {
            const ret = Object.create(arg0);
            return ret;
        },
        __wbg_done_547d467e97529006: function(arg0) {
            const ret = arg0.done;
            return ret;
        },
        __wbg_enqueue_bb06efec8458a81a: function() { return handleError(function (arg0, arg1) {
            arg0.enqueue(arg1);
        }, arguments); },
        __wbg_entries_616b1a459b85be0b: function(arg0) {
            const ret = Object.entries(arg0);
            return ret;
        },
        __wbg_error_58469b8474e13592: function() { return handleError(function (arg0) {
            const ret = arg0.error;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        }, arguments); },
        __wbg_fetch_37b5c90ecd638071: function(arg0, arg1) {
            const ret = fetch(arg0, arg1);
            return ret;
        },
        __wbg_fetch_a920260d19124083: function(arg0, arg1, arg2) {
            const ret = arg0.fetch(arg1, arg2);
            return ret;
        },
        __wbg_from_741da0f916ab74aa: function(arg0) {
            const ret = Array.from(arg0);
            return ret;
        },
        __wbg_getDirectoryHandle_a38f7b2c1aa52af4: function(arg0, arg1, arg2, arg3) {
            const ret = arg0.getDirectoryHandle(getStringFromWasm0(arg1, arg2), arg3);
            return ret;
        },
        __wbg_getDirectory_3af764c18446017f: function(arg0) {
            const ret = arg0.getDirectory();
            return ret;
        },
        __wbg_getFileHandle_029e7a3c6dee72cb: function(arg0, arg1, arg2) {
            const ret = arg0.getFileHandle(getStringFromWasm0(arg1, arg2));
            return ret;
        },
        __wbg_getFileHandle_326ca47811ae37a1: function(arg0, arg1, arg2, arg3) {
            const ret = arg0.getFileHandle(getStringFromWasm0(arg1, arg2), arg3);
            return ret;
        },
        __wbg_getFile_0e25dfe508c6bd0a: function(arg0) {
            const ret = arg0.getFile();
            return ret;
        },
        __wbg_getRandomValues_3f44b700395062e5: function() { return handleError(function (arg0, arg1) {
            globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
        }, arguments); },
        __wbg_getRandomValues_85bb9ec093ba4400: function() { return handleError(function (arg0, arg1) {
            globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
        }, arguments); },
        __wbg_getReader_bf34f92ee19c42b4: function() { return handleError(function (arg0) {
            const ret = arg0.getReader();
            return ret;
        }, arguments); },
        __wbg_getTime_f6ac312467f7cf09: function(arg0) {
            const ret = arg0.getTime();
            return ret;
        },
        __wbg_get_4848e350b40afc16: function(arg0, arg1) {
            const ret = arg0[arg1 >>> 0];
            return ret;
        },
        __wbg_get_560cb483e5c0133e: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.get(arg1);
            return ret;
        }, arguments); },
        __wbg_get_done_46f064515323cd59: function(arg0) {
            const ret = arg0.done;
            return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
        },
        __wbg_get_ed0642c4b9d31ddf: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(arg0, arg1);
            return ret;
        }, arguments); },
        __wbg_get_f96702c6245e4ef9: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(arg0, arg1);
            return ret;
        }, arguments); },
        __wbg_get_unchecked_7d7babe32e9e6a54: function(arg0, arg1) {
            const ret = arg0[arg1 >>> 0];
            return ret;
        },
        __wbg_get_value_28b7deb73d356d88: function(arg0) {
            const ret = arg0.value;
            return ret;
        },
        __wbg_get_with_ref_key_6412cf3094599694: function(arg0, arg1) {
            const ret = arg0[arg1];
            return ret;
        },
        __wbg_global_e30ac0b7684506d0: function(arg0) {
            const ret = arg0.global;
            return ret;
        },
        __wbg_has_3ec5c22db2e5237a: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.has(arg0, arg1);
            return ret;
        }, arguments); },
        __wbg_headers_e08dcb5aa09b9a63: function(arg0) {
            const ret = arg0.headers;
            return ret;
        },
        __wbg_indexedDB_065ce3ad400579e3: function() { return handleError(function (arg0) {
            const ret = arg0.indexedDB;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        }, arguments); },
        __wbg_indexedDB_a2139150e2ea2a08: function() { return handleError(function (arg0) {
            const ret = arg0.indexedDB;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        }, arguments); },
        __wbg_indexedDB_af74cb6df65fa636: function() { return handleError(function (arg0) {
            const ret = arg0.indexedDB;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        }, arguments); },
        __wbg_instanceof_ArrayBuffer_ff7c1337a5e3b33a: function(arg0) {
            let result;
            try {
                result = arg0 instanceof ArrayBuffer;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Blob_6b3922471f5ba34c: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Blob;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_DomException_37f96d3fb69189bd: function(arg0) {
            let result;
            try {
                result = arg0 instanceof DOMException;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Error_e3390d6805733dad: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Error;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_FileSystemDirectoryHandle_66b8b1a90ca7b685: function(arg0) {
            let result;
            try {
                result = arg0 instanceof FileSystemDirectoryHandle;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_FileSystemFileHandle_2236115c7caa5120: function(arg0) {
            let result;
            try {
                result = arg0 instanceof FileSystemFileHandle;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_FileSystemWritableFileStream_4854d3930b45b7de: function(arg0) {
            let result;
            try {
                result = arg0 instanceof FileSystemWritableFileStream;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_File_f48a30500b43b096: function(arg0) {
            let result;
            try {
                result = arg0 instanceof File;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_IdbDatabase_0af111edb4be95f4: function(arg0) {
            let result;
            try {
                result = arg0 instanceof IDBDatabase;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_IdbRequest_fc5918c726448f04: function(arg0) {
            let result;
            try {
                result = arg0 instanceof IDBRequest;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Map_a10a2795ef4bfe97: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Map;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Uint8Array_4b8da683deb25d72: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Uint8Array;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Window_c0fee4c064502536: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Window;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_isArray_db61795ad004c139: function(arg0) {
            const ret = Array.isArray(arg0);
            return ret;
        },
        __wbg_isSafeInteger_ea83862ba994770c: function(arg0) {
            const ret = Number.isSafeInteger(arg0);
            return ret;
        },
        __wbg_iterator_de403ef31815a3e6: function() {
            const ret = Symbol.iterator;
            return ret;
        },
        __wbg_length_0c32cb8543c8e4c8: function(arg0) {
            const ret = arg0.length;
            return ret;
        },
        __wbg_length_6e821edde497a532: function(arg0) {
            const ret = arg0.length;
            return ret;
        },
        __wbg_lowerBound_279c232a69ac0f79: function() { return handleError(function (arg0, arg1) {
            const ret = IDBKeyRange.lowerBound(arg0, arg1 !== 0);
            return ret;
        }, arguments); },
        __wbg_message_52a9425f28c45ebc: function(arg0, arg1) {
            const ret = arg1.message;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_message_7367f8c7d0fa1589: function(arg0) {
            const ret = arg0.message;
            return ret;
        },
        __wbg_name_d7bb38b41d6d953e: function(arg0, arg1) {
            const ret = arg1.name;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_navigator_9b09ea705d03d227: function(arg0) {
            const ret = arg0.navigator;
            return ret;
        },
        __wbg_new_0f6d2ddfe083319b: function(arg0) {
            const ret = new Date(arg0);
            return ret;
        },
        __wbg_new_3a112826a89cb962: function() { return handleError(function () {
            const ret = new Headers();
            return ret;
        }, arguments); },
        __wbg_new_4f9fafbb3909af72: function() {
            const ret = new Object();
            return ret;
        },
        __wbg_new_9abbf7148481485e: function() { return handleError(function () {
            const ret = new AbortController();
            return ret;
        }, arguments); },
        __wbg_new_a560378ea1240b14: function(arg0) {
            const ret = new Uint8Array(arg0);
            return ret;
        },
        __wbg_new_e3b04b4d53d1b593: function(arg0, arg1) {
            const ret = new Error(getStringFromWasm0(arg0, arg1));
            return ret;
        },
        __wbg_new_f3c9df4f38f3f798: function() {
            const ret = new Array();
            return ret;
        },
        __wbg_new_from_slice_2580ff33d0d10520: function(arg0, arg1) {
            const ret = new Uint8Array(getArrayU8FromWasm0(arg0, arg1));
            return ret;
        },
        __wbg_new_typed_14d7cc391ce53d2c: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined_______true_(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return ret;
            } finally {
                state0.a = 0;
            }
        },
        __wbg_new_with_byte_offset_and_length_6bfc75833d6170c8: function(arg0, arg1, arg2) {
            const ret = new Uint8Array(arg0, arg1 >>> 0, arg2 >>> 0);
            return ret;
        },
        __wbg_new_with_str_and_init_f663b6d334baa878: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = new Request(getStringFromWasm0(arg0, arg1), arg2);
            return ret;
        }, arguments); },
        __wbg_new_with_u8_array_sequence_2ae9f5628c4df63c: function() { return handleError(function (arg0) {
            const ret = new Blob(arg0);
            return ret;
        }, arguments); },
        __wbg_next_01132ed6134b8ef5: function(arg0) {
            const ret = arg0.next;
            return ret;
        },
        __wbg_next_b3713ec761a9dbfd: function() { return handleError(function (arg0) {
            const ret = arg0.next();
            return ret;
        }, arguments); },
        __wbg_now_2ef92f9e3342b41f: function() { return handleError(function () {
            const ret = Date.now();
            return ret;
        }, arguments); },
        __wbg_now_88621c9c9a4f3ffc: function() {
            const ret = Date.now();
            return ret;
        },
        __wbg_objectStore_3d4cade4416cd432: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.objectStore(getStringFromWasm0(arg1, arg2));
            return ret;
        }, arguments); },
        __wbg_oldVersion_f2860d32ce6f6bd7: function(arg0) {
            const ret = arg0.oldVersion;
            return ret;
        },
        __wbg_open_ac04ec9d75d0eeaf: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            const ret = arg0.open(getStringFromWasm0(arg1, arg2), arg3 >>> 0);
            return ret;
        }, arguments); },
        __wbg_prototypesetcall_3e05eb9545565046: function(arg0, arg1, arg2) {
            Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
        },
        __wbg_push_6bdbc990be5ac37b: function(arg0, arg1) {
            const ret = arg0.push(arg1);
            return ret;
        },
        __wbg_put_4485a4012273f7ef: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.put(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_queueMicrotask_abaf92f0bd4e80a4: function(arg0) {
            const ret = arg0.queueMicrotask;
            return ret;
        },
        __wbg_queueMicrotask_df5a6dac26d818f3: function(arg0) {
            queueMicrotask(arg0);
        },
        __wbg_read_316bf844c93a6ccc: function(arg0) {
            const ret = arg0.read();
            return ret;
        },
        __wbg_readyState_accbdf425c074d9c: function(arg0) {
            const ret = arg0.readyState;
            return (__wbindgen_enum_IdbRequestReadyState.indexOf(ret) + 1 || 3) - 1;
        },
        __wbg_releaseLock_b12f7593852f4c01: function(arg0) {
            arg0.releaseLock();
        },
        __wbg_resolve_0a79de24e9d2267b: function(arg0) {
            const ret = Promise.resolve(arg0);
            return ret;
        },
        __wbg_respond_87b5b47cbc9ee0b7: function() { return handleError(function (arg0, arg1) {
            arg0.respond(arg1 >>> 0);
        }, arguments); },
        __wbg_result_452c1006fc727317: function() { return handleError(function (arg0) {
            const ret = arg0.result;
            return ret;
        }, arguments); },
        __wbg_setTimeout_9c23a5767d86152f: function(arg0, arg1) {
            const ret = setTimeout(arg0, arg1);
            return ret;
        },
        __wbg_set_16a9c1a07b3d38ec: function(arg0, arg1, arg2) {
            arg0.set(getArrayU8FromWasm0(arg1, arg2));
        },
        __wbg_set_6be42768c690e380: function(arg0, arg1, arg2) {
            arg0[arg1] = arg2;
        },
        __wbg_set_6c60b2e8ad0e9383: function(arg0, arg1, arg2) {
            arg0[arg1 >>> 0] = arg2;
        },
        __wbg_set_8ee2d34facb8466e: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.set(arg0, arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_set_aa391f3af1ff0e9c: function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
            arg0.set(getStringFromWasm0(arg1, arg2), getStringFromWasm0(arg3, arg4));
        }, arguments); },
        __wbg_set_body_a304d09cb50cefbe: function(arg0, arg1) {
            arg0.body = arg1;
        },
        __wbg_set_cache_cc687e2b96e9608c: function(arg0, arg1) {
            arg0.cache = __wbindgen_enum_RequestCache[arg1];
        },
        __wbg_set_create_0654e513e8ccb2be: function(arg0, arg1) {
            arg0.create = arg1 !== 0;
        },
        __wbg_set_create_4b5cddb7e7c14744: function(arg0, arg1) {
            arg0.create = arg1 !== 0;
        },
        __wbg_set_credentials_7693e63055f5e838: function(arg0, arg1) {
            arg0.credentials = __wbindgen_enum_RequestCredentials[arg1];
        },
        __wbg_set_headers_6ab1105e542834e2: function(arg0, arg1) {
            arg0.headers = arg1;
        },
        __wbg_set_integrity_0dcf8feff0f7e4d3: function(arg0, arg1, arg2) {
            arg0.integrity = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_method_1971272fe557e972: function(arg0, arg1, arg2) {
            arg0.method = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_mode_d1b643087602281a: function(arg0, arg1) {
            arg0.mode = __wbindgen_enum_RequestMode[arg1];
        },
        __wbg_set_name_ab9c98596fd7310a: function(arg0, arg1, arg2) {
            arg0.name = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_onabort_6b6df7a41aa97c23: function(arg0, arg1) {
            arg0.onabort = arg1;
        },
        __wbg_set_oncomplete_20fb27150b4ee0d4: function(arg0, arg1) {
            arg0.oncomplete = arg1;
        },
        __wbg_set_onerror_2b7dfa4e6dea4159: function(arg0, arg1) {
            arg0.onerror = arg1;
        },
        __wbg_set_onerror_3c4b5087146b11b6: function(arg0, arg1) {
            arg0.onerror = arg1;
        },
        __wbg_set_onsuccess_f7e5b5cbed5008b1: function(arg0, arg1) {
            arg0.onsuccess = arg1;
        },
        __wbg_set_onupgradeneeded_d7e8e03a1999bf5d: function(arg0, arg1) {
            arg0.onupgradeneeded = arg1;
        },
        __wbg_set_redirect_1d6df2c80bf90934: function(arg0, arg1) {
            arg0.redirect = __wbindgen_enum_RequestRedirect[arg1];
        },
        __wbg_set_referrer_c0d0ce1b4c972d99: function(arg0, arg1, arg2) {
            arg0.referrer = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_referrer_policy_48f3f15e75682149: function(arg0, arg1) {
            arg0.referrerPolicy = __wbindgen_enum_ReferrerPolicy[arg1];
        },
        __wbg_set_signal_8564a226c5c6853c: function(arg0, arg1) {
            arg0.signal = arg1;
        },
        __wbg_signal_9172c3282bfba2f5: function(arg0) {
            const ret = arg0.signal;
            return ret;
        },
        __wbg_size_7306c9406e13bf29: function(arg0) {
            const ret = arg0.size;
            return ret;
        },
        __wbg_static_accessor_GLOBAL_THIS_a1248013d790bf5f: function() {
            const ret = typeof globalThis === 'undefined' ? null : globalThis;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_GLOBAL_f2e0f995a21329ff: function() {
            const ret = typeof global === 'undefined' ? null : global;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_SELF_24f78b6d23f286ea: function() {
            const ret = typeof self === 'undefined' ? null : self;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_WINDOW_59fd959c540fe405: function() {
            const ret = typeof window === 'undefined' ? null : window;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_status_44ecb0ac1da253f4: function(arg0) {
            const ret = arg0.status;
            return ret;
        },
        __wbg_storage_8f8e63186ec77353: function(arg0) {
            const ret = arg0.storage;
            return ret;
        },
        __wbg_target_732d56b173b7e87c: function(arg0) {
            const ret = arg0.target;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_then_00eed3ac0b8e82cb: function(arg0, arg1, arg2) {
            const ret = arg0.then(arg1, arg2);
            return ret;
        },
        __wbg_then_a0c8db0381c8994c: function(arg0, arg1) {
            const ret = arg0.then(arg1);
            return ret;
        },
        __wbg_toString_891d991e862e1d44: function(arg0) {
            const ret = arg0.toString();
            return ret;
        },
        __wbg_transaction_16a3adcdaf3724fd: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.transaction(getStringFromWasm0(arg1, arg2));
            return ret;
        }, arguments); },
        __wbg_transaction_913366b438022b35: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.transaction(arg1);
            return ret;
        }, arguments); },
        __wbg_transaction_cc65dcef07fabb06: function(arg0) {
            const ret = arg0.transaction;
            return ret;
        },
        __wbg_transaction_cf424de4566e417b: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            const ret = arg0.transaction(arg1, __wbindgen_enum_IdbTransactionMode[arg2], arg3);
            return ret;
        }, arguments); },
        __wbg_transaction_d3d20e99057e252e: function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
            const ret = arg0.transaction(getStringFromWasm0(arg1, arg2), __wbindgen_enum_IdbTransactionMode[arg3], arg4);
            return ret;
        }, arguments); },
        __wbg_upperBound_2f3c07fb628c7a3c: function() { return handleError(function (arg0, arg1) {
            const ret = IDBKeyRange.upperBound(arg0, arg1 !== 0);
            return ret;
        }, arguments); },
        __wbg_value_7f6052747ccf940f: function(arg0) {
            const ret = arg0.value;
            return ret;
        },
        __wbg_view_066479053a371038: function(arg0) {
            const ret = arg0.view;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_wasmfollow_new: function(arg0) {
            const ret = WasmFollow.__wrap(arg0);
            return ret;
        },
        __wbg_write_fc53b37dcc29642e: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.write(getArrayU8FromWasm0(arg1, arg2));
            return ret;
        }, arguments); },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [Externref], shim_idx: 1348, ret: Result(Unit), inner_ret: Some(Result(Unit)) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsError___true_);
            return ret;
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [Externref], shim_idx: 968, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue______true_);
            return ret;
        },
        __wbindgen_cast_0000000000000003: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("Event")], shim_idx: 1011, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_Event__Event______true_);
            return ret;
        },
        __wbindgen_cast_0000000000000004: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("IDBVersionChangeEvent")], shim_idx: 17, ret: Result(Unit), inner_ret: Some(Result(Unit)) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_IdbVersionChangeEvent__IdbVersionChangeEvent__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsValue___true_);
            return ret;
        },
        __wbindgen_cast_0000000000000005: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [], shim_idx: 1010, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true__1_);
            return ret;
        },
        __wbindgen_cast_0000000000000006: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [], shim_idx: 918, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true_);
            return ret;
        },
        __wbindgen_cast_0000000000000007: function(arg0) {
            // Cast intrinsic for `F64 -> Externref`.
            const ret = arg0;
            return ret;
        },
        __wbindgen_cast_0000000000000008: function(arg0) {
            // Cast intrinsic for `I64 -> Externref`.
            const ret = arg0;
            return ret;
        },
        __wbindgen_cast_0000000000000009: function(arg0, arg1) {
            // Cast intrinsic for `Ref(Slice(U8)) -> NamedExternref("Uint8Array")`.
            const ret = getArrayU8FromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_cast_000000000000000a: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_cast_000000000000000b: function(arg0) {
            // Cast intrinsic for `U64 -> Externref`.
            const ret = BigInt.asUintN(64, arg0);
            return ret;
        },
        __wbindgen_cast_000000000000000c: function(arg0, arg1) {
            var v0 = getArrayU8FromWasm0(arg0, arg1).slice();
            wasm.__wbindgen_free(arg0, arg1 * 1, 1);
            // Cast intrinsic for `Vector(U8) -> Externref`.
            const ret = v0;
            return ret;
        },
        __wbindgen_init_externref_table: function() {
            const table = wasm.__wbindgen_externrefs;
            const offset = table.grow(4);
            table.set(0, undefined);
            table.set(offset + 0, undefined);
            table.set(offset + 1, null);
            table.set(offset + 2, true);
            table.set(offset + 3, false);
        },
    };
    return {
        __proto__: null,
        "./acyclic_fs_wasm_bg.js": import0,
    };
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true__1_(arg0, arg1) {
    wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true__1_(arg0, arg1);
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true_(arg0, arg1) {
    wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke_______true_(arg0, arg1);
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue______true_(arg0, arg1, arg2) {
    wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue______true_(arg0, arg1, arg2);
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_Event__Event______true_(arg0, arg1, arg2) {
    wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_Event__Event______true_(arg0, arg1, arg2);
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsError___true_(arg0, arg1, arg2) {
    const ret = wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___wasm_bindgen_94fa5eb15954fe4d___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsError___true_(arg0, arg1, arg2);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_IdbVersionChangeEvent__IdbVersionChangeEvent__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsValue___true_(arg0, arg1, arg2) {
    const ret = wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___web_sys_cbfa82ad1bbe2c35___features__gen_IdbVersionChangeEvent__IdbVersionChangeEvent__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_94fa5eb15954fe4d___JsValue___true_(arg0, arg1, arg2);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

function wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined_______true_(arg0, arg1, arg2, arg3) {
    wasm.wasm_bindgen_94fa5eb15954fe4d___convert__closures_____invoke___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined___js_sys_8b25c55417977075___Function_fn_wasm_bindgen_94fa5eb15954fe4d___JsValue_____wasm_bindgen_94fa5eb15954fe4d___sys__Undefined_______true_(arg0, arg1, arg2, arg3);
}


const __wbindgen_enum_IdbRequestReadyState = ["pending", "done"];


const __wbindgen_enum_IdbTransactionMode = ["readonly", "readwrite", "versionchange", "readwriteflush", "cleanup"];


const __wbindgen_enum_ReadableStreamType = ["bytes"];


const __wbindgen_enum_ReferrerPolicy = ["", "no-referrer", "no-referrer-when-downgrade", "origin", "origin-when-cross-origin", "unsafe-url", "same-origin", "strict-origin", "strict-origin-when-cross-origin"];


const __wbindgen_enum_RequestCache = ["default", "no-store", "reload", "no-cache", "force-cache", "only-if-cached"];


const __wbindgen_enum_RequestCredentials = ["omit", "same-origin", "include"];


const __wbindgen_enum_RequestMode = ["same-origin", "no-cors", "cors", "navigate"];


const __wbindgen_enum_RequestRedirect = ["follow", "error", "manual"];
const BrowserChangeSetFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserchangeset_free(ptr >>> 0, 1));
const BrowserCheckoutFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browsercheckout_free(ptr >>> 0, 1));
const BrowserFilesystemClientFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserfilesystemclient_free(ptr >>> 0, 1));
const BrowserFsFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserfs_free(ptr >>> 0, 1));
const BrowserGenerationFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browsergeneration_free(ptr >>> 0, 1));
const BrowserGitCompatRepositoryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browsergitcompatrepository_free(ptr >>> 0, 1));
const BrowserHarnessClientFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserharnessclient_free(ptr >>> 0, 1));
const BrowserJoinPlanFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserjoinplan_free(ptr >>> 0, 1));
const BrowserResolvedFileFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserresolvedfile_free(ptr >>> 0, 1));
const BrowserResolvedFilesFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserresolvedfiles_free(ptr >>> 0, 1));
const BrowserSpeculationFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserspeculation_free(ptr >>> 0, 1));
const BrowserTransactionFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browsertransaction_free(ptr >>> 0, 1));
const BrowserVolumeFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browservolume_free(ptr >>> 0, 1));
const BrowserWorkspaceFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserworkspace_free(ptr >>> 0, 1));
const BrowserWorkspaceContextRegistryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_browserworkspacecontextregistry_free(ptr >>> 0, 1));
const HttpFollowCursorFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_httpfollowcursor_free(ptr >>> 0, 1));
const IntoUnderlyingByteSourceFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_intounderlyingbytesource_free(ptr >>> 0, 1));
const IntoUnderlyingSinkFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_intounderlyingsink_free(ptr >>> 0, 1));
const IntoUnderlyingSourceFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_intounderlyingsource_free(ptr >>> 0, 1));
const WasmFollowFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmfollow_free(ptr >>> 0, 1));
const WasmMemoryStreamFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmmemorystream_free(ptr >>> 0, 1));

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_externrefs.set(idx, obj);
    return idx;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
}

const CLOSURE_DTORS = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(state => wasm.__wbindgen_destroy_closure(state.a, state.b));

function debugString(val) {
    // primitive types
    const type = typeof val;
    if (type == 'number' || type == 'boolean' || val == null) {
        return  `${val}`;
    }
    if (type == 'string') {
        return `"${val}"`;
    }
    if (type == 'symbol') {
        const description = val.description;
        if (description == null) {
            return 'Symbol';
        } else {
            return `Symbol(${description})`;
        }
    }
    if (type == 'function') {
        const name = val.name;
        if (typeof name == 'string' && name.length > 0) {
            return `Function(${name})`;
        } else {
            return 'Function';
        }
    }
    // objects
    if (Array.isArray(val)) {
        const length = val.length;
        let debug = '[';
        if (length > 0) {
            debug += debugString(val[0]);
        }
        for(let i = 1; i < length; i++) {
            debug += ', ' + debugString(val[i]);
        }
        debug += ']';
        return debug;
    }
    // Test for built-in
    const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
    let className;
    if (builtInMatches && builtInMatches.length > 1) {
        className = builtInMatches[1];
    } else {
        // Failed to match the standard '[object ClassName]'
        return toString.call(val);
    }
    if (className == 'Object') {
        // we're a user defined class or Object
        // JSON.stringify avoids problems with cycles, and is generally much
        // easier than looping through ownProperties of `val`.
        try {
            return 'Object(' + JSON.stringify(val) + ')';
        } catch (_) {
            return 'Object';
        }
    }
    // errors
    if (val instanceof Error) {
        return `${val.name}: ${val.message}\n${val.stack}`;
    }
    // TODO we could test for more things here, like `Set`s and `Map`s.
    return className;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        const idx = addToExternrefTable0(e);
        wasm.__wbindgen_exn_store(idx);
    }
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

function makeMutClosure(arg0, arg1, f) {
    const state = { a: arg0, b: arg1, cnt: 1 };
    const real = (...args) => {

        // First up with a closure we increment the internal reference
        // count. This ensures that the Rust closure environment won't
        // be deallocated while we're invoking it.
        state.cnt++;
        const a = state.a;
        state.a = 0;
        try {
            return f(a, state.b, ...args);
        } finally {
            state.a = a;
            real._wbg_cb_unref();
        }
    };
    real._wbg_cb_unref = () => {
        if (--state.cnt === 0) {
            wasm.__wbindgen_destroy_closure(state.a, state.b);
            state.a = 0;
            CLOSURE_DTORS.unregister(state);
        }
    };
    CLOSURE_DTORS.register(real, state, state);
    return real;
}

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}

function passArrayJsValueToWasm0(array, malloc) {
    const ptr = malloc(array.length * 4, 4) >>> 0;
    for (let i = 0; i < array.length; i++) {
        const add = addToExternrefTable0(array[i]);
        getDataViewMemory0().setUint32(ptr + 4 * i, add, true);
    }
    WASM_VECTOR_LEN = array.length;
    return ptr;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_externrefs.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

let wasmModule, wasm;
function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    wasmModule = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    wasm.__wbindgen_start();
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = module.ok && expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('acyclic_fs_wasm_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };
