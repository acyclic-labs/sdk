use super::*;

/// Synchronous callback surface implemented by an embedded checkout adapter.
pub trait MountFilesystem: Send + Sync + 'static {
    /// Returns the sealed original native accounting and lifetime context.
    fn native_original_work(&self) -> Result<Option<NativeMaterializationWork>, MountSourceError> {
        Ok(None)
    }

    /// Returns an already-funded live authorization guard for a task source.
    fn native_callback_guard(&self) -> Option<Arc<dyn NativeMountCallbackGuard>> {
        None
    }

    /// Borrows the actual SDK checkout owner and canonical selected root.
    fn native_namespace_lineage(&self) -> Option<NativeMountSourceLineage<'_>> {
        None
    }

    /// Looks up through an already shared original native invocation.
    fn lookup_with_meter(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError>;
    /// Looks up and pins through the same existing invocation.
    fn lookup_pinned_with_meter(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError>;
    /// Registers the actual observer on the same original allocation ledger.
    fn observe_view_with_meter(
        &self,
        observer: std::sync::Weak<dyn ViewObserver>,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Fences actual source changes without another ledger or queue.
    fn fence_changes_with_meter(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Acquires the actual source binding lease with its physical drop proof.
    fn acquire_binding_lease_with_meter(
        &self,
        expected_epoch: Option<u64>,
        work: &NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError>;
    /// Captures exact paths through the existing original shared invocation.
    fn capture_host_paths_with_meter(
        &self,
        root: &Path,
        paths: &[MountPath],
        expected_root_identity: crate::NativeRootIdentity,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Captures one subtree through the same actual capture producer.
    fn capture_host_subtree_with_meter(
        &self,
        root: &Path,
        path: &MountPath,
        expected_root_identity: crate::NativeRootIdentity,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Reads a pinned range without erasing the actual owned chunk capacity.
    fn read_pinned_with_meter(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut NativeMountContentSink<'_>,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Reads actual link bytes and retains their producer-known capacity.
    fn read_link(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads the exact range using the existing shared native queue.
    fn read_range_with_meter(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        work: &NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
    /// Reads a page with the original ledger and actual capacity custody.
    fn read_directory_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError>;

    /// Removes the exact binding on a shared original invocation.
    fn remove(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Renames the exact binding with original source grants.
    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Mutates actual metadata and optional length through the same source.
    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Flushes the actual candidate on the shared original invocation.
    fn flush(&self, work: &NativeMaterializationWork) -> Result<(), MountSourceError>;
    /// Opens an actual attached handle and reports its pre-erasure SDK payload claim.
    fn open_file_with_meter(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Opens and proves the handle in the same coherent original invocation.
    fn open_file_with_lookup_with_meter(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError>;
    /// Opens the actual identity just created, without resolving a replacement.
    fn open_created_with_meter(
        &self,
        path: &MountPath,
        created: &MountLookup,
        work: &NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Captures the actual detached open-file identity with its original claim.
    fn detach_file_with_meter(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
    /// Creates an empty file, preserving an optional absent-since proof.
    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        absent_since: Option<ViewStamp>,
        work: &NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError>;
    /// Creates an actual directory through the same source engine.
    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        work: &NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError>;
    /// Consumes the exact already-admitted opaque symbolic-link target.
    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata,
        work: &NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError>;
    /// Creates the exact native special-file kind and device identity.
    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
        work: &NativeMaterializationWork,
    ) -> Result<MountLookup, MountSourceError>;
    /// Reads the exact owned attribute on the operative source invocation.
    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &NativeMaterializationWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError>;
    /// Lists the actual owned attribute page.
    fn list_attributes_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &NativeMaterializationWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError>;
    /// Consumes an exact already-admitted native attribute value.
    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Removes the exact native attribute.
    fn remove_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Clones the actual immutable range between authenticated file identities.
    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Creates the actual same-volume hard-link binding.
    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountSourceError>;
    /// Folds the actual source representation and reports exact owned capacity.
    fn folded_path_with_meter(
        &self,
        path: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(MountPath, u64)>, MountSourceError>;
    /// Queries actual source stability on the operative invocation.
    fn view_is_stable_with_meter(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<bool, MountSourceError>;
    /// Queries the actual source stamp.
    fn view_stamp_with_meter(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<Option<ViewStamp>, MountSourceError>;
    /// Queries the actual writable binding epoch.
    fn binding_epoch_with_meter(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<Option<u64>, MountSourceError>;
    /// Traverses actual source change history under the original item limits.
    fn unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &NativeMaterializationWork,
    ) -> Result<bool, MountSourceError>;
    /// Traverses only actual reported changes through the same history engine.
    fn reported_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &NativeMaterializationWork,
    ) -> Result<bool, MountSourceError>;
    /// Stops the actual owned source watcher; success confirms physical quiescence (or that this source owns no active watcher).
    fn shutdown_with_meter(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<bool, MountSourceError>;
    /// Retires this held source using only its constructor-owned cleanup credit.
    fn shutdown_with_cleanup_wait(
        &self,
        _wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        Err(MountSourceError::NativeUnsupported)
    }
    /// Completes this source's already accepted physical retirement.
    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        Err(MountSourceError::NativeUnsupported)
    }
    /// Traverses actual changes to this file identity under original limits.
    fn node_unchanged_since_with_meter(
        &self,
        file_id: FileId,
        stamp: ViewStamp,
        work: &NativeMaterializationWork,
    ) -> Result<bool, MountSourceError>;
    /// Traverses actual changes to this directory's immediate listing.
    fn listing_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        stamp: ViewStamp,
        work: &NativeMaterializationWork,
    ) -> Result<bool, MountSourceError>;

    /// Whether this source can represent native POSIX named attributes.
    fn supports_posix_named_attributes(&self) -> bool {
        false
    }

    /// Whether a native handle close is itself a publication boundary.
    fn flush_on_handle_close(&self) -> bool {
        true
    }

    /// Whether [`Self::flush`] can publish anything.
    fn flush_publishes(&self) -> bool {
        true
    }

    /// Whether a lookup may observe one coherent source view right now.
    fn view_is_stable(&self) -> bool {
        true
    }

    /// Samples this source's position in the order of view changes, before a lookup whose result a driver may cache.
    fn view_stamp(&self) -> Option<ViewStamp> {
        None
    }

    /// Whether a lookup of `path`, which resolved to `file_id` (`None`: to nothing) after `stamp` was sampled, still describes the view.
    fn unchanged_since(
        &self,
        _path: &MountPath,
        _file_id: Option<FileId>,
        _stamp: ViewStamp,
    ) -> bool {
        false
    }

    /// [`Self::unchanged_since`], counting only changes the source reported: facts read after `stamp` that a driver read again since, with the same result, still describe the view across changes the source could not yet confirm ([`super::demand::SourceChange::Unconfirmed`]).
    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.unchanged_since(path, file_id, stamp)
    }

    /// Whether this source reports every change to the node `file_id`, so facts about it may be kept (by a kernel, or an NFS client) until a reported change supersedes them.
    fn reports_changes_to(&self, _file_id: FileId) -> bool {
        self.view_stamp().is_some()
    }

    /// Whether facts about the node `file_id`, read after `stamp` was sampled, still describe it: no change after `stamp` touched the node under any of its names.
    fn node_unchanged_since(&self, _file_id: FileId, _stamp: ViewStamp) -> bool {
        false
    }

    /// Whether the listing of the directory at `path` is unchanged since `stamp`: no name directly beneath it was bound, unbound, or rebound since.
    fn listing_unchanged_since(&self, _path: &MountPath, _stamp: ViewStamp) -> bool {
        false
    }

    /// The spelling every equivalent spelling of `path` resolves to, when this source folds names; `None` when every spelling is distinct.
    fn folded_path(&self, _path: &MountPath) -> Option<MountPath> {
        None
    }

    /// Tells `observer` of every change to this source's view from now on, together with the origin that made it.
    fn observe_view(&self, _observer: std::sync::Weak<dyn ViewObserver>) {}

    /// Returns once every change made to what this source projects outside every view of it, and completed before the call, is recorded in the view.
    fn fence_changes(&self) -> Result<(), MountSourceError> {
        Ok(())
    }

    /// Changes only when existing path or handle bindings may be replaced by an external source transition.
    fn binding_epoch(&self) -> Option<u64> {
        None
    }

    /// Pins the current coherent projection view for one native callback.
    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        if !self.view_is_stable() {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(()))
    }

    /// Pins source binding identity without treating ordinary native writes as an external rebind.
    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        if !self.view_is_stable() || self.binding_epoch() != expected_epoch {
            return Err(MountSourceError::Stale);
        }
        Ok(Box::new(()))
    }

    /// Looks up one absolute portable path without following links.
    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError>;
    /// Looks up one path like [`Self::lookup`], also pinning the exact external content a regular file's later [`Self::read_pinned`] must reproduce.
    fn lookup_pinned(
        &self,
        path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        Ok(self.lookup(path)?.map(|lookup| (lookup, None)))
    }

    /// Streams one range of the content a [`Self::lookup_pinned`] pin promised for `path` to `sink`, in ascending gap-free pieces of at most `piece` bytes, resolving and opening the source once for the whole range.
    fn read_pinned(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        let _ = (path, pin, offset, length, piece, sink);
        Err(MountSourceError::Stale)
    }

    /// Opens one regular file as an attached path-independent handle.
    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
    /// Opens `path` as [`Self::open_file`] does, with the facts the opened file had: a source whose open already proved them reports them without asking the file again.
    fn open_file_with_lookup(
        &self,
        path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        let file = self.open_file(path)?;
        let lookup = file.lookup()?;
        Ok((file, lookup))
    }
    /// Opens `created`, the regular file [`Self::create_file`] just made at `path`.
    fn open_created(
        &self,
        path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let _ = created;
        self.open_file(path)
    }
    /// Captures one regular file as a detached path-independent open-file view.
    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;

    /// Reads at most one exact logical range.
    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;

    /// Finds the first sparse data or hole byte at/after one offset.
    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError>;
    /// Reads one bounded page using a handle-owned opaque cursor.
    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError>;

    /// Lists one bounded page of native named-attribute names.
    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError>;
    /// Replaces one exact file range.
    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError>;
    /// Changes one file's logical length.
    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError>;
    /// Applies one exact sparse allocation operation.
    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError>;
    /// Clones one immutable range between regular files without reading bytes.
    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError>;
    /// Reconciles exact final host state for one projected path and seals it.
    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError>;
    /// Reconciles exact final host states together, preserving hard-link topology and admitting one bounded checkout candidate.
    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError>;
    /// Reconciles one imported directory and every descendant atomically.
    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError>;
}

/// Closed, allocation-free cause set for admission before startup failure storage exists.
#[derive(Debug, Error)]
pub enum NativeMountStartupAdmissionError {
    /// Original native scope, resource, or cancellation cause, moved intact.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Original cancellation or demand admission failure.
    #[error("native startup demand failed: {0}")]
    Demand(#[source] crate::DemandError),
    /// Explicit process-shared services have not been initialized.
    #[error("native mount services are not initialized")]
    ServicesCold,
    /// Explicit startup previously failed with this original shared cause.
    #[error("native mount service startup failed: {0}")]
    ServiceUnavailable(#[source] Arc<std::io::Error>),
}

impl From<crate::WorkError> for NativeMountStartupAdmissionError {
    fn from(error: crate::WorkError) -> Self {
        Self::Materialization(error.into())
    }
}

impl From<crate::DemandError> for NativeMountStartupAdmissionError {
    fn from(error: crate::DemandError) -> Self {
        match error {
            crate::DemandError::Work(error) => error.into(),
            error => Self::Demand(error),
        }
    }
}

impl From<NativeMountStartupAdmissionError> for NativeMountError {
    fn from(error: NativeMountStartupAdmissionError) -> Self {
        match error {
            NativeMountStartupAdmissionError::Materialization(
                crate::NativeTreeMaterializationError::Work(error),
            ) => Self::Work(error),
            NativeMountStartupAdmissionError::Materialization(error) => {
                Self::Materialization(error)
            }
            NativeMountStartupAdmissionError::Demand(error) => error.into(),
            NativeMountStartupAdmissionError::ServicesCold => Self::ServicesNotInitialized,
            NativeMountStartupAdmissionError::ServiceUnavailable(error) => {
                Self::ServiceUnavailable(error)
            }
        }
    }
}
