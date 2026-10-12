use super::*;

/// Fail-closed embedded facade errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum FsError {
    /// Operation was cancelled before further facade work began.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// A scoped original native accounting operation retained its typed cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeKernel(#[from] NativeTreeMaterializationError),
    /// The caller's original frozen source identity or epoch diverged before sealing.
    #[error("frozen source snapshot is stale")]
    SourceSnapshotStale,
    /// An open handle's bounded proof, parent vector or collection-root registration could not allocate.
    #[error("filesystem handle allocation failed")]
    HandleAllocationFailed,
    /// The actual encoder buffer allocation failed without losing its allocator cause.
    #[error("filesystem encoding allocation failed: {0}")]
    EncodedAllocationFailed(#[source] std::collections::TryReserveError),
    /// Volume configuration is invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Durable local Stream initialization or recovery failed.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    LocalStream(#[from] acyclic_stream::LocalStreamError),
    /// Durable local Objects initialization or recovery failed.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    LocalObjects(#[from] acyclic_objects::v1::local::LocalOpenError),
    /// The canonical filesystem Objects bucket could not be opened.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem Objects bucket initialization failed: {0}")]
    LocalObjectsBucket(acyclic_objects::v1::Error),
    /// Local provider root setup failed before either canonical provider opened.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem root setup failed: {0}")]
    LocalRoot(std::io::Error),
    /// The private spill file for pre-publication staging could not be created.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local staging spill file setup failed: {0}")]
    LocalStaging(std::io::Error),
    /// A live process-local engine already owns this root with different limits.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem root is already open with different options")]
    LocalOptionsConflict,
    /// The bounded local root initialization worker could not complete.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem initialization worker is unavailable")]
    LocalInitializationWorker,
    /// Immutable-object accelerator bounds are invalid.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    ObjectCacheConfig(#[from] crate::cache::ObjectCacheConfigError),
    /// Checkout behavior is contradictory.
    #[error(transparent)]
    CheckoutMode(#[from] CheckoutModeError),
    /// Requested lifecycle is not guaranteed by the selected backends.
    #[error("durable volume requested from non-durable embedded backends")]
    UnsupportedDurability,
    /// A new name collides with an existing sibling under the volume's
    #[error("name collides with an existing entry under the volume's case-folding policy")]
    NameCollision,
    /// A caller-derived identity already names different authenticated state.
    #[error("file identity collides with different authenticated content")]
    FileIdentityCollision,
    /// A new name is not canonical Unicode NFC, required by the volume's
    #[error("name is not normalized to NFC as required by the volume's unicode policy")]
    NonNormalizedName,
    /// The facade does not yet expose this checkout behavior.
    #[error("checkout behavior is not implemented by this facade")]
    UnsupportedCheckout,
    /// Direct-live mutation requires a serialized-authority volume.
    #[error("direct-live mutation requires serialized-authority concurrency")]
    LiveRequiresSerializedAuthority,
    /// Another writer changed the authority before exclusive ownership could be acquired.
    #[error("exclusive writer ownership raced with authority head {actual:?}")]
    ExclusiveWriterConflict {
        /// Linearizable head that defeated this acquisition.
        actual: Head,
    },
    /// A direct-live operation requires a positive safe-retry bound.
    #[error("direct-live retry bound must be positive")]
    ZeroLiveRetryLimit,
    /// Direct-live retry observations violated the canonical transition order.
    #[error(transparent)]
    LiveRetry(#[from] LiveRetryError),
    /// A direct-live checkout already has an unresolved candidate operation.
    #[error("direct-live checkout already has a pending operation")]
    PendingLiveMutation,
    /// Live advancement would cross an exact observed or mutated region.
    #[error("live advancement conflicts with an observed or mutated region")]
    LiveConflict {
        /// Bounded region-specific conflicts.
        conflicts: Vec<RebaseConflict>,
        /// Whether additional conflicts were omitted by the configured bound.
        truncated: bool,
    },
    /// Native live advancement keeps its exact conflict payload on the
    #[cfg(not(target_arch = "wasm32"))]
    #[error("live advancement conflicts with an observed or mutated region")]
    NativeLiveConflict {
        /// Bounded region-specific conflicts.
        conflicts: Vec<RebaseConflict>,
        /// Whether additional conflicts were omitted by the configured bound.
        truncated: bool,
        /// Retires only after conflict names and vector backing are destroyed.
        allocation: NativeAllocation,
    },
    /// Authority storage failed.
    #[error(transparent)]
    Authority(AuthorityStoreError),
    /// Immutable object storage failed.
    #[error(transparent)]
    Object(#[from] ObjectStoreError),
    /// Generation closure authentication failed.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Canonical filesystem data is malformed or unsupported.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Canonical volume creation data is malformed or unsupported.
    #[error(transparent)]
    VolumeCreated(#[from] VolumeCreatedError),
    /// Canonical generation-retention fact is invalid.
    #[error(transparent)]
    RetentionCreated(#[from] RetentionCreatedError),
    /// Exact namespace-path resolution failed.
    #[error(transparent)]
    Path(#[from] PathLookupError),
    /// Bounded authenticated directory pagination failed.
    #[error(transparent)]
    Directory(#[from] DirectoryReadError),
    /// Bounded authenticated file-record pagination failed.
    #[error(transparent)]
    FileRecord(#[from] FileRecordReadError),
    /// Composed sparse regular-file range read failed.
    #[error(transparent)]
    FileRead(#[from] FileRangeReadError),
    /// Bounded authenticated blob construction failed.
    #[error(transparent)]
    BlobBuild(#[from] BlobBuildError),
    /// Authenticated blob-range reading failed.
    #[error(transparent)]
    BlobRead(#[from] BlobReadError),
    /// Authenticated named-attribute lookup failed.
    #[error(transparent)]
    AttributeLookup(#[from] AttributeLookupError),
    /// Authenticated named-attribute pagination failed.
    #[error(transparent)]
    AttributeList(DirectoryReadError),
    /// Sparse named-attribute path-copy failed.
    #[error(transparent)]
    AttributeMutation(#[from] AttributeMutationError),
    /// Tiny regular-file payload exceeded its canonical bound.
    #[error(transparent)]
    InlineFile(#[from] InlineFileDataError),
    /// Requested path is authentically absent.
    #[error("filesystem path does not exist")]
    NotFound,
    /// Requested operation requires a directory.
    #[error("filesystem path is not a directory")]
    NotDirectory,
    /// Requested operation requires a symbolic link.
    #[error("filesystem path is not a symbolic link")]
    NotSymbolicLink,
    /// Requested operation requires an opaque Windows reparse point.
    #[error("filesystem path is not a Windows reparse point")]
    NotReparsePoint,
    /// Special-file creation selected a kind incompatible with its payload.
    #[error("filesystem special-file kind is incompatible with the requested payload")]
    InvalidSpecialKind,
    /// The configured volume profile cannot represent this kind exactly.
    #[error("filesystem kind is unsupported by the configured volume profile")]
    UnsupportedFileKind,
    /// A directory binding and file-table record disagree or are missing.
    #[error("directory entry does not have a matching authenticated file record")]
    InvalidDirectoryRecord,
    /// Export manifest does not exactly match its authenticated closure.
    #[error("generation export manifest does not match the imported closure")]
    InvalidExportManifest,
    /// Resumable generation transfer validation or paging failed.
    #[error(transparent)]
    Transfer(#[from] GenerationTransferError),
    /// Checkout dependency capture failed.
    #[error(transparent)]
    Dependency(#[from] DependencyError),
    /// Exact authenticated dependency probing failed.
    #[error(transparent)]
    Probe(#[from] AuthenticatedProbeError),
    /// Observation-safe rebase classification failed.
    #[error(transparent)]
    Rebase(#[from] RebaseError<AuthenticatedProbeError>),
    /// One ordered generation mutation failed.
    #[error(transparent)]
    Mutation(#[from] GenerationMutationError),
    /// Path-independent open-file mutation failed.
    #[error(transparent)]
    DetachedMutation(#[from] RegularMutationError),
    /// Immutable checkpoint construction failed.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    /// Whole-closure generation publication failed.
    #[error(transparent)]
    Publication(#[from] PublicationError),
    /// Authority exists but has no volume creation fact.
    #[error("volume authority has no creation fact")]
    EmptyAuthority,
    /// Authority records do not contain the expected volume event.
    #[error("volume authority history has an invalid event shape")]
    InvalidAuthorityHistory,
    /// Authenticated data belongs to another volume.
    #[error("authenticated data belongs to another volume")]
    VolumeMismatch,
    /// Concurrent creation/publication produced a semantic conflict.
    #[error("volume creation was rejected by authority state")]
    CreationRejected,
    /// The named workspace has been terminally deleted.
    #[error("workspace is deleted")]
    WorkspaceDeleted,
    /// Writable overlays must be opened from the current authority head.
    #[error("writable private checkout requires a head selector")]
    WritableCheckoutRequiresHead,
    /// Mutation was requested from a checkout that cannot mutate.
    #[error("checkout does not admit private overlay mutation")]
    MutationNotAllowed,
    /// Commit was requested without a changed candidate generation.
    #[error("checkout has no pending mutation to commit")]
    NoPendingMutations,
    /// Explicit head refresh requires manual checkout consistency.
    #[error("checkout does not admit explicit head refresh")]
    RefreshNotAllowed,
    /// A dirty overlay must be safely rebased or discarded before refresh.
    #[error("pending overlay mutations require safe rebase before refresh")]
    PendingMutationsRequireRebase,
    /// Uncommitted operations exceed the volume's atomic replay bound.
    #[error("checkout pending mutation count exceeds the volume limit")]
    TooManyPendingMutations,
    /// Caller-supplied mutation storage exceeds the admitted persistent bound.
    #[error("mutation input capacity exceeds the volume limit")]
    ExcessiveMutationCapacity,
    /// Retaining a bounded replay log failed.
    #[error("pending mutation replay log allocation failed")]
    PendingMutationAllocationFailed,
    /// A prepared two-parent merge must be committed or discarded before mutation/rebase.
    #[error("prepared merge must be committed or discarded before further mutation")]
    PreparedMergePending,
    /// Diff roots or persistent page structure do not match the admitted format.
    #[error("generation diff structure is invalid")]
    InvalidDiff,
    /// Bounded diff result allocation failed.
    #[error("generation diff allocation failed")]
    DiffAllocationFailed,
    /// A three-way merge exceeded its admitted change frontier.
    #[error("three-way merge exceeds its change bound")]
    MergeChangeLimit,
    /// The selected second parent is not the current authority head.
    #[error("merge second parent must be the current authority head")]
    MergeParentNotHead,
    /// Sparse file-table merge application failed.
    #[error(transparent)]
    MergeFileTable(#[from] FileTableMutationError),
    /// Sparse directory-tree merge application failed.
    #[error(transparent)]
    MergeTree(#[from] TreeMutationError),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

impl From<AuthorityStoreError> for FsError {
    fn from(error: AuthorityStoreError) -> Self {
        match error {
            // A deleted workspace's authority is released for good; reading
            AuthorityStoreError::Retired => Self::WorkspaceDeleted,
            error => Self::Authority(error),
        }
    }
}

pub(super) fn validate_volume_capabilities(
    config: VolumeConfig,
    capabilities: EmbeddedCapabilities,
) -> Result<(), FsError> {
    config.validate()?;
    if config.lifecycle == Lifecycle::Durable && !capabilities.durable {
        return Err(FsError::UnsupportedDurability);
    }
    Ok(())
}

pub(super) fn validate_checkout(
    mode: CheckoutMode,
    config: VolumeConfig,
) -> Result<(), OperationFailure<FsError>> {
    mode.validate()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    if mode.mutations == MutationMode::DirectLive
        && config.concurrency != ConcurrencyMode::SerializedAuthority
    {
        return Err(OperationFailure::before_work(
            FsError::LiveRequiresSerializedAuthority,
        ));
    }
    Ok(())
}

pub(super) fn map_merge_failure(
    failure: OperationFailure<MergeGenerationError>,
    prior: WorkCounters,
) -> OperationFailure<FsError> {
    failure.map_with_prior_work(prior, |error| match error {
        MergeGenerationError::InvalidDiff => FsError::InvalidDiff,
        MergeGenerationError::ChangeLimit => FsError::MergeChangeLimit,
        MergeGenerationError::AllocationFailed => FsError::DiffAllocationFailed,
        MergeGenerationError::Object(error) => FsError::Object(error),
        MergeGenerationError::Decode(error) => FsError::Decode(error),
        MergeGenerationError::FileTable(error) => FsError::MergeFileTable(error),
        MergeGenerationError::Tree(error) => FsError::MergeTree(error),
        MergeGenerationError::ExtentRead(error) => {
            FsError::FileRead(FileRangeReadError::Extent(error))
        }
        MergeGenerationError::ExtentMutation(error) => {
            FsError::DetachedMutation(RegularMutationError::Extent(error))
        }
        MergeGenerationError::Checkpoint(error) => FsError::Checkpoint(error),
        MergeGenerationError::Cancelled(error) => FsError::Cancelled(error),
        MergeGenerationError::Work(error) => FsError::Work(error),
        MergeGenerationError::Kernel(error) => error,
    })
}

pub(super) fn map_transfer_error(error: GenerationTransferError) -> FsError {
    match error {
        GenerationTransferError::Cancelled(error) => FsError::Cancelled(error),
        GenerationTransferError::Manifest(_) | GenerationTransferError::ManifestMismatch => {
            FsError::InvalidExportManifest
        }
        GenerationTransferError::Closure(error) => FsError::Closure(error),
        GenerationTransferError::Object(error) => FsError::Object(error),
        GenerationTransferError::Work(error) => FsError::Work(error),
        error @ (GenerationTransferError::InvalidCursor
        | GenerationTransferError::EmptyBatch
        | GenerationTransferError::TooManyObjects
        | GenerationTransferError::AllocationFailed) => FsError::Transfer(error),
    }
}
