use super::*;

/// Customer workspace operation failure.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum WorkspaceError {
    /// Friendly name was invalid.
    #[error(transparent)]
    Name(#[from] WorkspaceNameError),
    /// The canonical engine rejected or could not complete the operation.
    #[error("workspace engine failure: {0}")]
    Engine(String),
    /// The original filesystem failure.
    #[error(transparent)]
    Filesystem(FsError),
    /// Caller cancellation stopped a measured workspace operation.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
    /// A measured workspace operation exhausted its admitted work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Both completed work operands when their sum cannot be represented.
    #[error("workspace work merge failed: {error}")]
    WorkMerge {
        /// Work completed before the backend call.
        prior: crate::WorkCounters,
        /// Work completed by the backend.
        backend: crate::WorkCounters,
        /// Checked arithmetic failure.
        #[source]
        error: crate::WorkError,
    },
    /// A fork generation belongs to another filesystem deployment.
    #[error("fork generation belongs to another filesystem deployment")]
    ForeignGeneration,
    /// A caller-pinned target generation is no longer current.
    #[error("workspace generation is stale")]
    StaleGeneration,
    /// The source identity or epoch changed before publication.
    #[error("workspace source snapshot is stale")]
    StaleSource,
    /// A conditional mutation targeted a different filesystem identity.
    #[error("workspace file identity is stale")]
    StaleIdentity,
    /// Canonical customer path was invalid.
    #[error("invalid workspace path: {0}")]
    Path(String),
    /// A typed path or Unicode projection failure.
    #[error("invalid workspace path: {0}")]
    NamespacePath(crate::kernel::NamespacePathError),
    /// Requested path does not exist.
    #[error("workspace path does not exist")]
    NotFound,
    /// An operation required a regular file.
    #[error("workspace path is not a regular file")]
    NotRegularFile,
    /// An operation required a directory.
    #[error("workspace path is not a directory")]
    NotDirectory,
    /// A bounded whole-file helper encountered a larger file.
    #[error("workspace file exceeds the caller's read bound")]
    ReadLimitExceeded,
    /// A retention label already identifies different immutable state.
    #[error("retention label is already bound to another generation")]
    RetentionConflict,
    /// Workspace semantics or lineage are incompatible for the requested join.
    #[error("workspaces are incompatible")]
    IncompatibleWorkspace,
    /// No common immutable ancestor exists within the admitted lineage.
    #[error("workspaces have no common ancestor")]
    NoCommonAncestor,
    /// Bounded lineage discovery exhausted its exact generation frontier.
    #[error("workspace lineage exceeds the configured bound")]
    LineageLimit,
    /// Join input, diff, or conflict bounds are zero or exhausted.
    #[error("workspace join exceeds its configured bound")]
    JoinLimit,
    /// An exact target generation is no longer the workspace's current head.
    #[error("workspace target generation is stale")]
    StaleTarget,
    /// A declarative merge resolution does not match its typed conflict.
    #[error("merge resolution is incompatible with its conflict")]
    InvalidMergeResolution,
    /// Change sets do not share one exact contiguous endpoint and deployment.
    #[error("change sets are not contiguous")]
    ChangeSetContinuity,
    /// Resolving changed paths exhausted the caller's namespace traversal bound.
    #[error("changed paths exceed the caller's traversal bound")]
    ChangedPathLimit,
    /// A staged concatenation omitted every part.
    #[error("staged content set is empty")]
    EmptyContentSet,
    /// The workspace has no authenticated foreign source in its ancestry.
    #[error("workspace is not a fork")]
    NotFork,
    /// Staged logical lengths overflowed the filesystem range.
    #[error("staged content length overflow")]
    ContentLengthOverflow,
}

impl WorkspaceError {
    pub(crate) fn engine(error: impl std::error::Error + 'static) -> Self {
        if let Some(cause) = workspace_bounded_error(&error) { return cause; }
        if let Some(failure) = (&error as &dyn std::any::Any).downcast_ref::<crate::OperationFailure<FsError>>()
            && let Some(cause) = workspace_bounded_error(&failure.error) { return cause; }
        Self::Engine(error.to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<crate::NativeTreeMaterializationError> for WorkspaceError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self { FsError::from(error).into() }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl From<crate::MaterializeError> for WorkspaceError {
    fn from(error: crate::MaterializeError) -> Self {
        match error {
            crate::MaterializeError::Work(error) => Self::Work(error),
            crate::MaterializeError::Cancelled(error) => Self::Cancelled(error),
            crate::MaterializeError::MissingPath => Self::NotFound,
            error => Self::Engine(error.to_string()),
        }
    }
}

/// Extracts quota and cancellation causes through transparent error wrappers.
pub(crate) fn workspace_bounded_error(error: &(dyn std::error::Error + 'static)) -> Option<WorkspaceError> {
    if let Some(error) = error.downcast_ref::<crate::WorkError>() { return Some(WorkspaceError::Work(*error)); }
    if error.is::<crate::CancellationError>() { return Some(WorkspaceError::Cancelled(crate::CancellationError)); }
    if let Some(error) = error.downcast_ref::<Box<FsError>>() { return workspace_bounded_error(error.as_ref()); }
    if let Some(WorkspaceError::WorkMerge { prior, backend, error }) = error.downcast_ref::<WorkspaceError>() {
        return Some(WorkspaceError::WorkMerge { prior: *prior, backend: *backend, error: *error });
    }
    macro_rules! bounded {
        ($kind:ty; [$($nested:ident),*]; [$($cancelled:ident),*]) => {{
            type Cause = $kind;
            if let Some(cause) = error.downcast_ref::<Cause>() {
                return match cause {
                    Cause::Work(cause) => Some(WorkspaceError::Work(*cause)),
                    $(Cause::$nested(cause) => workspace_bounded_error(cause),)*
                    $(Cause::$cancelled => Some(WorkspaceError::Cancelled(crate::CancellationError)),)*
                    _ => None,
                };
            }
        }};
    }
    bounded!(WorkspaceError; [Cancelled, Filesystem, NamespacePath]; []);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(FsError::NativeKernel(cause)) = error.downcast_ref::<FsError>() { return workspace_bounded_error(cause); }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::ObjectStoreError::Native(cause)) = error.downcast_ref::<crate::ObjectStoreError>() { return workspace_bounded_error(cause); }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::AuthorityStoreError::NativeOriginal(cause)) = error.downcast_ref::<crate::AuthorityStoreError>() { return workspace_bounded_error(cause); }
    #[cfg(not(target_arch = "wasm32"))]
    bounded!(crate::NativeTreeMaterializationError; [Io]; [Cancelled]);
    bounded!(FsError;
        [Cancelled, Authority, Object, Closure, Path, Directory, FileRecord, FileRead, BlobBuild,
         BlobRead, AttributeLookup, AttributeList, AttributeMutation, Probe, Rebase, Mutation,
         DetachedMutation, Checkpoint, Publication, MergeFileTable, MergeTree]; []);
    bounded!(crate::AuthorityStoreError; []; [Cancelled]);
    bounded!(crate::ObjectStoreError; []; [Cancelled]);
    bounded!(crate::kernel::GenerationMutationError; [Plan, Path, FileRecord, Tree, FileTable, Regular, Directory]; []);
    bounded!(crate::kernel::MutationPlanError; []; []);
    bounded!(crate::kernel::PathLookupError; [FileTable, Tree, NamespacePath, Dependency]; []);
    bounded!(crate::kernel::NamespacePathError; []; [Cancelled]);
    bounded!(crate::kernel::FileRecordReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::TreeReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::TreeMutationError; [Storage]; []);
    bounded!(crate::kernel::FileTableMutationError; [Storage]; []);
    bounded!(crate::kernel::RegularMutationError; [BlobBuild, BlobRead, Extent, Range, Clone, Storage]; []);
    bounded!(crate::kernel::ExtentMutationError; [Storage]; []);
    bounded!(crate::kernel::ExtentCloneError; [Source, Destination]; []);
    bounded!(crate::kernel::ExtentReadError; [Storage, Context]; [Cancelled]);
    bounded!(crate::kernel::BlobBuildError; [Source, Storage]; [Cancelled]);
    bounded!(crate::kernel::BlobReadError; [Storage, Context]; [Cancelled]);
    bounded!(crate::kernel::FileRangeReadError; [Extent, Blob, Context]; [Cancelled]);
    bounded!(crate::kernel::DirectoryReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::AttributeLookupError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::AttributeMutationError; [Storage]; []);
    bounded!(crate::kernel::AuthenticatedProbeError; [Storage, FileRecord, Tree, Directory, Extent, Blob]; [Cancelled]);
    bounded!(crate::kernel::RebaseError<crate::kernel::AuthenticatedProbeError>; [Probe]; [Cancelled]);
    bounded!(crate::kernel::CheckpointError; [Storage]; []);
    bounded!(crate::kernel::ClosureError; [Storage]; []);
    bounded!(crate::kernel::PublicationError; [Closure, Objects, Authority]; []);
    #[cfg(not(target_arch = "wasm32"))]
    bounded!(crate::materializer::NativeTreeMaterializationError; [Io]; [Cancelled]);
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    bounded!(crate::DemandError; []; [Cancelled]);
    #[cfg(feature = "native-mount")]
    if let Some(error) = error.downcast_ref::<crate::native_mount::MountSourceError>() {
        use crate::native_mount::MountSourceError as Cause;
        return match error.semantic_error() {
            Cause::Work(cause) => Some(WorkspaceError::Work(*cause)),
            Cause::Cancelled => Some(WorkspaceError::Cancelled(crate::CancellationError)),
            Cause::Filesystem(cause) => workspace_bounded_error(cause),
            Cause::NamespacePath(cause) => workspace_bounded_error(cause),
            Cause::NativeWork(cause) => workspace_bounded_error(cause),
            _ => None,
        };
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() { return error.get_ref().and_then(|cause| workspace_bounded_error(cause)); }
    error.source().and_then(workspace_bounded_error)
}

impl From<crate::AuthorityStoreError> for WorkspaceError {
    fn from(error: crate::AuthorityStoreError) -> Self {
        Self::from(FsError::from(error))
    }
}

impl From<FsError> for WorkspaceError {
    fn from(error: FsError) -> Self {
        if let Some(cause) = workspace_bounded_error(&error) { return cause; }
        match error {
            FsError::Path(crate::kernel::PathLookupError::NamespacePath(error)) => error.into(),
            FsError::SourceSnapshotStale => Self::StaleSource,
            FsError::NotFound => Self::NotFound,
            FsError::NotDirectory => Self::NotDirectory,
            FsError::FileRead(crate::kernel::FileRangeReadError::NotRegular) => Self::NotRegularFile,
            FsError::Mutation(crate::kernel::GenerationMutationError::FileIdentityConflict) => Self::StaleIdentity,
            error => Self::Filesystem(error),
        }
    }
}

impl From<crate::kernel::NamespacePathError> for WorkspaceError {
    fn from(error: crate::kernel::NamespacePathError) -> Self {
        match error {
            crate::kernel::NamespacePathError::Work(error) => Self::Work(error),
            crate::kernel::NamespacePathError::Cancelled => Self::Cancelled(crate::CancellationError),
            error => Self::NamespacePath(error),
        }
    }
}
