//! One status projection for every hosted filesystem operation. Stable semantic
//! failures must never masquerade as a retryable transport outage.

use crate::{AuthorityStoreError, FsError, ObjectStoreError, WorkspaceError};
use tonic::{Code, Status};
mod kernel;
use kernel::kernel_code;
#[cfg(any(feature = "native-watch", feature = "native-mount"))]
mod native;

pub(super) fn status(error: &WorkspaceError) -> Status {
    Status::new(workspace_code(error), error.to_string())
}

fn workspace_code(error: &WorkspaceError) -> Code {
    use WorkspaceError as E;
    match error {
        E::AccessDenied => Code::PermissionDenied,
        E::NotFound => Code::NotFound,
        E::Name(crate::WorkspaceNameError::Allocation) | E::Work(_) | E::WorkMerge { .. } => {
            Code::ResourceExhausted
        }
        E::Name(_)
        | E::Path(_)
        | E::ReadLimitExceeded
        | E::InvalidMergeResolution
        | E::NotRegularFile
        | E::NotDirectory
        | E::IsDirectory
        | E::EmptyContentSet
        | E::ContentLengthOverflow => Code::InvalidArgument,
        E::NamespacePath(error) => namespace_code(error),
        E::ForeignGeneration
        | E::StaleGeneration
        | E::StaleSource
        | E::StaleIdentity
        | E::IncompatibleWorkspace
        | E::ChangeSetContinuity
        | E::RetentionConflict
        | E::NoCommonAncestor
        | E::LineageLimit
        | E::JoinLimit
        | E::StaleTarget
        | E::ChangedPathLimit
        | E::DirectoryNotEmpty
        | E::NotFork => Code::FailedPrecondition,
        E::Cancelled(_) => Code::Cancelled,
        // Engine contains an opaque diagnostic, not evidence of an outage.
        E::Engine(_) => Code::Internal,
        E::Filesystem(error) => filesystem_code(error),
    }
}

fn filesystem_code(error: &FsError) -> Code {
    use FsError as E;
    match error {
        E::InvalidAccessPolicy => Code::PermissionDenied,
        E::UnauthenticatedAccessFacts => Code::Unauthenticated,
        E::Cancelled(_) => Code::Cancelled,
        E::AccessPolicyCacheCapacity
        | E::HandleAllocationFailed
        | E::EncodedAllocationFailed(_)
        | E::PendingMutationAllocationFailed
        | E::DiffAllocationFailed
        | E::MergeChangeLimit
        | E::TooManyPendingMutations
        | E::ExcessiveMutationCapacity
        | E::Work(_) => Code::ResourceExhausted,
        E::Config(_)
        | E::CheckoutMode(_)
        | E::NonNormalizedName
        | E::ZeroLiveRetryLimit
        | E::NotDirectory
        | E::NotSymbolicLink
        | E::NotReparsePoint
        | E::InvalidSpecialKind
        | E::InlineFile(_) => Code::InvalidArgument,
        E::NameCollision | E::FileIdentityCollision | E::CreationRejected => Code::AlreadyExists,
        E::ExclusiveWriterConflict { .. } | E::LiveConflict { .. } => Code::Aborted,
        E::SourceSnapshotStale
        | E::UnsupportedDurability
        | E::UnsupportedCheckout
        | E::LiveRequiresSerializedAuthority
        | E::PendingLiveMutation
        | E::UnsupportedFileKind
        | E::VolumeMismatch
        | E::WritableCheckoutRequiresHead
        | E::MutationNotAllowed
        | E::NoPendingMutations
        | E::RefreshNotAllowed
        | E::PendingMutationsRequireRebase
        | E::PreparedMergePending
        | E::MergeParentNotHead => Code::FailedPrecondition,
        E::NotFound | E::WorkspaceDeleted => Code::NotFound,
        E::LiveRetry(error) => match error {
            crate::kernel::LiveRetryError::ZeroAttempts => Code::InvalidArgument,
            crate::kernel::LiveRetryError::InvalidTransition => Code::Internal,
        },
        E::Authority(error) => authority_code(error),
        E::Object(error) => object_code(error),
        E::Path(error) => path_code(error),
        E::Mutation(error) => mutation_code(error),
        E::FileRead(error) => file_read_code(error),
        E::DetachedMutation(error) => regular_code(error),
        E::Dependency(error) => match error {
            crate::kernel::DependencyError::ZeroLimit
            | crate::kernel::DependencyError::EmptyContentRange
            | crate::kernel::DependencyError::RangeOverflow
            | crate::kernel::DependencyError::ZeroDirectoryPageLimit => Code::InvalidArgument,
            crate::kernel::DependencyError::ContradictoryState => Code::FailedPrecondition,
            crate::kernel::DependencyError::TooManyDependencies { .. } => Code::ResourceExhausted,
        },
        E::Decode(error) => kernel_code(error),
        E::VolumeCreated(error) => kernel_code(error),
        E::RetentionCreated(error) => kernel_code(error),
        E::InvalidDirectoryRecord
        | E::InvalidExportManifest
        | E::EmptyAuthority
        | E::InvalidAuthorityHistory
        | E::InvalidDiff => Code::DataLoss,
        // These kernels authenticate persisted state. Preserve any typed storage,
        // cancellation or admission cause beneath their transparent wrappers.
        E::Closure(error) => kernel_code(error),
        E::Directory(error) | E::AttributeList(error) => kernel_code(error),
        E::FileRecord(error) => kernel_code(error),
        E::BlobBuild(error) => kernel_code(error),
        E::BlobRead(error) => kernel_code(error),
        E::AttributeLookup(error) => kernel_code(error),
        E::AttributeMutation(error) => kernel_code(error),
        E::Transfer(error) => kernel_code(error),
        E::Probe(error) => kernel_code(error),
        E::Rebase(error) => kernel_code(error),
        E::Checkpoint(error) => kernel_code(error),
        E::Publication(error) => kernel_code(error),
        E::MergeFileTable(error) => kernel_code(error),
        E::MergeTree(error) => kernel_code(error),
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeKernel(error) => native_code(error),
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeLiveConflict { .. } => Code::Aborted,
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::LocalStream(error) => local_stream_code(error),
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::LocalObjects(error) => local_objects_code(error),
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::LocalObjectsBucket(error) => objects_service_code(error),
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::LocalRoot(error) | E::LocalStaging(error) => io_code(error),
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::LocalOptionsConflict => Code::FailedPrecondition,
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::LocalInitializationWorker => Code::Unavailable,
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        E::ObjectCacheConfig(_) => Code::InvalidArgument,
    }
}

fn authority_code(error: &AuthorityStoreError) -> Code {
    use AuthorityStoreError as E;
    match error {
        E::Missing | E::Retired => Code::NotFound,
        E::Cancelled => Code::Cancelled,
        E::NativeStream(error) => stream_code(error),
        E::DurableCodec(error) => match error {
            crate::StreamsAuthorityRecordError::InvalidLimit => Code::InvalidArgument,
            crate::StreamsAuthorityRecordError::RecordTooLarge { .. }
            | crate::StreamsAuthorityRecordError::LengthOverflow
            | crate::StreamsAuthorityRecordError::AllocationFailed => Code::ResourceExhausted,
            crate::StreamsAuthorityRecordError::InvalidDomain
            | crate::StreamsAuthorityRecordError::UnsupportedVersion(_)
            | crate::StreamsAuthorityRecordError::Truncated
            | crate::StreamsAuthorityRecordError::TrailingBytes
            | crate::StreamsAuthorityRecordError::ZeroEpoch
            | crate::StreamsAuthorityRecordError::ZeroSequence => Code::DataLoss,
        },
        E::CorruptStatic(_) | E::Corrupt(_) => Code::DataLoss,
        E::RejectedStatic(_)
        | E::Rejected(_)
        | E::ReplayCursorAhead { .. }
        | E::ReservationOriginalHeadUnavailable { .. } => Code::FailedPrecondition,
        E::InvalidReplayLimit | E::ReplayCursorProcessOverflow { .. } => Code::InvalidArgument,
        E::MemoryAuthorityPoisoned => Code::Internal,
        E::MemoryReplayAllocationFailed(_)
        | E::ReplayRecordTooLarge { .. }
        | E::PayloadTooLarge { .. }
        | E::EpochExhausted
        | E::SequenceExhausted
        | E::Work(_) => Code::ResourceExhausted,
        E::Io(error) => io_code(error),
        // A missing acknowledgement is explicitly retryable by the same identity;
        // its I/O cause cannot establish that the operation did not take place.
        E::Indeterminate { .. } => Code::Unavailable,
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeOriginalUnsupported => Code::Unimplemented,
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeOriginal(error) => native_code(error),
    }
}

fn object_code(error: &ObjectStoreError) -> Code {
    use ObjectStoreError as E;
    match error {
        E::Cancelled => Code::Cancelled,
        E::Missing => Code::NotFound,
        E::DigestMismatch | E::EmptyWriteBatch | E::EmptyReadBatch | E::InvalidRange => {
            Code::InvalidArgument
        }
        E::SdkAllocationFailed
        | E::MemoryTableAllocationFailed(_)
        | E::TooLarge { .. }
        | E::Work(_) => Code::ResourceExhausted,
        E::Corrupt | E::QuarantineFailed(_) => Code::DataLoss,
        E::Rejected(_) => Code::FailedPrecondition,
        E::Simulation(_) => Code::Unavailable,
        E::Io(error) => io_code(error),
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeUnavailable => Code::Unimplemented,
        #[cfg(not(target_arch = "wasm32"))]
        E::Native(error) => native_code(error),
        #[cfg(all(any(test, feature = "distributed"), not(target_arch = "wasm32")))]
        E::NativeObjects(error) => native_objects_code(error),
        #[cfg(any(test, feature = "distributed"))]
        E::LogicalObjects(error) => objects_service_code(error),
    }
}

fn stream_code(error: &acyclic_stream::StreamError) -> Code {
    use acyclic_stream::StreamError as E;
    match error {
        E::InvalidPath | E::InvalidArgument | E::LimitExceeded => Code::InvalidArgument,
        E::NotFound => Code::NotFound,
        E::AlreadyExists => Code::AlreadyExists,
        E::PrefixNotRetained | E::HierarchyChanged | E::IdempotencyMismatch => {
            Code::FailedPrecondition
        }
        E::OutOfRange => Code::OutOfRange,
        E::Capacity => Code::ResourceExhausted,
        E::AccessDenied => Code::PermissionDenied,
        E::Unavailable => Code::Unavailable,
        E::DeadlineElapsed => Code::DeadlineExceeded,
        E::Unsupported => Code::Unimplemented,
        E::InvalidResponse => Code::DataLoss,
    }
}

fn namespace_code(error: &crate::kernel::NamespacePathError) -> Code {
    use crate::kernel::NamespacePathError as E;
    match error {
        E::TooDeep
        | E::ComponentTooLong
        | E::PathTooLong
        | E::Name(_)
        | E::Path(_)
        | E::NonUnicodeProjection => Code::InvalidArgument,
        E::Work(_) | E::AllocationFailed => Code::ResourceExhausted,
        E::Cancelled => Code::Cancelled,
    }
}

fn path_code(error: &crate::kernel::PathLookupError) -> Code {
    use crate::kernel::PathLookupError as E;
    match error {
        E::Config(_) | E::EmptyBatch | E::PathBounds | E::NotDirectory => Code::InvalidArgument,
        E::TooManyPaths | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
        E::UnsupportedNameEncoding => Code::FailedPrecondition,
        E::MissingRootRecord | E::KindMismatch => Code::DataLoss,
        E::NamespacePath(error) => namespace_code(error),
        E::FileTable(error) => kernel_code(error),
        E::Tree(error) => kernel_code(error),
        E::Dependency(error) => kernel_code(error),
    }
}

fn mutation_code(error: &crate::kernel::GenerationMutationError) -> Code {
    use crate::kernel::GenerationMutationError as E;
    match error {
        E::Plan(error) => match error {
            crate::kernel::MutationPlanError::Empty
            | crate::kernel::MutationPlanError::InvalidInitialRecord
            | crate::kernel::MutationPlanError::RootMutation
            | crate::kernel::MutationPlanError::SameSourceAndDestination
            | crate::kernel::MutationPlanError::EmptyRange
            | crate::kernel::MutationPlanError::RangeOverflow
            | crate::kernel::MutationPlanError::WrongContentKind
            | crate::kernel::MutationPlanError::WrongMetadataKind => Code::InvalidArgument,
            crate::kernel::MutationPlanError::TooManyMutations
            | crate::kernel::MutationPlanError::IndexOverflow
            | crate::kernel::MutationPlanError::AllocationFailed
            | crate::kernel::MutationPlanError::Work(_) => Code::ResourceExhausted,
        },
        E::Config(_)
        | E::InvalidInitialLinkCount
        | E::DirectoryCycle
        | E::NotDirectory
        | E::IsDirectory => Code::InvalidArgument,
        E::TooManyPaths | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
        E::UnsupportedSymbolicLink
        | E::UnsupportedFileKind
        | E::UnsupportedSparseMutation
        | E::UnsupportedHardLink
        | E::FileIdentityConflict
        | E::DirectoryNotEmpty => Code::FailedPrecondition,
        E::MissingParent | E::MissingSource => Code::NotFound,
        E::AlreadyExists | E::FileIdentityAlreadyExists => Code::AlreadyExists,
        E::InconsistentState => Code::DataLoss,
        E::Path(error) => path_code(error),
        E::FileRecord(error) => kernel_code(error),
        E::Tree(error) => kernel_code(error),
        E::FileTable(error) => kernel_code(error),
        E::Regular(error) => regular_code(error),
        E::Directory(error) => kernel_code(error),
    }
}

fn file_read_code(error: &crate::kernel::FileRangeReadError) -> Code {
    use crate::kernel::FileRangeReadError as E;
    match error {
        E::NotRegular | E::InvalidRange => Code::InvalidArgument,
        E::Cancelled => Code::Cancelled,
        E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
        E::Extent(error) => kernel_code(error),
        E::Blob(error) => kernel_code(error),
        E::Context(error) => filesystem_code(error),
    }
}

fn regular_code(error: &crate::kernel::RegularMutationError) -> Code {
    use crate::kernel::RegularMutationError as E;
    match error {
        E::NotRegular
        | E::RangeOverflow
        | E::WrongContentKind
        | E::Inline(_)
        | E::SourceRangeOutOfBounds => Code::InvalidArgument,
        E::SparseSemanticsDisabled | E::KeepSizeBeyondEofUnsupported => Code::FailedPrecondition,
        E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
        E::Codec(_) => Code::DataLoss,
        E::Storage(error) => object_code(error),
        E::BlobBuild(error) => kernel_code(error),
        E::BlobRead(error) => kernel_code(error),
        E::Extent(error) => kernel_code(error),
        E::Range(error) => kernel_code(error),
        E::Clone(error) => kernel_code(error),
        E::Context(error) => filesystem_code(error),
    }
}

fn io_code(error: &std::io::Error) -> Code {
    if let Some(error) = crate::workspace::workspace_bounded_error(error) {
        return workspace_code(&error);
    }
    use std::io::ErrorKind as E;
    match error.kind() {
        E::NotFound => Code::NotFound,
        E::PermissionDenied => Code::PermissionDenied,
        E::AlreadyExists => Code::AlreadyExists,
        E::InvalidInput => Code::InvalidArgument,
        E::InvalidData | E::UnexpectedEof => Code::DataLoss,
        E::TimedOut => Code::DeadlineExceeded,
        E::OutOfMemory | E::StorageFull | E::QuotaExceeded => Code::ResourceExhausted,
        E::Unsupported => Code::Unimplemented,
        // ErrorKind is non-exhaustive. Unknown I/O failures are internal, never
        // evidence for a retryable service outage.
        E::ConnectionRefused
        | E::ConnectionReset
        | E::ConnectionAborted
        | E::NotConnected
        | E::BrokenPipe
        | E::WouldBlock
        | E::Interrupted => Code::Unavailable,
        _ => Code::Internal,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_code(error: &crate::NativeTreeMaterializationError) -> Code {
    use crate::NativeTreeMaterializationError as E;
    match error {
        E::InvalidLayout => Code::DataLoss,
        E::MissingEntry => Code::NotFound,
        E::ExternalMutation | E::PreimageConflict | E::AllocationChanged { .. } => Code::Aborted,
        E::UnexpectedBackup => Code::FailedPrecondition,
        E::UnsupportedRename => Code::Unimplemented,
        E::Cancelled => Code::Cancelled,
        E::OriginalWorkPoisoned | E::MissingCustody => Code::Internal,
        E::Io(error) => io_code(error),
        E::Work(_) | E::AllocationFailed(_) | E::PayloadTooLarge { .. } => Code::ResourceExhausted,
    }
}

#[cfg(any(test, feature = "distributed", feature = "local"))]
fn objects_service_code(error: &acyclic_objects::v1::Error) -> Code {
    use acyclic_objects::v1::wire::ErrorCode as E;
    match error.code {
        E::InvalidArgument => Code::InvalidArgument,
        E::NotFound => Code::NotFound,
        E::AlreadyExists => Code::AlreadyExists,
        E::PreconditionFailed | E::IdempotencyMismatch | E::NotModified => Code::FailedPrecondition,
        E::AccessDenied => Code::PermissionDenied,
        E::QuotaExceeded => Code::ResourceExhausted,
        E::Unavailable => Code::Unavailable,
        E::Unspecified => Code::Internal,
        E::Unsupported => Code::Unimplemented,
        E::RangeNotSatisfiable => Code::OutOfRange,
    }
}

#[cfg(all(any(test, feature = "distributed"), not(target_arch = "wasm32")))]
fn native_objects_code(error: &acyclic_objects::v1::NativeObjectsError) -> Code {
    use acyclic_objects::v1::NativeObjectsCause as E;
    match &error.cause {
        E::Service(error) => objects_service_code(error),
        E::Admission(error) => io_code(error),
        E::Unsupported => Code::Unimplemented,
        E::SharedFailure(error) => native_objects_code(error.error()),
    }
}

#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
fn local_stream_code(error: &acyclic_stream::LocalStreamError) -> Code {
    use acyclic_stream::LocalStreamError as E;
    match error {
        E::IncompleteFrame | E::Corrupt => Code::DataLoss,
        E::Io(error) => io_code(error),
        E::AlreadyOpen => Code::FailedPrecondition,
        E::InvalidLimits => Code::InvalidArgument,
        E::Replay(error) => stream_code(error),
        E::Executor => Code::Unavailable,
    }
}

#[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
fn local_objects_code(error: &acyclic_objects::v1::local::LocalOpenError) -> Code {
    use acyclic_objects::v1::local::LocalOpenError as E;
    match error {
        E::Invalid => Code::InvalidArgument,
        E::Corrupt => Code::DataLoss,
        E::AlreadyOwned => Code::FailedPrecondition,
        E::Unavailable => Code::Unavailable,
        E::Io(error) => io_code(error),
    }
}

#[cfg(test)]
mod tests;
