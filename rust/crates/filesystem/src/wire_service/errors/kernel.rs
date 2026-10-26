//! Exhaustive projections of kernel wrappers. `Error::source` alone cannot do
//! this: transparent errors forward past the wrapped enum and can hide a leaf.

use super::*;
use crate::kernel::*;

pub(super) trait KernelStatus {
    fn code(&self) -> Code;
}

pub(super) fn kernel_code(error: &impl KernelStatus) -> Code {
    error.code()
}

macro_rules! project {
    ($kind:ty, {$($arms:tt)*}) => {
        impl KernelStatus for $kind {
            fn code(&self) -> Code {
                type E = $kind;
                match self { $($arms)* }
            }
        }
    };
}
project!(DirectoryReadError, {
    E::Cancelled => Code::Cancelled,
    E::TraversalState => Code::Internal,
    E::ZeroLimit | E::LimitOverflow | E::InvalidLimits => Code::InvalidArgument,
    E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::HeightExceeded | E::Cycle | E::ChildBoundsMismatch => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(VolumeCreatedError, {
    E::Config(_) => Code::InvalidArgument,
    E::Decode(error) => kernel_code(error),
    E::Work(_) => Code::ResourceExhausted,
    E::Cancelled(_) => Code::Cancelled,
});
project!(RetentionCreatedError, {
    E::InvalidLabel | E::WrongObjectKind | E::Config(_) => Code::InvalidArgument,
    E::Decode(error) => kernel_code(error),
    E::Work(_) => Code::ResourceExhausted,
    E::Cancelled(_) => Code::Cancelled,
});
project!(FileRecordReadError, {
    E::Cancelled => Code::Cancelled,
    E::EmptyBatch | E::InvalidHeightLimit => Code::InvalidArgument,
    E::TooManyQueries | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::Cycle | E::HeightExceeded | E::InvalidRouting | E::ChildBoundsMismatch => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(TreeReadError, {
    E::Cancelled => Code::Cancelled,
    E::EmptyBatch | E::InvalidHeightLimit => Code::InvalidArgument,
    E::TooManyQueries | E::AllocationFailed | E::PageTooLarge { .. } | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::Cycle | E::HeightExceeded | E::InvalidRouting | E::ChildBoundsMismatch => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(BlobBuildError, {
    E::Cancelled => Code::Cancelled,
    E::InvalidOptions => Code::InvalidArgument,
    E::PageTooLarge { .. } | E::AllocationFailed | E::ReserveFailed(_) | E::TooLarge | E::Work(_) => Code::ResourceExhausted,
    E::NativeSourceUnavailable => Code::Unimplemented,
    E::InvalidIndexState => Code::Internal,
    E::Source(error) => io_code(error),
    E::Demand(error) => demand_code(error.error()),
    E::Context(error) => filesystem_code(error),
    E::Encode(error) => kernel_code(error),
    E::Storage(error) => object_code(error),
});
project!(BlobReadError, {
    E::Cancelled => Code::Cancelled,
    E::TraversalState => Code::Internal,
    E::InvalidRange | E::InvalidLimits => Code::InvalidArgument,
    E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::CycleOrHeight | E::ChildBoundsMismatch | E::IncompleteCoverage | E::ChunkLengthMismatch => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
    E::Context(error) => filesystem_code(error),
});
project!(AttributeLookupError, {
    E::Cancelled => Code::Cancelled,
    E::EmptyBatch | E::InvalidLimits => Code::InvalidArgument,
    E::TooManyQueries | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::HeightExceeded | E::CycleOrAlias | E::ChildBoundsMismatch | E::InvalidRouting => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(AttributeMutationError, {
    E::Empty | E::InvalidLimits | E::InvalidEntry | E::NameMismatch => Code::InvalidArgument,
    E::TooManyMutations | E::PageItemTooLarge | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::HeightExceeded | E::CycleOrAlias | E::ChildBoundsMismatch => Code::DataLoss,
    E::MutationContract => Code::Internal,
    E::AlreadyExists => Code::AlreadyExists,
    E::Missing => Code::NotFound,
    E::StateConflict => Code::FailedPrecondition,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(FileTableMutationError, {
    E::Empty | E::InvalidLimits | E::InvalidRecord | E::IdentityMismatch => Code::InvalidArgument,
    E::TooManyMutations | E::PageItemTooLarge | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::HeightExceeded | E::CycleOrAlias | E::ChildBoundsMismatch => Code::DataLoss,
    E::MutationContract => Code::Internal,
    E::AlreadyExists => Code::AlreadyExists,
    E::Missing => Code::NotFound,
    E::StateConflict => Code::FailedPrecondition,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(TreeMutationError, {
    E::Empty | E::InvalidLimits => Code::InvalidArgument,
    E::TooManyMutations | E::PageItemTooLarge | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::HeightExceeded | E::CycleOrAlias | E::ChildBoundsMismatch => Code::DataLoss,
    E::MutationContract => Code::Internal,
    E::AlreadyExists => Code::AlreadyExists,
    E::Missing => Code::NotFound,
    E::FileIdentityConflict => Code::FailedPrecondition,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
});
project!(ExtentReadError, {
    E::Cancelled => Code::Cancelled,
    E::TraversalState => Code::Internal,
    E::InvalidRange | E::InvalidSpanLimit | E::InvalidLimits => Code::InvalidArgument,
    E::AllocationFailed | E::TooManySpans | E::PageTooLarge { .. } | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::Cycle | E::HeightExceeded | E::ChildBoundsMismatch | E::IncompleteCoverage => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
    E::Context(error) => filesystem_code(error),
});
project!(ExtentMutationError, {
    E::Empty | E::InvalidLimits | E::EmptyRange | E::RangeOverflow | E::OutsideFile | E::WrongContentKind => Code::InvalidArgument,
    E::TooManyMutations | E::PageItemTooLarge | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::WrongRootKind | E::HeightExceeded | E::CycleOrAlias | E::ChildBoundsMismatch => Code::DataLoss,
    E::PatchInvariant => Code::Internal,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
    E::Context(error) => filesystem_code(error),
});
project!(ExtentCloneError, {
    E::WrongRootKind | E::EmptyRange | E::RangeOverflow | E::InvalidLimits => Code::InvalidArgument,
    E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::Source(error) => kernel_code(error),
    E::Destination(error) => kernel_code(error),
});
project!(AuthenticatedProbeError, {
    E::Cancelled => Code::Cancelled,
    E::InvalidLimits | E::ZeroBatchLimit | E::RecordIdentityMismatch => Code::InvalidArgument,
    E::TooManyRegions { .. } | E::AllocationFailed | E::EncodedAllocationFailed(_)
    | E::ContentRangeTooLarge | E::DirectoryPageTooLarge | E::Work(_) => Code::ResourceExhausted,
    E::CachePoisoned => Code::Internal,
    E::InvalidDependency(error) => filesystem_code(&FsError::Dependency(*error)),
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
    E::FileRecord(error) => kernel_code(error),
    E::Tree(error) => kernel_code(error),
    E::Directory(error) => kernel_code(error),
    E::Extent(error) => kernel_code(error),
    E::Blob(error) => kernel_code(error),
    E::Context(error) => filesystem_code(error),
});
project!(RebaseError<AuthenticatedProbeError>, {
    E::Cancelled => Code::Cancelled,
    E::DependenciesChanged => Code::Aborted,
    E::ZeroConflictLimit => Code::InvalidArgument,
    E::Work(_) | E::AllocationFailed => Code::ResourceExhausted,
    E::Probe(error) => kernel_code(error),
});
project!(CheckpointError, {
    E::WrongGenerationKind | E::WrongFileTableKind | E::DuplicateParent
    | E::CrossVolumeMerge | E::RootIdentityMismatch => Code::InvalidArgument,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
    E::Work(_) => Code::ResourceExhausted,
});
project!(PublicationError, {
    E::ChangeLimit | E::Work(_) => Code::ResourceExhausted,
    E::AuthorityMismatch { .. } | E::VolumeMismatch { .. } | E::ActiveProjection => Code::FailedPrecondition,
    E::GroupAborted => Code::Aborted,
    E::ChangeValidation(error) | E::ProjectionValidation(error) => filesystem_code(error),
    E::Closure(error) => kernel_code(error),
    E::Objects(error) => object_code(error),
    E::Authority(error) => authority_code(error),
});
project!(GenerationTransferError, {
    E::Cancelled(_) => Code::Cancelled,
    E::ManifestMismatch => Code::DataLoss,
    E::InvalidCursor | E::EmptyBatch => Code::InvalidArgument,
    E::TooManyObjects | E::AllocationFailed | E::Work(_) => Code::ResourceExhausted,
    E::Manifest(error) => kernel_code(error),
    E::Closure(error) => kernel_code(error),
    E::Object(error) => object_code(error),
});
project!(GenerationExportManifestError, {
    E::Config(_) => Code::InvalidArgument,
    E::Decode(error) => kernel_code(error),
    E::WrongRootKind | E::GenerationMismatch | E::MissingRoot | E::NonCanonicalObjectOrder => Code::DataLoss,
    E::TooManyObjects | E::TooManyFiles => Code::ResourceExhausted,
});
project!(CanonicalDecodeError, {
    E::AllocationFailed | E::ObjectTooLarge { .. } | E::FieldTooLarge { .. } | E::LengthOverflow => Code::ResourceExhausted,
    E::Invariant(CanonicalInvariant::Work(_)) => Code::ResourceExhausted,
    E::WrongDomain | E::UnsupportedVersion(_) | E::Truncated | E::TrailingBytes
    | E::UnknownTag { .. } | E::Invariant(_) => Code::DataLoss,
});
project!(ClosureError, {
    E::TooManyObjects { .. } | E::TooManyFiles { .. } | E::ClosureBytesExceeded { .. }
    | E::LogicalBytesOverflow | E::RangeOverflow | E::Work(_) => Code::ResourceExhausted,
    E::WrongObjectKind | E::RoutingAlias | E::RoutingBoundsMismatch | E::DuplicateFileRecord(_)
    | E::MissingRootFile | E::RootIsNotDirectory | E::UnsupportedVolumeSemantics
    | E::MissingFileRecord(_) | E::NamespaceKindMismatch(_) | E::LinkCountMismatch(_)
    | E::DirectoryHardLink(_) | E::MissingDirectoryTree(_) | E::UnreachableFileRecords
    | E::BlobLengthMismatch | E::BlobSpanOutsideObject => Code::DataLoss,
    E::Storage(error) => object_code(error),
    E::Decode(error) => kernel_code(error),
    E::FileRead(error) => file_read_code(error),
    E::BlobRead(error) => kernel_code(error),
    #[cfg(not(target_arch = "wasm32"))]
    E::Native(error) => native_code(error),
});

pub(super) fn demand_code(error: &crate::DemandError) -> Code {
    use crate::DemandError as E;
    if let Some(error) = crate::workspace::workspace_bounded_error(error) {
        return workspace_code(&error);
    }
    match error {
        E::StaleSource | E::StaleVersion | E::StaleCursor | E::SourceUnavailable => {
            Code::FailedPrecondition
        }
        E::CursorBusy => Code::Aborted,
        E::Absent => Code::NotFound,
        E::NotRegularFile | E::NotDirectory | E::InvalidRequest => Code::InvalidArgument,
        E::Cancelled => Code::Cancelled,
        E::WorkerUnavailable => Code::Unavailable,
        E::NativeWorkUnavailable => Code::Unimplemented,
        E::OriginalWorkPoisoned => Code::Internal,
        E::Work(_) | E::Allocation(_) => Code::ResourceExhausted,
        E::Io(error) => io_code(error),
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        E::SourceWatchStartup(error) => super::native::startup_code(error),
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        E::Observation(error) => super::native::watch_code(error),
        #[cfg(feature = "native-mount")]
        E::Mount(error) => super::native::mount_code(error),
        #[cfg(all(
            any(target_os = "linux", target_os = "macos", windows),
            any(feature = "native-mount", test)
        ))]
        E::NativeMetadata(_) => Code::InvalidArgument,
    }
}
