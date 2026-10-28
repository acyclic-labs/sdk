use super::*;

// These input cases are independent of the production match arms. Each real
// negative error variant is constructed and checked, including nested causes.
macro_rules! kernel_cases {
    ($name:ident: $kind:ty { $($code:ident => [$($error:expr),+ $(,)?];)+ }) => {
        kernel_cases!($name: $kind, kernel::kernel_code { $($code => [$($error),+];)+ });
    };
    ($name:ident: $kind:ty, $project:path { $($code:ident => [$($error:expr),+ $(,)?];)+ }) => {
        #[test]
        fn $name() {
            type E = $kind;
            $(for error in [$($error),+] {
                assert_eq!($project(&error), Code::$code, "{error:?}");
            })+
        }
    };
}

kernel_cases! { directory: DirectoryReadError {
    Cancelled => [E::Cancelled];
    Internal => [E::TraversalState];
    InvalidArgument => [E::ZeroLimit, E::LimitOverflow, E::InvalidLimits];
    ResourceExhausted => [E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::HeightExceeded, E::Cycle, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
}}
kernel_cases! { namespace: NamespacePathError, namespace_code {
    InvalidArgument => [E::TooDeep, E::ComponentTooLong, E::PathTooLong, E::Name(TreePageError::EmptyName), E::Path(crate::path::PathError::NotAbsolute), E::NonUnicodeProjection];
    ResourceExhausted => [E::Work(WorkError::Overflow), E::AllocationFailed];
    Cancelled => [E::Cancelled];
}}
kernel_cases! { path: PathLookupError, path_code {
    InvalidArgument => [E::Config(crate::model::VolumeConfigError::ZeroLimit), E::EmptyBatch, E::PathBounds, E::NotDirectory, E::NamespacePath(NamespacePathError::TooDeep)];
    ResourceExhausted => [E::TooManyPaths, E::AllocationFailed, E::Work(WorkError::Overflow)];
    FailedPrecondition => [E::UnsupportedNameEncoding];
    DataLoss => [E::MissingRootRecord, E::KindMismatch];
    NotFound => [E::FileTable(FileRecordReadError::Storage(ObjectStoreError::Missing)), E::Tree(TreeReadError::Storage(ObjectStoreError::Missing)), E::Dependency(AuthenticatedProbeError::Storage(ObjectStoreError::Missing))];
}}
kernel_cases! { mutation: GenerationMutationError, mutation_code {
    InvalidArgument => [E::Config(crate::model::VolumeConfigError::ZeroLimit), E::InvalidInitialLinkCount, E::DirectoryCycle, E::NotDirectory, E::IsDirectory, E::Plan(MutationPlanError::Empty)];
    ResourceExhausted => [E::TooManyPaths, E::AllocationFailed, E::Work(WorkError::Overflow)];
    FailedPrecondition => [E::UnsupportedSymbolicLink, E::UnsupportedFileKind, E::UnsupportedSparseMutation, E::UnsupportedHardLink, E::FileIdentityConflict, E::DirectoryNotEmpty];
    NotFound => [E::MissingParent, E::MissingSource, E::Path(PathLookupError::Tree(TreeReadError::Storage(ObjectStoreError::Missing))), E::FileRecord(FileRecordReadError::Storage(ObjectStoreError::Missing)), E::Tree(TreeMutationError::Missing), E::FileTable(FileTableMutationError::Missing), E::Regular(RegularMutationError::Storage(ObjectStoreError::Missing)), E::Directory(DirectoryReadError::Storage(ObjectStoreError::Missing))];
    AlreadyExists => [E::AlreadyExists, E::FileIdentityAlreadyExists];
    DataLoss => [E::InconsistentState];
}}
kernel_cases! { regular: RegularMutationError, regular_code {
    InvalidArgument => [E::NotRegular, E::RangeOverflow, E::WrongContentKind, E::Inline(InlineFileDataError::TooLarge), E::SourceRangeOutOfBounds];
    FailedPrecondition => [E::SparseSemanticsDisabled, E::KeepSizeBeyondEofUnsupported];
    ResourceExhausted => [E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::Codec(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing), E::BlobBuild(BlobBuildError::Storage(ObjectStoreError::Missing)), E::BlobRead(BlobReadError::Storage(ObjectStoreError::Missing)), E::Extent(ExtentMutationError::Storage(ObjectStoreError::Missing)), E::Range(ExtentReadError::Storage(ObjectStoreError::Missing)), E::Clone(ExtentCloneError::Source(ExtentReadError::Storage(ObjectStoreError::Missing)))];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
}}
kernel_cases! { file_read: FileRangeReadError, file_read_code {
    InvalidArgument => [E::NotRegular, E::InvalidRange];
    Cancelled => [E::Cancelled];
    ResourceExhausted => [E::AllocationFailed, E::Work(WorkError::Overflow)];
    NotFound => [E::Extent(ExtentReadError::Storage(ObjectStoreError::Missing)), E::Blob(BlobReadError::Storage(ObjectStoreError::Missing))];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
}}
kernel_cases! { publication: PublicationError {
    ResourceExhausted => [E::ChangeLimit, E::Work(WorkError::Overflow)];
    FailedPrecondition => [E::ActiveProjection, E::AuthorityMismatch {volume_id: crate::VolumeId::from_bytes([1;16]), expected: crate::AuthorityId::from_bytes([1;16]), actual: crate::AuthorityId::from_bytes([2;16])}, E::VolumeMismatch {expected: crate::VolumeId::from_bytes([1;16]), actual: crate::VolumeId::from_bytes([2;16])}];
    PermissionDenied => [E::ChangeValidation(Box::new(FsError::InvalidAccessPolicy)), E::ProjectionValidation(Box::new(FsError::InvalidAccessPolicy))];
    NotFound => [E::Closure(ClosureError::Storage(ObjectStoreError::Missing)), E::Objects(ObjectStoreError::Missing), E::Authority(AuthorityStoreError::Missing)];
}}
kernel_cases! { closure: ClosureError {
    ResourceExhausted => [E::TooManyObjects {observed: 2, maximum: 1}, E::TooManyFiles {observed: 2, maximum: 1}, E::ClosureBytesExceeded {observed: 2, maximum: 1}, E::LogicalBytesOverflow, E::RangeOverflow, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongObjectKind, E::RoutingAlias, E::RoutingBoundsMismatch, E::DuplicateFileRecord(crate::FileId::from_bytes([1;16])), E::MissingRootFile, E::RootIsNotDirectory, E::UnsupportedVolumeSemantics, E::MissingFileRecord(crate::FileId::from_bytes([1;16])), E::NamespaceKindMismatch(crate::FileId::from_bytes([1;16])), E::LinkCountMismatch(crate::FileId::from_bytes([1;16])), E::DirectoryHardLink(crate::FileId::from_bytes([1;16])), E::MissingDirectoryTree(crate::FileId::from_bytes([1;16])), E::UnreachableFileRecords, E::BlobLengthMismatch, E::BlobSpanOutsideObject, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing), E::FileRead(FileRangeReadError::Blob(BlobReadError::Storage(ObjectStoreError::Missing))), E::BlobRead(BlobReadError::Storage(ObjectStoreError::Missing))];
    Cancelled => [E::Native(crate::NativeTreeMaterializationError::Cancelled)];
}}
kernel_cases! { file_record: FileRecordReadError {
    Cancelled => [E::Cancelled];
    InvalidArgument => [E::EmptyBatch, E::InvalidHeightLimit];
    ResourceExhausted => [E::TooManyQueries, E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::Cycle, E::HeightExceeded, E::InvalidRouting, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
}}
kernel_cases! { tree_read: TreeReadError {
    Cancelled => [E::Cancelled];
    InvalidArgument => [E::EmptyBatch, E::InvalidHeightLimit];
    ResourceExhausted => [E::TooManyQueries, E::AllocationFailed, E::PageTooLarge {observed: 2, maximum: 1}, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::Cycle, E::HeightExceeded, E::InvalidRouting, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
}}
kernel_cases! { blob_build: BlobBuildError {
    Cancelled => [E::Cancelled];
    InvalidArgument => [E::InvalidOptions];
    ResourceExhausted => [E::PageTooLarge {observed: 2, maximum: 1}, E::AllocationFailed, E::ReserveFailed(allocation()), E::TooLarge, E::Work(WorkError::Overflow)];
    Unimplemented => [E::NativeSourceUnavailable];
    Internal => [E::InvalidIndexState];
    NotFound => [E::Storage(ObjectStoreError::Missing), E::Source(io(std::io::ErrorKind::NotFound))];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
    DataLoss => [E::Encode(CanonicalDecodeError::Truncated)];
}}
kernel_cases! { blob_read: BlobReadError {
    Cancelled => [E::Cancelled];
    Internal => [E::TraversalState];
    InvalidArgument => [E::InvalidRange, E::InvalidLimits];
    ResourceExhausted => [E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::CycleOrHeight, E::ChildBoundsMismatch, E::IncompleteCoverage, E::ChunkLengthMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
}}
kernel_cases! { attribute_lookup: AttributeLookupError {
    Cancelled => [E::Cancelled];
    InvalidArgument => [E::EmptyBatch, E::InvalidLimits];
    ResourceExhausted => [E::TooManyQueries, E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::HeightExceeded, E::CycleOrAlias, E::ChildBoundsMismatch, E::InvalidRouting, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
}}
kernel_cases! { attribute_mutation: AttributeMutationError {
    InvalidArgument => [E::Empty, E::InvalidLimits, E::InvalidEntry, E::NameMismatch];
    ResourceExhausted => [E::TooManyMutations, E::PageItemTooLarge, E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::HeightExceeded, E::CycleOrAlias, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    Internal => [E::MutationContract];
    AlreadyExists => [E::AlreadyExists];
    NotFound => [E::Missing, E::Storage(ObjectStoreError::Missing)];
    FailedPrecondition => [E::StateConflict];
}}
kernel_cases! { file_table_mutation: FileTableMutationError {
    InvalidArgument => [E::Empty, E::InvalidLimits, E::InvalidRecord, E::IdentityMismatch];
    ResourceExhausted => [E::TooManyMutations, E::PageItemTooLarge, E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::HeightExceeded, E::CycleOrAlias, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    Internal => [E::MutationContract];
    AlreadyExists => [E::AlreadyExists];
    NotFound => [E::Missing, E::Storage(ObjectStoreError::Missing)];
    FailedPrecondition => [E::StateConflict];
}}
kernel_cases! { tree_mutation: TreeMutationError {
    InvalidArgument => [E::Empty, E::InvalidLimits];
    ResourceExhausted => [E::TooManyMutations, E::PageItemTooLarge, E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::HeightExceeded, E::CycleOrAlias, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    Internal => [E::MutationContract];
    AlreadyExists => [E::AlreadyExists];
    NotFound => [E::Missing, E::Storage(ObjectStoreError::Missing)];
    FailedPrecondition => [E::FileIdentityConflict];
}}
kernel_cases! { extent_read: ExtentReadError {
    Cancelled => [E::Cancelled];
    Internal => [E::TraversalState];
    InvalidArgument => [E::InvalidRange, E::InvalidSpanLimit, E::InvalidLimits];
    ResourceExhausted => [E::AllocationFailed, E::TooManySpans, E::PageTooLarge {observed: 2, maximum: 1}, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::Cycle, E::HeightExceeded, E::ChildBoundsMismatch, E::IncompleteCoverage, E::Decode(CanonicalDecodeError::Truncated)];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
}}
kernel_cases! { extent_mutation: ExtentMutationError {
    InvalidArgument => [E::Empty, E::InvalidLimits, E::EmptyRange, E::RangeOverflow, E::OutsideFile, E::WrongContentKind];
    ResourceExhausted => [E::TooManyMutations, E::PageItemTooLarge, E::AllocationFailed, E::Work(WorkError::Overflow)];
    DataLoss => [E::WrongRootKind, E::HeightExceeded, E::CycleOrAlias, E::ChildBoundsMismatch, E::Decode(CanonicalDecodeError::Truncated)];
    Internal => [E::PatchInvariant];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
}}
kernel_cases! { extent_clone: ExtentCloneError {
    InvalidArgument => [E::WrongRootKind, E::EmptyRange, E::RangeOverflow, E::InvalidLimits];
    ResourceExhausted => [E::AllocationFailed, E::Work(WorkError::Overflow)];
    NotFound => [E::Source(ExtentReadError::Storage(ObjectStoreError::Missing)), E::Destination(ExtentMutationError::Storage(ObjectStoreError::Missing))];
}}
kernel_cases! { probe: AuthenticatedProbeError {
    Cancelled => [E::Cancelled];
    InvalidArgument => [E::InvalidLimits, E::ZeroBatchLimit, E::RecordIdentityMismatch, E::InvalidDependency(DependencyError::ZeroLimit)];
    ResourceExhausted => [E::TooManyRegions {maximum: 1}, E::AllocationFailed, E::EncodedAllocationFailed(allocation()), E::ContentRangeTooLarge, E::DirectoryPageTooLarge, E::Work(WorkError::Overflow)];
    Internal => [E::CachePoisoned];
    DataLoss => [E::Decode(CanonicalDecodeError::Truncated)];
    PermissionDenied => [E::Context(Box::new(FsError::InvalidAccessPolicy))];
    NotFound => [E::Storage(ObjectStoreError::Missing), E::FileRecord(FileRecordReadError::Storage(ObjectStoreError::Missing)), E::Tree(TreeReadError::Storage(ObjectStoreError::Missing)), E::Directory(DirectoryReadError::Storage(ObjectStoreError::Missing)), E::Extent(ExtentReadError::Storage(ObjectStoreError::Missing)), E::Blob(BlobReadError::Storage(ObjectStoreError::Missing))];
}}
kernel_cases! { rebase: RebaseError<AuthenticatedProbeError> {
    Cancelled => [E::Cancelled];
    InvalidArgument => [E::ZeroConflictLimit];
    ResourceExhausted => [E::Work(WorkError::Overflow), E::AllocationFailed];
    NotFound => [E::Probe(AuthenticatedProbeError::Storage(ObjectStoreError::Missing))];
}}
kernel_cases! { checkpoint: CheckpointError {
    InvalidArgument => [E::WrongGenerationKind, E::WrongFileTableKind, E::DuplicateParent, E::CrossVolumeMerge, E::RootIdentityMismatch];
    NotFound => [E::Storage(ObjectStoreError::Missing)];
    DataLoss => [E::Decode(CanonicalDecodeError::Truncated)];
    ResourceExhausted => [E::Work(WorkError::Overflow)];
}}
kernel_cases! { transfer: GenerationTransferError {
    Cancelled => [E::Cancelled(CancellationError)];
    DataLoss => [E::ManifestMismatch, E::Manifest(GenerationExportManifestError::MissingRoot), E::Closure(ClosureError::MissingRootFile)];
    InvalidArgument => [E::InvalidCursor, E::EmptyBatch];
    ResourceExhausted => [E::TooManyObjects, E::AllocationFailed, E::Work(WorkError::Overflow)];
    NotFound => [E::Object(ObjectStoreError::Missing)];
}}
kernel_cases! { manifest: GenerationExportManifestError {
    InvalidArgument => [E::Config(crate::model::VolumeConfigError::ZeroLimit)];
    DataLoss => [E::Decode(CanonicalDecodeError::Truncated), E::WrongRootKind, E::GenerationMismatch, E::MissingRoot, E::NonCanonicalObjectOrder];
    ResourceExhausted => [E::TooManyObjects, E::TooManyFiles];
}}
kernel_cases! { decode: CanonicalDecodeError {
    ResourceExhausted => [E::AllocationFailed, E::ObjectTooLarge {observed: 2, maximum: 1}, E::FieldTooLarge {observed: 2, maximum: 1}, E::LengthOverflow, E::Invariant(CanonicalInvariant::Work(WorkError::Overflow))];
    DataLoss => [E::WrongDomain, E::UnsupportedVersion(0), E::Truncated, E::TrailingBytes, E::UnknownTag {field: "kind", tag: 255}, E::Invariant(CanonicalInvariant::Static("invalid"))];
}}

#[test]
fn native_objects_keep_the_service_failure() {
    let error = acyclic_objects::NativeObjectsError::from(acyclic_objects::Error::from(
        acyclic_objects::wire::ErrorCode::AccessDenied,
    ));
    assert!(std::error::Error::source(&error).is_none());
    assert_eq!(
        status(&object(ObjectStoreError::NativeObjects(error))).code(),
        Code::PermissionDenied
    );
}
