use super::*;
use crate::kernel::*;
use crate::{CancellationError, WorkCounters, WorkError, WorkspaceNameError};
mod kernel_cases;

// Each named test is a negative request category with an independently stated
// expected wire code. Keeping the diagnostic checks also catches lost causes.
macro_rules! cases {
    ($($name:ident: $error:expr => $code:ident;)+) => {$(
        #[test]
        fn $name() -> Result<(), Box<dyn std::error::Error>> {
            let error: WorkspaceError = $error;
            let response = status(&error);
            assert_eq!(response.code(), Code::$code, "{error:?}");
            assert_eq!(response.message(), error.to_string());
            Ok(())
        }
    )+};
}

fn fs(error: FsError) -> WorkspaceError {
    WorkspaceError::Filesystem(error)
}
fn authority(error: AuthorityStoreError) -> WorkspaceError {
    fs(FsError::Authority(error))
}
fn object(error: ObjectStoreError) -> WorkspaceError {
    fs(FsError::Object(error))
}
fn allocation() -> std::collections::TryReserveError {
    let Err(error) = Vec::<u8>::new().try_reserve(usize::MAX) else {
        unreachable!("usize::MAX cannot be a Vec capacity")
    };
    error
}
fn io(kind: std::io::ErrorKind) -> std::io::Error {
    std::io::Error::new(kind, "original cause")
}

cases! {
    workspace_denied: WorkspaceError::AccessDenied => PermissionDenied;
    workspace_absent: WorkspaceError::NotFound => NotFound;
    workspace_name_empty: WorkspaceError::Name(WorkspaceNameError::Empty) => InvalidArgument;
    workspace_name_reserved: WorkspaceError::Name(WorkspaceNameError::Reserved) => InvalidArgument;
    workspace_name_long: WorkspaceError::Name(WorkspaceNameError::TooLong) => InvalidArgument;
    workspace_name_character: WorkspaceError::Name(WorkspaceNameError::InvalidCharacter) => InvalidArgument;
    workspace_name_allocation: WorkspaceError::Name(WorkspaceNameError::Allocation) => ResourceExhausted;
    workspace_path: WorkspaceError::Path("not canonical".into()) => InvalidArgument;
    workspace_namespace: WorkspaceError::NamespacePath(NamespacePathError::TooDeep) => InvalidArgument;
    workspace_engine: WorkspaceError::Engine("invalid internal state".into()) => Internal;
    workspace_filesystem: fs(FsError::WorkspaceDeleted) => NotFound;
    workspace_cancelled: WorkspaceError::Cancelled(CancellationError) => Cancelled;
    workspace_work: WorkspaceError::Work(WorkError::Overflow) => ResourceExhausted;
    workspace_work_merge: WorkspaceError::WorkMerge {
        prior: Box::new(WorkCounters::UNCHARGED), backend: Box::new(WorkCounters::UNCHARGED), error: WorkError::Overflow
    } => ResourceExhausted;
    workspace_foreign: WorkspaceError::ForeignGeneration => FailedPrecondition;
    workspace_stale_generation: WorkspaceError::StaleGeneration => FailedPrecondition;
    workspace_stale_source: WorkspaceError::StaleSource => FailedPrecondition;
    workspace_stale_identity: WorkspaceError::StaleIdentity => FailedPrecondition;
    workspace_regular: WorkspaceError::NotRegularFile => InvalidArgument;
    workspace_directory: WorkspaceError::NotDirectory => InvalidArgument;
    workspace_is_directory: WorkspaceError::IsDirectory => InvalidArgument;
    workspace_directory_not_empty: WorkspaceError::DirectoryNotEmpty => FailedPrecondition;
    workspace_read_bound: WorkspaceError::ReadLimitExceeded => InvalidArgument;
    workspace_retention_conflict: WorkspaceError::RetentionConflict => FailedPrecondition;
    workspace_incompatible: WorkspaceError::IncompatibleWorkspace => FailedPrecondition;
    workspace_no_ancestor: WorkspaceError::NoCommonAncestor => FailedPrecondition;
    workspace_lineage_bound: WorkspaceError::LineageLimit => FailedPrecondition;
    workspace_join_bound: WorkspaceError::JoinLimit => FailedPrecondition;
    workspace_stale_target: WorkspaceError::StaleTarget => FailedPrecondition;
    workspace_merge_resolution: WorkspaceError::InvalidMergeResolution => InvalidArgument;
    workspace_change_continuity: WorkspaceError::ChangeSetContinuity => FailedPrecondition;
    workspace_changed_path_bound: WorkspaceError::ChangedPathLimit => FailedPrecondition;
    workspace_empty_content: WorkspaceError::EmptyContentSet => InvalidArgument;
    workspace_not_fork: WorkspaceError::NotFork => FailedPrecondition;
    workspace_content_overflow: WorkspaceError::ContentLengthOverflow => InvalidArgument;

    authority_missing: authority(AuthorityStoreError::Missing) => NotFound;
    authority_retired: authority(AuthorityStoreError::Retired) => NotFound;
    authority_cancelled: authority(AuthorityStoreError::Cancelled) => Cancelled;
    authority_stream: authority(AuthorityStoreError::NativeStream(acyclic_stream::StreamError::AccessDenied)) => PermissionDenied;
    authority_codec: authority(AuthorityStoreError::DurableCodec(crate::StreamsAuthorityRecordError::InvalidDomain)) => DataLoss;
    authority_corrupt_static: authority(AuthorityStoreError::CorruptStatic("invalid durable state")) => DataLoss;
    authority_rejected_static: authority(AuthorityStoreError::RejectedStatic("rejected")) => FailedPrecondition;
    authority_replay_limit: authority(AuthorityStoreError::InvalidReplayLimit) => InvalidArgument;
    authority_poisoned: authority(AuthorityStoreError::MemoryAuthorityPoisoned) => Internal;
    authority_replay_ahead: authority(AuthorityStoreError::ReplayCursorAhead {after: crate::Sequence::new(2), head: crate::Sequence::new(1)}) => FailedPrecondition;
    authority_cursor_overflow: authority(AuthorityStoreError::ReplayCursorProcessOverflow {after: crate::Sequence::new(2)}) => InvalidArgument;
    authority_allocation: authority(AuthorityStoreError::MemoryReplayAllocationFailed(allocation())) => ResourceExhausted;
    authority_record_bound: authority(AuthorityStoreError::ReplayRecordTooLarge {observed: 2, maximum: 1}) => ResourceExhausted;
    authority_payload_bound: authority(AuthorityStoreError::PayloadTooLarge {observed: 2, maximum: 1}) => ResourceExhausted;
    authority_epoch: authority(AuthorityStoreError::EpochExhausted) => ResourceExhausted;
    authority_sequence: authority(AuthorityStoreError::SequenceExhausted) => ResourceExhausted;
    authority_reservation: authority(AuthorityStoreError::ReservationOriginalHeadUnavailable {
        authority_id: crate::AuthorityId::from_bytes([1;16]), operation_id: crate::OperationId::from_bytes([2;16]), gate_tail: 1
    }) => FailedPrecondition;
    authority_corrupt: authority(AuthorityStoreError::Corrupt("invalid durable state".into())) => DataLoss;
    authority_io: authority(AuthorityStoreError::Io(io(std::io::ErrorKind::ConnectionRefused))) => Unavailable;
    authority_indeterminate: authority(AuthorityStoreError::Indeterminate {operation: "commit", source: io(std::io::ErrorKind::NotFound)}) => Unavailable;
    authority_rejected: authority(AuthorityStoreError::Rejected("rejected".into())) => FailedPrecondition;
    authority_work: authority(AuthorityStoreError::Work(WorkError::BudgetExceeded { counter: "bytes", observed: 2, maximum: 1 })) => ResourceExhausted;

    object_cancelled: object(ObjectStoreError::Cancelled) => Cancelled;
    object_simulation: object(ObjectStoreError::Simulation("injected outage")) => Unavailable;
    object_missing: object(ObjectStoreError::Missing) => NotFound;
    object_digest: object(ObjectStoreError::DigestMismatch) => InvalidArgument;
    object_empty_write: object(ObjectStoreError::EmptyWriteBatch) => InvalidArgument;
    object_empty_read: object(ObjectStoreError::EmptyReadBatch) => InvalidArgument;
    object_sdk_allocation: object(ObjectStoreError::SdkAllocationFailed) => ResourceExhausted;
    object_table_allocation: object(ObjectStoreError::MemoryTableAllocationFailed(allocation())) => ResourceExhausted;
    object_bound: object(ObjectStoreError::TooLarge { observed: 2, maximum: 1 }) => ResourceExhausted;
    object_range: object(ObjectStoreError::InvalidRange) => InvalidArgument;
    object_corrupt: object(ObjectStoreError::Corrupt) => DataLoss;
    object_quarantine: object(ObjectStoreError::QuarantineFailed(io(std::io::ErrorKind::PermissionDenied))) => DataLoss;
    object_io: object(ObjectStoreError::Io(io(std::io::ErrorKind::TimedOut))) => DeadlineExceeded;
    object_rejected: object(ObjectStoreError::Rejected("rejected".into())) => FailedPrecondition;
    object_work: object(ObjectStoreError::Work(WorkError::Overflow)) => ResourceExhausted;
    object_logical: object(ObjectStoreError::LogicalObjects(acyclic_objects::wire::ErrorCode::AccessDenied.into())) => PermissionDenied;
}

#[cfg(not(target_arch = "wasm32"))]
cases! {
    authority_native_unsupported: authority(AuthorityStoreError::NativeOriginalUnsupported) => Unimplemented;
    authority_native: authority(AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::MissingEntry)) => NotFound;
    object_native_unsupported: object(ObjectStoreError::NativeUnavailable) => Unimplemented;
    object_native: object(ObjectStoreError::Native(crate::NativeTreeMaterializationError::Cancelled)) => Cancelled;
    object_native_objects: object(ObjectStoreError::NativeObjects(acyclic_objects::NativeObjectsCause::Service(acyclic_objects::wire::ErrorCode::QuotaExceeded.into()).into())) => ResourceExhausted;
}

#[test]
fn every_stream_category_survives_the_authority_wrapper() {
    use acyclic_stream::StreamError as E;
    let cases = [
        (E::InvalidPath, Code::InvalidArgument),
        (E::InvalidArgument, Code::InvalidArgument),
        (E::LimitExceeded, Code::InvalidArgument),
        (E::NotFound, Code::NotFound),
        (E::AlreadyExists, Code::AlreadyExists),
        (E::PrefixNotRetained, Code::FailedPrecondition),
        (E::OutOfRange, Code::OutOfRange),
        (E::HierarchyChanged, Code::FailedPrecondition),
        (E::IdempotencyMismatch, Code::FailedPrecondition),
        (E::Capacity, Code::ResourceExhausted),
        (E::AccessDenied, Code::PermissionDenied),
        (E::Unavailable, Code::Unavailable),
        (E::DeadlineElapsed, Code::DeadlineExceeded),
        (E::Unsupported, Code::Unimplemented),
        (E::RevisionForeign, Code::FailedPrecondition),
    ];
    assert_eq!(cases.len(), E::ALL.len());
    for (error, expected) in cases {
        assert_eq!(
            status(&authority(AuthorityStoreError::NativeStream(error))).code(),
            expected
        );
    }
}

#[test]
fn every_objects_service_category_survives_logical_and_native_wrappers() {
    use acyclic_objects::wire::ErrorCode as E;
    for (error, expected) in [
        (E::Unspecified, Code::Internal),
        (E::InvalidArgument, Code::InvalidArgument),
        (E::NotFound, Code::NotFound),
        (E::DigestMismatch, Code::InvalidArgument),
        (E::MissingReference, Code::FailedPrecondition),
        (E::Conflict, Code::FailedPrecondition),
        (E::QuotaExceeded, Code::ResourceExhausted),
        (E::Unsupported, Code::Unimplemented),
        (E::Unavailable, Code::Unavailable),
        (E::AccessDenied, Code::PermissionDenied),
        (E::RangeNotSatisfiable, Code::OutOfRange),
        (E::Corrupt, Code::DataLoss),
        (E::Suspended, Code::Unavailable),
        (E::OutcomeUnknown, Code::Unavailable),
    ] {
        assert_eq!(
            status(&object(ObjectStoreError::LogicalObjects(error.into()))).code(),
            expected
        );
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(
            status(&object(ObjectStoreError::NativeObjects(
                acyclic_objects::Error::from(error).into()
            )))
            .code(),
            expected
        );
    }
}

#[test]
fn transparent_kernel_wrappers_preserve_missing_denied_and_capacity_causes() {
    for (error, expected) in [
        (
            FsError::Directory(DirectoryReadError::Storage(ObjectStoreError::Missing)),
            Code::NotFound,
        ),
        (
            FsError::FileRecord(FileRecordReadError::Storage(ObjectStoreError::Rejected(
                "rejected".into(),
            ))),
            Code::FailedPrecondition,
        ),
        (
            FsError::Path(PathLookupError::Tree(TreeReadError::Storage(
                ObjectStoreError::LogicalObjects(
                    acyclic_objects::wire::ErrorCode::AccessDenied.into(),
                ),
            ))),
            Code::PermissionDenied,
        ),
        (
            FsError::Publication(PublicationError::Authority(AuthorityStoreError::Missing)),
            Code::NotFound,
        ),
        (
            FsError::Closure(ClosureError::Storage(ObjectStoreError::SdkAllocationFailed)),
            Code::ResourceExhausted,
        ),
        (
            FsError::FileRead(FileRangeReadError::Context(Box::new(
                FsError::UnauthenticatedAccessFacts,
            ))),
            Code::Unauthenticated,
        ),
    ] {
        assert_eq!(status(&fs(error)).code(), expected);
    }
}

cases! {
    fs_policy: fs(FsError::InvalidAccessPolicy) => PermissionDenied;
    fs_principal: fs(FsError::UnauthenticatedAccessFacts) => Unauthenticated;
    fs_policy_capacity: fs(FsError::AccessPolicyCacheCapacity) => ResourceExhausted;
    fs_cancelled: fs(FsError::Cancelled(CancellationError)) => Cancelled;
    fs_source_changed: fs(FsError::SourceSnapshotStale) => FailedPrecondition;
    fs_accounting_handoff: fs(FsError::AccountingHandoffActive) => FailedPrecondition;
    fs_handle_allocation: fs(FsError::HandleAllocationFailed) => ResourceExhausted;
    fs_encoding_allocation: fs(FsError::EncodedAllocationFailed(allocation())) => ResourceExhausted;
    fs_config: fs(FsError::Config(crate::model::VolumeConfigError::ZeroLimit)) => InvalidArgument;
    fs_checkout: fs(FsError::CheckoutMode(crate::model::CheckoutModeError::ReadOnlyMutation)) => InvalidArgument;
    fs_durability: fs(FsError::UnsupportedDurability) => FailedPrecondition;
    fs_name_collision: fs(FsError::NameCollision) => AlreadyExists;
    fs_identity_collision: fs(FsError::FileIdentityCollision) => AlreadyExists;
    fs_normalization: fs(FsError::NonNormalizedName) => InvalidArgument;
    fs_unsupported_checkout: fs(FsError::UnsupportedCheckout) => FailedPrecondition;
    fs_live_authority: fs(FsError::LiveRequiresSerializedAuthority) => FailedPrecondition;
    fs_exclusive_writer: fs(FsError::ExclusiveWriterConflict { actual: crate::Head::genesis(crate::Epoch::GENESIS) }) => Aborted;
    fs_live_attempts: fs(FsError::ZeroLiveRetryLimit) => InvalidArgument;
    fs_live_retry: fs(FsError::LiveRetry(LiveRetryError::InvalidTransition)) => Internal;
    fs_pending_live: fs(FsError::PendingLiveMutation) => FailedPrecondition;
    fs_live_conflict: fs(FsError::LiveConflict { conflicts: Vec::new(), truncated: true }) => Aborted;
    fs_authority: authority(AuthorityStoreError::Missing) => NotFound;
    fs_object: object(ObjectStoreError::Missing) => NotFound;
    fs_closure: fs(FsError::Closure(ClosureError::MissingRootFile)) => DataLoss;
    fs_decode: fs(FsError::Decode(CanonicalDecodeError::Truncated)) => DataLoss;
    fs_volume_created: fs(FsError::VolumeCreated(VolumeCreatedError::Decode(CanonicalDecodeError::Truncated))) => DataLoss;
    fs_retention_created: fs(FsError::RetentionCreated(RetentionCreatedError::Decode(CanonicalDecodeError::Truncated))) => DataLoss;
    fs_path: fs(FsError::Path(PathLookupError::TooManyPaths)) => ResourceExhausted;
    fs_directory: fs(FsError::Directory(DirectoryReadError::Cancelled)) => Cancelled;
    fs_record: fs(FsError::FileRecord(FileRecordReadError::TooManyQueries)) => ResourceExhausted;
    fs_file_read: fs(FsError::FileRead(FileRangeReadError::InvalidRange)) => InvalidArgument;
    fs_blob_build: fs(FsError::BlobBuild(BlobBuildError::InvalidOptions)) => InvalidArgument;
    fs_blob_read: fs(FsError::BlobRead(BlobReadError::InvalidRange)) => InvalidArgument;
    fs_attribute_lookup: fs(FsError::AttributeLookup(AttributeLookupError::AllocationFailed)) => ResourceExhausted;
    fs_attribute_list: fs(FsError::AttributeList(DirectoryReadError::ZeroLimit)) => InvalidArgument;
    fs_attribute_mutation: fs(FsError::AttributeMutation(AttributeMutationError::AlreadyExists)) => AlreadyExists;
    fs_inline: fs(FsError::InlineFile(InlineFileDataError::TooLarge)) => InvalidArgument;
    fs_not_found: fs(FsError::NotFound) => NotFound;
    fs_not_directory: fs(FsError::NotDirectory) => InvalidArgument;
    fs_not_link: fs(FsError::NotSymbolicLink) => InvalidArgument;
    fs_not_reparse: fs(FsError::NotReparsePoint) => InvalidArgument;
    fs_special_kind: fs(FsError::InvalidSpecialKind) => InvalidArgument;
    fs_unsupported_kind: fs(FsError::UnsupportedFileKind) => FailedPrecondition;
    fs_directory_record: fs(FsError::InvalidDirectoryRecord) => DataLoss;
    fs_export_manifest: fs(FsError::InvalidExportManifest) => DataLoss;
    fs_transfer: fs(FsError::Transfer(GenerationTransferError::EmptyBatch)) => InvalidArgument;
    fs_dependency: fs(FsError::Dependency(DependencyError::ContradictoryState)) => FailedPrecondition;
    fs_probe: fs(FsError::Probe(AuthenticatedProbeError::CachePoisoned)) => Internal;
    fs_rebase: fs(FsError::Rebase(RebaseError::ZeroConflictLimit)) => InvalidArgument;
    fs_mutation: fs(FsError::Mutation(GenerationMutationError::AlreadyExists)) => AlreadyExists;
    fs_detached_mutation: fs(FsError::DetachedMutation(RegularMutationError::NotRegular)) => InvalidArgument;
    fs_checkpoint: fs(FsError::Checkpoint(CheckpointError::CrossVolumeMerge)) => InvalidArgument;
    fs_publication: fs(FsError::Publication(PublicationError::ActiveProjection)) => FailedPrecondition;
    fs_empty_authority: fs(FsError::EmptyAuthority) => DataLoss;
    fs_authority_history: fs(FsError::InvalidAuthorityHistory) => DataLoss;
    fs_volume_mismatch: fs(FsError::VolumeMismatch) => FailedPrecondition;
    fs_creation: fs(FsError::CreationRejected) => AlreadyExists;
    fs_deleted: fs(FsError::WorkspaceDeleted) => NotFound;
    fs_writable_head: fs(FsError::WritableCheckoutRequiresHead) => FailedPrecondition;
    fs_mutation_denied: fs(FsError::MutationNotAllowed) => FailedPrecondition;
    fs_no_mutations: fs(FsError::NoPendingMutations) => FailedPrecondition;
    fs_refresh: fs(FsError::RefreshNotAllowed) => FailedPrecondition;
    fs_pending_rebase: fs(FsError::PendingMutationsRequireRebase) => FailedPrecondition;
    fs_pending_bound: fs(FsError::TooManyPendingMutations) => ResourceExhausted;
    fs_capacity: fs(FsError::ExcessiveMutationCapacity) => ResourceExhausted;
    fs_mutation_allocation: fs(FsError::PendingMutationAllocationFailed) => ResourceExhausted;
    fs_pending_merge: fs(FsError::PreparedMergePending) => FailedPrecondition;
    fs_diff: fs(FsError::InvalidDiff) => DataLoss;
    fs_diff_allocation: fs(FsError::DiffAllocationFailed) => ResourceExhausted;
    fs_merge_bound: fs(FsError::MergeChangeLimit) => ResourceExhausted;
    fs_merge_head: fs(FsError::MergeParentNotHead) => FailedPrecondition;
    fs_merge_filetable: fs(FsError::MergeFileTable(FileTableMutationError::StateConflict)) => FailedPrecondition;
    fs_merge_tree: fs(FsError::MergeTree(TreeMutationError::Missing)) => NotFound;
    fs_work: fs(FsError::Work(WorkError::Overflow)) => ResourceExhausted;
}

#[cfg(not(target_arch = "wasm32"))]
cases! {
    fs_native_kernel: fs(FsError::NativeKernel(crate::NativeTreeMaterializationError::MissingEntry)) => NotFound;
    fs_native_conflict: fs(FsError::NativeLiveConflict {
        conflicts: Vec::new(), truncated: true,
        allocation: crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(error, _)| error)?.claim(1, 1, WorkCounters::UNCHARGED)?,
    }) => Aborted;
    fs_local_stream: fs(FsError::LocalStream(acyclic_stream::LocalStreamError::Corrupt)) => DataLoss;
    fs_local_objects: fs(FsError::LocalObjects(acyclic_objects::local::LocalOpenError::AlreadyOwned)) => FailedPrecondition;
    fs_local_root: fs(FsError::LocalRoot(io(std::io::ErrorKind::PermissionDenied))) => PermissionDenied;
    fs_local_options: fs(FsError::LocalOptionsConflict) => FailedPrecondition;
    fs_local_worker: fs(FsError::LocalInitializationWorker) => Unavailable;
    fs_object_cache: fs(FsError::ObjectCacheConfig(crate::cache::ObjectCacheConfigError::ZeroBound)) => InvalidArgument;
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn every_native_materialization_category_keeps_its_wire_code() {
    use crate::NativeTreeMaterializationError as E;
    for (error, expected) in [
        (E::InvalidLayout, Code::DataLoss),
        (E::MissingEntry, Code::NotFound),
        (E::ExternalMutation, Code::Aborted),
        (E::UnexpectedBackup, Code::FailedPrecondition),
        (E::UnsupportedRename, Code::Unimplemented),
        (E::Cancelled, Code::Cancelled),
        (E::OriginalWorkPoisoned, Code::Internal),
        (E::MissingCustody, Code::Internal),
        (E::PreimageConflict, Code::Aborted),
        (
            E::Io(io(std::io::ErrorKind::PermissionDenied)),
            Code::PermissionDenied,
        ),
        (E::Work(WorkError::Overflow), Code::ResourceExhausted),
        (E::AllocationFailed(allocation()), Code::ResourceExhausted),
        (
            E::AllocationChanged {
                expected: crate::NativeRootIdentity::from_bytes([1; 16]),
                actual: crate::NativeRootIdentity::from_bytes([2; 16]),
            },
            Code::Aborted,
        ),
        (
            E::PayloadTooLarge {
                maximum: 1,
                actual: 2,
            },
            Code::ResourceExhausted,
        ),
    ] {
        assert_eq!(status(&fs(FsError::NativeKernel(error))).code(), expected);
    }
}

#[test]
fn storage_io_categories_do_not_become_blanket_unavailable() {
    use std::io::ErrorKind as E;
    for (error, expected) in [
        (E::NotFound, Code::NotFound),
        (E::PermissionDenied, Code::PermissionDenied),
        (E::AlreadyExists, Code::AlreadyExists),
        (E::InvalidInput, Code::InvalidArgument),
        (E::InvalidData, Code::DataLoss),
        (E::UnexpectedEof, Code::DataLoss),
        (E::TimedOut, Code::DeadlineExceeded),
        (E::OutOfMemory, Code::ResourceExhausted),
        (E::StorageFull, Code::ResourceExhausted),
        (E::QuotaExceeded, Code::ResourceExhausted),
        (E::Unsupported, Code::Unimplemented),
        (E::ConnectionRefused, Code::Unavailable),
        (E::ConnectionReset, Code::Unavailable),
        (E::ConnectionAborted, Code::Unavailable),
        (E::NotConnected, Code::Unavailable),
        (E::BrokenPipe, Code::Unavailable),
        (E::WouldBlock, Code::Unavailable),
        (E::Interrupted, Code::Unavailable),
        (E::Other, Code::Internal),
    ] {
        assert_eq!(
            status(&authority(AuthorityStoreError::Io(io(error)))).code(),
            expected
        );
        assert_eq!(
            status(&object(ObjectStoreError::Io(io(error)))).code(),
            expected
        );
    }
}

#[tokio::test]
async fn absent_and_deleted_workspaces_are_not_found_through_wire_entrypoints()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::wire_service::{FilesystemWireLimits, FilesystemWireService};
    use acyclic_fs_wire::wire::filesystem::v2 as wire;
    use tonic::Request;
    use wire::filesystem_service_server::FilesystemService;

    let service = FilesystemWireService::new(crate::Fs::memory(), FilesystemWireLimits::default())?;
    let reference = wire::WorkspaceRef {
        workspace_id: vec![1; 16],
        name: "absent".into(),
    };
    for selector in [
        wire::open_workspace_request::Selector::Name("absent".into()),
        wire::open_workspace_request::Selector::Workspace(reference.clone()),
    ] {
        let error = service
            .open_workspace(Request::new(wire::OpenWorkspaceRequest {
                selector: Some(selector),
            }))
            .await
            .err()
            .ok_or("missing workspace was accepted")?;
        assert_eq!(error.code(), Code::NotFound);
    }
    let error = service
        .get_head(Request::new(wire::GetHeadRequest {
            workspace: Some(reference),
        }))
        .await
        .err()
        .ok_or("missing workspace head was accepted")?;
    assert_eq!(error.code(), Code::NotFound);

    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "deleted".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: Some(wire::OperationOptions {
                idempotency_key: vec![2; 16],
            }),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing created workspace")?;
    let reference = created.workspace.ok_or("missing workspace reference")?;
    service
        .delete_workspace(Request::new(wire::DeleteWorkspaceRequest {
            workspace: Some(reference.clone()),
            operation: Some(wire::OperationOptions {
                idempotency_key: vec![3; 16],
            }),
        }))
        .await?;
    for selector in [
        wire::open_workspace_request::Selector::Name("deleted".into()),
        wire::open_workspace_request::Selector::Workspace(reference.clone()),
    ] {
        let error = service
            .open_workspace(Request::new(wire::OpenWorkspaceRequest {
                selector: Some(selector),
            }))
            .await
            .err()
            .ok_or("deleted workspace was accepted")?;
        assert_eq!(error.code(), Code::NotFound);
    }
    let error = service
        .get_head(Request::new(wire::GetHeadRequest {
            workspace: Some(reference),
        }))
        .await
        .err()
        .ok_or("deleted workspace head was accepted")?;
    assert_eq!(error.code(), Code::NotFound);
    Ok(())
}
