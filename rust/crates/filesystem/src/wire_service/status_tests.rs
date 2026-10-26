use super::*;
use crate::kernel::*;
use crate::model::*;
use crate::{AuthorityStoreError as A, FsError as F};
use tonic::Code;

fn allocation_failure() -> Result<std::collections::TryReserveError, Box<dyn std::error::Error>> {
    Vec::<u8>::new()
        .try_reserve(usize::MAX)
        .err()
        .ok_or_else(|| "usize::MAX was unexpectedly reservable".into())
}

#[tokio::test]
async fn absent_workspace_is_not_found_over_real_wire() -> Result<(), Box<dyn std::error::Error>> {
    use wire::filesystem_service_client::FilesystemServiceClient;
    use wire::filesystem_service_server::FilesystemServiceServer;
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FilesystemServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let result = async {
        let mut client = FilesystemServiceClient::connect(endpoint).await?;
        let response = client
            .open_workspace(wire::OpenWorkspaceRequest {
                selector: Some(wire::open_workspace_request::Selector::Name(
                    "absent".to_owned(),
                )),
            })
            .await;
        let error = response.err().ok_or("absent workspace was opened")?;
        Ok::<_, Box<dyn std::error::Error>>(error)
    }
    .await;
    let _ = stop.send(());
    server.await??;
    let error = result?;
    assert_eq!(error.code(), Code::NotFound, "{error}");
    Ok(())
}

#[test]
fn every_authority_error_has_an_explicit_status() -> Result<(), Box<dyn std::error::Error>> {
    let cases = vec![
        (A::Cancelled, Code::Cancelled),
        (A::NativeOriginalUnsupported, Code::Unimplemented),
        (
            A::NativeOriginal(crate::NativeTreeMaterializationError::Cancelled),
            Code::Cancelled,
        ),
        (
            A::NativeStream(acyclic_stream::StreamError::Unavailable),
            Code::Unavailable,
        ),
        (
            A::DurableCodec(crate::streams_record::StreamsAuthorityRecordError::InvalidDomain),
            Code::DataLoss,
        ),
        (A::CorruptStatic("corrupt"), Code::DataLoss),
        (A::RejectedStatic("rejected"), Code::Unavailable),
        (A::Missing, Code::NotFound),
        (A::Retired, Code::NotFound),
        (A::InvalidReplayLimit, Code::InvalidArgument),
        (A::MemoryAuthorityPoisoned, Code::Internal),
        (
            A::ReplayCursorAhead {
                after: Sequence::new(2),
                head: Sequence::new(1),
            },
            Code::InvalidArgument,
        ),
        (
            A::ReplayCursorProcessOverflow {
                after: Sequence::new(u64::MAX),
            },
            Code::ResourceExhausted,
        ),
        (
            A::MemoryReplayAllocationFailed(allocation_failure()?),
            Code::ResourceExhausted,
        ),
        (
            A::ReplayRecordTooLarge {
                observed: 2,
                maximum: 1,
            },
            Code::ResourceExhausted,
        ),
        (
            A::PayloadTooLarge {
                observed: 2,
                maximum: 1,
            },
            Code::ResourceExhausted,
        ),
        (A::EpochExhausted, Code::ResourceExhausted),
        (A::SequenceExhausted, Code::ResourceExhausted),
        (
            A::ReservationOriginalHeadUnavailable {
                authority_id: crate::AuthorityId::from_bytes([1; 16]),
                operation_id: crate::OperationId::from_bytes([2; 16]),
                gate_tail: 3,
            },
            Code::FailedPrecondition,
        ),
        (A::Corrupt("corrupt".to_owned()), Code::DataLoss),
        (A::Io(std::io::Error::other("backend")), Code::Unavailable),
        (
            A::Indeterminate {
                operation: "append",
                source: std::io::Error::other("ack"),
            },
            Code::Unavailable,
        ),
        (A::Rejected("rejected".to_owned()), Code::Unavailable),
        (A::Work(crate::WorkError::Overflow), Code::ResourceExhausted),
    ];
    let mut names = std::collections::BTreeSet::new();
    for (error, expected) in &cases {
        assert!(
            names.insert(<&'static str>::from(error)),
            "duplicate {error:?}"
        );
        assert_eq!(authority_status(error).code(), *expected, "{error:?}");
    }
    assert_eq!(names.len(), <A as strum::EnumCount>::COUNT);
    for (error, expected) in cases {
        assert_eq!(filesystem_status(&F::Authority(error)).code(), expected);
    }
    Ok(())
}

fn filesystem_admission_cases(
    work: &crate::NativeMaterializationWork,
) -> Result<Vec<(F, Code)>, Box<dyn std::error::Error>> {
    Ok(vec![
        (F::InvalidAccessPolicy, Code::PermissionDenied),
        (F::UnauthenticatedAccessFacts, Code::PermissionDenied),
        (F::AccessPolicyCacheCapacity, Code::ResourceExhausted),
        (F::Cancelled(crate::CancellationError), Code::Cancelled),
        (
            F::NativeKernel(crate::NativeTreeMaterializationError::Cancelled),
            Code::Cancelled,
        ),
        (F::SourceSnapshotStale, Code::FailedPrecondition),
        (F::HandleAllocationFailed, Code::ResourceExhausted),
        (
            F::EncodedAllocationFailed(allocation_failure()?),
            Code::ResourceExhausted,
        ),
        (
            F::Config(VolumeConfigError::ZeroLimit),
            Code::InvalidArgument,
        ),
        (
            F::LocalStream(acyclic_stream::LocalStreamError::IncompleteFrame),
            Code::Unavailable,
        ),
        (
            F::LocalObjects(acyclic_objects::v1::local::LocalOpenError::Invalid),
            Code::Unavailable,
        ),
        (
            F::LocalObjectsBucket(acyclic_objects::v1::wire::ErrorCode::Unsupported.into()),
            Code::Unavailable,
        ),
        (
            F::LocalRoot(std::io::Error::other("root")),
            Code::Unavailable,
        ),
        (
            F::LocalStaging(std::io::Error::other("staging")),
            Code::Unavailable,
        ),
        (F::LocalOptionsConflict, Code::FailedPrecondition),
        (F::LocalInitializationWorker, Code::Unavailable),
        (
            F::ObjectCacheConfig(crate::ObjectCacheConfigError::ZeroBound),
            Code::InvalidArgument,
        ),
        (
            F::CheckoutMode(CheckoutModeError::ReadOnlyMutation),
            Code::InvalidArgument,
        ),
        (F::UnsupportedDurability, Code::Unimplemented),
        (F::NameCollision, Code::AlreadyExists),
        (F::FileIdentityCollision, Code::AlreadyExists),
        (F::NonNormalizedName, Code::InvalidArgument),
        (F::UnsupportedCheckout, Code::Unimplemented),
        (F::LiveRequiresSerializedAuthority, Code::FailedPrecondition),
        (
            F::ExclusiveWriterConflict {
                actual: crate::Head::genesis(crate::Epoch::GENESIS),
            },
            Code::FailedPrecondition,
        ),
        (F::ZeroLiveRetryLimit, Code::InvalidArgument),
        (
            F::LiveRetry(LiveRetryError::InvalidTransition),
            Code::FailedPrecondition,
        ),
        (F::PendingLiveMutation, Code::FailedPrecondition),
        (
            F::LiveConflict {
                conflicts: vec![],
                truncated: false,
            },
            Code::FailedPrecondition,
        ),
        (
            F::NativeLiveConflict {
                conflicts: vec![],
                truncated: false,
                allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            },
            Code::FailedPrecondition,
        ),
        (F::Authority(A::Missing), Code::NotFound),
    ])
}

fn filesystem_storage_cases() -> Vec<(F, Code)> {
    vec![
        (
            F::Object(crate::ObjectStoreError::Missing),
            Code::Unavailable,
        ),
        (F::Closure(ClosureError::WrongObjectKind), Code::DataLoss),
        (F::Decode(CanonicalDecodeError::WrongDomain), Code::DataLoss),
        (
            F::VolumeCreated(VolumeCreatedError::Config(VolumeConfigError::ZeroLimit)),
            Code::DataLoss,
        ),
        (
            F::RetentionCreated(RetentionCreatedError::InvalidLabel),
            Code::DataLoss,
        ),
        (F::Path(PathLookupError::EmptyBatch), Code::Unavailable),
        (
            F::Directory(DirectoryReadError::TraversalState),
            Code::Unavailable,
        ),
        (
            F::FileRecord(FileRecordReadError::WrongRootKind),
            Code::Unavailable,
        ),
        (
            F::FileRead(FileRangeReadError::NotRegular),
            Code::Unavailable,
        ),
        (
            F::BlobBuild(BlobBuildError::InvalidIndexState),
            Code::Unavailable,
        ),
        (F::BlobRead(BlobReadError::WrongRootKind), Code::Unavailable),
        (
            F::AttributeLookup(AttributeLookupError::WrongRootKind),
            Code::Unavailable,
        ),
        (
            F::AttributeList(DirectoryReadError::WrongRootKind),
            Code::Unavailable,
        ),
        (
            F::AttributeMutation(AttributeMutationError::Empty),
            Code::Unavailable,
        ),
        (
            F::InlineFile(InlineFileDataError::TooLarge),
            Code::InvalidArgument,
        ),
        (F::NotFound, Code::NotFound),
        (F::NotDirectory, Code::InvalidArgument),
        (F::NotSymbolicLink, Code::InvalidArgument),
        (F::NotReparsePoint, Code::InvalidArgument),
        (F::InvalidSpecialKind, Code::InvalidArgument),
        (F::UnsupportedFileKind, Code::Unimplemented),
        (F::InvalidDirectoryRecord, Code::DataLoss),
        (F::InvalidExportManifest, Code::DataLoss),
    ]
}

fn filesystem_mutation_cases() -> Vec<(F, Code)> {
    vec![
        (
            F::Transfer(GenerationTransferError::InvalidCursor),
            Code::Unavailable,
        ),
        (F::Dependency(DependencyError::ZeroLimit), Code::Unavailable),
        (
            F::Probe(AuthenticatedProbeError::InvalidLimits),
            Code::Unavailable,
        ),
        (F::Rebase(RebaseError::ZeroConflictLimit), Code::Unavailable),
        (
            F::Mutation(GenerationMutationError::MissingParent),
            Code::Unavailable,
        ),
        (
            F::DetachedMutation(RegularMutationError::NotRegular),
            Code::Unavailable,
        ),
        (
            F::Checkpoint(CheckpointError::WrongGenerationKind),
            Code::Unavailable,
        ),
        (
            F::Publication(PublicationError::ChangeLimit),
            Code::Unavailable,
        ),
        (F::EmptyAuthority, Code::DataLoss),
        (F::InvalidAuthorityHistory, Code::DataLoss),
        (F::VolumeMismatch, Code::FailedPrecondition),
        (F::CreationRejected, Code::FailedPrecondition),
        (F::WorkspaceDeleted, Code::NotFound),
        (F::WritableCheckoutRequiresHead, Code::FailedPrecondition),
        (F::MutationNotAllowed, Code::FailedPrecondition),
        (F::NoPendingMutations, Code::FailedPrecondition),
        (F::RefreshNotAllowed, Code::FailedPrecondition),
        (F::PendingMutationsRequireRebase, Code::FailedPrecondition),
        (F::TooManyPendingMutations, Code::ResourceExhausted),
        (F::ExcessiveMutationCapacity, Code::ResourceExhausted),
        (F::PendingMutationAllocationFailed, Code::ResourceExhausted),
        (F::PreparedMergePending, Code::FailedPrecondition),
        (F::InvalidDiff, Code::DataLoss),
        (F::DiffAllocationFailed, Code::ResourceExhausted),
        (F::MergeChangeLimit, Code::ResourceExhausted),
        (F::MergeParentNotHead, Code::FailedPrecondition),
        (
            F::MergeFileTable(FileTableMutationError::Empty),
            Code::Unavailable,
        ),
        (F::MergeTree(TreeMutationError::Empty), Code::Unavailable),
        (F::Work(crate::WorkError::Overflow), Code::ResourceExhausted),
    ]
}

#[test]
fn every_filesystem_error_has_an_explicit_status() -> Result<(), Box<dyn std::error::Error>> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let cases = filesystem_admission_cases(&work)?
        .into_iter()
        .chain(filesystem_storage_cases())
        .chain(filesystem_mutation_cases());
    let mut names = std::collections::BTreeSet::new();
    for (error, expected) in cases {
        assert!(
            names.insert(<&'static str>::from(&error)),
            "duplicate {error:?}"
        );
        assert_eq!(filesystem_status(&error).code(), expected, "{error:?}");
        assert_eq!(status(&WorkspaceError::Filesystem(error)).code(), expected);
    }
    assert_eq!(names.len(), <F as strum::EnumCount>::COUNT);
    Ok(())
}
