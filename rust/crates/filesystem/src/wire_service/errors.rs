// Keep typed failures intact until this single transport boundary. Every
// variant is named so adding a domain failure requires choosing its wire code.
fn filesystem_status(error: &crate::FsError) -> Status {
    use crate::FsError as F;
    use tonic::Code;
    if let Some(bounded) = crate::workspace::workspace_bounded_error(error) {
        return status(&bounded);
    }
    let code = match error {
        F::Authority(error) => return authority_status(error),
        F::Cancelled(_) => Code::Cancelled,
        F::InvalidAccessPolicy | F::UnauthenticatedAccessFacts => Code::PermissionDenied,
        F::AccessPolicyCacheCapacity
        | F::HandleAllocationFailed
        | F::EncodedAllocationFailed(_)
        | F::TooManyPendingMutations
        | F::ExcessiveMutationCapacity
        | F::PendingMutationAllocationFailed
        | F::DiffAllocationFailed
        | F::MergeChangeLimit
        | F::Work(_) => Code::ResourceExhausted,
        F::NotFound | F::WorkspaceDeleted => Code::NotFound,
        F::NameCollision | F::FileIdentityCollision => Code::AlreadyExists,
        F::Config(_)
        | F::CheckoutMode(_)
        | F::NonNormalizedName
        | F::ZeroLiveRetryLimit
        | F::InlineFile(_)
        | F::NotDirectory
        | F::NotSymbolicLink
        | F::NotReparsePoint
        | F::InvalidSpecialKind => Code::InvalidArgument,
        F::UnsupportedDurability | F::UnsupportedCheckout | F::UnsupportedFileKind => {
            Code::Unimplemented
        }
        F::SourceSnapshotStale
        | F::LiveRequiresSerializedAuthority
        | F::ExclusiveWriterConflict { .. }
        | F::LiveRetry(_)
        | F::PendingLiveMutation
        | F::LiveConflict { .. }
        | F::VolumeMismatch
        | F::CreationRejected
        | F::WritableCheckoutRequiresHead
        | F::MutationNotAllowed
        | F::NoPendingMutations
        | F::RefreshNotAllowed
        | F::PendingMutationsRequireRebase
        | F::PreparedMergePending
        | F::MergeParentNotHead => Code::FailedPrecondition,
        #[cfg(not(target_arch = "wasm32"))]
        F::NativeLiveConflict { .. } => Code::FailedPrecondition,
        F::Closure(_)
        | F::Decode(_)
        | F::VolumeCreated(_)
        | F::RetentionCreated(_)
        | F::InvalidDirectoryRecord
        | F::InvalidExportManifest
        | F::EmptyAuthority
        | F::InvalidAuthorityHistory
        | F::InvalidDiff => Code::DataLoss,
        // Subsystem failures retain their existing availability classification.
        // Nested cancellation and quota causes were extracted by the shared
        // workspace classifier above, before this transport boundary.
        F::Object(_)
        | F::Path(_)
        | F::Directory(_)
        | F::FileRecord(_)
        | F::FileRead(_)
        | F::BlobBuild(_)
        | F::BlobRead(_)
        | F::AttributeLookup(_)
        | F::AttributeList(_)
        | F::AttributeMutation(_)
        | F::Transfer(_)
        | F::Dependency(_)
        | F::Probe(_)
        | F::Rebase(_)
        | F::Mutation(_)
        | F::DetachedMutation(_)
        | F::Checkpoint(_)
        | F::Publication(_)
        | F::MergeFileTable(_)
        | F::MergeTree(_) => Code::Unavailable,
        #[cfg(not(target_arch = "wasm32"))]
        F::NativeKernel(error) => return native_status(error),
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        F::LocalStream(_)
        | F::LocalObjects(_)
        | F::LocalObjectsBucket(_)
        | F::LocalRoot(_)
        | F::LocalStaging(_)
        | F::LocalInitializationWorker => Code::Unavailable,
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        F::LocalOptionsConflict => Code::FailedPrecondition,
        #[cfg(all(any(test, feature = "local"), not(target_arch = "wasm32")))]
        F::ObjectCacheConfig(_) => Code::InvalidArgument,
    };
    Status::new(code, error.to_string())
}

fn authority_status(error: &crate::AuthorityStoreError) -> Status {
    use crate::AuthorityStoreError as A;
    use tonic::Code;
    let code = match error {
        A::Missing | A::Retired => Code::NotFound,
        A::Cancelled => Code::Cancelled,
        A::InvalidReplayLimit | A::ReplayCursorAhead { .. } => Code::InvalidArgument,
        A::ReplayCursorProcessOverflow { .. }
        | A::MemoryReplayAllocationFailed(_)
        | A::ReplayRecordTooLarge { .. }
        | A::PayloadTooLarge { .. }
        | A::EpochExhausted
        | A::SequenceExhausted
        | A::Work(_) => Code::ResourceExhausted,
        A::DurableCodec(_) | A::CorruptStatic(_) | A::Corrupt(_) => Code::DataLoss,
        A::ReservationOriginalHeadUnavailable { .. } => Code::FailedPrecondition,
        A::MemoryAuthorityPoisoned => Code::Internal,
        A::NativeStream(_)
        | A::RejectedStatic(_)
        | A::Rejected(_)
        | A::Io(_)
        | A::Indeterminate { .. } => Code::Unavailable,
        #[cfg(not(target_arch = "wasm32"))]
        A::NativeOriginalUnsupported => Code::Unimplemented,
        #[cfg(not(target_arch = "wasm32"))]
        A::NativeOriginal(error) => return native_status(error),
    };
    Status::new(code, error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_status(error: &crate::NativeTreeMaterializationError) -> Status {
    use crate::NativeTreeMaterializationError as N;
    use tonic::Code;
    let code = match error {
        N::InvalidLayout => Code::InvalidArgument,
        N::MissingEntry => Code::NotFound,
        N::ExternalMutation
        | N::UnexpectedBackup
        | N::PreimageConflict
        | N::AllocationChanged { .. } => Code::FailedPrecondition,
        N::UnsupportedRename => Code::Unimplemented,
        N::Cancelled => Code::Cancelled,
        N::OriginalWorkPoisoned | N::MissingCustody => Code::Internal,
        N::Io(_) => Code::Unavailable,
        N::Work(_) | N::AllocationFailed(_) | N::PayloadTooLarge { .. } => Code::ResourceExhausted,
    };
    Status::new(code, error.to_string())
}
