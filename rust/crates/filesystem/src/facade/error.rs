use super::*;

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
