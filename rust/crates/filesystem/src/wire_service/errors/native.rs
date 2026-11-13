use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn startup_code(error: &crate::source_watch::SourceWatchStartupFailure) -> Code {
    match std::error::Error::source(error)
        .and_then(|error| error.downcast_ref::<crate::DemandError>())
    {
        Some(error) => kernel::demand_code(error),
        None => Code::Internal,
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn watch_code(error: &crate::watch::NativeWatchError) -> Code {
    use crate::watch::NativeWatchError as E;
    match error {
        E::Capture(error) => capture_code(error),
        E::InvalidOptions
        | E::RootIsNotDirectory
        | E::SymbolicLinkDirectory
        | E::ObservedPathNotDirectory
        | E::UnrepresentablePath
        | E::NotRegularFile
        | E::NotDirectory
        | E::InvalidRequest
        | E::ZeroPollLimit => Code::InvalidArgument,
        E::BoundedObservationUnavailable | E::NativeWorkUnavailable => Code::Unimplemented,
        E::RootChanged
        | E::StaleSource
        | E::StaleVersion
        | E::StaleCursor
        | E::SourceUnavailable
        | E::WatchCutMismatch
        | E::RescanOwnerMismatch
        | E::RescanAlreadyInProgress
        | E::RescanInProgress => Code::FailedPrecondition,
        E::CursorBusy => Code::Aborted,
        E::Absent => Code::NotFound,
        E::Io(error) => io_code(error),
        E::Backend(_) | E::WorkerUnavailable => Code::Unavailable,
        E::SourceStartup(error) | E::SourceStartupFailed { cause: error, .. } => {
            startup_code(error)
        }
        E::NativeWork(error) | E::StartupFailed { cause: error, .. } => native_code(error),
        E::StatePoisoned => Code::Internal,
        E::SequenceExhausted | E::Work(_) => Code::ResourceExhausted,
        E::Cancelled(_) => Code::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        E::Mount(error) => mount_code(error),
        #[cfg(all(
            any(target_os = "linux", target_os = "macos", windows),
            any(not(target_arch = "wasm32"), test)
        ))]
        E::Metadata(_) => Code::InvalidArgument,
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn mount_code(error: &crate::native_mount::MountSourceError) -> Code {
    use crate::native_mount::MountSourceError as E;
    match error {
        E::AccessDenied | E::ReadOnly | E::NativeCallbackDenied { .. } => Code::PermissionDenied,
        E::NotFound => Code::NotFound,
        E::AlreadyExists => Code::AlreadyExists,
        E::NotDirectory | E::IsDirectory | E::Invalid(_) => Code::InvalidArgument,
        E::DirectoryNotEmpty
        | E::OriginalSnapshotUnavailable
        | E::Stale
        | E::NativeRecordOwnershipConflict(_, _) => Code::FailedPrecondition,
        E::Unsupported(_) | E::NativeUnsupported => Code::Unimplemented,
        E::Engine(_) | E::NativeRecordContextTransaction(_, _) => Code::Internal,
        E::Filesystem(error) => filesystem_code(error),
        E::WorkspaceCause(error) => workspace_code(error),
        E::NamespacePath(error) => namespace_code(error),
        E::Io(error, _) => io_code(error),
        E::Indeterminate | E::ServicesNotInitialized => Code::Unavailable,
        E::Cancelled => Code::Cancelled,
        E::Work(_) | E::Allocation(_) => Code::ResourceExhausted,
        E::NativeWork(error)
        | E::RetirementFinalization {
            completion: error, ..
        } => native_code(error),
        E::Demand(error) => kernel::demand_code(error),
        E::NativeRecordJson(_, _) | E::NativeRecordCorrupt(_, _) => Code::DataLoss,
        E::ServiceUnavailable(error) => io_code(error),
        E::MutationFinalization { primary, .. } => match primary.as_ref() {
            Some(error) => mount_code(error),
            None => Code::Internal,
        },
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        E::Metadata(_) => Code::InvalidArgument,
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeWatchOriginalIo(error) => io_code(error),
        #[cfg(not(target_arch = "wasm32"))]
        E::NativeWatchOriginalBackend(_) => Code::Unavailable,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn capture_code(error: &crate::CaptureError) -> Code {
    use crate::CaptureError as E;
    match error {
        E::InvalidOptions | E::UnrepresentablePath => Code::InvalidArgument,
        E::RootChanged | E::RescanRequired { .. } | E::DirtyCheckout => Code::FailedPrecondition,
        E::UnsupportedKind => Code::Unimplemented,
        E::Kernel(error) => filesystem_code(error),
        E::Baseline(error) => core_state_code(error),
    }
}

fn core_state_code(error: &crate::LocalCoreStateStoreError) -> Code {
    use crate::LocalCoreStateStoreError as E;
    match error {
        E::DurabilityPoisoned => Code::Unavailable,
        E::Io(error) => io_code(error),
        E::OwnershipConflict(_) | E::MaterializationInProgress => Code::FailedPrecondition,
        E::Json(_) | E::Integrity | E::Corrupt(_) => Code::DataLoss,
        E::LazyWork(error) => lazy_code(error),
    }
}

fn lazy_code(error: &crate::LazyWorkspaceError) -> Code {
    use crate::LazyWorkspaceError as E;
    match error.semantic_error() {
        E::WorkspaceCause(error) => workspace_code(error),
        E::Filesystem(error) => filesystem_code(error),
        E::NamespacePath(error) => namespace_code(error),
        E::Demand(error) => kernel::demand_code(error),
        E::Store(_) | E::NativeLedgerMismatch | E::NativeRecordContextTransaction(_, _) => {
            Code::Internal
        }
        E::Concurrent | E::StaleSource | E::StaleCursor | E::StaleIdentity => Code::Aborted,
        E::NotFound => Code::NotFound,
        E::NotDirectory | E::NotRegularFile | E::InvalidPageBound => Code::InvalidArgument,
        E::TooLarge => Code::ResourceExhausted,
        E::UnsupportedNode => Code::Unimplemented,
        E::Cancelled => Code::Cancelled,
        E::OriginalSnapshotUnavailable
        | E::NativeRecordOwnershipConflict(_, _)
        | E::NativeRecordMaterializationInProgress => Code::FailedPrecondition,
        E::SourcePageCleanup { primary, .. } => lazy_code(primary),
        E::NativeWork(error) => native_code(error),
        E::NativeRecordIo(error, _) => io_code(error),
        E::NativeRecordJson(_, _) | E::NativeRecordCorrupt(_, _) | E::NativeRecordIntegrity => {
            Code::DataLoss
        }
    }
}
