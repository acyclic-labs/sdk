use super::*;

#[cfg(feature = "native-watch")]
pub(super) fn startup_code(error: &crate::source_watch::SourceWatchStartupFailure) -> Code {
    match std::error::Error::source(error)
        .and_then(|error| error.downcast_ref::<crate::DemandError>())
    {
        Some(error) => kernel::demand_code(error),
        None => Code::Internal,
    }
}

#[cfg(feature = "native-watch")]
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
        #[cfg(feature = "native-mount")]
        E::Mount(error) => mount_code(error),
        #[cfg(all(
            any(target_os = "linux", target_os = "macos", windows),
            any(feature = "native-mount", test)
        ))]
        E::Metadata(_) => Code::InvalidArgument,
    }
}

#[cfg(feature = "native-mount")]
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
        E::Work(_) | E::Allocation(_) | E::PublicationRetryLimit => Code::ResourceExhausted,
        E::NativeWork(error) => native_code(error),
        E::Demand(error) => kernel::demand_code(error),
        E::NativeRecordJson(_, _) | E::NativeRecordCorrupt(_, _) => Code::DataLoss,
        E::NativeRecordLockTimeout(_, _) => Code::DeadlineExceeded,
        E::ServiceUnavailable(error) => io_code(error),
        E::MutationFinalization { primary, .. } => match primary.as_ref() {
            Some(error) => mount_code(error),
            None => Code::Internal,
        },
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        E::Metadata(_) => Code::InvalidArgument,
        #[cfg(feature = "native-watch")]
        E::NativeWatchOriginalIo(error) => io_code(error),
        #[cfg(feature = "native-watch")]
        E::NativeWatchOriginalBackend(_) => Code::Unavailable,
    }
}

#[cfg(feature = "native-watch")]
fn capture_code(error: &crate::CaptureError) -> Code {
    use crate::CaptureError as E;
    match error {
        E::InvalidOptions | E::UnrepresentablePath => Code::InvalidArgument,
        E::RootChanged | E::RescanRequired { .. } | E::DirtyCheckout => Code::FailedPrecondition,
        E::UnsupportedKind => Code::Unimplemented,
        E::Kernel(error) => filesystem_code(error),
    }
}
