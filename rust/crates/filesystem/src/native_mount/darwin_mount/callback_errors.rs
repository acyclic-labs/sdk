use super::*;

pub(super) fn callback_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Source(error)
        | NativeMountError::Demand(crate::DemandError::Mount(error)) => source_errno(error),
        NativeMountError::Materialization(error) => {
            crate::native_mount::native_materialization_errno(error)
        }
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ECANCELED,
        NativeMountError::Allocation(_) => libc::ENOMEM,
        _ => libc::EIO,
    }
}

fn source_errno(error: &MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Stale
        | MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        MountSourceError::Cancelled
        | MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ECANCELED,
        MountSourceError::Allocation(_) => libc::ENOMEM,
        MountSourceError::Io(error, _) => error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NativeWork(error) => {
            crate::native_mount::native_materialization_errno(error)
        }
        MountSourceError::WorkspaceCause(error) => {
            crate::native_mount::native_workspace_errno(error)
        }
        MountSourceError::WorkspacePrefixFailure { source, .. } => {
            crate::native_mount::native_workspace_errno(source)
        }
        MountSourceError::NativeCallbackDenied { .. } => libc::EACCES,
        MountSourceError::NativeWorkspaceCause(error) => {
            crate::native_mount::native_workspace_errno(&error.source)
        }
        MountSourceError::NativeText { kind, .. } => {
            crate::native_mount::native_source_text_errno(*kind)
        }
        _ => libc::EIO,
    }
}

pub(super) fn fatal(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Work(_)
        | NativeMountError::Demand(crate::DemandError::Cancelled)
        | NativeMountError::ControlPending
        | NativeMountError::CallbackDenied { .. }
        | NativeMountError::DriverPanicked { .. } => true,
        NativeMountError::Materialization(error) => {
            crate::native_mount::native_materialization_failure_is_fatal(error)
        }
        NativeMountError::Source(error)
        | NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            match error.semantic_error() {
                MountSourceError::NativeWork(error) => {
                    crate::native_mount::native_materialization_failure_is_fatal(error)
                }
                MountSourceError::WorkspaceCause(error) => {
                    crate::native_mount::native_workspace_failure_is_fatal(error)
                }
                MountSourceError::WorkspacePrefixFailure { source, accounting } => {
                    crate::native_mount::native_workspace_failure_is_fatal(source)
                        || crate::native_mount::native_workspace_failure_is_fatal(accounting)
                }
                MountSourceError::NativeWorkspaceCause(error) => {
                    crate::native_mount::native_workspace_failure_is_fatal(&error.source)
                }
                MountSourceError::Cancelled
                | MountSourceError::Work(_)
                | MountSourceError::OriginalSnapshotUnavailable
                | MountSourceError::Indeterminate
                | MountSourceError::Filesystem(
                    crate::FsError::Cancelled(_) | crate::FsError::Work(_),
                ) => true,
                #[cfg(feature = "native-watch")]
                MountSourceError::SourceWatchStartup(_)
                | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. } => true,
                _ => false,
            }
        }
        _ => false,
    }
}
