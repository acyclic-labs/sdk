use super::*;

impl From<i32> for NativeMountError {
    fn from(error: i32) -> Self {
        Self::Io(std::io::Error::from_raw_os_error(error))
    }
}

pub(super) fn fatal_handler_error(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Work(_)
        | NativeMountError::DriverPanicked { .. }
        | NativeMountError::Allocation(_)
        | NativeMountError::ServicesNotInitialized
        | NativeMountError::ServiceUnavailable(_)
        | NativeMountError::FuseUnsupportedCapabilities(_)
        | NativeMountError::CallbackDenied { .. }
        | NativeMountError::ControlPending
        | NativeMountError::Demand(crate::DemandError::Work(_) | crate::DemandError::Cancelled) => {
            true
        }
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
                MountSourceError::NativeWorkspaceCause(error, _) => {
                    crate::native_mount::native_workspace_failure_is_fatal(error)
                }
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                MountSourceError::SourceWatchStartup(_)
                | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. } => true,
                MountSourceError::Work(_)
                | MountSourceError::Cancelled
                | MountSourceError::Allocation(_)
                | MountSourceError::ServicesNotInitialized
                | MountSourceError::ServiceUnavailable(_)
                | MountSourceError::OriginalSnapshotUnavailable
                | MountSourceError::Indeterminate
                | MountSourceError::Filesystem(
                    crate::FsError::Work(_)
                    | crate::FsError::Cancelled(_)
                    | crate::FsError::HandleAllocationFailed,
                )
                | MountSourceError::NamespacePath(
                    crate::kernel::NamespacePathError::Work(_)
                    | crate::kernel::NamespacePathError::Cancelled
                    | crate::kernel::NamespacePathError::AllocationFailed,
                ) => true,
                _ => false,
            }
        }
        _ => false,
    }
}

pub(super) fn native_callback_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Allocation(_) => libc::ENOMEM,
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Materialization(error) => {
            crate::native_mount::native_materialization_errno(error)
        }
        NativeMountError::Source(error)
        | NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            match error.semantic_error() {
                MountSourceError::NotFound => libc::ENOENT,
                MountSourceError::AlreadyExists => libc::EEXIST,
                MountSourceError::NativeWork(error) => {
                    crate::native_mount::native_materialization_errno(error)
                }
                MountSourceError::WorkspaceCause(error) => {
                    crate::native_mount::native_workspace_errno(error)
                }
                MountSourceError::NativeWorkspaceCause(error, _) => {
                    crate::native_mount::native_workspace_errno(error)
                }
                MountSourceError::Io(error, _) => error.raw_os_error().unwrap_or(libc::EIO),
                MountSourceError::Allocation(_)
                | MountSourceError::Filesystem(crate::FsError::HandleAllocationFailed)
                | MountSourceError::NamespacePath(
                    crate::kernel::NamespacePathError::AllocationFailed,
                ) => libc::ENOMEM,
                MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => {
                    libc::ECANCELED
                }
                MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)) => {
                    libc::EIO
                }
                MountSourceError::PublicationRetryLimit => libc::EIO,
                MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
                MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => {
                    libc::EOPNOTSUPP
                }
                MountSourceError::Stale => libc::ESTALE,
                MountSourceError::Cancelled
                | MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ECANCELED,
                MountSourceError::Filesystem(crate::FsError::NotFound) => libc::ENOENT,
                MountSourceError::Filesystem(crate::FsError::NotDirectory) => libc::ENOTDIR,
                _ => libc::EIO,
            }
        }
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ECANCELED,
        NativeMountError::FuseUnsupportedCapabilities(_) => libc::EOPNOTSUPP,
        _ => libc::EIO,
    }
}
