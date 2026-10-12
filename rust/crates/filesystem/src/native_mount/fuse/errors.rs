use super::*;

impl From<i32> for NativeMountError {
    fn from(error: i32) -> Self {
        Self::Io(std::io::Error::from_raw_os_error(error))
    }
}

pub(super) fn clone_fatal_source_error(error: &MountSourceError) -> Option<MountSourceError> {
    if let MountSourceError::Retained(owner) = error {
        return Some(MountSourceError::Retained(Arc::clone(owner)));
    }
    Some(match error {
        MountSourceError::Work(error) => MountSourceError::Work(error.clone()),
        MountSourceError::Cancelled => MountSourceError::Cancelled,
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)) => {
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error.clone()))
        }
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled) => {
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled)
        }
        MountSourceError::OriginalSnapshotUnavailable => {
            MountSourceError::OriginalSnapshotUnavailable
        }
        MountSourceError::Indeterminate => MountSourceError::Indeterminate,
        MountSourceError::Allocation(error) => MountSourceError::Allocation(error.clone()),
        MountSourceError::ServicesNotInitialized => MountSourceError::ServicesNotInitialized,
        MountSourceError::ServiceUnavailable(error) => {
            MountSourceError::ServiceUnavailable(Arc::clone(error))
        }
        MountSourceError::Filesystem(crate::FsError::Work(error)) => {
            MountSourceError::Filesystem(crate::FsError::Work(error.clone()))
        }
        MountSourceError::Filesystem(crate::FsError::Cancelled(error)) => {
            MountSourceError::Filesystem(crate::FsError::Cancelled(*error))
        }
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) => {
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(error.clone()))
        }
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => {
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled)
        }
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed) => {
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed)
        }
        _ => return None,
    })
}

pub(super) fn clone_fatal_handler_error(error: &NativeMountError) -> Option<NativeMountError> {
    match error {
        NativeMountError::Work(error) => Some(NativeMountError::Work(error.clone())),
        NativeMountError::Demand(crate::DemandError::Work(error)) => Some(
            NativeMountError::Demand(crate::DemandError::Work(error.clone())),
        ),
        NativeMountError::Demand(crate::DemandError::Cancelled) => {
            Some(NativeMountError::Demand(crate::DemandError::Cancelled))
        }
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error)) => {
            Some(NativeMountError::Materialization(
                crate::NativeTreeMaterializationError::Work(error.clone()),
            ))
        }
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) => {
            Some(NativeMountError::Materialization(
                crate::NativeTreeMaterializationError::Cancelled,
            ))
        }
        NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            clone_fatal_source_error(error)
                .map(|error| NativeMountError::Demand(crate::DemandError::Mount(error)))
        }
        NativeMountError::Source(error) => {
            clone_fatal_source_error(error).map(NativeMountError::Source)
        }
        NativeMountError::FuseUnsupportedCapabilities(flags) => {
            Some(NativeMountError::FuseUnsupportedCapabilities(*flags))
        }
        NativeMountError::Allocation(error) => Some(NativeMountError::Allocation(error.clone())),
        NativeMountError::ServicesNotInitialized => Some(NativeMountError::ServicesNotInitialized),
        NativeMountError::ServiceUnavailable(error) => {
            Some(NativeMountError::ServiceUnavailable(Arc::clone(error)))
        }
        _ => None,
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
                MountSourceError::NativeWorkspaceCause(error) => {
                    crate::native_mount::native_workspace_failure_is_fatal(&error.source)
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
                    crate::FsError::Work(_) | crate::FsError::Cancelled(_),
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
                MountSourceError::NativeWorkspaceCause(error) => {
                    crate::native_mount::native_workspace_errno(&error.source)
                }
                MountSourceError::Io(error, _) => error.raw_os_error().unwrap_or(libc::EIO),
                MountSourceError::Allocation(_)
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
