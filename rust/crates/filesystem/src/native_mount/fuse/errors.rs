use super::*;

#[derive(Debug)]
pub(super) enum FuseRequestError {
    Native(NativeMountError),
    Errno(i32),
}

impl From<FuseRequestError> for NativeMountError {
    fn from(error: FuseRequestError) -> Self {
        match error {
            FuseRequestError::Native(error) => error,
            FuseRequestError::Errno(error) => Self::Io(std::io::Error::from_raw_os_error(error)),
        }
    }
}

impl From<i32> for FuseRequestError {
    fn from(error: i32) -> Self { Self::Errno(error) }
}

impl From<NativeMountError> for FuseRequestError {
    fn from(error: NativeMountError) -> Self { Self::Native(error) }
}

impl From<MountSourceError> for FuseRequestError {
    fn from(error: MountSourceError) -> Self { Self::Native(error.into()) }
}

impl From<crate::DemandError> for FuseRequestError {
    fn from(error: crate::DemandError) -> Self { Self::Native(error.into()) }
}

impl From<crate::WorkError> for FuseRequestError {
    fn from(error: crate::WorkError) -> Self { Self::Native(error.into()) }
}

impl From<crate::NativeTreeMaterializationError> for FuseRequestError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self { Self::Native(error.into()) }
}

impl From<crate::native_mount::NativeMountStartupAdmissionError> for FuseRequestError {
    fn from(error: crate::native_mount::NativeMountStartupAdmissionError) -> Self { Self::Native(error.into()) }
}


pub(super) fn clone_fatal_source_error(error: &MountSourceError) -> Option<MountSourceError> {
    let cause = match error.semantic_error() {
        MountSourceError::Work(error) => MountSourceError::Work(error.clone()),
        MountSourceError::Cancelled => MountSourceError::Cancelled,
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)) =>
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error.clone())),
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled) =>
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled),
        MountSourceError::OriginalSnapshotUnavailable => MountSourceError::OriginalSnapshotUnavailable,
        MountSourceError::Indeterminate => MountSourceError::Indeterminate,
        MountSourceError::Allocation(error) => MountSourceError::Allocation(error.clone()),
        MountSourceError::ServicesNotInitialized => MountSourceError::ServicesNotInitialized,
        MountSourceError::ServiceUnavailable(error) => MountSourceError::ServiceUnavailable(Arc::clone(error)),
        MountSourceError::Filesystem(crate::FsError::Work(error)) =>
            MountSourceError::Filesystem(crate::FsError::Work(error.clone())),
        MountSourceError::Filesystem(crate::FsError::Cancelled(error)) =>
            MountSourceError::Filesystem(crate::FsError::Cancelled(*error)),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) =>
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(error.clone())),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) =>
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed) =>
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed),
        _ => return None,
    };
    if let MountSourceError::Retained(owner) = error {
        Some(MountSourceError::Retained(Arc::clone(owner)))
    } else { Some(cause) }
}

pub(super) fn clone_fatal_handler_error(error: &NativeMountError) -> Option<NativeMountError> {
    match error {
        NativeMountError::Work(error) => Some(NativeMountError::Work(error.clone())),
        NativeMountError::Demand(crate::DemandError::Work(error)) =>
            Some(NativeMountError::Demand(crate::DemandError::Work(error.clone()))),
        NativeMountError::Demand(crate::DemandError::Cancelled) =>
            Some(NativeMountError::Demand(crate::DemandError::Cancelled)),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error)) =>
            Some(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error.clone()))),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) =>
            Some(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled)),
        NativeMountError::Demand(crate::DemandError::Mount(error)) =>
            clone_fatal_source_error(error).map(|error| NativeMountError::Demand(crate::DemandError::Mount(error))),
        NativeMountError::Source(error) => clone_fatal_source_error(error).map(NativeMountError::Source),
        NativeMountError::FuseUnsupportedCapabilities(flags) => Some(NativeMountError::FuseUnsupportedCapabilities(*flags)),
        NativeMountError::Allocation(error) => Some(NativeMountError::Allocation(error.clone())),
        NativeMountError::ServicesNotInitialized => Some(NativeMountError::ServicesNotInitialized),
        NativeMountError::ServiceUnavailable(error) => Some(NativeMountError::ServiceUnavailable(Arc::clone(error))),
        _ => None,
    }
}

pub(super) fn fatal_handler_error(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Work(_) | NativeMountError::DriverPanicked { .. }
        | NativeMountError::Allocation(_) | NativeMountError::ServicesNotInitialized
        | NativeMountError::ServiceUnavailable(_) | NativeMountError::FuseUnsupportedCapabilities(_)
        | NativeMountError::CallbackDenied { .. } | NativeMountError::ControlPending
        | NativeMountError::Demand(crate::DemandError::Work(_) | crate::DemandError::Cancelled) => true,
        NativeMountError::Materialization(error) => crate::native_mount::native_materialization_failure_is_fatal(error),
        NativeMountError::Source(error) | NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            if let MountSourceError::NativeWork(error) = error.semantic_error() {
                return crate::native_mount::native_materialization_failure_is_fatal(error);
            }
            if let MountSourceError::WorkspaceCause(error) = error.semantic_error() {
                return crate::native_mount::native_workspace_failure_is_fatal(error);
            }
            if let MountSourceError::WorkspacePrefixFailure { source, accounting } = error.semantic_error() {
                return crate::native_mount::native_workspace_failure_is_fatal(source)
                    || crate::native_mount::native_workspace_failure_is_fatal(accounting);
            }
            if matches!(error.semantic_error(), MountSourceError::NativeCallbackDenied { .. }) {
                return false;
            }
            if let MountSourceError::NativeWorkspaceCause(error) = error.semantic_error() {
                return crate::native_mount::native_workspace_failure_is_fatal(&error.source);
            }
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            if matches!(error.semantic_error(), MountSourceError::SourceWatchStartup(_)
                | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. })
            {
                return true;
            }
            matches!(error.semantic_error(), MountSourceError::Work(_) | MountSourceError::Cancelled
                | MountSourceError::Allocation(_) | MountSourceError::ServicesNotInitialized
                | MountSourceError::ServiceUnavailable(_) | MountSourceError::OriginalSnapshotUnavailable
                | MountSourceError::Indeterminate
                | MountSourceError::Filesystem(crate::FsError::Work(_) | crate::FsError::Cancelled(_))
                | MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)
                    | crate::kernel::NamespacePathError::Cancelled
                    | crate::kernel::NamespacePathError::AllocationFailed))
        }
        _ => false,
    }
}

pub(super) fn source_callback_errno(error: &MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::NativeWork(error) => crate::native_mount::native_materialization_errno(error),
        MountSourceError::WorkspaceCause(error) => crate::native_mount::native_workspace_errno(error),
        MountSourceError::NativeWorkspaceCause(error) => crate::native_mount::native_workspace_errno(&error.source),
        MountSourceError::NativeText { kind, .. } =>
            crate::native_mount::native_source_text_errno(*kind),
        MountSourceError::Io(error, _) =>
            error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => libc::ECANCELED,
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)) => libc::EIO,
        MountSourceError::PublicationRetryLimit => libc::EIO,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Stale => libc::ESTALE,
        MountSourceError::Cancelled | MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ECANCELED,
        MountSourceError::Filesystem(crate::FsError::NotFound) => libc::ENOENT,
        MountSourceError::Filesystem(crate::FsError::NotDirectory) => libc::ENOTDIR,
        _ => libc::EIO,
    }
}

pub(super) fn native_callback_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Materialization(error) => crate::native_mount::native_materialization_errno(error),
        NativeMountError::Source(error) | NativeMountError::Demand(crate::DemandError::Mount(error)) => source_callback_errno(error),
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ECANCELED,
        NativeMountError::FuseUnsupportedCapabilities(_) => libc::EOPNOTSUPP,
        _ => libc::EIO,
    }
}
