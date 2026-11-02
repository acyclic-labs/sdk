//! One semantic classification for every native namespace adapter.
use crate::native_mount::{MountSourceError, NativeMountError};

mod retention;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallbackFailure {
    Denied,
    ReadOnly,
    NotFound,
    AlreadyExists,
    NotDirectory,
    IsDirectory,
    DirectoryNotEmpty,
    Invalid,
    Unsupported,
    Stale,
    Cancelled,
    OutOfMemory,
    TooLarge,
    Os(i32),
    Io,
}

impl CallbackFailure {
    fn io(error: &std::io::Error) -> Self {
        error.raw_os_error().map_or(Self::Io, Self::Os)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn errno(self) -> i32 {
        match self {
            Self::Denied => libc::EACCES,
            Self::ReadOnly => libc::EROFS,
            Self::NotFound => libc::ENOENT,
            Self::AlreadyExists => libc::EEXIST,
            Self::NotDirectory => libc::ENOTDIR,
            Self::IsDirectory => libc::EISDIR,
            Self::DirectoryNotEmpty => libc::ENOTEMPTY,
            Self::Invalid => libc::EINVAL,
            Self::Unsupported => libc::EOPNOTSUPP,
            Self::Stale => libc::ESTALE,
            Self::Cancelled => libc::ECANCELED,
            Self::OutOfMemory => libc::ENOMEM,
            Self::TooLarge => libc::EFBIG,
            Self::Os(code) => code,
            Self::Io => libc::EIO,
        }
    }
}

pub(crate) fn source_failure(error: &MountSourceError) -> CallbackFailure {
    use CallbackFailure as F;
    match error.semantic_error() {
        MountSourceError::AccessDenied | MountSourceError::NativeCallbackDenied { .. } => F::Denied,
        MountSourceError::ReadOnly => F::ReadOnly,
        MountSourceError::NotFound => F::NotFound,
        MountSourceError::AlreadyExists => F::AlreadyExists,
        MountSourceError::NotDirectory => F::NotDirectory,
        MountSourceError::IsDirectory => F::IsDirectory,
        MountSourceError::DirectoryNotEmpty => F::DirectoryNotEmpty,
        MountSourceError::Invalid(_) => F::Invalid,
        MountSourceError::NamespacePath(error) => namespace_failure(error),
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => F::Unsupported,
        MountSourceError::Stale => F::Stale,
        MountSourceError::Cancelled => F::Cancelled,
        MountSourceError::Allocation(_) => F::OutOfMemory,
        MountSourceError::Io(error, _) => F::io(error),
        MountSourceError::NativeWork(error) => materialization_failure(error),
        MountSourceError::WorkspaceCause(error) => workspace_failure(error),
        MountSourceError::Filesystem(error) => filesystem_failure(error),
        _ => F::Io,
    }
}

fn namespace_failure(error: &crate::kernel::NamespacePathError) -> CallbackFailure {
    use crate::kernel::NamespacePathError;
    match error {
        NamespacePathError::Cancelled => CallbackFailure::Cancelled,
        NamespacePathError::AllocationFailed => CallbackFailure::OutOfMemory,
        NamespacePathError::Work(_) => CallbackFailure::Io,
        _ => CallbackFailure::Invalid,
    }
}

pub(in crate::native_mount) fn filesystem_failure(error: &crate::FsError) -> CallbackFailure {
    use crate::{FsError as E, kernel::GenerationMutationError as M};
    use CallbackFailure as F;
    match error {
        E::Cancelled(_) => F::Cancelled,
        E::SourceSnapshotStale => F::Stale,
        E::NativeKernel(error) => materialization_failure(error),
        E::CreationRejected | E::Mutation(M::AlreadyExists) => F::AlreadyExists,
        E::NotFound | E::Mutation(M::MissingParent | M::MissingSource) => F::NotFound,
        E::NotDirectory | E::Mutation(M::NotDirectory) => F::NotDirectory,
        E::Mutation(M::IsDirectory) => F::IsDirectory,
        E::Mutation(M::DirectoryNotEmpty) => F::DirectoryNotEmpty,
        E::HandleAllocationFailed => F::OutOfMemory,
        _ => F::Io,
    }
}

pub(crate) fn materialization_failure(
    error: &crate::NativeTreeMaterializationError,
) -> CallbackFailure {
    use CallbackFailure as F;
    match error {
        crate::NativeTreeMaterializationError::Io(error) => F::io(error),
        crate::NativeTreeMaterializationError::Cancelled => F::Cancelled,
        crate::NativeTreeMaterializationError::AllocationFailed(_) => F::OutOfMemory,
        crate::NativeTreeMaterializationError::PayloadTooLarge { .. } => F::TooLarge,
        _ => F::Io,
    }
}

pub(crate) fn workspace_failure(error: &crate::WorkspaceError) -> CallbackFailure {
    use crate::WorkspaceError;
    use CallbackFailure as F;
    match error {
        WorkspaceError::AccessDenied => F::Denied,
        WorkspaceError::Cancelled(_) => F::Cancelled,
        WorkspaceError::Filesystem(error) => filesystem_failure(error),
        WorkspaceError::NotFound => F::NotFound,
        WorkspaceError::NotDirectory => F::NotDirectory,
        WorkspaceError::IsDirectory => F::IsDirectory,
        WorkspaceError::DirectoryNotEmpty => F::DirectoryNotEmpty,
        WorkspaceError::NamespacePath(error) => namespace_failure(error),
        WorkspaceError::Name(_) | WorkspaceError::Path(_) | WorkspaceError::NotRegularFile => {
            F::Invalid
        }
        WorkspaceError::ReadLimitExceeded => F::TooLarge,
        WorkspaceError::StaleGeneration
        | WorkspaceError::StaleSource
        | WorkspaceError::StaleIdentity
        | WorkspaceError::StaleTarget => F::Stale,
        _ => F::Io,
    }
}

pub(crate) fn callback_failure(error: &NativeMountError) -> CallbackFailure {
    use CallbackFailure as F;
    match error {
        NativeMountError::Source(error) => source_failure(error),
        NativeMountError::Demand(crate::DemandError::Mount(error)) => source_failure(error),
        NativeMountError::Materialization(error) => materialization_failure(error),
        NativeMountError::Io(error) => F::io(error),
        NativeMountError::Allocation(_) => F::OutOfMemory,
        NativeMountError::CallbackDenied { .. } => F::Denied,
        NativeMountError::Demand(crate::DemandError::Cancelled) => F::Cancelled,
        #[cfg(target_os = "linux")]
        NativeMountError::FuseUnsupportedCapabilities(_) => F::Unsupported,
        _ => F::Io,
    }
}

#[cfg(any(test, target_os = "linux", target_os = "macos"))]
pub(crate) use retention::callback_is_terminal as stops_mount;
pub(crate) use retention::source_is_terminal as source_stops_mount;
#[cfg(all(test, not(loom)))]
mod tests;
