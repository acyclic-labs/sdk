//! One semantic classification for every native namespace adapter.
use crate::native_mount::{MountSourceError, NativeMountError};

mod retention;
#[cfg(any(test, target_os = "linux", target_os = "macos"))]
pub(crate) use retention::callback_is_terminal;

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

fn filesystem_failure(error: &crate::FsError) -> CallbackFailure {
    use CallbackFailure as F;
    match error {
        crate::FsError::Cancelled(_) => F::Cancelled,
        crate::FsError::SourceSnapshotStale => F::Stale,
        crate::FsError::NativeKernel(error) => materialization_failure(error),
        crate::FsError::NotFound => F::NotFound,
        crate::FsError::NotDirectory => F::NotDirectory,
        crate::FsError::HandleAllocationFailed => F::OutOfMemory,
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

impl CallbackFailure {
    pub(crate) fn of(error: &NativeMountError) -> Self {
        callback_failure(error)
    }
    pub(crate) fn of_source(error: &MountSourceError) -> Self {
        source_failure(error)
    }
}
#[cfg(any(test, target_os = "linux", target_os = "macos"))]
pub(crate) use retention::callback_is_terminal as stops_mount;
pub(crate) use retention::source_is_terminal as source_stops_mount;
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// Every way a cancellation reaches a callback is `Cancelled`, and only
    /// staleness is `Stale`, on every OS; each OS spells both apart.
    #[test]
    fn cancellation_and_staleness_are_classified_once() {
        let cancelled = [
            NativeMountError::Demand(crate::DemandError::Cancelled),
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled),
            NativeMountError::Source(MountSourceError::Cancelled),
            NativeMountError::Source(MountSourceError::Filesystem(crate::FsError::Cancelled(
                crate::CancellationError,
            ))),
            NativeMountError::Source(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled,
            )),
            NativeMountError::Demand(crate::DemandError::Mount(Box::new(
                MountSourceError::Cancelled,
            ))),
            NativeMountError::Source(MountSourceError::WorkspaceCause(
                crate::WorkspaceError::Cancelled(crate::CancellationError),
            )),
        ];
        for error in &cancelled {
            assert_eq!(
                CallbackFailure::of(error),
                CallbackFailure::Cancelled,
                "{error:?}"
            );
        }
        let stale = [
            NativeMountError::Source(MountSourceError::Stale),
            NativeMountError::Source(MountSourceError::Filesystem(
                crate::FsError::SourceSnapshotStale,
            )),
            NativeMountError::Source(MountSourceError::WorkspaceCause(
                crate::WorkspaceError::StaleSource,
            )),
        ];
        for error in &stale {
            assert_eq!(
                CallbackFailure::of(error),
                CallbackFailure::Stale,
                "{error:?}"
            );
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert_eq!(CallbackFailure::Cancelled.errno(), libc::ECANCELED);
            assert_eq!(CallbackFailure::Stale.errno(), libc::ESTALE);
        }
        // ERROR_OPERATION_ABORTED and ERROR_FILE_INVALID.
        #[cfg(windows)]
        {
            assert_eq!(CallbackFailure::Cancelled.hresult().0 & 0xffff, 995);
            assert_eq!(CallbackFailure::Stale.hresult().0 & 0xffff, 1006);
        }
    }

    /// One outcome sequence through every backend's adapter: each answers
    /// its kernel with the spelling of the same classification, and stops
    /// its mount on exactly the same outcomes. FUSE and NFS answer through
    /// `callback_errno`, `ProjFS` through `native_hresult` and
    /// `source_hresult`; FUSE's `finish`, the NFS `fatal` rule and `ProjFS`'s
    /// notification rule are `stops_mount` and `source_stops_mount`.
    #[test]
    fn every_backend_adapter_answers_one_outcome_sequence_alike() {
        use CallbackFailure::{
            Cancelled, Denied, Invalid, Io, NotFound, OutOfMemory, ReadOnly, Stale, Unsupported,
        };
        let source = |error| NativeMountError::Source(error);
        let sequence: [(NativeMountError, CallbackFailure, bool); 13] = [
            (source(MountSourceError::NotFound), NotFound, false),
            (source(MountSourceError::AccessDenied), Denied, false),
            (source(MountSourceError::ReadOnly), ReadOnly, false),
            (
                source(MountSourceError::Invalid("bad".into())),
                Invalid,
                false,
            ),
            (
                source(MountSourceError::Unsupported("no".into())),
                Unsupported,
                false,
            ),
            (source(MountSourceError::Stale), Stale, false),
            (source(MountSourceError::Cancelled), Cancelled, true),
            (
                source(MountSourceError::WorkspaceCause(
                    crate::WorkspaceError::StaleSource,
                )),
                Stale,
                false,
            ),
            (
                NativeMountError::Demand(crate::DemandError::Cancelled),
                Cancelled,
                true,
            ),
            (
                NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled),
                Cancelled,
                true,
            ),
            (
                NativeMountError::Io(std::io::Error::other("host")),
                Io,
                false,
            ),
            (NativeMountError::ControlPending, Io, true),
            (
                NativeMountError::Allocation(Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err()),
                OutOfMemory,
                true,
            ),
        ];
        for (error, expected, stops) in &sequence {
            assert_eq!(CallbackFailure::of(error), *expected, "{error:?}");
            assert_eq!(stops_mount(error), *stops, "{error:?}");
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            assert_eq!(
                super::super::super::callback_errno(error),
                expected.errno(),
                "{error:?}"
            );
            #[cfg(windows)]
            assert_eq!(
                super::super::super::projfs::native_hresult(error),
                expected.hresult(),
                "{error:?}"
            );
            if let NativeMountError::Source(error) = error {
                assert_eq!(source_stops_mount(error), *stops, "{error:?}");
                #[cfg(windows)]
                assert_eq!(
                    super::super::super::projfs::source_hresult(error),
                    expected.hresult(),
                    "{error:?}"
                );
            }
        }
    }
}
