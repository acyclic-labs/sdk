//! What a failed native callback reports, classified once for every OS.
//!
//! Each backend answers its kernel in its own codes (an errno for FUSE and
//! NFS, an `HRESULT` for `ProjFS`), but what a failure means is decided here,
//! once: a cancellation is always [`CallbackFailure::Cancelled`] however it
//! arrives, and only a stale node, view or source is
//! [`CallbackFailure::Stale`]. An adapter only spells the classification.

use crate::native_mount::{MountSourceError, NativeMountError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallbackFailure {
    NotFound,
    AlreadyExists,
    NotDirectory,
    IsDirectory,
    DirectoryNotEmpty,
    Invalid,
    Unsupported,
    OutOfMemory,
    TooLarge,
    Denied,
    /// A write to a read-only tree.
    ReadOnly,
    Cancelled,
    Stale,
    /// The OS's own code, from a host call that failed with it.
    Os(i32),
    Io,
}

impl CallbackFailure {
    pub(crate) fn of(error: &NativeMountError) -> Self {
        match error {
            NativeMountError::Source(error) => Self::of_source(error),
            NativeMountError::Demand(crate::DemandError::Mount(error)) => Self::of_source(error),
            NativeMountError::Demand(crate::DemandError::Cancelled) => Self::Cancelled,
            NativeMountError::Materialization(error) => Self::of_materialization(error),
            NativeMountError::Io(error) => Self::of_io(error),
            NativeMountError::Allocation(_) => Self::OutOfMemory,
            NativeMountError::CallbackDenied { .. } => Self::Denied,
            #[cfg(target_os = "linux")]
            NativeMountError::FuseUnsupportedCapabilities(_) => Self::Unsupported,
            _ => Self::Io,
        }
    }

    pub(crate) fn of_source(error: &MountSourceError) -> Self {
        match error.semantic_error() {
            MountSourceError::NotFound => Self::NotFound,
            MountSourceError::AlreadyExists => Self::AlreadyExists,
            MountSourceError::NotDirectory => Self::NotDirectory,
            MountSourceError::IsDirectory => Self::IsDirectory,
            MountSourceError::DirectoryNotEmpty => Self::DirectoryNotEmpty,
            MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => Self::Invalid,
            MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => {
                Self::Unsupported
            }
            MountSourceError::Allocation(_) => Self::OutOfMemory,
            MountSourceError::Io(error, _) => Self::of_io(error),
            MountSourceError::NativeWork(error) => Self::of_materialization(error),
            MountSourceError::WorkspaceCause(error) => Self::of_workspace(error),
            MountSourceError::NativeCallbackDenied { .. } | MountSourceError::AccessDenied => {
                Self::Denied
            }
            MountSourceError::ReadOnly => Self::ReadOnly,
            MountSourceError::Filesystem(crate::FsError::Cancelled(_))
            | MountSourceError::Cancelled => Self::Cancelled,
            MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale)
            | MountSourceError::Stale => Self::Stale,
            MountSourceError::Engine(_)
            | MountSourceError::Work(_)
            | MountSourceError::Filesystem(_)
            | MountSourceError::ServicesNotInitialized
            | MountSourceError::ServiceUnavailable(_)
            | MountSourceError::MutationFinalization { .. }
            | MountSourceError::OriginalSnapshotUnavailable
            | MountSourceError::Indeterminate
            | MountSourceError::PublicationRetryLimit
            | MountSourceError::NativeRecordJson(_, _)
            | MountSourceError::NativeRecordLockTimeout(_, _)
            | MountSourceError::NativeRecordOwnershipConflict(_, _)
            | MountSourceError::NativeRecordContextTransaction(_, _)
            | MountSourceError::NativeRecordCorrupt(_, _)
            | MountSourceError::Metadata(_) => Self::Io,
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            MountSourceError::NativeWatchOriginalIo(_)
            | MountSourceError::NativeWatchOriginalBackend(_) => Self::Io,
        }
    }

    pub(crate) fn of_materialization(error: &crate::NativeTreeMaterializationError) -> Self {
        match error {
            crate::NativeTreeMaterializationError::Io(error) => Self::of_io(error),
            crate::NativeTreeMaterializationError::Cancelled => Self::Cancelled,
            crate::NativeTreeMaterializationError::AllocationFailed(_) => Self::OutOfMemory,
            crate::NativeTreeMaterializationError::PayloadTooLarge { .. } => Self::TooLarge,
            _ => Self::Io,
        }
    }

    pub(crate) fn of_workspace(error: &crate::WorkspaceError) -> Self {
        use crate::WorkspaceError;
        use crate::kernel::NamespacePathError;
        match error {
            WorkspaceError::Cancelled(_)
            | WorkspaceError::Filesystem(crate::FsError::Cancelled(_))
            | WorkspaceError::NamespacePath(NamespacePathError::Cancelled) => Self::Cancelled,
            WorkspaceError::Filesystem(crate::FsError::NativeKernel(error)) => {
                Self::of_materialization(error)
            }
            WorkspaceError::NotFound | WorkspaceError::Filesystem(crate::FsError::NotFound) => {
                Self::NotFound
            }
            WorkspaceError::NotDirectory
            | WorkspaceError::Filesystem(crate::FsError::NotDirectory) => Self::NotDirectory,
            WorkspaceError::IsDirectory => Self::IsDirectory,
            WorkspaceError::DirectoryNotEmpty => Self::DirectoryNotEmpty,
            WorkspaceError::NamespacePath(NamespacePathError::Work(_)) => Self::Io,
            WorkspaceError::NamespacePath(NamespacePathError::AllocationFailed)
            | WorkspaceError::Filesystem(crate::FsError::HandleAllocationFailed) => {
                Self::OutOfMemory
            }
            WorkspaceError::Name(_)
            | WorkspaceError::Path(_)
            | WorkspaceError::NotRegularFile
            | WorkspaceError::NamespacePath(_) => Self::Invalid,
            WorkspaceError::ReadLimitExceeded => Self::TooLarge,
            WorkspaceError::StaleGeneration
            | WorkspaceError::StaleSource
            | WorkspaceError::StaleIdentity
            | WorkspaceError::StaleTarget
            | WorkspaceError::Filesystem(crate::FsError::SourceSnapshotStale) => Self::Stale,
            _ => Self::Io,
        }
    }

    fn of_io(error: &std::io::Error) -> Self {
        error.raw_os_error().map_or(Self::Io, Self::Os)
    }

    /// The errno FUSE and NFS answer.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) const fn errno(self) -> i32 {
        match self {
            Self::NotFound => libc::ENOENT,
            Self::AlreadyExists => libc::EEXIST,
            Self::NotDirectory => libc::ENOTDIR,
            Self::IsDirectory => libc::EISDIR,
            Self::DirectoryNotEmpty => libc::ENOTEMPTY,
            Self::Invalid => libc::EINVAL,
            Self::Unsupported => libc::EOPNOTSUPP,
            Self::OutOfMemory => libc::ENOMEM,
            Self::TooLarge => libc::EFBIG,
            Self::Denied => libc::EACCES,
            Self::ReadOnly => libc::EROFS,
            Self::Cancelled => libc::ECANCELED,
            Self::Stale => libc::ESTALE,
            Self::Os(code) => code,
            Self::Io => libc::EIO,
        }
    }
}

/// Whether a failed callback stops the mount, on every backend alike.
///
/// A failure that answers the one request about the tree (a missing or
/// existing name, a wrong kind, an invalid or unsupported request, a stale
/// node, a host I/O error, an access policy's denial or a read-only tree) is
/// only replied. A failure of the mount's own
/// machinery (exhausted or cancelled work, a changed control or meter, a
/// denied callback, failed allocation, a panic, or anything unclassified)
/// is retained, and the mount reports it at its next sync or unmount.
/// `ProjFS` notifications fail only with source errors and ask
/// [`source_stops_mount`] directly.
#[cfg(any(target_os = "linux", target_os = "macos", test))]
pub(crate) fn stops_mount(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Io(_) => false,
        NativeMountError::Source(error) => source_stops_mount(error),
        NativeMountError::Demand(crate::DemandError::Mount(error)) => source_stops_mount(error),
        NativeMountError::Materialization(error) => materialization_stops_mount(error),
        _ => true,
    }
}

pub(crate) fn source_stops_mount(error: &MountSourceError) -> bool {
    use crate::kernel::NamespacePathError;
    match error.semantic_error() {
        MountSourceError::NativeWork(error) => materialization_stops_mount(error),
        MountSourceError::WorkspaceCause(error) => workspace_stops_mount(error),
        MountSourceError::NamespacePath(error) => matches!(
            error,
            NamespacePathError::Work(_)
                | NamespacePathError::Cancelled
                | NamespacePathError::AllocationFailed
        ),
        MountSourceError::Io(..)
        | MountSourceError::AccessDenied
        | MountSourceError::ReadOnly
        | MountSourceError::Stale
        | MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale)
        | MountSourceError::NotFound
        | MountSourceError::AlreadyExists
        | MountSourceError::NotDirectory
        | MountSourceError::IsDirectory
        | MountSourceError::DirectoryNotEmpty
        | MountSourceError::Invalid(_)
        | MountSourceError::Unsupported(_)
        | MountSourceError::NativeUnsupported => false,
        _ => true,
    }
}

fn materialization_stops_mount(error: &crate::NativeTreeMaterializationError) -> bool {
    matches!(
        error,
        crate::NativeTreeMaterializationError::Work(_)
            | crate::NativeTreeMaterializationError::Cancelled
            | crate::NativeTreeMaterializationError::AllocationFailed(_)
            | crate::NativeTreeMaterializationError::ExternalMutation
            | crate::NativeTreeMaterializationError::AllocationChanged { .. }
    )
}

fn workspace_stops_mount(error: &crate::WorkspaceError) -> bool {
    if let crate::WorkspaceError::Filesystem(crate::FsError::NativeKernel(error)) = error {
        return materialization_stops_mount(error);
    }
    matches!(
        CallbackFailure::of_workspace(error),
        CallbackFailure::Cancelled | CallbackFailure::OutOfMemory | CallbackFailure::Io
    )
}

#[cfg(all(test, not(acyclic_fs_loom)))]
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
