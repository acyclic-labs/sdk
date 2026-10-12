//! Native Linux FUSE, macOS NFS, and Windows `ProjFS` projection boundary.

use crate::NativeMaterializationWork;
use crate::demand::NativeMountAllocationCustody;
use crate::kernel::FileMetadata;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use crate::kernel::MetadataField;
use crate::{FileId, MountId, VolumeId};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::time::SystemTime;
use thiserror::Error;

/// Enters one driver callback's `acyclic.fs.mount.<callback>` span; the callback records `errno` on it when it fails.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
macro_rules! callback_span {
    ($level:ident, $callback:ident $(, $field:ident = $value:expr)* $(,)?) => {
        tracing::span!(
            tracing::Level::$level,
            concat!("acyclic.fs.mount.", stringify!($callback)),
            $($field = $value,)*
            errno = crate::obs::Empty,
        )
        .entered()
    };
}

mod adapter;
pub use adapter::{
    CheckoutMountSource, NativeSourceIoLease, SharedCheckout, SharedCheckoutGuard,
    SharedCheckoutState, initialize_native_mount_services,
};

mod view_gate;
pub(crate) use view_gate::ViewGateReadAdmissions;

mod view_ledger;
pub use view_ledger::{ViewObserver, ViewOrigin, ViewStamp};

mod lazy;
pub use lazy::{LazyMountSource, NativeMountSourceWatchOwner};

mod customer;
pub use customer::{
    LazyMount, Mount, MountLifecycleError, MountOptions, MountPublication, NativeMountDrainProof,
    NativeMountPublicationFailure, NativeWorkingSet,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod device;

pub use crate::{
    CaptureError, CaptureOptions, CaptureReceipt, WatchCaptureReceipt, capture_baseline,
    capture_paths, capture_root_identity, capture_subtree, capture_watch_batch,
};

mod materialize;
pub use materialize::materialize_checkout_with_work;
pub use materialize::{
    HostPathReplacement, HostPathRestore, MaterializationReceipt, MaterializeError,
    MaterializeOptions, materialize_checkout, materialize_checkout_paths,
    restore_checkout_host_path,
};
pub(crate) use materialize::{
    MaterializeMode, NativeMaterializationActionPhase,
    materialize_checkout_host_paths_with_work_guarded, materialize_reader_paths,
};

mod publication;
pub use publication::{seal_checkout, seal_checkout_with_permit};

mod storage_capabilities;
pub use storage_capabilities::{
    NativeStorageCapabilities, NativeStorageCapabilityError, probe_native_storage_capabilities,
};

mod storage_accelerations;
pub use storage_accelerations::{
    NativeBlockCloneAccelerationEvidence, NativeSparseAccelerationEvidence,
    NativeStorageAccelerationError, NativeStorageAccelerationEvidence,
    probe_native_storage_accelerations,
};

#[cfg(target_os = "macos")]
mod darwin_mount;
#[cfg(target_os = "macos")]
pub use darwin_mount::DarwinTeardownFailure;
#[cfg(target_os = "linux")]
mod fuse;

#[cfg(target_os = "macos")]
pub use darwin_mount::NativeMountWorkScope;
#[cfg(target_os = "linux")]
pub use fuse::NativeMountWorkScope;
#[cfg(windows)]
pub use projfs::NativeMountWorkScope;
/// Accounting activation is unavailable on this target.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub type NativeMountWorkScope = std::convert::Infallible;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn metadata_or<T>(field: MetadataField<T>, unavailable: T) -> T {
    match field {
        MetadataField::Unavailable => unavailable,
        MetadataField::Value(value) => value,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn system_time_ns(value: SystemTime) -> Result<i64, i32> {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_nanos()).map_err(|_| libc::EOVERFLOW),
        Err(error) => i64::try_from(error.duration().as_nanos())
            .map(|nanos| -nanos)
            .map_err(|_| libc::EOVERFLOW),
    }
}

/// Entries a driver requests from the source per directory page.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
const DIRECTORY_PAGE_SIZE: u32 = 256;
/// Named attributes a driver requests from the source per page.
#[cfg(any(target_os = "linux", target_os = "macos"))]
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
#[cfg(any(target_os = "linux", target_os = "macos"))]
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;

/// Metadata for a node created through a POSIX mount by `uid`/`gid` now.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn create_metadata(mode: u32, kind: u32, uid: u32, gid: u32) -> FileMetadata {
    let now = system_time_ns(SystemTime::now()).unwrap_or(i64::MAX);
    FileMetadata {
        posix_mode: MetadataField::Value((mode & 0o7777) | kind),
        posix_uid: MetadataField::Value(uid),
        posix_gid: MetadataField::Value(gid),
        posix_flags: MetadataField::Value(0),
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Value(now),
        modified_ns: MetadataField::Value(now),
        accessed_ns: MetadataField::Value(now),
        changed_ns: MetadataField::Value(now),
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the error by value"
)]
fn errno(error: MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Allocation(_) => libc::ENOMEM,
        MountSourceError::Io(error, _) => error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NativeWork(error) => native_materialization_errno(error),
        MountSourceError::WorkspaceCause(error) => native_workspace_errno(error),
        MountSourceError::WorkspacePrefixFailure { source, .. } => native_workspace_errno(source),
        MountSourceError::NativeWorkspaceCause(cause , _ ) => native_workspace_errno(cause),
        MountSourceError::NativeCallbackDenied { .. } => libc::EACCES,
        MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ESTALE,
        MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        MountSourceError::Stale | MountSourceError::Cancelled => libc::ESTALE,
        MountSourceError::Engine(_)
        | MountSourceError::Work(_)
        | MountSourceError::Filesystem(_)
        | MountSourceError::ServicesNotInitialized
        | MountSourceError::ServiceUnavailable(_)
        | MountSourceError::Retained(_)
        | MountSourceError::MutationFinalization { .. }
        | MountSourceError::OriginalSnapshotUnavailable
        | MountSourceError::Indeterminate
        | MountSourceError::SelectedPublication(_)
        | MountSourceError::PublicationRetryLimit
        | MountSourceError::NativeRecordJson(_, _)
        | MountSourceError::NativeRecordLockTimeout(_, _)
        | MountSourceError::NativeRecordOwnershipConflict(_, _)
        | MountSourceError::NativeRecordContextTransaction(_, _)
        | MountSourceError::NativeRecordCorrupt(_, _) => libc::EIO,
        MountSourceError::Metadata(_) => libc::EIO,
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        MountSourceError::SourceWatchStartup(_)
        | MountSourceError::NativeWatchOriginalIo(_)
        | MountSourceError::NativeWatchOriginalBackend(_)
        | MountSourceError::NativeWatchStartupFailed { .. }
        | MountSourceError::NativeWatchSourceStartupFailed { .. } => libc::EIO,
    }
}

#[cfg(target_os = "windows")]
mod projfs;
#[cfg(target_os = "windows")]
mod provider_stack;
#[cfg(test)]
mod stack_budget;

#[cfg(target_os = "linux")]
type DriverSession = fuse::FuseSession;
#[cfg(target_os = "macos")]
type DriverSession = darwin_mount::DarwinMountSession;
#[cfg(target_os = "windows")]
type DriverSession = projfs::ProjFsSession;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
type DriverSession = std::convert::Infallible;

macro_rules! with_driver {
    ($driver:expr, $session:ident, $operation:expr, $unsupported:expr $(,)?) => {
        with_driver!($driver, $session, $operation, $operation, $unsupported)
    };
    ($driver:expr, $session:ident, $operation:expr, $windows:expr, $unsupported:expr $(,)?) => {{
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let $session = $driver;
            $operation
        }
        #[cfg(target_os = "windows")]
        {
            let $session = $driver;
            $windows
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            let _ = $driver;
            $unsupported
        }
    }};
}

fn start_driver_into<F>(
    request: &NativeMountRequest,
    source: Arc<dyn MountFilesystem>,
    work: &NativeMaterializationWork,
    out: &mut Option<DriverSession>,
    retirement: &mut Option<NativeMountRetirement>,
    prepare: F,
) -> Result<(), NativeMountError>
where
    F: FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError>,
{
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        #[cfg(target_os = "linux")]
        let start = fuse::FuseSession::start_into;
        #[cfg(target_os = "macos")]
        let start = darwin_mount::DarwinMountSession::start_into;
        #[cfg(windows)]
        let start = projfs::ProjFsSession::start;
        start(request, source, work, out, retirement, prepare)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (&request, &source, &work, &out, &retirement, &prepare);
        Err(NativeMountError::UnsupportedTarget.into())
    }
}

mod source;
pub use source::*;
mod callbacks;
pub use callbacks::*;
mod filesystem;
pub use filesystem::*;
mod error;
pub use error::*;
mod recovery;
pub use recovery::*;
mod startup;
pub use startup::*;
mod session;
pub use session::*;
mod lifecycle;
pub use lifecycle::*;
mod fence;
use fence::*;
#[cfg(test)]
mod tests;
