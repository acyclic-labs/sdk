//! Native Linux FUSE, macOS NFS, and Windows `ProjFS` projection boundary.

use crate::NativeMaterializationWork;
use crate::demand::NativeMountAllocationCustody;
use crate::kernel::FileMetadata;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use crate::kernel::MetadataField;
use crate::{FileId, MountId, VolumeId};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::time::SystemTime;
use thiserror::Error;

/// Enters one driver callback's `acyclic.fs.mount.<callback>` span; the callback records `errno` on it when it fails.
#[cfg(any(target_os = "linux", target_os = "windows"))]
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

mod clock;
#[cfg(any(windows, test))]
mod custody;
#[cfg(all(
    feature = "test-support",
    any(target_os = "linux", target_os = "macos")
))]
#[doc(hidden)]
pub use clock::capture_mount_clock_callbacks;

mod actions;

mod adapter;
#[cfg(windows)]
mod cached_binding;
mod observation;
pub use adapter::{
    CheckoutMountSource, NativeSourceIoLease, SharedCheckout, SharedCheckoutGuard,
    SharedCheckoutState, initialize_native_mount_services,
};

mod view_gate;
pub(crate) use view_gate::ViewGateReadAdmissions;

mod shared;
mod view_ledger;
pub use view_ledger::{ViewObserver, ViewOrigin, ViewStamp};

mod lazy;
pub use lazy::{LazyMountSource, NativeMountSourceWatchOwner};

mod customer;
pub use customer::{
    LazyMount, Mount, MountLifecycleError, MountOptions, MountPublication, NativeMountDrainProof,
    NativeMountPublicationFailure, NativeWorkingSet, native_file_io,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod device;

pub use crate::{
    CaptureError, CaptureOptions, CaptureReceipt, WatchCaptureReceipt, capture_baseline,
    capture_paths, capture_root_identity, capture_subtree, capture_watch_batch,
};

mod materialize;
pub use materialize::materialize_checkout_with_work;
#[cfg(any(fuzzing, feature = "test-support"))]
pub use materialize::verify_native_staging_selection;
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
/// Callback threads one mount runs, whatever the host's core count: enough
/// that callbacks proceed while others block in the source, and a constant
/// cost per live mount.
#[cfg(any(target_os = "linux", target_os = "windows"))]
const CALLBACK_THREADS: u32 = 8;
/// Descriptors one mount's startup holds at once: its destination probe, the
/// device and the session's own copy of it, the mount helper's channel pair
/// and diagnostic file, and the root it validates.
const MOUNT_STARTUP_DESCRIPTORS: usize = 8;

/// Admits one mount's startup descriptors from the process-wide allowance,
/// waiting while other startups and materializations hold it, so mounts that
/// start together cannot exhaust the process's descriptors.
async fn admit_mount_startup()
-> Result<acyclic_native_runtime::DescriptorAdmission, NativeMountError> {
    Ok(acyclic_native_runtime::admit_descriptors(MOUNT_STARTUP_DESCRIPTORS).await?)
}
/// Named attributes a driver requests from the source per page.
#[cfg(any(target_os = "linux", target_os = "macos"))]
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
#[cfg(any(target_os = "linux", target_os = "macos"))]
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;

/// Metadata for a node created through a POSIX mount by `uid`/`gid` now.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn create_metadata(clock: &clock::Clock, mode: u32, kind: u32, uid: u32, gid: u32) -> FileMetadata {
    let now = system_time_ns(clock.now()).unwrap_or(i64::MAX);
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

#[cfg(test)]
#[cfg(test)]
mod outcome;

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(in crate::native_mount) fn callback_errno(error: &NativeMountError) -> i32 {
    shared::outcome::CallbackFailure::of(error).errno()
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
    #[cfg(not(any(target_os = "linux", windows)))]
    if request.shared {
        return Err(NativeMountError::UnsupportedTarget);
    }
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
mod path;
pub use path::MountPath;
mod callbacks;
pub use callbacks::*;
mod filesystem;
pub use filesystem::*;
mod error;
pub use error::*;
mod fence_io;
mod recovery;
#[cfg(any(target_os = "macos", test))]
mod recovery_order;
mod registry;
pub use recovery::*;
pub(crate) use shared::shared_view_destination;
pub use shared::{shared_view_namespace_contains, shared_view_root, shared_view_root_in};
mod startup;
pub use startup::*;
mod session;
pub use session::*;
#[cfg(target_os = "linux")]
mod disconnected;
mod lifecycle;
mod retirement;
#[cfg(target_os = "linux")]
pub use disconnected::{
    recover_disconnected_native_mount_destination, recover_disconnected_native_mount_destination_in,
};
pub use lifecycle::*;
mod fence;
use fence::*;
#[cfg(test)]
pub(crate) mod fixture_access;
#[cfg(test)]
mod tests;
