//! Self-contained macOS mount projection over loopback `NFSv4`.

#![allow(
    unsafe_code,
    reason = "drives the embedded C NFSv4 transport through FFI"
)]

use super::view_ledger::{copy_work, same_work, scan_work};
use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES,
    MountAttributeWriteMode, MountFilesystem, MountLookup, MountNodeKind, MountOpenFile, MountPath,
    MountSourceError, NativeMountError, NativeMountRequest, ViewObserver, ViewOrigin, ViewStamp,
    create_metadata, metadata_or, system_time_ns,
};
use crate::kernel::{FileMetadata, MetadataField};
use crate::{FileId, NativeAllocation, NativeMaterializationWork as Work, WorkCounters, WorkError};
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const ROOT_INODE: u64 = 1;
const MAXIMUM_LOOKUP_CACHE_ENTRIES: usize = 65_536;
const MAXIMUM_LISTING_RESTARTS: usize = 8;
const MAXIMUM_VERIFIED_LABELS: usize = 4_096;
const MAXIMUM_CALLBACK_BYTES: usize = i32::MAX as usize;
const DISKUTIL_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(4);
const MOUNT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(10);
const MOUNT_LOOP_EXIT_TIMEOUT: Duration = Duration::from_secs(4);
const DISKUTIL_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(2);
const DIRECT_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(test)]
const REPLY_DELIVERY_SLACK: Duration = Duration::from_millis(100);
const CALLBACK_DRAIN_POLL: Duration = Duration::from_millis(1);

mod work_scope;
use work_scope::*;
mod callback_gate;
use callback_gate::*;
mod change_labels;
use change_labels::*;
mod cleanup;
use cleanup::*;
mod attribute_ops;
mod file_ops;
mod namespace_ops;
mod paths;
#[cfg(feature = "test-support")]
pub(super) use paths::update_time;
use paths::*;

#[cfg(test)]
mod tests;

pub(super) use cleanup::DarwinRecoveryControl;
pub(super) use session::DarwinMountSession;
pub use work_scope::NativeMountWorkScope;

mod teardown_error;
pub use teardown_error::DarwinTeardownFailure;

mod mount_probe;
use mount_probe::*;
mod reclaim;
pub(in crate::native_mount) use reclaim::reclaim_stale_mounts;

const STOP_POLL_INTERVAL: Duration = Duration::from_millis(10);

mod context;
use context::*;

mod requests;
use requests::*;

mod protocol;
use protocol::*;

mod allocation;
use allocation::*;

mod directory;

mod session;
use session::*;

mod startup;
use startup::*;
