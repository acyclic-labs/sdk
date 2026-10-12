//! Self-contained macOS mount projection over loopback `NFSv4`.

#![allow(
    unsafe_code,
    reason = "drives the embedded C NFSv4 transport through FFI"
)]

use super::view_ledger::{copy_work, same_work, scan_work};
use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES,
    MountAttributeWriteMode, MountDirectoryEntry, MountFilesystem, MountLookup, MountNodeKind,
    MountOpenFile, MountPath, MountRangeAllocation, MountSeekTarget, MountSourceError,
    NativeMountError, NativeMountRequest, ViewObserver, ViewOrigin, ViewStamp, create_metadata,
    metadata_or, system_time_ns,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use std::collections::{HashMap, VecDeque};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

const ROOT_INODE: u64 = 1;
const MAXIMUM_LOOKUP_CACHE_ENTRIES: usize = 65_536;
const MAXIMUM_LISTING_RESTARTS: usize = 8;
const MAXIMUM_VERIFIED_LABELS: usize = 4_096;
const MAXIMUM_CALLBACK_BYTES: usize = i32::MAX as usize;
const RENAME_NOREPLACE: u32 = 1;
const FALLOC_FL_KEEP_SIZE: c_int = 0x01;
const FALLOC_FL_PUNCH_HOLE: c_int = 0x02;
const FALLOC_FL_ZERO_RANGE: c_int = 0x10;
const DISKUTIL_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(4);
const MOUNT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(10);
const MOUNT_LOOP_EXIT_TIMEOUT: Duration = Duration::from_secs(4);
const DISKUTIL_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(2);
const DIRECT_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(3);
const REPLY_DELIVERY_SLACK: Duration = Duration::from_millis(100);
const CALLBACK_DRAIN_POLL: Duration = Duration::from_millis(1);
mod mode {
    pub(super) const IFMT: u32 = libc::S_IFMT as u32;
    pub(super) const IFIFO: u32 = libc::S_IFIFO as u32;
    pub(super) const IFSOCK: u32 = libc::S_IFSOCK as u32;
    pub(super) const IFCHR: u32 = libc::S_IFCHR as u32;
    pub(super) const IFBLK: u32 = libc::S_IFBLK as u32;
    pub(super) const IFDIR: u32 = libc::S_IFDIR as u32;
    pub(super) const IFLNK: u32 = libc::S_IFLNK as u32;
    pub(super) const IFREG: u32 = libc::S_IFREG as u32;
}

mod transport;
use transport::*;
mod funding;
use funding::*;
mod work_scope;
use work_scope::*;
mod handles;
use handles::*;
mod state;
use state::*;
mod callback_gate;
use callback_gate::*;
mod flush_ledger;
use flush_ledger::*;
mod change_labels;
use change_labels::*;
mod context_ops;
use context_ops::*;
mod session;
use session::*;
mod cleanup;
use cleanup::*;
mod dispatch;
use dispatch::*;
mod file_ops;
use file_ops::*;
mod directory_ops;
use directory_ops::*;
mod namespace_ops;
use namespace_ops::*;
mod attribute_ops;
use attribute_ops::*;
mod paths;
use paths::*;

#[cfg(test)]
mod tests;

pub(super) use cleanup::DarwinRecoveryControl;
pub(super) use session::DarwinMountSession;
pub use work_scope::NativeMountWorkScope;

mod callback_errors;
use callback_errors::*;
mod requests;

mod source_bytes;
use source_bytes::*;

mod file_handles;

mod native_stat;

mod directory_handle;
use directory_handle::*;

mod resources;
use resources::*;

mod startup_buffers;
use startup_buffers::*;

mod teardown_error;
pub use teardown_error::DarwinTeardownFailure;

mod mount_probe;
use mount_probe::*;
mod retirement;
use retirement::*;
mod startup;

const STOP_POLL_INTERVAL: Duration = Duration::from_millis(10);
