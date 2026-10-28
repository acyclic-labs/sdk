//! Linux FUSE projection over the common callback contract.

#![allow(
    unsafe_code,
    reason = "FUSE mount, descriptor transfer and recovery syscalls"
)]

mod guest;
pub use guest::{GuestSessionAdmission, GuestSessionRetired};

use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES,
    MountDirectoryEntry, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest,
    ViewObserver, ViewOrigin, ViewStamp, create_metadata, metadata_or, system_time_ns,
};
use crate::kernel::{FileMetadata, MetadataField};
use crate::lock_order::{
    Condvar as RankedCondvar, Mutex as RankedMutex, MutexGuard as RankedGuard, OPERATION, VIEW,
};
use crate::materializer::NativeResourceCredit::{Copy, Items};
use fuser::{FileAttr, FileType, FopenFlags, INodeNo, InitFlags, TimeOrNow};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime};

#[cfg(test)]
pub(super) use tests::packets::trace_kernel_packets;
#[cfg(test)]
pub(super) use tests::write_gate::pause_next_write_after_admission;

const ROOT_INODE: u64 = 1;
/// Inodes per view of a shared instance. A view's inodes are
/// `serial * VIEW_INODE_SPAN + 1 ..`, so the high bits name the view.
const VIEW_INODE_SPAN: u64 = 1 << 32;
static ROOT_PATH: MountPath = MountPath::root();
const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY: usize = 1024;
const REQUESTED_CAPABILITIES: InitFlags = InitFlags::FUSE_DO_READDIRPLUS
    .union(InitFlags::FUSE_PARALLEL_DIROPS)
    .union(InitFlags::FUSE_AUTO_INVAL_DATA)
    .union(InitFlags::FUSE_CACHE_SYMLINKS);
use libc::{
    RENAME_NOREPLACE, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFREG, S_IFSOCK,
};
const DETACHED_COPY_CHUNK_BYTES: u32 = 1024 * 1024;
const MAXIMUM_DIRECTORY_STREAMS: usize = 256;

mod budget;
use budget::*;

mod attributes;
#[cfg(feature = "test-support")]
pub(super) use attributes::time_or_now_ns;
use attributes::*;

mod handles;
use handles::*;
mod dir;
use dir::*;
mod identity;
use identity::*;
mod read;
use read::*;
mod invalidation;
use invalidation::*;
mod state;
use state::*;
mod core;
pub use core::NativeMountWorkScope;
pub(super) use core::callback_errno;
use core::*;
mod lookup;
use lookup::*;
mod session;
pub(super) use session::FuseSession;
mod lifecycle;
mod mount;
pub(super) use lifecycle::FuseRecoveryControl;
use lifecycle::*;
mod write;
mod xattr;

impl From<i32> for NativeMountError {
    fn from(error: i32) -> Self {
        Self::Io(std::io::Error::from_raw_os_error(error))
    }
}

fn errno_on(span: &tracing::Span, error: i32) -> i32 {
    span.record("errno", error);
    error
}

mod notification;
mod packet;
mod packet_buffers;
mod packet_cache;
mod packet_cancellation;
use packet_cache::{ProjectionCache, ProjectionConfig};
mod packet_dispatch;
mod packet_publication;
mod packet_response;
mod packet_router;
mod packet_session;
mod packet_workers;
mod vhost_control;
mod vhost_session;
mod vring;
mod vring_control;
mod vring_event;
mod vring_memory;
mod vring_packet;
mod vring_worker;
use notification::CacheNotifier;
mod host_io;
mod host_session;
mod shared;
mod shared_instance;
mod transport_io;

#[cfg(test)]
pub(in crate::native_mount) mod tests;
