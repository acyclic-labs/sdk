//! Linux FUSE projection over the common callback contract.

#![allow(unsafe_code, reason = "FUSE mount, descriptor transfer and recovery syscalls")]

use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES,
    MountDirectoryEntry, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest,
    ViewObserver, ViewOrigin, ViewStamp, create_metadata, metadata_or,
    system_time_ns,
};
use crate::kernel::{FileMetadata, MetadataField};
use fuser::{
    BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileAttr,
    FileHandle as FuseFileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags,
    KernelConfig, LockOwner, Notifier, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen,
    ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, RwLockReadGuard,
    RwLockWriteGuard, Weak,
};
use std::time::{Duration, SystemTime};

#[cfg(test)]
static TEST_WRITE_GATE: OnceLock<Mutex<Option<(
    std::sync::mpsc::SyncSender<()>, std::sync::mpsc::Receiver<()>,
)>>> = OnceLock::new();

/// Pauses one FUSE write after handle admission.
#[cfg(test)]
pub(super) fn pause_next_write_after_admission()
    -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::SyncSender<()>) {
    let (admitted_send, admitted_receive) = std::sync::mpsc::sync_channel(0);
    let (release_send, release_receive) = std::sync::mpsc::sync_channel(0);
    *TEST_WRITE_GATE.get_or_init(|| Mutex::new(None)).lock()
        .unwrap_or_else(PoisonError::into_inner) = Some((admitted_send, release_receive));
    (admitted_receive, release_send)
}

#[cfg(test)]
fn pause_test_write() {
    let Some((admitted, release)) = TEST_WRITE_GATE.get_or_init(|| Mutex::new(None))
        .lock().unwrap_or_else(PoisonError::into_inner).take()
    else { return; };
    let _ = admitted.send(());
    let _ = release.recv_timeout(Duration::from_secs(10));
}

const ROOT_INODE: u64 = 1;
static ROOT_PATH: MountPath = MountPath::root();
const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const PAGE_STORE_LIMIT: u64 = 128 * 1024;
const MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY: usize = 1024;
const REQUESTED_CAPABILITIES: InitFlags = InitFlags::FUSE_DO_READDIRPLUS
    .union(InitFlags::FUSE_PARALLEL_DIROPS)
    .union(InitFlags::FUSE_AUTO_INVAL_DATA)
    .union(InitFlags::FUSE_CACHE_SYMLINKS);
use libc::{RENAME_NOREPLACE, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFREG, S_IFSOCK};
const DETACHED_COPY_CHUNK_BYTES: u32 = 1024 * 1024;
const MAXIMUM_DIRECTORY_STREAMS: usize = 256;


mod budget;
use budget::*;

mod attributes;
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
use core::*;
pub(super) use core::FuseStartupMeterScope;
mod retirement;
use retirement::*;
mod errors;
use errors::*;
mod lookup;
use lookup::*;
mod mount;
use mount::*;
mod session;
use session::*;
pub(super) use session::FuseSession;
mod lifecycle;
use lifecycle::*;
pub(super) use lifecycle::FuseRecoveryControl;
mod write;
use write::*;
mod xattr;
use xattr::*;

fn errno_on(span: &tracing::Span, error: i32) -> Errno {
    span.record("errno", error);
    Errno::from_i32(error)
}

macro_rules! respond {
    ($core:expr, $reply:ident, $span:ident, $work:ident, $result:expr, |$value:pat_param| $ok:expr) => {{
        let mut pending_reply = Some($reply);
        let result = $core.request(|$work| {
            let $value = $result?;
            let $reply = pending_reply.take().expect("one successful FUSE reply");
            let _ = $ok;
            Ok(())
        });
        if let Err(error) = $core.finish(result) {
            if let Some(reply) = pending_reply.take() {
                reply.error($crate::native_mount::fuse::errno_on(&$span, error));
            }
        }
    }};
}

mod callbacks;


fn reply_xattr(reply: ReplyXattr, value: &[u8], size: u32, span: &tracing::Span) {
    if size == 0 {
        reply.size(u32::try_from(value.len()).unwrap_or(u32::MAX));
    } else if value.len() <= size as usize {
        reply.data(value);
    } else {
        reply.error(errno_on(span, libc::ERANGE));
    }
}

#[cfg(test)]
mod tests;
