use super::super::{
    MountAttributePage, MountAttributeWriteMode, MountDirectoryPage, MountRangeAllocation,
    MountSourceError, ViewObserver,
};
use super::session::{FuseSessionState, run_fuse_session};
use super::{
    InodeEntry, KernelCacheItem, MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY, MountFilesystem,
    MountLookup, MountNode, MountNodeKind, MountOpenFile, MountPath, MountSeekTarget,
    NativeMountError, ProjectionCore, ROOT_INODE, ViewStamp, callback_errno, confirm_name,
    name_is_bound,
};
use crate::FileId;
use crate::kernel::FileMetadata;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

type MemorySource = super::super::CheckoutMountSource<
    crate::facade::MemoryAuthorityBackend,
    crate::facade::MemoryObjectBackend,
>;

fn fuse_fixture_work() -> crate::NativeMaterializationWork {
    cache_work(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
}

fn projection_core(
    source: Arc<dyn MountFilesystem>,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<Arc<super::ProjectionCore>, NativeMountError> {
    let original = source.native_original_work()?;
    let setup = original.clone().unwrap_or_else(fuse_fixture_work);
    let root = source
        .lookup_with_meter(&MountPath::root(), &setup)?
        .ok_or(libc::ENOENT)?;
    let stamp = source.view_stamp_with_meter(&setup)?;
    let work = match original {
        Some(work) => work,
        None => crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?,
    };
    super::ProjectionCore::new(source, root, stamp, true, 0, 0, &work)
}

#[cfg(acyclic_fs_loom)]
pub(super) mod model_checking;

mod cache;
mod folded;
mod folded_property;
mod mount;
mod native;

use cache::*;
use native::*;
