use super::super::{
    MountAttributePage, MountAttributeWriteMode, MountDirectoryPage, MountRangeAllocation,
    MountSourceError, ViewObserver,
};
use super::{
    FuseSessionState, InodeEntry, KernelCacheItem, MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY,
    MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile, MountPath,
    MountSeekTarget, NativeMountError, PageStores, ProjectionCore, ROOT_INODE, ViewStamp,
    cached_projected_lookup, confirm_name, forget_unbound_names, intern_projected, name_is_bound,
    native_callback_errno, replace_root_lookup, retire_reused_identity, run_fuse_session,
    stale_kernel_items,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
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
    let work = source
        .native_original_work()?
        .unwrap_or_else(fuse_fixture_work);
    let root = source
        .lookup_with_meter(&MountPath::root(), &work)?
        .ok_or(libc::ENOENT)?;
    let stamp = source.view_stamp_with_meter(&work)?;
    let mut core = super::ProjectionCore::new(source, root, stamp, true, 0, 0, &work)?;
    let configured = Arc::get_mut(&mut core).expect("fixture owns the new core");
    configured.handler_budget = budget;
    configured.handler_cancellation = cancellation.clone();
    Ok(core)
}

mod cache;
mod mount;
mod native;

use cache::*;
use native::*;
