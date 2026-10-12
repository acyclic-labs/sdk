use super::super::{
    MountAttributePage, MountAttributeWriteMode, MountDirectoryPage, MountRangeAllocation,
    MountSourceError, ViewObserver,
};
use super::{
    InodeEntry, KernelCacheItem, MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY, MountFilesystem,
    MountLookup, MountNode, MountNodeKind, MountOpenFile, MountPath, MountSeekTarget,
    NativeMountError, PageStores, ROOT_INODE, ViewStamp, cached_projected_lookup, confirm_name,
    forget_unbound_names, intern_projected, name_is_bound, native_callback_errno,
    replace_root_lookup, retire_reused_identity, stale_kernel_items,
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

fn fuse_fixture_errno(error: NativeMountError) -> i32 {
    native_callback_errno(&error)
}

mod cache;
mod mount;
mod native;

use cache::*;
use native::*;
