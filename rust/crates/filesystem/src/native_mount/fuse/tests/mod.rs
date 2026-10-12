use super::super::{
    MountAttributePage, MountAttributeWriteMode, MountDirectoryPage, MountRangeAllocation,
    MountSourceError, ViewObserver,
};
use super::{
    InodeEntry, KernelCacheItem, confirm_name, name_is_bound,
    MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY, MountFilesystem, MountLookup, MountNode,
    MountNodeKind, MountOpenFile, MountPath, MountSeekTarget, PageStores,
    ROOT_INODE, ViewStamp, admit_open, cached_projected_lookup, changes_content,
    forget_unbound_names, intern_projected, replace_root_lookup, retire_reused_identity,
    stale_kernel_items,
    NativeMountError, native_callback_errno,
};
use std::path::Path;
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

type MemorySource = super::super::CheckoutMountSource<
    crate::facade::MemoryAuthorityBackend,
    crate::facade::MemoryObjectBackend,
>;

fn fuse_fixture_work() -> crate::NativeMaterializationWork {
    cache_work(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
}

fn fuse_fixture_errno(error: NativeMountError) -> i32 {
    native_callback_errno(&error)
}

mod cache;
mod native;
mod mount;

use cache::*;
use native::*;
