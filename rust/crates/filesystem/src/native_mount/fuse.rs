//! Linux FUSE projection over the common callback contract.

#![allow(unsafe_code, reason = "FUSE mount, descriptor transfer and recovery syscalls")]

use super::view_ledger::ViewOriginScope;
use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES,
    MountDirectoryEntry, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest,
    ViewObserver, ViewOrigin, ViewStamp, create_metadata, metadata_or, source_error,
    system_time_ns,
};
use crate::kernel::{FileMetadata, MetadataField};
use fuser::{
    BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileAttr,
    FileHandle as FuseFileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags,
    KernelConfig, LockOwner, MountOption, Notifier, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
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
struct TestWriteGate {
    admitted: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static TEST_WRITE_GATE: std::sync::OnceLock<std::sync::Mutex<Option<TestWriteGate>>> =
    std::sync::OnceLock::new();

/// Pauses one FUSE write after handle admission.
#[cfg(test)]
pub(super) struct TestWriteControl {
    admitted: std::sync::mpsc::Receiver<()>,
    release: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
impl TestWriteControl {
    pub(super) fn wait_until_admitted(&self, timeout: Duration) -> bool {
        self.admitted.recv_timeout(timeout).is_ok()
    }

    pub(super) fn release(self) -> bool {
        self.release.send(()).is_ok()
    }
}

#[cfg(test)]
pub(super) fn pause_next_write_after_admission() -> TestWriteControl {
    let (admitted_send, admitted_receive) = std::sync::mpsc::sync_channel(0);
    let (release_send, release_receive) = std::sync::mpsc::sync_channel(0);
    let gate = TEST_WRITE_GATE.get_or_init(|| std::sync::Mutex::new(None));
    *gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(TestWriteGate {
        admitted: admitted_send,
        release: release_receive,
    });
    TestWriteControl {
        admitted: admitted_receive,
        release: release_send,
    }
}

#[cfg(test)]
fn pause_test_write() {
    let Some(gate) = TEST_WRITE_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    else {
        return;
    };
    let _ = gate.admitted.send(());
    let _ = gate.release.recv_timeout(Duration::from_secs(10));
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
const RENAME_NOREPLACE: u32 = 1;
use libc::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFREG, S_IFSOCK};
const DETACHED_COPY_CHUNK_BYTES: u32 = 1024 * 1024;
const MAXIMUM_DIRECTORY_STREAMS: usize = 256;

struct FuseOwned<T> {
    value: Option<T>,
    custody: Option<crate::demand::NativeMountAllocationCustody>,
    claimed_bytes: u64,
    native_allocation: Option<crate::NativeAllocation>,
}

impl<T> FuseOwned<T> {
    fn empty(value: T) -> Self {
        Self { value: Some(value), custody: None, claimed_bytes: 0, native_allocation: None }
    }

    fn native(value: T, allocation: crate::NativeAllocation) -> Self {
        Self { value: Some(value), custody: None, claimed_bytes: 0, native_allocation: Some(allocation) }
    }

    fn map<U>(mut self, map: impl FnOnce(T) -> U) -> FuseOwned<U> {
        FuseOwned {
            value: Some(map(self.value.take().expect("owned FUSE payload"))),
            custody: self.custody.take(),
            claimed_bytes: std::mem::take(&mut self.claimed_bytes),
            native_allocation: self.native_allocation.take(),
        }
    }

    fn claim(&mut self, bytes: u64, work: &super::NativeMountMeterWork)
        -> Result<(), crate::NativeTreeMaterializationError>
    {
        let materialization = work.native_materialization_work();
        materialization.admit(crate::WorkCounters::UNCHARGED)?;
        if bytes == 0 { return Ok(()); }
        let mut allocation = materialization.claim(bytes)?;
        if let Some(owned) = &mut self.native_allocation {
            owned.absorb(&mut allocation)?;
        } else {
            self.native_allocation = Some(allocation);
        }
        Ok(())
    }

    fn split_claim(&mut self, bytes: u64) -> FuseOwned<()> {
        if let Some(allocation) = &mut self.native_allocation {
            let split = allocation.split_off(bytes).expect("split only original admitted payload");
            return FuseOwned::native((), split);
        }
        self.claimed_bytes = self.claimed_bytes.checked_sub(bytes).expect("split only original admitted payload");
        FuseOwned { value: Some(()), custody: self.custody.clone(), claimed_bytes: bytes, native_allocation: None }
    }

    fn adopt(value: T, bytes: u64, work: &super::NativeMountMeterWork)
        -> Result<Self, super::NativeMountStartupAdmissionError>
    {
        if bytes == 0 {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
            return Ok(Self::empty(value));
        }
        let admitted = crate::demand::NativeMountAllocationCustody::from_admitted_with_meter(bytes, work);
        match admitted {
            Ok(custody) => Ok(Self { value: Some(value), custody: Some(custody), claimed_bytes: bytes, native_allocation: None }),
            Err(error) => {
                drop(value);
                work.native_materialization_work().original_demand_work()
                    .with_original_leaf(|original| original.release(bytes))??;
                Err(error)
            }
        }
    }


}

impl<T> std::ops::Deref for FuseOwned<T> {
    type Target = T;
    fn deref(&self) -> &T { self.value.as_ref().expect("owned FUSE payload") }
}

impl<T> std::ops::DerefMut for FuseOwned<T> {
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("owned FUSE payload") }
}

impl<T> Drop for FuseOwned<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        if let Some(custody) = self.custody.take() {
            custody.retire(self.claimed_bytes).expect("only dropped FUSE payload is retired");
        }
    }
}

impl<T: std::hash::Hash> std::hash::Hash for FuseOwned<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { (**self).hash(state); }
}
impl<T: PartialEq> PartialEq for FuseOwned<T> {
    fn eq(&self, other: &Self) -> bool { **self == **other }
}
impl<T: std::fmt::Debug> std::fmt::Debug for FuseOwned<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(formatter)
    }
}
impl<T: Default> Default for FuseOwned<T> {
    fn default() -> Self { Self::empty(T::default()) }
}
impl PartialEq<MountPath> for FuseOwned<MountPath> {
    fn eq(&self, other: &MountPath) -> bool { **self == *other }
}
impl<'a, T> IntoIterator for &'a FuseOwned<Vec<T>> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}
impl<'a, T> IntoIterator for &'a mut FuseOwned<Vec<T>> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter { self.iter_mut() }
}
impl<T> IntoIterator for FuseOwned<Vec<T>> {
    type Item = T;
    type IntoIter = FuseOwned<std::vec::IntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter { self.map(Vec::into_iter) }
}
impl<T> Iterator for FuseOwned<std::vec::IntoIter<T>> {
    type Item = T;
    fn next(&mut self) -> Option<T> { self.value.as_mut().expect("owned FUSE iterator").next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.value.as_ref().expect("owned FUSE iterator").size_hint() }
}
impl<T: PartialEq> PartialEq<Vec<T>> for FuseOwned<Vec<T>> {
    fn eq(&self, other: &Vec<T>) -> bool { **self == *other }
}


impl<T: Eq> Eq for FuseOwned<T> {}
impl std::borrow::Borrow<MountPath> for FuseOwned<MountPath> {
    fn borrow(&self) -> &MountPath { self }
}
impl std::borrow::Borrow<[u8]> for FuseOwned<Vec<u8>> {
    fn borrow(&self) -> &[u8] { self }
}


fn fuse_item(work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
    work.native_materialization_work().charge(crate::WorkCounters {
        items_examined: 1, ..crate::WorkCounters::UNCHARGED
    }).map_err(Into::into)
}

fn fuse_copy_admit(bytes: u64, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
    work.native_materialization_work().admit(crate::WorkCounters {
        bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED
    }).map_err(Into::into)
}

fn fuse_owned_source_chunk(chunk: crate::kernel::OwnedSourceChunk, work: &super::NativeMountMeterWork)
    -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError>
{
    if chunk.is_intrinsic() { return Ok(FuseOwned::empty(chunk)); }
    let bytes = chunk.logical_owned_bytes;
    FuseOwned::adopt(chunk, bytes, work).map_err(Into::into)
}

fn fuse_copy_record(bytes: u64, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
    work.native_materialization_work().record_admitted(crate::WorkCounters {
        bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED
    }).map_err(Into::into)
}

fn fuse_vec<T>(capacity: usize, work: &super::NativeMountMeterWork)
    -> Result<FuseOwned<Vec<T>>, FuseRequestError>
{
    let bytes = capacity.checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)? as u64;
    let mut owned = FuseOwned::empty(Vec::new());
    owned.claim(bytes, work)?;
    if bytes != 0 {
        owned.try_reserve_exact(capacity).map_err(NativeMountError::Allocation)?;
    }
    Ok(owned)
}

fn fuse_reserve<T>(owned: &mut FuseOwned<Vec<T>>, additional: usize,
    work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
{
    let required = owned.len().checked_add(additional).ok_or(crate::WorkError::Overflow)?;
    if required <= owned.capacity() { return Ok(()); }
    let capacity = required.max(owned.capacity().checked_mul(2).ok_or(crate::WorkError::Overflow)?);
    let copied = owned.len().checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)? as u64;
    fuse_copy_admit(copied, work)?;
    let mut replacement = fuse_vec(capacity, work)?;
    fuse_copy_admit(copied, work)?;
    replacement.append(owned);
    drop(std::mem::replace(owned, replacement));
    fuse_copy_record(copied, work)
}

fn fuse_copy_bytes(bytes: &[u8], work: &super::NativeMountMeterWork)
    -> Result<FuseOwned<Vec<u8>>, FuseRequestError>
{
    fuse_copy_admit(bytes.len() as u64, work)?;
    let mut copied = fuse_vec(bytes.len(), work)?;
    fuse_copy_admit(bytes.len() as u64, work)?;
    copied.extend_from_slice(bytes);
    fuse_copy_record(bytes.len() as u64, work)?;
    Ok(copied)
}

fn fuse_path_from_components<'a>(capacity: usize, components: impl Iterator<Item = &'a [u8]>,
    work: &super::NativeMountMeterWork) -> Result<FuseOwned<MountPath>, FuseRequestError>
{
    let mut path = fuse_vec::<Vec<u8>>(capacity, work)?;
    for component in components {
        fuse_item(work)?;
        let copied_bytes = (component.len() as u64).checked_add(std::mem::size_of::<Vec<u8>>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        fuse_copy_admit(copied_bytes, work)?;
        path.claim(component.len() as u64, work)?;
        let mut copied = Vec::new();
        copied.try_reserve_exact(component.len()).map_err(NativeMountError::Allocation)?;
        copied.extend_from_slice(component);
        path.push(copied);
        fuse_copy_record(copied_bytes, work)?;
    }
    Ok(path.map(|components| MountPath { components }))
}

fn fuse_clone_path(path: &MountPath, work: &super::NativeMountMeterWork)
    -> Result<FuseOwned<MountPath>, FuseRequestError>
{
    fuse_path_from_components(path.components().len(), path.components().iter().map(Vec::as_slice), work)
}

fn fuse_child_path(parent: &MountPath, name: &[u8], work: &super::NativeMountMeterWork)
    -> Result<FuseOwned<MountPath>, FuseRequestError>
{
    let capacity = parent.components().len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
    fuse_path_from_components(capacity, parent.components().iter().map(Vec::as_slice)
        .chain(std::iter::once(name)), work)
}

fn fuse_parent(path: &MountPath, work: &super::NativeMountMeterWork)
    -> Result<Option<FuseOwned<MountPath>>, FuseRequestError>
{
    let Some((_, parent)) = path.components().split_last() else { return Ok(None); };
    fuse_path_from_components(parent.len(), parent.iter().map(Vec::as_slice), work).map(Some)
}

fn fuse_has_prefix(path: &MountPath, prefix: &MountPath,
    work: &super::NativeMountMeterWork) -> Result<bool, FuseRequestError>
{
    if path.components().len() < prefix.components().len() { return Ok(false); }
    for (component, expected) in path.components().iter().zip(prefix.components()) {
        fuse_item(work)?;
        if component != expected { return Ok(false); }
    }
    Ok(true)
}

fn fuse_replace_prefix(path: &MountPath, source: &MountPath, destination: &MountPath,
    work: &super::NativeMountMeterWork) -> Result<Option<FuseOwned<MountPath>>, FuseRequestError>
{
    if !fuse_has_prefix(path, source, work)? { return Ok(None); }
    let suffix = &path.components()[source.components().len()..];
    let capacity = destination.components().len().checked_add(suffix.len()).ok_or(crate::WorkError::Overflow)?;
    fuse_path_from_components(capacity, destination.components().iter().map(Vec::as_slice)
        .chain(suffix.iter().map(Vec::as_slice)), work)
        .map(Some)
}

fn fuse_initialized<T>(value: T, bytes: u64, work: &super::NativeMountMeterWork)
    -> Result<FuseOwned<T>, FuseRequestError>
{
    let mut owner = FuseOwned::empty(value);
    owner.claim(bytes, work)?;
    Ok(owner)
}

fn fuse_arc_clone<T: ?Sized>(value: &Arc<T>, work: &super::NativeMountMeterWork)
    -> Result<Arc<T>, super::NativeMountStartupAdmissionError>
{
    let counters = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<Arc<T>>() as u64,
        ..Default::default()
    };
    let materialization = work.native_materialization_work();
    materialization.admit(counters)?;
    let cloned = Arc::clone(value);
    materialization.record_admitted(counters)?;
    Ok(cloned)
}

type FuseFile = Arc<FuseOwned<Arc<dyn MountOpenFile>>>;

enum FuseOpenFile {
    Source(FuseFile),
    Deferred(Arc<DeferredOpenFile>),
}

impl FuseOpenFile {
    fn file(&self, work: &super::NativeMountMeterWork)
        -> Result<std::borrow::Cow<'_, FuseFile>, MountSourceError> {
        match self {
            Self::Source(file) => Ok(std::borrow::Cow::Borrowed(file)),
            Self::Deferred(file) => file.file(work).map(std::borrow::Cow::Owned),
        }
    }

    fn clone(&self, work: &super::NativeMountMeterWork) -> Result<Self, FuseRequestError> {
        Ok(match self {
            Self::Source(file) => Self::Source(fuse_arc_clone(file, work)?),
            Self::Deferred(file) => Self::Deferred(fuse_arc_clone(file, work)?),
        })
    }

    fn same_file(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Source(left), Self::Source(right)) => Arc::ptr_eq(left, right),
            (Self::Deferred(left), Self::Deferred(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    fn settle(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        match self {
            Self::Source(file) => file.settle_with_meter(work),
            Self::Deferred(file) => file.settle(work),
        }
    }
}

fn fuse_open_file((file, bytes): (Arc<dyn MountOpenFile>, u64), work: &super::NativeMountMeterWork)
    -> Result<FuseFile, MountSourceError>
{
    let mut owner = FuseOwned::adopt(file, bytes, work).map_err(MountSourceError::from)?;
    owner.claim(std::mem::size_of::<FuseOwned<Arc<dyn MountOpenFile>>>() as u64, work)
        .map_err(MountSourceError::NativeWork)?;
    Ok(Arc::new(owner))
}

fn fuse_source_chunk(bytes: &[u8], work: &super::NativeMountMeterWork)
    -> Result<crate::kernel::OwnedSourceChunk, FuseRequestError>
{
    let length = bytes.len() as u64;
    fuse_copy_admit(length, work)?;
    let original = work.native_materialization_work();
    let mut allocation = original.claim(length).map_err(NativeMountError::from)?;
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(bytes.len()).map_err(NativeMountError::Allocation)?;
    allocation.reconcile_capacity(buffer.capacity() as u64).map_err(NativeMountError::from)?;
    buffer.extend_from_slice(bytes);
    fuse_copy_record(length, work)?;
    let logical_owned_bytes = allocation.bytes();
    let owned = acyclic_stream::OwnedBytes::from_vec_with_guard(
        buffer, allocation, original.runtime_meter_owner(),
    ).map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
            NativeMountError::from(original.runtime_error(error))
        }
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
        => {
            unreachable!("guarded vector conversion only fails original admission")
        }
    })?;
    Ok(crate::kernel::OwnedSourceChunk::intrinsic(owned, logical_owned_bytes))
}

#[derive(Default)]
struct ConfirmedNames {
    names: HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
}

impl ConfirmedNames {
    fn confirm(&mut self, file_id: crate::FileId, mut name: FuseOwned<MountPath>,
        stamp: Option<ViewStamp>, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        for ((known_id, known_name), held) in &mut self.names {
            fuse_item(work)?;
            if *known_id == file_id && **known_name == *name {
                *held = stamp;
                return Ok(());
            }
        }
        name.claim(std::mem::size_of::<((crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>)>() as u64, work)?;
        self.names.try_reserve(1).map_err(NativeMountError::Allocation)?;
        self.names.insert((file_id, name), stamp);
        Ok(())
    }

    fn still_bound(
        &self,
        source: &dyn MountFilesystem,
        file_id: crate::FileId,
        name: &MountPath,
        work: &super::NativeMountMeterWork,
    ) -> Result<bool, FuseRequestError> {
        for ((known_id, known_name), stamp) in &self.names {
            fuse_item(work)?;
            if *known_id == file_id && **known_name == *name {
                return match stamp {
                    Some(stamp) => Ok(source.unchanged_since_with_meter(name, None, *stamp, work)?),
                    None => Ok(true),
                };
            }
        }
        Ok(false)
    }
}

struct DirectoryStream {
    path: FuseOwned<MountPath>,
    parent_inode: u64,
    binding_epoch: Option<u64>,
    page: Option<super::MountDirectoryPage>,
    entry_position: usize,
    entries_stamp: Option<ViewStamp>,
    confirmed: ConfirmedNames,
    exhausted: bool,
    emitted: u64,
    parked: u64,
    park_replacement: Option<(u64, u64)>,
    storage: FuseOwned<()>,
}

impl DirectoryStream {
    fn entries_remaining(&self) -> usize {
        self.page.as_ref().map_or(0, |page| page.entries().len().saturating_sub(self.entry_position))
    }

    fn cursor(&self) -> Option<&[u8]> {
        self.page.as_ref().and_then(|page| page.next_cursor())
    }
}

#[derive(Default)]
struct DirectoryStreams {
    waiting: HashMap<(u64, u64), DirectoryStream>,
    parked: u64,
}

impl DirectoryStreams {
    fn take(&mut self, inode: u64, offset: u64) -> Option<DirectoryStream> {
        let mut stream = self.waiting.remove(&(inode, offset))?;
        // The initialized table row is gone; page/path owners remain live.
        drop(std::mem::replace(&mut stream.storage, FuseOwned::empty(())));
        stream.park_replacement = None;
        Some(stream)
    }

    fn prepare_park(&mut self, stream: &mut DirectoryStream,
        work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        let storage = fuse_initialized((), std::mem::size_of::<((u64, u64), DirectoryStream)>() as u64, work)?;
        self.waiting.try_reserve(1).map_err(NativeMountError::Allocation)?;
        let mut oldest = None;
        if self.waiting.len() >= MAXIMUM_DIRECTORY_STREAMS {
            for (key, candidate) in &self.waiting {
                fuse_item(work)?;
                if oldest.is_none_or(|(_, parked)| candidate.parked < parked) {
                    oldest = Some((*key, candidate.parked));
                }
            }
        }
        stream.park_replacement = oldest.map(|(key, _)| key);
        stream.storage = storage;
        Ok(())
    }

    fn park(&mut self, inode: u64, mut stream: DirectoryStream)
    {
        if let Some(key) = stream.park_replacement.take() { drop(self.waiting.remove(&key)); }
        self.parked = self.parked.wrapping_add(1);
        stream.parked = self.parked;
        self.waiting.insert((inode, stream.emitted), stream);
    }

    fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath,
        work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        let mut replacements = fuse_vec(0, work)?;
        for (key, stream) in &self.waiting {
            fuse_item(work)?;
            if let Some(path) = fuse_replace_prefix(&stream.path, source, destination, work)? {
                fuse_reserve(&mut replacements, 1, work)?;
                replacements.push((*key, path));
            }
        }
        for (key, path) in replacements {
            self.waiting.get_mut(&key).expect("planned stream remains locked").path = path;
        }
        Ok(())
    }
}

struct Binding {
    path: FuseOwned<MountPath>,
    verified: Option<ViewStamp>,
    kernel: Option<ViewStamp>,
}

impl Binding {
    fn new(path: FuseOwned<MountPath>, verified: Option<ViewStamp>) -> Self {
        Self {
            path,
            verified,
            kernel: None,
        }
    }
}

struct InodeEntry {
    bindings: FuseOwned<Vec<Binding>>,
    lookup: MountLookup,
    facts: Option<ViewStamp>,
    kernel: Option<ViewStamp>,
    lookup_references: u64,
    open_handles: u64,
    cached_content: Option<ContentVersion>,
    negative_children: HashMap<FuseOwned<Vec<u8>>, ViewStamp>,
    empty_since: Option<ViewStamp>,
    identity_storage: FuseOwned<()>,
    _storage: FuseOwned<()>,
}

impl InodeEntry {
    fn new(binding: FuseOwned<MountPath>, lookup: MountLookup,
        verified: Option<ViewStamp>, lookup_references: u64,
        work: &super::NativeMountMeterWork) -> Result<Self, FuseRequestError>
    {
        let mut bindings = fuse_vec(1, work)?;
        bindings.push(Binding::new(binding, verified));
        let mut storage = FuseOwned::empty(());
        storage.claim(std::mem::size_of::<(u64, InodeEntry)>() as u64, work)?;
        storage.claim(std::mem::size_of::<(crate::FileId, u64)>() as u64, work)?;
        let identity_storage = storage.split_claim(std::mem::size_of::<(crate::FileId, u64)>() as u64);
        Ok(Self {
            bindings, lookup, facts: verified, kernel: None, lookup_references,
            open_handles: 0, cached_content: None, negative_children: HashMap::new(),
            empty_since: None, identity_storage, _storage: storage,
        })
    }

    fn binding(&self, path: &MountPath, work: &super::NativeMountMeterWork)
        -> Result<Option<&Binding>, FuseRequestError>
    {
        for binding in &self.bindings {
            fuse_item(work)?;
            if binding.path == *path { return Ok(Some(binding)); }
        }
        Ok(None)
    }

    fn binding_mut(&mut self, path: &MountPath, work: &super::NativeMountMeterWork)
        -> Result<Option<&mut Binding>, FuseRequestError>
    {
        for binding in self.bindings.iter_mut() {
            fuse_item(work)?;
            if binding.path == *path { return Ok(Some(binding)); }
        }
        Ok(None)
    }

    fn anchor(&self) -> &MountPath {
        self.bindings
            .first()
            .map_or(&ROOT_PATH, |binding| &*binding.path)
    }

    fn current_facts(&self, source: &dyn MountFilesystem, work: &super::NativeMountMeterWork)
        -> Result<Option<ViewStamp>, FuseRequestError>
    {
        let Some(stamp) = self.facts else { return Ok(None); };
        let file_id = self.lookup.node.file_id;
        let current = if self.lookup.node.kind == MountNodeKind::Directory {
            match self.bindings.first() {
                Some(anchor) => source.unchanged_since_with_meter(&anchor.path, Some(file_id), stamp, work)?,
                None => false,
            }
        } else { source.node_unchanged_since_with_meter(file_id, stamp, work)? };
        Ok(current.then_some(stamp))
    }

    fn lacks_named_attributes(&self, source: &dyn MountFilesystem, work: &super::NativeMountMeterWork)
        -> Result<bool, FuseRequestError>
    {
        Ok(self.lookup.metadata.named_attributes == MetadataField::Unavailable
            && self.current_facts(source, work)?.is_some())
    }

    fn admit_cached_content(&mut self, lookup: &MountLookup) -> CachedContent {
        let opened = ContentVersion::of(lookup);
        match self.cached_content.replace(opened) {
            None => CachedContent::Absent,
            Some(cached) if cached == opened => CachedContent::Current,
            Some(_) => CachedContent::Superseded,
        }
    }

    fn remember_negative_child(&mut self, name: &[u8], stamp: ViewStamp,
        work: &super::NativeMountMeterWork) -> Result<bool, FuseRequestError>
    {
        fuse_item(work)?;
        if let Some(held) = self.negative_children.get_mut(name) {
            *held = (*held).min(stamp);
            return Ok(true);
        }
        if self.negative_children.len() >= MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY {
            return Ok(false);
        }
        let mut name = fuse_copy_bytes(name, work)?;
        name.claim(std::mem::size_of::<(FuseOwned<Vec<u8>>, ViewStamp)>() as u64, work)?;
        self.negative_children.try_reserve(1).map_err(NativeMountError::Allocation)?;
        self.negative_children.insert(name, stamp);
        Ok(true)
    }
}

fn hold(held: &mut Option<ViewStamp>, stamp: ViewStamp) {
    *held = Some(held.map_or(stamp, |held| held.min(stamp)));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContentVersion {
    logical_bytes: u64,
    modified_ns: MetadataField<i64>,
    changed_ns: MetadataField<i64>,
}

impl ContentVersion {
    fn of(lookup: &MountLookup) -> Self {
        Self {
            logical_bytes: lookup.node.logical_bytes,
            modified_ns: lookup.metadata.modified_ns,
            changed_ns: lookup.metadata.changed_ns,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CachedContent {
    Absent,
    Current,
    Superseded,
}

#[derive(Debug, Eq, PartialEq)]
enum KernelCacheItem {
    Inode(u64),
    Entry { parent: u64, name: FuseOwned<Vec<u8>> },
}

struct FileHandle {
    inode: u64,
    open_file: FuseOpenFile,
    operation: Arc<FuseOwned<Mutex<()>>>,
    dirty: bool,
    claim: Option<crate::FileId>,
    _storage: FuseOwned<()>,
}

struct DeferredOpenFile {
    source: Arc<dyn MountFilesystem>,
    path: FuseOwned<MountPath>,
    opened: MountLookup,
    file: Mutex<Option<FuseFile>>,
    _allocation: FuseOwned<()>,
}

impl DeferredOpenFile {
    fn file(&self, work: &super::NativeMountMeterWork) -> Result<FuseFile, MountSourceError> {
        let mut file = self.file.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = file.as_ref() {
            return fuse_arc_clone(file, work).map_err(MountSourceError::from);
        }
        let opened = self.source.open_file_with_meter(&self.path, work)?;
        let opened = fuse_open_file(opened, work)?;
        if opened.lookup_with_meter(work)?.node.file_id != self.opened.node.file_id {
            return Err(MountSourceError::Stale);
        }
        *file = Some(fuse_arc_clone(&opened, work).map_err(MountSourceError::from)?);
        Ok(opened)
    }


    fn discard_packet(packet: crate::kernel::OwnedSourceChunk, work: &super::NativeMountMeterWork)
        -> Result<(), MountSourceError>
    {
        let (bytes, owned_bytes, intrinsic) = packet.into_owned_parts();
        drop(bytes);
        if intrinsic { return Ok(()); }
        work.meter().lock().release(owned_bytes)
            .map_err(|error| MountSourceError::from(super::NativeMountStartupAdmissionError::from(error)))
    }

    fn settle(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let opened = {
            let file = self.file.lock().map_err(|_| MountSourceError::Stale)?;
            file.as_ref().map(|file| fuse_arc_clone(file, work)
                .map_err(MountSourceError::from)).transpose()?
        };
        match opened {
            Some(opened) => opened.settle_with_meter(work),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
struct Implicit {
    handle: u64,
    writes: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Access {
    Read,
    Write,
}

fn changes_content(flags: i32) -> bool {
    flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0
}

#[derive(Default)]
struct PageStores {
    in_flight: HashMap<crate::FileId, FuseOwned<ViewStamp>>,
    claims: HashMap<crate::FileId, FuseOwned<u64>>,
}

impl PageStores {
    fn may_begin(&self, file_id: crate::FileId) -> bool {
        !self.in_flight.contains_key(&file_id) && !self.claims.contains_key(&file_id)
    }

    fn begin(&mut self, file_id: crate::FileId, stamp: ViewStamp,
        work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        debug_assert!(self.may_begin(file_id));
        let stamp = fuse_initialized(stamp, std::mem::size_of::<(crate::FileId, FuseOwned<ViewStamp>)>() as u64, work)?;
        self.in_flight.try_reserve(1).map_err(NativeMountError::Allocation)?;
        self.in_flight.insert(file_id, stamp);
        Ok(())
    }

    fn land(&mut self, file_id: crate::FileId) {
        self.in_flight.remove(&file_id);
    }

    fn claim(&mut self, file_id: crate::FileId, work: &super::NativeMountMeterWork)
        -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        debug_assert!(!self.in_flight.contains_key(&file_id));
        if let Some(claims) = self.claims.get_mut(&file_id) {
            **claims = claims.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        } else {
            let claims = fuse_initialized(1, std::mem::size_of::<(crate::FileId, FuseOwned<u64>)>() as u64, work)?;
            self.claims.try_reserve(1).map_err(NativeMountError::Allocation)?;
            self.claims.insert(file_id, claims);
        }
        Ok(())
    }

    fn release(&mut self, file_id: crate::FileId) {
        if let Some(claims) = self.claims.get_mut(&file_id) {
            **claims -= 1;
            if **claims == 0 {
                self.claims.remove(&file_id);
            }
        }
    }

    fn precede(&self, target: ViewStamp) -> bool {
        self.in_flight.values().any(|stamp| **stamp < target)
    }
}

struct PageStore {
    inode: u64,
    file_id: crate::FileId,
    path: FuseOwned<MountPath>,
    stamp: ViewStamp,
    length: u32,
    open_file: FuseOpenFile,
}

struct OpenedFile<'a> {
    path: &'a MountPath,
    lookup: &'a MountLookup,
    stamp: Option<ViewStamp>,
    flags: i32,
    open_file: FuseOpenFile,
    dirty: bool,
    stores_pages: bool,
}

#[derive(Clone, Copy)]
struct AttributeDefaults {
    writable: bool,
    mount_uid: u32,
    mount_gid: u32,
}

#[derive(Default)]
struct InvalidationQueue {
    notified: Option<ViewStamp>,
    scanned: Option<ViewStamp>,
    processed: Option<ViewStamp>,
    deferred: FuseOwned<Vec<KernelCacheItem>>,
    pending_tail: Option<FuseOwned<std::vec::IntoIter<KernelCacheItem>>>,
    in_flight: bool,
    failure: Option<super::NativeMountCallbackFailure>,
}

impl InvalidationQueue {
    fn is_idle(&self) -> bool {
        !self.is_pending(self.notified)
    }

    fn is_pending(&self, target: Option<ViewStamp>) -> bool {
        self.processed < target || !self.deferred.is_empty()
            || self.pending_tail.as_ref().is_some_and(|items| !items.as_slice().is_empty()) || self.in_flight
    }

    fn take_deferred(&mut self) -> FuseOwned<std::vec::IntoIter<KernelCacheItem>> {
        self.in_flight = true;
        self.pending_tail.take().unwrap_or_else(|| std::mem::take(&mut self.deferred).map(Vec::into_iter))
    }

    fn retain_pending_tail(&mut self, items: FuseOwned<std::vec::IntoIter<KernelCacheItem>>) {
        assert!(self.pending_tail.is_none(), "one original invalidator pass owns its tail");
        if !items.as_slice().is_empty() { self.pending_tail = Some(items); }
    }

    fn finish(&mut self, through: Option<ViewStamp>,
        dropped: Result<(), super::NativeMountCallbackFailure>) {
        if self.pending_tail.is_none() { self.processed = self.processed.max(through); }
        self.in_flight = false;
        if let Err(error) = dropped {
            self.failure.get_or_insert(error);
        }
    }
}

struct Resolved {
    lookup: MountLookup,
    stamp: Option<ViewStamp>,
    through: Option<FuseOwned<MountPath>>,
}

enum NodeFacts {
    Handle(FuseOpenFile),
    Names {
        file_id: Option<crate::FileId>,
        paths: FuseOwned<Vec<FuseOwned<MountPath>>>,
    },
}

enum AttributeTarget {
    Handle(FuseOpenFile),
    Path(FuseOwned<MountPath>),
}

struct ChildName {
    spelled: FuseOwned<MountPath>,
    folded: Option<FuseOwned<MountPath>>,
}

impl ChildName {
    fn new(source: &dyn MountFilesystem, spelled: FuseOwned<MountPath>,
        work: &super::NativeMountMeterWork) -> Result<Self, FuseRequestError>
    {
        let folded = source.folded_path_with_meter(&spelled, work)?
            .map(|(path, bytes)| FuseOwned::adopt(path, bytes, work)).transpose()?;
        Ok(Self { spelled, folded })
    }

    fn key(&self) -> &MountPath {
        self.folded.as_deref().unwrap_or(&self.spelled)
    }

    fn is_exact(&self) -> bool {
        self.folded.is_none()
    }
}

#[cfg(test)]
fn fuse_fixture_work() -> super::NativeMountMeterWork {
    super::NativeMountMeterWork::from_meter(Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
        crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
    ))))
}

#[cfg(test)]
fn fuse_fixture_errno(error: FuseRequestError) -> i32 {
    match error {
        FuseRequestError::Errno(error) => error,
        FuseRequestError::Native(error) => native_callback_errno(&error),
    }
}

struct Entry {
    attr: FileAttr,
    ttl: Duration,
}

impl Entry {
    fn reply(self, reply: ReplyEntry) {
        reply.entry(&self.ttl, &self.attr, Generation(0));
    }
}

struct ProjectionState {
    defaults: AttributeDefaults,
    next_inode: u64,
    next_handle: u64,
    by_inode: HashMap<u64, InodeEntry>,
    inode_by_path: HashMap<FuseOwned<MountPath>, u64>,
    inode_by_file: HashMap<crate::FileId, u64>,
    files: HashMap<u64, FileHandle>,
    implicit: HashMap<u64, FuseOwned<Implicit>>,
    streams: DirectoryStreams,
    invalidation: InvalidationQueue,
    page_stores: PageStores,
}

impl ProjectionState {
    fn path(&self, inode: u64) -> Result<&MountPath, i32> {
        self.by_inode
            .get(&inode)
            .and_then(|entry| entry.bindings.first())
            .map(|binding| &*binding.path)
            .ok_or(libc::ESTALE)
    }

    fn node(&self, inode: u64) -> Result<MountNode, i32> {
        self.by_inode
            .get(&inode)
            .map(|entry| entry.lookup.node)
            .ok_or(libc::ESTALE)
    }

    fn child_path(&self, parent: u64, name: &OsStr, work: &super::NativeMountMeterWork)
        -> Result<FuseOwned<MountPath>, FuseRequestError>
    {
        fuse_item(work)?;
        let parent = self.path(parent)?;
        let name = name.as_bytes();
        let mut contains_separator = false;
        for byte in name {
            fuse_item(work)?;
            contains_separator |= *byte == b'/';
        }
        if name.is_empty() || name == b"." || name == b".." || contains_separator {
            return Err(libc::EINVAL.into());
        }
        fuse_child_path(parent, name, work)
    }

    fn admit_write(&self) -> Result<(), i32> {
        self.defaults.writable.then_some(()).ok_or(libc::EROFS)
    }

    fn intern(
        &mut self, source: &dyn MountFilesystem, path: FuseOwned<MountPath>, lookup: &MountLookup,
        stamp: Option<ViewStamp>, lookup_reference: bool, confirmed: &ConfirmedNames,
        work: &super::NativeMountMeterWork,
    ) -> Result<u64, FuseRequestError> {
        fuse_item(work)?;
        if let Some(previous) = self.inode_by_path.get(&*path).copied()
            && self.by_inode.get(&previous)
                .is_some_and(|entry| entry.lookup.node.file_id != lookup.node.file_id)
        {
            let held = match self.by_inode.get(&previous) {
                Some(entry) => entry.binding(&path, work)?.is_some_and(|binding| binding.kernel.is_some()),
                None => false,
            };
            if held {
                if let Some(parent) = fuse_parent(&path, work)? {
                    if let Some(parent) = self.inode_by_path.get(&*parent).copied() {
                        let name = fuse_copy_bytes(path.components().last().ok_or(libc::ESTALE)?, work)?;
                        fuse_reserve(&mut self.invalidation.deferred, 1, work)?;
                        self.invalidation.deferred.push(KernelCacheItem::Entry { parent, name });
                    }
                }
            }
            self.remove_binding(previous, &path, work)?;
        }
        if let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied() {
            forget_unbound_names(
                &mut self.by_inode, &mut self.inode_by_path, inode, &path,
                |name, verified| {
                    if confirmed.still_bound(source, lookup.node.file_id, name, work)? { return Ok(true); }
                    match verified {
                        Some(stamp) => Ok(source.unchanged_since_with_meter(name, None, stamp, work)?),
                        None => Ok(false),
                    }
                },
                &mut self.invalidation.deferred, work,
            )?;
            retire_reused_identity(&mut self.by_inode, &mut self.inode_by_file, inode);
        }
        intern_projected(&mut self.next_inode, &mut self.by_inode,
            &mut self.inode_by_path, &mut self.inode_by_file, path, lookup, stamp, lookup_reference, work)
    }

    fn unvouched_names(&self, source: &dyn MountFilesystem, file_id: crate::FileId,
        path: &MountPath, work: &super::NativeMountMeterWork)
        -> Result<FuseOwned<Vec<FuseOwned<MountPath>>>, FuseRequestError>
    {
        let mut names = fuse_vec(0, work)?;
        let Some(entry) = self.inode_by_file.get(&file_id).and_then(|inode| self.by_inode.get(inode))
            else { return Ok(names); };
        for binding in &entry.bindings {
            fuse_item(work)?;
            if binding.path == *path { continue; }
            if let Some(stamp) = binding.verified {
                if source.unchanged_since_with_meter(&binding.path, None, stamp, work)? { continue; }
            }
            let name = fuse_clone_path(&binding.path, work)?;
            fuse_reserve(&mut names, 1, work)?;
            names.push(name);
        }
        Ok(names)
    }

    fn release_lookup_reference(&mut self, inode: u64, references: u64,
        work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        if let Some(entry) = self.by_inode.get(&inode) {
            let remaining = entry.lookup_references.saturating_sub(references);
            let implicit = u64::from(remaining == 0 && self.implicit.contains_key(&inode));
            if inode != ROOT_INODE && remaining == 0 && entry.open_handles <= implicit {
                for _binding in &entry.bindings { fuse_item(work)?; }
            }
        }
        self.release_lookup_reference_administrative(inode, references);
        Ok(())
    }

    fn release_lookup_reference_administrative(&mut self, inode: u64, references: u64) {
        if inode == ROOT_INODE {
            return;
        }
        let forgotten = self.by_inode.get_mut(&inode).is_some_and(|entry| {
            entry.lookup_references = entry.lookup_references.saturating_sub(references);
            entry.lookup_references == 0
        });
        if forgotten && let Some(implicit) = self.implicit.remove(&inode) {
            let _ = self.discard_file_handle(inode, implicit.handle);
        }
        let remove = self
            .by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references == 0 && entry.open_handles == 0);
        if remove && let Some(mut entry) = self.by_inode.remove(&inode) {
            for binding in &entry.bindings {
                if self.inode_by_path.get(&binding.path) == Some(&inode) {
                    self.inode_by_path.remove(&binding.path);
                }
            }
            forget_file(&mut self.inode_by_file, entry.lookup.node.file_id, inode, &mut entry.identity_storage);
        }
    }

    fn admissible(&self, source: &dyn MountFilesystem, anchor: &MountPath,
        file_id: Option<crate::FileId>, stamp: Option<ViewStamp>, work: &super::NativeMountMeterWork)
        -> Result<bool, FuseRequestError>
    {
        let Some(stamp) = stamp else { return Ok(false); };
        if file_id.is_some_and(|file_id| !source.reports_changes_to(file_id)) { return Ok(false); }
        if self.invalidation.scanned.is_none_or(|scanned| scanned <= stamp) { return Ok(true); }
        Ok(source.unchanged_since_with_meter(anchor, file_id, stamp, work)?)
    }

    fn admit_attributes(&mut self, source: &dyn MountFilesystem, inode: u64,
        stamp: Option<ViewStamp>, work: &super::NativeMountMeterWork) -> Result<Duration, FuseRequestError>
    {
        fuse_item(work)?;
        let Some(entry) = self.by_inode.get(&inode) else { return Ok(Duration::ZERO); };
        let admitted = self.admissible(source, entry.anchor(), Some(entry.lookup.node.file_id), stamp, work)?;
        if let Some(entry) = self.by_inode.get_mut(&inode) { hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN)); }
        Ok(if admitted { CACHE_TTL } else { Duration::ZERO })
    }

    fn admit_entry(&mut self, source: &dyn MountFilesystem, inode: u64, name: &ChildName,
        stamp: Option<ViewStamp>, work: &super::NativeMountMeterWork) -> Result<Duration, FuseRequestError>
    {
        let attributes = self.admit_attributes(source, inode, stamp, work)?;
        if !name.is_exact() { return Ok(Duration::ZERO); }
        let Some(entry) = self.by_inode.get(&inode) else { return Ok(Duration::ZERO); };
        let admitted = self.admissible(source, name.key(), Some(entry.lookup.node.file_id), stamp, work)?;
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            if let Some(binding) = entry.binding_mut(name.key(), work)? {
                hold(&mut binding.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
            }
        }
        Ok(if admitted { attributes } else { Duration::ZERO })
    }

    fn empty_since(&self, inode: u64) -> Option<ViewStamp> {
        self.by_inode.get(&inode)?.empty_since
    }

    fn negative_entry(
        &mut self,
        source: &dyn MountFilesystem,
        parent: u64,
        name: &ChildName,
        stamp: Option<ViewStamp>,
        work: &super::NativeMountMeterWork,
    ) -> Result<Entry, FuseRequestError> {
        let ttl = self
            .admit_negative(source, parent, name, stamp, work)?
            .ok_or(libc::ENOENT)?;
        let mut attr = self.node_attr(ROOT_INODE).map_err(|_| libc::ESTALE)?;
        attr.ino = INodeNo(0);
        Ok(Entry { attr, ttl })
    }

    fn absent_since(&self, parent: u64, name: &ChildName) -> Option<ViewStamp> {
        let component = name.key().components().last()?;
        name.is_exact()
            .then(|| {
                self.by_inode
                    .get(&parent)?
                    .negative_children
                    .get(component.as_slice())
                    .copied()
            })
            .flatten()
    }

    fn admit_negative(&mut self, source: &dyn MountFilesystem, parent: u64, name: &ChildName,
        stamp: Option<ViewStamp>, work: &super::NativeMountMeterWork) -> Result<Option<Duration>, FuseRequestError>
    {
        let Some(stamp) = stamp else { return Ok(None); };
        if !name.is_exact() || !self.admissible(source, name.key(), None, Some(stamp), work)? { return Ok(None); }
        let Some(component) = name.key().components().last() else { return Ok(None); };
        let Some(entry) = self.by_inode.get_mut(&parent) else { return Ok(None); };
        Ok(entry.remember_negative_child(component, stamp, work)?.then_some(CACHE_TTL))
    }

    fn admit_listing(&mut self, source: &dyn MountFilesystem, inode: u64,
        stamp: Option<ViewStamp>, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        let Some(entry) = self.by_inode.get(&inode) else { return Ok(()); };
        let admitted = self.admissible(source, entry.anchor(), Some(entry.lookup.node.file_id), stamp, work)?;
        if !admitted { fuse_reserve(&mut self.invalidation.deferred, 1, work)?; }
        if let Some(entry) = self.by_inode.get_mut(&inode) { hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN)); }
        if !admitted { self.invalidation.deferred.push(KernelCacheItem::Inode(inode)); }
        Ok(())
    }

    fn apply(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        resolved: &Resolved,
        work: &super::NativeMountMeterWork,
    ) -> Result<(FileAttr, Duration), FuseRequestError> {
        let lookup = resolved.lookup;
        if inode == ROOT_INODE {
            replace_root_lookup(&mut self.by_inode, &mut self.inode_by_file, lookup, work)?;
        }
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != lookup.node.file_id {
            return Err(libc::ESTALE.into());
        }
        entry.lookup = lookup;
        entry.facts = resolved.stamp;
        if let Some(path) = resolved.through.as_ref() {
            if let Some(binding) = entry.binding_mut(path, work)? {
                binding.verified = resolved.stamp;
            }
        }
        let ttl = self.admit_attributes(source, inode, resolved.stamp, work)?;
        Ok((self.attr(inode, &lookup)?, ttl))
    }

    fn node_facts(&self, source: &dyn MountFilesystem, inode: u64, handle: Option<u64>,
        work: &super::NativeMountMeterWork) -> Result<Result<Resolved, NodeFacts>, FuseRequestError>
    {
        fuse_item(work)?;
        let entry = self.by_inode.get(&inode).ok_or(libc::ESTALE)?;
        if let Some(handle) = handle {
            let open_file = self.open_handle(inode, handle, work)?;
            return Ok(match entry.current_facts(source, work)? {
                Some(stamp) => Ok(Resolved { lookup: entry.lookup, stamp: Some(stamp), through: None }),
                None => Err(NodeFacts::Handle(open_file)),
            });
        }
        if let Some(stamp) = entry.current_facts(source, work)? {
            let through = entry.bindings.first().map(|binding| fuse_clone_path(&binding.path, work)).transpose()?;
            return Ok(Ok(Resolved { lookup: entry.lookup, stamp: Some(stamp), through }));
        }
        if entry.bindings.is_empty() {
            return self.open_inode(inode, work)?.map(|file| Err(NodeFacts::Handle(file))).ok_or(libc::ESTALE.into());
        }
        let mut paths = fuse_vec(entry.bindings.len(), work)?;
        for binding in &entry.bindings { fuse_item(work)?; paths.push(fuse_clone_path(&binding.path, work)?); }
        Ok(Err(NodeFacts::Names { file_id: (inode != ROOT_INODE).then_some(entry.lookup.node.file_id), paths }))
    }

    fn node_attr(&self, inode: u64) -> Result<FileAttr, i32> {
        let lookup = self.by_inode.get(&inode).ok_or(libc::ESTALE)?.lookup;
        self.attr(inode, &lookup)
    }

    fn attr(&self, inode: u64, lookup: &MountLookup) -> Result<FileAttr, i32> {
        let node = lookup.node;
        let kind = file_type(node.kind)?;
        let defaults = self.defaults;
        let default_mode = if defaults.writable { 0o755 } else { 0o555 };
        Ok(FileAttr {
            ino: INodeNo(inode),
            size: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            atime: metadata_time(lookup.metadata.accessed_ns),
            mtime: metadata_time(lookup.metadata.modified_ns),
            ctime: metadata_time(lookup.metadata.changed_ns),
            crtime: metadata_time(lookup.metadata.created_ns),
            kind,
            perm: u16::try_from(metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777)
                .unwrap_or(0o7777),
            nlink: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, defaults.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, defaults.mount_gid),
            rdev: node
                .device
                .map(|(major, minor)| native_device_number(major, minor))
                .transpose()?
                .unwrap_or(0),
            blksize: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }

    fn admit_file_handle(
        &mut self, source: &dyn MountFilesystem, inode: u64, opened: OpenedFile<'_>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(u64, FopenFlags, Option<PageStore>), FuseRequestError> {
        fuse_item(work)?;
        let OpenedFile { path, lookup, stamp, flags, open_file, dirty, stores_pages } = opened;
        let file_id = lookup.node.file_id;
        let handle = self.next_handle;
        if self.files.contains_key(&handle) { return Err(libc::EMFILE.into()); }
        let admitted = self.admissible(source, path, Some(file_id), stamp, work)?;
        let previous = self.by_inode.get(&inode).ok_or(libc::ESTALE)?.cached_content;
        let opened_version = ContentVersion::of(lookup);
        let cached = match previous {
            None => CachedContent::Absent,
            Some(previous) if previous == opened_version => CachedContent::Current,
            Some(_) => CachedContent::Superseded,
        };
        let claim = changes_content(flags).then_some(file_id);
        let storage = fuse_initialized((), std::mem::size_of::<(u64, FileHandle)>() as u64, work)?;
        let operation = Arc::new(fuse_initialized(Mutex::new(()),
            std::mem::size_of::<FuseOwned<Mutex<()>>>() as u64, work)?);
        self.files.try_reserve(1).map_err(NativeMountError::Allocation)?;
        let store = match (stamp, u32::try_from(lookup.node.logical_bytes)) {
            (Some(stamp), Ok(length)) if stores_pages && admitted && cached == CachedContent::Absent
                && claim.is_none() && flags & libc::O_DIRECT == 0 && length != 0
                && u64::from(length) <= PAGE_STORE_LIMIT && self.page_stores.may_begin(file_id) =>
            {
                let path = fuse_clone_path(path, work)?;
                let open_file = open_file.clone(work)?;
                self.page_stores.begin(file_id, stamp, work)?;
                Some(PageStore { inode, file_id, path, stamp, length, open_file })
            }
            _ => None,
        };
        if let Some(file_id) = claim { self.page_stores.claim(file_id, work)?; }
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        entry.open_handles = entry.open_handles.saturating_add(1);
        hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        entry.cached_content = Some(opened_version);
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        self.files.insert(handle, FileHandle { inode, open_file, operation, dirty, claim, _storage: storage });
        let flushes_on_close = flags & libc::O_ACCMODE != libc::O_RDONLY && source.flush_on_handle_close();
        let mut open_flags = FopenFlags::empty();
        open_flags.set(FopenFlags::FOPEN_KEEP_CACHE, admitted && cached == CachedContent::Current);
        open_flags.set(FopenFlags::FOPEN_NOFLUSH, !flushes_on_close);
        Ok((handle, open_flags, store))
    }

    fn discard_file_handle(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let Some(file) = self.files.remove(&handle) else {
            return Err(libc::ESTALE);
        };
        if file.inode != inode {
            self.files.insert(handle, file);
            return Err(libc::ESTALE);
        }
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            self.files.insert(handle, file);
            return Err(libc::ESTALE);
        };
        if entry.open_handles == 0 {
            self.files.insert(handle, file);
            return Err(libc::EIO);
        }
        entry.open_handles -= 1;
        if let Some(file_id) = file.claim {
            self.page_stores.release(file_id);
        }
        if entry.bindings.is_empty()
            && entry.lookup_references == 0
            && entry.open_handles == 0
            && let Some(mut entry) = self.by_inode.remove(&inode)
        {
            forget_file(&mut self.inode_by_file, entry.lookup.node.file_id, inode, &mut entry.identity_storage);
        }
        Ok(())
    }

    fn open_handle(&self, inode: u64, handle: u64, work: &super::NativeMountMeterWork)
        -> Result<FuseOpenFile, FuseRequestError>
    {
        fuse_item(work)?;
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode { return Err(libc::ESTALE.into()); }
        file.open_file.clone(work)
    }

    fn open_handle_operation(&self, inode: u64, handle: u64, work: &super::NativeMountMeterWork)
        -> Result<Arc<FuseOwned<Mutex<()>>>, FuseRequestError>
    {
        fuse_item(work)?;
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode { return Err(libc::ESTALE.into()); }
        fuse_arc_clone(&file.operation, work).map_err(Into::into)
    }

    fn mark_handle_dirty(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let file = self.files.get_mut(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        file.dirty = true;
        Ok(())
    }

    fn open_inode(&self, inode: u64, work: &super::NativeMountMeterWork)
        -> Result<Option<FuseOpenFile>, FuseRequestError>
    {
        for file in self.files.values() {
            fuse_item(work)?;
            if file.inode == inode { return file.open_file.clone(work).map(Some); }
        }
        Ok(None)
    }

    fn attributes_to_read(&self, source: &dyn MountFilesystem, inode: u64,
        work: &super::NativeMountMeterWork) -> Result<Option<AttributeTarget>, FuseRequestError>
    {
        fuse_item(work)?;
        if let Some(entry) = self.by_inode.get(&inode) {
            if entry.lacks_named_attributes(source, work)? { return Ok(None); }
        }
        self.attribute_target(inode, work).map(Some)
    }

    fn attribute_target(&self, inode: u64, work: &super::NativeMountMeterWork)
        -> Result<AttributeTarget, FuseRequestError>
    {
        match self.open_inode(inode, work)? {
            Some(open_file) => Ok(AttributeTarget::Handle(open_file)),
            None => Ok(AttributeTarget::Path(fuse_clone_path(self.path(inode)?, work)?)),
        }
    }

    fn apply_handle_facts(&mut self, inode: u64, lookup: MountLookup, stamp: Option<ViewStamp>) {
        if let Some(entry) = self.by_inode.get_mut(&inode)
            && entry.lookup.node.file_id == lookup.node.file_id
        {
            entry.lookup = lookup;
            entry.facts = stamp;
        }
    }

    fn retain_detached_handles(&mut self, inode: u64, detached: &FuseOpenFile,
        work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        let mut assigned = 0_u64;
        for file in self.files.values_mut() {
            fuse_item(work)?;
            if file.inode == inode {
                file.open_file = detached.clone(work)?;
                assigned = assigned.saturating_add(1);
            }
        }
        let expected = self.by_inode.get(&inode).map_or(0, |entry| entry.open_handles);
        debug_assert_eq!(assigned, expected);
        Ok(())
    }

    fn unopened_referenced_inode(&self, lookup: Option<&MountLookup>) -> Option<u64> {
        let lookup = lookup?;
        if lookup.node.kind != MountNodeKind::Regular || lookup.node.link_count != 1 {
            return None;
        }
        let inode = self.inode_by_file.get(&lookup.node.file_id).copied()?;
        self.by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references != 0 && entry.open_handles == 0)
            .then_some(inode)
    }

    fn detaching_inode(&self, lookup: Option<&MountLookup>) -> Option<u64> {
        let lookup = lookup?;
        if lookup.node.kind != MountNodeKind::Regular || lookup.node.link_count != 1 {
            return None;
        }
        let inode = self.inode_by_file.get(&lookup.node.file_id).copied()?;
        self.by_inode
            .get(&inode)
            .is_some_and(|entry| entry.open_handles != 0)
            .then_some(inode)
    }

    fn remove_path_cache(&mut self, path: &MountPath, work: &super::NativeMountMeterWork)
        -> Result<(), FuseRequestError>
    {
        fuse_item(work)?;
        if let Some(inode) = self.inode_by_path.get(path).copied() {
            unbind(&mut self.by_inode, &mut self.inode_by_file, inode, path, work)?;
            self.inode_by_path.remove(path);
        }
        Ok(())
    }

    fn remove_binding(&mut self, inode: u64, path: &MountPath, work: &super::NativeMountMeterWork)
        -> Result<(), FuseRequestError>
    {
        unbind(&mut self.by_inode, &mut self.inode_by_file, inode, path, work)?;
        if self.inode_by_path.get(path) == Some(&inode) { self.inode_by_path.remove(path); }
        Ok(())
    }

    fn invalidate_prefix(&mut self, prefix: &MountPath, work: &super::NativeMountMeterWork)
        -> Result<(), FuseRequestError>
    {
        let mut affected = fuse_vec(0, work)?;
        for (inode, entry) in &self.by_inode {
            for binding in &entry.bindings {
                fuse_item(work)?;
                if fuse_has_prefix(&binding.path, prefix, work)? {
                    let path = fuse_clone_path(&binding.path, work)?;
                    fuse_reserve(&mut affected, 1, work)?;
                    affected.push((*inode, path));
                }
            }
        }
        for (inode, path) in affected { self.remove_binding(inode, &path, work)?; }
        Ok(())
    }

    fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath,
        work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError>
    {
        let mut replacements = fuse_vec(0, work)?;
        let mut paths = HashMap::new();
        for (inode, entry) in &self.by_inode {
            for (index, binding) in entry.bindings.iter().enumerate() {
                fuse_item(work)?;
                let rebased = fuse_replace_prefix(&binding.path, source, destination, work)?;
                let mut key = fuse_clone_path(rebased.as_deref().unwrap_or(&binding.path), work)?;
                key.claim(std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64, work)?;
                paths.try_reserve(1).map_err(NativeMountError::Allocation)?;
                let previous = paths.insert(key, *inode);
                debug_assert!(previous.is_none(), "a path is bound to one inode");
                if let Some(path) = rebased {
                    fuse_reserve(&mut replacements, 1, work)?;
                    replacements.push((*inode, index, path));
                }
            }
        }
        // All fallible path buffers are ready before any inode alias changes.
        self.streams.rename_prefix(source, destination, work)?;
        for (inode, index, path) in replacements {
            self.by_inode.get_mut(&inode).expect("planned inode remains locked").bindings[index].path = path;
        }
        drop(std::mem::replace(&mut self.inode_by_path, paths));
        Ok(())
    }

    fn directory_open_flags_with_meter(source: &dyn MountFilesystem, work: &super::NativeMountMeterWork)
        -> Result<FopenFlags, FuseRequestError>
    {
        Ok(if source.view_stamp_with_meter(work)?.is_some() {
            FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE
        } else { FopenFlags::empty() })
    }

    fn directory_stream(&mut self, source: &dyn MountFilesystem, inode: u64, offset: u64,
        work: &super::NativeMountMeterWork) -> Result<DirectoryStream, FuseRequestError>
    {
        fuse_item(work)?;
        if let Some(stream) = self.streams.take(inode, offset) { return Ok(stream); }
        if !source.view_is_stable_with_meter(work)? { return Err(libc::ESTALE.into()); }
        let path = fuse_clone_path(self.path(inode)?, work)?;
        let parent_inode = fuse_parent(&path, work)?
            .and_then(|parent| self.inode_by_path.get(&*parent).copied()).unwrap_or(ROOT_INODE);
        Ok(DirectoryStream {
            path, parent_inode, binding_epoch: source.binding_epoch_with_meter(work)?,
            page: None, entry_position: 0, entries_stamp: None, confirmed: ConfirmedNames::default(),
            exhausted: false, emitted: 0, parked: 0, park_replacement: None, storage: FuseOwned::empty(()),
        })
    }

    fn list_directory<L: DirectoryListing>(
        &mut self, source: &dyn MountFilesystem, inode: u64, stream: &mut DirectoryStream,
        listing: &mut Option<L>, _span: &tracing::Span, work: &super::NativeMountMeterWork,
    ) -> Result<(), FuseRequestError> {
        let mut listed = false;
        let mut unlisted_reference = None;
        let result = (|| -> Result<(), FuseRequestError> {
            if stream.emitted < 2 {
                let attr = self.node_attr(inode)?;
                let resumed = stream.emitted;
                let dots = [(inode, ".", 1), (stream.parent_inode, "..", 2)];
                for (dot_inode, name, next) in dots.into_iter().filter(|dot| dot.2 > resumed) {
                    fuse_item(work)?;
                    let dot = FileAttr { ino: INodeNo(dot_inode), ..attr };
                    fuse_copy_admit(name.len() as u64, work)?;
                    if listing.as_mut().expect("unsent listing").push(
                        next, OsStr::new(name), &dot, &Duration::ZERO)
                    {
                        listing.take().expect("unsent listing").ok();
                        return Ok(());
                    }
                    fuse_copy_record(name.len() as u64, work)?;
                    stream.emitted = next;
                }
            }
            self.admit_listing(source, inode, stream.entries_stamp, work)?;
            loop {
                let next = stream.emitted.saturating_add(1);
                let Some(entry) = stream.page.as_ref()
                    .and_then(|page| page.entries().get(stream.entry_position))
                else {
                    listing.take().expect("unsent listing").ok();
                    return Ok(());
                };
                fuse_item(work)?;
                let path = fuse_child_path(&stream.path, &entry.name, work)?;
                let child = ChildName::new(source, path, work)?;
                let current = match stream.entries_stamp {
                    Some(stamp) if source.unchanged_since_with_meter(&stream.path, None, stamp, work)?
                        && source.unchanged_since_with_meter(child.key(), Some(entry.node.file_id), stamp, work)? =>
                            Some(stamp),
                    _ => None,
                };
                let page_lookup = MountLookup { node: entry.node, metadata: entry.metadata };
                // A stale page must not roll back facts mounted writes refreshed.
                let lookup = if current.is_some() { page_lookup } else {
                    self.inode_by_file.get(&page_lookup.node.file_id)
                        .and_then(|known| self.by_inode.get(known)).map_or(page_lookup, |known| known.lookup)
                };
                let child_inode = self.intern(source, fuse_clone_path(child.key(), work)?, &lookup,
                    current, L::COUNTS_LOOKUPS, &stream.confirmed, work)?;
                if L::COUNTS_LOOKUPS { unlisted_reference = Some(child_inode); }
                let attr = self.attr(child_inode, &lookup)?;
                let ttl = if L::COUNTS_LOOKUPS {
                    self.admit_entry(source, child_inode, &child, current, work)?
                } else { Duration::ZERO };
                fuse_copy_admit(entry.name.len() as u64, work)?;
                if listing.as_mut().expect("unsent listing").push(
                    next, OsStr::from_bytes(&entry.name), &attr, &ttl)
                {
                    if let Some(inode) = unlisted_reference.take() {
                        self.release_lookup_reference_administrative(inode, 1);
                    }
                    listing.take().expect("unsent listing").ok();
                    return Ok(());
                }
                fuse_copy_record(entry.name.len() as u64, work)?;
                unlisted_reference = None;
                listed = true;
                stream.emitted = next;
                stream.entry_position += 1;
            }
        })();
        if let Some(inode) = unlisted_reference {
            self.release_lookup_reference_administrative(inode, 1);
        }
        if result.is_err() && listed {
            listing.take().expect("unsent listing prefix").ok();
        }
        result
    }

}

fn stale_kernel_items(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<FuseOwned<MountPath>, u64>,
    unchanged_since: impl Fn(&MountPath, Option<crate::FileId>, ViewStamp) -> Result<bool, FuseRequestError>,
    through: ViewStamp, work: &super::NativeMountMeterWork,
) -> Result<FuseOwned<Vec<KernelCacheItem>>, FuseRequestError> {
    enum Action { Inode(u64), Negative { inode: u64, item: usize }, Binding { inode: u64, index: usize } }
    let mut items = fuse_vec(0, work)?;
    let mut actions = fuse_vec(0, work)?;
    for (inode, entry) in by_inode.iter() {
        fuse_item(work)?;
        let file_id = entry.lookup.node.file_id;
        let anchor = entry.bindings.first().map_or(&ROOT_PATH, |binding| &*binding.path);
        if let Some(held) = entry.kernel {
            if !unchanged_since(anchor, Some(file_id), held)? {
                fuse_reserve(&mut items, 1, work)?;
                fuse_reserve(&mut actions, 1, work)?;
                items.push(KernelCacheItem::Inode(*inode));
                actions.push(Action::Inode(*inode));
            }
        }
        for (name, held) in &entry.negative_children {
            fuse_item(work)?;
            let path = fuse_child_path(anchor, name, work)?;
            if !unchanged_since(&path, None, *held)? {
                let name = fuse_copy_bytes(name, work)?;
                fuse_reserve(&mut items, 1, work)?;
                fuse_reserve(&mut actions, 1, work)?;
                actions.push(Action::Negative { inode: *inode, item: items.len() });
                items.push(KernelCacheItem::Entry { parent: *inode, name });
            }
        }
        for (index, binding) in entry.bindings.iter().enumerate() {
            fuse_item(work)?;
            if let Some(held) = binding.kernel {
                if !unchanged_since(&binding.path, Some(file_id), held)? {
                    if let Some(parent) = fuse_parent(&binding.path, work)? {
                        if let Some(parent_inode) = inode_by_path.get(&*parent).copied() {
                            let name = fuse_copy_bytes(binding.path.components().last().ok_or(libc::ESTALE)?, work)?;
                            fuse_reserve(&mut items, 1, work)?;
                            items.push(KernelCacheItem::Entry { parent: parent_inode, name });
                        }
                    }
                    fuse_reserve(&mut actions, 1, work)?;
                    actions.push(Action::Binding { inode: *inode, index });
                }
            }
        }
    }
    for action in &actions {
        match action {
            Action::Inode(inode) => by_inode.get_mut(inode).expect("planned inode remains locked").kernel = Some(through),
            Action::Binding { inode, index } =>
                by_inode.get_mut(inode).expect("planned binding remains locked").bindings[*index].kernel = None,
            Action::Negative { inode, item } => {
                let KernelCacheItem::Entry { name, .. } = &items[*item] else { unreachable!("planned negative entry") };
                drop(by_inode.get_mut(inode).expect("planned directory remains locked").negative_children.remove(name.as_slice()));
            }
        }
    }
    Ok(items)
}

fn cached_projected_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>, inode_by_path: &HashMap<FuseOwned<MountPath>, u64>,
    path: &MountPath, unchanged_since: impl FnOnce(crate::FileId, ViewStamp) -> Result<bool, FuseRequestError>,
    work: &super::NativeMountMeterWork,
) -> Result<Option<(u64, MountLookup, ViewStamp)>, FuseRequestError> {
    fuse_item(work)?;
    let Some(inode) = inode_by_path.get(path).copied() else { return Ok(None); };
    let Some(entry) = by_inode.get_mut(&inode) else { return Ok(None); };
    let Some(stamp) = entry.binding(path, work)?.and_then(|binding| binding.verified)
        .zip(entry.facts).map(|(name, facts)| name.min(facts)) else { return Ok(None); };
    if !unchanged_since(entry.lookup.node.file_id, stamp)? { return Ok(None); }
    entry.lookup_references = entry.lookup_references.saturating_add(1);
    Ok(Some((inode, entry.lookup, stamp)))
}

fn replace_root_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>, inode_by_file: &mut HashMap<crate::FileId, u64>,
    lookup: MountLookup, work: &super::NativeMountMeterWork,
) -> Result<(), FuseRequestError> {
    fuse_item(work)?;
    if lookup.node.kind != MountNodeKind::Directory { return Err(libc::EIO.into()); }
    let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
    let previous = root.lookup.node.file_id;
    if previous != lookup.node.file_id {
        if inode_by_file.get(&lookup.node.file_id).is_some_and(|inode| *inode != ROOT_INODE) {
            return Err(libc::EIO.into());
        }
        let storage = fuse_initialized((), std::mem::size_of::<(crate::FileId, u64)>() as u64, work)?;
        inode_by_file.try_reserve(1).map_err(NativeMountError::Allocation)?;
        if inode_by_file.get(&previous) == Some(&ROOT_INODE) { inode_by_file.remove(&previous); }
        inode_by_file.insert(lookup.node.file_id, ROOT_INODE);
        root.identity_storage = storage;
        root.lookup = lookup;
    }
    Ok(())
}

fn forget_unbound_names(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<FuseOwned<MountPath>, u64>,
    inode: u64, path: &MountPath,
    still_bound: impl Fn(&MountPath, Option<ViewStamp>) -> Result<bool, FuseRequestError>,
    deferred: &mut FuseOwned<Vec<KernelCacheItem>>, work: &super::NativeMountMeterWork,
) -> Result<(), FuseRequestError> {
    let mut items = fuse_vec(0, work)?;
    let Some(entry) = by_inode.get(&inode) else { return Ok(()); };
    let mut removed = fuse_vec(0, work)?;
    for (index, binding) in entry.bindings.iter().enumerate() {
        fuse_item(work)?;
        if binding.path == *path || still_bound(&binding.path, binding.verified)? { continue; }
        if binding.kernel.is_some() {
            if let Some(parent) = fuse_parent(&binding.path, work)? {
                if let Some(parent_inode) = inode_by_path.get(&*parent).copied() {
                    let component = binding.path.components().last().ok_or(libc::ESTALE)?;
                    let name = fuse_copy_bytes(component, work)?;
                    fuse_reserve(&mut items, 1, work)?;
                    items.push(KernelCacheItem::Entry { parent: parent_inode, name });
                }
            }
        }
        fuse_reserve(&mut removed, 1, work)?;
        removed.push(index);
    }
    let width = std::mem::size_of::<Binding>();
    let mut remaining = entry.bindings.len();
    let mut copied = 0_u64;
    for index in removed.iter().rev().copied() {
        let shifted = (remaining - index - 1).checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        copied = copied.checked_add(shifted).ok_or(crate::WorkError::Overflow)?;
        remaining -= 1;
    }
    fuse_reserve(deferred, items.len(), work)?;
    let appended = items.len().checked_mul(std::mem::size_of::<KernelCacheItem>()).ok_or(crate::WorkError::Overflow)? as u64;
    fuse_copy_admit(copied.checked_add(appended).ok_or(crate::WorkError::Overflow)?, work)?;
    deferred.append(&mut items);
    fuse_copy_record(appended, work)?;
    let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
    for index in removed.iter().rev().copied() {
        let binding = entry.bindings.remove(index);
        if inode_by_path.get(&*binding.path) == Some(&inode) { inode_by_path.remove(&*binding.path); }
        drop(binding);
    }
    fuse_copy_record(copied, work)?;
    Ok(())
}

fn bind_path(
    by_inode: &mut HashMap<u64, InodeEntry>, inode_by_path: &mut HashMap<FuseOwned<MountPath>, u64>,
    inode_by_file: &mut HashMap<crate::FileId, u64>, path: &MountPath, inode: u64,
    work: &super::NativeMountMeterWork,
) -> Result<(), FuseRequestError> {
    fuse_item(work)?;
    if let Some(previous) = inode_by_path.get(path).copied() {
        if previous != inode { unbind(by_inode, inode_by_file, previous, path, work)?; }
        *inode_by_path.get_mut(path).expect("existing path remains locked") = inode;
        return Ok(());
    }
    let mut key = fuse_clone_path(path, work)?;
    key.claim(std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64, work)?;
    inode_by_path.try_reserve(1).map_err(NativeMountError::Allocation)?;
    inode_by_path.insert(key, inode);
    Ok(())
}

fn unbind(
    by_inode: &mut HashMap<u64, InodeEntry>, inode_by_file: &mut HashMap<crate::FileId, u64>,
    inode: u64, path: &MountPath, work: &super::NativeMountMeterWork,
) -> Result<(), FuseRequestError> {
    let Some(entry) = by_inode.get_mut(&inode) else { return Ok(()); };
    let mut matched = None;
    for (index, binding) in entry.bindings.iter().enumerate() {
        fuse_item(work)?;
        if binding.path == *path { matched = Some(index); break; }
    }
    if let Some(index) = matched {
        let copied = (entry.bindings.len() - index - 1).checked_mul(std::mem::size_of::<Binding>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        fuse_copy_admit(copied, work)?;
        drop(entry.bindings.remove(index));
        fuse_copy_record(copied, work)?;
    }
    let remove = entry.bindings.is_empty() && entry.lookup_references == 0 && entry.open_handles == 0;
    if remove {
        let mut entry = by_inode.remove(&inode).expect("matched inode remains locked");
        forget_file(inode_by_file, entry.lookup.node.file_id, inode, &mut entry.identity_storage);
        drop(entry);
    }
    Ok(())
}

fn forget_file(
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    file_id: crate::FileId,
    inode: u64,
    identity_storage: &mut FuseOwned<()>,
) {
    if inode_by_file.get(&file_id) == Some(&inode) {
        inode_by_file.remove(&file_id);
        drop(std::mem::take(identity_storage));
    }
}

fn retire_reused_identity(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    inode: u64,
) {
    if inode != ROOT_INODE
        && let Some(entry) = by_inode.get_mut(&inode)
        && entry.bindings.is_empty()
    {
        forget_file(inode_by_file, entry.lookup.node.file_id, inode, &mut entry.identity_storage);
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "projection indexes are updated together"
)]
fn intern_projected(
    next_inode: &mut u64, by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<FuseOwned<MountPath>, u64>, inode_by_file: &mut HashMap<crate::FileId, u64>,
    path: FuseOwned<MountPath>, lookup: &MountLookup, stamp: Option<ViewStamp>,
    lookup_reference: bool, work: &super::NativeMountMeterWork,
) -> Result<u64, FuseRequestError> {
    fuse_item(work)?;
    if let Some(inode) = inode_by_file.get(&lookup.node.file_id).copied() {
        let needs_binding = by_inode.get(&inode).ok_or(libc::ESTALE)?.binding(&path, work)?.is_none();
        if needs_binding { fuse_reserve(&mut by_inode.get_mut(&inode).ok_or(libc::ESTALE)?.bindings, 1, work)?; }
        bind_path(by_inode, inode_by_path, inode_by_file, &path, inode, work)?;
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if needs_binding { entry.bindings.push(Binding::new(path, stamp)); }
        else if let Some(binding) = entry.binding_mut(&path, work)? { binding.verified = stamp; }
        entry.lookup = *lookup;
        entry.facts = stamp;
        if lookup_reference { entry.lookup_references = entry.lookup_references.saturating_add(1); }
        return Ok(inode);
    }
    let inode = *next_inode;
    if inode <= ROOT_INODE { return Err(libc::EOVERFLOW.into()); }
    let following = inode.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let mut key = fuse_clone_path(&path, work)?;
    key.claim(std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64, work)?;
    let entry = InodeEntry::new(path, *lookup, stamp, u64::from(lookup_reference), work)?;
    by_inode.try_reserve(1).map_err(NativeMountError::Allocation)?;
    inode_by_path.try_reserve(1).map_err(NativeMountError::Allocation)?;
    inode_by_file.try_reserve(1).map_err(NativeMountError::Allocation)?;
    if let Some(previous) = inode_by_path.get(&*key).copied() {
        unbind(by_inode, inode_by_file, previous, &key, work)?;
    }
    inode_by_path.insert(key, inode);
    inode_by_file.insert(lookup.node.file_id, inode);
    by_inode.insert(inode, entry);
    *next_inode = following;
    Ok(inode)
}

trait DirectoryListing {
    const COUNTS_LOOKUPS: bool;

    fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, ttl: &Duration) -> bool;

    fn ok(self);
    fn error(self, error: Errno);
}

impl DirectoryListing for ReplyDirectory {
    const COUNTS_LOOKUPS: bool = false;

    fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, _ttl: &Duration) -> bool {
        self.add(attr.ino, offset, attr.kind, name)
    }

    fn ok(self) {
        Self::ok(self);
    }

    fn error(self, error: Errno) {
        Self::error(self, error);
    }
}

impl DirectoryListing for ReplyDirectoryPlus {
    const COUNTS_LOOKUPS: bool = true;

    fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, ttl: &Duration) -> bool {
        self.add(attr.ino, offset, name, ttl, attr, Generation(0))
    }

    fn ok(self) {
        Self::ok(self);
    }

    fn error(self, error: Errno) {
        Self::error(self, error);
    }
}

#[derive(Default)]
struct FuseHandlerReports {
    completed_future_request_work: crate::WorkCounters,
    first_fatal: Option<super::NativeMountCallbackFailure>,
}

fn fuse_verify_callback_guard(
    guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    work: Option<&super::NativeMountMeterWork>,
) -> Result<(), NativeMountError> {
    let Some(guard) = guard else { return Ok(()); };
    let work = work.ok_or(NativeMountError::ControlPending)?;
    super::adapter::CallbackRuntime::existing()?.verify_native_callback_guard(guard.as_ref(), work)
}

fn fuse_same_native_view(left: &super::NativeMountMeterWork, right: &super::NativeMountMeterWork) -> bool {
    Arc::ptr_eq(left.meter(), right.meter())
        && left.resource_allowance().map(Arc::as_ptr) == right.resource_allowance().map(Arc::as_ptr)
        && left.capture_origin().map(Weak::as_ptr) == right.capture_origin().map(Weak::as_ptr)
        && left.invocation_view_state().map(Arc::as_ptr) == right.invocation_view_state().map(Arc::as_ptr)
        && left.source_execution_view().map(Arc::as_ptr) == right.source_execution_view().map(Arc::as_ptr)
}

fn fuse_native_source_original(
    source: &dyn MountFilesystem, candidate: Option<&super::NativeMountMeterWork>,
) -> Result<Option<super::NativeMountMeterWork>, NativeMountError> {
    let original = source.native_original_work()?;
    if let Some(original) = &original {
        if let Some(candidate) = candidate {
            if !fuse_same_native_view(original, candidate) {
                return Err(NativeMountError::ControlPending);
            }
        }
        original.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
    }
    Ok(original)
}

struct ProjectionCore {
    source: Arc<dyn MountFilesystem>,
    handler_budget: crate::WorkBudget,
    handler_cancellation: crate::CancellationToken,
    startup_meter: Mutex<super::NativeMountMeterActivation>,
    handler_failed: AtomicBool,
    handler_error: Mutex<FuseHandlerReports>,
    state: Mutex<ProjectionState>,
    names: RwLock<()>,
    invalidation: Condvar,
    page_stored: Condvar,
    stopping: AtomicBool,
    origin: ViewOrigin,
    notifier: OnceLock<Notifier>,
    #[cfg(test)]
    requests: Mutex<Vec<&'static str>>,
    native_allocation: Option<crate::NativeAllocation>,
    callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    source_original_work: Option<super::NativeMountMeterWork>,
    allocation_custody: Option<crate::demand::NativeMountAllocationCustody>,
}

impl Drop for ProjectionCore {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        drop(std::mem::take(&mut state.inode_by_path));
        drop(std::mem::take(&mut state.inode_by_file));
        drop(std::mem::take(&mut state.by_inode));
    }
}

fn run_fuse_retirement(owned: (Arc<ProjectionCore>, Arc<FuseDriverControl>, crate::NativeAllocation)) {
    let (core, control, _) = &owned;
    let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
    while !state.startup_finished {
        state = control.changed.wait(state).unwrap_or_else(PoisonError::into_inner);
    }
    drop(state);
    if let Some(notifier) = core.notifier.get() {
        run_invalidator(core, notifier);
    }
    loop {
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.stop_requested && !state.retired {
            state = control.changed.wait(state).unwrap_or_else(PoisonError::into_inner);
        }
        if state.retired { return; }
        state.stop_requested = false;
        drop(state);
        let result = retire_fuse_driver(control, None);
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Err(error) = result {
            if state.failure.is_none() { state.failure = Some(error); }
        }
        control.changed.notify_all();
        if state.retired { return; }
    }
}

pub(super) struct FuseStartupMeterScope {
    core: Arc<ProjectionCore>,
    meter: super::NativeMountMeterWork,
    generation: u64,
}

impl FuseStartupMeterScope {
    fn new(core: &Arc<ProjectionCore>, meter: &super::NativeMountMeterWork)
        -> Result<Self, NativeMountError> {
        let generation = core.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).enter(meter)?;
        core.invalidation.notify_all();
        Ok(Self { core: Arc::clone(core), meter: meter.clone(), generation })
    }
    /// Retains the original callback meter for accepted I/O.
    pub(super) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::new(&self.core, &self.meter)
    }
}

impl Drop for FuseStartupMeterScope {
    fn drop(&mut self) {
        self.core.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).leave(&self.meter, self.generation);
    }
}

impl ProjectionCore {
    fn new(
        source: Arc<dyn MountFilesystem>, state: ProjectionState,
        handler_budget: crate::WorkBudget, handler_cancellation: &crate::CancellationToken,
    ) -> Result<Self, NativeMountError> {
        let source_original_work = fuse_native_source_original(source.as_ref(), None)?;
        let callback_guard = source.native_callback_guard();
        fuse_verify_callback_guard(callback_guard.as_ref(), source_original_work.as_ref())?;
        Ok(Self {
            source,
            allocation_custody: None,
            handler_budget,
            handler_cancellation: handler_cancellation.clone(),
            startup_meter: Mutex::new(super::NativeMountMeterActivation::default()),
            handler_failed: AtomicBool::new(false),
            handler_error: Mutex::new(FuseHandlerReports::default()),
            state: Mutex::new(state),
            names: RwLock::new(()),
            invalidation: Condvar::new(),
            page_stored: Condvar::new(),
            stopping: AtomicBool::new(false),
            origin: ViewOrigin::new(),
            notifier: OnceLock::new(),
            #[cfg(test)]
            requests: Mutex::new(Vec::new()),
            native_allocation: None,
            callback_guard,
            source_original_work,
        })
    }
    fn root_with_meter(
        source: Arc<dyn MountFilesystem>, root: MountLookup, stamp: Option<ViewStamp>,
        writable: bool, uid: u32, gid: u32, native: &super::NativeMountMeterWork,
    ) -> Result<Arc<Self>, NativeMountError> {
        fuse_native_source_original(source.as_ref(), Some(native))?;
        let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(native)?;
        let materialization = native.native_materialization_work();
        let state = ProjectionState {
            defaults: AttributeDefaults { writable, mount_uid: uid, mount_gid: gid },
            next_inode: ROOT_INODE + 1, next_handle: 1,
            by_inode: HashMap::new(), inode_by_path: HashMap::new(), inode_by_file: HashMap::new(),
            files: HashMap::new(), implicit: HashMap::new(),
            streams: DirectoryStreams::default(), invalidation: InvalidationQueue::default(),
            page_stores: PageStores::default(),
        };
        let (budget, cancellation) = { let work = native.meter().lock(); (work.budget, work.cancellation().clone()) };
        let mut core = Self::new(source, state, budget, &cancellation)?;
        core.allocation_custody = Some(custody.clone());
        let entry = InodeEntry::new(FuseOwned::empty(MountPath::root()), root, stamp, 1, native).map_err(NativeMountError::from)?;
        core.native_allocation = Some(materialization.claim(std::mem::size_of::<Self>() as u64)?);
        core.state.get_mut().unwrap_or_else(PoisonError::into_inner).by_inode =
            HashMap::from([(ROOT_INODE, entry)]);
        let bytes = std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64;
        core.native_allocation.as_mut().expect("original root table guard").grow(bytes)?;
        core.state.get_mut().unwrap_or_else(PoisonError::into_inner).inode_by_path =
            HashMap::from([(FuseOwned::empty(MountPath::root()), ROOT_INODE)]);
        core.state.get_mut().unwrap_or_else(PoisonError::into_inner).inode_by_file =
            HashMap::from([(root.node.file_id, ROOT_INODE)]);
        Ok(Arc::new(core))
    }

    fn state(&self) -> Result<MutexGuard<'_, ProjectionState>, i32> {
        if self.stopping.load(Ordering::Acquire) { return Err(libc::ENODEV); }
        let state = self.state.lock().map_err(|_| libc::EIO)?;
        // Waiters observe queued mutations after reacquiring this lock.
        self.invalidation.notify_all();
        Ok(state)
    }

    fn state_to_finish(&self) -> MutexGuard<'_, ProjectionState> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.invalidation.notify_all();
        state
    }

    fn names(&self) -> Result<RwLockReadGuard<'_, ()>, i32> {
        self.names.read().map_err(|_| libc::EIO)
    }

    fn names_exclusive(&self) -> Result<RwLockWriteGuard<'_, ()>, i32> {
        self.names.write().map_err(|_| libc::EIO)
    }

    fn request(&self, _operation: &'static str) -> FuseRequestScope<'_> {
        FuseRequestScope { core: self, #[cfg(test)] operation: _operation, _origin: self.origin.enter() }
    }

    fn take_handler_error(&self) -> Option<NativeMountError> {
        self.handler_error.lock().unwrap_or_else(PoisonError::into_inner).first_fatal.take()
            .map(|failure| failure.cause)
    }

    fn take_handler_failure(&self) -> Option<super::NativeMountCallbackFailure> {
        let first = self.handler_error.lock().unwrap_or_else(PoisonError::into_inner).first_fatal.take();
        first.or_else(|| self.state_to_finish().invalidation.failure.take())
    }

    fn completed_callback_work(&self) -> crate::WorkCounters {
        self.handler_error.lock().unwrap_or_else(PoisonError::into_inner).completed_future_request_work
    }

    fn record_completed_request_work(&self, work: &crate::WorkCounters) -> Result<(), crate::WorkError> {
        self.handler_error.lock().unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work.try_add_assign(work)
    }

    fn retain_handler_failure(&self, failure: super::NativeMountCallbackFailure) {
        self.handler_failed.store(true, Ordering::Release);
        let mut reports = self.handler_error.lock().unwrap_or_else(PoisonError::into_inner);
        if reports.first_fatal.is_none() { reports.first_fatal = Some(failure); }
        self.invalidation.notify_all();
    }

    fn conserve_completed_request(&self, work: &crate::WorkCounters) {
        if let Err(error) = self.record_completed_request_work(work) {
            self.retain_handler_failure(super::NativeMountCallbackFailure {
                cause: error.into(), work: *work,
            });
        }
    }


    fn record_handler_failure(&self, work: &super::NativeMountMeterWork, error: NativeMountError) {
        if fatal_handler_error(&error) {
            let prefix = work.meter().lock().work;
            self.retain_handler_failure(super::NativeMountCallbackFailure { cause: error, work: prefix });
        }
    }

    fn active_meter(&self) -> Option<super::NativeMountMeterWork> {
        self.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).upgrade()
    }

    fn callback_meter(&self) -> Result<Option<super::NativeMountMeterWork>, NativeMountError> {
        let active = self.active_meter();
        let candidate = active.as_ref().or(self.source_original_work.as_ref());
        let current = fuse_native_source_original(self.source.as_ref(), candidate)?;
        if self.source_original_work.is_some() != current.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        if let Some(original) = &self.source_original_work {
            let current = current.as_ref().expect("installed native original remains present");
            if !fuse_same_native_view(original, current) {
                return Err(NativeMountError::ControlPending);
            }
        }
        let selected = candidate.cloned();
        if let Some(work) = &selected {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        }
        let current_guard = self.source.native_callback_guard();
        match (&self.callback_guard, &current_guard) {
            (None, None) => {}
            (Some(original), Some(current)) if Arc::ptr_eq(original, current) => {
                fuse_verify_callback_guard(Some(original), selected.as_ref())?;
            }
            _ => return Err(NativeMountError::ControlPending),
        }
        Ok(selected)
    }

    fn claim_content(&self, file_id: crate::FileId, work: &super::NativeMountMeterWork)
        -> Result<ContentClaim<'_>, FuseRequestError>
    {
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)?;
        let mut state = self.state()?;
        while state.page_stores.in_flight.contains_key(&file_id) {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)?;
            state = self.page_stored.wait(state).map_err(|_| libc::EIO)?;
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)?;
        }
        state.page_stores.claim(file_id, work)?;
        Ok(ContentClaim { core: self, file_id })
    }
}

#[derive(Debug)]
enum FuseRequestError {
    Native(NativeMountError),
    Errno(i32),
}

impl From<FuseRequestError> for NativeMountError {
    fn from(error: FuseRequestError) -> Self {
        match error {
            FuseRequestError::Native(error) => error,
            FuseRequestError::Errno(error) => Self::Io(std::io::Error::from_raw_os_error(error)),
        }
    }
}

impl From<i32> for FuseRequestError {
    fn from(error: i32) -> Self { Self::Errno(error) }
}

impl From<NativeMountError> for FuseRequestError {
    fn from(error: NativeMountError) -> Self { Self::Native(error) }
}

impl From<MountSourceError> for FuseRequestError {
    fn from(error: MountSourceError) -> Self { Self::Native(error.into()) }
}

impl From<crate::DemandError> for FuseRequestError {
    fn from(error: crate::DemandError) -> Self { Self::Native(error.into()) }
}

impl From<crate::WorkError> for FuseRequestError {
    fn from(error: crate::WorkError) -> Self { Self::Native(error.into()) }
}

impl From<crate::NativeTreeMaterializationError> for FuseRequestError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self { Self::Native(error.into()) }
}

impl From<super::NativeMountStartupAdmissionError> for FuseRequestError {
    fn from(error: super::NativeMountStartupAdmissionError) -> Self { Self::Native(error.into()) }
}


fn clone_fatal_source_error(error: &MountSourceError) -> Option<MountSourceError> {
    let cause = match error.semantic_error() {
        MountSourceError::Work(error) => MountSourceError::Work(error.clone()),
        MountSourceError::Cancelled => MountSourceError::Cancelled,
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)) =>
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error.clone())),
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled) =>
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled),
        MountSourceError::OriginalSnapshotUnavailable => MountSourceError::OriginalSnapshotUnavailable,
        MountSourceError::Indeterminate => MountSourceError::Indeterminate,
        MountSourceError::NativeRecordMaterializationInProgress =>
            MountSourceError::NativeRecordMaterializationInProgress,
        MountSourceError::Allocation(error) => MountSourceError::Allocation(error.clone()),
        MountSourceError::ServicesNotInitialized => MountSourceError::ServicesNotInitialized,
        MountSourceError::ServiceUnavailable(error) => MountSourceError::ServiceUnavailable(Arc::clone(error)),
        MountSourceError::Filesystem(crate::FsError::Work(error)) =>
            MountSourceError::Filesystem(crate::FsError::Work(error.clone())),
        MountSourceError::Filesystem(crate::FsError::Cancelled(error)) =>
            MountSourceError::Filesystem(crate::FsError::Cancelled(*error)),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) =>
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(error.clone())),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) =>
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed) =>
            MountSourceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed),
        _ => return None,
    };
    if let MountSourceError::Retained(owner) = error {
        Some(MountSourceError::Retained(Arc::clone(owner)))
    } else { Some(cause) }
}

fn clone_fatal_handler_error(error: &NativeMountError) -> Option<NativeMountError> {
    match error {
        NativeMountError::Work(error) => Some(NativeMountError::Work(error.clone())),
        NativeMountError::Demand(crate::DemandError::Work(error)) =>
            Some(NativeMountError::Demand(crate::DemandError::Work(error.clone()))),
        NativeMountError::Demand(crate::DemandError::Cancelled) =>
            Some(NativeMountError::Demand(crate::DemandError::Cancelled)),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error)) =>
            Some(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error.clone()))),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) =>
            Some(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled)),
        NativeMountError::Demand(crate::DemandError::Mount(error)) =>
            clone_fatal_source_error(error).map(|error| NativeMountError::Demand(crate::DemandError::Mount(error))),
        NativeMountError::Source(error) => clone_fatal_source_error(error).map(NativeMountError::Source),
        NativeMountError::FuseUnsupportedCapabilities(flags) => Some(NativeMountError::FuseUnsupportedCapabilities(*flags)),
        NativeMountError::Allocation(error) => Some(NativeMountError::Allocation(error.clone())),
        NativeMountError::ServicesNotInitialized => Some(NativeMountError::ServicesNotInitialized),
        NativeMountError::ServiceUnavailable(error) => Some(NativeMountError::ServiceUnavailable(Arc::clone(error))),
        _ => None,
    }
}

fn fatal_handler_error(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Work(_) | NativeMountError::DriverPanicked { .. }
        | NativeMountError::Allocation(_) | NativeMountError::ServicesNotInitialized
        | NativeMountError::ServiceUnavailable(_) | NativeMountError::FuseUnsupportedCapabilities(_)
        | NativeMountError::CallbackDenied { .. } | NativeMountError::ControlPending
        | NativeMountError::Demand(crate::DemandError::Work(_) | crate::DemandError::Cancelled) => true,
        NativeMountError::Materialization(error) => super::native_materialization_failure_is_fatal(error),
        NativeMountError::Source(error) | NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            if let MountSourceError::NativeWork(error) = error.semantic_error() {
                return super::native_materialization_failure_is_fatal(error);
            }
            if let MountSourceError::WorkspaceCause(error) = error.semantic_error() {
                return super::native_workspace_failure_is_fatal(error);
            }
            if let MountSourceError::WorkspacePrefixFailure { source, accounting } = error.semantic_error() {
                return super::native_workspace_failure_is_fatal(source)
                    || super::native_workspace_failure_is_fatal(accounting);
            }
            if matches!(error.semantic_error(), MountSourceError::NativeCallbackDenied { .. }) {
                return false;
            }
            if let MountSourceError::NativeWorkspaceCause(error) = error.semantic_error() {
                return super::native_workspace_failure_is_fatal(&error.source);
            }
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            if matches!(error.semantic_error(), MountSourceError::SourceWatchStartup(_)
                | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. })
            {
                return true;
            }
            matches!(error.semantic_error(), MountSourceError::Work(_) | MountSourceError::Cancelled
                | MountSourceError::Allocation(_) | MountSourceError::ServicesNotInitialized
                | MountSourceError::ServiceUnavailable(_) | MountSourceError::OriginalSnapshotUnavailable
                | MountSourceError::Indeterminate | MountSourceError::NativeRecordMaterializationInProgress
                | MountSourceError::Filesystem(crate::FsError::Work(_) | crate::FsError::Cancelled(_))
                | MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)
                    | crate::kernel::NamespacePathError::Cancelled
                    | crate::kernel::NamespacePathError::AllocationFailed))
        }
        _ => false,
    }
}

fn source_callback_errno(error: &MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::NativeWork(error) => super::native_materialization_errno(error),
        MountSourceError::WorkspaceCause(error) => super::native_workspace_errno(error),
        MountSourceError::NativeWorkspaceCause(error) => super::native_workspace_errno(&error.source),
        MountSourceError::NativeText { kind, .. } | MountSourceError::NativeStaticText { kind, .. } =>
            super::native_source_text_errno(*kind),
        MountSourceError::Io(error) | MountSourceError::NativeRecordIo(error, _) =>
            error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => libc::ECANCELED,
        MountSourceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)) => libc::EIO,
        MountSourceError::PublicationRetryLimit => libc::EIO,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Stale => libc::ESTALE,
        MountSourceError::Cancelled | MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ECANCELED,
        MountSourceError::Filesystem(crate::FsError::NotFound) => libc::ENOENT,
        MountSourceError::Filesystem(crate::FsError::NotDirectory) => libc::ENOTDIR,
        _ => libc::EIO,
    }
}

fn native_callback_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Materialization(error) => super::native_materialization_errno(error),
        NativeMountError::Source(error) | NativeMountError::Demand(crate::DemandError::Mount(error)) => source_callback_errno(error),
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ECANCELED,
        NativeMountError::FuseUnsupportedCapabilities(_) => libc::EOPNOTSUPP,
        _ => libc::EIO,
    }
}

struct FuseRequestScope<'a> {
    core: &'a ProjectionCore,
    #[cfg(test)]
    operation: &'static str,
    _origin: ViewOriginScope,
}

impl FuseRequestScope<'_> {
    fn run<T>(
        &self, callback: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, FuseRequestError>,
    ) -> Result<crate::OperationReceipt<T>, super::NativeMountCallbackFailure> {
        let meter = self.core.callback_meter().map_err(|cause| super::NativeMountCallbackFailure {
            cause,
            work: self.core.source_original_work.as_ref()
                .map_or(crate::WorkCounters::UNCHARGED, |work| work.meter().lock().work),
        })?;
        #[cfg(test)]
        self.core.requests.lock().unwrap_or_else(PoisonError::into_inner).push(self.operation);
        let run = |work: &super::NativeMountMeterWork| {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
            callback(work).map_err(NativeMountError::from)
        };
        if let Some(meter) = meter {
            let result = super::run_native_mount_handler_on_meter(&meter, run);
            let work = meter.meter().lock().work;
            result.map(|value| crate::OperationReceipt { value, work })
                .map_err(|cause| super::NativeMountCallbackFailure { cause, work })
        } else {
            let result = super::run_native_mount_handler(
                self.core.handler_budget, &self.core.handler_cancellation, run,
            );
            let work = match &result { Ok(receipt) => &receipt.work, Err(failure) => &failure.work };
            self.core.conserve_completed_request(work);
            result
        }
    }

    fn finish<T>(&self, result: Result<crate::OperationReceipt<T>, super::NativeMountCallbackFailure>) -> Result<T, i32> {
        result.map(|receipt| receipt.value).map_err(|failure| {
            let errno = native_callback_errno(&failure.cause);
            if fatal_handler_error(&failure.cause) { self.core.retain_handler_failure(failure); }
            errno
        })
    }
}

struct ContentClaim<'a> {
    core: &'a ProjectionCore,
    file_id: crate::FileId,
}

impl Drop for ContentClaim<'_> {
    fn drop(&mut self) {
        self.core
            .state_to_finish()
            .page_stores
            .release(self.file_id);
    }
}

impl ViewObserver for ProjectionCore {
    fn allocation_custody(&self) -> Option<crate::demand::NativeMountAllocationCustody> {
        self.allocation_custody.clone()
    }
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
        if origin == self.origin {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidation.notified = state.invalidation.notified.max(Some(position));
        self.invalidation.notify_all();
    }
}


struct FuseInvalidationBatch<'a> {
    core: &'a ProjectionCore,
    items: FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
}

impl Drop for FuseInvalidationBatch<'_> {
    fn drop(&mut self) {
        let items = std::mem::replace(&mut self.items, FuseOwned::empty(Vec::new().into_iter()));
        self.core.state_to_finish().invalidation.retain_pending_tail(items);
    }
}
fn run_invalidator(core: &ProjectionCore, notifier: &Notifier) {
    let mut blocked: Option<(Option<ViewStamp>, usize, (Weak<crate::demand::NativeDemandReadMeter>,
        Option<Weak<crate::materializer::NativeResourceAllowance>>))> = None;
    loop {
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        let scope_identity = loop {
            if core.stopping.load(Ordering::Acquire) { return; }
            let identity = core.startup_meter.lock().unwrap_or_else(PoisonError::into_inner)
                .upgrade().as_ref().map_or_else(|| (Weak::new(), None), |work|
                    (Arc::downgrade(work.meter()), work.resource_allowance().map(Arc::downgrade)));
            let denied = blocked.as_ref().is_some_and(|(stamp, deferred, previous)| {
                *stamp == state.invalidation.notified && *deferred == state.invalidation.deferred.len()
                    && Weak::ptr_eq(&previous.0, &identity.0)
                    && match (&previous.1, &identity.1) {
                        (None, None) => true,
                        (Some(previous), Some(current)) => Weak::ptr_eq(previous, current),
                        _ => false,
                    }
            });
            if !state.invalidation.is_idle() && !denied { break identity; }
            state = core.invalidation.wait(state).unwrap_or_else(PoisonError::into_inner);
        };
        let mut wait_key = (state.invalidation.notified, state.invalidation.deferred.len(), scope_identity);
        drop(state);
        let mut completed_through = None;
        let mut sdk_failure = None;
        let result = core.request("invalidate").run(|work| {
            let items = {
                let mut state = core.state_to_finish();
                let items = state.invalidation.take_deferred();
                wait_key.0 = state.invalidation.notified;
                wait_key.1 = state.invalidation.deferred.len();
                items
            };
            let mut items = FuseInvalidationBatch { core, items };
            let dropped = drop_kernel_caches(notifier, &mut items.items, work, &mut sdk_failure);
            drop(items);
            dropped?;
            let items = {
                let mut state = core.state_to_finish();
                let through = state.invalidation.notified;
                if let Some(through) = through && state.invalidation.scanned < Some(through) {
                    let state = &mut *state;
                    let items = stale_kernel_items(&mut state.by_inode, &state.inode_by_path,
                        |path, file_id, stamp| core.source.unchanged_since_with_meter(path, file_id, stamp, work)
                            .map_err(FuseRequestError::from), through, work)
                        .map_err(NativeMountError::from)?;
                    state.invalidation.scanned = Some(through);
                    completed_through = Some(through);
                    items.map(Vec::into_iter)
                } else {
                    completed_through = through;
                    FuseOwned::empty(Vec::new().into_iter())
                }
            };
            let mut items = FuseInvalidationBatch { core, items };
            let dropped = drop_kernel_caches(notifier, &mut items.items, work, &mut sdk_failure);
            drop(items);
            dropped?;
            sdk_failure.take().map_or(Ok(()), Err).map_err(FuseRequestError::from)
        });
        let result = result.map(|receipt| receipt.value);
        let denied = result.is_err();
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        if denied { core.handler_failed.store(true, Ordering::Release); }
        if let Some(cause) = sdk_failure {
            if let Err(failure) = &result {
                state.invalidation.failure.get_or_insert(super::NativeMountCallbackFailure {
                    cause, work: failure.work,
                });
            }
        }
        state.invalidation.finish(completed_through, result);
        core.invalidation.notify_all();
        blocked = if denied { Some(wait_key) } else { None };
    }
}

fn drop_kernel_caches(
    notifier: &Notifier, items: &mut FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
    work: &super::NativeMountMeterWork,
    first_sdk_failure: &mut Option<NativeMountError>,
) -> Result<(), NativeMountError> {
    while let Some(item) = items.as_slice().first() {
        work.native_materialization_work().charge(crate::WorkCounters {
            backend_write_operations: 1, items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let dropped = match item {
            KernelCacheItem::Inode(inode) => notifier.inval_inode(INodeNo(*inode), 0, 0),
            KernelCacheItem::Entry { parent, name } =>
                notifier.inval_entry(INodeNo(*parent), OsStr::from_bytes(name)),
        };
        // The real SDK attempted this item, including a genuine failed call.
        drop(items.next());
        if let Err(error) = dropped && error.kind() != std::io::ErrorKind::NotFound {
            first_sdk_failure.get_or_insert(NativeMountError::Io(error));
        }
    }
    Ok(())
}

fn resolve_path(
    source: &dyn MountFilesystem,
    path: &MountPath,
    work: &super::NativeMountMeterWork,
) -> Result<(Option<MountLookup>, Option<ViewStamp>), FuseRequestError> {
    let binding_epoch = source.binding_epoch_with_meter(work)?;
    let _lease = source.acquire_binding_lease_with_meter(binding_epoch, work).map_err(FuseRequestError::from)?;
    let stamp = source.view_stamp_with_meter(work)?;
    let lookup = source.lookup_with_meter(path, work).map_err(FuseRequestError::from)?;
    if !source.view_is_stable_with_meter(work)? || source.binding_epoch_with_meter(work)? != binding_epoch {
        return Err(libc::ESTALE.into());
    }
    Ok((lookup, stamp))
}

fn resolve_node(
    source: &dyn MountFilesystem,
    facts: Result<Resolved, NodeFacts>,
    work: &super::NativeMountMeterWork,
) -> Result<Resolved, FuseRequestError> {
    match facts {
        Ok(current) => Ok(current),
        Err(NodeFacts::Handle(open_file)) => {
            let stamp = source.view_stamp_with_meter(work)?;
            Ok(Resolved {
                lookup: open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?,
                stamp,
                through: None,
            })
        }
        Err(NodeFacts::Names { file_id, paths }) => {
            for path in paths {
                let (lookup, stamp) = resolve_path(source, &path, work)?;
                if let Some(lookup) = lookup
                    && file_id.is_none_or(|file_id| lookup.node.file_id == file_id)
                {
                    return Ok(Resolved {
                        lookup,
                        stamp,
                        through: Some(path),
                    });
                }
            }
            Err(libc::ENOENT.into())
        }
    }
}

struct FuseProjection {
    core: Arc<ProjectionCore>,
    skips_opendir: bool,
    skips_open: bool,
}

struct FuseMount {
    path: FuseOwned<Vec<u8>>,
    device: Option<OwnedFd>,
    helper: Option<std::process::Child>,
    socket: Option<std::os::unix::net::UnixStream>,
    mounted: bool,
}

impl FuseMount {
    fn prepare(destination: &Path, work: &super::NativeMountMeterWork)
        -> Result<Self, NativeMountError>
    {
        let bytes = destination.as_os_str().as_bytes();
        if bytes.contains(&0) { return Err(NativeMountError::InvalidDestination); }
        let count = bytes.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let mut path = fuse_vec(count, work).map_err(NativeMountError::from)?;
        fuse_copy_admit(bytes.len() as u64, work).map_err(NativeMountError::from)?;
        path.extend_from_slice(bytes);
        path.push(0);
        fuse_copy_record(bytes.len() as u64, work).map_err(NativeMountError::from)?;
        Ok(Self { path, device: None, helper: None, socket: None, mounted: false })
    }

    fn start(&mut self, config: &Config, writable: bool, mode: libc::mode_t)
        -> std::io::Result<()>
    {
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        if config.mount_options.contains(&MountOption::AutoUnmount) {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let path = std::ffi::CStr::from_bytes_with_nul(&self.path)
            .map_err(|_| std::io::ErrorKind::InvalidInput)?;
        self.device = Some(std::fs::OpenOptions::new().read(true).write(true)
            .open("/dev/fuse")?.into());
        let fd = self.device.as_ref().ok_or(std::io::ErrorKind::NotConnected)?.as_raw_fd();
        // SAFETY: getuid/getgid have no preconditions.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        let options = std::ffi::CString::new(format!(
            "fd={fd},rootmode={mode:o},user_id={uid},group_id={gid},default_permissions",
        ))?;
        let flags = libc::MS_NODEV | libc::MS_NOSUID | libc::MS_NOATIME
            | if writable { 0 } else { libc::MS_RDONLY };
        // SAFETY: all strings are terminated and the owned device stays open.
        if unsafe { libc::mount(c"acyclic-fs".as_ptr(), path.as_ptr(), c"fuse".as_ptr(),
            flags, options.as_ptr().cast()) } == 0 {
            self.mounted = true;
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EPERM) { return Err(error); }
        self.device.take();
        let (child_socket, socket) = std::os::unix::net::UnixStream::pair()?;
        self.socket = Some(socket);
        let fd = child_socket.as_raw_fd();
        let mut command = Command::new("fusermount3");
        command.stdout(Stdio::null()).stderr(Stdio::null())
            .args(["-o", if writable { "fsname=acyclic-fs,default_permissions,exec,noatime" }
                else { "fsname=acyclic-fs,default_permissions,exec,noatime,ro" }, "--"])
            .arg(OsStr::from_bytes(path.to_bytes())).env("_FUSE_COMMFD", fd.to_string());
        // SAFETY: the child only clears CLOEXEC on its inherited socket before exec.
        unsafe { command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) == -1 { Err(std::io::Error::last_os_error()) }
            else { Ok(()) }
        }); }
        self.helper = Some(command.spawn()?);
        drop(child_socket);
        self.device = Some(receive_fuse_device(
            self.socket.as_ref().ok_or(std::io::ErrorKind::NotConnected)?,
        )?);
        self.mounted = true;
        self.socket.take();
        Ok(())
    }

    fn try_unmount(&mut self) -> std::io::Result<()> {
        if !self.mounted {
            return if self.helper.is_some() { Err(std::io::Error::from_raw_os_error(libc::EBUSY)) }
                else { Ok(()) };
        }
        let mut poll = libc::pollfd {
            fd: self.device.as_ref().ok_or(std::io::ErrorKind::NotConnected)?.as_raw_fd(),
            events: 0, revents: 0,
        };
        loop {
            // SAFETY: poll references one initialized descriptor owned by this mount.
            if unsafe { libc::poll(&mut poll, 1, 0) } >= 0 { break; }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted { return Err(error); }
        }
        if poll.revents & libc::POLLERR == 0 {
            let path = std::ffi::CStr::from_bytes_with_nul(&self.path)
                .map_err(|_| std::io::ErrorKind::InvalidInput)?;
            // SAFETY: the owned pathname is terminated and the device remains open.
            if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        self.mounted = false;
        Ok(())
    }

    fn drain_helper(&mut self) -> std::io::Result<()> {
        self.socket.take();
        if let Some(helper) = self.helper.as_mut() {
            match helper.wait() {
                Ok(_) => {}
                Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {}
                Err(error) => return Err(error),
            }
            self.helper.take();
        }
        Ok(())
    }
}

fn receive_fuse_device(socket: &std::os::unix::net::UnixStream) -> std::io::Result<OwnedFd> {
    let mut byte = 0u8;
    let mut iov = libc::iovec { iov_base: std::ptr::from_mut(&mut byte).cast(), iov_len: 1 };
    let mut control = [0usize; 4];
    // SAFETY: zero is valid for msghdr; its buffers remain alive through recvmsg.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = std::mem::size_of_val(&control);
    let received = loop {
        // SAFETY: message contains valid writable data and aligned control buffers.
        let received = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_CMSG_CLOEXEC) };
        if received >= 0 { break received; }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted { return Err(error); }
    };
    let mut device = None;
    // SAFETY: CMSG helpers traverse the kernel-filled control buffer; every received fd is owned.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let count = ((*header).cmsg_len - libc::CMSG_LEN(0) as usize)
                    / std::mem::size_of::<libc::c_int>();
                for index in 0..count {
                    let fd = libc::CMSG_DATA(header).cast::<libc::c_int>().add(index).read_unaligned();
                    let owned = OwnedFd::from_raw_fd(fd);
                    if device.is_none() { device = Some(owned); }
                }
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if received == 0 || message.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    device.ok_or_else(|| std::io::ErrorKind::InvalidData.into())
}

struct FuseRetirementHelper {
    program: &'static std::ffi::CStr,
    frame: Option<acyclic_native_runtime::PreparedClosedProcess>,
    helper: Option<acyclic_native_runtime::ProcessTree>,
    completed: Option<bool>,
    failure: Option<NativeMountError>,
    credit: Arc<crate::materializer::NativeOwnedCleanupCredit>,
    terminal_credit: Arc<crate::materializer::NativeOwnedCleanupCredit>,
}

impl FuseRetirementHelper {
    fn prepare(work: &crate::NativeMaterializationWork)
        -> Result<Option<Self>, NativeMountError>
    {
        let program = match fuse_recovery_helper(work) {
            Ok(program) => program,
            Err(NativeMountError::CapabilityUnavailable(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        Self::prepare_program(program, work).map(Some)
    }

    fn prepare_program(
        program: &'static std::ffi::CStr,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let normal = work.allocation_work();
        let frame = acyclic_native_runtime::ProcessTree::prepare_closed_process_measured(
            5, normal.runtime_meter_owner(),
        ).map_err(|error| normal.runtime_error(error))?;
        // Reserve the bounded observation and termination loops, including reap.
        let observed = 3_001_u64 + 3_000 + 5_001 + 5_000 + 1;
        let mut reserved = normal.reserve_work(crate::WorkCounters {
            backend_read_operations: observed + 1,
            backend_write_operations: 13,
            items_examined: 6,
            bytes_copied: (7 * std::mem::size_of::<*mut libc::c_char>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let terminal = reserved.partition_work(crate::WorkCounters {
            backend_read_operations: 5_001 + 5_000 + 1,
            backend_write_operations: 3,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let terminal_credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(terminal)?;
        let credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(reserved)?;
        Ok(Self { program, frame: Some(frame), helper: None,
            completed: None, failure: None, credit, terminal_credit })
    }

    fn retire(&mut self, path: &std::ffi::CStr, identity: Option<crate::NativeRootIdentity>)
        -> Result<(), NativeMountError>
    {
        if let Some(success) = self.completed {
            return if success { Ok(()) } else { Err(NativeMountError::FuseShutdownFailed) };
        }
        let observed = (|| {
            if self.failure.is_some() { return Ok(None); }
            if self.helper.is_none() {
                if let Some(identity) = identity {
                    let metadata = fuse_recovery_stat(path, &mut &self.credit)?;
                    if metadata.st_dev as u64 != identity.device || metadata.st_ino as u64 != identity.object {
                        return Err(NativeMountError::DestinationBusy);
                    }
                }
                acyclic_native_runtime::ProcessTree::spawn_closed_prepared(
                    self.program, &[c"-u", c"-q", c"-z", c"--", path],
                    &mut self.frame, self.credit.as_ref(), &mut self.helper,
                ).map_err(|error| self.credit.original_work().runtime_error(error))?;
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            for observation in 0..=3_000 {
                let status = self.helper.as_mut().expect("accepted helper remains owned")
                    .try_wait_measured(self.credit.as_ref())
                    .map_err(|error| self.credit.original_work().runtime_error(error))?;
                if status.is_some() || observation == 3_000 || std::time::Instant::now() >= deadline {
                    return Ok(status);
                }
                fuse_recovery_attempt(&mut &self.credit, false, || {
                    std::thread::sleep(Duration::from_millis(10)); Ok(())
                })?;
            }
            unreachable!("finite helper observation returns at its last admitted visit")
        })();
        let status = match observed {
            Ok(status) => status,
            Err(error) => { self.failure.get_or_insert(error); None }
        };
        // Terminal cleanup has separate original credits.
        self.drain_process()?;
        self.completed = Some(self.failure.is_none() && status.is_some_and(|status| status.success()));
        if let Some(error) = self.failure.take() { return Err(error); }
        match status {
            Some(status) if status.success() => Ok(()),
            Some(_) => Err(NativeMountError::FuseShutdownFailed),
            None => Err(NativeMountError::Lifecycle("fusermount retirement exceeded 30 seconds")),
        }
    }

    fn drain_process(&mut self) -> Result<(), NativeMountError> {
        if let Some(helper) = self.helper.as_mut() {
            helper.terminate_measured(self.terminal_credit.as_ref())
                .map_err(|error| self.terminal_credit.original_work().runtime_error(error))?;
            drop(self.helper.take());
        }
        drop(self.frame.take());
        Ok(())
    }
}

struct FuseSdkInput {
    state: Mutex<FuseSdkInputState>,
    ready: Condvar,
    _allocation: crate::NativeAllocation,
}

struct FuseSdkInputState {
    session: Option<fuser::Session<FuseProjection>>,
    submitted: bool,
}

impl FuseSdkInput {
    fn run(&self) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.submitted {
            state = self.ready.wait(state).unwrap_or_else(PoisonError::into_inner);
        }
        let session = state.session.take();
        drop(state);
        match session { Some(session) => session.run(), None => Ok(()) }
    }

    fn submit(&self, session: Option<fuser::Session<FuseProjection>>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.submitted { return; }
        state.session = session;
        state.submitted = true;
        self.ready.notify_all();
    }
}

struct FuseDriverControl {
    state: Mutex<FuseDriverControlState>,
    changed: Condvar,
    _allocation: crate::NativeAllocation,
}

struct FuseDriverControlState {
    physical: Option<FusePhysicalSession>,
    retirement: Option<super::NativeMountRetirement>,
    loan_custody: (bool, bool),
    startup_finished: bool,
    stop_requested: bool,
    retired: bool,
    failure: Option<NativeMountError>,
}

struct FuseDriverLoan {
    control: Arc<FuseDriverControl>,
    physical: Option<FusePhysicalSession>,
    retirement: Option<super::NativeMountRetirement>,
    startup: bool,
    committed: bool,
}

impl FuseDriverLoan {
    fn take(control: &Arc<FuseDriverControl>) -> Result<Self, NativeMountError> {
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        let physical = state.physical.take().ok_or(NativeMountError::ControlPending)?;
        let retirement = state.retirement.take();
        let startup = !state.startup_finished;
        state.loan_custody = retirement.as_ref().map_or((false, false), |packet| (
            packet.destination_guard.is_some(),
            packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
        ));
        Ok(Self { control: Arc::clone(control), physical: Some(physical), retirement,
            startup, committed: false })
    }
}

impl Drop for FuseDriverLoan {
    fn drop(&mut self) {
        if self.startup && !self.committed {
            if let Some(physical) = &self.physical {
                physical.core.stopping.store(true, Ordering::Release);
                physical.core.invalidation.notify_all();
                if let Some(input) = &physical.sdk_input { input.submit(None); }
            }
        }
        let mut state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.physical = self.physical.take();
        state.retirement = self.retirement.take();
        state.loan_custody = (false, false);
        if self.startup {
            state.startup_finished = true;
            if !self.committed { state.stop_requested = true; }
        }
        self.control.changed.notify_all();
    }
}

pub(super) struct FuseSession {
    control: Arc<FuseDriverControl>,
    core: Arc<ProjectionCore>,
    invalidator: Option<std::thread::JoinHandle<()>>,
}

struct FusePhysicalSession {
    session: Option<FuseMount>,
    sdk_input: Option<Arc<FuseSdkInput>>,
    sdk_worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    core: Arc<ProjectionCore>,
    active_root: Option<cap_std::fs::Dir>,
    native_root_identity: Option<crate::NativeRootIdentity>,
    retirement_helper: Option<FuseRetirementHelper>,
    birth_cleanup: crate::NativeWorkReservation,
}

impl FusePhysicalSession {
    fn stop(&mut self, native: Option<&super::NativeMountMeterWork>, wait: Option<&crate::CancellationToken>)
        -> Result<(), NativeMountError>
    {
        let Self { session, sdk_input, sdk_worker, core, active_root,
            retirement_helper, native_root_identity, birth_cleanup } = self;
        let loan = if native.is_none() {
            Some(match wait {
                Some(wait) => birth_cleanup.cleanup_loan(wait),
                None => birth_cleanup.accepted_cleanup_loan(),
            })
        } else { None };
        if session.is_none()
            && sdk_worker.is_none() && active_root.is_none()
            && retirement_helper.is_none() { return Ok(()); }
        let callback_native = match native {
            Some(native) => {
                native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
                native.clone()
            }
            None => {
                let loan = loan.as_ref().ok_or(NativeMountError::ControlPending)?;
                loan.check_wait()?;
                loan.original_work().native_mount_work()
            }
        };
        // Callbacks retain their original grants and cancellation.
        let _active = FuseStartupMeterScope::new(core, &callback_native)?;
        let started_io = |actual| -> Result<(), NativeMountError> {
            match native {
                Some(native) => native.native_materialization_work().charge(actual)?,
                None => loan.as_ref().ok_or(NativeMountError::ControlPending)?.started_io(actual)?,
            }
            Ok(())
        };
        let write = crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED };
        if active_root.is_some() {
            match native {
                Some(native) => {
                    let materialization = native.native_materialization_work();
                    let mut host = &materialization;
                    let cleanup = crate::native_host::NativeHostWork::reserve_cleanup_write(&mut host)?;
                    let directory = active_root.take().expect("preadmitted root capability remains owned");
                    let (_, recorded) = crate::native_host::NativeHostWork::attempt_cleanup_write(
                        &mut host, cleanup, || drop(directory),
                    );
                    recorded.map_err(NativeMountError::from)?;
                }
                None => {
                    started_io(write)?;
                    drop(active_root.take());
                }
            }
        }
        if let Some(mount) = session.as_mut() {
            started_io(write)?;
            core.stopping.store(true, Ordering::Release);
            core.invalidation.notify_all();
            // Kernel refusal retains the mount for retry.
            if let Err(error) = mount.try_unmount() {
                if error.raw_os_error() != Some(libc::EPERM) { return Err(error.into()); }
                let helper = retirement_helper.as_mut().ok_or(
                    NativeMountError::CapabilityUnavailable(
                        "no canonical fusermount helper is available for retirement".into(),
                    ),
                )?;
                helper.retire(std::ffi::CStr::from_bytes_with_nul(&mount.path)
                    .map_err(|_| NativeMountError::InvalidDestination)?, *native_root_identity)?;
                mount.mounted = false;
            }
            mount.drain_helper()?;
            drop(session.take());
        }
        if let Some(input) = sdk_input { input.submit(None); }
        if sdk_worker.is_some() {
            started_io(crate::WorkCounters {
                backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
            })?;
            let worker = sdk_worker.take().expect("accepted SDK worker remains owned");
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error.into()),
                Err(panic) => return Err(NativeMountError::DriverPanicked {
                    operation: "FUSE SDK request worker", panic: Mutex::new(panic),
                }),
            }
        }
        if let Some(helper) = retirement_helper.as_mut() {
            helper.drain_process()?;
        }
        // Workers are terminal; release unused helper backing and credits.
        drop(retirement_helper.take());
        Ok(())
    }

    fn authenticate_root_with_meter(
        &mut self, destination: &Path, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        let materialization = native.native_materialization_work();
        let mut host = &materialization;
        let (directory, identity) = crate::native_host::open_root_directory_identity_with_work(destination, &mut host)?;
        self.active_root = Some(directory);
        self.native_root_identity = Some(identity);
        self.close_root_with_meter(native)
    }

    fn close_root_with_meter(&mut self, native: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        if self.active_root.is_none() { return Ok(()); }
        let materialization = native.native_materialization_work();
        let mut host = &materialization;
        let cleanup = crate::native_host::NativeHostWork::reserve_cleanup_write(&mut host)?;
        let directory = self.active_root.take().expect("preadmitted root capability remains owned");
        let (_, recorded) = crate::native_host::NativeHostWork::attempt_cleanup_write(
            &mut host, cleanup, || drop(directory),
        );
        recorded.map_err(NativeMountError::from)
    }

    /// Parses and queues the real owned invalidation plan on the invoking meter.
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let scope = self.core.request("invalidate_path");
        let result = scope.run(|work| {
            let _active = if self.core.active_meter().is_none() {
                Some(FuseStartupMeterScope::new(&self.core, work)?)
            } else { None };
            let path = path.strip_prefix(b"/").unwrap_or(path);
            let mut components = path.split(|byte| *byte == b'/').peekable();
            let mut parent = FuseOwned::empty(MountPath::root());
            let mut name = None;
            while let Some(component) = components.next() {
                fuse_item(work)?;
                if component.is_empty() || component.contains(&0) || component == b"." || component == b".." {
                    return Err(NativeMountError::Lifecycle("FUSE invalidation requires a canonical non-root path").into());
                }
                if components.peek().is_some() { parent = fuse_child_path(&parent, component, work)?; }
                else { name = Some(component); }
            }
            let name = name.ok_or(NativeMountError::Lifecycle("FUSE invalidation requires a canonical non-root path"))?;
            let source = self.core.source.as_ref();
            let child = ChildName::new(source, fuse_child_path(&parent, name, work)?, work)?;
            let parent = match source.folded_path_with_meter(&parent, work)? {
                Some((path, bytes)) => FuseOwned::adopt(path, bytes, work)?,
                None => parent,
            };
            let mut state = self.core.state()?;
            fuse_item(work)?;
            let file_inode = state.inode_by_path.get(child.key()).copied();
            fuse_item(work)?;
            let parent_inode = state.inode_by_path.get(&*parent).copied();
            let count = usize::from(file_inode.is_some()) + 2 * usize::from(parent_inode.is_some());
            let mut items = fuse_vec(count, work)?;
            if let Some(inode) = file_inode { items.push(KernelCacheItem::Inode(inode)); }
            if let Some(parent) = parent_inode {
                items.push(KernelCacheItem::Entry { parent, name: fuse_copy_bytes(name, work)? });
                items.push(KernelCacheItem::Inode(parent));
            }
            let copied = items.len().checked_mul(std::mem::size_of::<KernelCacheItem>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            fuse_copy_admit(copied, work)?;
            fuse_reserve(&mut state.invalidation.deferred, items.len(), work)?;
            state.invalidation.deferred.append(&mut items);
            fuse_copy_record(copied, work)?;
            drop(items);
            let target = state.invalidation.notified;
            self.core.invalidation.notify_all();
            loop {
                work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)?;
                if self.core.handler_failed.load(Ordering::Acquire) || state.invalidation.failure.is_some() {
                    // The actual original packet remains owned for public query.
                    return Err(NativeMountError::Lifecycle("FUSE invalidation previously failed").into());
                }
                if self.core.stopping.load(Ordering::Acquire) {
                    return Err(NativeMountError::Lifecycle("FUSE session is stopped").into());
                }
                if !state.invalidation.is_pending(target) { break; }
                state = self.core.invalidation.wait(state).map_err(|_| libc::EIO)?;
            }
            Ok(())
        });
        result.map(|receipt| receipt.value).map_err(|failure| {
            if let Some(cause) = clone_fatal_handler_error(&failure.cause) {
                self.core.retain_handler_failure(super::NativeMountCallbackFailure { cause, work: failure.work });
            } else if fatal_handler_error(&failure.cause) {
                self.core.retain_handler_failure(failure);
                return NativeMountError::Lifecycle("FUSE invalidation handler failed");
            }
            failure.cause
        })
    }

    pub(super) fn revalidate_with_meter(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let _active = FuseStartupMeterScope::new(&self.core, native)?;
        self.core.source.fence_changes_with_meter(native).map_err(source_error)?;
        let mut state = self.core.state.lock()
            .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?;
        let target = state.invalidation.notified;
        loop {
            native.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            })?;
            if self.core.stopping.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle("session is stopped"));
            }
            if state.invalidation.failure.is_some() {
                return Err(NativeMountError::Lifecycle("FUSE invalidation previously failed"));
            }
            let condition = if state.invalidation.is_pending(target) {
                &self.core.invalidation
            } else if target.is_some_and(|target| state.page_stores.precede(target)) {
                &self.core.page_stored
            } else { return Ok(()); };
            state = condition.wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?.0;
        }
    }


}

fn retire_fuse_driver(
    control: &Arc<FuseDriverControl>, wait: Option<&crate::CancellationToken>,
) -> Result<(), NativeMountError> {
    let mut loan = FuseDriverLoan::take(control)?;
    let physical = loan.physical.as_mut().expect("physical driver loan");
    physical.stop(None, wait)?;
    if let Some(retirement) = loan.retirement.as_mut() {
        match wait {
            Some(wait) => { retirement.retire_with_cleanup_wait(wait)?; }
            None => { retirement.retire_accepted_cleanup()?; }
        }
    }
    let retired = loan.retirement.as_ref().is_none_or(super::NativeMountRetirement::is_retired);
    drop(loan);
    let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
    state.retired = retired;
    control.changed.notify_all();
    Ok(())
}

fn run_fuse_sdk(owned: (Arc<FuseSdkInput>, Arc<FuseDriverControl>, Arc<ProjectionCore>, crate::NativeAllocation))
    -> std::io::Result<()>
{
    let (input, control, core, _) = &owned;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| input.run()));
    core.stopping.store(true, Ordering::Release);
    core.invalidation.notify_all();
    let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
    state.stop_requested = true;
    control.changed.notify_all();
    drop(state);
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

impl FuseSession {
    pub(super) fn start_into<F>(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>,
        source_work: &super::NativeMountMeterWork, out: &mut Option<Self>,
        retirement: &mut Option<super::NativeMountRetirement>, prepare: F,
    ) -> Result<(), super::DriverStartFailure>
    where F: FnOnce(&mut super::NativeMountRetirement) -> Result<(), NativeMountError> {
        if out.is_some() { return Err(NativeMountError::ControlPending.into()); }
        fuse_native_source_original(source.as_ref(), Some(source_work)).map_err(NativeMountError::from)?;
        let materialization = source_work.native_materialization_work();
        materialization.admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)?;
        let stamp = source.view_stamp_with_meter(source_work).map_err(source_error)?;
        let root = source.lookup_with_meter(&MountPath::root(), source_work).map_err(source_error)?
            .ok_or(NativeMountError::Lifecycle("volume root is absent"))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(NativeMountError::Lifecycle("volume root is not a directory").into());
        }
        let metadata = crate::native_host::unix_name_with_work(
            request.destination.as_os_str(), &mut &materialization, fuse_recovery_stat,
        ).map_err(NativeMountError::from)?;
        let core = ProjectionCore::root_with_meter(source, root, stamp, request.writable,
            metadata.st_uid, metadata.st_gid, source_work)?;
        let _startup = FuseStartupMeterScope::new(&core, source_work)?;
        core.source.observe_view_with_meter(Arc::<ProjectionCore>::downgrade(&core), source_work)
            .map_err(source_error)?;
        let mut config = Config::default();
        config.n_threads = Some(8);
        let mount = FuseMount::prepare(&request.destination, source_work)?;
        let birth_cleanup = materialization.reserve_work(crate::WorkCounters {
            backend_write_operations: 3, backend_read_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)?;
        let normal = materialization.allocation_work();
        let retirement_helper = FuseRetirementHelper::prepare(&normal)
            .map_err(super::DriverStartFailure::from)?;
        let input_allocation = normal.claim(std::mem::size_of::<FuseSdkInput>() as u64)
            .map_err(NativeMountError::from)?;
        let input = Arc::new(FuseSdkInput {
            state: Mutex::new(FuseSdkInputState { session: None, submitted: false }),
            ready: Condvar::new(), _allocation: input_allocation,
        });
        let control_allocation = normal.claim(std::mem::size_of::<FuseDriverControl>() as u64)
            .map_err(NativeMountError::from)?;
        let control = Arc::new(FuseDriverControl {
            state: Mutex::new(FuseDriverControlState {
                physical: Some(FusePhysicalSession {
                    session: None, sdk_input: Some(Arc::clone(&input)),
                    sdk_worker: None, core: Arc::clone(&core),
                    active_root: None,
                    native_root_identity: None, retirement_helper, birth_cleanup,
                }),
                retirement: None, loan_custody: (false, false), startup_finished: false, stop_requested: false,
                retired: false, failure: None,
            }),
            changed: Condvar::new(), _allocation: control_allocation,
        });
        let allocation = normal.claim(std::mem::size_of::<(
            Arc<ProjectionCore>, Arc<FuseDriverControl>, crate::NativeAllocation,
        )>() as u64)?;
        let job = (Arc::clone(&core), Arc::clone(&control), allocation);
        materialization.charge(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)?;
        let invalidator = std::thread::Builder::new().spawn(move || run_fuse_retirement(job))
            .map_err(NativeMountError::from)?;
        *out = Some(Self { control: Arc::clone(&control), core: Arc::clone(&core),
            invalidator: Some(invalidator) });
        {
            let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.retirement = retirement.take();
        }
        let mut loan = FuseDriverLoan::take(&control)?;
        let allocation = normal.claim(std::mem::size_of::<(
            Arc<FuseSdkInput>, Arc<FuseDriverControl>, Arc<ProjectionCore>, crate::NativeAllocation,
        )>() as u64)?;
        let job = (Arc::clone(&input), Arc::clone(&control), Arc::clone(&core), allocation);
        materialization.charge(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)?;
        let physical = loan.physical.as_mut().expect("constructor owns original driver");
        physical.sdk_worker = Some(std::thread::Builder::new()
            .spawn(move || run_fuse_sdk(job)).map_err(NativeMountError::from)?);
        if let Some(packet) = loan.retirement.as_mut() { prepare(packet)?; }
        let physical = loan.physical.as_mut().expect("constructor owns original driver");
        materialization.charge(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)?;
        physical.session = Some(mount);
        let filesystem = FuseProjection {
            core: Arc::clone(&physical.core),
            skips_opendir: false, skips_open: false,
        };
        let mount = physical.session.as_mut().ok_or(NativeMountError::ControlPending)?;
        let started = mount.start(&config, request.writable, metadata.st_mode);
        if let Err(error) = started {
            return Err(physical.core.take_handler_error().unwrap_or_else(|| error.into()).into());
        }
        let fd = mount.device.as_ref().ok_or(NativeMountError::ControlPending)?.try_clone()
            .map_err(NativeMountError::from)?;
        let session = fuser::Session::from_fd(filesystem, fd, config.acl, config)
            .map_err(|error| physical.core.take_handler_error().unwrap_or_else(|| error.into()))?;
        let _ = physical.core.notifier.set(session.notifier());
        input.submit(Some(session));
        if let Some(helper) = mount.helper.as_mut() {
            let status = helper.wait().map_err(NativeMountError::from)?;
            mount.helper.take();
            if !status.success() { return Err(NativeMountError::FuseShutdownFailed.into()); }
        }
        physical.authenticate_root_with_meter(&request.destination, source_work)?;
        loan.committed = true;
        Ok(())
    }

    fn with_physical<T>(
        &self, operation: impl FnOnce(&mut FusePhysicalSession) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let mut loan = FuseDriverLoan::take(&self.control)?;
        operation(loan.physical.as_mut().expect("actual physical driver loan"))
    }

    pub(super) fn begin_original_work(
        &self, work: &super::NativeMountMeterWork,
    ) -> Result<FuseStartupMeterScope, NativeMountError> {
        self.with_physical(|physical| {
            if physical.session.is_none() { return Err(NativeMountError::Lifecycle("session is stopped")); }
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
            FuseStartupMeterScope::new(&physical.core, work)
        })
    }
    pub(super) fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        let state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.physical.as_ref().filter(|physical| physical.session.is_some())
            .and_then(|physical| physical.native_root_identity)
    }
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        self.with_physical(|physical| physical.invalidate(path))
    }
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        self.with_physical(|physical| {
            let cancellation = crate::CancellationToken::new();
            let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| NativeMountError::from(cause))?;
            physical.revalidate_with_meter(&work.native_mount_work())
        })
    }
    pub(super) fn revalidate_with_meter(&self, work: &super::NativeMountMeterWork)
        -> Result<(), NativeMountError> {
        self.with_physical(|physical| physical.revalidate_with_meter(work))
    }
    pub(super) fn take_callback_failure(&self) -> Option<super::NativeMountCallbackFailure> {
        self.core.take_handler_failure()
    }
    pub(super) fn handler_failed(&self) -> bool { self.core.handler_failed.load(Ordering::Acquire) }
    pub(super) fn completed_callback_work(&self) -> crate::WorkCounters { self.core.completed_callback_work() }
    #[cfg(test)]
    pub(super) fn take_requests(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.core.requests.lock().unwrap_or_else(PoisonError::into_inner))
    }
    pub(super) fn namespace_detach_confirmed(&self) -> bool {
        let state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.physical.as_ref().is_some_and(|physical| physical.session.is_none())
    }
    pub(super) fn controls_pending(&self) -> bool {
        !self.control.state.lock().unwrap_or_else(PoisonError::into_inner).retired
    }
    pub(super) fn retirement_ownership(&self) -> (bool, bool) {
        let state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.retirement.as_ref().map_or(state.loan_custody, |packet| (
            packet.destination_guard.is_some(),
            packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
        ))
    }
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> { self.finish_stop(None) }
    pub(super) fn stop_with_cleanup_wait(&mut self, wait: &crate::CancellationToken)
        -> Result<(), NativeMountError> { self.finish_stop(Some(wait)) }
    pub(super) fn stop_with_meter(&mut self, work: &super::NativeMountMeterWork)
        -> Result<(), NativeMountError> {
        self.with_physical(|physical| physical.stop(Some(work), None))?;
        self.finish_stop(None)
    }
    fn finish_stop(&mut self, wait: Option<&crate::CancellationToken>) -> Result<(), NativeMountError> {
        if let Some(wait) = wait { wait.check().map_err(|_| crate::DemandError::Cancelled)?; }
        self.core.stopping.store(true, Ordering::Release);
        self.core.invalidation.notify_all();
        let mut state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.stop_requested = true;
        self.control.changed.notify_all();
        while !state.retired {
            if let Some(error) = state.failure.take() { return Err(error); }
            drop(state);
            if let Some(wait) = wait { wait.check().map_err(|_| crate::DemandError::Cancelled)?; }
            state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
            if !state.retired && state.failure.is_none() {
                state = self.control.changed.wait_timeout(state, Duration::from_millis(10))
                    .unwrap_or_else(PoisonError::into_inner).0;
            }
        }
        let failure = state.failure.take();
        drop(state);
        if self.invalidator.is_some() {
            self.with_physical(|physical| {
                let loan = physical.birth_cleanup.accepted_cleanup_loan();
                loan.started_io(crate::WorkCounters {
                    backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
                })?;
                Ok(())
            })?;
            let worker = self.invalidator.take().expect("original invalidator join remains owned");
            worker.join().map_err(|panic| NativeMountError::DriverPanicked {
                operation: "FUSE retirement producer", panic: Mutex::new(panic),
            })?;
        }
        match failure { Some(error) => Err(error), None => Ok(()) }
    }
}

impl Drop for FuseSession {
    fn drop(&mut self) {
        self.core.stopping.store(true, Ordering::Release);
        self.core.invalidation.notify_all();
        let mut state = self.control.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.stop_requested = true;
        self.control.changed.notify_all();
    }
}

/// Crash-recovery custody paired by the root owner with its original fence.
#[derive(Default)]
pub(super) struct FuseRecoveryControl {
    helper: Option<acyclic_native_runtime::ProcessTree>,
    helper_work: Option<crate::NativeMaterializationWork>,
    deadline: Option<std::time::Instant>,
    failed_spawn: bool,
    detached: bool,
}

impl FuseRecoveryControl {
    pub(super) fn namespace_detach_confirmed(&self) -> bool { self.detached }
    pub(super) fn controls_pending(&self) -> bool { self.helper.is_some() }

    fn retire_helper(&mut self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        if let Some(helper) = &mut self.helper {
            helper.terminate_measured(work.runtime_meter()).map_err(|error| work.runtime_error(error))?;
        }
        self.helper.take();
        self.helper_work.take();
        self.deadline = None;
        self.failed_spawn = false;
        Ok(())
    }

    /// Retires only the accepted helper, never the destination namespace.
    pub(super) fn retire_controls_with_work(
        &mut self, original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.retire_helper(original)
    }

    /// Performs crash recovery on the supplied meter.
    pub(super) fn recover_measured(
        &mut self, destination: &Path, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        let materialization = native.native_materialization_work();
        let work = &materialization;
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let mounted = fuse_recovery_is_mounted(destination, work)?;
        self.detached = !mounted;
        if self.helper.is_some() && (!mounted || self.failed_spawn) { self.retire_helper(work)?; }
        if !mounted { return Ok(()); }
        if self.helper.is_none() {
            let direct = crate::native_host::unix_name_with_work(
                destination.as_os_str(), &mut &*work, |path, work| {
                    fuse_recovery_attempt(work, true, || {
                        // SAFETY: the admitted terminated pathname stays alive
                        // throughout this actual native control call.
                        if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } == 0 { Ok(()) }
                        else { Err(std::io::Error::last_os_error()) }
                    })
                },
            );
            if let Err(error) = direct {
                if !matches!(&error, crate::NativeTreeMaterializationError::Io(_)) { return Err(error.into()); }
            }
            if !fuse_recovery_is_mounted(destination, work)? {
                self.detached = true;
                return Ok(());
            }
            let program = fuse_recovery_helper(work)?;
            self.helper_work = Some(work.clone());
            let spawned = crate::native_host::unix_name_with_work(
                destination.as_os_str(), &mut &*work, |path, work| {
                    acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                        program, &[c"-u", c"-q", c"-z", c"--", path], work.runtime_meter(), &mut self.helper,
                    ).map_err(|error| work.runtime_error(error))
                },
            );
            if self.helper.is_some() {
                self.deadline = Some(std::time::Instant::now() + Duration::from_secs(30));
                self.failed_spawn = spawned.is_err();
            }
            if self.helper.is_none() { self.helper_work.take(); }
            spawned?;
        }
        loop {
            let status = {
                let helper = self.helper.as_mut().expect("accepted helper remains owned");
                helper.try_wait_measured(work.runtime_meter()).map_err(|error| work.runtime_error(error))?
            };
            if status.is_some() { break; }
            if self.deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                self.retire_helper(work)?;
                return Err(NativeMountError::Lifecycle("fusermount crash recovery exceeded 30 seconds"));
            }
            fuse_recovery_attempt(&mut &*work, false, || { std::thread::sleep(Duration::from_millis(10)); Ok(()) })?;
        }
        self.retire_helper(work)?;
        if fuse_recovery_is_mounted(destination, work)? {
            return Err(NativeMountError::Lifecycle("FUSE destination remained mounted after crash recovery"));
        }
        self.detached = true;
        Ok(())
    }
}

fn fuse_recovery_attempt<T, W: crate::native_host::NativeHostWork>(
    work: &mut W, write: bool,
    call: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, W::Error> {
    let delta = if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    };
    crate::native_host::NativeHostWork::admit(work, delta)?;
    crate::native_host::NativeHostWork::record(work, delta)?;
    call().map_err(Into::into)
}

fn fuse_recovery_stat<W: crate::native_host::NativeHostWork>(
    path: &std::ffi::CStr, work: &mut W,
) -> Result<libc::stat, W::Error> {
    loop {
        let result = crate::native_host::NativeHostWork::attempt_io(work,
            crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }, || {
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: pathname is terminated; success initializes all fields.
            if unsafe { libc::stat(path.as_ptr(), metadata.as_mut_ptr()) } == 0 {
                Ok(unsafe { metadata.assume_init() })
            } else { Err(std::io::Error::last_os_error()) }
        });
        if result.as_ref().err().is_some_and(|error| W::io_error_kind(error)
            == Some(std::io::ErrorKind::Interrupted)) { continue; }
        return result;
    }
}

fn fuse_recovery_is_mounted(
    destination: &Path, work: &crate::NativeMaterializationWork,
) -> Result<bool, NativeMountError> {
    let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
    let parent = crate::native_host::unix_name_with_work(
        parent.as_os_str(), &mut &*work, fuse_recovery_stat,
    )?;
    let destination = crate::native_host::unix_name_with_work(
        destination.as_os_str(), &mut &*work, fuse_recovery_stat,
    );
    match destination {
        Ok(metadata) => Ok(metadata.st_dev != parent.st_dev),
        Err(crate::NativeTreeMaterializationError::Io(error)) if error.raw_os_error() == Some(libc::ENOTCONN) => Ok(true),
        Err(crate::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn fuse_recovery_helper(
    work: &crate::NativeMaterializationWork,
) -> Result<&'static std::ffi::CStr, NativeMountError> {
    for candidate in [c"/usr/bin/fusermount3", c"/bin/fusermount3", c"/usr/bin/fusermount", c"/bin/fusermount"] {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        match fuse_recovery_stat(candidate, &mut &*work) {
            Ok(metadata) if metadata.st_mode & libc::S_IFMT == libc::S_IFREG => return Ok(candidate),
            Ok(_) | Err(crate::NativeTreeMaterializationError::Io(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(NativeMountError::CapabilityUnavailable(
        "no canonical fusermount helper is available for crash recovery".into(),
    ))
}

#[allow(
    clippy::struct_field_names,
    reason = "field names follow the SETATTR attribute names"
)]
struct AttributeChanges {
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<TimeOrNow>,
    mtime: Option<TimeOrNow>,
    changed: Option<SystemTime>,
    crtime: Option<SystemTime>,
    flags: Option<u32>,
}

impl AttributeChanges {
    fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.uid.is_none()
            && self.gid.is_none()
            && self.size.is_none()
            && self.atime.is_none()
            && self.mtime.is_none()
            && self.changed.is_none()
            && self.crtime.is_none()
            && self.flags.is_none()
    }

    fn applied_to(&self, mut metadata: FileMetadata) -> Result<FileMetadata, i32> {
        if let Some(mode) = self.mode {
            metadata.posix_mode = MetadataField::Value(mode);
        }
        if let Some(uid) = self.uid {
            metadata.posix_uid = MetadataField::Value(uid);
        }
        if let Some(gid) = self.gid {
            metadata.posix_gid = MetadataField::Value(gid);
        }
        if let Some(atime) = self.atime {
            metadata.accessed_ns = MetadataField::Value(time_or_now_ns(atime)?);
        }
        if let Some(mtime) = self.mtime {
            metadata.modified_ns = MetadataField::Value(time_or_now_ns(mtime)?);
        }
        if let Some(changed) = self.changed {
            metadata.changed_ns = MetadataField::Value(system_time_ns(changed)?);
        }
        if let Some(created) = self.crtime {
            metadata.created_ns = MetadataField::Value(system_time_ns(created)?);
        }
        if let Some(flags) = self.flags {
            metadata.posix_flags = MetadataField::Value(u64::from(flags));
        }
        Ok(metadata)
    }
}

impl FuseProjection {
    fn check_source_write(&self, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
        match (&self.core.callback_guard, self.core.source.native_callback_guard()) {
            (None, None) => Ok(()),
            (Some(original), Some(current)) if Arc::ptr_eq(original, &current) =>
                super::adapter::CallbackRuntime::existing()?
                    .verify_native_callback_write_guard(original.as_ref(), work)
                    .map_err(FuseRequestError::from),
            _ => Err(NativeMountError::ControlPending.into()),
        }
    }

    fn source_write<T>(
        &self,
        work: &super::NativeMountMeterWork,
        effect: impl FnOnce() -> Result<T, FuseRequestError>,
    ) -> Result<T, FuseRequestError> {
        self.check_source_write(work)?;
        let result = effect();
        if let Err(refusal) = self.check_source_write(work) {
            let cause = match result {
                Err(cause) => NativeMountError::from(cause),
                Ok(_) => NativeMountError::from(refusal),
            };
            let errno = native_callback_errno(&cause);
            self.core.retain_handler_failure(super::NativeMountCallbackFailure {
                cause, work: work.meter().lock().work,
            });
            return Err(FuseRequestError::Errno(errno));
        }
        result
    }

    fn child(&self, state: &ProjectionState, parent: u64, name: &OsStr, work: &super::NativeMountMeterWork) -> Result<ChildName, FuseRequestError> {
        Ok(ChildName::new(
            self.core.source.as_ref(),
            state.child_path(parent, name, work)?,
            work,
        )?)
    }

    fn confirm_names<'a>(
        &self,
        mut confirmed: ConfirmedNames,
        resolved: impl IntoIterator<Item = (&'a MountPath, MountNode)>,
        work: &super::NativeMountMeterWork,
    ) -> Result<ConfirmedNames, FuseRequestError> {
        let source = self.core.source.as_ref();
        for (path, node) in resolved {
            fuse_item(work)?;
            if node.link_count <= 1 { continue; }
            let mut names = self.core.state()?.unvouched_names(source, node.file_id, path, work)?;
            for name in names.drain(..) {
                match resolve_path(source, &name, work) {
                    Ok((Some(found), stamp)) if found.node.file_id == node.file_id => {
                        confirmed.confirm(node.file_id, name, stamp, work)?;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        self.core.record_handler_failure(work, NativeMountError::from(error));
                        confirmed.confirm(node.file_id, name, None, work)?;
                    }
                }
            }
        }
        Ok(confirmed)
    }

    fn lookup_entry(&self, parent: u64, name: &OsStr, work: &super::NativeMountMeterWork) -> Result<Entry, FuseRequestError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let child = {
            let mut state = self.core.state()?;
            let child = self.child(&state, parent, name, work)?;
            let ProjectionState { by_inode, inode_by_path, .. } = &mut *state;
            if let Some((inode, lookup, stamp)) = cached_projected_lookup(by_inode, inode_by_path, child.key(),
                |file, stamp| source.unchanged_since_with_meter(child.key(), Some(file), stamp, work).map_err(FuseRequestError::from), work)? {
                let attr = state.attr(inode, &lookup)?;
                let ttl = state.admit_entry(source, inode, &child, Some(stamp), work)?;
                return Ok(Entry { attr, ttl });
            }
            if let Some(empty) = state.empty_since(parent)
                && child.is_exact()
                && source.unchanged_since_with_meter(child.key(), None, empty, work)?
            {
                return Ok(state.negative_entry(source, parent, &child, Some(empty), work)?);
            }
            child
        };
        let (found, stamp) = resolve_path(source, &child.spelled, work)?;
        let confirmed = match found {
            Some(lookup) => {
                self.confirm_names(ConfirmedNames::default(), [(child.key(), lookup.node)], work)?
            }
            None => ConfirmedNames::default(),
        };
        let mut state = self.core.state()?;
        let Some(lookup) = found else {
            state.remove_path_cache(child.key(), work)?;
            return Ok(state.negative_entry(source, parent, &child, stamp, work)?);
        };
        let inode = state.intern(
            source,
            fuse_clone_path(child.key(), work)?,
            &lookup,
            stamp,
            true,
            &confirmed,
            work,
        )?;
        let attr = state
            .attr(inode, &lookup)
            .map_err(|error| state.release_lookup_reference(inode, 1, work).err().unwrap_or_else(|| error.into()))?;
        let ttl = state.admit_entry(source, inode, &child, stamp, work)?;
        Ok(Entry { attr, ttl })
    }

    fn node_attributes(
        &self,
        inode: u64,
        handle: Option<u64>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(FileAttr, Duration), FuseRequestError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let facts = {
            let mut state = self.core.state()?;
            match state.node_facts(source, inode, handle, work)? {
                Ok(current) => {
                    let attr = state.attr(inode, &current.lookup)?;
                    let ttl = state.admit_attributes(source, inode, current.stamp, work)?;
                    return Ok((attr, ttl));
                }
                facts => facts,
            }
        };
        let resolved = resolve_node(source, facts, work)?;
        Ok(self.core.state()?.apply(source, inode, &resolved, work)?)
    }

    fn set_attributes(
        &self,
        inode: u64,
        handle: Option<u64>,
        changes: &AttributeChanges,
        work: &super::NativeMountMeterWork,
    ) -> Result<(FileAttr, Duration), FuseRequestError> {
        if !changes.is_empty() { self.check_source_write(work)?; }
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let (facts, open_file) = {
            let state = self.core.state()?;
            state.admit_write().map_err(|_| libc::EOPNOTSUPP)?;
            let open_file = handle
                .map(|handle| state.open_handle(inode, handle, work))
                .transpose()?;
            (state.node_facts(source, inode, handle, work)?, open_file)
        };
        let current = resolve_node(source, facts, work)?;
        let _claim = changes
            .size
            .map(|_| self.core.claim_content(current.lookup.node.file_id, work))
            .transpose()?;
        let resolved = if changes.is_empty() {
            current
        } else {
            let metadata = changes.applied_to(current.lookup.metadata)?;
            let stamp = source.view_stamp_with_meter(work)?;
            match (&open_file, current.through) {
                (Some(open_file), _) => {
                    self.source_write(work, || open_file.file(work)?.set_attributes_with_meter(metadata, changes.size, work)
                        .map_err(FuseRequestError::from))?;
                    Resolved {
                        lookup: open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?,
                        stamp,
                        through: None,
                    }
                }
                (None, Some(path)) => {
                    self.source_write(work, || source
                        .set_attributes_with_meter(&path, metadata, changes.size, work)
                        .map_err(FuseRequestError::from))?;
                    resolve_node(
                        source,
                        Err(NodeFacts::Names {
                            file_id: Some(current.lookup.node.file_id),
                            paths: { let mut paths = fuse_vec(1, work)?; paths.push(path); paths },
                        }),
                        work,
                    )?
                }
                (None, None) => return Err(libc::ESTALE.into()),
            }
        };
        let mut state = self.core.state()?;
        if !changes.is_empty()
            && let Some(handle) = handle
        {
            state.mark_handle_dirty(inode, handle)?;
        }
        Ok(state.apply(source, inode, &resolved, work)?)
    }

    fn create_node(
        &self,
        parent: u64,
        name: &OsStr,
        create: impl FnOnce(&MountPath, &super::NativeMountMeterWork) -> Result<MountLookup, FuseRequestError>,
        work: &super::NativeMountMeterWork,
    ) -> Result<Entry, FuseRequestError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let child = {
            let state = self.core.state()?;
            state.admit_write()?;
            self.child(&state, parent, name, work)?
        };
        let stamp = source.view_stamp_with_meter(work)?;
        let lookup = self.source_write(work, || create(&child.spelled, work))?;
        let empty = if lookup.node.kind == MountNodeKind::Directory {
            let after = source.view_stamp_with_meter(work)?;
            match stamp {
                Some(before) if source.listing_unchanged_since_with_meter(child.key(), before, work)? => after,
                _ => None,
            }
        } else { None };
        // A node just created has no recorded name to confirm.
        let confirmed =
            self.confirm_names(ConfirmedNames::default(), [(child.key(), lookup.node)], work)?;
        let mut state = self.core.state()?;
        let inode = state.intern(
            source,
            fuse_clone_path(child.key(), work)?,
            &lookup,
            stamp,
            true,
            &confirmed,
            work,
        )?;
        if let Some(entry) = state.by_inode.get_mut(&inode) {
            entry.empty_since = empty;
        }
        let attr = state
            .attr(inode, &lookup)
            .map_err(|error| state.release_lookup_reference(inode, 1, work).err().unwrap_or_else(|| error.into()))?;
        let ttl = state.admit_entry(source, inode, &child, stamp, work)?;
        Ok(Entry { attr, ttl })
    }

    fn hold_unopened(&self, lookup: Option<&MountLookup>, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
        if !self.skips_open {
            return Ok(());
        }
        let inode = self.core.state()?.unopened_referenced_inode(lookup);
        // The caller holds the names lock exclusively.
        inode.map_or(Ok(()), |inode| {
            self.reopen_implicit_named(inode, None, Access::Read, work)
                .map(|_| ())
        })
    }

    fn remove_name(&self, parent: u64, name: &OsStr, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names_exclusive()?;
        let child = {
            let state = self.core.state()?;
            state.admit_write()?;
            self.child(&state, parent, name, work)?
        };
        let path = &child.spelled;
        let current = source.lookup_with_meter(path, work).map_err(FuseRequestError::from)?;
        self.hold_unopened(current.as_ref(), work)?;
        let detaching = self.core.state()?.detaching_inode(current.as_ref());
        let detached = detaching
            .map(|inode| source.detach_file_with_meter(path, work).map_err(FuseRequestError::from).and_then(|detached| Ok((inode, FuseOpenFile::Source(fuse_open_file(detached, work)?)))))
            .transpose()
            .map_err(FuseRequestError::from)?;
        let removed = self.source_write(work, || source
            .remove_with_meter(path, current.map(|lookup| lookup.node.file_id), work)
            .map_err(FuseRequestError::from));
        let mut state = self.core.state()?;
        if let Some((inode, detached)) = detached {
            state.retain_detached_handles(inode, &detached, work)?;
        }
        removed?;
        state.remove_path_cache(child.key(), work)?;
        Ok(())
    }

    fn rename_name(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), FuseRequestError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        let replace = flags & RENAME_NOREPLACE == 0;
        let _names = self.core.names_exclusive()?;
        let (from, to) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                self.child(&state, parent, name, work)?,
                self.child(&state, new_parent, new_name, work)?,
            )
        };
        let replaced = if replace {
            source.lookup_with_meter(&to.spelled, work).map_err(FuseRequestError::from)?
        } else {
            None
        };
        self.hold_unopened(replaced.as_ref(), work)?;
        let detaching = self.core.state()?.detaching_inode(replaced.as_ref());
        let detached = detaching
            .map(|inode| {
                source
                    .detach_file_with_meter(&to.spelled, work)
                    .map_err(FuseRequestError::from)
                    .and_then(|detached| Ok((inode, FuseOpenFile::Source(fuse_open_file(detached, work)?))))
            })
            .transpose()
            .map_err(FuseRequestError::from)?;
        let renamed = self.source_write(work, || source
            .rename_with_meter(&from.spelled, &to.spelled, replace, work)
            .map_err(FuseRequestError::from));
        let mut state = self.core.state()?;
        if let Some((inode, detached)) = detached {
            state.retain_detached_handles(inode, &detached, work)?;
        }
        renamed?;
        if from.key() != to.key() {
            state.invalidate_prefix(to.key(), work)?;
            state.rename_prefix(from.key(), to.key(), work)?;
        }
        Ok(())
    }

    fn link_name(&self, inode: u64, new_parent: u64, new_name: &OsStr, work: &super::NativeMountMeterWork) -> Result<Entry, FuseRequestError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let (from, to, facts) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                fuse_clone_path(state.path(inode)?, work)?,
                self.child(&state, new_parent, new_name, work)?,
                state.node_facts(source, inode, None, work)?,
            )
        };
        let current = resolve_node(source, facts, work)?;
        let mut projected = current.lookup;
        projected.node.link_count = projected
            .node
            .link_count
            .checked_add(1)
            .ok_or(libc::EMLINK)?;
        self.source_write(work, || source.hard_link_with_meter(&from, &to.spelled, work).map_err(FuseRequestError::from))?;
        let mut state = self.core.state()?;
        let attr = state.attr(inode, &projected)?;
        let binding = {
            let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
            if entry.lookup.node.file_id != projected.node.file_id {
                return Err(libc::ESTALE.into());
            }
            if entry.binding(to.key(), work)?.is_none() {
                fuse_reserve(&mut entry.bindings, 1, work)?;
                Some(Binding::new(fuse_clone_path(to.key(), work)?, None))
            } else { None }
        };
        let ProjectionState {
            by_inode,
            inode_by_path,
            inode_by_file,
            ..
        } = &mut *state;
        bind_path(by_inode, inode_by_path, inode_by_file, to.key(), inode, work)?;
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if let Some(binding) = binding { entry.bindings.push(binding); }
        // Derived locally from the facts before the link, not read back.
        entry.lookup = projected;
        entry.facts = None;
        entry.lookup_references = entry.lookup_references.saturating_add(1);
        let ttl = state.admit_entry(source, inode, &to, current.stamp, work)?;
        Ok(Entry { attr, ttl })
    }

    fn read_link(&self, inode: u64, work: &super::NativeMountMeterWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let path = fuse_clone_path(self.core.state()?.path(inode)?, work)?;
        let stamp = source.view_stamp_with_meter(work)?;
        let target = source.read_link_with_meter(&path, work).map_err(FuseRequestError::from)?;
        let target = fuse_owned_source_chunk(target, work)?;
        // The kernel keeps the target it receives.
        self.core.state()?.admit_attributes(source, inode, stamp, work)?;
        Ok(target)
    }

    fn open_node(&self, inode: u64, flags: i32, work: &super::NativeMountMeterWork) -> Result<(u64, FopenFlags), FuseRequestError> {
        self.open_node_storing(inode, flags, true, work)
    }

    fn open_node_storing(
        &self,
        inode: u64,
        flags: i32,
        storing: bool,
        work: &super::NativeMountMeterWork,
    ) -> Result<(u64, FopenFlags), FuseRequestError> {
        let _names = self.core.names()?;
        self.open_node_named(inode, flags, storing, work)
    }

    fn open_node_named(
        &self,
        inode: u64,
        flags: i32,
        storing: bool,
        work: &super::NativeMountMeterWork,
    ) -> Result<(u64, FopenFlags), FuseRequestError> {
        if flags & libc::O_TRUNC != 0 { self.check_source_write(work)?; }
        let source = self.core.source.as_ref();
        let (path, file_id, stamp_before, binding_before, held) = {
            let state = self.core.state()?;
            admit_open(state.defaults.writable, flags)?;
            let node = state.node(inode)?;
            if node.kind != MountNodeKind::Regular {
                return Err(libc::EISDIR.into());
            }
            let held = (!changes_content(flags) && flags & libc::O_DIRECT == 0)
                .then(|| state.by_inode.get(&inode))
                .flatten()
                .filter(|entry| entry.cached_content == Some(ContentVersion::of(&entry.lookup)))
                .and_then(|entry| Some((entry.lookup, entry.facts?)));
            (
                fuse_clone_path(state.path(inode)?, work)?,
                node.file_id,
                source.view_stamp_with_meter(work)?,
                source.binding_epoch_with_meter(work)?,
                held,
            )
        };
        let _claim = changes_content(flags)
            .then(|| self.core.claim_content(file_id, work))
            .transpose()?;
        let held = match held {
            Some((lookup, facts)) if lookup.node.file_id == file_id => {
                source.unchanged_since_with_meter(&path, Some(file_id), facts, work)?
                    .then_some((lookup, facts))
            }
            _ => None,
        };
        let (open_file, refreshed, stamp, truncated) = match held {
            Some((lookup, facts)) => {
                let allocation = fuse_initialized((), std::mem::size_of::<DeferredOpenFile>() as u64, work)?;
                let deferred = FuseOpenFile::Deferred(Arc::new(DeferredOpenFile {
                    source: fuse_arc_clone(&self.core.source, work)?,
                    path: fuse_clone_path(&path, work)?,
                    opened: lookup,
                    file: Mutex::new(None),
                    _allocation: allocation,
                }));
                (deferred, lookup, Some(facts), false)
            }
            None => self.open_source(source, &path, file_id, flags, stamp_before, work)?,
        };
        let _binding = source
            .acquire_binding_lease_with_meter(binding_before, work)
            .map_err(FuseRequestError::from)?;
        let mut state = self.core.state()?;
        if state.path(inode).ok() != Some(&*path) {
            return Err(libc::ESTALE.into());
        }
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != file_id {
            return Err(libc::ESTALE.into());
        }
        entry.lookup = refreshed;
        entry.facts = stamp;
        if let Some(binding) = entry.binding_mut(&path, work)? {
            binding.verified = stamp;
        }
        let (handle, open_flags, store) = state.admit_file_handle(
            source,
            inode,
            OpenedFile {
                path: &path,
                lookup: &refreshed,
                stamp,
                flags,
                open_file,
                dirty: truncated,
                stores_pages: storing,
            },
            work,
        )?;
        drop(state);
        Ok((handle, self.store_pages(store, open_flags, work)?))
    }

    #[allow(
        clippy::type_complexity,
        reason = "returns the opened file with its facts, stamp, and truncation flag in one tuple"
    )]
    fn open_source(
        &self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: crate::FileId,
        flags: i32,
        stamp_before: Option<ViewStamp>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(FuseOpenFile, MountLookup, Option<ViewStamp>, bool), FuseRequestError> {
        if flags & libc::O_TRUNC != 0 { self.check_source_write(work)?; }
        let (open_file, opened) = source.open_file_with_lookup_with_meter(path, work).map_err(FuseRequestError::from)?;
        let open_file = FuseOpenFile::Source(fuse_open_file(open_file, work)?);
        if opened.node.file_id != file_id {
            return Err(libc::ESTALE.into());
        }
        let truncated = flags & libc::O_TRUNC != 0;
        if truncated {
            self.source_write(work, || open_file.file(work)?.resize_with_meter(0, work).map_err(FuseRequestError::from))?;
        }
        let unchanged = if !truncated {
            match stamp_before {
                Some(stamp) => source.unchanged_since_with_meter(path, Some(file_id), stamp, work)?,
                None => false,
            }
        } else { false };
        let (refreshed, stamp) = if unchanged {
            (opened, stamp_before)
        } else {
            let stamp = source.view_stamp_with_meter(work)?;
            let lookup = open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?;
            if lookup.node.file_id != file_id { return Err(libc::ESTALE.into()); }
            (lookup, stamp)
        };
        Ok((open_file, refreshed, stamp, truncated))
    }

    fn store_pages(
        &self,
        store: Option<PageStore>,
        mut open_flags: FopenFlags,
        work: &super::NativeMountMeterWork,
    ) -> Result<FopenFlags, FuseRequestError> {
        let Some(store) = store else { return Ok(open_flags); };
        let stored = (|| -> Result<bool, FuseRequestError> {
            let Some(notifier) = self.core.notifier.get() else { return Ok(false); };
            let bytes = store.open_file.file(work)?.read_up_to_with_meter(0, store.length, work)
                .map_err(FuseRequestError::from)?;
            let bytes = fuse_owned_source_chunk(bytes, work)?;
            let delta = crate::WorkCounters {
                backend_write_operations: 1,
                output_bytes: bytes.bytes.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            let materialization = work.native_materialization_work();
            materialization.admit(delta).map_err(NativeMountError::from)?;
            materialization.charge(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(NativeMountError::from)?;
            let result = notifier.store(INodeNo(store.inode), 0, &bytes.bytes);
            materialization.record_admitted(crate::WorkCounters {
                output_bytes: if result.is_ok() { delta.output_bytes } else { 0 },
                ..crate::WorkCounters::UNCHARGED
            }).map_err(NativeMountError::from)?;
            result.map_err(NativeMountError::Io)?;
            Ok(true)
        })();
        let stored = match stored {
            Ok(stored) => stored,
            Err(error) => {
                self.core.record_handler_failure(work, NativeMountError::from(error));
                false
            }
        };
        let mut state = self.core.state_to_finish();
        state.page_stores.land(store.file_id);
        self.core.page_stored.notify_all();
        let admitted = match state.admissible(
            self.core.source.as_ref(), &store.path, Some(store.file_id), Some(store.stamp),
            work,
        ) {
            Ok(admitted) => admitted,
            Err(error) => {
                self.core.record_handler_failure(work, NativeMountError::from(error));
                false
            }
        };
        if !admitted {
            match fuse_reserve(&mut state.invalidation.deferred, 1, work) {
                Ok(()) => state.invalidation.deferred.push(KernelCacheItem::Inode(store.inode)),
                Err(error) => self.core.record_handler_failure(work, NativeMountError::from(error)),
            }
        }
        drop(state);
        open_flags.set(FopenFlags::FOPEN_KEEP_CACHE, stored && admitted);
        Ok(open_flags)
    }

    fn create_file(
        &self,
        request: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: i32,
        work: &super::NativeMountMeterWork,
    ) -> Result<(Entry, u64, FopenFlags), FuseRequestError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let (child, absent_since) = {
            let state = self.core.state()?;
            state.admit_write()?;
            let child = self.child(&state, parent, name, work)?;
            let absent_since = state.absent_since(parent, &child);
            (child, absent_since)
        };
        let path = &child.spelled;
        let stamp = source.view_stamp_with_meter(work)?;
        let claim = |file_id| {
            changes_content(flags)
                .then(|| self.core.claim_content(file_id, work))
                .transpose()
        };
        let (existing, proof) = match absent_since {
            Some(held) if source.unchanged_since_with_meter(child.key(), None, held, work)? => (None, Some(held)),
            _ => (source.lookup_with_meter(path, work).map_err(FuseRequestError::from)?, stamp),
        };
        let (lookup, open_file, dirty, _claim, confirmed) = if let Some(existing) = existing {
            {
                if flags & libc::O_EXCL != 0 {
                    return Err(libc::EEXIST.into());
                }
                if existing.node.kind != MountNodeKind::Regular {
                    return Err(libc::EISDIR.into());
                }
                // Confirmed before the open changes anything.
                let confirmed =
                    self.confirm_names(ConfirmedNames::default(), [(child.key(), existing.node)], work)?;
                let claimed = claim(existing.node.file_id)?;
                let open_file = FuseOpenFile::Source(fuse_open_file(source.open_file_with_meter(path, work)?, work)?);
                let opened = open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?;
                if opened.node.file_id != existing.node.file_id {
                    return Err(libc::ESTALE.into());
                }
                if flags & libc::O_TRUNC == 0 {
                    (opened, open_file, false, claimed, confirmed)
                } else {
                    self.source_write(work, || open_file.file(work)?.resize_with_meter(0, work).map_err(FuseRequestError::from))?;
                    let resized = open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?;
                    (resized, open_file, true, claimed, confirmed)
                }
            }
        } else {
            let metadata = create_metadata(mode, S_IFREG, request.uid(), request.gid());
            let created = self.source_write(work, || match proof {
                Some(proof) => source.create_absent_file_with_meter(path, metadata, proof, work),
                None => source.create_file_with_meter(path, metadata, work),
            }.map_err(FuseRequestError::from))?;
            let claimed = claim(created.node.file_id)?;
            (
                created,
                FuseOpenFile::Source(fuse_open_file(source.open_created_with_meter(path, &created, work)?, work)?),
                true,
                claimed,
                // A file just created has no other name.
                ConfirmedNames::default(),
            )
        };
        let mut state = self.core.state()?;
        let inode = state.intern(
            source,
            fuse_clone_path(child.key(), work)?,
            &lookup,
            stamp,
            true,
            &confirmed,
            work,
        )?;
        let created = state.attr(inode, &lookup).map_err(FuseRequestError::from).and_then(|attr| {
            let opened = OpenedFile {
                path: child.key(),
                lookup: &lookup,
                stamp,
                flags,
                open_file,
                dirty,
                stores_pages: true,
            };
            let (handle, open_flags, store) = state.admit_file_handle(source, inode, opened, work)?;
            Ok((attr, handle, open_flags, store))
        });
        let (attr, handle, open_flags, store) =
            created.map_err(|error| state.release_lookup_reference(inode, 1, work).err().unwrap_or_else(|| error.into()))?;
        let ttl = state.admit_entry(source, inode, &child, stamp, work)?;
        drop(state);
        let open_flags = self.store_pages(store, open_flags, work)?;
        Ok((Entry { attr, ttl }, handle, open_flags))
    }

    fn with_handle<T>(
        &self,
        inode: u64,
        handle: u64,
        access: Access,
        operation: impl FnOnce(u64) -> Result<T, FuseRequestError>,
        work: &super::NativeMountMeterWork,
    ) -> Result<T, FuseRequestError> {
        if access == Access::Write { self.check_source_write(work)?; }
        if handle != 0 || !self.skips_open {
            return operation(handle);
        }
        let held = self.core.state()?.implicit.get(&inode).map(|held| **held);
        match (held, access) {
            (Some(implicit), Access::Read) => operation(implicit.handle),
            (Some(implicit), Access::Write) if implicit.writes => operation(implicit.handle),
            (_, Access::Write) => operation(self.reopen_implicit(inode, held, access, work)?),
            (None, Access::Read) => {
                let (transient, _) = self.open_node_storing(inode, libc::O_RDONLY, false, work)?;
                let result = operation(transient);
                let _ = self.core.state()?.discard_file_handle(inode, transient);
                result
            }
        }
    }

    fn reopen_implicit(
        &self,
        inode: u64,
        stale: Option<Implicit>,
        access: Access,
        work: &super::NativeMountMeterWork,
    ) -> Result<u64, FuseRequestError> {
        let _names = self.core.names()?;
        self.reopen_implicit_named(inode, stale, access, work)
    }

    fn reopen_implicit_named(
        &self,
        inode: u64,
        stale: Option<Implicit>,
        access: Access,
        work: &super::NativeMountMeterWork,
    ) -> Result<u64, FuseRequestError> {
        let writes = access == Access::Write || stale.is_some_and(|stale| stale.writes);
        let flags = if writes { libc::O_RDWR } else { libc::O_RDONLY };
        let (opened, _) = self.open_node_named(inode, flags, false, work)?;
        let mut state = self.core.state()?;
        let current = state.implicit.get(&inode).map(|implicit| implicit.handle);
        if current.is_some() && current != stale.map(|stale| stale.handle) {
            // Another callback reopened it meanwhile.
            let _ = state.discard_file_handle(inode, opened);
            return current.ok_or_else(|| libc::ESTALE.into());
        }
        if let Some(stale) = state.implicit.insert(
            inode,
            fuse_initialized(Implicit {
                handle: opened,
                writes,
            }, std::mem::size_of::<(u64, FuseOwned<Implicit>)>() as u64, work)?,
        ) {
            let _ = state.discard_file_handle(inode, stale.handle);
        }
        Ok(opened)
    }

    fn read_through(&self, inode: u64, handle: u64, offset: u64, size: u32, work: &super::NativeMountMeterWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let read = self.with_handle(inode, handle, Access::Read, |opened| {
            self.read_handle(inode, opened, offset, size, work)
        }, work);
        match read {
            Err(FuseRequestError::Errno(libc::ESTALE) | FuseRequestError::Native(NativeMountError::Source(MountSourceError::Stale))) if handle == 0 && self.skips_open => {
                let Some(stale) = self.core.state()?.implicit.get(&inode).map(|held| **held) else {
                    return Err(libc::ESTALE.into());
                };
                let reopened = self.reopen_implicit(inode, Some(stale), Access::Read, work)?;
                self.read_handle(inode, reopened, offset, size, work)
            }
            read => read,
        }
    }

    fn read_handle(&self, inode: u64, handle: u64, offset: u64, size: u32, work: &super::NativeMountMeterWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let open_file = self.core.state()?.open_handle(inode, handle, work)?;
        let bytes = open_file.file(work)?.read_up_to_with_meter(offset, size, work).map_err(FuseRequestError::from)?;
        let bytes = fuse_owned_source_chunk(bytes, work)?;
        if self.core.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV.into());
        }
        Ok(bytes)
    }

    fn write_handle(&self, inode: u64, handle: u64, offset: u64, data: &[u8], work: &super::NativeMountMeterWork) -> Result<u32, FuseRequestError> {
        self.check_source_write(work)?;
        let operation = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.open_handle_operation(inode, handle, work)?
        };
        let _operation = operation.lock().map_err(|_| libc::EIO)?;
        let open_file = {
            let mut state = self.core.state()?;
            let file = state
                .files
                .get_mut(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
                .ok_or(libc::ESTALE)?;
            file.dirty = true;
            file.open_file.clone(work)?
        };

        #[cfg(test)]
        pause_test_write();

        let length = u32::try_from(data.len()).map_err(|_| libc::EOVERFLOW)?;
        let bytes = fuse_source_chunk(data, work)?;
        self.source_write(work, || {
            let file = match open_file.file(work) {
                Ok(file) => file,
                Err(error) => { DeferredOpenFile::discard_packet(bytes, work)?; return Err(error.into()); }
            };
            file.write_range_with_meter(offset, bytes, work).map_err(FuseRequestError::from)
        })?;
        let stamp = self.core.source.as_ref().view_stamp_with_meter(work)?;
        let refreshed = Some(open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?);
        let mut state = self.core.state()?;
        let current = state.files.get(&handle).is_some_and(|file| {
            file.inode == inode
                && Arc::ptr_eq(&file.operation, &operation)
                && file.open_file.same_file(&open_file)
        });
        if current && let Some(lookup) = refreshed {
            state.apply_handle_facts(inode, lookup, stamp);
        }
        Ok(length)
    }

    fn flush_handle(&self, inode: u64, handle: u64, force: bool, release: bool, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
        let authored_flush = {
            let state = self.core.state()?;
            let file = state.files.get(&handle).filter(|file| file.inode == inode).ok_or(libc::ESTALE)?;
            force || (file.dirty && self.core.source.as_ref().flush_on_handle_close())
        };
        if authored_flush { self.check_source_write(work)?; }
        let operation = self.core.state()?.open_handle_operation(inode, handle, work)?;
        let _operation = operation.lock().map_err(|_| libc::EIO)?;
        let (should_flush, open_file) = {
            let state = self.core.state()?;
            let file = state
                .files
                .get(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
                .ok_or(libc::ESTALE)?;
            (
                force || (file.dirty && self.core.source.as_ref().flush_on_handle_close()),
                file.open_file.clone(work)?,
            )
        };
        let settled = if force {
            self.source_write(work, || open_file.settle(work).map_err(FuseRequestError::from))
        } else {
            Ok(())
        };
        let flushed = settled.and_then(|()| {
            if should_flush {
                self.source_write(work, || self.core.source.as_ref().flush_with_meter(work).map_err(FuseRequestError::from))
            } else {
                Ok(())
            }
        });
        let mut state = self.core.state()?;
        let current = state
            .files
            .get(&handle)
            .is_some_and(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation));
        if !current {
            return Err(libc::ESTALE.into());
        }
        if flushed.is_ok()
            && let Some(file) = state.files.get_mut(&handle)
        {
            file.dirty = false;
        }
        if release {
            let discarded = state.discard_file_handle(inode, handle);
            flushed.and(discarded.map_err(FuseRequestError::from))
        } else {
            flushed
        }
    }

    fn list_directory<L: DirectoryListing>(
        &self,
        inode: u64,
        offset: u64,
        listing: &mut Option<L>,
        span: &tracing::Span,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), FuseRequestError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let mut stream = self.core.state()?.directory_stream(source, inode, offset, work)?;
        self.position_directory_stream(&mut stream, offset, work)?;
        let expected = stream.binding_epoch;
        let _lease = source.acquire_binding_lease_with_meter(expected, work)
            .map_err(FuseRequestError::from)?;
        if !source.view_is_stable_with_meter(work)? || source.binding_epoch_with_meter(work)? != expected {
            return Err(libc::ESTALE.into());
        }
        let mut state = self.core.state()?;
        state.streams.prepare_park(&mut stream, work)?;
        state.list_directory(source, inode, &mut stream, listing, span, work)?;
        state.streams.park(inode, stream);
        Ok(())
    }

    fn position_directory_stream(
        &self,
        stream: &mut DirectoryStream,
        offset: u64,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), FuseRequestError> {
        let source = self.core.source.as_ref();
        stream.emitted = stream.emitted.max(offset.min(2));
        loop {
            fuse_item(work)?;
            if stream.emitted < offset && stream.entries_remaining() != 0 {
                stream.entry_position += 1;
                stream.emitted += 1;
                continue;
            }
            if stream.entries_remaining() != 0 || stream.exhausted { return Ok(()); }
            let lease = source.acquire_binding_lease_with_meter(stream.binding_epoch, work)
                .map_err(FuseRequestError::from)?;
            let stamp = source.view_stamp_with_meter(work)?;
            let page = source.read_directory_with_meter(&stream.path, stream.cursor(), DIRECTORY_PAGE_SIZE, work)
                .map_err(FuseRequestError::from)?;
            if !source.view_is_stable_with_meter(work)? || source.binding_epoch_with_meter(work)? != stream.binding_epoch {
                return Err(libc::ESTALE.into());
            }
            stream.exhausted = page.next_cursor().is_none();
            drop(lease);
            stream.confirmed = self.confirm_listed_names(&stream.path, page.entries(), stamp, work)?;
            stream.page = Some(page);
            stream.entry_position = 0;
            stream.entries_stamp = stamp;
        }
    }

    fn confirm_listed_names(
        &self,
        directory: &MountPath,
        entries: &[MountDirectoryEntry],
        stamp: Option<ViewStamp>,
        work: &super::NativeMountMeterWork,
    ) -> Result<ConfirmedNames, FuseRequestError> {
        let source = self.core.source.as_ref();
        let mut listed = fuse_vec(entries.len(), work)?;
        for entry in entries {
            fuse_item(work)?;
            if entry.node.link_count <= 1 { continue; }
            let name = ChildName::new(source, fuse_child_path(directory, &entry.name, work)?, work)?;
            listed.push((fuse_clone_path(name.key(), work)?, entry.node));
        }
        let mut confirmed = self.confirm_names(
            ConfirmedNames::default(),
            listed.iter().map(|(path, node)| (&**path, *node)),
            work,
        )?;
        for (path, node) in listed.drain(..) {
            confirmed.confirm(node.file_id, path, stamp, work)?;
        }
        Ok(confirmed)
    }

    fn attributes_to_write(&self, inode: u64, work: &super::NativeMountMeterWork) -> Result<AttributeTarget, FuseRequestError> {
        self.check_source_write(work)?;
        let state = self.core.state()?;
        state.admit_write()?;
        Ok(state.attribute_target(inode, work)?)
    }

    fn attributes_to_read(&self, inode: u64, work: &super::NativeMountMeterWork) -> Result<Option<AttributeTarget>, FuseRequestError> {
        Ok(self.core.state()?.attributes_to_read(self.core.source.as_ref(), inode, work)?)
    }

    fn write_named_attribute(
        &self,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), FuseRequestError> {
        if position != 0 || flags & !(libc::XATTR_CREATE | libc::XATTR_REPLACE) != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        let mode = match (
            flags & libc::XATTR_CREATE != 0,
            flags & libc::XATTR_REPLACE != 0,
        ) {
            (true, true) => return Err(libc::EINVAL.into()),
            (true, false) => super::MountAttributeWriteMode::Create,
            (false, true) => super::MountAttributeWriteMode::Replace,
            (false, false) => super::MountAttributeWriteMode::Upsert,
        };
        let _names = self.core.names()?;
        let target = self.attributes_to_write(inode, work)?;
        let value = fuse_source_chunk(value, work)?;
        self.source_write(work, || match target {
            AttributeTarget::Handle(open_file) => {
                let file = match open_file.file(work) {
                    Ok(file) => file,
                    Err(error) => { DeferredOpenFile::discard_packet(value, work)?; return Err(error.into()); }
                };
                file.write_attribute_with_meter(name.as_bytes(), value, mode, work)
            }
            AttributeTarget::Path(path) => {
                self.core.source.as_ref()
                    .write_attribute_with_meter(&path, name.as_bytes(), value, mode, work)
            }
        }.map_err(FuseRequestError::from))
    }

    fn read_named_attribute(&self, inode: u64, name: &OsStr, work: &super::NativeMountMeterWork) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
        let _names = self.core.names()?;
        let value = match self.attributes_to_read(inode, work)? {
            Some(AttributeTarget::Handle(open_file)) => open_file.file(work)?.read_attribute_with_meter(name.as_bytes(), work),
            Some(AttributeTarget::Path(path)) => {
                self.core.source.as_ref().read_attribute_with_meter(&path, name.as_bytes(), work)
            }
            None => Ok(None),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => FuseRequestError::Errno(libc::ENODATA),
            error => error.into(),
        })?
        .ok_or_else(|| FuseRequestError::Errno(libc::ENODATA))?;
        fuse_owned_source_chunk(value, work)
    }

    fn list_named_attributes(
        &self,
        inode: u64,
        work: &super::NativeMountMeterWork,
    ) -> Result<FuseOwned<Vec<u8>>, FuseRequestError> {
        let _names = self.core.names()?;
        let Some(target) = self.attributes_to_read(inode, work)? else { return Ok(fuse_vec(0, work)?); };
        let mut previous: Option<FuseOwned<super::MountAttributePage>> = None;
        let mut encoded = fuse_vec(0, work)?;
        loop {
            fuse_item(work)?;
            let cursor = previous.as_ref().and_then(|page| page.next_cursor.as_deref());
            let (page, allocation) = match &target {
                AttributeTarget::Handle(open_file) => open_file.file(work)?.list_attributes_with_meter(cursor, ATTRIBUTE_PAGE_SIZE, work),
                AttributeTarget::Path(path) => self.core.source.as_ref().list_attributes_with_meter(path, cursor, ATTRIBUTE_PAGE_SIZE, work),
            }.map_err(FuseRequestError::from)?;
            let page = FuseOwned::native(page, allocation);
            for name in &page.names {
                fuse_item(work)?;
                let required = name.len().checked_add(1).ok_or(libc::EOVERFLOW)?;
                if encoded.len().checked_add(required).ok_or(libc::EOVERFLOW)? > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(libc::E2BIG.into());
                }
                fuse_copy_admit(required as u64, work)?;
                fuse_reserve(&mut encoded, required, work)?;
                encoded.extend_from_slice(name);
                encoded.push(0);
                fuse_copy_record(required as u64, work)?;
            }
            let Some(next) = page.next_cursor.as_deref() else { return Ok(encoded); };
            if cursor == Some(next) { return Err(libc::EIO.into()); }
            previous = Some(page);
        }
    }

    fn remove_named_attribute(&self, inode: u64, name: &OsStr, work: &super::NativeMountMeterWork) -> Result<(), FuseRequestError> {
        let _names = self.core.names()?;
        let target = self.attributes_to_write(inode, work)?;
        self.source_write(work, || match target {
            AttributeTarget::Handle(open_file) => open_file.file(work)?.remove_attribute_with_meter(name.as_bytes(), work),
            AttributeTarget::Path(path) => self.core.source.as_ref().remove_attribute_with_meter(&path, name.as_bytes(), work),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => FuseRequestError::Errno(libc::ENODATA),
            error => error.into(),
        }))
    }

    fn allocate(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        length: u64,
        mode: i32,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), FuseRequestError> {
        self.check_source_write(work)?;
        const KEEP_SIZE: i32 = 0x01;
        const PUNCH_HOLE: i32 = 0x02;
        const ZERO_RANGE: i32 = 0x10;
        let open_file = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.open_handle(inode, handle, work)?
        };
        if length == 0 {
            return Err(libc::EINVAL.into());
        }
        let operation = if mode == (PUNCH_HOLE | KEEP_SIZE) {
            crate::MountRangeAllocation::PunchHole
        } else if mode == ZERO_RANGE || mode == (ZERO_RANGE | KEEP_SIZE) {
            crate::MountRangeAllocation::ZeroRange {
                extend: mode == ZERO_RANGE,
            }
        } else if mode == 0 || mode == KEEP_SIZE {
            crate::MountRangeAllocation::Preallocate {
                keep_size: mode == KEEP_SIZE,
            }
        } else {
            return Err(libc::EOPNOTSUPP.into());
        };
        self.source_write(work, || open_file.file(work)?.allocate_range_with_meter(offset, length, operation, work)
            .map_err(FuseRequestError::from))?;
        self.core.state()?.mark_handle_dirty(inode, handle)?;
        let stamp = self.core.source.as_ref().view_stamp_with_meter(work)?;
        let refreshed = Some(open_file.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?);
        let mut state = self.core.state()?;
        if let Some(lookup) = refreshed {
            state.apply_handle_facts(inode, lookup, stamp);
        }
        Ok(())
    }

    fn seek(&self, inode: u64, handle: u64, offset: i64, whence: i32, work: &super::NativeMountMeterWork) -> Result<i64, FuseRequestError> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL.into()),
        };
        let open_file = self.core.state()?.open_handle(inode, handle, work)?;
        let found = open_file.file(work)?.seek_with_meter(offset, target, work)
            .map_err(FuseRequestError::from)?
            .ok_or(libc::ENXIO)?;
        i64::try_from(found).map_err(|_| libc::EOVERFLOW.into())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the copy_file_range request: two inodes, two handles, two offsets, a length, and flags"
    )]
    fn copy_range(
        &self,
        source_inode: u64,
        source_handle: u64,
        source_offset: u64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: u64,
        length: u64,
        flags: u64,
        work: &super::NativeMountMeterWork,
    ) -> Result<u32, FuseRequestError> {
        self.check_source_write(work)?;
        let (source_open, destination_open) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                state.open_handle(source_inode, source_handle, work)?,
                state.open_handle(destination_inode, destination_handle, work)?,
            )
        };
        u32::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        if flags != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        if length == 0 {
            return Ok(0);
        }
        let (transferred, failure) = copy_open_range(
            self,
            &source_open,
            source_offset,
            &destination_open,
            destination_offset,
            length,
            work,
        )?;
        if let Some(error) = failure {
            self.core.record_handler_failure(work, NativeMountError::from(error));
        }
        self.core.state()?.mark_handle_dirty(destination_inode, destination_handle)?;
        let stamp = self.core.source.as_ref().view_stamp_with_meter(work)?;
        let refreshed = Some(destination_open.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?);
        let mut state = self.core.state()?;
        if let Some(lookup) = refreshed {
            state.apply_handle_facts(destination_inode, lookup, stamp);
        }
        Ok(transferred)
    }
}

fn copy_open_range(
    projection: &FuseProjection,
    source_open: &FuseOpenFile,
    source_offset: u64,
    destination_open: &FuseOpenFile,
    destination_offset: u64,
    length: u64,
    work: &super::NativeMountMeterWork,
) -> Result<(u32, Option<FuseRequestError>), FuseRequestError> {
    projection.check_source_write(work)?;
    let source = projection.core.source.as_ref();
    let source_lookup = source_open.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?;
    let destination_lookup = destination_open.file(work)?.lookup_with_meter(work).map_err(FuseRequestError::from)?;
    let source_size = source_lookup.node.logical_bytes;
    let copy_length = length.min(source_size.saturating_sub(source_offset));
    if copy_length == 0 {
        return Ok((0, None));
    }
    if source_lookup.node.link_count != 0 && destination_lookup.node.link_count != 0 {
        match projection.source_write(work, || source.clone_range_by_id_with_meter(
            source_lookup.node.file_id,
            source_offset,
            destination_lookup.node.file_id,
            destination_offset,
            copy_length,
            work,
        ).map_err(FuseRequestError::from)) {
            Ok(()) => return Ok((u32::try_from(copy_length).map_err(|_| libc::EOVERFLOW)?, None)),
            Err(FuseRequestError::Native(NativeMountError::Source(
                MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported
                | MountSourceError::NativeText { kind: super::NativeSourceTextKind::Unsupported, .. }
                | MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Unsupported, .. }))) => {}
            Err(error) => return Err(error),
        }
    }
    let same_open_file = source_open.same_file(destination_open);
    let backwards = same_open_file
        && destination_offset > source_offset
        && destination_offset < source_offset.saturating_add(copy_length);
    let mut transferred = 0_u64;
    let mut failure = None;
    while transferred < copy_length {
        if let Err(error) = projection.check_source_write(work) {
            if transferred == 0 { return Err(error); }
            failure = Some(error);
            break;
        }
        if let Err(error) = fuse_item(work) {
            if transferred == 0 { return Err(error); }
            failure = Some(error);
            break;
        }
        let remaining = copy_length - transferred;
        let chunk = remaining.min(u64::from(DETACHED_COPY_CHUNK_BYTES));
        let relative = if backwards {
            remaining - chunk
        } else {
            transferred
        };
        let chunk = u32::try_from(chunk).unwrap_or(DETACHED_COPY_CHUNK_BYTES);
        let sparse = match source_open.file(work).and_then(|file| file.read_sparse_range_with_meter(source_offset + relative, chunk, work)) {
            Ok(sparse) => sparse,
            Err(error) if transferred == 0 => return Err(error.into()),
            Err(error) => { failure = Some(error.into()); break; }
        };
        let copied = sparse.0.logical_bytes;
        if let Err(error) = projection.source_write(work, || destination_open.file(work)?.write_sparse_range_with_meter(destination_offset + relative, sparse, work)
            .map_err(FuseRequestError::from))
        {
            if transferred == 0 {
                return Err(error);
            }
            failure = Some(error);
            break;
        }
        transferred = transferred.saturating_add(copied);
        if copied < u64::from(chunk) {
            break;
        }
    }
    Ok((u32::try_from(transferred).map_err(|_| libc::EOVERFLOW)?, failure))
}

fn errno_on(span: &tracing::Span, error: i32) -> Errno {
    span.record("errno", error);
    Errno::from_i32(error)
}

macro_rules! respond {
    ($reply:ident, $request:ident, $work:ident, $result:expr, |$value:pat_param| $ok:expr) => {{
        let mut pending_reply = Some($reply);
        let result = $request.0.run(|$work| {
            let $value = $result?;
            let $reply = pending_reply.take().expect("one successful FUSE reply");
            let _ = $ok;
            Ok(())
        });
        if let Err(error) = $request.0.finish(result) {
            if let Some(reply) = pending_reply.take() {
                reply.error($crate::native_mount::fuse::errno_on(&$request.1, error));
            }
        }
    }};
}

impl Filesystem for FuseProjection {
    fn init(&mut self, request: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = request;
        let scope = self.core.request("init");
        let result = scope.run(|work| {
            // Only the original stamped source can keep skipped listings coherent.
            let skips_opendir = config.capabilities().contains(InitFlags::FUSE_NO_OPENDIR_SUPPORT)
                && ProjectionState::directory_open_flags_with_meter(self.core.source.as_ref(), work)?
                    .contains(FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE);
            let skips_open = config.capabilities().contains(InitFlags::FUSE_NO_OPEN_SUPPORT)
                && self.core.source.as_ref().view_stamp_with_meter(work)?.is_some()
                && !self.core.source.as_ref().flush_on_handle_close();
            let supported = REQUESTED_CAPABILITIES & config.capabilities();
            config.add_capabilities(supported)
                .map_err(NativeMountError::FuseUnsupportedCapabilities)?;
            Ok((skips_opendir, skips_open))
        });
        let (skips_opendir, skips_open) = scope.finish(result)
            .map_err(std::io::Error::from_raw_os_error)?;
        self.skips_opendir = skips_opendir;
        self.skips_open = skips_open;
        Ok(())
    }

    fn lookup(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _ = request;
        let _request = (
            self.core.request("lookup"),
            callback_span!(TRACE, lookup, ino = parent.0),
        );
        respond!(
            reply,
            _request, work,
            self.lookup_entry(parent.0, name, work),
            |entry| entry.reply(reply)
        );
    }

    fn getattr(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: Option<FuseFileHandle>,
        reply: ReplyAttr,
    ) {
        let _ = request;
        let _request = (
            self.core.request("getattr"),
            callback_span!(TRACE, getattr, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            self.node_attributes(
                inode.0,
                fh.map(|handle| handle.0).filter(|handle| *handle != 0),
                work,
            ),
            |(attr, ttl)| reply.attr(&ttl, &attr)
        );
    }

    fn forget(&self, request: &Request, inode: INodeNo, nlookup: u64) {
        let _ = request;
        let _request = (
            self.core.request("forget"),
            callback_span!(TRACE, forget, ino = inode.0),
        );
        let result = _request.0.run(|work| {
            self.core.state_to_finish().release_lookup_reference(inode.0, nlookup, work)
        });
        let _ = _request.0.finish(result);
    }

    fn readlink(&self, request: &Request, inode: INodeNo, reply: ReplyData) {
        let _ = request;
        let _request = (
            self.core.request("readlink"),
            callback_span!(TRACE, readlink, ino = inode.0),
        );
        respond!(reply, _request, work, self.read_link(inode.0, work), |target| reply
            .data(&target.bytes));
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn setattr(
        &self,
        request: &Request,
        inode: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<FuseFileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let _ = request;
        let _request = (
            self.core.request("setattr"),
            callback_span!(DEBUG, setattr, ino = inode.0),
        );
        let changes = AttributeChanges {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            changed: chgtime.or(ctime),
            crtime,
            flags: flags.map(|flags| flags.bits()),
        };
        respond!(
            reply,
            _request, work,
            {
                if bkuptime.is_some() { return Err(libc::EOPNOTSUPP.into()); }
                self.set_attributes(
                    inode.0,
                    fh.map(|handle| handle.0).filter(|handle| *handle != 0),
                    &changes,
                    work,
                )
            },
            |(attr, ttl)| reply.attr(&ttl, &attr)
        );
    }

    fn mkdir(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let _request = (
            self.core.request("mkdir"),
            callback_span!(TRACE, mkdir, ino = parent.0),
        );
        let metadata = create_metadata(mode & !umask, S_IFDIR, request.uid(), request.gid());
        respond!(
            reply,
            _request, work,
            self.create_node(parent.0, name, |path, work| self
                .core.source.as_ref()
                .create_directory_with_meter(path, metadata, work).map_err(FuseRequestError::from), work),
            |entry| entry.reply(reply)
        );
    }

    fn mknod(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let _request = (
            self.core.request("mknod"),
            callback_span!(TRACE, mknod, ino = parent.0),
        );
        let metadata = create_metadata(mode & !umask, mode & S_IFMT, request.uid(), request.gid());
        respond!(
            reply,
            _request, work,
            {
                let (kind, device) = match mode & S_IFMT {
                    S_IFIFO => (MountNodeKind::Fifo, None),
                    S_IFSOCK => (MountNodeKind::Socket, None),
                    S_IFCHR => (
                        MountNodeKind::CharacterDevice,
                        Some(native_device_parts(rdev)),
                    ),
                    S_IFBLK => (MountNodeKind::BlockDevice, Some(native_device_parts(rdev))),
                    _ => return Err(libc::EOPNOTSUPP.into()),
                };
                self.create_node(parent.0, name, |path, work| self
                    .core.source.as_ref()
                    .create_special_with_meter(path, kind, device, metadata, work).map_err(FuseRequestError::from), work)
            },
            |entry| entry.reply(reply)
        );
    }

    fn symlink(
        &self,
        request: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let _request = (
            self.core.request("symlink"),
            callback_span!(TRACE, symlink, ino = parent.0),
        );
        let metadata = create_metadata(0o777, S_IFLNK, request.uid(), request.gid());
        respond!(
            reply,
            _request, work,
            self.create_node(parent.0, link_name, |path, work| {
                let target = fuse_source_chunk(target.as_os_str().as_bytes(), work)?;
                self.core.source.as_ref().create_symbolic_link_with_meter(path, target, metadata, work)
                    .map_err(FuseRequestError::from)
            }, work),
            |entry| entry.reply(reply)
        );
    }

    fn unlink(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _ = request;
        let _request = (
            self.core.request("unlink"),
            callback_span!(TRACE, unlink, ino = parent.0),
        );
        respond!(reply, _request, work, self.remove_name(parent.0, name, work), |()| {
            reply.ok();
        });
    }

    fn rmdir(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _ = request;
        let _request = (
            self.core.request("rmdir"),
            callback_span!(TRACE, rmdir, ino = parent.0),
        );
        respond!(reply, _request, work, self.remove_name(parent.0, name, work), |()| {
            reply.ok();
        });
    }

    fn rename(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        new_parent: INodeNo,
        new_name: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let _ = request;
        let _request = (
            self.core.request("rename"),
            callback_span!(DEBUG, rename, ino = parent.0),
        );
        respond!(
            reply,
            _request, work,
            self.rename_name(parent.0, name, new_parent.0, new_name, flags.bits(), work),
            |()| reply.ok()
        );
    }

    fn link(
        &self,
        request: &Request,
        inode: INodeNo,
        new_parent: INodeNo,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        let _ = request;
        let _request = (
            self.core.request("link"),
            callback_span!(TRACE, link, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            self.link_name(inode.0, new_parent.0, new_name, work),
            |entry| entry.reply(reply)
        );
    }

    fn open(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _ = request;
        let _request = (
            self.core.request("open"),
            callback_span!(TRACE, open, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            {
                // `ENOSYS` tells the kernel to open every file without asking.
                if self.skips_open { return Err(libc::ENOSYS.into()); }
                self.open_node(inode.0, flags.0, work)
            },
            |(handle, flags)| {
                reply.opened(FuseFileHandle(handle), flags);
            }
        );
    }

    fn read(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let _ = (request, flags, lock_owner);
        let _request = (
            self.core.request("read"),
            callback_span!(
                TRACE,
                read,
                ino = inode.0,
                fh = fh.0,
                offset = offset,
                len = size
            ),
        );
        respond!(
            reply,
            _request, work,
            self.read_through(inode.0, fh.0, offset, size, work),
            |bytes| {
                reply.data(&bytes.bytes);
            }
        );
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn write(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let _ = (request, write_flags, flags, lock_owner);
        let _request = (
            self.core.request("write"),
            callback_span!(
                TRACE,
                write,
                ino = inode.0,
                fh = fh.0,
                offset = offset,
                len = data.len()
            ),
        );
        respond!(
            reply,
            _request, work,
            self.with_handle(inode.0, fh.0, Access::Write, |handle| {
                self.write_handle(inode.0, handle, offset, data, work)
            }, work),
            |written| {
                reply.written(written);
            }
        );
    }

    fn fsync(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _ = (request, datasync);
        let _request = (
            self.core.request("fsync"),
            callback_span!(DEBUG, fsync, ino = inode.0, fh = fh.0),
        );
        respond!(reply, _request, work, self.with_handle(inode.0, fh.0, Access::Read, |handle| {
            self.flush_handle(inode.0, handle, true, false, work)
        }, work), |()| {
            reply.ok();
        });
    }

    fn flush(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let _ = (request, lock_owner);
        let _request = (
            self.core.request("flush"),
            callback_span!(DEBUG, flush, ino = inode.0, fh = fh.0),
        );
        respond!(
            reply,
            _request, work,
            {
                // No open handle exists for this actual negotiated capability.
                if self.skips_open && fh.0 == 0 { return Err(libc::ENOSYS.into()); }
                self.flush_handle(inode.0, fh.0, false, false, work)
            },
            |()| {
                reply.ok();
            }
        );
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn release(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        let _ = (request, flags, lock_owner, flush);
        let _request = (
            self.core.request("release"),
            callback_span!(DEBUG, release, ino = inode.0, fh = fh.0),
        );
        respond!(
            reply,
            _request, work,
            self.flush_handle(inode.0, fh.0, false, true, work),
            |()| {
                reply.ok();
            }
        );
    }

    fn opendir(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _ = (request, inode, flags);
        let _request = (
            self.core.request("opendir"),
            callback_span!(TRACE, opendir, ino = inode.0),
        );
        respond!(reply, _request, work, {
            // `ENOSYS` asks the kernel to keep subsequent listings without opens.
            if self.skips_opendir { return Err(libc::ENOSYS.into()); }
            ProjectionState::directory_open_flags_with_meter(self.core.source.as_ref(), work)
        }, |flags| reply.opened(FuseFileHandle(0), flags));
    }

    fn readdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let _ = (request, fh);
        let _request = (
            self.core.request("readdir"),
            callback_span!(TRACE, readdir, ino = inode.0, fh = fh.0, offset = offset),
        );
        let mut reply = Some(reply);
        let result = _request.0.run(|work| {
            self.list_directory(inode.0, offset, &mut reply, &_request.1, work)
        });
        if let Err(error) = _request.0.finish(result) {
            // The real late-error prefix may already have sent a partial listing.
            if let Some(reply) = reply { reply.error(errno_on(&_request.1, error)); }
        }
    }

    fn readdirplus(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
        let _ = (request, fh);
        let _request = (
            self.core.request("readdirplus"),
            callback_span!(
                TRACE,
                readdirplus,
                ino = inode.0,
                fh = fh.0,
                offset = offset
            ),
        );
        let mut reply = Some(reply);
        let result = _request.0.run(|work| {
            self.list_directory(inode.0, offset, &mut reply, &_request.1, work)
        });
        if let Err(error) = _request.0.finish(result) {
            // The real late-error prefix may already have sent a partial listing.
            if let Some(reply) = reply { reply.error(errno_on(&_request.1, error)); }
        }
    }

    fn releasedir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let _ = (request, inode, fh, flags);
        let _request = (
            self.core.request("releasedir"),
            callback_span!(TRACE, releasedir, ino = inode.0, fh = fh.0),
        );
        respond!(reply, _request, work, { let _ = work; Ok::<_, FuseRequestError>(()) }, |()| reply.ok());
    }

    fn fsyncdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _ = (request, inode, fh, datasync);
        let _request = (
            self.core.request("fsyncdir"),
            callback_span!(TRACE, fsyncdir, ino = inode.0, fh = fh.0),
        );
        respond!(
            reply,
            _request, work,
            self.source_write(work, || self.core.source.as_ref().flush_with_meter(work).map_err(FuseRequestError::from)),
            |()| reply.ok()
        );
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn setxattr(
        &self,
        request: &Request,
        inode: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let _ = request;
        let _request = (
            self.core.request("setxattr"),
            callback_span!(TRACE, setxattr, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            self.write_named_attribute(inode.0, name, value, flags, position, work),
            |()| reply.ok()
        );
    }

    fn getxattr(
        &self,
        request: &Request,
        inode: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let _ = request;
        let _request = (
            self.core.request("getxattr"),
            callback_span!(TRACE, getxattr, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            self.read_named_attribute(inode.0, name, work),
            |value| {
                reply_xattr(reply, &value.bytes, size, &_request.1);
            }
        );
    }

    fn listxattr(&self, request: &Request, inode: INodeNo, size: u32, reply: ReplyXattr) {
        let _ = request;
        let _request = (
            self.core.request("listxattr"),
            callback_span!(TRACE, listxattr, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            self.list_named_attributes(inode.0, work),
            |encoded| {
                reply_xattr(reply, &encoded, size, &_request.1);
            }
        );
    }

    fn removexattr(&self, request: &Request, inode: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _ = request;
        let _request = (
            self.core.request("removexattr"),
            callback_span!(TRACE, removexattr, ino = inode.0),
        );
        respond!(
            reply,
            _request, work,
            self.remove_named_attribute(inode.0, name, work),
            |()| {
                reply.ok();
            }
        );
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn fallocate(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let _ = request;
        let _request = (
            self.core.request("fallocate"),
            callback_span!(
                TRACE,
                fallocate,
                ino = inode.0,
                fh = fh.0,
                offset = offset,
                len = length
            ),
        );
        respond!(
            reply,
            _request, work,
            self.with_handle(inode.0, fh.0, Access::Write, |handle| {
                self.allocate(inode.0, handle, offset, length, mode, work)
            }, work),
            |()| reply.ok()
        );
    }

    fn lseek(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        let _ = request;
        let _request = (
            self.core.request("lseek"),
            callback_span!(TRACE, lseek, ino = inode.0, fh = fh.0, offset = offset),
        );
        respond!(reply, _request, work, self.with_handle(inode.0, fh.0, Access::Read, |handle| {
            self.seek(inode.0, handle, offset, whence, work)
        }, work), |found| {
            reply.offset(found);
        });
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn copy_file_range(
        &self,
        request: &Request,
        inode_in: INodeNo,
        fh_in: FuseFileHandle,
        offset_in: u64,
        inode_out: INodeNo,
        fh_out: FuseFileHandle,
        offset_out: u64,
        length: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        let _ = request;
        let _request = (
            self.core.request("copy_file_range"),
            callback_span!(TRACE, copy_file_range, len = length),
        );
        respond!(reply, _request, work, self.with_handle(inode_in.0, fh_in.0, Access::Read, |handle_in| {
            self.with_handle(inode_out.0, fh_out.0, Access::Write, |handle_out| {
                self.copy_range(
                    inode_in.0,
                    handle_in,
                    offset_in,
                    inode_out.0,
                    handle_out,
                    offset_out,
                    length,
                    flags.bits(),
                   work,
                )
            }, work)
        }, work), |written| reply.written(written));
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fuser Filesystem trait fixes this signature"
    )]
    fn create(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let _request = (
            self.core.request("create"),
            callback_span!(DEBUG, create, ino = parent.0),
        );
        respond!(
            reply,
            _request, work,
            self.create_file(request, parent.0, name, mode & !umask, flags, work),
            |(entry, handle, open_flags)| reply.created(
                &entry.ttl,
                &entry.attr,
                Generation(0),
                FuseFileHandle(handle),
                open_flags,
            )
        );
    }
}

fn reply_xattr(reply: ReplyXattr, value: &[u8], size: u32, span: &tracing::Span) {
    if size == 0 {
        reply.size(u32::try_from(value.len()).unwrap_or(u32::MAX));
    } else if value.len() <= size as usize {
        reply.data(value);
    } else {
        reply.error(errno_on(span, libc::ERANGE));
    }
}

fn file_type(kind: MountNodeKind) -> Result<FileType, i32> {
    Ok(match kind {
        MountNodeKind::Regular => FileType::RegularFile,
        MountNodeKind::Directory => FileType::Directory,
        MountNodeKind::SymbolicLink => FileType::Symlink,
        MountNodeKind::Fifo => FileType::NamedPipe,
        MountNodeKind::Socket => FileType::Socket,
        MountNodeKind::CharacterDevice => FileType::CharDevice,
        MountNodeKind::BlockDevice => FileType::BlockDevice,
        MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
    })
}

fn metadata_time(field: MetadataField<i64>) -> SystemTime {
    match field {
        MetadataField::Unavailable => SystemTime::UNIX_EPOCH,
        MetadataField::Value(value) if value >= 0 => {
            SystemTime::UNIX_EPOCH + Duration::from_nanos(value.unsigned_abs())
        }
        MetadataField::Value(value) => {
            SystemTime::UNIX_EPOCH - Duration::from_nanos(value.unsigned_abs())
        }
    }
}

fn time_or_now_ns(value: TimeOrNow) -> Result<i64, i32> {
    system_time_ns(match value {
        TimeOrNow::SpecificTime(value) => value,
        TimeOrNow::Now => SystemTime::now(),
    })
}

fn native_device_parts(device: u32) -> (u32, u32) {
    super::device::split_device(u64::from(device))
}

#[cfg(target_os = "linux")]
fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    super::device::join_device(major, minor)
        .and_then(|device| u32::try_from(device).map_err(|_| libc::EOVERFLOW))
}

#[cfg(target_os = "macos")]
fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    super::device::join_device(major, minor).map(|device| u32::from_ne_bytes(device.to_ne_bytes()))
}

fn admit_open(writable: bool, flags: i32) -> Result<(), i32> {
    if flags & libc::O_TRUNC != 0 && !writable {
        Err(libc::EROFS)
    } else {
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {

    #[test]
    fn filtered_callback_error_replies_without_overwriting_parent_errno()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::Errno;
        #[derive(Clone, Copy)]
        struct Reply<'a>(&'a std::cell::Cell<bool>);
        impl Reply<'_> {
            fn error(self, error: Errno) {
                assert_eq!(error, Errno::from_i32(libc::EIO));
                self.0.set(true);
            }
        }
        let (source, _) = shared_sources()?;
        let mut policy = finite_handler_policy();
        policy.bytes_copied = 0;
        let core = super::ProjectionCore::new(Arc::new(source), empty_projection_state(),
            policy, &crate::CancellationToken::new())?;
        let replied = std::cell::Cell::new(false);
        let reply = Reply(&replied);
        crate::obs::assert_filtered_callback_isolation(|span| {
            let request = (core.request("filtered_callback"), span);
            respond!(reply, request, work,
                super::fuse_copy_bytes(b"refused before copy", work).map(|_| ()), |()| drop(reply));
            libc::EIO
        });
        assert!(replied.get());
        Ok(())
    }
    use super::super::{
        MountAttributePage, MountAttributeWriteMode, MountDirectoryPage, MountRangeAllocation,
        MountSourceError, MountViewLease, ViewObserver,
    };
    use super::{
        CachedContent, ConfirmedNames, InodeEntry, KernelCacheItem,
        MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY, MountFilesystem, MountLookup, MountNode,
        MountNodeKind, MountOpenFile, MountPath, MountSeekTarget, PageStores,
        ROOT_INODE, ViewStamp, admit_open, cached_projected_lookup, changes_content,
        forget_unbound_names, intern_projected, replace_root_lookup, retire_reused_identity,
        stale_kernel_items,
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


    #[test]
    fn administrative_helper_retains_actual_tree_and_original_payload_on_denial()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let creator = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut control = super::FuseRecoveryControl::default();
        acyclic_native_runtime::ProcessTree::spawn_closed_measured(
            c"/bin/sleep", &[c"60"], creator.runtime_meter(), &mut control.helper,
        ).map_err(|error| creator.runtime_error(error))?;
        control.helper_work = Some(creator.clone());
        control.deadline = Some(std::time::Instant::now() + Duration::from_secs(30));
        let creator_receipt = creator.receipt();
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(control.retire_controls_with_work(&denied),
            Err(crate::NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert!(control.controls_pending());
        assert!(!control.helper.as_ref().ok_or("actual tree lost")?.is_reaped());
        assert!(control.helper_work.as_ref().ok_or("payload owner lost")?.shares_ledger(&creator));
        assert!(!control.namespace_detach_confirmed());
        assert_eq!(denied.receipt().backend_write_operations, 0);

        let cancelled_token = crate::CancellationToken::new();
        let cancelled = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancelled_token,
        ).map_err(|(cause, _)| cause)?;
        let cancelled_receipt = cancelled.receipt();
        cancelled_token.cancel();
        assert!(control.retire_controls_with_work(&cancelled).is_err());
        assert_eq!(cancelled.receipt(), cancelled_receipt);
        assert!(control.controls_pending());

        let original_admin = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 4096, backend_write_operations: 64,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        control.retire_controls_with_work(&original_admin)?;
        assert!(!control.controls_pending());
        assert!(control.helper_work.is_none());
        assert!(control.deadline.is_none());
        assert!(!control.namespace_detach_confirmed(), "process reap is not namespace evidence");
        assert_eq!(creator.receipt(), creator_receipt, "retirement uses the supplied original administrative work");
        assert!(original_admin.receipt().backend_write_operations > 0);
        assert!(original_admin.receipt().backend_read_operations > 0);
        Ok(())
    }

    #[test]
    fn administrative_recovery_retry_uses_its_own_finite_action_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let creator = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut control = super::FuseRecoveryControl::default();
        acyclic_native_runtime::ProcessTree::spawn_closed_measured(
            c"/bin/sleep", &[c"60"], creator.runtime_meter(), &mut control.helper,
        ).map_err(|error| creator.runtime_error(error))?;
        control.helper_work = Some(creator.clone());
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(control.recover_measured(temporary.path(), &denied.native_mount_work()),
            Err(crate::NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert!(control.controls_pending());
        assert!(!control.namespace_detach_confirmed());
        assert!(control.helper_work.as_ref().ok_or("payload owner lost")?.shares_ledger(&creator));
        let retry = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 4096, backend_write_operations: 64,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        let creator_receipt = creator.receipt();
        control.recover_measured(temporary.path(), &retry.native_mount_work())?;
        assert!(!control.controls_pending());
        assert!(control.namespace_detach_confirmed(), "only the real directory probe establishes absence");
        assert!(control.helper_work.is_none());
        assert_eq!(creator.receipt(), creator_receipt);
        assert!(retry.receipt().backend_read_operations >= 2);
        assert!(retry.receipt().backend_write_operations > 0);
        Ok(())
    }
    fn regular(file_id: FileId, logical_bytes: u64) -> MountLookup {
        MountLookup {
            node: MountNode {
                file_id,
                kind: MountNodeKind::Regular,
                logical_bytes,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        }
    }

    fn directory(file_id: FileId) -> MountLookup {
        let mut lookup = regular(file_id, 0);
        lookup.node.kind = MountNodeKind::Directory;
        lookup
    }

    fn name(value: &str) -> MountPath {
        MountPath::root().child(value.as_bytes().to_vec())
    }

    fn empty_projection_state() -> super::ProjectionState {
        super::ProjectionState {
            defaults: super::AttributeDefaults {
                writable: true,
                mount_uid: 0,
                mount_gid: 0,
            },
            next_inode: ROOT_INODE + 1,
            next_handle: 1,
            by_inode: HashMap::new(),
            inode_by_path: HashMap::new(),
            inode_by_file: HashMap::new(),
            files: HashMap::new(),
            implicit: HashMap::new(),
            streams: super::DirectoryStreams::default(),
            invalidation: super::InvalidationQueue::default(),
            page_stores: PageStores::default(),
        }
    }

    #[test]
    fn kernel_forget_retires_only_an_inode_with_no_open_handle() -> Result<(), i32> {
        let mut state = empty_projection_state();
        let mut linked = regular(FileId::new(), 1);
        linked.node.link_count = 2;
        let intern = |state: &mut super::ProjectionState, path| {
            intern_projected(&mut state.next_inode, &mut state.by_inode, &mut state.inode_by_path, &mut state.inode_by_file, super::FuseOwned::empty(path), &linked, None, true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)
        };
        let forgotten = intern(&mut state, name("first"))?;
        state.release_lookup_reference(forgotten, 1, &super::fuse_fixture_work()).unwrap();
        assert!(!state.by_inode.contains_key(&forgotten));
        let current = intern(&mut state, name("alias"))?;
        assert_ne!(
            current, forgotten,
            "separate unheld lookups can get new inodes"
        );

        state
            .by_inode
            .get_mut(&current)
            .ok_or(libc::ESTALE)?
            .open_handles = 1;
        state.release_lookup_reference(current, 1, &super::fuse_fixture_work()).unwrap();
        assert_eq!(
            intern(&mut state, name("first"))?,
            current,
            "an open inode remains the same for its other hardlink name"
        );
        Ok(())
    }

    fn positions<const N: usize>() -> [ViewStamp; N] {
        let slot = AtomicU64::new(0);
        std::array::from_fn(|_| ViewStamp::record(&slot))
    }

    #[test]
    fn retained_pages_require_the_version_they_were_admitted_under() {
        let file_id = FileId::new();
        let mut entry = InodeEntry::new(super::FuseOwned::empty(name("file")), regular(file_id, 4), None, 1, &super::fuse_fixture_work()).unwrap();
        let admitted = regular(file_id, 4);
        let resized = regular(file_id, 5);
        let mut touched = admitted;
        touched.metadata.modified_ns = MetadataField::Value(7);

        assert_eq!(
            entry.admit_cached_content(&admitted),
            CachedContent::Absent,
            "nothing is cached yet"
        );
        assert_eq!(
            entry.admit_cached_content(&admitted),
            CachedContent::Current
        );
        assert_eq!(
            entry.admit_cached_content(&resized),
            CachedContent::Superseded
        );
        assert_eq!(
            entry.admit_cached_content(&touched),
            CachedContent::Superseded
        );
        assert_eq!(entry.admit_cached_content(&touched), CachedContent::Current);
    }

    fn finite_cache_budget() -> crate::WorkBudget {
        crate::WorkBudget {
            authority_records_read: 0, authority_records_appended: 0,
            authority_bytes_read: 0, authority_bytes_written: 0,
            object_probes: 0, backend_read_operations: 0, backend_write_operations: 0,
            durability_operations: 0, page_reads: 0, page_writes: 0,
            object_bytes_read: 0, object_bytes_written: 0, bytes_hashed: 0,
            bytes_copied: 1_000_000, bytes_encoded: 0, source_bytes_read: 0,
            source_path_components: 10_000, source_entries_visited: 0, output_bytes: 0,
            items_examined: 10_000, items_returned: 0, allocation_operations: 10_000,
            peak_allocation_bytes: 1_000_000, materializations: 0,
        }
    }

    fn cache_work(budget: crate::WorkBudget, cancellation: &crate::CancellationToken)
        -> super::super::NativeMountMeterWork
    {
        super::super::NativeMountMeterWork::from_meter(Arc::new(crate::demand::NativeDemandReadMeter(
            Mutex::new(crate::demand::DemandWork::new(budget, cancellation)),
        )))
    }

    #[test]
    fn original_invalidation_tail_survives_refusal_without_copying_or_overtaking() {
        let [held, through] = positions();
        let work = cache_work(finite_cache_budget(), &crate::CancellationToken::new());
        let mut deferred = super::fuse_vec(2, &work).expect("original queued capacity");
        deferred.extend([KernelCacheItem::Inode(10), KernelCacheItem::Inode(11)]);
        let mut queue = super::InvalidationQueue {
            notified: Some(through), scanned: Some(through), processed: Some(held),
            deferred, ..Default::default()
        };
        let copied = work.meter().lock().work.bytes_copied;
        let mut in_flight = queue.take_deferred();
        assert_eq!(in_flight.next(), Some(KernelCacheItem::Inode(10)));
        let mut denied_budget = finite_cache_budget();
        denied_budget.bytes_copied = 0;
        let denied = cache_work(denied_budget, &crate::CancellationToken::new());
        let failure = super::fuse_clone_path(&name("refused"), &denied).expect_err("real copy refusal");
        queue.retain_pending_tail(in_flight);
        queue.finish(Some(through), Err(super::super::NativeMountCallbackFailure {
            cause: crate::NativeMountError::from(failure), work: denied.meter().lock().work,
        }));
        assert_eq!(queue.processed, Some(held));
        assert!(queue.is_pending(Some(through)));
        queue.deferred = super::fuse_vec(1, &work).expect("later callback queue");
        queue.deferred.push(KernelCacheItem::Inode(12));
        let mut tail = queue.take_deferred();
        assert_eq!(tail.next(), Some(KernelCacheItem::Inode(11)));
        assert_eq!(tail.next(), None);
        assert_eq!(queue.deferred.as_slice(), &[KernelCacheItem::Inode(12)]);
        assert_eq!(work.meter().lock().work.bytes_copied, copied);
    }

    #[test]
    fn negative_entries_are_cacheable_only_while_tracked() {
        let [older, newer] = positions();
        let mut entry = InodeEntry::new(super::FuseOwned::empty(MountPath::root()), directory(FileId::new()), None, 1, &super::fuse_fixture_work()).unwrap();
        let work = super::fuse_fixture_work();
        for index in 0..MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY {
            assert!(entry.remember_negative_child(format!("absent-{index}").as_bytes(), newer, &work).expect("tracked name"));
        }
        assert!(entry.remember_negative_child(b"absent-0", older, &work).expect("tracked name"));
        assert!(!entry.remember_negative_child(b"untracked", newer, &work).expect("bounded name table"));
        // A tracked entry keeps the oldest position the kernel may hold.
        assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
        assert!(entry.remember_negative_child(b"absent-0", newer, &work).expect("tracked name"));
        assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
    }

    #[test]
    fn each_name_keeps_its_own_verification() -> Result<(), i32> {
        let [first, second, third] = positions();
        let mut linked = regular(FileId::new(), 3);
        linked.node.link_count = 2;
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::new();
        let mut by_path = HashMap::new();
        let mut by_file = HashMap::new();
        let mut intern = |by_inode: &mut HashMap<u64, InodeEntry>, path: &str, stamp| {
            intern_projected(&mut next_inode, by_inode, &mut by_path, &mut by_file, super::FuseOwned::empty(name(path)), &linked, stamp, true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)
        };
        let inode = intern(&mut by_inode, "alias", Some(first))?;
        assert_eq!(intern(&mut by_inode, "original", Some(second))?, inode);
        // Refreshing one name re-verifies only that name.
        assert_eq!(intern(&mut by_inode, "original", Some(third))?, inode);
        let entry = &by_inode[&inode];
        assert_eq!(
            entry.binding(&name("alias"), &super::fuse_fixture_work()).unwrap().and_then(|b| b.verified),
            Some(first)
        );
        assert_eq!(
            entry.binding(&name("original"), &super::fuse_fixture_work()).unwrap().and_then(|b| b.verified),
            Some(third)
        );
        assert_eq!(entry.facts, Some(third));

        // Reuse through a name is bounded by that name's own verification.
        let mut sampled = Vec::new();
        for path in ["alias", "original"] {
            cached_projected_lookup(&mut by_inode, &by_path, &name(path), |_, stamp| Ok({
                sampled.push(stamp);
                false
            }), &super::fuse_fixture_work()).unwrap();
        }
        assert_eq!(sampled, [first, third]);
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if let Some(binding) = entry.binding_mut(&name("alias"), &super::fuse_fixture_work()).unwrap() {
            binding.verified = None;
        }
        assert!(
            cached_projected_lookup(&mut by_inode, &by_path, &name("alias"), |_, _| Ok(true), &super::fuse_fixture_work()).unwrap().is_none(),
            "an unverified name is never reused"
        );
        Ok(())
    }

    #[test]
    fn a_name_joins_its_node_whatever_link_count_its_facts_carry() -> Result<(), i32> {
        let [before, after] = positions();
        let stale = regular(FileId::new(), 3);
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(super::FuseOwned::empty(MountPath::root()), directory(FileId::new()), Some(before), 1, &super::fuse_fixture_work()).unwrap(),
        )]);
        let mut by_path = HashMap::from([(super::FuseOwned::empty(MountPath::root()), ROOT_INODE)]);
        let mut by_file = HashMap::new();
        let first = intern_projected(&mut next_inode, &mut by_inode, &mut by_path, &mut by_file, super::FuseOwned::empty(name("a")), &stale, Some(before), true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;
        let second = intern_projected(&mut next_inode, &mut by_inode, &mut by_path, &mut by_file, super::FuseOwned::empty(name("b")), &stale, Some(after), true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;
        assert_eq!(first, second);
        assert_eq!(by_inode[&first].bindings.len(), 2);
        Ok(())
    }

    #[test]
    fn a_reused_identity_gets_a_fresh_inode() -> Result<(), i32> {
        let [before, after] = positions();
        let reused = regular(FileId::new(), 3);
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(super::FuseOwned::empty(MountPath::root()), directory(FileId::new()), Some(before), 1, &super::fuse_fixture_work()).unwrap(),
        )]);
        let mut by_path = HashMap::from([(super::FuseOwned::empty(MountPath::root()), ROOT_INODE)]);
        let mut by_file = HashMap::new();
        let old = intern_projected(&mut next_inode, &mut by_inode, &mut by_path, &mut by_file, super::FuseOwned::empty(name("removed")), &reused, Some(before), true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;
        if let Some(binding) = by_inode
            .get_mut(&old)
            .and_then(|entry| entry.binding_mut(&name("removed"), &super::fuse_fixture_work()).unwrap())
        {
            binding.kernel = Some(before);
        }
        let forgotten = { let mut deferred = super::FuseOwned::default(); forget_unbound_names(&mut by_inode, &mut by_path, old, &name("created"), |path, _| Ok(*path != name("removed")), &mut deferred, &super::fuse_fixture_work()).unwrap(); deferred };
        assert!(matches!(
            forgotten.as_slice(),
            [KernelCacheItem::Entry { parent: ROOT_INODE, name }] if name.as_slice() == b"removed"
        ));
        retire_reused_identity(&mut by_inode, &mut by_file, old);
        let created = intern_projected(&mut next_inode, &mut by_inode, &mut by_path, &mut by_file, super::FuseOwned::empty(name("created")), &reused, Some(after), true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;
        assert_ne!(created, old, "the new file gets a fresh inode");
        assert_eq!(by_inode[&created].anchor(), &name("created"));
        assert!(
            by_inode[&old].bindings.is_empty(),
            "the old inode names no file"
        );
        assert_eq!(by_path.get(&name("created")), Some(&created));
        assert_eq!(by_file.get(&reused.node.file_id), Some(&created));
        Ok(())
    }

    #[test]
    fn lookup_cache_requires_an_unchanged_view() {
        let path = name("cached");
        let lookup = regular(FileId::from_bytes([3; 16]), 7);
        let [stamp] = positions();
        let mut by_inode =
            HashMap::from([(2, InodeEntry::new(super::FuseOwned::empty(path.clone()), lookup, Some(stamp), 0, &super::fuse_fixture_work()).unwrap())]);
        let by_path = HashMap::from([(super::FuseOwned::empty(path.clone()), 2)]);

        assert!(cached_projected_lookup(&mut by_inode, &by_path, &path, |_, _| Ok(false), &super::fuse_fixture_work()).unwrap().is_none());
        assert_eq!(by_inode[&2].lookup_references, 0);
        assert_eq!(
            cached_projected_lookup(&mut by_inode, &by_path, &path, |file_id, sampled| Ok({
                file_id == lookup.node.file_id && sampled == stamp
            }), &super::fuse_fixture_work()).unwrap(),
            Some((2, lookup, stamp))
        );
        assert_eq!(by_inode[&2].lookup_references, 1);
    }

    #[test]
    fn page_stores_and_content_claims_exclude_each_other() {
        let [before, target, after] = positions();
        let (stored, claimed) = (FileId::new(), FileId::new());
        let mut stores = PageStores::default();
        assert!(stores.may_begin(stored));
        stores.begin(stored, before, &super::fuse_fixture_work()).unwrap();
        assert!(!stores.may_begin(stored), "one store per file at a time");
        assert!(stores.precede(target), "the barrier waits for older reads");
        assert!(!stores.precede(before));

        stores.claim(claimed, &super::fuse_fixture_work()).unwrap();
        stores.claim(claimed, &super::fuse_fixture_work()).unwrap();
        assert!(!stores.may_begin(claimed));
        stores.release(claimed);
        assert!(!stores.may_begin(claimed), "every claim must be released");
        stores.release(claimed);
        assert!(stores.may_begin(claimed));

        stores.land(stored);
        assert!(!stores.precede(target));
        stores.begin(stored, after, &super::fuse_fixture_work()).unwrap();
        assert!(!stores.precede(target), "a newer read supersedes nothing");
        stores.land(stored);
        stores.claim(stored, &super::fuse_fixture_work()).unwrap();
        assert!(!stores.may_begin(stored));
    }

    #[test]
    fn only_content_changing_opens_claim() {
        assert!(!changes_content(libc::O_RDONLY));
        assert!(!changes_content(libc::O_RDONLY | libc::O_DIRECT));
        assert!(changes_content(libc::O_WRONLY));
        assert!(changes_content(libc::O_RDWR));
        assert!(changes_content(libc::O_RDONLY | libc::O_TRUNC));
    }

    #[test]
    fn invalidation_drops_exactly_the_superseded_kernel_items() -> Result<(), i32> {
        let [held, through] = positions();
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(super::FuseOwned::empty(MountPath::root()), directory(FileId::new()), Some(held), 1, &super::fuse_fixture_work()).unwrap(),
        )]);
        let mut by_path = HashMap::from([(super::FuseOwned::empty(MountPath::root()), ROOT_INODE)]);
        let mut by_file = HashMap::new();
        let mut linked = regular(FileId::new(), 3);
        linked.node.link_count = 2;
        let mut intern = |by_inode: &mut HashMap<u64, InodeEntry>, path: &str| {
            intern_projected(&mut next_inode, by_inode, &mut by_path, &mut by_file, super::FuseOwned::empty(name(path)), &linked, Some(held), true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)
        };
        let file = intern(&mut by_inode, "changed")?;
        assert_eq!(intern(&mut by_inode, "kept")?, file);
        let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
        root.kernel = Some(held);
        let work = super::fuse_fixture_work();
        assert!(root.remember_negative_child(b"appeared", held, &work).expect("tracked name"));
        assert!(root.remember_negative_child(b"still-absent", held, &work).expect("tracked name"));
        let entry = by_inode.get_mut(&file).ok_or(libc::ESTALE)?;
        entry.kernel = Some(held);
        for binding in &mut entry.bindings {
            binding.kernel = Some(held);
        }
        let changed = [name("changed"), name("appeared")];

        let mut items = stale_kernel_items(&mut by_inode, &by_path, |path, _, _| Ok(!changed.contains(path)), through, &super::fuse_fixture_work()).unwrap();

        let entry = |parent: u64, name: &str| KernelCacheItem::Entry {
            parent,
            name: super::FuseOwned::empty(name.as_bytes().to_vec()),
        };
        let mut expected = vec![
            // The file's anchor is its first name, which changed.
            KernelCacheItem::Inode(file),
            entry(ROOT_INODE, "changed"),
            entry(ROOT_INODE, "appeared"),
        ];
        let key = |item: &KernelCacheItem| format!("{item:?}");
        items.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(items, expected);
        let root = &by_inode[&ROOT_INODE];
        assert_eq!(root.kernel, Some(held), "an unchanged inode stays held");
        assert!(
            root.negative_children
                .contains_key(b"still-absent".as_slice())
        );
        assert!(!root.negative_children.contains_key(b"appeared".as_slice()));
        let file = &by_inode[&file];
        // Pages an open handle reads after the drop postdate the check.
        assert_eq!(file.kernel, Some(through));
        assert_eq!(file.binding(&name("changed"), &super::fuse_fixture_work()).unwrap().and_then(|b| b.kernel), None);
        assert_eq!(
            file.binding(&name("kept"), &super::fuse_fixture_work()).unwrap().and_then(|b| b.kernel),
            Some(held)
        );
        Ok(())
    }

    #[test]
    fn directory_streams_wait_at_their_offset_and_evict_the_oldest() {
        let stream = |emitted| super::DirectoryStream {
            path: super::FuseOwned::empty(MountPath::root()),
            parent_inode: ROOT_INODE,
            binding_epoch: None,
            page: None,
            entry_position: 0,
            entries_stamp: None,
            confirmed: ConfirmedNames::default(),
            exhausted: false,
            emitted,
            parked: 0,
            park_replacement: None,
            storage: super::FuseOwned::empty(()),
        };
        let mut streams = super::DirectoryStreams::default();
        let park = |streams: &mut super::DirectoryStreams, mut stream: super::DirectoryStream| {
            let work = super::fuse_fixture_work();
            streams.prepare_park(&mut stream, &work).expect("park capacity");
            streams.park(ROOT_INODE, stream);
        };
        for offset in 0..super::MAXIMUM_DIRECTORY_STREAMS as u64 {
            park(&mut streams, stream(offset));
        }
        assert!(streams.take(ROOT_INODE, 3).is_some());
        assert!(
            streams.take(ROOT_INODE, 3).is_none(),
            "a stream resumes once"
        );
        park(&mut streams, stream(3));
        park(&mut streams, stream(1_000));
        assert!(
            streams.take(ROOT_INODE, 0).is_none(),
            "the longest-waiting stream makes room"
        );
        assert!(streams.take(ROOT_INODE, 3).is_some());
        assert!(streams.take(ROOT_INODE, 1_000).is_some());
        assert!(streams.take(ROOT_INODE + 1, 1).is_none());
    }

    #[test]
    fn revalidation_waits_for_deferred_notifications_in_flight() {
        let target = Some(ViewStamp::ORIGIN);
        let mut queue = super::InvalidationQueue {
            notified: target,
            scanned: target,
            processed: target,
            ..Default::default()
        };
        assert!(queue.is_idle());
        queue.deferred.push(KernelCacheItem::Inode(ROOT_INODE));
        assert!(queue.is_pending(target));
        let items = queue.take_deferred();
        assert_eq!(items.len(), 1);
        assert!(queue.deferred.is_empty());
        assert!(
            queue.is_pending(target),
            "kernel notifications have not finished"
        );
        assert!(!queue.is_idle());
        queue.deferred.push(KernelCacheItem::Inode(ROOT_INODE));
        queue.finish(target, Ok(()));
        assert!(
            queue.is_pending(target),
            "a later deferred item is still owed"
        );
        assert_eq!(queue.take_deferred().len(), 1);
        queue.finish(target, Ok(()));
        assert!(queue.is_idle());
    }

    #[test]
    fn releasing_deferred_items_wakes_the_invalidator() -> Result<(), Box<dyn std::error::Error>> {
        use super::ProjectionCore;
        use std::sync::PoisonError;
        for finishing in [false, true] {
            let (source, _) = shared_sources()?;
            let core = Arc::new(ProjectionCore::new(
                Arc::new(source),
                empty_projection_state(),
                crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )?);
            let waiting = core.state.lock().map_err(|_| "poisoned")?;
            let invalidator = std::thread::spawn({
                let core = Arc::clone(&core);
                move || {
                    let state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
                    let (state, waited) = core
                        .invalidation
                        .wait_timeout_while(state, std::time::Duration::from_secs(20), |state| {
                            state.invalidation.deferred.is_empty()
                        })
                        .unwrap_or_else(PoisonError::into_inner);
                    (state.invalidation.deferred.len(), waited.timed_out())
                }
            });
            drop(waiting);
            // Let the invalidator sleep before the item is deferred.
            std::thread::sleep(std::time::Duration::from_millis(50));
            let mut state = if finishing {
                core.state_to_finish()
            } else {
                core.state().map_err(|_| "stopped")?
            };
            state
                .invalidation
                .deferred
                .push(KernelCacheItem::Inode(ROOT_INODE));
            drop(state);
            let (deferred, timed_out) = invalidator.join().map_err(|_| "invalidator panicked")?;
            assert_eq!((deferred, timed_out), (1, false), "finishing: {finishing}");
        }
        Ok(())
    }

    #[test]
    fn a_confirmation_at_a_position_lapses_when_its_name_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let file = source.create_file(&name("a"), FileMetadata::default())?;
        source.hard_link(&name("a"), &name("b"))?;
        let file_id = file.node.file_id;
        let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;

        let work = super::fuse_fixture_work();
        let mut confirmed = ConfirmedNames::default();
        confirmed.confirm(file_id, super::FuseOwned::empty(name("b")), Some(stamp), &work).expect("confirmed name");
        confirmed.confirm(file_id, super::FuseOwned::empty(name("a")), None, &work).expect("confirmed name");
        assert!(confirmed.still_bound(&source, file_id, &name("b"), &work).expect("name history"));
        assert!(confirmed.still_bound(&source, file_id, &name("a"), &work).expect("name history"));
        assert!(!confirmed.still_bound(&source, FileId::new(), &name("b"), &work).expect("name history"));
        assert!(!confirmed.still_bound(&source, file_id, &name("c"), &work).expect("name history"));

        // The name changes after the page was read, before it is emitted.
        source.remove(&name("b"), Some(file_id))?;
        source.create_file(&name("b"), FileMetadata::default())?;
        assert!(!confirmed.still_bound(&source, file_id, &name("b"), &work).expect("name history"));
        // A sibling changing leaves another name's confirmation standing.
        let mut sibling = ConfirmedNames::default();
        let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
        sibling.confirm(file_id, super::FuseOwned::empty(name("a")), Some(stamp), &work).expect("confirmed name");
        source.create_file(&name("c"), FileMetadata::default())?;
        assert!(sibling.still_bound(&source, file_id, &name("a"), &work).expect("name history"));
        Ok(())
    }

    #[test]
    fn node_facts_follow_the_node_and_directory_facts_its_listing()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let file = source.create_file(&name("file"), FileMetadata::default())?;
        let directory = source.create_directory(&name("directory"), FileMetadata::default())?;
        let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
        let mut file_entry = InodeEntry::new(super::FuseOwned::empty(name("file")), file, Some(stamp), 1, &super::fuse_fixture_work()).unwrap();
        let directory_entry = InodeEntry::new(super::FuseOwned::empty(name("directory")), directory, Some(stamp), 1, &super::fuse_fixture_work()).unwrap();
        let work = super::fuse_fixture_work();
        assert_eq!(file_entry.current_facts(&source, &work).expect("node history"), Some(stamp));
        assert!(file_entry.lacks_named_attributes(&source, &work).expect("node history"));

        // Names changing around a node leave its facts current...
        source.create_file(&name("sibling"), FileMetadata::default())?;
        source.hard_link(&name("sibling"), &name("alias"))?;
        assert_eq!(file_entry.current_facts(&source, &work).expect("node history"), Some(stamp));
        // ...but not a directory's, whose listing they change.
        source.create_file(
            &name("directory").child(b"child".to_vec()),
            FileMetadata::default(),
        )?;
        assert_eq!(directory_entry.current_facts(&source, &work).expect("listing history"), None);

        // A change to the node supersedes its facts.
        source.write_range(&name("file"), 0, Bytes::from_static(b"changed"))?;
        assert_eq!(file_entry.current_facts(&source, &work).expect("node history"), None);
        assert!(!file_entry.lacks_named_attributes(&source, &work).expect("node history"));
        file_entry.facts = source.view_stamp();
        file_entry.lookup = source.lookup(&name("file"))?.ok_or("file")?;
        assert!(file_entry.lacks_named_attributes(&source, &work).expect("node history"));
        source.write_attribute(
            &name("file"),
            b"user.present",
            Bytes::from_static(b"value"),
            MountAttributeWriteMode::Upsert,
        )?;
        assert!(!file_entry.lacks_named_attributes(&source, &work).expect("node history"));
        Ok(())
    }

    #[test]
    fn rename_noreplace_matches_linux_libc() {
        assert_eq!(super::RENAME_NOREPLACE, libc::RENAME_NOREPLACE);
    }

    #[test]
    fn read_only_mount_rejects_truncating_open_before_source_mutation() {
        assert_eq!(admit_open(false, libc::O_TRUNC), Err(libc::EROFS));
        assert_eq!(admit_open(false, libc::O_RDONLY), Ok(()));
        assert_eq!(admit_open(true, libc::O_TRUNC), Ok(()));
    }

    #[test]
    fn root_inode_survives_checkout_identity_changes() -> Result<(), i32> {
        let old_file = FileId::from_bytes([1; 16]);
        let new_file = FileId::from_bytes([2; 16]);
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(super::FuseOwned::empty(MountPath::root()), directory(old_file), None, 1, &super::fuse_fixture_work()).unwrap(),
        )]);
        let mut inode_by_file = HashMap::from([(old_file, ROOT_INODE)]);

        replace_root_lookup(&mut by_inode, &mut inode_by_file, directory(new_file), &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;

        assert_eq!(by_inode[&ROOT_INODE].lookup.node.file_id, new_file);
        assert_eq!(by_inode[&ROOT_INODE].anchor(), &MountPath::root());
        assert_eq!(inode_by_file.get(&new_file), Some(&ROOT_INODE));
        assert!(!inode_by_file.contains_key(&old_file));
        assert_eq!(
            replace_root_lookup(&mut by_inode, &mut inode_by_file, regular(FileId::new(), 0), &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno),
            Err(libc::EIO),
            "the root stays a directory"
        );
        Ok(())
    }

    #[test]
    fn enumerated_entries_receive_nonzero_inodes_without_fake_lookup_references() -> Result<(), i32>
    {
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::<u64, InodeEntry>::new();
        let mut inode_by_path = HashMap::new();
        let mut inode_by_file = HashMap::new();
        let lookup = regular(FileId::new(), 4);
        let path = name("preexisting.bin");

        let enumerated = intern_projected(&mut next_inode, &mut by_inode, &mut inode_by_path, &mut inode_by_file, super::FuseOwned::empty(path.clone()), &lookup, None, false, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;
        assert_ne!(enumerated, 0);
        assert_eq!(by_inode[&enumerated].lookup_references, 0);

        let looked_up = intern_projected(&mut next_inode, &mut by_inode, &mut inode_by_path, &mut inode_by_file, super::FuseOwned::empty(path), &lookup, None, true, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno)?;
        assert_eq!(looked_up, enumerated);
        assert_eq!(by_inode[&enumerated].lookup_references, 1);

        let retained_by_inode_count = by_inode.len();
        let retained_inode_by_file_count = inode_by_file.len();
        next_inode = u64::MAX;
        let overflow = regular(FileId::new(), 4);
        assert_eq!(
            intern_projected(&mut next_inode, &mut by_inode, &mut inode_by_path, &mut inode_by_file, super::FuseOwned::empty(name("overflow.bin")), &overflow, None, false, &super::fuse_fixture_work()).map_err(super::fuse_fixture_errno),
            Err(libc::EOVERFLOW)
        );
        assert_eq!(next_inode, u64::MAX);
        assert_eq!(by_inode.len(), retained_by_inode_count);
        assert_eq!(inode_by_file.len(), retained_inode_by_file_count);
        assert_eq!(by_inode[&enumerated].lookup_references, 1);
        assert_eq!(inode_by_file[&lookup.node.file_id], enumerated);
        Ok(())
    }

    type MemoryCheckout =
        crate::Checkout<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;

    fn posix_config() -> crate::model::VolumeConfig {
        let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
        config.profile = crate::model::FilesystemProfile::Posix;
        config
    }

    fn memory_checkout(
        config: crate::model::VolumeConfig,
        mode: crate::model::CheckoutMode,
        count: usize,
    ) -> Result<(Vec<MemoryCheckout>, crate::model::VolumeConfig), Box<dyn std::error::Error>> {
        use crate::model::GenerationSelector;
        let fs = crate::Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkouts = runtime.block_on(async {
            let cancellation = crate::CancellationToken::new();
            let volume = fs
                .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            let mut checkouts = Vec::with_capacity(count);
            for _ in 0..count {
                checkouts.push(
                    volume
                        .checkout(
                            GenerationSelector::Head,
                            mode,
                            crate::WorkBudget::UNBOUNDED,
                            &cancellation,
                        )
                        .await?
                        .value,
                );
            }
            Ok::<_, crate::OperationFailure<crate::FsError>>(checkouts)
        })?;
        Ok((checkouts, config))
    }

    fn tracking_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
        let (checkouts, config) = memory_checkout(
            posix_config(),
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::TrackingSafe,
                mutations: MutationMode::PrivateOverlay,
            },
            2,
        )?;
        let mut sources = checkouts.into_iter().map(|checkout| {
            MemorySource::new(
                Arc::new(super::super::SharedCheckout::new(checkout)),
                config,
            )
        });
        match (sources.next(), sources.next()) {
            (Some(reader), Some(writer)) => Ok((reader?, writer?)),
            _ => Err("two checkouts were requested".into()),
        }
    }

    fn shared_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        shared_sources_of(posix_config())
    }

    fn shared_sources_of(
        config: crate::model::VolumeConfig,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
        acyclic_native_runtime::initialize_native_io_services()?;
        let (checkouts, config) = memory_checkout(
            config,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            1,
        )?;
        let checkout = checkouts.into_iter().next().ok_or("one checkout")?;
        let shared = Arc::new(super::super::SharedCheckout::new(checkout));
        Ok((
            MemorySource::new(Arc::clone(&shared), config)?,
            MemorySource::new(shared, config)?,
        ))
    }

    #[derive(Debug, thiserror::Error)]
    #[error("installed FUSE callback owner has no authored WRITE permission")]
    struct FuseWriteDenied;

    struct FuseWriteGuard {
        original: crate::NativeMaterializationWork,
        writable: std::sync::atomic::AtomicBool,
    }

    impl super::super::NativeMountCallbackGuard for FuseWriteGuard {
        fn check<'a>(&'a self, work: &'a super::super::NativeMountMeterWork)
            -> Result<super::super::NativeMountCallbackFuture<'a>, crate::NativeMountError>
        {
            if !self.original.shares_ledger(&work.native_materialization_work()) {
                return Err(crate::NativeMountError::ControlPending);
            }
            super::super::NativeMountCallbackFuture::new(work, || async move {
                work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
                Ok(())
            })
        }

        fn check_write<'a>(&'a self, work: &'a super::super::NativeMountMeterWork)
            -> Result<super::super::NativeMountCallbackFuture<'a>, crate::NativeMountError>
        {
            if !self.original.shares_ledger(&work.native_materialization_work()) {
                return Err(crate::NativeMountError::ControlPending);
            }
            if !self.writable.load(std::sync::atomic::Ordering::Acquire) {
                return Err(super::super::NativeMountCallbackReservation::prepare(work)?
                    .into_error(FuseWriteDenied));
            }
            super::super::NativeMountCallbackFuture::new_guarded(work, move || async move {
                if self.writable.load(std::sync::atomic::Ordering::Acquire) { Ok(()) }
                else { Err(FuseWriteDenied) }
            })
        }
    }

    #[test]
    fn installed_write_denial_preserves_reads_and_post_effect_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (checkouts, config) = memory_checkout(
            posix_config(), crate::model::CheckoutMode::tracking_transaction(), 1,
        )?;
        let shared = Arc::new(super::super::SharedCheckout::new(
            checkouts.into_iter().next().ok_or("one checkout")?,
        ));
        let around = MemorySource::new(Arc::clone(&shared), config)?;
        let path = name("write-authority");
        let created = around.create_file(&path, FileMetadata::default())?;
        let original = crate::NativeMaterializationWork::new(
            finite_handler_policy(), &crate::CancellationToken::new(),
        ).map_err(|(cause, _)| cause)?;
        let guard = Arc::new(FuseWriteGuard {
            original: original.clone(), writable: std::sync::atomic::AtomicBool::new(false),
        });
        let source = MemorySource::new_at_meter(
            shared, config, crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            original.claim(0)?, &original.native_mount_work(), Some(&original),
        )?.with_native_callback_guard(guard.clone());
        let source = source.into_arc_meter(&original.native_mount_work())?;
        let core = Arc::new(super::ProjectionCore::new(
            source.clone(), empty_projection_state(), finite_handler_policy(),
            &crate::CancellationToken::new(),
        )?);
        let projection = super::FuseProjection {
            core: Arc::clone(&core), skips_opendir: false, skips_open: false,
        };
        let work = core.callback_meter()?.ok_or("installed original")?;
        assert!(projection.open_source(source.as_ref(), &path, created.node.file_id,
            libc::O_RDONLY, None, &work).is_ok(), "READ does not require authored WRITE");
        assert!(projection.open_source(source.as_ref(), &path, created.node.file_id,
            libc::O_TRUNC, None, &work).is_err());
        assert!(around.lookup(&path)?.is_some());
        let denied = |result: Result<(), super::FuseRequestError>| {
            assert!(matches!(result, Err(super::FuseRequestError::Native(
                crate::NativeMountError::CallbackDenied { .. }))));
        };
        denied(projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("write-authority"), &work));
        denied(projection.rename_name(ROOT_INODE, std::ffi::OsStr::new("write-authority"),
            ROOT_INODE, std::ffi::OsStr::new("renamed"), 0, &work));
        denied(projection.link_name(0, ROOT_INODE, std::ffi::OsStr::new("linked"), &work).map(|_| ()));
        denied(projection.write_handle(0, 0, 0, b"denied", &work).map(|_| ()));
        denied(projection.allocate(0, 0, 0, 1, 0, &work));
        denied(projection.copy_range(0, 0, 0, 0, 0, 0, 1, 0, &work).map(|_| ()));
        denied(projection.create_node(ROOT_INODE, std::ffi::OsStr::new("denied-directory"),
            |path, work| source.create_directory_with_meter(path, FileMetadata::default(), work)
                .map_err(super::FuseRequestError::from), &work).map(|_| ()));
        denied(projection.write_named_attribute(0, std::ffi::OsStr::new("user.denied"),
            b"denied", 0, 0, &work));
        denied(projection.remove_named_attribute(0, std::ffi::OsStr::new("user.denied"), &work));
        assert!(projection.source_write(&work, || source.remove_with_meter(
            &path, Some(created.node.file_id), &work,
        ).map_err(super::FuseRequestError::from)).is_err());
        assert!(around.lookup(&path)?.is_some(), "pre-effect refusal cannot remove the file");
        guard.writable.store(true, std::sync::atomic::Ordering::Release);
        let removed = projection.source_write(&work, || {
            source.remove_with_meter(&path, Some(created.node.file_id), &work)
                .map_err(super::FuseRequestError::from)?;
            guard.writable.store(false, std::sync::atomic::Ordering::Release);
            Ok(())
        });
        assert!(removed.is_err(), "revocation after the real remove cannot report success");
        assert!(around.lookup(&path)?.is_none(), "no fictitious rollback");
        let retained = core.take_handler_failure().ok_or("post-effect refusal custody")?;
        assert!(matches!(retained.cause, crate::NativeMountError::CallbackDenied { .. }));
        assert_eq!(retained.work, original.receipt());
        assert_eq!(core.completed_callback_work(), crate::WorkCounters::UNCHARGED);
        Ok(())
    }


    fn finite_handler_policy() -> crate::WorkBudget {
        crate::WorkBudget {
            authority_records_read: 1 << 20, authority_records_appended: 1 << 20,
            authority_bytes_read: 1 << 26, authority_bytes_written: 1 << 26,
            object_probes: 1 << 20, backend_read_operations: 1 << 20,
            backend_write_operations: 1 << 20, durability_operations: 1 << 20,
            page_reads: 1 << 20, page_writes: 1 << 20,
            object_bytes_read: 1 << 26, object_bytes_written: 1 << 26,
            bytes_hashed: 1 << 26, bytes_copied: 1 << 26, bytes_encoded: 1 << 26,
            source_bytes_read: 1 << 26, source_path_components: 1 << 20,
            source_entries_visited: 1 << 20, output_bytes: 1 << 26,
            items_examined: 1 << 20, items_returned: 1 << 20,
            allocation_operations: 1 << 20, peak_allocation_bytes: 1 << 26,
            materializations: 1 << 20,
        }
    }

    #[test]
    fn actual_source_effect_precedes_callback_cancellation_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, around) = shared_sources()?;
        let path = name("removed-before-cancellation");
        let created = around.create_file(&path, FileMetadata::default())?;
        let source = Arc::new(source);
        let cancellation = crate::CancellationToken::new();
        let core = super::ProjectionCore::new(
            source.clone(), empty_projection_state(), finite_handler_policy(), &cancellation,
        )?;
        let scope = core.request("unlink");
        let result = scope.run(|work| {
            source.remove_with_meter(&path, Some(created.node.file_id), work)?;
            cancellation.cancel();
            source.lookup_with_meter(&path, work).map_err(super::FuseRequestError::from)
        });
        let Err(failure) = result else {
            return Err("post-effect cancellation was reported as success".into());
        };
        assert_ne!(failure.work, crate::WorkCounters::UNCHARGED);
        assert_eq!(super::native_callback_errno(&failure.cause), libc::ECANCELED);
        let prefix = failure.work;
        assert_eq!(scope.finish::<Option<MountLookup>>(Err(failure)), Err(libc::ECANCELED));
        assert!(core.handler_failed.load(std::sync::atomic::Ordering::Acquire));
        let retained = core.take_handler_failure().ok_or("actual fatal request failure missing")?;
        assert!(core.handler_failed.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(retained.work, prefix);
        assert_eq!(core.completed_callback_work(), prefix);
        assert!(around.lookup(&path)?.is_none(), "the completed SDK remove is not rolled back or hidden");
        Ok(())
    }

    #[test]
    fn authentic_missing_source_name_does_not_poison_handler_health()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let source = Arc::new(source);
        let cancellation = crate::CancellationToken::new();
        let core = super::ProjectionCore::new(
            source.clone(), empty_projection_state(), finite_handler_policy(), &cancellation,
        )?;
        let path = name("authentically-absent");
        let scope = core.request("lookup");
        let result = scope.run(|work| {
            let path = super::fuse_clone_path(&path, work)?;
            source.lookup_with_meter(&path, work)?
                .ok_or(MountSourceError::NotFound).map_err(super::FuseRequestError::from)
        });
        let Err(failure) = &result else {
            return Err("real absent lookup lost its original request packet".into());
        };
        let prefix = failure.work;
        assert_eq!(core.completed_callback_work(), prefix);
        assert_eq!(scope.finish(result).err(), Some(libc::ENOENT));
        assert!(core.take_handler_failure().is_none());
        assert_eq!(core.completed_callback_work(), prefix, "SDK projection does not fold twice");
        assert!(!core.handler_failed.load(std::sync::atomic::Ordering::Acquire));
        Ok(())
    }

    #[test]
    fn active_callback_borrows_original_meter_without_future_receipt_double_count()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, around) = shared_sources()?;
        let path = name("shared-original-callback");
        around.create_file(&path, FileMetadata::default())?;
        let source: Arc<dyn MountFilesystem> = Arc::new(source);
        let cancellation = crate::CancellationToken::new();
        let receipt = super::super::run_native_mount_handler(finite_handler_policy(), &cancellation, |work| {
            let root = source.lookup_with_meter(&MountPath::root(), work)?
                .ok_or(MountSourceError::NotFound)?;
            let stamp = source.view_stamp_with_meter(work)?;
            let core = super::ProjectionCore::root_with_meter(
                Arc::clone(&source), root, stamp, true, 0, 0, work)?;
            let _active = super::FuseStartupMeterScope::new(&core, work)?;
            let scope = core.request("lookup");
            let result = scope.run(|callback_work| {
                assert!(Arc::ptr_eq(callback_work.meter(), work.meter()));
                let path = super::fuse_clone_path(&path, callback_work)?;
                source.lookup_with_meter(&path, callback_work).map_err(super::FuseRequestError::from)
            });
            assert!(scope.finish(result).map_err(std::io::Error::from_raw_os_error)?.is_some());
            assert_eq!(core.completed_callback_work(), crate::WorkCounters::UNCHARGED);
            assert!(work.meter().lock().work.bytes_copied != 0);
            Ok(())
        }).map_err(|failure| failure.cause)?;
        assert!(receipt.work.bytes_copied != 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_installed_callbacks_observe_original_finite_mount_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, around) = shared_sources()?;
        around.create_file(&name("cancelled-native-lookup"), FileMetadata::default())?;
        let volume_id = source.volume_id()?;
        let destination = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let mut session = super::super::mount_native_measured(crate::NativeMountRequest {
            mount_id: crate::MountId::new(), volume_id,
            destination: destination.path().to_path_buf(), writable: true,
        }, Arc::new(source), finite_handler_policy(), &cancellation).map_err(|(cause, _)| cause)?.value;
        cancellation.cancel();
        let error = std::fs::metadata(destination.path().join("cancelled-native-lookup"))
            .expect_err("installed callback cannot replace the original cancellation root");
        assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
        session.stop_measured(finite_handler_policy(), &crate::CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
        Ok(())
    }


    #[test]
    fn measured_write_copy_refusal_precedes_the_real_source_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let path = name("copy-denied");
        source.create_file(&path, FileMetadata::default())?;
        let opened = source.open_file(&path)?;
        opened.write_range(0, Bytes::from_static(b"original"))?;
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_handler_policy();
        budget.bytes_copied = 0;
        let failure = super::super::run_native_mount_handler(budget, &cancellation, |work| {
            let bytes = super::fuse_source_chunk(b"replacement", work)
                .map_err(crate::NativeMountError::from)?;
            opened.write_range_with_meter(0, bytes, work)
                .map_err(crate::NativeMountError::Source)
        }).expect_err("zero copied-byte authority must refuse the packet before source mutation");
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(opened.read_range(0, 8)?.as_ref(), b"original");
        Ok(())
    }

    #[test]
    fn measured_deferred_open_refuses_an_actual_rebound_file_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let source: Arc<dyn MountFilesystem> = Arc::new(source);
        let path = name("deferred-rebound");
        let opened = source.create_file(&path, FileMetadata::default())?;
        let cancellation = crate::CancellationToken::new();
        let deferred = super::super::run_native_mount_handler(
            finite_handler_policy(), &cancellation, |work| {
                let path = super::fuse_clone_path(&path, work)
                    .map_err(crate::NativeMountError::from)?;
                let allocation = super::fuse_initialized(
                    (), std::mem::size_of::<super::DeferredOpenFile>() as u64, work,
                ).map_err(crate::NativeMountError::from)?;
                Ok(Arc::new(super::DeferredOpenFile {
                    source: super::fuse_arc_clone(&source, work)
                        .map_err(crate::NativeMountError::from)?,
                    path,
                    opened,
                    file: std::sync::Mutex::new(None),
                    _allocation: allocation,
                }))
            },
        ).map_err(|failure| failure.cause)?.value;
        source.remove(&path, Some(opened.node.file_id))?;
        let rebound = source.create_file(&path, FileMetadata::default())?;
        assert_ne!(rebound.node.file_id, opened.node.file_id);
        let failure = super::super::run_native_mount_handler(
            finite_handler_policy(), &cancellation,
            |work| deferred.file(work)?.lookup_with_meter(work).map_err(crate::NativeMountError::Source),
        ).expect_err("a lazy open must not attach its inode to a replacement");
        assert!(matches!(failure.cause,
            crate::NativeMountError::Source(super::super::MountSourceError::Stale)));
        assert!(deferred.file.lock().map_err(|_| "poisoned deferred file")?.is_none());
        assert!(failure.work.items_examined > 0);
        Ok(())
    }

    fn mount(
        source: Arc<dyn MountFilesystem>,
        volume_id: crate::VolumeId,
        destination: &std::path::Path,
    ) -> Result<crate::NativeMountSession, crate::NativeMountError> {
        crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id,
                destination: destination.to_path_buf(),
                writable: true,
            },
            source,
        )
    }

    fn listing(directory: &std::path::Path) -> std::io::Result<Vec<std::ffi::OsString>> {
        let mut names = std::fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        Ok(names)
    }

    #[test]
    fn last_facade_drop_signals_the_original_keeper_and_retires_its_actual_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let core = Arc::new(super::ProjectionCore::new(
            Arc::new(source), empty_projection_state(), finite_handler_policy(),
            &crate::CancellationToken::new(),
        )?);
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 0, backend_write_operations: 1,
            ..finite_handler_policy()
        }, &cancellation).map_err(|(cause, _)| cause)?;
        let birth_cleanup = original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let directory = tempfile::tempdir()?;
        let root = cap_std::fs::Dir::open_ambient_dir(directory.path(), cap_std::ambient_authority())?;
        let control_allocation = original.claim(std::mem::size_of::<super::FuseDriverControl>() as u64)?;
        let control = Arc::new(super::FuseDriverControl {
            state: super::Mutex::new(super::FuseDriverControlState {
                physical: Some(super::FusePhysicalSession {
                    session: None, sdk_input: None, sdk_worker: None,
                    core: Arc::clone(&core),
                    active_root: Some(root), native_root_identity: None, retirement_helper: None,
                    birth_cleanup,
                }),
                retirement: Some(super::super::NativeMountRetirement {
                    retained_source: None, destination_guard: None, startup_source_watch: None,
                }),
                loan_custody: (false, false), startup_finished: true, stop_requested: false,
                retired: false, failure: None,
            }),
            changed: super::Condvar::new(), _allocation: control_allocation,
        });
        let job_allocation = original.claim(std::mem::size_of::<(
            Arc<super::ProjectionCore>, Arc<super::FuseDriverControl>, crate::NativeAllocation,
        )>() as u64)?;
        let job = (Arc::clone(&core), Arc::clone(&control), job_allocation);
        let (done, terminal) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || { super::run_fuse_retirement(job); done.send(()).unwrap(); });
        let facade = super::FuseSession {
            control: Arc::clone(&control), core: Arc::clone(&core),
            invalidator: None,
        };
        let before = original.receipt();
        cancellation.cancel();
        drop(facade);
        terminal.recv_timeout(std::time::Duration::from_secs(10))?;
        worker.join().map_err(|_| "the actual keeper panicked")?;
        let state = control.state.lock().unwrap_or_else(super::PoisonError::into_inner);
        assert!(state.retired && state.failure.is_none());
        let physical = state.physical.as_ref().ok_or("the actual physical holder disappeared")?;
        assert!(physical.active_root.is_none());
        assert_eq!(physical.birth_cleanup.remaining_work(), crate::WorkCounters::UNCHARGED);
        assert!(state.retirement.as_ref().is_some_and(super::super::NativeMountRetirement::is_retired));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations,
            "last facade drop and the real keeper terminal phase allocate no new controls");
        assert!(original.cancellation().is_cancelled());
        Ok(())
    }

    #[test]
    fn preborn_helper_frame_reaps_real_child_after_original_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                backend_read_operations: 16_004, backend_write_operations: 13,
                ..finite_handler_policy()
            }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut helper = super::FuseRetirementHelper::prepare_program(
            c"/bin/true", &original,
        )?;
        let before = original.receipt();
        cancellation.cancel();
        helper.retire(c"/unused", None)?;
        assert!(helper.helper.is_none() && helper.frame.is_none());
        let after = original.receipt();
        assert_eq!(after.allocation_operations, before.allocation_operations,
            "all known argv/environment/control backing existed before birth");
        assert!(original.cancellation().is_cancelled());
        helper.retire(c"/unused", None)?;
        assert_eq!(original.receipt(), after, "terminal replay cannot spawn another process");
        Ok(())
    }

    #[test]
    fn preborn_helper_keeps_original_spawn_cause_after_genuine_sdk_refusal()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                backend_read_operations: 16_004, backend_write_operations: 13,
                ..finite_handler_policy()
            }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut helper = super::FuseRetirementHelper::prepare_program(
            c"/this-native-helper-program-authentically-does-not-exist", &original,
        )?;
        let before = original.receipt();
        cancellation.cancel();
        let error = helper.retire(c"/unused", None).expect_err("the real posix_spawn must report ENOENT");
        assert!(matches!(error, crate::NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Io(error)
        ) if error.raw_os_error() == Some(libc::ENOENT)));
        assert!(helper.helper.is_none() && helper.frame.is_none());
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_changes_around_the_mount_reach_every_kernel_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mounted_source, around) = shared_sources()?;
        let mounted_source = Arc::new(mounted_source);
        let file = name("file");
        around.create_file(&file, FileMetadata::default())?;
        around.write_range(&file, 0, Bytes::from_static(b"first"))?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&mounted_source) as Arc<dyn MountFilesystem>,
            mounted_source.volume_id()?,
            temporary.path(),
        )?;
        let mounted = temporary.path().join("file");
        let added = temporary.path().join("added");
        // Cache data, a negative entry, and the listing in the kernel.
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert!(!added.exists());
        assert_eq!(listing(temporary.path())?, ["file"]);

        // Same length, same (unavailable) times: only invalidation can tell.
        around.write_range(&file, 0, Bytes::from_static(b"other"))?;
        around.create_file(&name("added"), FileMetadata::default())?;
        session.revalidate()?;
        assert_eq!(std::fs::read(&mounted)?, b"other");
        assert!(added.is_file());
        assert_eq!(listing(temporary.path())?, ["added", "file"]);

        // Without the barrier the change still arrives, promptly.
        around.remove(&name("added"), None)?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while added.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "a removal around the mount never reached the kernel"
            );
            std::thread::yield_now();
        }
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_an_open_reader_sees_a_write_through_another_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Read as _, Seek as _, Write as _};

        let (inner, _) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        let source = Arc::new(inner);
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
            volume_id,
            temporary.path(),
        )?;
        let shared = temporary.path().join("shared");
        std::fs::write(&shared, b"before")?;
        assert_eq!(std::fs::read(&shared)?, b"before");
        let mut writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&shared)?;
        let mut reader = std::fs::File::open(&shared)?;
        writer.seek(std::io::SeekFrom::End(0))?;
        writer.write_all(b"-after")?;
        writer.sync_all()?;
        let mut observed = Vec::new();
        reader.read_to_end(&mut observed)?;
        assert_eq!(observed, b"before-after");
        drop((writer, reader));
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_writes_keep_node_facts_exact() -> Result<(), Box<dyn std::error::Error>> {
        let (inner, _) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        let counted = Arc::new(GatedSource::ungated(Arc::new(inner)));
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&counted) as Arc<dyn MountFilesystem>,
            volume_id,
            temporary.path(),
        )?;
        let written = temporary.path().join("written");
        std::fs::write(&written, b"first")?;
        let before = counted.reads().len();
        for body in [b"second write".as_slice(), b"third".as_slice()] {
            let mut file = std::fs::OpenOptions::new().write(true).open(&written)?;
            std::io::Write::write_all(&mut file, body)?;
            drop(file);
            assert_eq!(std::fs::metadata(&written)?.len(), 12);
        }
        assert!(
            !counted.reads()[before..].contains(&b"written".to_vec()),
            "a stat or capability check after a write went back to the source"
        );
        assert_eq!(std::fs::read(&written)?, b"thirdd write");
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_listings_resume_across_pages_and_readers() -> Result<(), Box<dyn std::error::Error>> {
        let (inner, around) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        inner.create_directory(&name("many"), FileMetadata::default())?;
        let mut expected = (0..700)
            .map(|index| format!("entry-{index:04}"))
            .collect::<Vec<_>>();
        for entry in &expected {
            inner.create_file(
                &name("many").child(entry.as_bytes().to_vec()),
                FileMetadata::default(),
            )?;
        }
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::new(inner) as Arc<dyn MountFilesystem>,
            volume_id,
            temporary.path(),
        )?;
        let many = temporary.path().join("many");
        let names = |directory: &std::path::Path| -> std::io::Result<Vec<String>> {
            let mut names = listing(directory)?
                .into_iter()
                .map(|name| name.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            names.sort();
            Ok(names)
        };
        // Several readers list concurrently, each across many kernel pages.
        std::thread::scope(|scope| {
            let readers = (0..4)
                .map(|_| scope.spawn(|| names(&many)))
                .collect::<Vec<_>>();
            for reader in readers {
                let listed = reader.join().map_err(|_| "reader panicked")??;
                assert_eq!(listed, expected);
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        // A change around the mount reaches the cached listing.
        around.create_file(
            &name("many").child(b"added".to_vec()),
            FileMetadata::default(),
        )?;
        session.revalidate()?;
        expected.push("added".to_owned());
        expected.sort();
        assert_eq!(names(&many)?, expected);
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    fn only_folding_volumes_with_case_variants_fold_names() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::model::{CaseSensitivity, FilesystemProfile, Lifecycle, VolumeConfig};
        let folds = |profile, case_sensitivity| -> Result<bool, Box<dyn std::error::Error>> {
            let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
            config.profile = profile;
            config.case_sensitivity = case_sensitivity;
            let (source, _) = shared_sources_of(config)?;
            let folded = source.folded_path(&name("Dir").child(b"MiXeD".to_vec()));
            assert_eq!(
                folded.is_some(),
                source.folded_path(&MountPath::root()).is_some(),
                "folding is a property of the volume, not of one path"
            );
            if let Some(folded) = folded {
                assert_eq!(folded, name("dir").child(b"mixed".to_vec()));
            }
            Ok(source.folded_path(&MountPath::root()).is_some())
        };
        assert!(folds(
            FilesystemProfile::Portable,
            CaseSensitivity::ProfileFolded
        )?);
        assert!(!folds(
            FilesystemProfile::Portable,
            CaseSensitivity::Sensitive
        )?);
        // POSIX names are opaque bytes, so they have no case variants.
        assert!(!folds(
            FilesystemProfile::Posix,
            CaseSensitivity::ProfileFolded
        )?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_folded_names_stay_exact_through_the_mount() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt as _;

        let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
        config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
        let (source, _) = shared_sources_of(config)?;
        assert_eq!(
            source.folded_path(&name("Foo")),
            Some(name("foo")),
            "a folding volume keys every spelling alike"
        );
        let volume_id = source.volume_id()?;
        source.create_file(&name("foo"), FileMetadata::default())?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(Arc::new(source), volume_id, temporary.path())?;
        let at = |name: &str| temporary.path().join(name);
        let exists = |name: &str| at(name).exists();

        // Every spelling resolves to the one node.
        let inode = std::fs::metadata(at("foo"))?.ino();
        assert_eq!(std::fs::metadata(at("FOO"))?.ino(), inode);
        assert_eq!(std::fs::metadata(at("Foo"))?.ino(), inode);
        // A rename through one spelling reaches every other spelling at once.
        std::fs::rename(at("foo"), at("bar"))?;
        assert!(!exists("FOO") && !exists("Foo") && !exists("foo"));
        assert_eq!(std::fs::metadata(at("BAR"))?.ino(), inode);
        // So does a create after absences were looked up in other spellings.
        assert!(!exists("NEW") && !exists("New"));
        std::fs::write(at("new"), b"new")?;
        assert_eq!(std::fs::read(at("NEW"))?, b"new");
        assert_eq!(std::fs::read(at("New"))?, b"new");
        // And a removal through yet another spelling.
        std::fs::remove_file(at("NeW"))?;
        assert!(!exists("new") && !exists("NEW"));
        std::fs::remove_file(at("Bar"))?;
        assert!(!exists("bar") && !exists("BAR"));
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_rebind_reaches_every_kernel_cache() -> Result<(), Box<dyn std::error::Error>> {
        let (reader, writer) = tracking_sources()?;
        let reader = Arc::new(reader);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let file = name("file");
        writer.create_file(&file, FileMetadata::default())?;
        writer.write_range(&file, 0, Bytes::from_static(b"first"))?;
        writer.sync()?;
        runtime.block_on(reader.advance_to_head_async())?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&reader) as Arc<dyn MountFilesystem>,
            reader.volume_id()?,
            temporary.path(),
        )?;
        let mounted = temporary.path().join("file");
        let added = temporary.path().join("added");
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert!(!added.exists());
        assert_eq!(listing(temporary.path())?, ["file"]);

        writer.write_range(&file, 0, Bytes::from_static(b"other"))?;
        writer.create_file(&name("added"), FileMetadata::default())?;
        writer.sync()?;
        runtime.block_on(reader.advance_to_head_async())?;
        session.revalidate()?;
        assert_eq!(std::fs::read(&mounted)?, b"other");
        assert!(added.is_file());
        assert_eq!(listing(temporary.path())?, ["added", "file"]);
        assert!(session.stop()?);
        Ok(())
    }

    struct GatedSource {
        inner: Arc<dyn MountFilesystem>,
        gated_lookup: Vec<u8>,
        gated_directory: Vec<u8>,
        entered: SyncSender<()>,
        released: Mutex<Receiver<()>>,
        reads: Mutex<Vec<Vec<u8>>>,
    }

    impl GatedSource {
        fn ungated(inner: Arc<dyn MountFilesystem>) -> Self {
            let (entered, _) = sync_channel(0);
            let (_, released) = sync_channel(0);
            Self {
                inner,
                gated_lookup: Vec::new(),
                gated_directory: Vec::new(),
                entered,
                released: Mutex::new(released),
                reads: Mutex::new(Vec::new()),
            }
        }

        fn record_read(&self, path: &MountPath) {
            self.reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(path.components().last().cloned().unwrap_or_default());
        }

        fn reads(&self) -> Vec<Vec<u8>> {
            self.reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl GatedSource {
        fn pass(&self, path: &MountPath, gated: &[u8]) {
            if path.components().last().is_some_and(|last| last == gated) {
                let _ = self.entered.send(());
                let released = self
                    .released
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let _ = released.recv_timeout(Duration::from_secs(10));
            }
        }
    }

    macro_rules! source_methods {
        ($($name:ident($($arg:ident: $ty:ty),*) $(-> $result:ty)?;)*) => {
            $(fn $name(&self, $($arg: $ty),*) $(-> $result)? {
                self.inner.$name($($arg),*)
            })*
        };
    }

    impl MountFilesystem for GatedSource {
        source_methods! {
            native_callback_guard() -> Option<Arc<dyn super::super::NativeMountCallbackGuard>>;
            native_original_work() -> Result<Option<super::super::NativeMountMeterWork>, MountSourceError>;
            supports_posix_named_attributes() -> bool;
            flush_on_handle_close() -> bool;
            view_is_stable() -> bool;
            view_stamp() -> Option<ViewStamp>;
            node_unchanged_since(file_id: FileId, stamp: ViewStamp) -> bool;
            unchanged_since(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool;
            listing_unchanged_since(path: &MountPath, stamp: ViewStamp) -> bool;
            observe_view(observer: Weak<dyn ViewObserver>);
            observe_view_with_meter(observer: Weak<dyn ViewObserver>, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            fence_changes_with_meter(work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            acquire_binding_lease_with_meter(epoch: Option<u64>, work: &super::super::NativeMountMeterWork) -> Result<Box<dyn MountViewLease>, MountSourceError>;
            capture_host_paths_with_meter(root: &Path, paths: &[MountPath], identity: crate::NativeRootIdentity, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            capture_host_subtree_with_meter(root: &Path, path: &MountPath, identity: crate::NativeRootIdentity, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            read_pinned_with_meter(path: &MountPath, pin: super::super::MountContentPin, offset: u64, length: u64, piece: u32, sink: &mut super::super::NativeMountContentSink<'_>, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            read_link_with_meter(path: &MountPath, work: &super::super::NativeMountMeterWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
            read_range_with_meter(path: &MountPath, offset: u64, length: u32, work: &super::super::NativeMountMeterWork) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
            read_directory_with_meter(path: &MountPath, cursor: Option<&[u8]>, maximum: u32, work: &super::super::NativeMountMeterWork) -> Result<MountDirectoryPage, MountSourceError>;
            observe_view_with_work(observer: Weak<dyn ViewObserver>, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            fence_changes_with_work(work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            binding_epoch() -> Option<u64>;
            acquire_view_lease() -> Result<Box<dyn MountViewLease>, MountSourceError>;
            acquire_binding_lease(expected_epoch: Option<u64>) -> Result<Box<dyn MountViewLease>, MountSourceError>;
            acquire_binding_lease_with_work(expected_epoch: Option<u64>, work: &mut super::super::NativeMountWork<'_>) -> Result<Box<dyn MountViewLease>, MountSourceError>;
            lookup_with_work(path: &MountPath, work: &mut super::super::NativeMountWork<'_>) -> Result<Option<MountLookup>, MountSourceError>;
            lookup_pinned_with_work(path: &MountPath, work: &mut super::super::NativeMountWork<'_>) -> Result<Option<(MountLookup, Option<super::super::MountContentPin>)>, MountSourceError>;
            read_pinned_with_work(path: &MountPath, pin: super::super::MountContentPin, offset: u64, length: u64, piece: u32, sink: &mut super::super::NativeMountContentSink<'_>, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            open_file(path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
            detach_file(path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError>;
            read_link(path: &MountPath) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
            read_link_with_work(path: &MountPath, work: &mut super::super::NativeMountWork<'_>) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
            read_range(path: &MountPath, offset: u64, length: u32) -> Result<acyclic_stream::OwnedBytes, MountSourceError>;
            read_range_with_work(path: &MountPath, offset: u64, length: u32, work: &mut super::super::NativeMountWork<'_>) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>;
            seek(path: &MountPath, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError>;
            read_directory(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountDirectoryPage, MountSourceError>;
            read_directory_with_work(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32, work: &mut super::super::NativeMountWork<'_>) -> Result<MountDirectoryPage, MountSourceError>;
            create_file(path: &MountPath, metadata: FileMetadata) -> Result<MountLookup, MountSourceError>;
            create_symbolic_link(path: &MountPath, target: Bytes, metadata: FileMetadata) -> Result<MountLookup, MountSourceError>;
            create_special(path: &MountPath, kind: MountNodeKind, device: Option<(u32, u32)>, metadata: FileMetadata) -> Result<MountLookup, MountSourceError>;
            set_attributes(path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>) -> Result<(), MountSourceError>;
            list_attributes(path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32) -> Result<MountAttributePage, MountSourceError>;
            write_attribute(path: &MountPath, name: &[u8], value: Bytes, mode: MountAttributeWriteMode) -> Result<(), MountSourceError>;
            remove_attribute(path: &MountPath, name: &[u8]) -> Result<(), MountSourceError>;
            write_range(path: &MountPath, offset: u64, bytes: Bytes) -> Result<(), MountSourceError>;
            resize(path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError>;
            allocate_range(path: &MountPath, offset: u64, length: u64, operation: MountRangeAllocation) -> Result<(), MountSourceError>;
            clone_range(source: &MountPath, source_offset: u64, destination: &MountPath, destination_offset: u64, length: u64) -> Result<(), MountSourceError>;
            clone_range_by_id(source_file_id: FileId, source_offset: u64, destination_file_id: FileId, destination_offset: u64, length: u64) -> Result<(), MountSourceError>;
            remove(path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError>;
            rename(source: &MountPath, destination: &MountPath, replace: bool) -> Result<(), MountSourceError>;
            hard_link(source: &MountPath, destination: &MountPath) -> Result<(), MountSourceError>;
            flush() -> Result<(), MountSourceError>;
            capture_host_path(source_root: &std::path::Path, path: &MountPath) -> Result<(), MountSourceError>;
            capture_host_paths(source_root: &std::path::Path, paths: &[MountPath]) -> Result<(), MountSourceError>;
            capture_host_subtree(source_root: &std::path::Path, path: &MountPath) -> Result<(), MountSourceError>;
            capture_host_paths_with_work(source_root: &std::path::Path, paths: &[MountPath], expected_root_identity: crate::NativeRootIdentity, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            capture_host_subtree_with_work(source_root: &std::path::Path, path: &MountPath, expected_root_identity: crate::NativeRootIdentity, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            remove_with_meter(path: &MountPath, expected: Option<FileId>, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            remove_with_work(path: &MountPath, expected: Option<FileId>, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            rename_with_meter(source: &MountPath, destination: &MountPath, replace: bool, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            rename_with_work(source: &MountPath, destination: &MountPath, replace: bool, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            set_attributes_with_meter(path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            set_attributes_with_work(path: &MountPath, metadata: FileMetadata, logical_bytes: Option<u64>, work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            flush_with_meter(work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            flush_with_work(work: &mut super::super::NativeMountWork<'_>) -> Result<(), MountSourceError>;
            open_file_with_meter(path: &MountPath, work: &super::super::NativeMountMeterWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
            open_file_with_lookup_with_meter(path: &MountPath, work: &super::super::NativeMountMeterWork) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError>;
            open_created_with_meter(path: &MountPath, created: &MountLookup, work: &super::super::NativeMountMeterWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
            detach_file_with_meter(path: &MountPath, work: &super::super::NativeMountMeterWork) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>;
            create_file_with_meter(path: &MountPath, metadata: FileMetadata, work: &super::super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
            create_absent_file_with_meter(path: &MountPath, metadata: FileMetadata, stamp: ViewStamp, work: &super::super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
            create_symbolic_link_with_meter(path: &MountPath, target: crate::kernel::OwnedSourceChunk, metadata: FileMetadata, work: &super::super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
            create_special_with_meter(path: &MountPath, kind: MountNodeKind, device: Option<(u32,u32)>, metadata: FileMetadata, work: &super::super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError>;
            list_attributes_with_meter(path: &MountPath, cursor: Option<&[u8]>, maximum: u32, work: &super::super::NativeMountMeterWork) -> Result<(super::super::MountAttributePage, crate::NativeAllocation), MountSourceError>;
            write_attribute_with_meter(path: &MountPath, name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: super::super::MountAttributeWriteMode, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            remove_attribute_with_meter(path: &MountPath, name: &[u8], work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            clone_range_by_id_with_meter(source: FileId, source_offset: u64, destination: FileId, destination_offset: u64, length: u64, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            hard_link_with_meter(source: &MountPath, destination: &MountPath, work: &super::super::NativeMountMeterWork) -> Result<(), MountSourceError>;
            folded_path_with_meter(path: &MountPath, work: &super::super::NativeMountMeterWork) -> Result<Option<(MountPath,u64)>, MountSourceError>;
            view_is_stable_with_meter(work: &super::super::NativeMountMeterWork) -> Result<bool, MountSourceError>;
            view_stamp_with_meter(work: &super::super::NativeMountMeterWork) -> Result<Option<ViewStamp>, MountSourceError>;
            binding_epoch_with_meter(work: &super::super::NativeMountMeterWork) -> Result<Option<u64>, MountSourceError>;
            unchanged_since_with_meter(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp, work: &super::super::NativeMountMeterWork) -> Result<bool, MountSourceError>;
            reported_unchanged_since_with_meter(path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp, work: &super::super::NativeMountMeterWork) -> Result<bool, MountSourceError>;
            shutdown_with_meter(work: &super::super::NativeMountMeterWork) -> Result<bool, MountSourceError>;
            node_unchanged_since_with_meter(file_id: FileId, stamp: ViewStamp, work: &super::super::NativeMountMeterWork) -> Result<bool, MountSourceError>;
            listing_unchanged_since_with_meter(path: &MountPath, stamp: ViewStamp, work: &super::super::NativeMountMeterWork) -> Result<bool, MountSourceError>;
        }

        fn lookup_with_meter(
            &self, path: &MountPath, work: &super::super::NativeMountMeterWork,
        ) -> Result<Option<MountLookup>, MountSourceError> {
            self.record_read(path);
            self.pass(path, &self.gated_lookup);
            self.inner.lookup_with_meter(path, work)
        }
        fn lookup_pinned_with_meter(
            &self, path: &MountPath, work: &super::super::NativeMountMeterWork,
        ) -> Result<Option<(MountLookup, Option<super::super::MountContentPin>)>, MountSourceError> {
            self.record_read(path);
            self.pass(path, &self.gated_lookup);
            self.inner.lookup_pinned_with_meter(path, work)
        }

        fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
            self.record_read(path);
            self.pass(path, &self.gated_lookup);
            self.inner.lookup(path)
        }

        fn create_directory(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
        ) -> Result<MountLookup, MountSourceError> {
            self.pass(path, &self.gated_directory);
            self.inner.create_directory(path, metadata)
        }

        fn read_attribute(
            &self,
            path: &MountPath,
            name: &[u8],
        ) -> Result<Option<acyclic_stream::OwnedBytes>, MountSourceError> {
            self.record_read(path);
            self.inner.read_attribute(path, name)
        }

        fn create_directory_with_meter(&self, path: &MountPath, metadata: FileMetadata, work: &super::super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError> {
            self.pass(path, &self.gated_directory);
            self.inner.create_directory_with_meter(path, metadata, work)
        }

        fn read_attribute_with_meter(&self, path: &MountPath, name: &[u8], work: &super::super::NativeMountMeterWork) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
            self.record_read(path);
            self.inner.read_attribute_with_meter(path, name, work)
        }

    }

    fn finishes_within(limit: Duration, operation: impl FnOnce() + Send + 'static) -> bool {
        let (done, finished) = sync_channel(1);
        std::thread::spawn(move || {
            operation();
            let _ = done.send(());
        });
        finished.recv_timeout(limit).is_ok()
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_callbacks_proceed_while_others_block_in_the_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let (inner, _) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        for directory in ["slow", "busy", "free"] {
            inner.create_directory(&name(directory), FileMetadata::default())?;
        }
        let present = name("free").child(b"present".to_vec());
        inner.create_file(&present, FileMetadata::default())?;
        inner.write_range(&present, 0, Bytes::from_static(b"present"))?;
        let (entered, entries) = sync_channel(4);
        let (release, released) = sync_channel(4);
        let gated = Arc::new(GatedSource {
            inner: Arc::new(inner),
            gated_lookup: b"blocked-lookup".to_vec(),
            gated_directory: b"blocked-mkdir".to_vec(),
            entered,
            released: Mutex::new(released),
            reads: Mutex::new(Vec::new()),
        });
        let temporary = tempfile::tempdir()?;
        let mut session = mount(gated, volume_id, temporary.path())?;
        let root = temporary.path().to_path_buf();

        let slow = root.join("slow").join("blocked-lookup");
        let blocked_lookup = std::thread::spawn(move || slow.exists());
        let busy = root.join("busy").join("blocked-mkdir");
        let blocked_mkdir = std::thread::spawn(move || std::fs::create_dir(busy));
        for _ in 0..2 {
            entries.recv_timeout(Duration::from_secs(5))?;
        }
        let free = root.join("free");
        assert!(
            finishes_within(Duration::from_secs(5), move || {
                assert!(!free.join("absent").exists());
                assert_eq!(
                    std::fs::read(free.join("present")).ok().as_deref(),
                    Some(b"present".as_slice())
                );
                assert!(listing(&free).is_ok_and(|names| names == ["present"]));
            }),
            "a callback waited on another callback's source work"
        );
        for _ in 0..2 {
            release.send(())?;
        }
        assert!(!blocked_lookup.join().map_err(|_| "lookup panicked")?);
        blocked_mkdir.join().map_err(|_| "mkdir panicked")??;
        assert!(session.stop()?);
        Ok(())
    }
}
