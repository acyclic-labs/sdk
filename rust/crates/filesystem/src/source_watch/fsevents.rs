//! macOS notifications from one FSEvents stream over a held source root.
#![allow(unsafe_code, reason = "uses the documented fcntl and fstat contracts")]

use super::{Delivery, HostChange, arc_control_bytes};
use crate::demand::{DemandError, NativeSourceWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::fsevents::{EventBatch, EventStream, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED,
    KERNEL_DROPPED, MOUNT, MUST_SCAN_SUBDIRS, UNMOUNT, USER_DROPPED};
use crate::{CancellationToken, WorkCounters, WorkError};
use std::ffi::{CStr, OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::time::Duration;

const ITEM_FLAGS: u32 = 0x00ff_ff00;

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    stream: Mutex<Option<EventStream>>,
}

struct Shared {
    root: PathBuf,
    held: (u64, u64),
    delivery: Arc<Delivery>,
    stream_bytes: AtomicU64,
    completed: AtomicBool,
    ownership: SourcePayloadOwnership,
}

impl NativeSourceWatch {
    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let _failure = delivery.prepare_startup_failure(work)?;
        let ownership = SourcePayloadOwnership::new(work)?;
        let mut ordinary_bytes = 0;
        let result = (|| {
            let root = held_path(directory.directory_fd(), work)?;
            ordinary_bytes = root.capacity() as u64;
            ownership.retain(ordinary_bytes, work)?;
            work.charge(WorkCounters { backend_read_operations: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let held = identity(&std::fs::symlink_metadata(&root)?);
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: fstat initializes stat on success and borrows the held descriptor.
            if unsafe { libc::fstat(directory.directory_fd(), stat.as_mut_ptr()) } < 0 { return Err(io::Error::last_os_error().into()); }
            // SAFETY: fstat succeeded.
            let stat = unsafe { stat.assume_init() };
            if held != (stat.st_dev as u64, stat.st_ino) { return Err(DemandError::SourceUnavailable); }
            let bytes = arc_control_bytes::<Shared>();
            work.allocate(bytes)?;
            ordinary_bytes += bytes;
            ownership.retain(bytes, work)?;
            let shared = Arc::new(Shared { root, held, delivery, ownership,
                stream_bytes: AtomicU64::new(0), completed: AtomicBool::new(false) });
            let bytes = arc_control_bytes::<Arc<Shared>>();
            work.allocate(bytes)?;
            ordinary_bytes += bytes;
            shared.ownership.retain(bytes, work)?;
            let reader = shared.clone();
            let handler = Arc::new(move |events: EventBatch<'_>| {
                let mut ordinary = None;
                let mut work = reader.delivery.callback_work(&mut ordinary);
                if let Err(error) = reader.events(events.iter(), &mut work) { reader.delivery.failed_with_work(error, &mut work); }
            });
            let mut stream = EventStream::start_borrowed_with_work(
                std::slice::from_ref(&shared.root), handler, Some(&shared.stream_bytes), work,
            )?;
            stream.set_terminal(shared.clone());
            Ok(Self { shared, stream: Mutex::new(Some(stream)) })
        })();
        if !work.is_native_original() { work.release(ordinary_bytes)?; }
        result
    }

    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(&self, _: Duration, work: &mut W) -> Result<bool, DemandError> {
        work.check()?;
        self.shared.delivery.take_failure(work)?;
        work.charge(WorkCounters { backend_read_operations: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if std::fs::symlink_metadata(&self.shared.root).is_ok_and(|metadata| identity(&metadata) == self.shared.held) {
            self.delivery().deliver_with_work(&[HostChange::Unconfirmed], work)?;
        } else if self.delivery().exact.swap(false, Ordering::AcqRel) {
            self.delivery().deliver_with_work(&[HostChange::Everything], work)?;
        }
        Ok(false)
    }

    pub(crate) fn fence(&self) -> io::Result<()> {
        let mut ordinary = None;
        let mut work = self.delivery().callback_work(&mut ordinary);
        self.fence_with_work(Duration::ZERO, &mut work).map(|_| ()).map_err(crate::demand::native_demand_read_error)
    }

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        self.stop(work, None, false)
    }

    pub(crate) fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        let birth = self.delivery().original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        self.stop(&mut NativeSourceWork::from_original_work(birth), Some(wait), true)
    }

    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        let birth = self.delivery().original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        self.stop(&mut NativeSourceWork::from_original_work(birth), None, true)
    }

    fn stop<W: SourceDemandReadWork>(&self, work: &mut W, wait: Option<&CancellationToken>, prepaid: bool) -> Result<(), DemandError> {
        if !prepaid { work.check()?; }
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        self.delivery().ensure_stop_available()?;
        let mut owner = if wait.is_some() {
            match self.stream.try_lock() {
                Ok(owner) => owner,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
            }
        } else { self.stream.lock().unwrap_or_else(PoisonError::into_inner) };
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        let Some(stream) = owner.as_mut() else {
            return if self.is_stopped() { Ok(()) } else { Err(DemandError::WorkerUnavailable) };
        };
        let result = if prepaid {
            stream.ensure_cleanup_wait_available(work)?;
            stream.cleanup_admitted(work)
        } else { stream.stop_with_work(work) };
        let bytes = stream.logical_owned_bytes();
        if stream.is_stopped() {
            *owner = None;
            self.shared.stream_bytes.store(bytes, Ordering::Release);
            self.delivery().exact.store(false, Ordering::Release);
        }
        if prepaid && !self.is_stopped() { return result.and(Err(DemandError::WorkerUnavailable)); }
        match wait { Some(wait) => result.and(wait.check().map_err(|_| DemandError::Cancelled)), None => result }
    }

    pub(crate) fn is_stopped(&self) -> bool { self.shared.completed.load(Ordering::Acquire) }
    pub(crate) fn platform_owned_bytes(&self) -> u64 {
        let stream = self.shared.stream_bytes.load(Ordering::Acquire);
        arc_control_bytes::<Shared>() + self.shared.root.capacity() as u64 + stream
            + if stream == 0 { 0 } else { arc_control_bytes::<Arc<Shared>>() }
    }
    pub(crate) fn delivery(&self) -> &Delivery { &self.shared.delivery }

    #[cfg(test)]
    pub(crate) fn inject(&self, path: &Path, flags: u32) {
        let mut ordinary = None;
        let mut work = self.delivery().callback_work(&mut ordinary);
        if let Err(error) = self.shared.events(std::iter::once((path, flags)), &mut work) { self.delivery().failed_with_work(error, &mut work); }
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if self.is_stopped() { return; }
        let mut ordinary = None;
        let mut work = self.shared.delivery.callback_work(&mut ordinary);
        let owner = self.stream.get_mut().unwrap_or_else(PoisonError::into_inner);
        if let Some(stream) = owner.as_mut() { let _ = stream.cleanup_admitted(&mut work); }
        *owner = None;
    }
}

impl Shared {
    fn events<'a, W: SourceDemandReadWork>(&self, events: impl ExactSizeIterator<Item = (&'a Path, u32)>, work: &mut W) -> Result<(), DemandError> {
        let count = events.len() as u64;
        let slots = count.checked_mul(size_of::<HostChange>() as u64).ok_or(WorkError::Overflow)?;
        work.check()?;
        work.admit(WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED })?;
        work.with_original_leaf(|work| work.reserve(crate::materializer::NativeResourceCredit::Items, count))?;
        if let Err(error) = work.allocate(slots) {
            work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, count).expect("balanced event reservation"));
            return Err(error);
        }
        let mut changes = Vec::with_capacity(count as usize);
        let mut payload = 0_u64;
        let result = (|| {
            for (path, flags) in events {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                if flags & (USER_DROPPED | KERNEL_DROPPED | MOUNT | UNMOUNT) != 0 { changes.push(HostChange::Everything); }
                else if let Some(relative) = relative_path(path, &self.root, work)? {
                    let bytes = relative.as_os_str().as_bytes().len() as u64;
                    let copy = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
                    let retained = payload.checked_add(bytes).ok_or(WorkError::Overflow)?;
                    work.admit(copy)?;
                    work.allocate(bytes)?;
                    payload = retained;
                    changes.push(change(relative.to_path_buf(), flags));
                    work.record(copy)?;
                }
            }
            self.delivery.deliver_with_work(&changes, work)
        })();
        let returned = changes.len() as u64;
        drop(changes);
        let cleaned = work.with_original_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::Items, returned, returned));
        work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, count - returned).expect("balanced event reservation"));
        work.release(payload + slots)?;
        result.and(cleaned)
    }
}

impl crate::fsevents::StreamTerminal for Shared {
    fn stream_stopped(&self) {
        if let Err(error) = self.ownership.retire(arc_control_bytes::<Arc<Shared>>()) {
            let mut ordinary = None;
            let mut work = self.delivery.callback_work(&mut ordinary);
            self.delivery.failed_with_work(error, &mut work);
        }
        self.stream_bytes.store(0, Ordering::Release);
        self.delivery.exact.store(false, Ordering::Release);
        self.completed.store(true, Ordering::Release);
    }
}

fn relative_path<'a, W: SourceDemandReadWork>(path: &'a Path, root: &Path, work: &mut W) -> Result<Option<&'a Path>, DemandError> {
    let mut path = path.components();
    for expected in root.components() {
        work.charge(WorkCounters { source_path_components: 1, ..WorkCounters::UNCHARGED })?;
        if path.next() != Some(expected) { return Ok(None); }
    }
    Ok(Some(path.as_path()))
}

fn change(relative: PathBuf, flags: u32) -> HostChange {
    let unspecific = flags & ITEM_FLAGS == 0 || flags & MUST_SCAN_SUBDIRS != 0;
    if relative.as_os_str().is_empty() {
        return if unspecific {
            HostChange::Everything
        } else {
            HostChange::Altered(relative)
        };
    }
    if unspecific || flags & (ITEM_CREATED | ITEM_REMOVED | ITEM_RENAMED) != 0 {
        HostChange::Rebound(relative)
    } else {
        HostChange::Altered(relative)
    }
}

fn identity(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    (metadata.dev(), metadata.ino())
}


fn held_path<W: SourceDemandReadWork>(descriptor: i32, work: &mut W) -> Result<PathBuf, DemandError> {
    let mut buffer = [0_u8; libc::PATH_MAX as usize];
    work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    // SAFETY: F_GETPATH writes at most PATH_MAX bytes into this live array.
    if unsafe { libc::fcntl(descriptor, libc::F_GETPATH, buffer.as_mut_ptr()) } < 0 { return Err(io::Error::last_os_error().into()); }
    let bytes = CStr::from_bytes_until_nul(&buffer).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?.to_bytes();
    let mut copy = work.reserve_source_copy(bytes.len() as u64)?;
    let mut allocation = work.claim(bytes.len() as u64)?;
    let mut path = OsString::new();
    let result = (|| {
        let reserved = path.try_reserve_exact(bytes.len());
        let reconciled = work.reconcile_allocation(&mut allocation, path.capacity() as u64);
        reserved.map_err(DemandError::Allocation)?;
        reconciled?;
        path.push(OsStr::from_bytes(bytes));
        let copied = WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED };
        match copy.take() {
            Some(mut copy) => copy.record_completed(copied).map_err(crate::native_name::original_name_error)?,
            None => work.record(copied)?,
        }
        work.check()
    })();
    if let Err(error) = result { drop(path); work.release(allocation)?; return Err(error); }
    Ok(path.into())
}

#[cfg(test)]
#[path = "fsevents/tests.rs"]
mod tests;
