//! Native source change notifications.

#![allow(
    unsafe_code,
    reason = "fcntl is a C API; each call documents its contract"
)]

use super::{Delivery, HostChange};
use crate::fsevents::{
    EventBatch, EventStream, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, KERNEL_DROPPED, MOUNT,
    MUST_SCAN_SUBDIRS, UNMOUNT, USER_DROPPED,
};
use crate::demand::{DemandWork, SourceDemandReadWork};
use crate::{DemandError, WorkBudget, WorkCounters};
use std::ffi::CStr;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Every flag that says what changed about an item; an event with none of them says only that something at its path did.
const ITEM_FLAGS: u32 = 0x00ff_ff00;

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    stream: Mutex<Option<EventStream>>,
}

struct Shared {
    /// The held root's real path, which the stream reports paths under.
    root: PathBuf,
    /// The held root's device and inode.
    held: (u64, u64),
    /// The original stream's payload, published before native delivery starts.
    stream_bytes: AtomicU64,
    /// Published only after successful native quiescence and resource drop.
    completed: AtomicBool,
    delivery: Arc<Delivery>,
    ownership: crate::demand::SourcePayloadOwnership,
}

impl NativeSourceWatch {
    pub(crate) fn start_platform(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::start_driver(directory, delivery, &mut work)
            .map_err(crate::demand::native_demand_read_error)
    }

    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let _carrier = delivery.prepare_startup_failure(work)?;
        let mut ordinary_owned_bytes = 0_u64;
        let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
        let result = (|| {
            let root = held_path(directory.directory_fd(), work)?;
            let root_bytes = root.capacity() as u64;
            ordinary_owned_bytes = root_bytes;
            ownership.retain(root_bytes, work)?;
            work.charge(WorkCounters {
                backend_read_operations: 1, items_examined: 1,
                ..WorkCounters::default()
            })?;
            let held = identity(&std::fs::symlink_metadata(&root).map_err(DemandError::Io)?);
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: fstat initializes the complete stat structure on success.
            if unsafe { libc::fstat(directory.directory_fd(), metadata.as_mut_ptr()) } < 0 {
                return Err(DemandError::Io(io::Error::last_os_error()));
            }
            // SAFETY: the successful fstat initialized metadata.
            let metadata = unsafe { metadata.assume_init() };
            if held != (metadata.st_dev as u64, metadata.st_ino) {
                return Err(DemandError::SourceUnavailable);
            }
            work.allocate(super::arc_control_bytes::<Shared>())?;
            ordinary_owned_bytes += super::arc_control_bytes::<Shared>();
            ownership.retain(super::arc_control_bytes::<Shared>(), work)?;
            let shared = Arc::new(Shared {
                root, held, stream_bytes: AtomicU64::new(0),
                completed: AtomicBool::new(false), delivery, ownership,
            });
            let reader = Arc::clone(&shared);
            // The actual closure owns one Arc pointer plus its canonical header.
            work.allocate(super::arc_control_bytes::<Arc<Shared>>())?;
            ordinary_owned_bytes += super::arc_control_bytes::<Arc<Shared>>();
            shared.ownership.retain(super::arc_control_bytes::<Arc<Shared>>(), work)?;
            let handler = Arc::new(move |events: EventBatch<'_>| {
                let mut callback = reader.delivery.callback_work();
                if let Err(error) = reader.events(events.iter(), &mut callback) {
                    reader.delivery.failed_with_work(error, &mut callback);
                }
            });
            let mut stream = EventStream::start_borrowed_with_work(
                std::slice::from_ref(&shared.root), handler, Some(&shared.stream_bytes), work,
            )?;
            stream.set_terminal(Arc::clone(&shared) as Arc<dyn crate::fsevents::StreamTerminal>);
            if !work.is_native_original() {
                work.release(ordinary_owned_bytes)?;
                ordinary_owned_bytes = 0;
            }
            Ok(Self { shared, stream: Mutex::new(Some(stream)) })
        })();
        if result.is_err() {
            if !work.is_native_original() { work.release(ordinary_owned_bytes)?; }
        }
        result
    }

    /// Checks the original held-root identity, without pretending that the event daemon's delayed delivery is an ordered native barrier.

    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self, _timeout: Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        let shared = &self.shared;
        shared.delivery.take_failure(work)?;
        work.charge(WorkCounters {
            backend_read_operations: 1, items_examined: 1,
            ..WorkCounters::default()
        })?;
        let named = std::fs::symlink_metadata(&shared.root)
            .is_ok_and(|metadata| identity(&metadata) == shared.held);
        if named {
            shared.delivery.deliver_with_work(&[HostChange::Unconfirmed], work)?;
        } else if shared.delivery.exact.swap(false, std::sync::atomic::Ordering::AcqRel) {
            shared.delivery.deliver_with_work(&[HostChange::Everything], work)?;
        }
        Ok(false)
    }

    pub(crate) fn fence(&self) -> io::Result<()> {
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        self.fence_with_work(Duration::ZERO, &mut work)
            .map(|_| ())
            .map_err(crate::demand::native_demand_read_error)
    }

    /// Stops the original stream and waits for its last native callback.

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        self.shared.delivery.ensure_stop_available()?;
        let mut custody = self.stream.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(stream) = custody.as_mut() else {
            return if self.is_stopped() { Ok(()) } else { Err(DemandError::WorkerUnavailable) };
        };
        let result = stream.stop_with_work(work);
        let stream_bytes = stream.logical_owned_bytes();
        if stream.is_stopped() {
            *custody = None;
            self.shared.stream_bytes.store(stream_bytes, Ordering::Release);
            self.shared.delivery.exact.store(false, std::sync::atomic::Ordering::Release);
        }
        result
    }

    /// Uses only the actual stream's original birth-paid cleanup reservation.
    pub(crate) fn stop_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        let birth = self.shared.delivery.original.as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(birth);
        self.stop_cleanup_wait_with_work(&mut work, Some(wait))
    }

    /// Completes only this original stream's accepted before-birth suffix.
    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        let birth = self.shared.delivery.original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(birth);
        self.stop_cleanup_wait_with_work(&mut work, None)
    }

    fn stop_cleanup_wait_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), DemandError> {
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        self.shared.delivery.ensure_stop_available()?;
        let mut custody = if wait.is_some() {
            match self.stream.try_lock() {
                Ok(custody) => custody,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
            }
        } else { self.stream.lock().unwrap_or_else(std::sync::PoisonError::into_inner) };
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        let Some(stream) = custody.as_mut() else {
            return if self.is_stopped() { Ok(()) } else { Err(DemandError::WorkerUnavailable) };
        };
        stream.ensure_cleanup_wait_available(work)?;
        let result = stream.cleanup_admitted(work);
        let stream_bytes = stream.logical_owned_bytes();
        if stream.is_stopped() {
            *custody = None;
            self.shared.stream_bytes.store(stream_bytes, Ordering::Release);
            self.shared.delivery.exact.store(false, Ordering::Release);
        }
        if !self.is_stopped() {
            return result.and(Err(DemandError::WorkerUnavailable));
        }
        match wait {
            Some(wait) => result.and(wait.check().map_err(|_| DemandError::Cancelled)),
            None => result,
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.shared.completed.load(Ordering::Acquire)
    }

    pub(crate) fn platform_owned_bytes(&self) -> u64 {
        let stream_bytes = self.shared.stream_bytes.load(Ordering::Acquire);
        super::arc_control_bytes::<Shared>() + self.shared.root.capacity() as u64 + stream_bytes
            + if stream_bytes == 0 { 0 } else { super::arc_control_bytes::<Arc<Shared>>() }
    }

    pub(crate) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Delivers one event as the stream would.
    #[cfg(test)]
    pub(crate) fn inject(&self, path: &std::path::Path, flags: u32) {
        let mut work = self.shared.delivery.callback_work();
        if let Err(error) = self.shared.events(
            std::iter::once((path, flags)), &mut work,
        ) {
            self.shared.delivery.failed_with_work(error, &mut work);
        }
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if self.is_stopped() { return; }
        let mut work = self.shared.delivery.callback_work();
        let custody = self.stream.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(stream) = custody.as_mut() {
            let _ = stream.cleanup_admitted(&mut work);
        }
        *custody = None;
    }
}

impl Shared {
    fn events<'a, W: SourceDemandReadWork>(
        &self, events: impl ExactSizeIterator<Item = (&'a std::path::Path, u32)>, work: &mut W,
    ) -> Result<(), DemandError> {
        let count = events.len();
        let slots = (count as u64).checked_mul(size_of::<HostChange>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        let cleanup = u64::try_from(count).map_err(|_| crate::WorkError::Overflow)?;
        work.check()?;
        work.admit(WorkCounters {
            items_examined: cleanup, ..WorkCounters::default()
        })?;
        work.with_original_leaf(|work| work.reserve(crate::materializer::NativeResourceCredit::Items, cleanup))?;
        if let Err(error) = work.allocate(slots) {
            work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, cleanup).expect("original reservation is balanced"));
            return Err(error);
        }
        let mut changes = Vec::with_capacity(count);
        let mut cleanup_remaining = cleanup;
        let mut payload = 0_u64;
        let result = (|| {
            for (path, flags) in events {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if flags & (USER_DROPPED | KERNEL_DROPPED | MOUNT | UNMOUNT) != 0 {
                    changes.push(HostChange::Everything);
                } else if let Some(relative) = relative_path(path, &self.root, work)? {
                    let owned = relative.as_os_str().as_bytes().len() as u64;
                    let copy = WorkCounters { bytes_copied: owned, ..WorkCounters::default() };
                    work.admit(copy)?;
                    let retained = payload.checked_add(owned).ok_or(crate::WorkError::Overflow)?;
                    work.allocate(owned)?;
                    payload = retained;
                    let relative = relative.to_path_buf();
                    changes.push(change(relative, flags));
                    work.record(copy)?;
                }
            }
            self.delivery.deliver_with_work(&changes, work)
        })();
        let mut cleaned = Ok(());
        while let Some(change) = changes.pop() {
            cleanup_remaining -= 1;
            drop(change);
            cleaned = cleaned.and(work.with_original_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::Items, 1, 1)));
        }
        work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, cleanup_remaining).expect("original reservation is balanced"));
        drop(changes);
        work.release(payload + slots)?;
        result.and(cleaned)
    }
}

impl crate::fsevents::StreamTerminal for Shared {
    fn stream_stopped(&self) {
        let handler_bytes = super::arc_control_bytes::<Arc<Shared>>();
        if let Err(error) = self.ownership.retire(handler_bytes) {
            let mut work = self.delivery.callback_work();
            self.delivery.failed_with_work(error, &mut work);
        }
        self.stream_bytes.store(0, Ordering::Release);
        self.delivery.exact.store(false, Ordering::Release);
        self.completed.store(true, Ordering::Release);
    }
}

/// The same component-prefix semantics as Path::strip_prefix, admitting each actual root/path comparison instead of charging an opaque traversal.
fn relative_path<'a, W: SourceDemandReadWork>(
    path: &'a std::path::Path, root: &std::path::Path, work: &mut W,
) -> Result<Option<&'a std::path::Path>, DemandError> {
    let mut path = path.components();
    let mut root = root.components();
    while !root.as_path().as_os_str().is_empty() {
        work.charge(WorkCounters { source_path_components: 1, ..WorkCounters::default() })?;
        let component = root.next().ok_or(DemandError::InvalidRequest)?;
        if path.next() != Some(component) {
            return Ok(None);
        }
    }
    Ok(Some(path.as_path()))
}

/// What one event says of the path it names.
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

/// The real path of a held directory, as `FSEvents` reports paths.
fn held_path<W: SourceDemandReadWork>(descriptor: i32, work: &mut W) -> Result<PathBuf, DemandError> {
    use crate::native_host::NativeHostWork as _;
    // F_GETPATH's fixed native bound requires no request-owned heap buffer.
    let mut buffer = [0_u8; libc::PATH_MAX as usize];
    work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    // SAFETY: F_GETPATH writes at most PATH_MAX bytes into this live array.
    if unsafe { libc::fcntl(descriptor, libc::F_GETPATH, buffer.as_mut_ptr()) } < 0 {
        return Err(DemandError::Io(io::Error::last_os_error()));
    }
    let borrowed = CStr::from_bytes_until_nul(&buffer)
        .map_err(|_| DemandError::Io(io::Error::from(io::ErrorKind::InvalidData)))?;
    let bytes = borrowed.to_bytes();
    let mut copy = work.reserve_source_copy(bytes.len() as u64)?;
    let mut allocation = work.claim(bytes.len() as u64)?;
    let mut owned = std::ffi::OsString::new();
    let result = (|| {
        let reserved = owned.try_reserve_exact(bytes.len());
        let reconciled = work.reconcile_allocation(&mut allocation, owned.capacity() as u64);
        reserved.map_err(DemandError::Allocation)?;
        reconciled?;
        owned.push(std::ffi::OsStr::from_bytes(bytes));
        if let Some(copy) = copy.take() {
            { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED }) }.map_err(crate::native_name::original_name_error)?;
        } else {
            work.record(WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED })?;
        }
        work.check()
    })();
    if let Err(error) = result {
        drop(owned);
        work.release(allocation)?;
        return Err(error);
    }
    Ok(owned.into())
}

#[cfg(test)]
mod tests {
    use super::{HostChange, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, MUST_SCAN_SUBDIRS, change};
    use std::path::PathBuf;

    const ITEM_MODIFIED: u32 = 0x1000;
    const ITEM_IS_FILE: u32 = 0x1_0000;
    const ITEM_IS_DIR: u32 = 0x2_0000;

    struct Ignored;
    impl super::super::HostChangeSink for Ignored {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(
            &self, _: &[HostChange], _: &mut super::DemandWork,
        ) -> Result<(), crate::DemandError> { Ok(()) }
    }

    fn finite_budget() -> crate::WorkBudget {
        crate::WorkBudget {
            backend_read_operations: 32,
            backend_write_operations: 32,
            source_entries_visited: 2,
            source_path_components: 1024,
            items_examined: 1024,
            bytes_copied: 1024 * 1024,
            allocation_operations: 16,
            peak_allocation_bytes: 1024 * 1024,
            ..crate::WorkBudget::default()
        }
    }

    struct Counted(std::sync::atomic::AtomicUsize);
    impl super::super::HostChangeSink for Counted {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(
            &self, changes: &[HostChange], work: &mut super::DemandWork,
        ) -> Result<(), crate::DemandError> {
            work.charge(crate::WorkCounters {
                items_returned: changes.len() as u64, ..crate::WorkCounters::default()
            })?;
            assert!(matches!(&changes[0], HostChange::Rebound(path) if path == std::path::Path::new("a/b")));
            self.0.fetch_add(changes.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn callback_delivers_admitted_paths_and_retains_denied_copy_failures()
        -> Result<(), Box<dyn std::error::Error>>
    {
        for bytes_copied in [0, finite_budget().bytes_copied] {
            let mut budget = finite_budget();
            budget.items_returned = 1;
            budget.bytes_copied = bytes_copied;
            let cancellation = crate::cancellation::CancellationToken::new();
            let original = super::DemandWork::new(budget, &cancellation);
            let sink = std::sync::Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
            let delivery = std::sync::Arc::new(super::super::Delivery { budget: original.budget, cancellation: original.cancellation().clone(), ..super::super::Delivery::new(sink.clone()) });
            let shared = super::Shared {
                root: PathBuf::from("/root"), held: (0, 0),
                stream_bytes: super::AtomicU64::new(0), delivery,
                completed: super::AtomicBool::new(false),
                ownership: crate::demand::SourcePayloadOwnership::default(),
            };
            let mut callback = shared.delivery.callback_work();
            let result = shared.events(
                std::iter::once((std::path::Path::new("/root/a/b"), ITEM_CREATED)), &mut callback,
            );
            if bytes_copied == 0 {
                let error = result.err().ok_or("callback copied a denied path")?;
                assert!(matches!(error, crate::DemandError::Work(_)));
                assert_eq!(sink.0.load(std::sync::atomic::Ordering::Relaxed), 0);
                assert_eq!(callback.receipt_work().bytes_copied, 0);
                shared.delivery.failed_with_work(error, &mut callback);
                let mut next = super::DemandWork::new(finite_budget(), &cancellation);
                assert!(matches!(shared.delivery.take_failure(&mut next), Err(crate::DemandError::Work(_))));
                assert!(!shared.delivery.is_exact());
            } else {
                result?;
                assert_eq!(sink.0.load(std::sync::atomic::Ordering::Relaxed), 1);
                assert_eq!(callback.receipt_work().items_returned, 1);
            }
            assert_eq!(crate::demand::SourceDemandReadWork::with_original_leaf(&mut callback, |work| work.live_allocation_bytes()), 0);
        }
        Ok(())
    }

    #[test]
    fn callback_keeps_the_original_subscription_cancellation()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let cancellation = crate::cancellation::CancellationToken::new();
        let original = super::DemandWork::new(finite_budget(), &cancellation);
        let delivery = std::sync::Arc::new(super::super::Delivery { budget: original.budget, cancellation: original.cancellation().clone(), ..super::super::Delivery::new(std::sync::Arc::new(Ignored)) });
        let shared = super::Shared {
            root: PathBuf::from("/root"), held: (0, 0),
            stream_bytes: super::AtomicU64::new(0), delivery,
            completed: super::AtomicBool::new(false),
            ownership: crate::demand::SourcePayloadOwnership::default(),
        };
        cancellation.cancel();
        let mut callback = shared.delivery.callback_work();
        assert!(matches!(shared.events(
            std::iter::once((std::path::Path::new("/root/a/b"), ITEM_CREATED)), &mut callback,
        ), Err(crate::DemandError::Cancelled)));
        assert_eq!(callback.receipt_work(), crate::WorkCounters::default());
        assert_eq!(crate::demand::SourceDemandReadWork::with_original_leaf(&mut callback, |work| work.live_allocation_bytes()), 0);
        Ok(())
    }

    #[test]
    fn stop_denial_and_precancellation_preserve_the_original_stream_for_retry()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let watch = super::NativeSourceWatch::start_platform(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )),
        )?;
        let owned = watch.platform_owned_bytes();
        assert!(!watch.is_stopped());
        assert!(matches!(
            watch.stop_with_cleanup_wait(&crate::CancellationToken::new()),
            Err(crate::DemandError::NativeWorkUnavailable),
        ));
        assert!(matches!(watch.stop_accepted_cleanup(), Err(crate::DemandError::NativeWorkUnavailable)));
        assert_eq!(watch.platform_owned_bytes(), owned);
        for (writes, reads) in [(3, 1), (4, 0)] {
            let mut work = super::DemandWork::new(crate::WorkBudget {
                backend_write_operations: writes, backend_read_operations: reads,
                ..crate::WorkBudget::default()
            }, &crate::cancellation::CancellationToken::new());
            assert!(matches!(watch.stop_with_work(&mut work), Err(crate::DemandError::Work(_))));
            assert_eq!(work.work, crate::WorkCounters::default());
            assert!(!watch.is_stopped());
            assert!(watch.delivery().is_exact());
            assert_eq!(watch.platform_owned_bytes(), owned);
            assert!(watch.stream.lock().map_err(|_| "poisoned stream")?.is_some());
        }
        let cancellation = crate::cancellation::CancellationToken::new();
        cancellation.cancel();
        let stop_budget = crate::WorkBudget {
            backend_write_operations: 4, backend_read_operations: 1,
            ..crate::WorkBudget::default()
        };
        let mut cancelled = super::DemandWork::new(stop_budget, &cancellation);
        assert!(matches!(watch.stop_with_work(&mut cancelled), Err(crate::DemandError::Cancelled)));
        assert_eq!(cancelled.work, crate::WorkCounters::default());
        assert!(!watch.is_stopped());
        assert!(watch.delivery().is_exact());
        assert_eq!(watch.platform_owned_bytes(), owned);

        let mut retry = super::DemandWork::new(
            stop_budget, &crate::cancellation::CancellationToken::new(),
        );
        watch.stop_with_work(&mut retry)?;
        assert!(watch.is_stopped());
        assert_eq!(retry.work.allocation_operations, 0);
        assert_eq!(retry.work.peak_allocation_bytes, 0);
        assert_eq!(retry.work.items_examined, 0);
        assert!(!watch.delivery().is_exact());
        assert!(watch.platform_owned_bytes() < owned);
        assert!(watch.stream.lock().map_err(|_| "poisoned stream")?.is_none());
        let mut again = super::DemandWork::new(
            crate::WorkBudget::default(), &crate::cancellation::CancellationToken::new(),
        );
        watch.stop_with_work(&mut again)?;
        assert!(watch.is_stopped());
        assert_eq!(again.work, crate::WorkCounters::default());
        drop(watch);
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn cleanup_wait_preserves_cancelled_birth_and_finishes_late_cancellation()
        -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::demand::{DemandWork, NativeDemandReadMeter, NativeSourceWork, SourceDemandReadWork};
        use crate::native_host::NativeHostWork;
        use crate::{CancellationToken, DemandError, WorkBudget, WorkCounters};
        use std::sync::{Arc, Mutex};

        struct LateFault<'a> {
            inner: NativeSourceWork<'a>,
            cancellation: &'a CancellationToken,
            triggered: bool,
        }
        impl NativeHostWork for LateFault<'_> {
            type Error = DemandError;
            type Allocation = u64;
            type CleanupWrite = u64;
            fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
                <DemandWork as NativeHostWork>::io_error_kind(error)
            }
            fn allocation_failed(error: std::collections::TryReserveError) -> DemandError { DemandError::Allocation(error) }
            fn check(&self) -> Result<(), DemandError> { self.inner.check() }
            fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.admit(delta) }
            fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.started_io(delta) }
            fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
                let result = self.inner.record(delta);
                if !self.triggered && delta.backend_write_operations != 0 {
                    self.triggered = true;
                    self.cancellation.cancel();
                }
                result
            }
            fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> { self.inner.claim(bytes) }
            fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> { self.inner.reserve_opaque_decode(maximum) }
            fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), DemandError> { self.inner.reconcile_allocation(allocation, actual) }
            fn release(&mut self, bytes: u64) -> Result<(), DemandError> { self.inner.release(bytes) }
            fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> { self.inner.reserve_cleanup_write() }
            fn cancel_cleanup_write(&mut self, reservation: u64) { self.inner.cancel_cleanup_write(reservation); }
            fn attempt_cleanup_write<T>(&mut self, reservation: u64, operation: impl FnOnce() -> T)
                -> (T, Result<(), DemandError>) {
                self.inner.attempt_cleanup_write(reservation, operation)
            }
        }
        impl SourceDemandReadWork for LateFault<'_> {
            fn with_native_read_meter<T>(
                &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
            ) -> T { self.inner.with_native_read_meter(operation) }
            fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
                let before = self.inner.original.with_original_leaf(|work| work.work.backend_write_operations);
                let result = self.inner.with_original_leaf(operation);
                let after = self.inner.original.with_original_leaf(|work| work.work.backend_write_operations);
                if !self.triggered && after > before {
                    self.triggered = true;
                    self.cancellation.cancel();
                }
                result
            }
            fn is_native_original(&self) -> bool { true }
            fn native_source_work(&self) -> Option<NativeSourceWork<'_>> { Some(self.inner) }
        }

        crate::fsevents::EventStream::initialize_services()?;
        for late_wait in [false, true] {
            let root = tempfile::tempdir()?;
            let held = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
            let root_cancellation = CancellationToken::new();
            let mut budget = finite_budget();
            budget.backend_read_operations = 128;
            budget.backend_write_operations = 128;
            budget.allocation_operations = 128;
            budget.items_examined = 4096;
            let birth = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &root_cancellation))));
            let mut startup = NativeSourceWork::new(&birth);
            let delivery = Arc::new(super::super::Delivery::new_with_work(Arc::new(Ignored), &mut startup)?);
            let watch = super::NativeSourceWatch::start_driver(&held, delivery, &mut startup)?;
            let owned = watch.platform_owned_bytes();
            let before = birth.with_original_leaf(|work| work.work);
            let held_budget = birth.with_original_leaf(|work| work.budget);
            let declined = CancellationToken::new();
            declined.cancel();
            assert!(matches!(watch.stop_with_cleanup_wait(&declined), Err(DemandError::Cancelled)));
            assert!(!watch.is_stopped());
            assert!(watch.delivery().is_exact());
            assert_eq!(watch.platform_owned_bytes(), owned);
            assert_eq!(birth.with_original_leaf(|work| (work.work, work.budget)), (before, held_budget));
            let wait = CancellationToken::new();
            let custody = watch.stream.lock().map_err(|_| "poisoned actual stream custody")?;
            assert!(matches!(watch.stop_with_cleanup_wait(&wait), Err(DemandError::WorkerUnavailable)));
            drop(custody);
            assert_eq!(birth.with_original_leaf(|work| work.work), before);
            root_cancellation.cancel();
            assert!(matches!(watch.stop_with_work(&mut NativeSourceWork::new(&birth)), Err(DemandError::Cancelled)));
            if late_wait {
                let mut cleanup = LateFault {
                    inner: NativeSourceWork::new(&birth), cancellation: &wait,
                    triggered: false,
                };
                assert!(matches!(watch.stop_cleanup_wait_with_work(&mut cleanup, Some(&wait)), Err(DemandError::Cancelled)));
                assert!(cleanup.triggered, "wait cancellation follows the actual original stream stop");
            } else {
                watch.stop_with_cleanup_wait(&wait)?;
            }
            assert!(watch.is_stopped(), "terminal follows native serial queue drain and context release");
            assert_eq!(watch.shared.stream_bytes.load(super::Ordering::Acquire), 0);
            assert!(watch.stream.lock().map_err(|_| "poisoned retired stream custody")?.is_none());
            assert!(watch.platform_owned_bytes() < owned);
            let stopped = birth.with_original_leaf(|work| {
                assert_eq!(work.work.allocation_operations, before.allocation_operations);
                assert_eq!(work.work.peak_allocation_bytes, before.peak_allocation_bytes);
                assert_eq!(work.budget, budget);
                assert!(work.cancellation().is_cancelled());
                assert!(matches!(work.check(), Err(DemandError::Cancelled)));
                work.work
            });
            watch.stop_with_cleanup_wait(&CancellationToken::new())?;
            watch.stop_accepted_cleanup()?;
            drop(watch);
            assert_eq!(birth.with_original_leaf(|work| work.work), stopped);
        }
        Ok(())
    }

    #[test]
    fn startup_denial_cleans_the_actual_created_stream_and_queue()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        drop(super::NativeSourceWatch::start_platform(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )),
        )?);
        for limit in [1, 7, 9, 10, 11, 12] {
            let mut budget = finite_budget();
            budget.backend_write_operations = limit;
            let mut work = super::DemandWork::new(
                budget, &crate::cancellation::CancellationToken::new(),
            );
            let error = super::NativeSourceWatch::start_driver(
                &directory, std::sync::Arc::new(super::super::Delivery { budget: work.budget, cancellation: work.cancellation().clone(), ..super::super::Delivery::new(std::sync::Arc::new(Ignored)) }), &mut work,
            ).err().ok_or("startup used the reserved teardown allowance")?;
            assert!(matches!(error, crate::DemandError::Work(_)));
            assert_eq!(work.budget, budget);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn callback_cancellation_still_destroys_each_admitted_owned_change()
        -> Result<(), Box<dyn std::error::Error>>
    {
        struct Cancel(crate::cancellation::CancellationToken);
        impl super::super::HostChangeSink for Cancel {
            fn logical_owned_bytes(&self) -> u64 { size_of::<Self>() as u64 }
            fn host_changed(
                &self, _: &[HostChange], work: &mut super::DemandWork,
            ) -> Result<(), crate::DemandError> {
                self.0.cancel();
                work.check()
            }
        }
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut budget = finite_budget();
        budget.items_examined = 2;
        let original = super::DemandWork::new(budget, &cancellation);
        let delivery = std::sync::Arc::new(super::super::Delivery { budget: original.budget, cancellation: original.cancellation().clone(), ..super::super::Delivery::new(std::sync::Arc::new(Cancel(cancellation))) });
        let shared = super::Shared {
            root: PathBuf::from("/root"), held: (0, 0),
            stream_bytes: super::AtomicU64::new(0), delivery,
            completed: super::AtomicBool::new(false),
            ownership: crate::demand::SourcePayloadOwnership::default(),
        };
        let mut callback = shared.delivery.callback_work();
        assert!(matches!(shared.events(
            std::iter::once((std::path::Path::new("/root/a/b"), ITEM_CREATED)), &mut callback,
        ), Err(crate::DemandError::Cancelled)));
        assert_eq!(crate::demand::SourceDemandReadWork::with_original_leaf(&mut callback, |work| work.live_allocation_bytes()), 0);
        assert_eq!(crate::demand::SourceDemandReadWork::with_original_leaf(&mut callback, |work| work.budget), budget);
        Ok(())
    }

    #[test]
    fn measured_stop_allows_the_actual_callback_to_read_payload_during_quiescence()
        -> Result<(), Box<dyn std::error::Error>>
    {
        struct Gate {
            state: std::sync::Mutex<(bool, bool)>,
            ready: std::sync::Condvar,
            watch: std::sync::OnceLock<std::sync::Weak<super::super::NativeSourceWatch>>,
            observed_bytes: super::AtomicU64,
            observed_stopped: super::AtomicBool,
            self_stop: std::sync::Mutex<Option<(Result<(), crate::DemandError>, crate::WorkCounters)>>,
            #[cfg(feature = "native-mount")]
            original: std::sync::OnceLock<std::sync::Arc<crate::demand::NativeDemandReadMeter>>,
            #[cfg(feature = "native-mount")]
            original_unlocked: super::AtomicBool,
        }
        impl Gate {
            fn release(&self) {
                self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).1 = true;
                self.ready.notify_all();
            }
        }
        impl super::super::HostChangeSink for Gate {
            fn logical_owned_bytes(&self) -> u64 { size_of::<Self>() as u64 }
            fn host_changed(
                &self, _: &[HostChange], _: &mut super::DemandWork,
            ) -> Result<(), crate::DemandError> {
                let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.0 = true;
                self.ready.notify_all();
                while !state.1 {
                    state = self.ready.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                drop(state);
                #[cfg(feature = "native-mount")]
                if let Some(original) = self.original.get() {
                    self.original_unlocked.store(original.0.try_lock().is_ok(), super::Ordering::Release);
                }
                let watch = self.watch.get().and_then(std::sync::Weak::upgrade)
                    .ok_or(crate::DemandError::WorkerUnavailable)?;
                self.observed_bytes.store(watch.platform_owned_bytes(), super::Ordering::Release);
                self.observed_stopped.store(watch.is_stopped(), super::Ordering::Release);
                let mut stop = super::DemandWork::new(crate::WorkBudget {
                    backend_write_operations: 4, backend_read_operations: 1,
                    ..crate::WorkBudget::default()
                }, &crate::cancellation::CancellationToken::new());
                let result = watch.stop_with_work(&mut stop);
                *self.self_stop.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((result, stop.work));
                Ok(())
            }
        }
        struct Release<'a>(&'a Gate);
        impl Drop for Release<'_> {
            fn drop(&mut self) { self.0.release(); }
        }
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let gate = std::sync::Arc::new(Gate {
            state: std::sync::Mutex::new((false, false)), ready: std::sync::Condvar::new(),
            watch: std::sync::OnceLock::new(), observed_bytes: super::AtomicU64::new(0),
            observed_stopped: super::AtomicBool::new(true), self_stop: std::sync::Mutex::new(None),
            #[cfg(feature = "native-mount")]
            original: std::sync::OnceLock::new(),
            #[cfg(feature = "native-mount")]
            original_unlocked: super::AtomicBool::new(false),
        });
        let watch = std::sync::Arc::new(super::super::NativeSourceWatch::start(&directory, gate.clone())?);
        let _release = Release(&gate);
        gate.watch.set(std::sync::Arc::downgrade(&watch)).map_err(|_| "watch was already installed")?;
        #[cfg(feature = "native-mount")]
        let original = std::sync::Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
            super::DemandWork::new(crate::WorkBudget {
                backend_write_operations: 4, backend_read_operations: 1,
                ..crate::WorkBudget::default()
            }, &crate::cancellation::CancellationToken::new()),
        )));
        #[cfg(feature = "native-mount")]
        gate.original.set(original.clone()).map_err(|_| "original stop work was already installed")?;
        assert!(!watch.is_stopped());
        let live_bytes = watch.platform_owned_bytes();
        std::fs::write(root.path().join("callback"), b"event")?;
        let state = gate.state.lock().map_err(|_| "poisoned callback gate")?;
        let (state, _) = gate.ready.wait_timeout_while(
            state, std::time::Duration::from_secs(10), |state| !state.0,
        ).map_err(|_| "poisoned callback wait")?;
        let entered = state.0;
        drop(state);
        if !entered { return Err("native callback did not arrive".into()); }
        let (finished, completion) = std::sync::mpsc::channel();
        let stopped = std::thread::scope(|scope| {
            let stop = scope.spawn(|| {
                #[cfg(feature = "native-mount")]
                let result = watch.stop_with_work(&mut crate::demand::NativeSourceWork::new(&original));
                #[cfg(not(feature = "native-mount"))]
                let result = {
                    let mut work = super::DemandWork::new(crate::WorkBudget {
                        backend_write_operations: 4, backend_read_operations: 1,
                        ..crate::WorkBudget::default()
                    }, &crate::cancellation::CancellationToken::new());
                    watch.stop_with_work(&mut work)
                };
                let _ = finished.send(());
                result
            });
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let custody_held = loop {
                match watch.stream.try_lock() {
                    Err(std::sync::TryLockError::WouldBlock) => break true,
                    Err(std::sync::TryLockError::Poisoned(_)) => break false,
                    Ok(_) => {}
                }
                if std::time::Instant::now() >= deadline { break false; }
                std::thread::yield_now();
            };
            let before_release = completion.recv_timeout(std::time::Duration::from_millis(100));
            let stopped_before_release = watch.is_stopped();
            gate.release();
            let stopped = stop.join().map_err(|_| "stop worker panicked")?;
            assert!(matches!(before_release, Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
            assert!(custody_held, "native stop did not take stream custody");
            assert!(!stopped_before_release, "completion was published before callback quiescence");
            assert_eq!(gate.observed_bytes.load(super::Ordering::Acquire), live_bytes);
            #[cfg(feature = "native-mount")]
            assert!(gate.original_unlocked.load(super::Ordering::Acquire), "native queue drain held the original work mutex");
            assert!(!gate.observed_stopped.load(super::Ordering::Acquire));
            let self_stop = gate.self_stop.lock().map_err(|_| "poisoned self-stop result")?;
            let (result, work) = self_stop.as_ref().ok_or("callback did not attempt self-stop")?;
            assert!(matches!(result, Err(crate::DemandError::WorkerUnavailable)));
            assert_eq!(*work, crate::WorkCounters::default());
            Ok::<_, Box<dyn std::error::Error>>(stopped)
        })?;
        stopped?;
        assert!(watch.is_stopped());
        assert!(!watch.delivery().is_exact());
        assert!(watch.stream.lock().map_err(|_| "poisoned stream")?.is_none());
        assert!(watch.platform_owned_bytes() < live_bytes);
        Ok(())
    }

    #[test]
    fn denied_root_copy_never_reaches_metadata_or_the_framework()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let mut budget = finite_budget();
        budget.bytes_copied = 0;
        let mut work = super::DemandWork::new(
            budget, &crate::cancellation::CancellationToken::new(),
        );
        let error = super::NativeSourceWatch::start_driver(
            &directory, std::sync::Arc::new(super::super::Delivery { budget: work.budget, cancellation: work.cancellation().clone(), ..super::super::Delivery::new(std::sync::Arc::new(Ignored)) }), &mut work,
        ).err().ok_or("root conversion exceeded its admitted copy allowance")?;
        assert!(matches!(error, crate::DemandError::Work(_)));
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn cancelled_start_admits_no_root_conversion()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let cancellation = crate::cancellation::CancellationToken::new();
        cancellation.cancel();
        let mut work = super::DemandWork::new(finite_budget(), &cancellation);
        let error = super::NativeSourceWatch::start_driver(
            &directory, std::sync::Arc::new(super::super::Delivery { budget: work.budget, cancellation: work.cancellation().clone(), ..super::super::Delivery::new(std::sync::Arc::new(Ignored)) }), &mut work,
        ).err().ok_or("cancelled startup unexpectedly ran")?;
        assert!(matches!(error, crate::DemandError::Cancelled));
        assert_eq!(work.work, crate::WorkCounters::default());
        Ok(())
    }

    #[test]
    fn measured_fence_rejects_denied_reads_and_abandons_a_replaced_root()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        std::fs::create_dir(&source)?;
        let directory = crate::native_host::HostRoot::open(&source, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let watch = super::NativeSourceWatch::start_platform(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )),
        )?;
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut denied = super::DemandWork::new(crate::WorkBudget::default(), &cancellation);
        assert!(matches!(
            watch.fence_with_work(std::time::Duration::ZERO, &mut denied),
            Err(crate::DemandError::Work(_))
        ));
        assert_eq!(denied.work, crate::WorkCounters::default());
        assert!(watch.delivery().is_exact());
        std::fs::rename(&source, root.path().join("moved"))?;
        std::fs::create_dir(&source)?;
        let mut work = super::DemandWork::new(crate::WorkBudget {
            backend_read_operations: 1, items_examined: 1,
            ..crate::WorkBudget::default()
        }, &cancellation);
        assert!(!watch.fence_with_work(std::time::Duration::ZERO, &mut work)?);
        assert!(!watch.delivery().is_exact());
        assert_eq!(work.work.source_entries_visited, 0);
        Ok(())
    }

    /// `FSEvents` proves no delivery order, so a fence reports everything.
    #[test]
    fn a_fence_reports_everything() -> Result<(), Box<dyn std::error::Error>> {
        struct Recorded(std::sync::Mutex<Vec<HostChange>>);
        impl super::super::HostChangeSink for Recorded {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(
                &self, changes: &[HostChange], work: &mut super::DemandWork,
            ) -> Result<(), crate::DemandError> {
                work.check()?;
                if let Ok(mut recorded) = self.0.lock() {
                    recorded.extend_from_slice(changes);
                }
                Ok(())
            }
        }
        let root = tempfile::tempdir()?;
        let recorded = std::sync::Arc::new(Recorded(std::sync::Mutex::new(Vec::new())));
        let watch = super::NativeSourceWatch::start_platform(
            &crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?,
            std::sync::Arc::new(super::super::Delivery::new(recorded.clone())),
        )?;
        watch.fence()?;
        let recorded = recorded.0.lock().map_err(|_| "poisoned")?;
        assert!(recorded.contains(&HostChange::Unconfirmed));
        assert!(!recorded.contains(&HostChange::Everything));
        Ok(())
    }

    #[test]
    fn original_callback_last_owner_drop_runs_the_same_queue_terminal_suffix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::demand::{NativeDemandReadMeter, NativeSourceWork};
        use crate::{CancellationToken, DemandError};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};
        struct Retires {
            watch: Mutex<Option<super::NativeSourceWatch>>,
            original: std::sync::Weak<NativeDemandReadMeter>,
            cancellation: CancellationToken,
            invoked: AtomicBool,
        }
        impl super::super::HostChangeSink for Retires {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut super::DemandWork) -> Result<(), DemandError> {
                Err(DemandError::NativeWorkUnavailable)
            }
            fn host_changed_with_native_work(
                &self, _: &[HostChange], work: &mut NativeSourceWork<'_>,
            ) -> Result<(), DemandError> {
                if self.invoked.swap(true, Ordering::AcqRel) { return Ok(()); }
                let original = self.original.upgrade().ok_or(DemandError::WorkerUnavailable)?;
                assert!(Arc::ptr_eq(&original, work.original));
                assert!(original.0.try_lock().is_ok(), "callback runs outside the original admission lock");
                self.cancellation.cancel();
                let watch = self.watch.lock().map_err(|_| DemandError::WorkerUnavailable)?.take();
                drop(watch);
                Ok(())
            }
        }
        crate::fsevents::EventStream::initialize_services()?;
        let root = tempfile::tempdir()?;
        let held = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let cancellation = CancellationToken::new();
        let mut budget = finite_budget();
        budget.backend_read_operations = 128;
        budget.backend_write_operations = 128;
        budget.items_examined = 4096;
        budget.allocation_operations = 128;
        let original = Arc::new(NativeDemandReadMeter(Mutex::new(super::DemandWork::new(budget, &cancellation))));
        let lifetime = Arc::downgrade(&original);
        let sink = Arc::new(Retires {
            watch: Mutex::new(None), original: lifetime.clone(), cancellation,
            invoked: AtomicBool::new(false),
        });
        let mut work = NativeSourceWork::new(&original);
        let delivery = Arc::new(super::super::Delivery::new_with_work(sink.clone(), &mut work)?);
        let watch = super::NativeSourceWatch::start_driver(&held, delivery, &mut work)?;
        let completion = Arc::clone(&watch.shared);
        let funded_live = original.with_original_leaf(|work| work.live_allocation_bytes());
        *sink.watch.lock().map_err(|_| "poisoned callback custody")? = Some(watch);
        std::fs::write(root.path().join("retire"), b"native event")?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !completion.completed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(sink.invoked.load(Ordering::Acquire));
        assert!(completion.completed.load(Ordering::Acquire), "terminal flag follows actual stream/context/queue release");
        assert_eq!(completion.stream_bytes.load(Ordering::Acquire), 0);
        original.with_original_leaf(|work| {
            assert_eq!(work.budget.backend_write_operations, budget.backend_write_operations);
            assert_eq!(work.budget.backend_read_operations, budget.backend_read_operations);
        });
        drop(completion);
        original.with_original_leaf(|work| {
            work.reconcile_native_controls()?;
            // The original registry's own capacity remains legitimately live.
            assert!(work.live_allocation_bytes() < funded_live);
            Ok::<_, crate::WorkError>(())
        })?;
        drop(original);
        while lifetime.upgrade().is_some() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(lifetime.upgrade().is_none(), "no context-slot or custody/root strong cycle remains");
        Ok(())
    }

    /// An event that does not say which item it changed, or says its directory must be scanned, covers everything beneath its path.
    #[test]
    fn unspecific_events_cover_everything_beneath_their_path() {
        let root = PathBuf::new;
        let name = || PathBuf::from("d");
        assert_eq!(change(root(), 0), HostChange::Everything);
        assert_eq!(
            change(root(), MUST_SCAN_SUBDIRS | ITEM_IS_DIR),
            HostChange::Everything
        );
        assert_eq!(
            change(root(), ITEM_CREATED | ITEM_IS_DIR),
            HostChange::Altered(root())
        );
        assert_eq!(change(name(), 0), HostChange::Rebound(name()));
        assert_eq!(
            change(name(), MUST_SCAN_SUBDIRS),
            HostChange::Rebound(name())
        );
        for flag in [ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED] {
            assert_eq!(
                change(name(), flag | ITEM_MODIFIED | ITEM_IS_FILE),
                HostChange::Rebound(name())
            );
        }
        assert_eq!(
            change(name(), ITEM_MODIFIED | ITEM_IS_FILE),
            HostChange::Altered(name())
        );
    }
}
