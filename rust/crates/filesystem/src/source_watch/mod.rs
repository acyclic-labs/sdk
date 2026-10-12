//! Host notifications of changes made to a native source directory outside
//! every view of it.
//!
//! A view remembers what it read from a source: resolved names, absences,
//! attributes, listings, and the kernel caches built from them. The source
//! changes outside the view, so each remembered fact holds only until the
//! host reports a change to what it depends on. One watch per source root
//! turns the host's own notifications into exactly those reports: a name
//! rebound (created, removed, or renamed), which covers everything reached
//! through it; an entry altered in place (its content or attributes), which
//! covers the entry and the listing that shows it; and, where the host can
//! still identify it, the node, which covers every other name bound to it.
//!
//! # Backends
//!
//! - Linux: one inotify instance, with a watch on every directory the
//!   source reads before it reads it (see [`NativeSourceWatch::admit`]).
//!   The kernel queues each event before the system call that caused it
//!   returns.
//! - macOS: one `FSEvents` stream over the root. Its reports arrive shortly
//!   after each change but in no order a fence could rely on, so a fence
//!   there reports everything instead (see the backend's docs).
//! - Windows: `ReadDirectoryChangesExW` over the whole root, which the file
//!   system reports into before the operation that caused it completes.
//!
//! A root whose changes do not all pass through this kernel (a network or
//! user-space file system) has no watch: every view of it reads it afresh.
//!
//! # Exactness
//!
//! A fact is read after the view samples its ledger stamp, and is reused
//! only while nothing it depends on recorded a later position. A reported
//! change is recorded after the host made it. So a read that races an
//! unreported change either read the source before the change, and its fact
//! is invalidated once the report is recorded, or read it after, and its
//! fact is already current. What a lookup may still return is the answer
//! from before a change whose report has not been recorded yet: the change
//! takes effect in the view when its report is recorded rather than when
//! the host operation returned. Between processes that do not otherwise
//! communicate, that is indistinguishable from the change completing a
//! little later, which is how every native file system orders concurrent
//! operations. A process that learns of a change by another channel (a
//! build step waiting for a writer to exit) and must see it through the
//! mount calls [`NativeSourceWatch::fence`] first, which the mount's
//! revalidation does: once it returns, every change that completed before
//! it was called has been recorded, exactly on Linux and Windows and, on
//! macOS, by recording that anything may have changed. Hosts report a file's content and size
//! at different points: Linux at every write, macOS and Windows once the
//! writer closes (or flushes) the file, which is NFS close-to-open
//! consistency.
//!
//! # What no host reports
//!
//! Every host reports a change under the name it was made through, and only
//! beneath the watched root (on Linux, beneath the directories watched). So
//! a file with another name outside the root, written through that name,
//! changes unreported. A view therefore never relies on reports for a node
//! the source says has more than one name: it reads such a node afresh on
//! every use, and drivers cache nothing of it. What remains unreported are
//! the changes to a node with one name when it was read: a name added to it
//! outside the root, and writes through that name; and, on Windows, whose
//! listings do not count names, writes through any name outside the root.
//! Writes through a shared memory mapping raise no report of their own and
//! are reported when the writer closes the file, which is NFS close-to-open
//! consistency.
//!
//! Where the host names only the path it changed (Linux and macOS), the
//! node is the one the path binds when the report is handled: a change
//! through one hard link reaches every name of the node, but removing one
//! of several links leaves the others' link count as the view last read it
//! until something reports a change to them.
//!
//! A host that loses notifications says so (a queue overflow, dropped
//! events, a lost buffer); the watch then reports that everything may have
//! changed, which invalidates every remembered fact. A host that stops
//! reporting altogether (a replaced root, an exhausted watch limit) makes
//! the watch inexact for good: it reports everything once more and the view
//! reads the source afresh from then on.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::demand::{DemandError, DemandWork, NativeSourceWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::native_host::NativeHostWork;
use crate::performance::WorkCounters;

#[cfg(windows)]
mod directory_changes;
#[cfg(target_os = "macos")]
mod fsevents;
#[cfg(target_os = "linux")]
mod inotify;

#[cfg(windows)]
use directory_changes::PlatformWatch;
#[cfg(target_os = "macos")]
use fsevents::PlatformWatch;
#[cfg(target_os = "linux")]
use inotify::PlatformWatch;

/// Known SDK control storage: Arc's two counters, alignment, and Rust payload.
pub(super) const fn arc_control_bytes<T>() -> u64 {
    let header = std::mem::size_of::<[usize; 2]>();
    let alignment = std::mem::align_of::<T>();
    let payload_offset = (header + alignment - 1) & !(alignment - 1);
    (payload_offset + std::mem::size_of::<T>()) as u64
}

/// Transfers a genuine before-birth terminal allowance into the existing original reservation engine.
#[cfg(windows)]
pub(super) fn prepare_native_cleanup_credit<W: SourceDemandReadWork>(
    work: &mut W, maximum: WorkCounters,
) -> Result<Option<crate::materializer::NativeWorkReservation>, DemandError> {
    work.check()?;
    if !work.is_native_original() { return Ok(None); }
    let source = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?;
    let original = source.materialization_work()?.allocation_work();
    original.reserve_work(maximum)
        .map(Some)
        .map_err(crate::native_name::original_name_error)
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub(super) fn reserve_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) -> Result<(), DemandError> {
    use acyclic_native_runtime::NativeOperationKind::{Read, Write};
    work.reserve(crate::demand::DemandWork::operation_credit(Read), delta.backend_read_operations).map_err(|error| work.record_native_io_failure(error))
        .map_err(|error| work.take_native_io_failure().unwrap_or(DemandError::Io(error)))?;
    if let Err(error) = work.reserve(crate::demand::DemandWork::operation_credit(Write), delta.backend_write_operations).map_err(|error| work.record_native_io_failure(error)) {
        let error = work.take_native_io_failure().unwrap_or(DemandError::Io(error));
        work.refund(crate::demand::DemandWork::operation_credit(Read), delta.backend_read_operations).expect("original reservation is balanced");
        return Err(error);
    }
    if let Err(error) = work.reserve(crate::materializer::NativeResourceCredit::Items, delta.items_examined) {
        work.refund(crate::demand::DemandWork::operation_credit(Read), delta.backend_read_operations).expect("original reservation is balanced");
        work.refund(crate::demand::DemandWork::operation_credit(Write), delta.backend_write_operations).expect("original reservation is balanced");
        return Err(error);
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub(super) fn cancel_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) {
    use acyclic_native_runtime::NativeOperationKind::{Read, Write};
    work.refund(crate::demand::DemandWork::operation_credit(Read), delta.backend_read_operations).expect("original reservation is balanced");
    work.refund(crate::demand::DemandWork::operation_credit(Write), delta.backend_write_operations).expect("original reservation is balanced");
    work.refund(crate::materializer::NativeResourceCredit::Items, delta.items_examined).expect("original reservation is balanced");
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub(super) fn record_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) -> Result<(), DemandError> {
    use acyclic_native_runtime::NativeOperationKind::{Read, Write};
    let mut result = Ok(());
    for (kind, count) in [(Read, delta.backend_read_operations), (Write, delta.backend_write_operations)] {
        for _ in 0..count {
            let recorded = work.complete(crate::demand::DemandWork::operation_credit(kind), 1, 1).map_err(|error| work.record_native_io_failure(error))
                .map_err(|error| work.take_native_io_failure().unwrap_or(DemandError::Io(error)));
            result = result.and(recorded);
        }
    }
    result.and(work.complete(crate::materializer::NativeResourceCredit::Items, delta.items_examined, delta.items_examined))
}

/// One change the host reported beneath a watched root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HostChange {
    /// This root-relative path (empty: the root itself) may bind another entry, or none, and so may everything reached through it.
    Rebound(PathBuf),
    /// The entry at this root-relative path may have changed in place: its content or attributes.
    Altered(PathBuf),
    /// The file with this host file index, on the root's volume, may have changed under every name bound to it.
    #[cfg(windows)]
    File(u64),
    /// Notifications were lost: anything may have changed.
    Everything,
    /// Changes may not all have been delivered yet, though none is known to have been lost: a fence that cannot prove delivery (see the macOS and Windows backends) reports this instead.
    #[cfg(any(target_os = "macos", windows))]
    Unconfirmed,
}

#[cfg(windows)]
pub(crate) enum HostChangeWide<'a> {
    Path { action: u32, path: &'a [u16] },
    File(u64),
    Everything,
    Unconfirmed,
}

/// Receives each batch of changes, in the order the host reported them.
pub(crate) trait HostChangeSink: Send + Sync {
    /// SDK payload owned by this watch's concrete sink wrapper, excluding borrowed caller controls and provider-private allocator metadata.
    fn logical_owned_bytes(&self) -> u64;
    /// Called with no watch lock held that a lookup could wait on.
    fn host_changed(
        &self, changes: &[HostChange], work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;

    fn host_changed_with_native_work(
        &self, _changes: &[HostChange], _work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> { Err(DemandError::NativeWorkUnavailable) }

    #[cfg(windows)]
    fn host_changed_wide(
        &self, changes: &[HostChangeWide<'_>], work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;

    #[cfg(windows)]
    fn host_changed_wide_with_native_work(
        &self, _changes: &[HostChangeWide<'_>], _work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> { Err(DemandError::NativeWorkUnavailable) }
}

#[cfg(target_os = "linux")]
pub(crate) trait NativeEventSink: Send + Sync {
    fn logical_owned_bytes(&self) -> u64;
    fn event_measured(
        &self,
        relative: &std::path::Path,
        mask: u32,
        cookie: usize,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;
    fn invalidate_measured(
        &self,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError>;
    fn event_with_native_work(
        &self, _relative: &std::path::Path, _mask: u32, _cookie: usize,
        _work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> { Err(DemandError::NativeWorkUnavailable) }
    fn invalidate_with_native_work(
        &self, _work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> { Err(DemandError::NativeWorkUnavailable) }
}

// Callback custody is thread-local control only, not source authority. Frames
// live on the invoking stack, including nested callbacks of different watches.
struct CallbackFrame {
    delivery: *const Delivery,
    previous: *const CallbackFrame,
}

std::thread_local! {
    static ACTIVE_CALLBACK: std::cell::Cell<*const CallbackFrame> =
        const { std::cell::Cell::new(std::ptr::null()) };
}

struct RestoreCallback(*const CallbackFrame);

impl Drop for RestoreCallback {
    fn drop(&mut self) {
        ACTIVE_CALLBACK.with(|active| active.set(self.0));
    }
}

/// A callback borrows the original invocation; only legacy watches own a callback-local allowance.
pub(super) enum CallbackWork<'a> {
    Ordinary(DemandWork),
    Original(NativeSourceWork<'a>),
}

impl CallbackWork<'_> {
    pub(super) fn receipt_work(&self) -> WorkCounters {
        match self {
            Self::Ordinary(work) => work.work,
            Self::Original(_) => WorkCounters::UNCHARGED,
        }
    }
}

impl NativeHostWork for CallbackWork<'_> {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
        <DemandWork as NativeHostWork>::io_error_kind(error)
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> DemandError { DemandError::Allocation(error) }
    fn invalid_request() -> DemandError { DemandError::InvalidRequest }
    fn check(&self) -> Result<(), DemandError> {
        match self { Self::Ordinary(work) => work.check(), Self::Original(work) => work.check() }
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        match self { Self::Ordinary(work) => work.admit(delta), Self::Original(work) => work.admit(delta) }
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        match self { Self::Ordinary(work) => work.record(delta), Self::Original(work) => work.record(delta) }
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        match self {
            Self::Ordinary(work) => work.started_io(delta),
            Self::Original(work) => work.started_io(delta),
        }
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        match self { Self::Ordinary(work) => work.claim(bytes), Self::Original(work) => work.claim(bytes) }
    }
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> {
        match self {
            Self::Ordinary(work) => work.reserve_opaque_decode(maximum),
            Self::Original(work) => work.reserve_opaque_decode(maximum),
        }
    }
    fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), DemandError> {
        match self {
            Self::Ordinary(work) => work.reconcile_allocation(allocation, actual),
            Self::Original(work) => work.reconcile_allocation(allocation, actual),
        }
    }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        match self { Self::Ordinary(work) => work.release(bytes), Self::Original(work) => work.release(bytes) }
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> {
        match self {
            Self::Ordinary(work) => work.reserve_cleanup_write(),
            Self::Original(work) => work.reserve_cleanup_write(),
        }
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        match self {
            Self::Ordinary(work) => work.cancel_cleanup_write(reservation),
            Self::Original(work) => work.cancel_cleanup_write(reservation),
        }
    }
    fn attempt_cleanup_write<T>(
        &mut self, reservation: u64, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), DemandError>) {
        match self {
            Self::Ordinary(work) => work.attempt_cleanup_write(reservation, operation),
            Self::Original(work) => work.attempt_cleanup_write(reservation, operation),
        }
    }
}

impl SourceDemandReadWork for CallbackWork<'_> {
    fn with_native_read_meter<T>(
        &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        match self {
            Self::Ordinary(work) => work.with_native_read_meter(operation),
            Self::Original(work) => work.with_native_read_meter(operation),
        }
    }
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        match self {
            Self::Ordinary(work) => operation(work),
            Self::Original(work) => work.with_original_leaf(operation),
        }
    }
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        match self { Self::Ordinary(work) => Some(operation(work)), Self::Original(_) => None }
    }
    fn native_source_work(&self) -> Option<NativeSourceWork<'_>> {
        match self { Self::Ordinary(_) => None, Self::Original(work) => work.native_source_work() }
    }
    fn is_native_original(&self) -> bool { matches!(self, Self::Original(_)) }
    #[cfg(feature = "native-mount")]
    fn original_meter(&self) -> Option<&Arc<crate::demand::NativeDemandReadMeter>> {
        match self { Self::Ordinary(_) => None, Self::Original(work) => work.original_meter() }
    }
}

enum CallbackFailure {
    Ordinary { error: DemandError, work: WorkCounters },
    OriginalAlreadyCharged(DemandError),
}

/// Where a watch delivers and whether it still covers every change.
pub(crate) struct Delivery {
    sink: Arc<dyn HostChangeSink>,
    exact: AtomicBool,
    budget: crate::performance::WorkBudget,
    cancellation: crate::cancellation::CancellationToken,
    failure: std::sync::Mutex<Option<CallbackFailure>>,
    #[cfg(target_os = "linux")]
    events: Option<Arc<dyn NativeEventSink>>,
    startup_failure: std::sync::Mutex<std::sync::Weak<SourceWatchStartupFailure>>,
    ownership: SourcePayloadOwnership,
    original: Option<crate::NativeMaterializationWork>,
}

impl Delivery {
    fn callback<T>(&self, invoke: impl FnOnce() -> T) -> T {
        let frame = CallbackFrame {
            delivery: self,
            previous: ACTIVE_CALLBACK.with(std::cell::Cell::get),
        };
        ACTIVE_CALLBACK.with(|active| active.set(&frame));
        let _restore = RestoreCallback(frame.previous);
        invoke()
    }

    #[allow(unsafe_code, reason = "reads live thread-local callback frames")]
    fn ensure_stop_available(&self) -> Result<(), crate::demand::DemandError> {
        ACTIVE_CALLBACK.with(|active| {
            let mut frame = active.get();
            while !frame.is_null() {
                // SAFETY: this thread's chain points only to live invoking
                // stack frames; unwinding restores the previous frame first.
                let current = unsafe { &*frame };
                if std::ptr::eq(current.delivery, self) {
                    return Err(crate::demand::DemandError::WorkerUnavailable);
                }
                frame = current.previous;
            }
            Ok(())
        })
    }

    fn new(sink: Arc<dyn HostChangeSink>) -> Self {
        Self {
            sink,
            exact: AtomicBool::new(true),
            budget: crate::performance::WorkBudget::UNBOUNDED,
            cancellation: crate::cancellation::CancellationToken::new(),
            failure: std::sync::Mutex::new(None),
            startup_failure: std::sync::Mutex::new(std::sync::Weak::new()),
            #[cfg(target_os = "linux")]
            events: None,
            ownership: SourcePayloadOwnership::default(),
            original: None,
        }
    }

    fn new_measured(sink: Arc<dyn HostChangeSink>, work: &crate::demand::DemandWork) -> Self {
        Self {
            sink,
            exact: AtomicBool::new(true),
            budget: work.budget,
            cancellation: work.cancellation().clone(),
            failure: std::sync::Mutex::new(None),
            startup_failure: std::sync::Mutex::new(std::sync::Weak::new()),
            #[cfg(target_os = "linux")]
            events: None,
            ownership: SourcePayloadOwnership::default(),
            original: None,
        }
    }

    fn new_with_work<W: SourceDemandReadWork>(
        sink: Arc<dyn HostChangeSink>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let original = work.native_source_work().map(|work| work.owned().allocation_work());
        let controls = |work: &mut DemandWork| (work.budget, work.cancellation().clone());
        let (budget, cancellation) = if original.is_some() {
            work.with_original_leaf(controls)
        } else {
            work.with_ordinary_work(controls).ok_or(DemandError::NativeWorkUnavailable)?
        };
        let ownership = SourcePayloadOwnership::new(work)?;
        Ok(Self {
            sink, exact: AtomicBool::new(true), budget, cancellation,
            failure: std::sync::Mutex::new(None),
            startup_failure: std::sync::Mutex::new(std::sync::Weak::new()),
            #[cfg(target_os = "linux")]
            events: None,
            ownership, original,
        })
    }

    fn prepare_startup_failure<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<Option<Arc<SourceWatchStartupFailure>>, DemandError> {
        if self.original.is_none() { return Ok(None); }
        self.prepare_failure(work).map(Some)
    }

    fn prepare_failure<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<Arc<SourceWatchStartupFailure>, DemandError> {
        {
            let pending = self.startup_failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = pending.upgrade() { return Ok(pending); }
        }
        let ownership = SourcePayloadOwnership::new(work)?;
        let bytes = arc_control_bytes::<SourceWatchStartupFailure>();
        work.allocate(bytes)?;
        let failure = Arc::new(SourceWatchStartupFailure {
            cause: std::sync::OnceLock::new(), watch: std::sync::Mutex::new(None),
            stopped: AtomicBool::new(false), owned_bytes: std::sync::atomic::AtomicU64::new(0),
            ownership, _original: self.original.clone(),
        });
        failure.ownership.retain(bytes, work)?;
        if !work.is_native_original() { work.release(bytes)?; }
        let mut pending = self.startup_failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pending) = pending.upgrade() { return Ok(pending); }
        *pending = Arc::downgrade(&failure);
        Ok(failure)
    }

    fn callback_work(&self) -> CallbackWork<'_> {
        match &self.original {
            Some(original) => CallbackWork::Original(NativeSourceWork::from_native_materialization_work(original)),
            None => CallbackWork::Ordinary(DemandWork::new(self.budget, &self.cancellation)),
        }
    }

    fn birth_matches<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        let Some(birth) = &self.original else { return false; };
        let Some(current) = work.native_source_work() else { return false; };
        let current = current.owned();
        Arc::ptr_eq(&birth.state, &current.state)
            && match (birth.resource.as_ref(), current.resource.as_ref()) {
                (None, None) => true,
                (Some(birth), Some(current)) => Arc::ptr_eq(birth, current),
                _ => false,
            }
    }

    fn failed_with_work<W: SourceDemandReadWork>(&self, error: DemandError, work: &mut W) {
        self.exact.store(false, Ordering::Release);
        let failure = if work.is_native_original() {
            CallbackFailure::OriginalAlreadyCharged(error)
        } else {
            let prior = work.with_original_leaf(|work| work.work);
            CallbackFailure::Ordinary { error, work: prior }
        };
        let mut failed = self.failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if failed.is_none() { *failed = Some(failure); }
    }

    fn take_failure<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        if let Some(failure) = self.failure.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).take()
        {
            match failure {
                CallbackFailure::Ordinary { error, work: prior } => {
                    work.merge(prior)?;
                    return Err(error);
                }
                CallbackFailure::OriginalAlreadyCharged(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn deliver_with_work<W: SourceDemandReadWork>(
        &self, changes: &[HostChange], work: &mut W,
    ) -> Result<(), DemandError> {
        if changes.is_empty() { return Ok(()); }
        if let Some(mut native) = work.native_source_work() {
            return self.callback(|| self.sink.host_changed_with_native_work(changes, &mut native));
        }
        work.with_ordinary_work(|work| self.callback(|| self.sink.host_changed(changes, work)))
            .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }

    fn invalidate_with_work<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        #[cfg(target_os = "linux")]
        if let Some(sink) = &self.events {
            if let Some(mut native) = work.native_source_work() {
                return self.callback(|| sink.invalidate_with_native_work(&mut native));
            }
            return work.with_ordinary_work(|work| self.callback(|| sink.invalidate_measured(work)))
                .unwrap_or(Err(DemandError::NativeWorkUnavailable));
        }
        self.deliver_with_work(&[HostChange::Everything], work)
    }

    #[cfg(target_os = "linux")]
    fn event_with_work<W: SourceDemandReadWork>(
        &self, relative: &std::path::Path, mask: u32, cookie: usize, work: &mut W,
    ) -> Result<(), DemandError> {
        let sink = self.events.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        if let Some(mut native) = work.native_source_work() {
            return self.callback(|| sink.event_with_native_work(relative, mask, cookie, &mut native));
        }
        work.with_ordinary_work(|work| self.callback(|| sink.event_measured(relative, mask, cookie, work)))
            .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }

    #[cfg(windows)]
    fn deliver_wide_with_work<W: SourceDemandReadWork>(
        &self, changes: &[HostChangeWide<'_>], work: &mut W,
    ) -> Result<(), DemandError> {
        if changes.is_empty() { return Ok(()); }
        if let Some(mut native) = work.native_source_work() {
            return self.callback(|| self.sink.host_changed_wide_with_native_work(changes, &mut native));
        }
        work.with_ordinary_work(|work| self.callback(|| self.sink.host_changed_wide(changes, work)))
            .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }

    fn deliver(&self, changes: &[HostChange]) {
        let mut work = self.callback_work();
        if let Err(error) = self.deliver_with_work(changes, &mut work) {
            self.failed_with_work(error, &mut work);
        }
    }

    /// The host stopped reporting changes exactly: report everything once, after which the watch is inexact for good.
    fn abandon(&self) {
        if self.exact.swap(false, Ordering::AcqRel) {
            self.deliver(&[HostChange::Everything]);
        }
    }

    fn is_exact(&self) -> bool {
        self.exact.load(Ordering::Acquire)
    }
}

/// The live watch of one native source root.
pub(crate) struct NativeSourceWatch {
    platform: PlatformWatch,
}

/// An original startup failure retaining its actual native owner.
pub struct SourceWatchStartupFailure {
    cause: std::sync::OnceLock<DemandError>,
    watch: std::sync::Mutex<Option<NativeSourceWatch>>,
    stopped: AtomicBool,
    owned_bytes: std::sync::atomic::AtomicU64,
    ownership: SourcePayloadOwnership,
    _original: Option<crate::NativeMaterializationWork>,
}

struct FailedWatchLease<'a> {
    owner: Option<NativeSourceWatch>,
    failure: &'a SourceWatchStartupFailure,
}

impl Drop for FailedWatchLease<'_> {
    fn drop(&mut self) {
        if let Some(watch) = self.owner.take() {
            self.failure.stopped.store(watch.is_stopped(), Ordering::Release);
            self.failure.owned_bytes.store(watch.logical_owned_bytes(), Ordering::Release);
            *self.failure.watch.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watch);
        }
    }
}

impl std::fmt::Debug for SourceWatchStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("SourceWatchStartupFailure");
        if let Some(cause) = self.cause.get() { debug.field("cause", cause); }
        debug.finish_non_exhaustive()
    }
}

impl std::fmt::Display for SourceWatchStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.cause.get() {
            Some(cause) => std::fmt::Display::fmt(cause, formatter),
            None => formatter.write_str("native source startup has not failed"),
        }
    }
}

impl std::error::Error for SourceWatchStartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.get().map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

impl SourceWatchStartupFailure {
    fn retain_failed_platform(self: &Arc<Self>, error: DemandError, platform: PlatformWatch) -> DemandError {
        platform.delivery().exact.store(false, Ordering::Release);
        assert!(self.cause.set(error).is_ok(), "one startup control retains one actual native owner");
        let watch = NativeSourceWatch { platform };
        self.stopped.store(watch.is_stopped(), Ordering::Release);
        self.owned_bytes.store(watch.logical_owned_bytes(), Ordering::Release);
        *self.watch.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watch);
        DemandError::SourceWatchStartup(Arc::clone(self))
    }

    /// Quiesces the retained owner using the current cleanup invocation.
    pub fn stop_with_native_work(
        &self, work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        self.stop_with_work(&mut NativeSourceWork::from_native_materialization_work(work))
    }

    /// Retires the retained original watch using only its birth-paid cleanup.
    pub fn stop_with_cleanup_wait(&self, wait: &crate::CancellationToken) -> Result<(), DemandError> {
        self.stop_original_with_wait(Some(wait))
    }

    /// Retires only the retained owner's already-accepted physical resources.
    pub fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.stop_original_with_wait(None)
    }

    fn stop_original_with_wait(&self, wait: Option<&crate::CancellationToken>) -> Result<(), DemandError> {
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        let (owner, poisoned) = match self.watch.try_lock() {
            Ok(mut custody) => (custody.take(), false),
            Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => (poisoned.into_inner().take(), true),
        };
        let Some(owner) = owner else {
            return if poisoned { Err(DemandError::OriginalWorkPoisoned) }
                else if self.is_stopped() { Ok(()) } else { Err(DemandError::WorkerUnavailable) };
        };
        let lease = FailedWatchLease { owner: Some(owner), failure: self };
        let native = lease.owner.as_ref().expect("the original lease retains the native owner");
        let result = match wait {
            Some(wait) => native.stop_with_cleanup_wait(wait),
            None => native.stop_accepted_cleanup(),
        };
        if poisoned { result.and(Err(DemandError::OriginalWorkPoisoned)) } else { result }
    }

    /// Uses the same retained-owner lease and original native cleanup engine.
    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), DemandError> {
        let owner = self.watch.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        let Some(owner) = owner else {
            return if self.is_stopped() { Ok(()) } else { Err(DemandError::WorkerUnavailable) };
        };
        let lease = FailedWatchLease { owner: Some(owner), failure: self };
        lease.owner.as_ref().expect("the lease retains the native owner")
            .stop_with_work(work)
    }

    /// True only after the retained native owner has actually quiesced.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        arc_control_bytes::<Self>() + self.owned_bytes.load(Ordering::Acquire)
    }
}

impl NativeSourceWatch {
    /// Starts reporting changes beneath `root`, whose held directory is `directory`, to `sink`.
    pub(crate) fn start(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
    ) -> std::io::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        Self::start_measured(directory, sink, &mut crate::demand::DemandWork::new(
            crate::performance::WorkBudget::UNBOUNDED, &cancellation,
        )).map_err(|error| match error {
            crate::demand::DemandError::Io(error) => error,
            error => std::io::Error::other(error),
        })
    }

    #[cfg(windows)]
    pub(crate) fn start_held_with_sink_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>,
        complete: bool, recursive: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        let delivery = Delivery::new_with_work(sink, work)?;
        let bytes = arc_control_bytes::<Delivery>();
        work.allocate(bytes)?;
        let delivery = Arc::new(delivery);
        delivery.ownership.retain(bytes, work)?;
        let result = PlatformWatch::start_held_root_with_work(directory, delivery, complete, recursive, work)
            .map(|platform| Self { platform });
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }

    pub(crate) fn start_measured(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Self, crate::demand::DemandError> {
        Self::start_with_work(directory, sink, work)
    }

    pub(crate) fn start_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_with_output_mode(directory, sink, work, false)
    }

    pub(crate) fn start_with_output_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_with_output_mode(directory, sink, work, true)
    }

    fn start_with_output_mode<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, work: &mut W,
        commit_output: bool,
    ) -> Result<Self, DemandError> {
        let delivery = Delivery::new_with_work(sink, work)?;
        let bytes = arc_control_bytes::<Delivery>();
        work.allocate(bytes)?;
        let delivery = Arc::new(delivery);
        delivery.ownership.retain(bytes, work)?;
        let failure = if commit_output {
            Some(delivery.prepare_failure(work)?)
        } else { None };
        let result = PlatformWatch::start_with_work(directory, delivery, work).and_then(|platform| {
            if let Some(failure) = failure {
                if let Err(error) = work.with_original_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::ReturnedItems, 1, 1)) {
                    return Err(failure.retain_failed_platform(error, platform));
                }
            }
            Ok(Self { platform })
        });
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn start_with_events(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
        events: Arc<dyn NativeEventSink>,
        budget: crate::performance::WorkBudget,
        cancellation: &crate::cancellation::CancellationToken,
    ) -> std::io::Result<Self> {
        let mut delivery = Delivery::new_measured(
            sink, &crate::demand::DemandWork::new(budget, cancellation),
        );
        delivery.events = Some(events);
        Ok(Self { platform: PlatformWatch::start(directory, Arc::new(delivery))? })
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn start_with_events_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>,
        events: Arc<dyn NativeEventSink>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let mut delivery = Delivery::new_with_work(sink, work)?;
        delivery.events = Some(events);
        let bytes = arc_control_bytes::<Delivery>();
        work.allocate(bytes)?;
        let delivery = Arc::new(delivery);
        delivery.ownership.retain(bytes, work)?;
        let result = PlatformWatch::start_with_work(directory, delivery, work)
            .map(|platform| Self { platform });
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn unadmit_measured(
        &self, directory: &std::path::Path, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.unadmit_with_work(directory, work)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn unadmit_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, directory: &std::path::Path, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        self.platform.unadmit_with_work(directory, work)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn fence_events_measured(
        &self, timeout: std::time::Duration, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, crate::demand::DemandError> {
        self.fence_events_with_work(timeout, work)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn fence_events_with_work<W: SourceDemandReadWork>(
        &self, timeout: std::time::Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        self.fence_with_work(timeout, work)
    }

    /// Starts the existing Windows driver over one already validated held directory.
    #[cfg(windows)]
    pub(crate) fn start_directory_measured(
        directory: &cap_std::fs::Dir,
        sink: Arc<dyn HostChangeSink>,
        complete: bool,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Self, crate::demand::DemandError> {
        Self::start_directory_with_work(directory, sink, complete, work)
    }

    #[cfg(windows)]
    pub(crate) fn start_directory_with_work<W: SourceDemandReadWork>(
        directory: &cap_std::fs::Dir, sink: Arc<dyn HostChangeSink>, complete: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        let delivery = Delivery::new_with_work(sink, work)?;
        let bytes = arc_control_bytes::<Delivery>();
        work.allocate(bytes)?;
        let delivery = Arc::new(delivery);
        delivery.ownership.retain(bytes, work)?;
        let result = PlatformWatch::start_directory_with_work(directory, delivery, complete, work)
            .map(|platform| Self { platform });
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }

    #[cfg(windows)]
    pub(crate) fn owned_bytes(&self) -> u64 {
        self.platform.logical_owned_bytes()
            + arc_control_bytes::<Delivery>()
    }

    /// Admits native shutdown before mutating the original owner, then waits for actual quiescence.
    pub(crate) fn stop_measured(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.stop_with_work(work)
    }

    pub(crate) fn stop_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        self.platform.stop_with_work(work)
    }

    /// Uses the platform's existing original birth cleanup and native stop suffix.
    pub(crate) fn stop_with_cleanup_wait(&self, wait: &crate::CancellationToken) -> Result<(), DemandError> {
        self.platform.stop_with_cleanup_wait(wait)
    }

    /// Consumes only this watch's genuine original birth-paid stop custody.
    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.platform.stop_accepted_cleanup()
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let delivery = self.platform.delivery();
        let bytes = self.platform.logical_owned_bytes()
            + arc_control_bytes::<Delivery>()
            + delivery.sink.logical_owned_bytes();
        #[cfg(target_os = "linux")]
        let bytes = bytes + delivery.events.as_ref().map_or(0, |sink| sink.logical_owned_bytes());
        bytes
    }

    /// True only after actual native quiescence and resource retirement.
    pub(crate) fn is_stopped(&self) -> bool {
        self.platform.is_stopped()
    }

    #[cfg(windows)]
    pub(crate) fn fence_for(&self, timeout: std::time::Duration) -> std::io::Result<()> {
        self.platform.fence_for(timeout)
    }

    /// Watches the root-relative directory `directory`, and every directory on the way to it, before the source reads beneath it.
    #[cfg(target_os = "linux")]
    pub(crate) fn admit(&self, directory: &std::path::Path) -> bool {
        self.platform.admit(directory)
    }

    /// Admits each Linux prefix with cancellation, native-call and allocation admission before the corresponding work.
    pub(crate) fn admit_measured(
        &self,
        directory: &std::path::Path,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, crate::demand::DemandError> {
        self.admit_with_work(directory, work)
    }

    pub(crate) fn admit_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        directory: &std::path::Path,
        work: &mut W,
    ) -> Result<bool, crate::demand::DemandError> {
        #[cfg(target_os = "linux")]
        return self.platform.admit_with_work(directory, work);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = directory;
            work.check()?;
            Ok(false)
        }
    }

    /// Returns once every change that completed before the call has been delivered.
    pub(crate) fn fence(&self) -> std::io::Result<()> {
        self.platform.fence()
    }

    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self, timeout: std::time::Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        self.platform.delivery().take_failure(work)?;
        #[cfg(target_os = "linux")]
        { self.platform.fence_events_with_work(timeout, work) }
        #[cfg(not(target_os = "linux"))]
        { self.platform.fence_with_work(timeout, work) }
    }

    /// Whether every change still reaches the sink.
    pub(crate) fn is_exact(&self) -> bool {
        self.platform.delivery().is_exact()
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, not(target_os = "linux")))]
#[test]
fn measured_admission_on_recursive_hosts_does_not_spend_work() -> Result<(), Box<dyn std::error::Error>> {
    struct Ignored;
    impl HostChangeSink for Ignored {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, _: &[HostChange], work: &mut crate::demand::DemandWork)
            -> Result<(), crate::demand::DemandError> { work.check() }
        #[cfg(windows)]
        fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut crate::demand::DemandWork)
            -> Result<(), crate::demand::DemandError> { work.check() }
    }
    let root = tempfile::tempdir()?;
    let watch = NativeSourceWatch::start(
        &crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?,
        Arc::new(Ignored),
    )?;
    let cancellation = crate::cancellation::CancellationToken::new();
    let mut uncancelled = crate::demand::DemandWork::new(crate::WorkBudget::default(), &cancellation);
    assert!(!watch.admit_measured(std::path::Path::new("not/read"), &mut uncancelled)?);
    assert_eq!(uncancelled.work, crate::WorkCounters::default());
    cancellation.cancel();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::default(), &cancellation);
    assert!(matches!(watch.admit_measured(std::path::Path::new("not/read"), &mut work),
        Err(crate::demand::DemandError::Cancelled)));
    assert_eq!(work.work, crate::WorkCounters::default());
    Ok(())
}
