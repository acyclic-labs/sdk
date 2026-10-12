//! Native source change notifications.

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
pub(crate) use directory_changes::NativeSourceWatch;
#[cfg(target_os = "macos")]
pub(crate) use fsevents::NativeSourceWatch;
#[cfg(target_os = "linux")]
pub(crate) use inotify::NativeSourceWatch;

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
    fn retain_failed_platform(self: &Arc<Self>, error: DemandError, platform: NativeSourceWatch) -> DemandError {
        platform.delivery().exact.store(false, Ordering::Release);
        assert!(self.cause.set(error).is_ok(), "one startup control retains one actual native owner");
        let watch = platform;
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
    #[cfg(target_os = "linux")]
    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
        &self, timeout: std::time::Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        self.delivery().take_failure(work)?;
        self.fence_events_with_work(timeout, work)
    }

    /// Starts reporting changes beneath `root`, whose held directory is `directory`, to `sink`.
    pub(crate) fn start(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
    ) -> std::io::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        Self::start_with_work(directory, sink, &mut crate::demand::DemandWork::new(
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
        let result = Self::start_held_root_with_work(directory, delivery, complete, recursive, work);
        if !work.is_native_original() { work.release(bytes)?; }
        result
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
        let result = Self::start_driver(directory, delivery, work).and_then(|platform| {
            if let Some(failure) = failure {
                if let Err(error) = work.with_original_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::ReturnedItems, 1, 1)) {
                    return Err(failure.retain_failed_platform(error, platform));
                }
            }
            Ok(platform)
        });
        if !work.is_native_original() { work.release(bytes)?; }
        result
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
        let result = Self::start_driver(directory, delivery, work);
        if !work.is_native_original() { work.release(bytes)?; }
        result
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
        let result = Self::start_directory_driver(directory, delivery, complete, work);
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }

    #[cfg(windows)]
    pub(crate) fn owned_bytes(&self) -> u64 {
        self.platform_owned_bytes()
            + arc_control_bytes::<Delivery>()
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let delivery = self.delivery();
        let bytes = self.platform_owned_bytes()
            + arc_control_bytes::<Delivery>()
            + delivery.sink.logical_owned_bytes();
        #[cfg(target_os = "linux")]
        let bytes = bytes + delivery.events.as_ref().map_or(0, |sink| sink.logical_owned_bytes());
        bytes
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn admit_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        directory: &std::path::Path,
        work: &mut W,
    ) -> Result<bool, crate::demand::DemandError> {
        let _ = directory;
        work.check()?;
        Ok(false)
    }

    /// Whether every change still reaches the sink.
    pub(crate) fn is_exact(&self) -> bool {
        self.delivery().is_exact()
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
    assert!(!watch.admit_with_work(std::path::Path::new("not/read"), &mut uncancelled)?);
    assert_eq!(uncancelled.work, crate::WorkCounters::default());
    cancellation.cancel();
    let mut work = crate::demand::DemandWork::new(crate::WorkBudget::default(), &cancellation);
    assert!(matches!(watch.admit_with_work(std::path::Path::new("not/read"), &mut work),
        Err(crate::demand::DemandError::Cancelled)));
    assert_eq!(work.work, crate::WorkCounters::default());
    Ok(())
}
