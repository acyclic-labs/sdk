//! Native source change notifications.
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak, atomic::{AtomicBool, Ordering}};
use crate::demand::{DemandError, DemandWork, NativeSourceWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::WorkCounters;

mod callback_work;
mod startup_failure;
pub(crate) use callback_work::CallbackWork;
pub use startup_failure::SourceWatchStartupFailure;
#[cfg(windows)] mod directory_changes;
#[cfg(target_os = "macos")] mod fsevents;
#[cfg(target_os = "linux")] mod inotify;
#[cfg(windows)] pub(crate) use directory_changes::NativeSourceWatch;
#[cfg(target_os = "macos")] pub(crate) use fsevents::NativeSourceWatch;
#[cfg(target_os = "linux")] pub(crate) use inotify::NativeSourceWatch;

pub(super) const fn arc_control_bytes<T>() -> u64 {
    let alignment = std::mem::align_of::<T>();
    ((std::mem::size_of::<[usize; 2]>() + alignment - 1) & !(alignment - 1)) as u64 + std::mem::size_of::<T>() as u64
}

#[cfg(windows)]
pub(super) fn prepare_native_cleanup_credit<W: SourceDemandReadWork>(work: &mut W, maximum: WorkCounters)
    -> Result<Option<crate::materializer::NativeWorkReservation>, DemandError>
{
    work.check()?;
    if !work.is_native_original() { return Ok(None); }
    work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?.materialization_work()?
        .allocation_work().reserve_work(maximum).map(Some).map_err(crate::native_name::original_name_error)
}

fn cleanup_credits(delta: WorkCounters) -> [(crate::materializer::NativeResourceCredit, u64); 3] {
    use acyclic_native_runtime::NativeOperationKind::{Read, Write};
    [(DemandWork::operation_credit(Read), delta.backend_read_operations),
        (DemandWork::operation_credit(Write), delta.backend_write_operations),
        (crate::materializer::NativeResourceCredit::Items, delta.items_examined)]
}

pub(super) fn reserve_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) -> Result<(), DemandError> {
    let credits = cleanup_credits(delta);
    for (index, (kind, count)) in credits.iter().enumerate() {
        if let Err(mut error) = work.reserve(*kind, *count) {
            if index < 2 {
                let io = work.record_native_io_failure(error);
                error = work.take_native_io_failure().unwrap_or(DemandError::Io(io));
            }
            for (kind, count) in &credits[..index] { work.refund(*kind, *count).expect("balanced native cleanup reservation"); }
            return Err(error);
        }
    }
    Ok(())
}

pub(super) fn cancel_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) {
    for (kind, count) in cleanup_credits(delta) { work.refund(kind, count).expect("balanced native cleanup reservation"); }
}

pub(super) fn record_cleanup_credits(work: &mut DemandWork, delta: WorkCounters) -> Result<(), DemandError> {
    let mut result = Ok(());
    for (index, (kind, count)) in cleanup_credits(delta).into_iter().enumerate() {
        for _ in 0..count {
            let mut recorded = work.complete(kind, 1, 1);
            if index < 2 { recorded = recorded.map_err(|error| { let io = work.record_native_io_failure(error); work.take_native_io_failure().unwrap_or(DemandError::Io(io)) }); }
            result = result.and(recorded);
        }
    }
    result
}

/// One change the host reported beneath a watched root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HostChange {
    /// This root-relative path may bind another file; empty means the root.
    Rebound(PathBuf),
    /// This root-relative entry may have changed in place.
    Altered(PathBuf),
    /// The file with this index on the root's volume may have changed.
    #[cfg(windows)]
    File(u64),
    /// Notifications were lost: anything may have changed.
    Everything,
    /// Delivery may be incomplete, though no notification is known to be lost.
    #[cfg(any(target_os = "macos", windows))]
    Unconfirmed,
}


#[cfg(windows)]
pub(crate) enum HostChangeWide<'a> {
    Path { action: u32, path: &'a [u16] }, File(u64), Everything, Unconfirmed,
}

/// Receives host changes using the invoking watch's accounting context.
pub(crate) trait HostChangeSink: Send + Sync {
    fn logical_owned_bytes(&self) -> u64;
    fn host_changed(&self, changes: &[HostChange], work: &mut CallbackWork<'_>) -> Result<(), DemandError>;
    #[cfg(windows)]
    fn host_changed_wide(&self, changes: &[HostChangeWide<'_>], work: &mut CallbackWork<'_>) -> Result<(), DemandError>;
}

#[cfg(target_os = "linux")]
pub(crate) trait NativeEventSink: Send + Sync {
    fn logical_owned_bytes(&self) -> u64;
    fn event(&self, relative: &std::path::Path, mask: u32, cookie: usize, work: &mut CallbackWork<'_>) -> Result<(), DemandError>;
    fn invalidate(&self, work: &mut CallbackWork<'_>) -> Result<(), DemandError>;
}

struct CallbackFrame { delivery: *const Delivery, previous: *const CallbackFrame }
std::thread_local! { static ACTIVE_CALLBACK: std::cell::Cell<*const CallbackFrame> = const { std::cell::Cell::new(std::ptr::null()) }; }
struct RestoreCallback(*const CallbackFrame);
impl Drop for RestoreCallback { fn drop(&mut self) { ACTIVE_CALLBACK.with(|active| active.set(self.0)); } }

/// The existing delivery path and its bounded invocation state.
pub(crate) struct Delivery {
    sink: Arc<dyn HostChangeSink>, exact: AtomicBool,
    budget: crate::WorkBudget, cancellation: crate::CancellationToken,
    failure: Mutex<Option<(DemandError, Option<WorkCounters>)>>,
    #[cfg(target_os = "linux")] events: Option<Arc<dyn NativeEventSink>>,
    startup_failure: Mutex<Weak<SourceWatchStartupFailure>>,
    ownership: SourcePayloadOwnership,
    original: Option<crate::demand::OriginalDemandWork>,
}

impl Delivery {
    fn new(sink: Arc<dyn HostChangeSink>) -> Self {
        Self { sink, exact: AtomicBool::new(true), budget: crate::WorkBudget::UNBOUNDED,
            cancellation: crate::CancellationToken::new(), failure: Mutex::new(None), startup_failure: Mutex::new(Weak::new()),
            #[cfg(target_os = "linux")] events: None,
            ownership: SourcePayloadOwnership::default(), original: None }
    }
    fn new_with_work<W: SourceDemandReadWork>(sink: Arc<dyn HostChangeSink>, work: &mut W) -> Result<Self, DemandError> {
        let mut delivery = Self::new(sink);
        delivery.original = work.native_source_work().map(|work| work.original_demand_work().allocation_work());
        let controls = |work: &mut DemandWork| (work.budget, work.cancellation().clone());
        (delivery.budget, delivery.cancellation) = if delivery.original.is_some() { work.with_original_leaf(controls) }
            else { work.with_ordinary_work(controls).ok_or(DemandError::NativeWorkUnavailable)? };
        delivery.ownership = SourcePayloadOwnership::new(work)?;
        Ok(delivery)
    }
    fn prepare_startup_failure<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<Option<Arc<SourceWatchStartupFailure>>, DemandError> {
        if self.original.is_some() { self.prepare_failure(work).map(Some) } else { Ok(None) }
    }
    fn prepare_failure<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<Arc<SourceWatchStartupFailure>, DemandError> {
        if let Some(failure) = self.startup_failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).upgrade() { return Ok(failure); }
        let ownership = SourcePayloadOwnership::new(work)?;
        let bytes = arc_control_bytes::<SourceWatchStartupFailure>();
        work.allocate(bytes)?;
        let failure = Arc::new(SourceWatchStartupFailure { cause: std::sync::OnceLock::new(), watch: Mutex::new(None),
            owned_bytes: std::sync::atomic::AtomicU64::new(0), ownership });
        failure.ownership.retain(bytes, work)?;
        if !work.is_native_original() { work.release(bytes)?; }
        let mut pending = self.startup_failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = pending.upgrade() { return Ok(existing); }
        *pending = Arc::downgrade(&failure);
        Ok(failure)
    }
    fn callback_work<'a>(&'a self, ordinary: &'a mut Option<DemandWork>) -> CallbackWork<'a> {
        match &self.original {
            Some(original) => CallbackWork::Original(NativeSourceWork::from_original_work(original)),
            None => CallbackWork::Ordinary(ordinary.get_or_insert_with(|| DemandWork::new(self.budget, &self.cancellation))),
        }
    }
    fn birth_matches<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        let Some(birth) = &self.original else { return false; };
        let Some(current) = work.native_source_work() else { return false; };
        let current = current.original_demand_work();
        Arc::ptr_eq(&birth.state, &current.state) && match (birth.resource.as_ref(), current.resource.as_ref()) {
            (None, None) => true, (Some(birth), Some(current)) => Arc::ptr_eq(birth, current), _ => false,
        }
    }
    fn callback<T>(&self, invoke: impl FnOnce() -> T) -> T {
        let frame = CallbackFrame { delivery: self, previous: ACTIVE_CALLBACK.with(std::cell::Cell::get) };
        ACTIVE_CALLBACK.with(|active| active.set(&frame));
        let _restore = RestoreCallback(frame.previous);
        invoke()
    }
    #[allow(unsafe_code, reason = "reads this thread's live callback stack")]
    fn ensure_stop_available(&self) -> Result<(), DemandError> {
        ACTIVE_CALLBACK.with(|active| {
            let mut frame = active.get();
            while !frame.is_null() {
                // SAFETY: frames live on this thread's stack until the restore guard runs.
                let current = unsafe { &*frame };
                if std::ptr::eq(current.delivery, self) { return Err(DemandError::WorkerUnavailable); }
                frame = current.previous;
            }
            Ok(())
        })
    }
    fn failed_with_work<W: SourceDemandReadWork>(&self, error: DemandError, work: &mut W) {
        self.exact.store(false, Ordering::Release);
        let prior = if work.is_native_original() { None } else { Some(work.with_original_leaf(|work| work.work)) };
        let mut failure = self.failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() { *failure = Some((error, prior)); }
    }
    fn take_failure<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        if let Some((error, prior)) = self.failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            if let Some(prior) = prior { work.merge(prior)?; }
            return Err(error);
        }
        Ok(())
    }
    fn with_work<T, W: SourceDemandReadWork>(&self, work: &mut W, invoke: impl FnOnce(&mut CallbackWork<'_>) -> Result<T, DemandError>) -> Result<T, DemandError> {
        if let Some(native) = work.native_source_work() { return self.callback(|| invoke(&mut CallbackWork::Original(native))); }
        work.with_ordinary_work(|work| self.callback(|| invoke(&mut CallbackWork::Ordinary(work))))
            .unwrap_or(Err(DemandError::NativeWorkUnavailable))
    }
    fn deliver_with_work<W: SourceDemandReadWork>(&self, changes: &[HostChange], work: &mut W) -> Result<(), DemandError> {
        if changes.is_empty() { return Ok(()); }
        self.with_work(work, |work| self.sink.host_changed(changes, work))
    }
    fn invalidate_with_work<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        #[cfg(target_os = "linux")]
        if let Some(sink) = &self.events { return self.with_work(work, |work| sink.invalidate(work)); }
        self.deliver_with_work(&[HostChange::Everything], work)
    }
    #[cfg(target_os = "linux")]
    fn event_with_work<W: SourceDemandReadWork>(&self, relative: &std::path::Path, mask: u32, cookie: usize, work: &mut W) -> Result<(), DemandError> {
        let sink = self.events.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        self.with_work(work, |work| sink.event(relative, mask, cookie, work))
    }
    #[cfg(windows)]
    fn deliver_wide_with_work<W: SourceDemandReadWork>(&self, changes: &[HostChangeWide<'_>], work: &mut W) -> Result<(), DemandError> {
        if changes.is_empty() { return Ok(()); }
        self.with_work(work, |work| self.sink.host_changed_wide(changes, work))
    }
    fn deliver(&self, changes: &[HostChange]) {
        let mut ordinary = None;
        let mut work = self.callback_work(&mut ordinary);
        if let Err(error) = self.deliver_with_work(changes, &mut work) { self.failed_with_work(error, &mut work); }
    }
    fn abandon(&self) { if self.exact.swap(false, Ordering::AcqRel) { self.deliver(&[HostChange::Everything]); } }
    fn is_exact(&self) -> bool { self.exact.load(Ordering::Acquire) }
}

impl NativeSourceWatch {
    pub(crate) fn start(directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>) -> std::io::Result<Self> {
        Self::start_with_work(directory, sink, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)
    }
    pub(crate) fn start_with_work<W: SourceDemandReadWork>(directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, work: &mut W) -> Result<Self, DemandError> {
        Self::start_output(directory, sink, work, false)
    }
    pub(crate) fn start_with_output_with_work<W: SourceDemandReadWork>(directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, work: &mut W) -> Result<Self, DemandError> {
        Self::start_output(directory, sink, work, true)
    }
    fn start_output<W: SourceDemandReadWork>(directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, work: &mut W, returned: bool) -> Result<Self, DemandError> {
        let delivery = Delivery::new_with_work(sink, work)?;
        let bytes = arc_control_bytes::<Delivery>();
        work.allocate(bytes)?;
        let delivery = Arc::new(delivery);
        delivery.ownership.retain(bytes, work)?;
        let failure = if returned { Some(delivery.prepare_failure(work)?) } else { None };
        let result = Self::start_driver(directory, delivery, work).and_then(|owner| {
            if let Some(failure) = failure {
                if let Err(error) = work.with_original_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::ReturnedItems, 1, 1)) {
                    return Err(failure.retain_failed_platform(error, owner));
                }
            }
            Ok(owner)
        });
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn start_with_events_with_work<W: SourceDemandReadWork>(directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, events: Arc<dyn NativeEventSink>, work: &mut W) -> Result<Self, DemandError> {
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
    pub(crate) fn start_directory_with_work<W: SourceDemandReadWork>(directory: &cap_std::fs::Dir, sink: Arc<dyn HostChangeSink>, complete: bool, work: &mut W) -> Result<Self, DemandError> {
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
    pub(crate) fn start_held_with_sink_with_work<W: SourceDemandReadWork>(directory: &crate::native_host::HostRoot, sink: Arc<dyn HostChangeSink>, complete: bool, recursive: bool, work: &mut W) -> Result<Self, DemandError> {
        let delivery = Delivery::new_with_work(sink, work)?;
        let bytes = arc_control_bytes::<Delivery>();
        work.allocate(bytes)?;
        let delivery = Arc::new(delivery);
        delivery.ownership.retain(bytes, work)?;
        let result = Self::start_held_root_with_work(directory, delivery, complete, recursive, work);
        if !work.is_native_original() { work.release(bytes)?; }
        result
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(&self, timeout: std::time::Duration, work: &mut W) -> Result<bool, DemandError> {
        work.check()?;
        self.delivery().take_failure(work)?;
        self.fence_events_with_work(timeout, work)
    }
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn admit_with_work<W: SourceDemandReadWork>(&self, _: &std::path::Path, work: &mut W) -> Result<bool, DemandError> { work.check()?; Ok(false) }
    #[cfg(windows)]
    pub(crate) fn owned_bytes(&self) -> u64 { self.platform_owned_bytes() + arc_control_bytes::<Delivery>() }
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let delivery = self.delivery();
        let bytes = self.platform_owned_bytes() + arc_control_bytes::<Delivery>() + delivery.sink.logical_owned_bytes();
        #[cfg(target_os = "linux")]
        let bytes = bytes + delivery.events.as_ref().map_or(0, |sink| sink.logical_owned_bytes());
        bytes
    }
    pub(crate) fn is_exact(&self) -> bool { self.delivery().is_exact() }
}

#[cfg(test)] mod tests;
