use super::*;
use crate::native_mount::{
    NativeMountCallbackGuard, NativeMountMeterActivation, NativeMountStartupAdmissionError,
    run_native_mount_handler, run_native_mount_handler_on_meter, view_ledger::same_work,
};
use crate::sync::{Mutex, atomic::AtomicBool};
use crate::{NativeAllocation, NativeMaterializationWork as Work, WorkCounters};

pub(super) fn callback_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::Source(error) => crate::native_mount::errno(error),
        NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            crate::native_mount::errno(error)
        }
        NativeMountError::Materialization(error) => {
            crate::native_mount::native_materialization_errno(error)
        }
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Allocation(_) => libc::ENOMEM,
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ESTALE,
        NativeMountError::FuseUnsupportedCapabilities(_) => libc::EOPNOTSUPP,
        _ => libc::EIO,
    }
}

#[derive(Default)]
pub(super) struct FuseHandlerReports {
    pub(super) completed_future_request_work: WorkCounters,
    pub(super) first_fatal: Option<(NativeMountError, WorkCounters)>,
}

#[cfg(test)]
thread_local! {
    /// Inodes whose attributes this thread's state guards dropped, so a test
    /// without a kernel sees each invalidation as it is delivered.
    pub(super) static DROPPED_ATTRIBUTES: std::cell::RefCell<Vec<u64>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

pub(super) struct StateGuard<'a> {
    state: Option<RankedGuard<'a, ProjectionState>>,
    core: &'a ProjectionCore,
}

impl std::ops::Deref for StateGuard<'_> {
    type Target = ProjectionState;
    fn deref(&self) -> &ProjectionState {
        match &self.state {
            Some(state) => state,
            None => unreachable!("the guard owns its lock until drop"),
        }
    }
}

impl std::ops::DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut ProjectionState {
        match &mut self.state {
            Some(state) => state,
            None => unreachable!("the guard owns its lock until drop"),
        }
    }
}

impl StateGuard<'_> {
    #[track_caller]
    pub(super) fn wait_timeout(
        mut self,
        condition: &RankedCondvar,
        duration: Duration,
    ) -> Result<Self, NativeMountError> {
        let Some(state) = self.state.take() else {
            unreachable!("the guard owns its lock until drop");
        };
        self.core.invalidation.notify_all();
        #[cfg(all(test, acyclic_fs_loom))]
        invalidation::model_checking::before_waiter_wait();
        self.state = Some(
            condition
                .wait_timeout(state, duration)
                .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?
                .0,
        );
        Ok(self)
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        let Some(mut state) = self.state.take() else {
            return;
        };
        let mut superseded = std::mem::take(&mut state.superseded_attributes);
        drop(state);
        #[cfg(test)]
        DROPPED_ATTRIBUTES.with_borrow_mut(|dropped| {
            dropped.extend(superseded.iter().map(|(inode, _)| *inode));
        });
        #[cfg(all(test, acyclic_fs_loom))]
        let notify = !invalidation::model_checking::omit_guard_notification();
        #[cfg(not(all(test, acyclic_fs_loom)))]
        let notify = true;
        if notify {
            self.core.invalidation.notify_all();
        }
        if let Some(notifier) = self.core.notifier.get() {
            for (inode, credit) in superseded.iter_mut() {
                // Drop only attributes, so this never waits for kernel pages or requests.
                let _ = notifier.inval_inode(INodeNo(*inode), -1, 0);
                credit.record_deferred(WorkCounters {
                    backend_write_operations: 1,
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                });
            }
        }
    }
}

pub(super) fn fuse_native_source_original(
    source: &dyn MountFilesystem,
    candidate: Option<&Work>,
) -> Result<Option<Work>, NativeMountError> {
    let original = source.native_original_work()?;
    if let Some(original) = &original {
        if candidate.is_some_and(|work| !same_work(original, work)) {
            return Err(NativeMountError::ControlPending);
        }
        original.admit(WorkCounters::UNCHARGED)?;
    }
    Ok(original)
}

fn verify_guard(
    guard: Option<&Arc<dyn NativeMountCallbackGuard>>,
    work: Option<&Work>,
) -> Result<(), NativeMountError> {
    if let Some(guard) = guard {
        let work = work.ok_or(NativeMountError::ControlPending)?;
        crate::native_mount::adapter::CallbackRuntime::existing()?
            .verify_callback(work, || guard.check(work))?;
    }
    Ok(())
}

pub(super) struct ProjectionCore {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) clock: super::super::clock::Clock,
    pub(super) state: RankedMutex<ProjectionState, VIEW>,
    pub(super) names: RwLock<()>,
    pub(super) invalidation: RankedCondvar,
    pub(super) notifier: OnceLock<Notifier>,
    pub(super) stopping: AtomicBool,
    pub(super) origin: ViewOrigin,
    pub(super) startup_meter: Mutex<NativeMountMeterActivation>,
    pub(super) handler_failed: AtomicBool,
    pub(super) handler_error: Mutex<FuseHandlerReports>,
    pub(super) callback_guard: Option<Arc<dyn NativeMountCallbackGuard>>,
    source_original_work: Option<Work>,
    allocation_custody: Arc<crate::demand::NativeMountAllocationCustody>,
    _allocation: NativeAllocation,
}

/// Retains the original callback meter for accepted native I/O.
#[must_use]
pub struct NativeMountWorkScope {
    core: Arc<ProjectionCore>,
    meter: Work,
    generation: u64,
}

impl NativeMountWorkScope {
    pub(super) fn new(core: &Arc<ProjectionCore>, meter: &Work) -> Result<Self, NativeMountError> {
        let generation = core
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enter(meter)?;
        let _state = core.state_to_finish();
        Ok(Self {
            core: Arc::clone(core),
            meter: meter.clone(),
            generation,
        })
    }
    /// Retains the original callback meter for accepted I/O.
    pub fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::new(&self.core, &self.meter)
    }
}

impl Drop for NativeMountWorkScope {
    fn drop(&mut self) {
        self.core
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .leave(&self.meter, self.generation);
        let _state = self.core.state_to_finish();
    }
}

impl ProjectionCore {
    pub(super) fn new(
        source: Arc<dyn MountFilesystem>,
        root: MountLookup,
        stamp: Option<ViewStamp>,
        writable: bool,
        uid: u32,
        gid: u32,
        work: &Work,
    ) -> Result<Arc<Self>, NativeMountError> {
        let source_original_work = fuse_native_source_original(source.as_ref(), Some(work))?;
        let callback_guard = source.native_callback_guard();
        verify_guard(callback_guard.as_ref(), source_original_work.as_ref())?;
        let allocation_custody =
            crate::demand::NativeMountAllocationCustody::new(&mut work.clone())
                .map_err(NativeMountStartupAdmissionError::from)?;
        let allocation = work.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        let mut state = ProjectionState::new(writable, uid, gid);
        state.next_inode = ROOT_INODE;
        state.intern_projected(
            FuseOwned::empty(MountPath::root()),
            &root,
            stamp,
            true,
            work,
        )?;
        Ok(Arc::new(Self {
            source,
            clock: super::super::clock::Clock::capture(),
            state: RankedMutex::new(state),
            names: RwLock::new(()),
            invalidation: RankedCondvar::new(),
            notifier: OnceLock::new(),
            stopping: AtomicBool::new(false),
            origin: ViewOrigin::new(),
            startup_meter: Mutex::new(NativeMountMeterActivation::default()),
            handler_failed: AtomicBool::new(false),
            handler_error: Mutex::new(FuseHandlerReports::default()),
            callback_guard,
            source_original_work,
            allocation_custody,
            _allocation: allocation,
        }))
    }

    pub(super) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        let _state = self.state_to_finish();
    }

    #[track_caller]
    pub(super) fn state(&self) -> Result<StateGuard<'_>, i32> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV);
        }
        let state = self.state.lock().map_err(|_| libc::EIO)?;
        Ok(StateGuard {
            state: Some(state),
            core: self,
        })
    }

    #[track_caller]
    pub(super) fn state_to_finish(&self) -> StateGuard<'_> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        StateGuard {
            state: Some(state),
            core: self,
        }
    }

    pub(super) fn names(&self) -> Result<RwLockReadGuard<'_, ()>, i32> {
        self.names.read().map_err(|_| libc::EIO)
    }

    pub(super) fn names_exclusive(&self) -> Result<RwLockWriteGuard<'_, ()>, i32> {
        self.names.write().map_err(|_| libc::EIO)
    }

    pub(super) fn take_handler_failure(&self) -> Option<(NativeMountError, WorkCounters)> {
        self.handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first_fatal
            .take()
    }

    pub(super) fn completed_callback_work(&self) -> WorkCounters {
        self.handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work
    }

    pub(super) fn retain_handler_failure(&self, failure: (NativeMountError, WorkCounters)) {
        self.handler_failed.store(true, Ordering::Release);
        let mut reports = self
            .handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if reports.first_fatal.is_none() {
            reports.first_fatal = Some(failure);
        }
        self.invalidation.notify_all();
    }

    pub(super) fn callback_meter(&self) -> Result<Option<Work>, NativeMountError> {
        let active = self
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade();
        let candidate = active.as_ref().or(self.source_original_work.as_ref());
        let current = fuse_native_source_original(self.source.as_ref(), candidate)?;
        match (&self.source_original_work, &current) {
            (None, None) => (),
            (Some(original), Some(current)) if same_work(original, current) => (),
            _ => return Err(NativeMountError::ControlPending),
        }
        let current_guard = self.source.native_callback_guard();
        match (&self.callback_guard, &current_guard) {
            (None, None) => (),
            (Some(original), Some(current)) if Arc::ptr_eq(original, current) => (),
            _ => return Err(NativeMountError::ControlPending),
        }
        verify_guard(self.callback_guard.as_ref(), candidate)?;
        Ok(candidate.cloned())
    }

    #[allow(
        clippy::result_large_err,
        reason = "The original callback failure and counters stay inline without an unadmitted allocation."
    )]
    pub(super) fn request<T>(
        &self,
        callback: impl FnOnce(&Work) -> Result<T, NativeMountError>,
    ) -> Result<T, (NativeMountError, WorkCounters)> {
        let _origin = self.origin.enter();
        let meter = self.callback_meter().map_err(|cause| {
            (
                cause,
                self.source_original_work
                    .as_ref()
                    .map_or(WorkCounters::UNCHARGED, |work| work.state.lock().work),
            )
        })?;
        let run = |work: &Work| {
            work.admit(WorkCounters::UNCHARGED)?;
            callback(work)
        };
        if let Some(meter) = meter {
            return run_native_mount_handler_on_meter(&meter, run)
                .map_err(|cause| (cause, meter.state.lock().work));
        }
        let (budget, cancellation) = {
            let work = self._allocation.work().state.lock();
            (work.budget, work.cancellation().clone())
        };
        let result = run_native_mount_handler(budget, &cancellation, run)
            .map_err(|(error, work)| (error, *work));
        let prefix = match &result {
            Ok(receipt) => receipt.work,
            Err(failure) => failure.1,
        };
        let recorded = self
            .handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work
            .try_add_assign(&prefix);
        if let Err(error) = recorded {
            self.retain_handler_failure((error.into(), prefix));
        }
        result.map(|receipt| receipt.value)
    }

    pub(super) fn finish<T>(
        &self,
        result: Result<T, (NativeMountError, WorkCounters)>,
    ) -> Result<T, i32> {
        result.map_err(|failure| {
            let errno = callback_errno(&failure.0);
            let reply_only = match &failure.0 {
                NativeMountError::Io(_) => true,
                NativeMountError::Source(_)
                | NativeMountError::Demand(crate::DemandError::Mount(_)) => {
                    let error = match &failure.0 {
                        NativeMountError::Source(error) => error,
                        NativeMountError::Demand(crate::DemandError::Mount(error)) => {
                            error.as_ref()
                        }
                        _ => unreachable!(),
                    };
                    match error.semantic_error() {
                        MountSourceError::Io(_, _)
                        | MountSourceError::Stale
                        | MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale) => true,
                        MountSourceError::NamespacePath(
                            crate::kernel::NamespacePathError::Work(_)
                            | crate::kernel::NamespacePathError::Cancelled
                            | crate::kernel::NamespacePathError::AllocationFailed,
                        ) => false,
                        _ => !matches!(
                            errno,
                            libc::EIO
                                | libc::ENOMEM
                                | libc::ECANCELED
                                | libc::ESTALE
                                | libc::EACCES
                        ),
                    }
                }
                NativeMountError::Materialization(error) => {
                    !crate::native_mount::native_materialization_failure_is_fatal(error)
                }
                _ => false,
            };
            if !reply_only {
                self.retain_handler_failure(failure);
            }
            errno
        })
    }
}

impl ViewObserver for ProjectionCore {
    fn allocation_custody(&self) -> Option<Arc<crate::demand::NativeMountAllocationCustody>> {
        Some(self.allocation_custody.clone())
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

pub(super) struct FuseProjection {
    pub(super) core: Arc<ProjectionCore>,
}
