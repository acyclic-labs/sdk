use super::*;

#[derive(Default)]
pub(super) struct FuseHandlerReports {
    pub(super) completed_future_request_work: crate::WorkCounters,
    pub(super) first_fatal: Option<(NativeMountError, crate::WorkCounters)>,
}

pub(super) fn fuse_verify_callback_guard(
    guard: Option<&Arc<dyn crate::native_mount::NativeMountCallbackGuard>>,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<(), NativeMountError> {
    let Some(guard) = guard else {
        return Ok(());
    };
    let work = work.ok_or(NativeMountError::ControlPending)?;
    crate::native_mount::adapter::CallbackRuntime::existing()?
        .verify_native_callback_guard(guard.as_ref(), work)
}

pub(super) fn fuse_same_native_view(
    left: &crate::NativeMaterializationWork,
    right: &crate::NativeMaterializationWork,
) -> bool {
    left.shares_allocation_context(right)
        && left.capture_origin().map(Weak::as_ptr) == right.capture_origin().map(Weak::as_ptr)
        && left.invocation_view_state().map(Arc::as_ptr)
            == right.invocation_view_state().map(Arc::as_ptr)
        && left.source_execution_view().map(Arc::as_ptr)
            == right.source_execution_view().map(Arc::as_ptr)
}

pub(super) fn fuse_native_source_original(
    source: &dyn MountFilesystem,
    candidate: Option<&crate::NativeMaterializationWork>,
) -> Result<Option<crate::NativeMaterializationWork>, NativeMountError> {
    let original = source.native_original_work()?;
    if let Some(original) = &original {
        if let Some(candidate) = candidate {
            if !fuse_same_native_view(original, candidate) {
                return Err(NativeMountError::ControlPending);
            }
        }
        original.admit(crate::WorkCounters::UNCHARGED)?;
    }
    Ok(original)
}

pub(super) struct ProjectionCore {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) handler_budget: crate::WorkBudget,
    pub(super) handler_cancellation: crate::CancellationToken,
    pub(super) startup_meter: Mutex<crate::native_mount::NativeMountMeterActivation>,
    pub(super) handler_failed: AtomicBool,
    pub(super) handler_error: Mutex<FuseHandlerReports>,
    pub(super) state: Mutex<ProjectionState>,
    pub(super) names: RwLock<()>,
    pub(super) invalidation: Condvar,
    pub(super) page_stored: Condvar,
    pub(super) stopping: AtomicBool,
    pub(super) origin: ViewOrigin,
    pub(super) notifier: OnceLock<Notifier>,
    pub(super) native_allocation: Option<crate::NativeAllocation>,
    pub(super) callback_guard: Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>>,
    pub(super) source_original_work: Option<crate::NativeMaterializationWork>,
    pub(super) allocation_custody: Option<crate::demand::NativeMountAllocationCustody>,
}

pub(in crate::native_mount) struct FuseStartupMeterScope {
    pub(super) core: Arc<ProjectionCore>,
    pub(super) meter: crate::NativeMaterializationWork,
    pub(super) generation: u64,
}

impl FuseStartupMeterScope {
    pub(super) fn new(
        core: &Arc<ProjectionCore>,
        meter: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let generation = core
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enter(meter)?;
        core.invalidation.notify_all();
        Ok(Self {
            core: Arc::clone(core),
            meter: meter.clone(),
            generation,
        })
    }
    /// Retains the original callback meter for accepted I/O.
    pub(in crate::native_mount) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::new(&self.core, &self.meter)
    }
}

impl Drop for FuseStartupMeterScope {
    fn drop(&mut self) {
        self.core
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .leave(&self.meter, self.generation);
    }
}

impl ProjectionCore {
    pub(super) fn new(
        source: Arc<dyn MountFilesystem>,
        state: ProjectionState,
        handler_budget: crate::WorkBudget,
        handler_cancellation: &crate::CancellationToken,
    ) -> Result<Self, NativeMountError> {
        let source_original_work = fuse_native_source_original(source.as_ref(), None)?;
        let callback_guard = source.native_callback_guard();
        fuse_verify_callback_guard(callback_guard.as_ref(), source_original_work.as_ref())?;
        Ok(Self {
            source,
            allocation_custody: None,
            handler_budget,
            handler_cancellation: handler_cancellation.clone(),
            startup_meter: Mutex::new(crate::native_mount::NativeMountMeterActivation::default()),
            handler_failed: AtomicBool::new(false),
            handler_error: Mutex::new(FuseHandlerReports::default()),
            state: Mutex::new(state),
            names: RwLock::new(()),
            invalidation: Condvar::new(),
            page_stored: Condvar::new(),
            stopping: AtomicBool::new(false),
            origin: ViewOrigin::new(),
            notifier: OnceLock::new(),
            native_allocation: None,
            callback_guard,
            source_original_work,
        })
    }
    pub(super) fn root(
        source: Arc<dyn MountFilesystem>,
        root: MountLookup,
        stamp: Option<ViewStamp>,
        writable: bool,
        uid: u32,
        gid: u32,
        native: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeMountError> {
        fuse_native_source_original(source.as_ref(), Some(native))?;
        let custody = crate::demand::NativeMountAllocationCustody::new(
            &mut crate::demand::NativeSourceWork::from_native_materialization_work(native),
        )
        .map_err(crate::native_mount::NativeMountStartupAdmissionError::from)?;
        let state = ProjectionState::new(AttributeDefaults {
            writable,
            mount_uid: uid,
            mount_gid: gid,
        });
        let (budget, cancellation) = {
            let work = native.native_read_meter().lock();
            (work.budget, work.cancellation().clone())
        };
        let mut core = Self::new(source, state, budget, &cancellation)?;
        core.allocation_custody = Some(custody);
        let entry = InodeEntry::new(FuseOwned::empty(MountPath::root()), root, stamp, 1, native)?;
        core.native_allocation = Some(native.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?);
        core.state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .by_inode = HashMap::from([(ROOT_INODE, entry)]);
        let bytes = std::mem::size_of::<(FuseOwned<MountPath>, u64)>() as u64;
        core.native_allocation
            .as_mut()
            .expect("original root table guard")
            .grow(bytes)?;
        core.state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .inode_by_path = HashMap::from([(FuseOwned::empty(MountPath::root()), ROOT_INODE)]);
        core.state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .inode_by_file = HashMap::from([(root.node.file_id, ROOT_INODE)]);
        Ok(Arc::new(core))
    }

    pub(super) fn state(&self) -> Result<MutexGuard<'_, ProjectionState>, i32> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV);
        }
        let state = self.state.lock().map_err(|_| libc::EIO)?;
        // Waiters observe queued mutations after reacquiring this lock.
        self.invalidation.notify_all();
        Ok(state)
    }

    pub(super) fn state_to_finish(&self) -> MutexGuard<'_, ProjectionState> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.invalidation.notify_all();
        state
    }

    pub(super) fn names(&self) -> Result<RwLockReadGuard<'_, ()>, i32> {
        self.names.read().map_err(|_| libc::EIO)
    }

    pub(super) fn names_exclusive(&self) -> Result<RwLockWriteGuard<'_, ()>, i32> {
        self.names.write().map_err(|_| libc::EIO)
    }

    pub(super) fn take_handler_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        let first = self
            .handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first_fatal
            .take();
        first.or_else(|| self.state_to_finish().invalidation.failure.take())
    }

    pub(super) fn completed_callback_work(&self) -> crate::WorkCounters {
        self.handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work
    }

    pub(super) fn retain_handler_failure(&self, failure: (NativeMountError, crate::WorkCounters)) {
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

    pub(super) fn conserve_completed_request(&self, work: &crate::WorkCounters) {
        let recorded = self
            .handler_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work
            .try_add_assign(work);
        if let Err(error) = recorded {
            self.retain_handler_failure((error.into(), *work));
        }
    }

    pub(super) fn record_handler_failure(
        &self,
        work: &crate::NativeMaterializationWork,
        error: NativeMountError,
    ) {
        if fatal_handler_error(&error) {
            let prefix = work.native_read_meter().lock().work;
            self.retain_handler_failure((error, prefix));
        }
    }

    pub(super) fn active_meter(&self) -> Option<crate::NativeMaterializationWork> {
        self.startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade()
    }

    pub(super) fn callback_meter(
        &self,
    ) -> Result<Option<crate::NativeMaterializationWork>, NativeMountError> {
        let active = self.active_meter();
        let candidate = active.as_ref().or(self.source_original_work.as_ref());
        let current = fuse_native_source_original(self.source.as_ref(), candidate)?;
        if self.source_original_work.is_some() != current.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        if let Some(original) = &self.source_original_work {
            let current = current
                .as_ref()
                .expect("installed native original remains present");
            if !fuse_same_native_view(original, current) {
                return Err(NativeMountError::ControlPending);
            }
        }
        let selected = candidate.cloned();
        if let Some(work) = &selected {
            work.admit(crate::WorkCounters::UNCHARGED)?;
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

    pub(super) fn claim_content(
        &self,
        file_id: crate::FileId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<ContentClaim<'_>, NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let mut state = self.state()?;
        while state.page_stores.in_flight.contains_key(&file_id) {
            work.admit(crate::WorkCounters::UNCHARGED)?;
            state = self.page_stored.wait(state).map_err(|_| libc::EIO)?;
            work.admit(crate::WorkCounters::UNCHARGED)?;
        }
        state.page_stores.claim(file_id, work)?;
        Ok(ContentClaim {
            core: self,
            file_id,
        })
    }
    pub(super) fn request<T>(
        &self,
        callback: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, NativeMountError>,
    ) -> Result<crate::OperationReceipt<T>, (NativeMountError, crate::WorkCounters)> {
        let _origin = self.origin.enter();
        let meter = self.callback_meter().map_err(|cause| {
            (
                cause,
                self.source_original_work
                    .as_ref()
                    .map_or(crate::WorkCounters::UNCHARGED, |work| {
                        work.native_read_meter().lock().work
                    }),
            )
        })?;
        let run = |work: &crate::NativeMaterializationWork| {
            work.admit(crate::WorkCounters::UNCHARGED)?;
            callback(work)
        };
        if let Some(meter) = meter {
            let result = crate::native_mount::run_native_mount_handler_on_meter(&meter, run);
            let work = meter.native_read_meter().lock().work;
            result
                .map(|value| crate::OperationReceipt { value, work })
                .map_err(|cause| (cause, work))
        } else {
            let result = crate::native_mount::run_native_mount_handler(
                self.handler_budget,
                &self.handler_cancellation,
                run,
            );
            let work = match &result {
                Ok(receipt) => &receipt.work,
                Err(failure) => &failure.1,
            };
            self.conserve_completed_request(work);
            result
        }
    }

    pub(super) fn finish<T>(
        &self,
        result: Result<crate::OperationReceipt<T>, (NativeMountError, crate::WorkCounters)>,
    ) -> Result<T, i32> {
        result.map(|receipt| receipt.value).map_err(|failure| {
            let errno = native_callback_errno(&failure.0);
            if fatal_handler_error(&failure.0) {
                self.retain_handler_failure(failure);
            }
            errno
        })
    }
}

pub(super) struct ContentClaim<'a> {
    pub(super) core: &'a ProjectionCore,
    pub(super) file_id: crate::FileId,
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

pub(super) struct FuseProjection {
    pub(super) core: Arc<ProjectionCore>,
    pub(super) skips_opendir: bool,
    pub(super) skips_open: bool,
}
