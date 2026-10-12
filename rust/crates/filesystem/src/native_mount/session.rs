use super::*;

/// Process-owned mounted namespace.
pub struct NativeMountSession {
    // Native producers retire before source data and the destination fence.
    pub(super) driver: Option<DriverSession>,
    pub(super) startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    pub(super) retained_source: Option<Arc<dyn MountFilesystem>>,
    pub(super) request: NativeMountRequest,
    pub(super) destination_guard: Option<MountDestinationGuard>,
    pub(super) measured_cleanup_failed: bool,
    pub(super) namespace_detach_confirmed: bool,
    pub(super) completed_future_callback_work: crate::WorkCounters,
    pub(super) retired_callback_failure:
        std::sync::Mutex<Option<(NativeMountError, crate::WorkCounters)>>,
    // SDK namespace DATA can outlive physical retirement without retaining
    // a source, context, root handle, or provider callback.
    #[cfg(windows)]
    pub(super) namespace_outcome: Option<projfs::ProjFsNamespaceOutcome>,
    #[cfg(windows)]
    pub(super) namespace_binding: Option<NativeMountNamespaceBinding>,
    // Destination data and all native owners physically drop before its guard.
    pub(super) _destination_custody: Option<crate::NativeAllocation>,
}

impl NativeMountSession {
    pub(super) fn new(
        request: NativeMountRequest,
        retained_source: Option<Arc<dyn MountFilesystem>>,
        destination_custody: Option<crate::NativeAllocation>,
    ) -> Self {
        Self {
            request,
            driver: None,
            destination_guard: None,
            retained_source,
            startup_source_watch: None,
            measured_cleanup_failed: false,
            namespace_detach_confirmed: false,
            completed_future_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: std::sync::Mutex::new(None),
            #[cfg(windows)]
            namespace_outcome: None,
            #[cfg(windows)]
            namespace_binding: None,
            _destination_custody: destination_custody,
        }
    }

    /// Activates the original meter without borrowing this session across I/O.
    pub fn begin_original_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<NativeMountWorkScope, NativeMountError> {
        let driver = self
            .driver
            .as_ref()
            .ok_or_else(|| NativeMountError::Io(std::io::ErrorKind::NotConnected.into()))?;
        with_driver!(
            driver,
            session,
            session.begin_original_work(work),
            Err(NativeMountError::UnsupportedTarget),
        )
    }

    /// Authenticated active kernel-root identity captured by this driver during its original constructor.
    #[must_use]
    pub fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        self.driver
            .as_ref()
            .and_then(|driver| with_driver!(driver, session, session.native_root_identity(), None))
    }

    /// Stable mount identity.
    #[must_use]
    pub const fn mount_id(&self) -> MountId {
        self.request.mount_id
    }

    /// Exact mounted destination.
    #[must_use]
    pub fn destination(&self) -> &Path {
        &self.request.destination
    }

    /// Records a genuine failure in the enclosing measured cleanup stage.
    pub fn record_measured_cleanup_failure(&mut self) {
        self.measured_cleanup_failed = true;
    }

    /// Borrows actual cleanup identity, retained ownership, and detach evidence.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountCleanupState<'_> {
        let (driver_fence, driver_source) = self.driver.as_ref().map_or((false, false), |driver| {
            with_driver!(
                driver,
                session,
                session.retirement_ownership(),
                (false, false)
            )
        });
        NativeMountCleanupState {
            mount_id: self.request.mount_id,
            destination: &self.request.destination,
            driver_retained: self.driver.is_some(),
            destination_fence_retained: self.destination_guard.is_some() || driver_fence,
            source_retained: self.retained_source.is_some()
                || self.startup_source_watch.is_some()
                || driver_source,
            namespace_detach_confirmed: self
                .driver
                .as_ref()
                .map_or(self.namespace_detach_confirmed, |driver| {
                    with_driver!(driver, session, session.namespace_detach_confirmed(), false)
                }),
            measured_cleanup_failed: self.measured_cleanup_failed,
        }
    }

    #[cfg(windows)]
    pub(super) fn take_retired_namespace_outcome(
        &mut self,
    ) -> Option<projfs::ProjFsNamespaceOutcome> {
        if !self.cleanup_state().cleanup_complete() {
            return None;
        }
        self.namespace_outcome.take()
    }

    /// Moves the first retained fatal SDK callback failure and its actual completed request work.
    pub fn take_callback_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        let retained = self
            .retired_callback_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(failure) = retained {
            return Some(failure);
        }
        self.driver
            .as_ref()
            .and_then(|driver| with_driver!(driver, session, session.take_callback_failure(), None))
    }

    /// Completed future SDK request work, including ordinary errno answers.
    #[must_use]
    pub fn completed_callback_work(&self) -> crate::WorkCounters {
        self.driver
            .as_ref()
            .map_or(self.completed_future_callback_work, |driver| {
                with_driver!(
                    driver,
                    session,
                    session.completed_callback_work(),
                    crate::WorkCounters::UNCHARGED
                )
            })
    }

    /// Flushes the actual SDK callbacks without borrowing work across a wait.
    pub fn flush_callbacks(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        match self.driver.as_ref() {
            #[cfg(target_os = "windows")]
            Some(session) => session.flush_callbacks(work),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Some(_) => Ok(()),
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            Some(_) => {
                let _ = work;
                Err(NativeMountError::UnsupportedTarget)
            }
            None => Ok(()),
        }
    }

    /// Revalidates on the same original shared callback and control meter.
    pub fn revalidate(&self, work: &NativeMaterializationWork) -> Result<(), NativeMountError> {
        let driver = self
            .driver
            .as_ref()
            .ok_or_else(|| NativeMountError::Io(std::io::ErrorKind::NotConnected.into()))?;
        with_driver!(
            driver,
            session,
            session.revalidate(work),
            Err(NativeMountError::UnsupportedTarget)
        )
    }

    /// Drops the kernel's cached entry/attributes for one mount-relative path (leading `/` optional).
    pub fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let driver = self
            .driver
            .as_ref()
            .ok_or_else(|| NativeMountError::Driver("session is stopped".to_owned()))?;
        with_driver!(
            driver,
            session,
            session.invalidate(path),
            session.invalidate(
                path,
                &NativeMaterializationWork::new(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .map_err(|(cause, _)| projfs::detach_work_error(cause))?
            ),
            Err(NativeMountError::UnsupportedTarget)
        )
    }

    /// Detaches the kernel namespace and ends the driver session; returns whether a live session was actually stopped.
    pub fn stop(&mut self) -> Result<bool, NativeMountError> {
        if self.cleanup_state().cleanup_complete() {
            return Ok(false);
        }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.measured_cleanup_failed = true;
            return Err(error.into());
        }
        self.stop_measured(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map(|receipt| receipt.value)
        .map_err(|(error, _work)| error)
    }

    /// Detaches through the existing platform driver and destination-fence machinery, admitting every actual native leaf on one original ledger.
    pub fn stop_measured(
        &mut self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((error, work)) => {
                self.measured_cleanup_failed = true;
                return Err((error.into(), work));
            }
        };
        match self.stop_with_meter(&work) {
            Ok(value) => Ok(crate::OperationReceipt {
                value,
                work: work.receipt(),
            }),
            Err(error) => Err((error, work.receipt())),
        }
    }

    /// Continues the same physical cleanup engine on the original shared Arc.
    pub fn stop_with_meter(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(NativeSessionStopWork::Meter(work))
    }

    /// Retires the original namespace using its before-start accepted controls.
    pub fn stop_with_cleanup_wait(
        &mut self,
        wait: &crate::CancellationToken,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(NativeSessionStopWork::AcceptedWait(wait))
    }

    pub(super) fn stop_with_context(
        &mut self,
        work: NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            let mut changed = false;
            if let Some(driver) = self.driver.as_mut() {
                with_driver!(
                    &mut *driver,
                    session,
                    session.stop(&work),
                    session.stop(match &work {
                        NativeSessionStopWork::Meter(meter) => meter.cancellation(),
                        NativeSessionStopWork::AcceptedWait(wait) => wait,
                    }),
                    Err(NativeMountError::UnsupportedTarget)
                )?;
                self.namespace_detach_confirmed = with_driver!(
                    &*driver,
                    session,
                    session.namespace_detach_confirmed(),
                    false
                );
                // Copy settled callback totals and faults before dropping the driver context.
                self.completed_future_callback_work = with_driver!(
                    &*driver,
                    session,
                    session.completed_callback_work(),
                    crate::WorkCounters::UNCHARGED
                );
                *self
                    .retired_callback_failure
                    .get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    with_driver!(&*driver, session, session.take_callback_failure(), None);
                #[cfg(windows)]
                {
                    self.namespace_outcome = driver.take_namespace_outcome();
                }
                drop(self.driver.take());
                changed = true;
            }
            if let Some(source) = self.retained_source.as_ref() {
                match &work {
                    NativeSessionStopWork::Meter(meter) => source.shutdown_with_meter(meter),
                    NativeSessionStopWork::AcceptedWait(wait) => {
                        source.shutdown_with_cleanup_wait(wait)
                    }
                }
                .map_err(NativeMountError::from)?;
                // The source confirmed actual watcher quiescence before this
                // owner releases its Arc; failed/cancelled joins keep it intact.
                drop(self.retained_source.take());
                changed = true;
            }
            if let Some(owner) = self.startup_source_watch.as_mut() {
                let stopped = if owner.is_stopped() {
                    Ok(false)
                } else {
                    match &work {
                        NativeSessionStopWork::Meter(meter) => owner
                            .stop(&mut adapter::NativeDemandCarrier::Native(meter))
                            .map_err(NativeMountError::from),
                        NativeSessionStopWork::AcceptedWait(wait) => owner
                            .stop_with_cleanup_wait(wait)
                            .map_err(NativeMountError::from),
                    }
                };
                if owner.is_stopped() {
                    // Terminal transfer of already owned data is not a new
                    // native permission, and survives a late admission error.
                    drop(self.startup_source_watch.take());
                    changed = true;
                }
                stopped?;
            }
            if let Some(guard) = self.destination_guard.as_mut() {
                match &work {
                    NativeSessionStopWork::Meter(meter) => guard.release(&mut &**meter)?,
                    NativeSessionStopWork::AcceptedWait(wait) => {
                        guard.release_cleanup(Some(wait))?
                    }
                }
                drop(self.destination_guard.take());
                changed = true;
            }
            Ok(changed)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }
}

pub(super) enum NativeSessionStopWork<'a> {
    Meter(&'a NativeMaterializationWork),
    AcceptedWait(&'a crate::CancellationToken),
}
