use super::*;

/// Process-owned mounted namespace.
pub struct NativeMountSession {
    // Native producers retire before source data and the destination fence.
    pub(super) driver: Option<DriverSession>,
    pub(super) startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    pub(super) retained_source: Option<Arc<dyn MountFilesystem>>,
    pub(super) terminal_source_failure: Option<NativeMountError>,
    pub(super) terminal_failures: retirement::TerminalFailures,
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
    // Destination data and all native owners physically drop before its guard.
    pub(super) _destination_custody: Option<crate::NativeAllocation>,
}

/// Synchronous SDK controls that async callers submit to an owned worker.
#[derive(Clone, Copy)]
pub(in crate::native_mount) enum NativeSessionControl {
    FlushCallbacks,
    Revalidate,
}

impl NativeMountSession {
    /// Recognizes projection staging still owned by this mount's held resources.
    pub fn owns_parent_entry(
        &self,
        entry: &crate::native_host::HostListedEntry,
        work: &NativeMaterializationWork,
    ) -> Result<bool, crate::NativeTreeMaterializationError> {
        // Ownership fences live in the registry, never in the destination's
        // parent. No authored sibling can therefore be an ownership fence.
        #[cfg(windows)]
        if let Some(driver) = &self.driver {
            return driver.owns_parent_entry(entry, work);
        }
        #[cfg(not(windows))]
        let _ = (entry, work);
        Ok(false)
    }

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
            terminal_source_failure: None,
            terminal_failures: retirement::TerminalFailures::default(),
            startup_source_watch: None,
            measured_cleanup_failed: false,
            namespace_detach_confirmed: false,
            completed_future_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: std::sync::Mutex::new(None),
            #[cfg(windows)]
            namespace_outcome: None,
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

    pub(super) fn take_retired_failures(&mut self) -> Option<retirement::TerminalFailures> {
        self.cleanup_state()
            .cleanup_complete()
            .then(|| std::mem::take(&mut self.terminal_failures))
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

    pub(super) fn capture_host_paths(
        &self,
        paths: &[MountPath],
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        #[cfg(target_os = "windows")]
        {
            self.driver
                .as_ref()
                .ok_or(NativeMountError::ControlPending)?
                .capture_host_paths(paths, work)
        }
        #[cfg(not(target_os = "windows"))]
        {
            self.retained_source
                .as_ref()
                .ok_or(NativeMountError::ControlPending)?
                .capture_host_paths_with_meter(
                    self.destination(),
                    paths,
                    self.native_root_identity()
                        .ok_or(NativeMountError::ControlPending)?,
                    work,
                )
                .map_err(NativeMountError::Source)
        }
    }

    pub(in crate::native_mount) fn start_control(
        &self,
        work: &NativeMaterializationWork,
        control: NativeSessionControl,
    ) -> Result<
        Option<acyclic_native_runtime::NativeCompletion<Result<(), NativeMountError>>>,
        NativeMountError,
    > {
        #[cfg(windows)]
        {
            match self.driver.as_ref() {
                Some(session) => session.start_control(work, control).map(Some),
                None => {
                    if matches!(control, NativeSessionControl::Revalidate) {
                        self.revalidate(work)?;
                    }
                    Ok(None)
                }
            }
        }
        #[cfg(not(windows))]
        {
            match control {
                NativeSessionControl::FlushCallbacks => self.flush_callbacks(work)?,
                NativeSessionControl::Revalidate => self.revalidate(work)?,
            }
            Ok(None)
        }
    }

    /// Flushes the actual SDK callbacks without borrowing work across a wait.
    pub fn flush_callbacks(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        #[cfg(not(windows))]
        let _ = work;
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
                .map_err(|(cause, _)| NativeMountError::from(cause))?
            ),
            Err(NativeMountError::UnsupportedTarget)
        )
    }

    /// Detaches the kernel namespace and ends the driver session; returns whether a live session was actually stopped.
    pub fn stop(&mut self) -> Result<bool, NativeMountError> {
        if self.cleanup_state().cleanup_complete() && self.terminal_failures.is_empty() {
            return Ok(false);
        }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.measured_cleanup_failed = true;
            return Err(error.into());
        }
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| {
            self.measured_cleanup_failed = true;
            NativeMountError::from(error)
        })?;
        self.stop_with_meter(&work)
    }

    /// Continues the same physical cleanup engine on the original shared Arc.
    pub fn stop_with_meter(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(&NativeSessionStopWork::Meter(work))
    }

    /// Retires the original namespace using its before-start accepted controls.
    pub fn stop_with_cleanup_wait(
        &mut self,
        wait: &crate::CancellationToken,
    ) -> Result<bool, NativeMountError> {
        self.stop_with_context(&NativeSessionStopWork::AcceptedWait(wait))
    }

    pub(super) fn stop_with_context(
        &mut self,
        work: &NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            let changed = self.retire_with_context(work)?;
            match self.terminal_failures.take() {
                Some(error) => Err(error),
                None => Ok(changed),
            }
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }

    pub(super) fn retire_with_context(
        &mut self,
        work: &NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        let changed = self.stop_namespace_with_context(work)?;
        Ok(self.release_destination_with_context(work)? || changed)
    }

    // The destination fence remains owned after physical namespace retirement.
    // Customer mounts finish their held-entry cleanup before releasing it.
    pub(super) fn stop_namespace_with_context(
        &mut self,
        work: &NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            let mut changed = false;
            if let Some(driver) = self.driver.as_mut() {
                self.terminal_failures = with_driver!(
                    &mut *driver,
                    session,
                    session.stop(work),
                    session.stop(work),
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
                    if let Some(outcome) = self.namespace_outcome.as_mut() {
                        self.terminal_failures = std::mem::take(&mut outcome.failures);
                    }
                }
                drop(self.driver.take());
                changed = true;
            }
            changed |= retirement::retire_sources(
                &mut self.retained_source,
                &mut self.startup_source_watch,
                &mut self.terminal_source_failure,
                work,
                retirement::SourceRetirementWait::Caller,
            )?;
            Ok(changed)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }
    pub(super) fn prepare_destination_cleanup(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if self.request.shared {
            // A shared view owns a namespace lease, not a mount destination.
            // Its driver retires that lease; only the shared instance may
            // clean up the physical root and release its destination fence.
            return Ok(());
        }
        if let Some(guard) = self.destination_guard.as_mut() {
            return guard.prepare_destination_cleanup(&self.request.destination, work);
        }
        let driver = self
            .driver
            .as_mut()
            .ok_or(NativeMountError::ControlPending)?;
        with_driver!(
            driver,
            session,
            session.prepare_destination_cleanup(&self.request.destination, work),
            Err(NativeMountError::UnsupportedTarget),
        )
    }

    #[cfg(windows)]
    pub(super) fn prepare_projection_cleanup(&mut self) -> Result<(), NativeMountError> {
        self.driver
            .as_mut()
            .map_or(Ok(()), |driver| driver.prepare_projection_cleanup())
    }

    pub(super) fn release_destination_with_context(
        &mut self,
        work: &NativeSessionStopWork<'_>,
    ) -> Result<bool, NativeMountError> {
        if self.destination_guard.is_none() {
            return retirement::finish_retirement(&mut self.terminal_source_failure, false);
        }
        let result =
            retirement::retire_destination(&mut self.destination_guard, work).and_then(|changed| {
                retirement::finish_retirement(&mut self.terminal_source_failure, changed)
            });
        self.measured_cleanup_failed = result.is_err();
        result
    }
}

impl Drop for NativeMountSession {
    fn drop(&mut self) {
        // Last-owner release has the same physical retirement boundary as an
        // explicit stop. It consumes birth-paid cleanup, never a fresh meter
        // or cancellation token. If a native operation refuses, its existing
        // driver owner retains the original resources for retirement.
        let _ = self.stop_with_context(&NativeSessionStopWork::AcceptedDrop);
    }
}

pub(super) enum NativeSessionStopWork<'a> {
    Meter(&'a NativeMaterializationWork),
    AcceptedWait(&'a crate::CancellationToken),
    AcceptedDrop,
}

impl NativeSessionStopWork<'_> {
    pub(super) fn wait(&self) -> Option<&crate::CancellationToken> {
        match self {
            Self::Meter(work) => Some(work.cancellation()),
            Self::AcceptedWait(wait) => Some(wait),
            Self::AcceptedDrop => None,
        }
    }

    pub(super) fn check_wait(&self) -> Result<(), NativeMountError> {
        if let Some(wait) = self.wait() {
            wait.check().map_err(|_| crate::DemandError::Cancelled)?;
        }
        Ok(())
    }
}
