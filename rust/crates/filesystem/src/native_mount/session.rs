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
    // Destination data and all native owners physically drop before its guard.
    pub(super) _destination_custody: Option<crate::NativeAllocation>,
}

impl NativeMountSession {
    /// Recognizes only entries still owned by this mount's held parent resources.
    pub fn owns_parent_entry(
        &self,
        entry: &crate::native_host::HostListedEntry,
        work: &NativeMaterializationWork,
    ) -> Result<bool, crate::NativeTreeMaterializationError> {
        let Some(stat) = &entry.stat else {
            return Ok(false);
        };
        let guard = self.destination_guard.as_ref();
        #[cfg(windows)]
        let guard = guard.or_else(|| {
            self.driver
                .as_ref()?
                .retirement
                .as_ref()?
                .destination_guard
                .as_ref()
        });
        if let Some(guard) = guard {
            for (name, file) in [(
                guard
                    .fence_name
                    .as_ref()
                    .and_then(|name| std::str::from_utf8(name).ok()),
                guard.file.as_ref(),
            )] {
                if let (Some(name), Some(file)) = (name, file) {
                    work.charge(
                        crate::materializer::NativeResourceCredit::Items.delta(name.len() as u64),
                    )?;
                    if entry.is_named(name)
                        && stat.identity()
                            == crate::native_host::stat_file(file, &mut &*work)?.identity()
                    {
                        return Ok(true);
                    }
                }
            }
        }
        #[cfg(windows)]
        if let Some(driver) = &self.driver {
            return driver.owns_parent_entry(entry, work);
        }
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
        if self.cleanup_state().cleanup_complete() {
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
            let changed = self.stop_namespace_with_context(work)?;
            Ok(self.release_destination_with_context(work)? || changed)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
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
                with_driver!(
                    &mut *driver,
                    session,
                    session.stop(work),
                    session.stop(match work {
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
                match work {
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
                    match work {
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
            Ok(changed)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }
    pub(super) fn prepare_destination_cleanup(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
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
        let Some(guard) = self.destination_guard.as_mut() else {
            return Ok(false);
        };
        let result = guard.remove_destination().and_then(|()| {
            if guard.birth_cleanup.is_some() {
                // Namespace producers have retired. Complete the accepted fence
                // release on its original credit, even after caller cancellation.
                guard.release_cleanup(None)
            } else {
                match work {
                    NativeSessionStopWork::Meter(meter) => guard.release(&mut &**meter),
                    NativeSessionStopWork::AcceptedWait(wait) => guard.release_cleanup(Some(wait)),
                }
            }
        });
        self.measured_cleanup_failed = result.is_err();
        result?;
        drop(self.destination_guard.take());
        Ok(true)
    }
}

pub(super) enum NativeSessionStopWork<'a> {
    Meter(&'a NativeMaterializationWork),
    AcceptedWait(&'a crate::CancellationToken),
}
