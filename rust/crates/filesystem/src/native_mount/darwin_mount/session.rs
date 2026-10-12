use super::*;

pub(in crate::native_mount) struct DarwinMountSession {
    pub(super) resources: Option<Arc<DriverSessionResources>>,
    pub(super) destination: Arc<DarwinOwned<CString>>,
    pub(super) thread: Option<JoinHandle<c_int>>,
    pub(super) loop_finished_before_teardown: Option<bool>,
    pub(super) loop_result: Option<Result<c_int, NativeMountError>>,
    pub(super) teardown_complete: bool,
    pub(super) unmount: Arc<DarwinStopController>,
    pub(super) handler_failure: Arc<DarwinOwned<Mutex<DarwinCallbackState>>>,
    pub(super) native_root_identity: Option<crate::NativeRootIdentity>,
}

impl DarwinMountSession {
    pub(super) fn resources(&self) -> Result<&DriverSessionResources, NativeMountError> {
        self.resources.as_deref().ok_or(NativeMountError::Lifecycle(
            "Darwin mount session has stopped",
        ))
    }

    pub(in crate::native_mount) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let resources = self.resources()?;
        resources
            .context
            .run(false, |work| {
                let capacity = path
                    .len()
                    .checked_add(2)
                    .ok_or(crate::WorkError::Overflow)?;
                let mut terminated = vector(work, capacity)?;
                work.charge(copy_work(path.len() as u64 + 1))?;
                if path.first() != Some(&b'/') {
                    terminated.push(b'/');
                }
                terminated.extend_from_slice(path);
                terminated.push(0);
                let terminated = terminated.map(CString::from_vec_with_nul);
                let path = terminated
                    .value
                    .as_ref()
                    .map_err(|_| driver_errno(libc::EINVAL))?;
                work.charge(WRITE_WORK)?;
                resources.context.forget_view();
                // SAFETY: the session and terminated name remain owned throughout invalidation.
                let status = unsafe {
                    acyclic_fs_darwin_mount_invalidate(
                        resources.pointer() as *mut c_void,
                        path.as_ptr(),
                    )
                };
                if status == 0 {
                    Ok(())
                } else {
                    Err(driver_errno(status))
                }
            })
            .map_err(|status| {
                self.take_callback_failure()
                    .map_or_else(|| driver_errno(status), |(cause, _)| cause)
            })
    }

    pub(in crate::native_mount) fn begin_original_work(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<NativeMountWorkScope, NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let resources = self.resources()?;
        if resources.pointer() == 0 {
            return Err(NativeMountError::Lifecycle("session is stopped"));
        }
        NativeMountWorkScope::new(Arc::clone(&resources.context), work)
    }

    pub(in crate::native_mount) fn revalidate(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let active = self.begin_original_work(work)?;
        active.context.source.fence_changes_with_meter(work)?;
        active.context.revalidate(work).map(|_| ())
    }

    pub(in crate::native_mount) fn native_root_identity(
        &self,
    ) -> Option<crate::NativeRootIdentity> {
        if self.teardown_complete || self.namespace_detach_confirmed() {
            None
        } else {
            self.native_root_identity
        }
    }

    pub(in crate::native_mount) fn take_callback_failure(
        &self,
    ) -> Option<(NativeMountError, crate::WorkCounters)> {
        self.handler_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_failure()
    }

    pub(in crate::native_mount) fn completed_callback_work(&self) -> crate::WorkCounters {
        self.handler_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work
    }

    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        self.unmount
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .control
            .detached
    }

    pub(in crate::native_mount) fn retirement_ownership(&self) -> (bool, bool) {
        self.unmount
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retirement
            .as_ref()
            .map_or((false, false), |packet| {
                (
                    packet.destination_guard.is_some(),
                    packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
                )
            })
    }

    pub(in crate::native_mount) fn stop(
        &mut self,
        work: &crate::native_mount::NativeSessionStopWork<'_>,
    ) -> Result<(), NativeMountError> {
        let wait = match work {
            crate::native_mount::NativeSessionStopWork::Meter(work) => work.cancellation(),
            crate::native_mount::NativeSessionStopWork::AcceptedWait(wait) => wait,
        };
        wait.check()
            .map_err(|_| NativeMountError::from(crate::DemandError::Cancelled))?;
        let controller = Arc::clone(&self.unmount);
        let mut observer = {
            let mut state = controller
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if self.teardown_complete {
                if let Some(retirement) = &mut state.retirement {
                    retirement.retire(Some(wait))?;
                }
                return Ok(());
            }
            if let Some(error) = state.failure.take() {
                return Err(error);
            }
            state
                .observer
                .take()
                .ok_or(NativeMountError::ControlPending)?
        };
        controller.requested.store(true, Ordering::Release);
        controller.changed.notify_all();
        let result = (|| {
            let original = observer.work().clone();
            let loan = observer.loan(Some(wait));
            let work = ProbeWork {
                original: &original,
                cleanup: Some(&loan),
            };
            work.check()?;
            if self.loop_finished_before_teardown.is_none() {
                self.loop_finished_before_teardown = Some(work.cpu(scan_work(1), || {
                    self.thread.is_none() || self.unmount.provider_exited.load(Ordering::Acquire)
                })?);
            }
            let was_mounted = {
                let mut state = controller
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let StopState {
                    control,
                    probe,
                    credit,
                    ..
                } = &mut *state;
                let native = credit.work().clone();
                let cleanup = credit.loan(Some(wait));
                control.detach(
                    Path::new(std::ffi::OsStr::from_bytes(self.destination.as_bytes())),
                    self.resources.as_ref(),
                    probe,
                    &ProbeWork {
                        original: &native,
                        cleanup: Some(&cleanup),
                    },
                )?
            };
            controller.changed.notify_all();
            work.check()?;
            if self.thread.is_some() && !self.unmount.provider_exited.load(Ordering::Acquire) {
                if let Some(resources) = &self.resources {
                    resources.interrupt(Some(wait))?;
                }
            }
            self.finish_thread(MOUNT_LOOP_EXIT_TIMEOUT, &work, wait)?;
            let mut state = controller
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.control.poll_direct(&work)?;
            if state.control.controls_pending() {
                return Err(NativeMountError::ControlPending);
            }
            let before = self
                .loop_finished_before_teardown
                .expect("teardown sampled the provider");
            let result = match self.loop_result.as_mut().expect("joined provider") {
                Ok(0) if !before || !was_mounted => Ok(()),
                Ok(status) => Err(NativeMountError::DarwinTeardown(
                    DarwinTeardownFailure::LoopExited(*status),
                )),
                Err(error) => Err(std::mem::replace(
                    error,
                    NativeMountError::Lifecycle("Darwin mount loop panicked"),
                )),
            };
            if result.is_ok() {
                self.teardown_complete = true;
            }
            result
        })();
        controller
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observer = Some(observer);
        controller.changed.notify_all();
        result
    }

    pub(super) fn finish_thread(
        &mut self,
        timeout: Duration,
        work: &ProbeWork<'_, '_>,
        wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        if self.thread.is_some() {
            let deadline = Instant::now() + timeout;
            loop {
                let (exited, failure) = work.cpu(scan_work(1), || {
                    (
                        self.thread.as_ref().is_none_or(JoinHandle::is_finished),
                        self.unmount
                            .state
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .failure
                            .take(),
                    )
                })?;
                if exited {
                    break;
                }
                if let Some(error) = failure {
                    return Err(error);
                }
                if Instant::now() >= deadline {
                    return Err(NativeMountError::Io(std::io::ErrorKind::TimedOut.into()));
                }
                work.read()?;
                self.unmount.wait_until(STOP_POLL_INTERVAL, || {
                    self.thread.as_ref().is_none_or(JoinHandle::is_finished)
                });
            }
            if let Some(resources) = &self.resources {
                resources.start_join(wait)?;
            } else {
                work.read()?;
            }
            self.loop_result = Some(self.thread.take().expect("admitted loop").join().map_err(
                |panic| NativeMountError::DriverPanicked {
                    operation: "Darwin mount loop shutdown",
                    panic: Mutex::new(panic),
                },
            ));
        }
        if let Some(resources) = &mut self.resources {
            let resources = Arc::get_mut(resources).ok_or(NativeMountError::ControlPending)?;
            resources.free(Some(wait))?;
            drop(self.resources.take());
            let mut state = self
                .unmount
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(retirement) = &mut state.retirement {
                retirement.retire(Some(wait))?;
            }
        }
        Ok(())
    }
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        if self.teardown_complete {
            return;
        }
        self.unmount.requested.store(true, Ordering::Release);
        self.unmount.changed.notify_all();
        if !self.unmount.provider_exited.load(Ordering::Acquire) {
            if let Some(resources) = &self.resources {
                resources.interrupt(None).expect("birth-paid SDK interrupt");
            }
        }
    }
}
