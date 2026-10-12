use super::*;

pub(in crate::native_mount) struct DarwinMountSession {
    pub(super) resources: Option<Arc<DriverSessionResources>>,
    pub(super) destination: DarwinOwned<PathBuf>,
    pub(super) thread: Option<JoinHandle<c_int>>,
    pub(super) events: Arc<LoopEvents>,
    pub(super) loop_finished_before_teardown: Option<bool>,
    pub(super) loop_result: Option<MountLoopResult>,
    pub(super) loop_panic: Option<Mutex<Box<dyn std::any::Any + Send>>>,
    pub(super) teardown_complete: bool,
    pub(super) unmount: Arc<DarwinStopController>,
    pub(super) handler_failure: Arc<DarwinOwned<Mutex<DarwinCallbackState>>>,
    pub(super) native_root_identity: Option<crate::NativeRootIdentity>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum MountLoopResult {
    Exited(c_int),
    Panicked,
    TimedOut,
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
                let mut terminated = owned(work, capacity as u64, || {
                    let mut value = Vec::new();
                    value.try_reserve_exact(capacity)?;
                    Ok(value)
                })?;
                terminated
                    .allocation
                    .as_mut()
                    .expect("funded name")
                    .reconcile_capacity(terminated.value.capacity() as u64)?;
                copying(work, copy_work(path.len() as u64 + 1), || {
                    if path.first() != Some(&b'/') {
                        terminated.push(b'/');
                    }
                    terminated.extend_from_slice(path);
                    terminated.push(0);
                })?;
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
    ) -> Result<DarwinStartupMeterScope, NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let resources = self.resources()?;
        if resources.pointer() == 0 {
            return Err(NativeMountError::Lifecycle("session is stopped"));
        }
        DarwinStartupMeterScope::new(Arc::clone(&resources.context), work)
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
            let original = observer.loan(Some(wait)).original_work();
            let loan = observer.loan(Some(wait));
            let work = ProbeWork {
                original: &original,
                cleanup: Some(&loan),
            };
            work.check()?;
            if self.loop_finished_before_teardown.is_none() {
                self.loop_finished_before_teardown = Some(work.cpu(scan_work(1), || {
                    self.thread.is_none() || self.events.current().provider_exited
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
                let native = credit.loan(Some(wait)).original_work();
                let cleanup = credit.loan(Some(wait));
                control.detach(
                    &self.destination,
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
            if self
                .loop_result
                .is_none_or(|result| matches!(result, MountLoopResult::TimedOut))
            {
                if !self.events.current().provider_exited {
                    if let Some(resources) = &self.resources {
                        resources.interrupt(Some(wait))?;
                    }
                }
                self.loop_result =
                    Some(self.finish_thread(MOUNT_LOOP_EXIT_TIMEOUT, &work, wait)?);
            } else if self.resources.is_some() {
                self.finish_thread(MOUNT_LOOP_EXIT_TIMEOUT, &work, wait)?;
            }
            let mut state = controller
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.control.poll_direct(&work)?;
            if state.control.controls_pending() {
                return Err(NativeMountError::ControlPending);
            }
            if let Some(panic) = self.loop_panic.take() {
                return Err(NativeMountError::DriverPanicked {
                    operation: "Darwin mount loop shutdown",
                    panic,
                });
            }
            let result = classify_detached_loop(
                self.loop_result
                    .as_ref()
                    .ok_or(NativeMountError::DarwinTeardown(
                        DarwinTeardownFailure::MissingTeardownState,
                    ))?,
                was_mounted,
                self.loop_finished_before_teardown
                    .ok_or(NativeMountError::DarwinTeardown(
                        DarwinTeardownFailure::MissingTeardownState,
                    ))?,
            );
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
    ) -> Result<MountLoopResult, NativeMountError> {
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
                    return Ok(MountLoopResult::TimedOut);
                }
                work.read()?;
                self.events.wait_until(STOP_POLL_INTERVAL, |_| {
                    self.thread.as_ref().is_none_or(JoinHandle::is_finished)
                });
            }
            if let Some(resources) = &self.resources {
                resources.start_join(wait)?;
            } else {
                work.read()?;
            }
            self.loop_result = Some(match self.thread.take().expect("admitted loop").join() {
                Ok(status) => MountLoopResult::Exited(status),
                Err(panic) => {
                    self.loop_panic = Some(Mutex::new(panic));
                    MountLoopResult::Panicked
                }
            });
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
        Ok(self.loop_result.unwrap_or(MountLoopResult::Exited(0)))
    }
}

pub(super) fn classify_detached_loop(
    join: &MountLoopResult,
    was_mounted: bool,
    finished_before_teardown: bool,
) -> Result<(), NativeMountError> {
    let failure = match (join, was_mounted, finished_before_teardown) {
        (MountLoopResult::Exited(0), _, false) | (MountLoopResult::Exited(0), false, true) => {
            return Ok(());
        }
        (MountLoopResult::Exited(status), _, true) => {
            DarwinTeardownFailure::LoopExitedBeforeDetach(*status)
        }
        (MountLoopResult::Exited(status), _, false) => {
            DarwinTeardownFailure::LoopExitedAfterDetach(*status)
        }
        (MountLoopResult::Panicked, _, _) => DarwinTeardownFailure::LoopPanicked,
        (MountLoopResult::TimedOut, _, _) => DarwinTeardownFailure::LoopExitTimedOut,
    };
    Err(NativeMountError::DarwinTeardown(failure))
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        if self.teardown_complete {
            return;
        }
        self.unmount.requested.store(true, Ordering::Release);
        self.unmount.changed.notify_all();
        if !self.events.current().provider_exited {
            if let Some(resources) = &self.resources {
                resources.interrupt(None).expect("birth-paid SDK interrupt");
            }
        }
    }
}
