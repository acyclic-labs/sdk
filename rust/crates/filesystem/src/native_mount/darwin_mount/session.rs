use super::*;

impl DriverSessionResources {
    pub(super) fn wait_until(&self, timeout: Duration, done: impl Fn(&LoopState) -> bool) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        drop(
            self.changed
                .wait_timeout_while(state, timeout, |state| !done(state))
                .unwrap_or_else(PoisonError::into_inner),
        );
    }

    pub(super) fn retire(
        &self,
        state: &mut LoopState,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let LoopState {
            resources,
            control,
            probe,
            retirement,
            ..
        } = state;
        if let Some((_, destination, _)) = resources.as_ref() {
            let loan = crate::materializer::NativeCleanupLoan {
                credit: &self.credit,
                wait,
            };
            if !control.detached || control.controls_pending() {
                control.detach(
                    Path::new(std::ffi::OsStr::from_bytes(destination.as_bytes())),
                    Some(destination.as_c_str()),
                    probe,
                    &ProbeWork::Cleanup(&loan),
                )?;
            }
            if control.controls_pending() {
                return Err(NativeMountError::ControlPending);
            }
            drop(control.prepared_direct.take());
            drop(control.diskutil.take());
            self.free(wait)?;
        }
        if let Some(packet) = retirement {
            packet.retire(wait)?;
            if !packet.is_retired() {
                return Err(NativeMountError::ControlPending);
            }
        }
        drop(resources.take());
        Ok(())
    }

    pub(super) fn finish(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.requested.store(false, Ordering::Release);
        self.changed.notify_all();
        match self.retire(&mut state, None) {
            Ok(()) => return,
            Err(error) => state.failure = Some(error),
        }
        self.credit.started_drop_io(READ_WORK);
        drop(
            self.changed
                .wait_while(state, |state| {
                    if !self.requested.swap(false, Ordering::AcqRel) {
                        return true;
                    }
                    match self.retire(state, None) {
                        Ok(()) => false,
                        Err(error) => {
                            if state.failure.is_none() {
                                state.failure = Some(error);
                            }
                            true
                        }
                    }
                })
                .unwrap_or_else(PoisonError::into_inner),
        );
    }
}

pub(super) unsafe extern "C" fn report_mounted(events: *mut c_void) {
    // SAFETY: the provider retains this owner through its callback.
    let events = unsafe { &*events.cast_const().cast::<DriverSessionResources>() };
    events
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .mounted = true;
    events.changed.notify_all();
}

pub(in crate::native_mount) struct DarwinMountSession {
    pub(super) thread: Option<JoinHandle<c_int>>,
    pub(super) loop_finished_before_teardown: Option<bool>,
    pub(super) loop_result: Option<Result<c_int, NativeMountError>>,
    pub(super) unmount: Arc<DriverSessionResources>,
    pub(super) handler_failure: Arc<DarwinOwned<Mutex<DarwinCallbackState>>>,
    pub(super) native_root_identity: Option<crate::NativeRootIdentity>,
}

impl DarwinMountSession {
    pub(in crate::native_mount) fn invalidate(&self, _path: &[u8]) -> Result<(), NativeMountError> {
        let context = self.unmount.context()?;
        context
            .run(false, |work| {
                work.charge(WRITE_WORK)?;
                context.forget_view();
                // SAFETY: this owner retains the SDK; the bridge ignores the name.
                let status = unsafe {
                    acyclic_fs_darwin_mount_invalidate(
                        self.unmount.pointer() as *mut c_void,
                        ptr::null(),
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
        work: &Work,
    ) -> Result<NativeMountWorkScope, NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        let context = self.unmount.context()?;
        if self.unmount.pointer() == 0 {
            return Err(NativeMountError::Lifecycle("session is stopped"));
        }
        NativeMountWorkScope::new(context, work)
    }

    pub(in crate::native_mount) fn revalidate(&self, work: &Work) -> Result<(), NativeMountError> {
        let active = self.begin_original_work(work)?;
        active.context.source.fence_changes_with_meter(work)?;
        active.context.revalidate(work).map(|_| ())
    }

    pub(in crate::native_mount) fn native_root_identity(
        &self,
    ) -> Option<crate::NativeRootIdentity> {
        if self.unmount.pointer() == 0 || self.namespace_detach_confirmed() {
            None
        } else {
            self.native_root_identity
        }
    }

    pub(in crate::native_mount) fn take_callback_failure(
        &self,
    ) -> Option<(NativeMountError, WorkCounters)> {
        self.handler_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_failure()
    }

    pub(in crate::native_mount) fn completed_callback_work(&self) -> WorkCounters {
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
        let events = Arc::clone(&self.unmount);
        let loan = crate::materializer::NativeCleanupLoan {
            credit: &events.credit,
            wait: Some(wait),
        };
        let work = ProbeWork::Cleanup(&loan);
        let was_mounted = {
            let mut state = events.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.resources.is_none() && self.thread.is_none() {
                events.retire(&mut state, Some(wait))?;
                drop(state);
                return self.classify_detached_loop(false);
            }
            if let Some(error) = state.failure.take() {
                return Err(error);
            }
            events.requested.store(true, Ordering::Release);
            if self.loop_finished_before_teardown.is_none() {
                self.loop_finished_before_teardown =
                    Some(self.thread.is_none() || events.provider_exited.load(Ordering::Acquire));
            }
            events.changed.notify_all();
            let LoopState {
                resources,
                control,
                probe,
                ..
            } = &mut *state;
            match resources.as_ref() {
                Some((_, destination, _)) => control.detach(
                    Path::new(std::ffi::OsStr::from_bytes(destination.as_bytes())),
                    Some(destination.as_c_str()),
                    probe,
                    &work,
                )?,
                None => false,
            }
        };
        events.changed.notify_all();
        if !events.provider_exited.load(Ordering::Acquire) {
            events.interrupt(Some(wait))?;
        }
        let deadline = Instant::now() + MOUNT_LOOP_EXIT_TIMEOUT;
        while let Some(thread) = &self.thread {
            work.check()?;
            if thread.is_finished() {
                break;
            }
            if let Some(error) = events
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .failure
                .take()
            {
                return Err(error);
            }
            if Instant::now() >= deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            work.read()?;
            events.wait_until(STOP_POLL_INTERVAL, |_| thread.is_finished());
        }
        if self.thread.is_some() {
            events.start_join(wait)?;
            self.loop_result = Some(
                self.thread
                    .take()
                    .expect("finished provider")
                    .join()
                    .map_err(|panic| NativeMountError::DriverPanicked {
                        operation: "Darwin mount loop shutdown",
                        panic: Mutex::new(panic),
                    }),
            );
        }
        let mut state = events.state.lock().unwrap_or_else(PoisonError::into_inner);
        events.retire(&mut state, Some(wait))?;
        drop(state);
        self.classify_detached_loop(was_mounted)
    }

    fn classify_detached_loop(&mut self, was_mounted: bool) -> Result<(), NativeMountError> {
        let Some(result) = self.loop_result.as_mut() else {
            return Ok(());
        };
        let before = self
            .loop_finished_before_teardown
            .expect("sampled provider");
        match result {
            Ok(0) if !before || !was_mounted => Ok(()),
            Ok(status) => Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure(
                *status,
            ))),
            Err(error) => Err(std::mem::replace(
                error,
                NativeMountError::Lifecycle("Darwin mount loop panicked"),
            )),
        }
    }
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        self.unmount.requested.store(true, Ordering::Release);
        self.unmount.changed.notify_all();
        if !self.unmount.provider_exited.load(Ordering::Acquire) {
            self.unmount
                .interrupt(None)
                .expect("birth-paid SDK interrupt");
        }
    }
}
