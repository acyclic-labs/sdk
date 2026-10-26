use super::*;
use crate::native_mount::retirement::SourceRetirementWait;

impl DriverSessionResources {
    pub(super) fn wait_until(&self, timeout: Duration, done: impl Fn(&LoopState) -> bool) {
        let state = self.state();
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
        join: SourceRetirementWait,
    ) -> Result<(), NativeMountError> {
        let LoopState {
            resources,
            control,
            probe,
            retirement,
            ..
        } = state;
        if let Some((_, destination, _)) = resources.as_ref() {
            if !control.detached || control.controls_pending() {
                control.detach(
                    Path::new(std::ffi::OsStr::from_bytes(destination.as_bytes())),
                    probe,
                    &ProbeWork::Cleanup(&self.credit, wait),
                )?;
            }
            if control.controls_pending() {
                return Err(NativeMountError::ControlPending);
            }
            self.free(wait)?;
        }
        if let Some(packet) = retirement {
            let result = packet.retire(wait, join);
            if packet.is_retired() {
                drop(resources.take());
            }
            result?;
            if !packet.is_retired() {
                return Err(NativeMountError::ControlPending);
            }
        }
        drop(resources.take());
        Ok(())
    }

    pub(super) fn finish(&self) {
        let mut state = self.state();
        self.changed.notify_all();
        let mut waiting = false;
        loop {
            match self.retire(&mut state, None, SourceRetirementWait::Physical) {
                Ok(()) => return,
                Err(error) => {
                    state.failure.cleanup.get_or_insert(error);
                }
            }
            if state.resources.is_none()
                && state
                    .retirement
                    .as_ref()
                    .is_none_or(crate::native_mount::NativeMountRetirement::is_retired)
            {
                // The outcome is retained; no producer remains whose progress
                // could justify waiting for another cleanup request.
                return;
            }
            if !waiting {
                self.credit.started_drop_io(READ_WORK);
                waiting = true;
            }
            // Completion retains its wake independently of the session lock.
            drop(state);
            self.retirement_wake.wait(None);
            state = self.state();
        }
    }
}

pub(super) unsafe extern "C" fn report_mounted(events: *mut c_void) {
    // SAFETY: the provider retains this owner through its callback.
    let events = unsafe { &*events.cast_const().cast::<DriverSessionResources>() };
    events.state().mounted = true;
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
                let native = self.unmount.native();
                // SAFETY: this owner retains the SDK; the bridge ignores the name.
                let status = unsafe {
                    acyclic_fs_darwin_mount_invalidate(native.value as *mut c_void, ptr::null())
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
        NativeMountWorkScope::new(&context, work)
    }

    pub(in crate::native_mount) fn revalidate(&self, work: &Work) -> Result<(), NativeMountError> {
        let _active = self.begin_original_work(work)?;
        let context = self.unmount.context()?;
        context.source.fence_changes_with_meter(work)?;
        context.revalidate(work).map(|_| ())
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
        self.unmount.state().control.detached
    }

    pub(in crate::native_mount) fn prepare_destination_cleanup(
        &mut self,
        destination: &Path,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        // Lock order: customer session -> driver state -> original work ledger.
        self.unmount
            .state()
            .retirement
            .as_mut()
            .and_then(|packet| packet.destination_guard.as_mut())
            .ok_or(NativeMountError::ControlPending)?
            .prepare_destination_cleanup(destination, work)
    }

    pub(in crate::native_mount) fn retirement_ownership(&self) -> (bool, bool) {
        self.unmount
            .state()
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
    ) -> Result<crate::native_mount::retirement::TerminalFailures, NativeMountError> {
        work.check_wait()?;
        let wait = work.wait();
        let events = Arc::clone(&self.unmount);
        let work = ProbeWork::Cleanup(&events.credit, wait);
        let was_mounted = {
            let mut state = events.state();
            if state.resources.is_none() && self.thread.is_none() {
                drop(state);
                return self.retired_outcome(&events, wait, false);
            }
            if (state.resources.is_some()
                || state
                    .retirement
                    .as_ref()
                    .is_some_and(|packet| !packet.is_retired()))
                && let Some(error) = state.failure.take()
            {
                return Err(error);
            }
            events.retirement_wake.notify();
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
                    probe,
                    &work,
                )?,
                None => false,
            }
        };
        events.changed.notify_all();
        if !events.provider_exited.load(Ordering::Acquire) {
            events.interrupt(wait)?;
        }
        let deadline = Instant::now() + MOUNT_LOOP_EXIT_TIMEOUT;
        while let Some(thread) = &self.thread {
            work.check()?;
            if thread.is_finished() {
                break;
            }
            {
                let mut state = events.state();
                if (state.resources.is_some()
                    || state
                        .retirement
                        .as_ref()
                        .is_some_and(|packet| !packet.is_retired()))
                    && let Some(error) = state.failure.take()
                {
                    return Err(error);
                }
            }
            if Instant::now() >= deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            work.read()?;
            events.wait_until(STOP_POLL_INTERVAL, |_| thread.is_finished());
        }
        if self.thread.is_some() {
            events.sdk_credit.started_io(READ_WORK, wait)?;
            self.loop_result = Some(
                self.thread
                    .take()
                    .ok_or(NativeMountError::ControlPending)?
                    .join()
                    .map_err(|panic| NativeMountError::DriverPanicked {
                        operation: "Darwin mount loop shutdown",
                        panic: Mutex::new(panic),
                    }),
            );
        }
        self.retired_outcome(&events, wait, was_mounted)
    }

    fn retired_outcome(
        &mut self,
        events: &DriverSessionResources,
        wait: Option<&crate::CancellationToken>,
        was_mounted: bool,
    ) -> Result<crate::native_mount::retirement::TerminalFailures, NativeMountError> {
        let mut state = events.state();
        if let Err(error) = events.retire(&mut state, wait, SourceRetirementWait::Caller) {
            if state.resources.is_some()
                || state
                    .retirement
                    .as_ref()
                    .is_some_and(|packet| !packet.is_retired())
            {
                return Err(error);
            }
            state.failure.cleanup = Some(error);
        }
        if let Some(error) = self.classify_detached_loop(was_mounted)? {
            state.failure.producer = Some(error);
        }
        Ok(std::mem::take(&mut state.failure))
    }

    fn classify_detached_loop(
        &mut self,
        was_mounted: bool,
    ) -> Result<Option<NativeMountError>, NativeMountError> {
        if self.loop_result.is_none() {
            return Ok(None);
        }
        let before = self
            .loop_finished_before_teardown
            .ok_or(NativeMountError::ControlPending)?;
        let result = self
            .loop_result
            .take()
            .ok_or(NativeMountError::ControlPending)?;
        match result {
            Ok(0) if !before || !was_mounted => Ok(None),
            Ok(status) => Ok(Some(NativeMountError::DarwinTeardown(
                DarwinTeardownFailure(status),
            ))),
            Err(error) => Ok(Some(error)),
        }
    }
}

impl Drop for DarwinMountSession {
    #[allow(
        clippy::expect_used,
        reason = "Interrupt without a wait token consumes birth-paid credit and has no fallible path."
    )]
    fn drop(&mut self) {
        self.unmount.retirement_wake.notify();
        self.unmount.changed.notify_all();
        if !self.unmount.provider_exited.load(Ordering::Acquire) {
            self.unmount
                .interrupt(None)
                .expect("birth-paid SDK interrupt");
        }
    }
}
