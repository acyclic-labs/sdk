use super::*;

pub(super) fn run_fuse_retirement(owned: (Arc<FuseDriverControl>, crate::NativeAllocation)) {
    let (control, _) = &owned;
    let core = &control.core;
    let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
    while !state.startup_finished {
        state = control
            .changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner);
    }
    drop(state);
    if let Some(notifier) = core.notifier.get() {
        run_invalidator(core, notifier);
    }
    loop {
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.stop_requested && !state.retired {
            state = control
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        if state.retired {
            return;
        }
        state.stop_requested = false;
        drop(state);
        let result = (|| {
            let mut loan = FuseDriverLoan::take(control)?;
            loan.physical
                .as_mut()
                .expect("physical driver loan")
                .stop(control, None)?;
            if let Some(retirement) = loan.retirement.as_mut() {
                retirement.retire(None)?;
            }
            Ok::<_, NativeMountError>(
                loan.retirement
                    .as_ref()
                    .is_none_or(crate::native_mount::NativeMountRetirement::is_retired),
            )
        })();
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        match result {
            Ok(retired) => state.retired = retired,
            Err(error) => {
                state.failure.get_or_insert(error);
            }
        }
        control.changed.notify_all();
        if state.retired {
            return;
        }
    }
}

pub(super) struct FuseRetirementHelper {
    pub(super) program: &'static std::ffi::CStr,
    pub(super) frame: Option<acyclic_native_runtime::PreparedClosedProcess>,
    pub(super) helper: Option<acyclic_native_runtime::ProcessTree>,
    pub(super) completed: Option<bool>,
    pub(super) failure: Option<NativeMountError>,
    pub(super) credit: Arc<crate::materializer::NativeOwnedCleanupCredit>,
    pub(super) terminal_credit: Arc<crate::materializer::NativeOwnedCleanupCredit>,
}

impl FuseRetirementHelper {
    pub(super) fn prepare(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<Self>, NativeMountError> {
        let program = match fuse_recovery_helper(work) {
            Ok(program) => program,
            Err(NativeMountError::CapabilityUnavailable(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        Self::prepare_program(program, work).map(Some)
    }

    pub(super) fn prepare_program(
        program: &'static std::ffi::CStr,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let normal = work.allocation_work();
        let frame =
            acyclic_native_runtime::ProcessTree::prepare_closed_process(5, normal.runtime_meter())
                .map_err(|error| normal.runtime_error(error))?;
        // Reserve the bounded observation and termination loops, including reap.
        let observed = 3_001_u64 + 3_000 + 5_001 + 5_000 + 1;
        let mut reserved = normal.reserve_work(crate::WorkCounters {
            backend_read_operations: observed + 1,
            backend_write_operations: 13,
            items_examined: 6,
            bytes_copied: (7 * std::mem::size_of::<*mut libc::c_char>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let terminal = reserved.partition_work(crate::WorkCounters {
            backend_read_operations: 5_001 + 5_000 + 1,
            backend_write_operations: 3,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let terminal_credit =
            crate::materializer::NativeOwnedCleanupCredit::from_reserved(terminal)?;
        let credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(reserved)?;
        Ok(Self {
            program,
            frame: Some(frame),
            helper: None,
            completed: None,
            failure: None,
            credit,
            terminal_credit,
        })
    }

    pub(super) fn retire(
        &mut self,
        path: &std::ffi::CStr,
        identity: Option<crate::NativeRootIdentity>,
    ) -> Result<(), NativeMountError> {
        if let Some(success) = self.completed {
            return if success {
                Ok(())
            } else {
                Err(NativeMountError::FuseShutdownFailed)
            };
        }
        let observed = (|| {
            if self.failure.is_some() {
                return Ok(None);
            }
            if self.helper.is_none() {
                if let Some(identity) = identity {
                    let metadata = fuse_recovery_stat(path, &mut &self.credit)?;
                    if metadata.st_dev as u64 != identity.device
                        || metadata.st_ino as u64 != identity.object
                    {
                        return Err(NativeMountError::DestinationBusy);
                    }
                }
                acyclic_native_runtime::ProcessTree::spawn_closed_prepared(
                    self.program,
                    &[c"-u", c"-q", c"-z", c"--", path],
                    &mut self.frame,
                    self.credit.as_ref(),
                    &mut self.helper,
                )
                .map_err(|error| self.credit.original_work().runtime_error(error))?;
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            for observation in 0..=3_000 {
                let status = self
                    .helper
                    .as_mut()
                    .expect("accepted helper remains owned")
                    .try_wait(Some(self.credit.as_ref()))
                    .map_err(|error| self.credit.original_work().runtime_error(error))?;
                if status.is_some() || observation == 3_000 || std::time::Instant::now() >= deadline
                {
                    return Ok(status);
                }
                crate::native_host::NativeHostWork::attempt_io(
                    &mut &self.credit,
                    crate::WorkCounters {
                        backend_read_operations: 1,
                        ..crate::WorkCounters::UNCHARGED
                    },
                    || {
                        std::thread::sleep(Duration::from_millis(10));
                        Ok(())
                    },
                )?;
            }
            unreachable!("finite helper observation returns at its last admitted visit")
        })();
        let status = match observed {
            Ok(status) => status,
            Err(error) => {
                self.failure.get_or_insert(error);
                None
            }
        };
        // Terminal cleanup has separate original credits.
        self.drain_process()?;
        self.completed =
            Some(self.failure.is_none() && status.is_some_and(|status| status.success()));
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        match status {
            Some(status) if status.success() => Ok(()),
            Some(_) => Err(NativeMountError::FuseShutdownFailed),
            None => Err(NativeMountError::Lifecycle(
                "fusermount retirement exceeded 30 seconds",
            )),
        }
    }

    pub(super) fn drain_process(&mut self) -> Result<(), NativeMountError> {
        if let Some(helper) = self.helper.as_mut() {
            helper
                .terminate(Some(self.terminal_credit.as_ref()))
                .map_err(|error| self.terminal_credit.original_work().runtime_error(error))?;
            drop(self.helper.take());
        }
        drop(self.frame.take());
        Ok(())
    }
}

pub(super) struct FuseDriverControl {
    pub(super) state: Mutex<FuseDriverControlState>,
    pub(super) changed: Condvar,
    pub(super) core: Arc<ProjectionCore>,
    pub(super) _allocation: crate::NativeAllocation,
}

impl FuseDriverControl {
    pub(super) fn submit_sdk(&self, session: Option<fuser::Session<FuseProjection>>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.sdk_session.is_none() {
            state.sdk_session = Some(session);
            self.changed.notify_all();
        }
    }
}

pub(super) struct FuseDriverControlState {
    // Outer presence makes SDK submission one-shot.
    pub(super) sdk_session: Option<Option<fuser::Session<FuseProjection>>>,
    pub(super) physical: Option<FusePhysicalSession>,
    pub(super) retirement: Option<crate::native_mount::NativeMountRetirement>,
    pub(super) loan_custody: (bool, bool),
    pub(super) startup_finished: bool,
    pub(super) stop_requested: bool,
    pub(super) retired: bool,
    pub(super) failure: Option<NativeMountError>,
}

pub(super) struct FuseDriverLoan {
    pub(super) control: Arc<FuseDriverControl>,
    pub(super) physical: Option<FusePhysicalSession>,
    pub(super) retirement: Option<crate::native_mount::NativeMountRetirement>,
    pub(super) committed: bool,
}

impl FuseDriverLoan {
    pub(super) fn take(control: &Arc<FuseDriverControl>) -> Result<Self, NativeMountError> {
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        let physical = state
            .physical
            .take()
            .ok_or(NativeMountError::ControlPending)?;
        let retirement = state.retirement.take();
        state.loan_custody = retirement.as_ref().map_or((false, false), |packet| {
            (
                packet.destination_guard.is_some(),
                packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
            )
        });
        Ok(Self {
            control: Arc::clone(control),
            physical: Some(physical),
            retirement,
            committed: state.startup_finished,
        })
    }
}

impl Drop for FuseDriverLoan {
    fn drop(&mut self) {
        if !self.committed {
            if self.physical.is_some() {
                self.control.core.stopping.store(true, Ordering::Release);
                self.control.core.invalidation.notify_all();
                self.control.submit_sdk(None);
            }
        }
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.physical = self.physical.take();
        state.retirement = self.retirement.take();
        state.loan_custody = (false, false);
        state.startup_finished = true;
        if !self.committed {
            state.stop_requested = true;
        }
        self.control.changed.notify_all();
    }
}

pub(super) struct FusePhysicalSession {
    pub(super) session: Option<FuseMount>,
    pub(super) sdk_worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    pub(super) active_root: Option<cap_std::fs::Dir>,
    pub(super) native_root_identity: Option<crate::NativeRootIdentity>,
    pub(super) retirement_helper: Option<FuseRetirementHelper>,
    pub(super) birth_cleanup: crate::NativeWorkReservation,
}

impl FusePhysicalSession {
    pub(super) fn stop(
        &mut self,
        control: &FuseDriverControl,
        native: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), NativeMountError> {
        let core = &control.core;
        let Self {
            session,
            sdk_worker,
            active_root,
            retirement_helper,
            native_root_identity,
            birth_cleanup,
        } = self;
        let loan = if native.is_none() {
            Some(birth_cleanup.loan(None))
        } else {
            None
        };
        if session.is_none()
            && sdk_worker.is_none()
            && active_root.is_none()
            && retirement_helper.is_none()
        {
            return Ok(());
        }
        let callback_native = match native {
            Some(native) => {
                native.admit(crate::WorkCounters::UNCHARGED)?;
                native.clone()
            }
            None => {
                let loan = loan.as_ref().ok_or(NativeMountError::ControlPending)?;
                loan.check_wait()?;
                loan.original_work()
            }
        };
        // Callbacks retain their original grants and cancellation.
        let _active = NativeMountWorkScope::new(core, &callback_native)?;
        let started_io = |actual| -> Result<(), NativeMountError> {
            match native {
                Some(native) => native.charge(actual)?,
                None => loan
                    .as_ref()
                    .ok_or(NativeMountError::ControlPending)?
                    .started_io(actual)?,
            }
            Ok(())
        };
        let write = crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        if active_root.is_some() {
            match native {
                Some(native) => {
                    close_fuse_root(active_root, native)?;
                }
                None => {
                    started_io(write)?;
                    drop(active_root.take());
                }
            }
        }
        if let Some(mount) = session.as_mut() {
            started_io(write)?;
            core.stopping.store(true, Ordering::Release);
            core.invalidation.notify_all();
            // Kernel refusal retains the mount for retry.
            if let Err(error) = mount.try_unmount() {
                if error.raw_os_error() != Some(libc::EPERM) {
                    return Err(error.into());
                }
                let helper =
                    retirement_helper
                        .as_mut()
                        .ok_or(NativeMountError::CapabilityUnavailable(
                            "no canonical fusermount helper is available for retirement",
                        ))?;
                helper.retire(
                    std::ffi::CStr::from_bytes_with_nul(&mount.path)
                        .map_err(|_| NativeMountError::InvalidDestination)?,
                    *native_root_identity,
                )?;
                mount.mounted = false;
            }
            mount.drain_helper()?;
            drop(session.take());
        }
        control.submit_sdk(None);
        if sdk_worker.is_some() {
            started_io(crate::WorkCounters {
                backend_read_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let worker = sdk_worker
                .take()
                .expect("accepted SDK worker remains owned");
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error.into()),
                Err(panic) => {
                    return Err(NativeMountError::DriverPanicked {
                        operation: "FUSE SDK request worker",
                        panic: Mutex::new(panic),
                    });
                }
            }
        }
        if let Some(helper) = retirement_helper.as_mut() {
            helper.drain_process()?;
        }
        // Workers are terminal; release unused helper backing and credits.
        drop(retirement_helper.take());
        Ok(())
    }
}

pub(super) fn close_fuse_root(
    active_root: &mut Option<cap_std::fs::Dir>,
    native: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    if active_root.is_none() {
        return Ok(());
    }
    let cleanup = native.reserve_cleanup_write()?;
    let directory = active_root
        .take()
        .expect("preadmitted root capability remains owned");
    let (_, recorded) = cleanup.attempt_recorded(|| drop(directory));
    recorded.map_err(NativeMountError::from)
}

pub(super) fn run_fuse_sdk(
    owned: (Arc<FuseDriverControl>, crate::NativeAllocation),
) -> std::io::Result<()> {
    let (control, _) = &owned;
    let core = &control.core;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.sdk_session.is_none() {
            state = control
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        let session = state
            .sdk_session
            .as_mut()
            .expect("SDK submission was received")
            .take();
        drop(state);
        match session {
            Some(session) => session.run(),
            None => Ok(()),
        }
    }));
    core.stopping.store(true, Ordering::Release);
    core.invalidation.notify_all();
    let mut state = control.state.lock().unwrap_or_else(PoisonError::into_inner);
    state.stop_requested = true;
    control.changed.notify_all();
    drop(state);
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
