use super::*;
use std::future::Future;
use std::task::Poll;

#[derive(Default)]
pub(in crate::native_mount) struct DarwinRecoveryControl {
    pub(super) direct: Option<DirectDetach>,
    pub(super) diskutil: Option<Diskutil>,
    pub(super) detached: bool,
    pub(super) start_aborted: bool,
}

pub(super) struct DirectDetach {
    pub(super) task: acyclic_native_runtime::NativeCompletion<std::io::Result<c_int>>,
    pub(super) original: Work,
    // NativeCompletion submits only on its first poll.
    pub(super) started: bool,
}

impl DirectDetach {
    pub(super) fn new(
        path: Arc<DarwinOwned<CString>>,
        original: &Work,
    ) -> Result<Self, NativeMountError> {
        let meter = original
            .reserve_work(WRITE_WORK)?
            .into_runtime_credit()?
            .runtime_meter();
        let mut operation = acyclic_native_runtime::NativeOperationReservation::new(
            Arc::clone(&meter),
            acyclic_native_runtime::NativeOperationKind::Write,
            1,
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        let task = acyclic_native_runtime::run_blocking_io(
            move || {
                let (status, recorded) = operation.attempt(|| {
                    // SAFETY: the closure retains the terminated path through unmount.
                    if unsafe { libc::unmount(path.as_ptr(), libc::MNT_FORCE) } == 0 {
                        Ok(0)
                    } else {
                        Err(std::io::Error::last_os_error())
                    }
                });
                status.and_then(|status| recorded.map(|()| status))
            },
            Some((meter, true)),
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        Ok(Self {
            task,
            original: original.allocation_work(),
            started: false,
        })
    }

    pub(super) fn poll(
        &mut self,
        work: &ProbeWork<'_, '_>,
    ) -> Result<Poll<Result<(), NativeMountError>>, NativeMountError> {
        work.check()?;
        self.started = true;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::pin::Pin::new(&mut self.task)
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        }));
        Ok(match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(
                result
                    .and_then(std::convert::identity)
                    .map(|_| ())
                    .map_err(|error| self.original.runtime_error(error).into()),
            ),
            Err(panic) => Poll::Ready(Err(NativeMountError::DriverPanicked {
                operation: "Darwin direct detach",
                panic: Mutex::new(panic),
            })),
        })
    }
}

pub(super) struct Diskutil {
    tree: Option<acyclic_native_runtime::ProcessTree>,
    frame: Option<acyclic_native_runtime::PreparedClosedProcess>,
    meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    original: Work,
    deadline: Instant,
}

impl Diskutil {
    pub(super) fn prepare(original: &Work) -> Result<Self, NativeMountError> {
        let observations = control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let termination = control_visits(
            acyclic_native_runtime::ProcessTree::TERMINATION_TIMEOUT,
            acyclic_native_runtime::ProcessTree::TERMINATION_POLL_INTERVAL,
        )?;
        let meter = original
            .reserve_work(WorkCounters {
                backend_read_operations: termination
                    .checked_mul(5)
                    .and_then(|n| n.checked_add(observations + 1))
                    .ok_or(WorkError::Overflow)?,
                backend_write_operations: observations
                    .checked_add(10)
                    .ok_or(WorkError::Overflow)?,
                items_examined: 4,
                bytes_copied: (5 * std::mem::size_of::<*mut c_char>()) as u64,
                ..WorkCounters::UNCHARGED
            })?
            .into_runtime_credit()?
            .runtime_meter();
        let frame = acyclic_native_runtime::ProcessTree::prepare_closed_process(
            3,
            original.allocation_work().runtime_meter(),
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        Ok(Self {
            tree: None,
            frame: Some(frame),
            meter,
            original: original.allocation_work(),
            deadline: Instant::now(),
        })
    }

    fn spawn(&mut self, destination: &CStr) -> Result<(), NativeMountError> {
        let result = acyclic_native_runtime::ProcessTree::spawn_closed_prepared(
            c"/usr/sbin/diskutil",
            &[c"unmount", c"force", destination],
            &mut self.frame,
            self.meter.as_ref(),
            &mut self.tree,
        );
        // A partially born child must be terminated on the next attempt.
        self.deadline = Instant::now()
            + if result.is_ok() {
                DISKUTIL_UNMOUNT_TIMEOUT
            } else {
                Duration::ZERO
            };
        result.map_err(|error| self.original.runtime_error(error).into())
    }

    fn terminate(
        &mut self,
        meter: &dyn acyclic_native_runtime::NativeReadMeter,
    ) -> Result<(), NativeMountError> {
        self.tree
            .as_mut()
            .ok_or(NativeMountError::ControlPending)?
            .terminate(Some(meter))
            .map_err(|error| self.original.runtime_error(error).into())
    }
}

pub(super) fn control_visits(
    timeout: Duration,
    interval: Duration,
) -> Result<u64, NativeMountError> {
    u64::try_from(timeout.as_nanos().div_ceil(interval.as_nanos()) + 1)
        .map_err(|_| WorkError::Overflow.into())
}

impl DarwinRecoveryControl {
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        self.direct.as_ref().is_some_and(|task| task.started)
            || self
                .diskutil
                .as_ref()
                .is_some_and(|child| child.tree.is_some())
    }

    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        self.detached
    }

    pub(super) fn detach(
        &mut self,
        destination: &Path,
        name: Option<&CStr>,
        probe: &mut MountProbe,
        work: &ProbeWork<'_, '_>,
    ) -> Result<bool, NativeMountError> {
        let mounted = probe.mounted(destination, work)?;
        self.detached = !mounted;
        if mounted && self.start_aborted {
            return Err(NativeMountError::DestinationBusy);
        }
        if !mounted {
            if let Some(child) = &mut self.diskutil {
                if child.tree.is_some() {
                    work.check()?;
                    child.terminate(child.meter.clone().as_ref())?;
                }
            }
            self.diskutil = None;
            if self.direct.as_ref().is_some_and(|task| !task.started) {
                self.direct = None;
            }
            self.poll_direct(work)?;
            return Ok(false);
        }
        if self
            .diskutil
            .as_ref()
            .is_none_or(|child| child.tree.is_none())
        {
            if self.direct.is_none() && name.is_none() {
                self.direct = Some(DirectDetach::new(
                    shared(
                        work.original(),
                        native_name(destination.as_os_str().as_bytes(), work.original())?,
                    )?,
                    work.original(),
                )?);
            }
            let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
            while self.direct.is_some() {
                match self.poll_direct(work) {
                    Ok(()) => {}
                    Err(NativeMountError::Io(_))
                    | Err(NativeMountError::Materialization(
                        crate::NativeTreeMaterializationError::Io(_),
                    )) => break,
                    Err(error) => return Err(error),
                }
                if self.direct.is_none() || Instant::now() >= deadline {
                    break;
                }
                work.read()?;
                std::thread::sleep(STOP_POLL_INTERVAL);
            }
            if !probe.mounted(destination, work)? {
                self.detached = true;
                self.diskutil = None;
                return Ok(true);
            }
            work.check()?;
            if self.diskutil.is_none() {
                if name.is_some() {
                    return Err(NativeMountError::ControlPending);
                }
                self.diskutil = Some(Diskutil::prepare(work.original())?);
            }
            let child = self.diskutil.as_mut().expect("prepared fallback");
            let result = match name {
                Some(name) => child.spawn(name),
                None => child.spawn(&native_name(
                    destination.as_os_str().as_bytes(),
                    work.original(),
                )?),
            };
            if child.tree.is_none() {
                self.diskutil = None;
            }
            result?;
        }
        let (status, timed_out) = loop {
            work.check()?;
            let child = self.diskutil.as_mut().expect("retained process");
            let status = child
                .tree
                .as_mut()
                .expect("spawned process")
                .try_wait(Some(child.meter.as_ref()))
                .map_err(|error| NativeMountError::from(child.original.runtime_error(error)))?;
            let timed_out = Instant::now() >= child.deadline;
            if status.is_some() || timed_out {
                work.check()?;
                child.terminate(child.meter.clone().as_ref())?;
                break (status, timed_out);
            }
            work.read()?;
            std::thread::sleep(STOP_POLL_INTERVAL);
        };
        self.diskutil = None;
        if !probe.mounted(destination, work)? {
            self.detached = true;
            return Ok(true);
        }
        if timed_out {
            return Err(NativeMountError::Io(std::io::ErrorKind::TimedOut.into()));
        }
        if let Some(status) = status.filter(|status| !status.success()) {
            return Err(NativeMountError::DriverExit {
                operation: "Darwin unmount",
                status,
            });
        }
        let deadline = Instant::now() + DISKUTIL_VISIBILITY_TIMEOUT;
        while probe.mounted(destination, work)? {
            if Instant::now() >= deadline {
                return Err(NativeMountError::Lifecycle(
                    "Darwin mount remains visible after unmount",
                ));
            }
            work.read()?;
            std::thread::sleep(STOP_POLL_INTERVAL);
        }
        self.detached = true;
        Ok(true)
    }

    pub(super) fn poll_direct(&mut self, work: &ProbeWork<'_, '_>) -> Result<(), NativeMountError> {
        if let Some(task) = &mut self.direct {
            if let Poll::Ready(result) = task.poll(work)? {
                self.direct = None;
                result?;
            }
        }
        Ok(())
    }

    pub(in crate::native_mount) fn recover(
        &mut self,
        destination: &Path,
        original: &Work,
    ) -> Result<(), NativeMountError> {
        let work = ProbeWork::Live(original);
        self.detach(
            destination,
            None,
            &mut MountProbe::new(destination, original)?,
            &work,
        )?;
        self.poll_direct(&work)?;
        if self.controls_pending() {
            Err(NativeMountError::ControlPending)
        } else {
            Ok(())
        }
    }

    pub(in crate::native_mount) fn retire_controls(
        &mut self,
        original: &Work,
    ) -> Result<(), NativeMountError> {
        let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
        let work = ProbeWork::Live(original);
        let mut failure = None;
        if self.direct.as_ref().is_some_and(|task| !task.started) {
            self.direct = None;
        }
        while self.direct.is_some() {
            let result = self.poll_direct(&work);
            if self.direct.is_none() {
                failure = result.err();
                break;
            }
            result?;
            if Instant::now() >= deadline {
                return Err(NativeMountError::ControlPending);
            }
            work.read()?;
            std::thread::sleep(STOP_POLL_INTERVAL);
        }
        if let Some(child) = &mut self.diskutil {
            if child.tree.is_some() {
                if let Err(error) = child.terminate(original.runtime_meter().as_ref()) {
                    return Err(failure.unwrap_or(error));
                }
            }
            self.diskutil = None;
        }
        failure.map_or(Ok(()), Err)
    }
}
