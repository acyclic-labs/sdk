use super::*;
use std::future::Future;
use std::task::Poll;

#[derive(Default)]
pub(in crate::native_mount) struct DarwinRecoveryControl {
    pub(super) direct: Option<DirectDetach>,
    pub(super) prepared_direct: Option<DirectDetach>,
    pub(super) diskutil: Option<Diskutil>,
    pub(super) detached: bool,
    pub(super) start_aborted: bool,
}

pub(super) struct DirectDetach {
    pub(super) task: acyclic_native_runtime::NativeCompletion<std::io::Result<c_int>>,
    pub(super) original: Work,
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
        let operation = acyclic_native_runtime::NativeOperationReservation::new(
            Arc::clone(&meter),
            acyclic_native_runtime::NativeOperationKind::Write,
            1,
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        let retained = original.allocation_work();
        let task = acyclic_native_runtime::run_blocking_io(
            move || {
                let mut operation = operation;
                let (status, recorded) = operation.attempt(|| {
                    // SAFETY: the closure owns the terminated path through the syscall.
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
            original: retained,
        })
    }

    pub(super) fn poll(
        &mut self,
        work: &ProbeWork<'_, '_>,
    ) -> Result<Poll<Result<(), NativeMountError>>, NativeMountError> {
        work.check()?;
        let completed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::pin::Pin::new(&mut self.task)
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        }));
        Ok(match completed {
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
    deadline: Option<Instant>,
}

impl Diskutil {
    pub(super) fn prepare(original: &Work) -> Result<Self, NativeMountError> {
        let observations = control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let termination = control_visits(
            acyclic_native_runtime::ProcessTree::TERMINATION_TIMEOUT,
            acyclic_native_runtime::ProcessTree::TERMINATION_POLL_INTERVAL,
        )?;
        let credit = original
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
            .into_runtime_credit()?;
        let frame = acyclic_native_runtime::ProcessTree::prepare_closed_process(
            3,
            original.allocation_work().runtime_meter(),
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        Ok(Self {
            tree: None,
            frame: Some(frame),
            meter: credit.runtime_meter(),
            original: original.allocation_work(),
            deadline: None,
        })
    }

    fn spawn(&mut self, destination: &CStr) -> Result<(), NativeMountError> {
        let deadline = Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT;
        let result = if self.frame.is_some() {
            acyclic_native_runtime::ProcessTree::spawn_closed_prepared(
                c"/usr/sbin/diskutil",
                &[c"unmount", c"force", destination],
                &mut self.frame,
                self.meter.as_ref(),
                &mut self.tree,
            )
        } else {
            acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                c"/usr/sbin/diskutil",
                &[c"unmount", c"force", destination],
                self.meter.as_ref(),
                &mut self.tree,
            )
        };
        self.deadline = result.is_ok().then_some(deadline);
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
    let visits = timeout.as_nanos().div_ceil(interval.as_nanos()) + 1;
    u64::try_from(visits).map_err(|_| WorkError::Overflow.into())
}

impl DarwinRecoveryControl {
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        self.direct.is_some()
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
        if let Some(child) = &mut self.diskutil {
            if child.tree.is_some() && (!mounted || child.deadline.is_none()) {
                work.check()?;
                let meter = Arc::clone(&child.meter);
                child.terminate(meter.as_ref())?;
                drop(self.diskutil.take());
            }
        }
        if !mounted {
            drop(self.prepared_direct.take());
            drop(self.diskutil.take());
            self.poll_direct(work)?;
            return Ok(false);
        }
        if self
            .diskutil
            .as_ref()
            .is_none_or(|child| child.tree.is_none())
        {
            if self.direct.is_none() {
                work.check()?;
                self.direct = match self.prepared_direct.take() {
                    Some(task) => Some(task),
                    None if name.is_some() => None,
                    None => Some(DirectDetach::new(
                        shared(
                            work.original(),
                            native_name(destination.as_os_str().as_bytes(), work.original())?,
                        )?,
                        work.original(),
                    )?),
                };
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
            } else {
                self.poll_direct(work)?;
            }
            if !probe.mounted(destination, work)? {
                self.detached = true;
                drop(self.diskutil.take());
                return Ok(true);
            }
            work.check()?;
            if self.diskutil.is_none() {
                if name.is_some() {
                    return Err(NativeMountError::ControlPending);
                }
                self.diskutil = Some(Diskutil {
                    tree: None,
                    frame: None,
                    meter: work.original().runtime_meter(),
                    original: work.original().allocation_work(),
                    deadline: None,
                });
            }
            let result = if let Some(name) = name {
                self.diskutil
                    .as_mut()
                    .expect("installed process owner")
                    .spawn(name)
            } else {
                let path = native_name(destination.as_os_str().as_bytes(), work.original())?;
                self.diskutil
                    .as_mut()
                    .expect("installed process owner")
                    .spawn(&path)
            };
            if self
                .diskutil
                .as_ref()
                .is_some_and(|child| child.tree.is_none())
            {
                drop(self.diskutil.take());
            }
            result?;
        }
        let (status, timed_out) = loop {
            let child = self.diskutil.as_mut().expect("retained process");
            work.check()?;
            let status = child
                .tree
                .as_mut()
                .expect("spawned process")
                .try_wait(Some(child.meter.as_ref()))
                .map_err(|error| NativeMountError::from(child.original.runtime_error(error)))?;
            let timed_out =
                Instant::now() >= child.deadline.expect("spawned process has a deadline");
            if status.is_some() || timed_out {
                work.check()?;
                let meter = Arc::clone(&child.meter);
                child.terminate(meter.as_ref())?;
                break (status, timed_out);
            }
            work.read()?;
            std::thread::sleep(STOP_POLL_INTERVAL);
        };
        drop(self.diskutil.take());
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
                drop(self.direct.take());
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
        let mut probe = MountProbe::new(destination, original)?;
        let work = ProbeWork::Live(original);
        self.detach(destination, None, &mut probe, &work)?;
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
            drop(self.diskutil.take());
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
