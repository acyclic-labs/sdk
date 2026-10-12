use super::*;
use std::future::Future;
use std::task::Poll;

#[derive(Default)]
pub(in crate::native_mount) struct DarwinRecoveryControl {
    pub(super) direct: Option<DirectDetach>,
    pub(super) prepared_direct: Option<DirectDetach>,
    pub(super) diskutil: Option<Diskutil>,
    pub(super) prepared_diskutil: Option<Diskutil>,
    pub(super) detached: bool,
    pub(super) start_aborted: bool,
}

pub(super) struct DirectDetach {
    task: Option<acyclic_native_runtime::NativeCompletion<std::io::Result<c_int>>>,
    failure: Option<std::io::Error>,
    original: crate::NativeMaterializationWork,
}

impl DirectDetach {
    pub(super) fn new(
        resources: Option<Arc<DriverSessionResources>>,
        destination: &Path,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let path = if resources.is_none() {
            Some(native_name(destination.as_os_str().as_bytes(), original)?)
        } else {
            None
        };
        let meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = if resources.is_some() {
            original
                .reserve_work(WRITE_WORK)?
                .into_runtime_credit()?
                .runtime_meter()
        } else {
            original.runtime_meter()
        };
        let operation = acyclic_native_runtime::NativeOperationReservation::new(
            Arc::clone(&meter),
            acyclic_native_runtime::NativeOperationKind::Write,
            1,
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        let prepared = resources.is_some();
        let retained = original.allocation_work();
        let task = acyclic_native_runtime::run_blocking_io(
            move || {
                let path = resources.as_ref().map_or_else(
                    || path.as_ref().expect("recovery path").as_ptr(),
                    |resources| resources.startup.destination.as_ptr(),
                );
                let mut operation = operation;
                let (status, recorded) = operation.attempt(|| {
                    // SAFETY: the closure owns the terminated path through the syscall.
                    if unsafe { libc::unmount(path, libc::MNT_FORCE) } == 0 {
                        Ok(0)
                    } else {
                        Err(std::io::Error::last_os_error())
                    }
                });
                status.and_then(|status| recorded.map(|()| status))
            },
            Some((meter, prepared)),
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        Ok(Self {
            task: Some(task),
            failure: None,
            original: retained,
        })
    }

    pub(super) fn poll(
        &mut self,
        work: &ProbeWork<'_, '_>,
    ) -> Result<Poll<Result<(), NativeMountError>>, NativeMountError> {
        work.check()?;
        if self.failure.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        let completed = work.cpu(scan_work(1), || -> Result<_, NativeMountError> {
            if work.cleanup.is_some_and(|loan| loan.wait.is_none()) {
                work.read()?;
                let task = self.task.take().ok_or(NativeMountError::ControlPending)?;
                Ok(Poll::Ready(task.wait(None, std::future::pending(), || {})))
            } else {
                let task = self.task.as_mut().ok_or(NativeMountError::ControlPending)?;
                Ok(std::pin::Pin::new(task)
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop())))
            }
        })??;
        match completed {
            Poll::Pending => Ok(Poll::Pending),
            Poll::Ready(Err(error)) => {
                self.failure = Some(error);
                Err(NativeMountError::ControlPending)
            }
            Poll::Ready(Ok(status)) => {
                Ok(Poll::Ready(status.map(|_| ()).map_err(|error| {
                    self.original.runtime_error(error).into()
                })))
            }
        }
    }
}

pub(super) struct Diskutil {
    tree: Option<acyclic_native_runtime::ProcessTree>,
    frame: Option<acyclic_native_runtime::PreparedClosedProcess>,
    meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    original: crate::NativeMaterializationWork,
    deadline: Instant,
    failed_spawn: bool,
}

impl Diskutil {
    pub(super) fn prepare(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let observations = control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let termination = control_visits(
            acyclic_native_runtime::ProcessTree::TERMINATION_TIMEOUT,
            acyclic_native_runtime::ProcessTree::TERMINATION_POLL_INTERVAL,
        )?;
        let credit = original
            .reserve_work(crate::WorkCounters {
                backend_read_operations: termination
                    .checked_mul(5)
                    .and_then(|n| n.checked_add(observations + 1))
                    .ok_or(crate::WorkError::Overflow)?,
                backend_write_operations: observations
                    .checked_add(10)
                    .ok_or(crate::WorkError::Overflow)?,
                items_examined: 4,
                bytes_copied: (5 * std::mem::size_of::<*mut c_char>()) as u64,
                ..crate::WorkCounters::UNCHARGED
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
            deadline: Instant::now(),
            failed_spawn: false,
        })
    }

    fn spawn(&mut self, destination: &CStr) -> Result<(), NativeMountError> {
        self.deadline = Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT;
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
        self.failed_spawn = result.is_err();
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
    let interval = interval.as_nanos();
    let visits = timeout
        .as_nanos()
        .checked_add(interval - 1)
        .ok_or(crate::WorkError::Overflow)?
        / interval;
    u64::try_from(visits.checked_add(1).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| crate::WorkError::Overflow.into())
}

impl DarwinRecoveryControl {
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        self.direct.is_some() || self.diskutil.is_some()
    }
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        self.detached
    }

    pub(super) fn detach(
        &mut self,
        destination: &Path,
        resources: Option<&Arc<DriverSessionResources>>,
        probe: &mut MountProbe,
        work: &ProbeWork<'_, '_>,
    ) -> Result<bool, NativeMountError> {
        let mounted = probe.mounted(destination, work)?;
        self.detached = !mounted;
        if mounted && self.start_aborted {
            return Err(NativeMountError::DestinationBusy);
        }
        if let Some(child) = &mut self.diskutil {
            if !mounted || child.failed_spawn {
                work.check()?;
                let meter = Arc::clone(&child.meter);
                child.terminate(meter.as_ref())?;
                drop(self.diskutil.take());
            }
        }
        if !mounted {
            drop(self.prepared_direct.take());
            drop(self.prepared_diskutil.take());
            self.poll_direct(work)?;
            return Ok(false);
        }
        if self.diskutil.is_none() {
            if self.direct.is_none() {
                work.check()?;
                self.direct = match self.prepared_direct.take() {
                    Some(task) => Some(task),
                    None if resources.is_some() => None,
                    None => Some(DirectDetach::new(None, destination, work.original)?),
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
                drop(self.prepared_diskutil.take());
                return Ok(true);
            }
            work.check()?;
            self.diskutil = Some(match self.prepared_diskutil.take() {
                Some(child) => child,
                None if resources.is_some() => return Err(NativeMountError::ControlPending),
                None => Diskutil {
                    tree: None,
                    frame: None,
                    meter: work.original.runtime_meter(),
                    original: work.original.allocation_work(),
                    deadline: Instant::now(),
                    failed_spawn: false,
                },
            });
            let result = if let Some(resources) = resources {
                self.diskutil
                    .as_mut()
                    .expect("installed process owner")
                    .spawn(&resources.startup.destination)
            } else {
                let path = native_name(destination.as_os_str().as_bytes(), work.original)?;
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
            let status = work.cpu(scan_work(1), || {
                child
                    .tree
                    .as_mut()
                    .expect("spawned process")
                    .try_wait(Some(child.meter.as_ref()))
                    .map_err(|error| NativeMountError::from(child.original.runtime_error(error)))
            })??;
            let timed_out = Instant::now() >= child.deadline;
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
            return Err(NativeMountError::DarwinTeardown(
                DarwinTeardownFailure::DiskutilExit(status),
            ));
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
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut probe = MountProbe::new(destination, original)?;
        let work = ProbeWork {
            original,
            cleanup: None,
        };
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
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
        let work = ProbeWork {
            original,
            cleanup: None,
        };
        let mut failure = None;
        while let Some(task) = &mut self.direct {
            original.charge(scan_work(1))?;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Some(error) = task.failure.take() {
                    Poll::Ready(Err(error))
                } else {
                    std::pin::Pin::new(task.task.as_mut().expect("accepted task retained"))
                        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                }
            }));
            match result {
                Err(panic) => {
                    failure = Some(NativeMountError::DriverPanicked {
                        operation: "Darwin administrative direct detach",
                        panic: Mutex::new(panic),
                    });
                    drop(self.direct.take());
                }
                Ok(Poll::Ready(result)) => {
                    let task = self.direct.take().expect("completed task");
                    failure = result
                        .and_then(std::convert::identity)
                        .err()
                        .map(|error| task.original.runtime_error(error).into());
                }
                Ok(Poll::Pending) => {
                    if Instant::now() >= deadline {
                        return Err(NativeMountError::ControlPending);
                    }
                    work.read()?;
                    std::thread::sleep(STOP_POLL_INTERVAL);
                }
            }
        }
        if let Some(child) = &mut self.diskutil {
            if let Err(error) = child.terminate(original.runtime_meter().as_ref()) {
                return Err(failure.unwrap_or(error));
            }
            drop(self.diskutil.take());
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
