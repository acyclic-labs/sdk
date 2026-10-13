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
    pub(super) credit: Arc<crate::NativeWorkReservation>,
    // NativeCompletion submits only on its first poll.
    pub(super) started: bool,
}

impl DirectDetach {
    pub(super) fn new(
        path: Arc<DarwinOwned<CString>>,
        original: &Work,
    ) -> Result<Self, NativeMountError> {
        let credit = original.reserve_work(WRITE_WORK)?.into_runtime_credit()?;
        let meter = credit.runtime_meter();
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
            credit,
            started: false,
        })
    }

    pub(super) fn poll(
        &mut self,
        work: &ProbeWork<'_>,
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
                    .map_err(|error| self.credit.work().runtime_error(error).into()),
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
    credit: Arc<crate::NativeWorkReservation>,
    deadline: Instant,
}

impl Diskutil {
    pub(super) fn prepare(original: &Work) -> Result<Self, NativeMountError> {
        const OBSERVATIONS: u64 = control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL);
        const TERMINATION: u64 = control_visits(
            acyclic_native_runtime::ProcessTree::TERMINATION_TIMEOUT,
            acyclic_native_runtime::ProcessTree::TERMINATION_POLL_INTERVAL,
        );
        let credit = original
            .reserve_work(WorkCounters {
                backend_read_operations: TERMINATION * 5 + OBSERVATIONS + 1,
                backend_write_operations: OBSERVATIONS + 10,
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
            credit,
            deadline: Instant::now(),
        })
    }

    fn spawn(&mut self, destination: &CStr) -> Result<(), NativeMountError> {
        let result = acyclic_native_runtime::ProcessTree::spawn_closed_prepared(
            c"/usr/sbin/diskutil",
            &[c"unmount", c"force", destination],
            &mut self.frame,
            self.credit.as_ref(),
            &mut self.tree,
        );
        // A partially born child must be terminated on the next attempt.
        self.deadline = Instant::now()
            + if result.is_ok() {
                DISKUTIL_UNMOUNT_TIMEOUT
            } else {
                Duration::ZERO
            };
        result.map_err(|error| self.credit.work().runtime_error(error).into())
    }

    fn terminate(
        &mut self,
        meter: &dyn acyclic_native_runtime::NativeReadMeter,
    ) -> Result<(), NativeMountError> {
        self.tree
            .as_mut()
            .ok_or(NativeMountError::ControlPending)?
            .terminate(Some(meter))
            .map_err(|error| self.credit.work().runtime_error(error).into())
    }
}

pub(super) const fn control_visits(timeout: Duration, interval: Duration) -> u64 {
    let visits = timeout.as_nanos().div_ceil(interval.as_nanos()) + 1;
    assert!(visits <= u64::MAX as u128);
    visits as u64
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
        probe: &mut MountProbe,
        work: &ProbeWork<'_>,
    ) -> Result<bool, NativeMountError> {
        self.detached = !probe.mounted(destination, work)?;
        if self.detached {
            if let Some(child) = self.diskutil.as_mut().filter(|child| child.tree.is_some()) {
                work.check()?;
                child.terminate(child.credit.clone().as_ref())?;
            }
            self.diskutil = None;
            self.direct = self.direct.take().filter(|task| task.started);
            self.poll_direct(work)?;
            return Ok(false);
        }
        if self.start_aborted {
            return Err(NativeMountError::DestinationBusy);
        }
        if self
            .diskutil
            .as_ref()
            .is_none_or(|child| child.tree.is_none())
        {
            if self.direct.is_none() && matches!(work, ProbeWork::Live(_)) {
                self.direct = Some(DirectDetach::new(Arc::clone(&probe.0), work.original())?);
            }
            match self.wait_direct(work) {
                Ok(())
                | Err(NativeMountError::ControlPending)
                | Err(NativeMountError::Io(_))
                | Err(NativeMountError::Materialization(
                    crate::NativeTreeMaterializationError::Io(_),
                )) => {}
                Err(error) => return Err(error),
            }
            self.detached = !probe.mounted(destination, work)?;
            if self.detached {
                self.diskutil = None;
                return Ok(true);
            }
            work.check()?;
            if self.diskutil.is_none() {
                if matches!(work, ProbeWork::Cleanup(..)) {
                    return Err(NativeMountError::ControlPending);
                }
                self.diskutil = Some(Diskutil::prepare(work.original())?);
            }
            let child = self.diskutil.as_mut().expect("prepared fallback");
            let spawned = child.spawn(probe.0.as_c_str());
            if child.tree.is_none() {
                self.diskutil = None;
            }
            spawned?;
        }
        let (status, timed_out) = loop {
            work.check()?;
            let child = self.diskutil.as_mut().expect("retained fallback");
            let status = child
                .tree
                .as_mut()
                .expect("spawned process")
                .try_wait(Some(child.credit.as_ref()))
                .map_err(|error| child.credit.work().runtime_error(error))?;
            let timed_out = Instant::now() >= child.deadline;
            if status.is_some() || timed_out {
                work.check()?;
                child.terminate(child.credit.clone().as_ref())?;
                break (status, timed_out);
            }
            work.read()?;
            std::thread::sleep(STOP_POLL_INTERVAL);
        };
        self.diskutil = None;
        self.detached = !probe.mounted(destination, work)?;
        if self.detached {
            return Ok(true);
        }
        if timed_out {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
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

    pub(super) fn poll_direct(&mut self, work: &ProbeWork<'_>) -> Result<(), NativeMountError> {
        if let Some(task) = &mut self.direct {
            if let Poll::Ready(result) = task.poll(work)? {
                self.direct = None;
                result?;
            }
        }
        Ok(())
    }

    fn wait_direct(&mut self, work: &ProbeWork<'_>) -> Result<(), NativeMountError> {
        let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
        while self.direct.is_some() {
            self.poll_direct(work)?;
            if self.direct.is_none() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(NativeMountError::ControlPending);
            }
            work.read()?;
            std::thread::sleep(STOP_POLL_INTERVAL);
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
        self.direct = self.direct.take().filter(|task| task.started);
        let result = self.wait_direct(&ProbeWork::Live(original));
        if self.direct.is_some() {
            return result;
        }
        if let Some(child) = self.diskutil.as_mut().filter(|child| child.tree.is_some()) {
            if let Err(error) = child.terminate(original.runtime_meter().as_ref()) {
                return Err(result.err().unwrap_or(error));
            }
        }
        self.diskutil = None;
        result
    }
}
