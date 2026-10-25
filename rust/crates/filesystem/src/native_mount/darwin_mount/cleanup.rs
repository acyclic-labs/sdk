use super::*;
use std::future::Future;
use std::task::Poll;

#[derive(Default)]
pub(in crate::native_mount) struct DarwinRecoveryControl {
    pub(super) direct: Option<DirectDetach>,
    pub(super) detached: bool,
    pub(super) start_aborted: bool,
}

pub(super) struct DirectDetach {
    pub(super) task: acyclic_native_runtime::NativeCompletion<std::io::Result<c_int>>,
    pub(super) credit: Arc<crate::NativeWorkReservation>,
    // NativeCompletion submits only on its first poll.
    pub(super) started: bool,
    pub(super) wake: std::sync::Weak<DriverSessionResources>,
}

impl DirectDetach {
    pub(super) fn new(
        path: Arc<DarwinOwned<CString>>,
        original: &Work,
    ) -> Result<Self, NativeMountError> {
        let credit = original
            .reserve_work(WorkCounters {
                backend_write_operations: 2,
                ..WorkCounters::UNCHARGED
            })?
            .into_runtime_credit()?;
        let meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = credit.clone();
        let mut operation = acyclic_native_runtime::NativeOperationReservation::new(
            Arc::clone(&meter),
            acyclic_native_runtime::NativeOperationKind::Write,
            2,
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        let task = acyclic_native_runtime::run_blocking_io(
            move || {
                super::reclaim::retire_mount(path.as_c_str(), |invoke, path| {
                    let (status, recorded) = operation.attempt(|| invoke(path));
                    status.and(recorded)
                })
                .map(|()| 0)
            },
            Some((meter, true)),
        )
        .map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        Ok(Self {
            task,
            credit,
            started: false,
            wake: std::sync::Weak::new(),
        })
    }

    pub(super) fn poll(
        &mut self,
        work: &ProbeWork<'_>,
    ) -> Result<Poll<Result<(), NativeMountError>>, NativeMountError> {
        work.check()?;
        self.started = true;
        let wake = self.wake.upgrade().map(std::task::Waker::from);
        let waker = wake.as_ref().unwrap_or(std::task::Waker::noop());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::pin::Pin::new(&mut self.task).poll(&mut std::task::Context::from_waker(waker))
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

// A pending accepted operation owns the destination fence through the provider
// resources. Completion must wake that owner even after a caller stops waiting.
impl std::task::Wake for DriverSessionResources {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.retirement_wake.notify();
        self.changed.notify_all();
    }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "The assertion bounds the computed visit count before conversion."
)]
pub(super) const fn control_visits(timeout: Duration, interval: Duration) -> u64 {
    let visits = timeout.as_nanos().div_ceil(interval.as_nanos()) + 1;
    assert!(visits <= u64::MAX as u128);
    visits as u64
}

impl DarwinRecoveryControl {
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        self.direct.as_ref().is_some_and(|task| task.started)
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
            self.direct = self.direct.take().filter(|task| task.started);
            self.poll_direct(work)?;
            return Ok(false);
        }
        if self.start_aborted {
            return Err(NativeMountError::DestinationBusy);
        }
        if self.direct.is_none() {
            if matches!(work, ProbeWork::Cleanup(..)) {
                return Err(NativeMountError::ControlPending);
            }
            self.direct = Some(DirectDetach::new(Arc::clone(&probe.0), work.original())?);
        }
        let result = self.wait_direct(work);
        self.detached = !probe.mounted(destination, work)?;
        result?;
        if !self.detached {
            return Err(NativeMountError::Lifecycle(
                "Darwin mount remains visible after unmount",
            ));
        }
        Ok(true)
    }

    pub(super) fn poll_direct(&mut self, work: &ProbeWork<'_>) -> Result<(), NativeMountError> {
        if let Some(task) = &mut self.direct
            && let Poll::Ready(result) = task.poll(work)?
        {
            self.direct = None;
            result?;
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
        self.wait_direct(&ProbeWork::Live(original))
    }
}
