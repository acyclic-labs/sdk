use super::*;
use std::future::Future;
use std::task::Poll;

#[derive(Default)]
pub(in crate::native_mount) struct DarwinRecoveryControl {
    pub(super) direct: Option<DirectDetach>,
    pub(super) detached: bool,
    pub(super) start_aborted: bool,
    pub(super) recovery_probe: Option<MountProbe>,
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
        let completed = match result {
            Ok(Poll::Pending) => return Ok(Poll::Pending),
            Ok(Poll::Ready(result)) => Ok(result),
            Err(panic) => Err(panic),
        };
        Ok(Poll::Ready(Self::completed(&self.credit, completed)))
    }

    fn completed(
        credit: &crate::NativeWorkReservation,
        result: std::thread::Result<std::io::Result<std::io::Result<c_int>>>,
    ) -> Result<(), NativeMountError> {
        match result {
            Ok(result) => result
                .and_then(std::convert::identity)
                .map(|_| ())
                .map_err(|error| credit.work().runtime_error(error).into()),
            Err(panic) => Err(NativeMountError::DriverPanicked {
                operation: "Darwin direct detach",
                panic: Mutex::new(panic),
            }),
        }
    }

    pub(super) fn retire(self) -> Result<(), NativeMountError> {
        let Self { task, credit, .. } = self;
        // The runtime owns the completion wake and its birth allocation. A
        // cancelled invocation cannot revoke retirement of an already started
        // native call, and no second polling/wake implementation is needed.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.wait(None, std::future::pending(), || {})
        }));
        Self::completed(&credit, result)
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

impl DarwinRecoveryControl {
    pub(super) fn bind_mount(
        &mut self,
        mount: &libc::statfs,
        original: &Work,
    ) -> Result<(), NativeMountError> {
        if self.recovery_probe.is_some() || self.controls_pending() || self.detached {
            return Err(NativeMountError::ControlPending);
        }
        self.recovery_probe = Some(MountProbe::for_mount(mount, original)?);
        Ok(())
    }

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

    pub(super) fn wait_direct(&mut self, work: &ProbeWork<'_>) -> Result<(), NativeMountError> {
        work.check()?;
        let Some(task) = &mut self.direct else {
            return Ok(());
        };
        let cancellation = match work {
            ProbeWork::Live(original) => Some(original.cancellation_wait()?),
            ProbeWork::Cleanup(_, wait) => wait.map(|wait| wait.cancelled()),
        };
        task.started = true;
        // Borrow the completion: cancelling this caller leaves the accepted
        // native operation in its original custody for physical retirement.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.task.wait_interruptible(async {
                match cancellation {
                    Some(cancellation) => cancellation.await,
                    None => std::future::pending().await,
                }
            })
        }));
        let completed = match result {
            Ok(Ok(None)) => {
                // Replace the departing caller's waker with the retained
                // retirement owner before returning cancellation.
                let credit = Arc::clone(&task.credit);
                let completed = task.poll(&ProbeWork::Cleanup(&credit, None))?;
                if let Poll::Ready(result) = completed {
                    self.direct = None;
                    result?;
                }
                return Err(crate::DemandError::Cancelled.into());
            }
            Ok(Ok(Some(result))) => Ok(Ok(result)),
            Ok(Err(error)) => Ok(Err(error)),
            Err(panic) => Err(panic),
        };
        if let Some(task) = self.direct.take() {
            DirectDetach::completed(&task.credit, completed)?;
        }
        Ok(())
    }

    pub(in crate::native_mount) fn recover(
        &mut self,
        destination: &Path,
        original: &Work,
    ) -> Result<(), NativeMountError> {
        let work = ProbeWork::Live(original);
        let bound = self.recovery_probe.is_some();
        let mut probe = match self.recovery_probe.take() {
            Some(probe) => probe,
            None => MountProbe::new(destination, original)?,
        };
        let result = (|| {
            self.detach(destination, &mut probe, &work)?;
            self.poll_direct(&work)?;
            if self.controls_pending() {
                Err(NativeMountError::ControlPending)
            } else {
                Ok(())
            }
        })();
        if bound {
            self.recovery_probe = Some(probe);
        }
        result
    }

    pub(in crate::native_mount) fn retire_controls(
        &mut self,
        _original: &Work,
    ) -> Result<(), NativeMountError> {
        self.direct
            .take()
            .filter(|task| task.started)
            .map_or(Ok(()), DirectDetach::retire)
    }
}
