use super::*;
use crate::materializer::NativeWorkReservation;

pub(super) struct DarwinStopController {
    pub(super) state: Mutex<StopState>,
    pub(super) changed: Condvar,
    pub(super) requested: AtomicBool,
    pub(super) mounted: AtomicBool,
    pub(super) provider_exited: AtomicBool,
    _allocation: crate::NativeAllocation,
}

pub(super) struct StopState {
    pub(super) control: DarwinRecoveryControl,
    pub(super) probe: MountProbe,
    pub(super) credit: NativeWorkReservation,
    pub(super) observer: Option<NativeWorkReservation>,
    pub(super) start_wait: NativeWorkReservation,
    retained_wait: NativeWorkReservation,
    pub(super) retirement: Option<crate::native_mount::NativeMountRetirement>,
    pub(super) failure: Option<NativeMountError>,
}

impl DarwinStopController {
    pub(super) fn new(
        resources: &Arc<DriverSessionResources>,
        destination: &Path,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeMountError> {
        let mut probe = MountProbe::new(destination, original)?;
        probe.preallocate(original)?;
        let probes = control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL)?
            .checked_add(3)
            .ok_or(crate::WorkError::Overflow)?;
        let mut waits = 0_u64;
        for timeout in [
            DIRECT_UNMOUNT_TIMEOUT,
            DISKUTIL_UNMOUNT_TIMEOUT,
            DISKUTIL_VISIBILITY_TIMEOUT,
            MOUNT_LOOP_EXIT_TIMEOUT,
        ] {
            waits = waits
                .checked_add(control_visits(timeout, STOP_POLL_INTERVAL)?)
                .ok_or(crate::WorkError::Overflow)?;
        }
        let items = (libc::PATH_MAX as u64)
            .checked_add(probe.mounts.capacity() as u64)
            .and_then(|n| n.checked_mul(probes))
            .and_then(|n| n.checked_add(waits + 3))
            .ok_or(crate::WorkError::Overflow)?;
        let name = destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?
            .as_bytes();
        let mut credit = original.reserve_work(crate::WorkCounters {
            backend_read_operations: probes
                .checked_mul(5)
                .and_then(|n| n.checked_add(waits))
                .ok_or(crate::WorkError::Overflow)?,
            backend_write_operations: probes,
            items_examined: items,
            bytes_copied: probes
                .checked_mul(name.len() as u64)
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let visits = control_visits(MOUNT_LOOP_EXIT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let observer = credit.partition_work(crate::WorkCounters {
            backend_read_operations: visits,
            items_examined: visits.checked_add(2).ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let state = StopState {
            control: DarwinRecoveryControl {
                prepared_direct: Some(DirectDetach::new(
                    Arc::clone(&resources.startup.destination),
                    original,
                )?),
                prepared_diskutil: Some(Diskutil::prepare(original)?),
                ..DarwinRecoveryControl::default()
            },
            probe,
            credit,
            observer: Some(observer),
            start_wait: original.reserve_work(READ_WORK)?,
            retained_wait: original.reserve_work(READ_WORK)?,
            retirement: None,
            failure: None,
        };
        let allocation = original.claim(
            (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Arc::new(Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
            requested: AtomicBool::new(false),
            mounted: AtomicBool::new(false),
            provider_exited: AtomicBool::new(false),
            _allocation: allocation,
        }))
    }

    pub(super) fn wait_until(&self, timeout: Duration, done: impl Fn() -> bool) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        drop(
            self.changed
                .wait_timeout_while(state, timeout, |_| !done())
                .unwrap_or_else(PoisonError::into_inner),
        );
    }

    pub(super) fn retire(
        state: &mut StopState,
        resources: &Arc<DriverSessionResources>,
    ) -> Result<(), NativeMountError> {
        let original = state.credit.work().clone();
        let loan = state.credit.loan(None);
        let work = ProbeWork {
            original: &original,
            cleanup: Some(&loan),
        };
        if !state.control.detached || state.control.controls_pending() {
            let destination = Path::new(std::ffi::OsStr::from_bytes(
                resources.startup.destination.as_bytes(),
            ));
            state
                .control
                .detach(destination, Some(resources), &mut state.probe, &work)?;
        }
        if state.control.controls_pending() {
            return Err(NativeMountError::ControlPending);
        }
        drop(state.control.prepared_direct.take());
        drop(state.control.prepared_diskutil.take());
        resources.free(None)?;
        if let Some(retirement) = &mut state.retirement {
            retirement.retire(None)?;
            if !retirement.is_retired() {
                return Err(NativeMountError::ControlPending);
            }
        }
        Ok(())
    }

    pub(super) fn finish(&self, resources: &Arc<DriverSessionResources>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.requested.store(false, Ordering::Release);
        match Self::retire(&mut state, resources) {
            Ok(()) => return,
            Err(error) => state.failure = Some(error),
        }
        state.retained_wait.started_drop_io(READ_WORK);
        state = self
            .changed
            .wait_while(state, |state| {
                if !self.requested.swap(false, Ordering::AcqRel) {
                    return true;
                }
                match Self::retire(state, resources) {
                    Ok(()) => false,
                    Err(error) => {
                        if state.failure.is_none() {
                            state.failure = Some(error);
                        }
                        true
                    }
                }
            })
            .unwrap_or_else(PoisonError::into_inner);
        drop(state);
    }
}

pub(super) unsafe extern "C" fn report_mounted(controller: *mut c_void) {
    // SAFETY: the loop retains this controller through the native callback.
    let controller = unsafe { &*controller.cast_const().cast::<DarwinStopController>() };
    controller.mounted.store(true, Ordering::Release);
    controller.changed.notify_all();
}
