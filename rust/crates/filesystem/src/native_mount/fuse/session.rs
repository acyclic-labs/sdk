use super::*;
use crate::materializer::NativeResourceCredit::{ReadOperations, WriteOperations};
use crate::native_mount::{NativeMountRetirement, NativeSessionStopWork};
use crate::{
    NativeAllocation, NativeMaterializationWork, NativeRootIdentity, NativeWorkReservation,
    WorkCounters, WorkError,
};

pub(in crate::native_mount) struct FuseSession {
    pub(super) core: Arc<ProjectionCore>,
    pub(super) state: Arc<Mutex<FuseSessionState>>,
    pub(super) invalidator: Option<std::thread::JoinHandle<()>>,
}

pub(super) struct FuseSessionState {
    pub(super) path: FuseOwned<Vec<u8>>,
    pub(super) before: Option<(u64, u64)>,
    pub(super) unmounter: Option<fuser::SessionUnmounter>,
    pub(super) worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    pub(super) device: Option<OwnedFd>,
    pub(super) identity: Option<NativeRootIdentity>,
    pub(super) retirement: Option<NativeMountRetirement>,
    pub(super) cleanup: NativeWorkReservation,
    pub(super) failure: Option<NativeMountError>,
    pub(super) _allocation: NativeAllocation,
}

impl FuseSessionState {
    pub(super) fn is_retired(&self) -> bool {
        self.worker.is_none()
            && self.before.is_none()
            && self
                .retirement
                .as_ref()
                .is_none_or(NativeMountRetirement::is_retired)
    }

    fn stop(
        &mut self,
        core: &Arc<ProjectionCore>,
        work: Option<&NativeMaterializationWork>,
    ) -> Result<(), NativeMountError> {
        if self.is_retired() {
            return Ok(());
        }
        let credit = self.cleanup.loan(None);
        let started = |actual| -> Result<(), NativeMountError> {
            match work {
                Some(work) => work.charge(actual)?,
                None => credit.started_io(actual)?,
            }
            Ok(())
        };
        if let Some(unmounter) = self.unmounter.as_mut() {
            started(WriteOperations.delta(1))?;
            core.stop();
            unmounter.unmount()?;
        }
        if let Some(device) = self.device.as_ref() {
            started(ReadOperations.delta(1))?;
            let mut poll = libc::pollfd {
                fd: device.as_raw_fd(),
                events: 0,
                revents: 0,
            };
            // SAFETY: poll owns one initialized descriptor record.
            if unsafe { libc::poll(&mut poll, 1, 0) } < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if poll.revents & libc::POLLERR == 0 {
                return Err(NativeMountError::FuseShutdownFailed);
            }
        } else if let Some(before) = self.before {
            let path =
                std::ffi::CStr::from_bytes_with_nul(&self.path).expect("terminated destination");
            let metadata = match work {
                Some(work) => fuse_recovery_stat(path, &mut &*work)?,
                None => {
                    started(ReadOperations.delta(1))?;
                    rustix::fs::stat(path).map_err(std::io::Error::from)?
                }
            };
            if (metadata.st_dev as u64, metadata.st_ino as u64) != before {
                return Err(NativeMountError::FuseShutdownFailed);
            }
        }
        self.unmounter.take();
        self.device.take();
        self.before.take();
        if self.worker.is_some() {
            started(ReadOperations.delta(1))?;
            self.worker
                .take()
                .expect("accepted SDK worker")
                .join()
                .map_err(|panic| NativeMountError::DriverPanicked {
                    operation: "FUSE SDK request worker",
                    panic: Mutex::new(panic),
                })??;
        }
        if let Some(retirement) = self.retirement.as_mut() {
            retirement.retire(None)?;
        }
        Ok(())
    }
}

impl FuseSession {
    pub(in crate::native_mount) fn start_into(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &NativeMaterializationWork,
        out: &mut Option<Self>,
        retirement: &mut Option<NativeMountRetirement>,
        prepare: impl FnOnce(&mut NativeMountRetirement) -> Result<(), NativeMountError>,
    ) -> Result<(), NativeMountError> {
        if out.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        fuse_native_source_original(source.as_ref(), Some(work))?;
        work.admit(WorkCounters::UNCHARGED)?;
        let stamp = source.view_stamp_with_meter(work)?;
        let root = source
            .lookup_with_meter(&MountPath::root(), work)?
            .ok_or(NativeMountError::Lifecycle("volume root is absent"))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(NativeMountError::Lifecycle(
                "volume root is not a directory",
            ));
        }
        let metadata = crate::native_host::unix_name(
            request.destination.as_os_str(),
            &mut &*work,
            fuse_recovery_stat,
        )?;
        let core = ProjectionCore::new(
            source,
            root,
            stamp,
            request.writable,
            metadata.st_uid,
            metadata.st_gid,
            work,
        )?;
        let _startup = NativeMountWorkScope::new(&core, work)?;
        core.source
            .observe_view_with_meter(Arc::downgrade(&core), work)?;
        let allocation = work.claim(
            (std::mem::size_of::<Mutex<FuseSessionState>>()
                + std::mem::size_of::<(Arc<ProjectionCore>, Arc<Mutex<FuseSessionState>>)>()
                + std::mem::size_of::<fuser::Session<FuseProjection>>()) as u64,
            3,
            WorkCounters::UNCHARGED,
        )?;
        let cleanup = work.reserve_work(WorkCounters {
            backend_write_operations: 1,
            backend_read_operations: 3,
            ..WorkCounters::UNCHARGED
        })?;
        let close_root = work.reserve_cleanup_write()?;
        let bytes = request.destination.as_os_str().as_bytes();
        let mut path = fuse_vec(bytes.len().checked_add(1).ok_or(WorkError::Overflow)?, work)?;
        work.admit(Copy.delta(bytes.len() as u64))?;
        path.extend_from_slice(bytes);
        path.push(0);
        work.record_admitted(Copy.delta(bytes.len() as u64))?;
        let state = Arc::new(Mutex::new(FuseSessionState {
            path,
            before: Some((metadata.st_dev as u64, metadata.st_ino as u64)),
            unmounter: None,
            worker: None,
            device: None,
            identity: None,
            retirement: None,
            cleanup,
            failure: None,
            _allocation: allocation,
        }));
        let job = (Arc::clone(&core), Arc::clone(&state));
        let mut started = state.lock().unwrap_or_else(PoisonError::into_inner);
        work.charge(WriteOperations.delta(1))?;
        let invalidator = std::thread::Builder::new().spawn(move || run_fuse_session(job))?;
        *out = Some(Self {
            core: Arc::clone(&core),
            state: Arc::clone(&state),
            invalidator: Some(invalidator),
        });
        started.retirement = retirement.take();
        let result = (|| {
            if let Some(packet) = started.retirement.as_mut() {
                prepare(packet)?;
            }
            let mut config = Config::default();
            config.mount_options = vec![
                fuser::MountOption::FSName("acyclic-fs".to_owned()),
                fuser::MountOption::DefaultPermissions,
                fuser::MountOption::Exec,
                fuser::MountOption::NoAtime,
            ];
            if !request.writable {
                config.mount_options.push(fuser::MountOption::RO);
            }
            config.n_threads = Some(8);
            config.clone_fd = false;
            work.charge(WriteOperations.delta(2))?;
            let filesystem = FuseProjection {
                core: Arc::clone(&core),
                skips_opendir: false,
                skips_open: false,
            };
            let mut session = fuser::Session::new(filesystem, &request.destination, &config)
                .map_err(|error| {
                    core.take_handler_failure()
                        .map(|failure| failure.0)
                        .unwrap_or_else(|| error.into())
                })?;
            let _ = core.notifier.set(session.notifier());
            started.unmounter = Some(session.unmount_callable());
            started.device = Some(std::os::fd::AsFd::as_fd(&session).try_clone_to_owned()?);
            // Session::spawn consumes its join handle on unmount refusal; retain it ourselves.
            started.worker = Some(std::thread::Builder::new().spawn(move || session.run())?);
            let (directory, identity) =
                crate::native_host::open_root_directory(&request.destination, &mut &*work)?;
            started.identity = Some(identity);
            close_root.attempt(|| drop(directory)).1?;
            Ok(())
        })();
        if result.is_err() {
            core.stop();
        }
        result
    }

    pub(in crate::native_mount) fn begin_original_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<NativeMountWorkScope, NativeMountError> {
        if self.core.stopping.load(Ordering::Acquire) {
            return Err(NativeMountError::Lifecycle("session is stopped"));
        }
        work.admit(WorkCounters::UNCHARGED)?;
        NativeMountWorkScope::new(&self.core, work)
    }
    pub(in crate::native_mount) fn native_root_identity(&self) -> Option<NativeRootIdentity> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.device.as_ref().and(state.identity)
    }
    pub(in crate::native_mount) fn take_callback_failure(
        &self,
    ) -> Option<(NativeMountError, WorkCounters)> {
        self.core.take_handler_failure()
    }
    pub(in crate::native_mount) fn handler_failed(&self) -> bool {
        self.core.handler_failed.load(Ordering::Acquire)
    }
    pub(in crate::native_mount) fn completed_callback_work(&self) -> WorkCounters {
        self.core.completed_callback_work()
    }
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .before
            .is_none()
    }
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_retired()
    }
    pub(in crate::native_mount) fn retirement_ownership(&self) -> (bool, bool) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.retirement.as_ref().map_or((false, false), |packet| {
            (
                packet.destination_guard.is_some(),
                packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
            )
        })
    }
    pub(in crate::native_mount) fn stop(
        &mut self,
        work: &NativeSessionStopWork<'_>,
    ) -> Result<(), NativeMountError> {
        let meter = match work {
            NativeSessionStopWork::Meter(work) => {
                work.admit(WorkCounters::UNCHARGED)?;
                Some(*work)
            }
            NativeSessionStopWork::AcceptedWait(wait) => {
                wait.check().map_err(|_| crate::DemandError::Cancelled)?;
                None
            }
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if meter.is_none()
            && let Some(error) = state.failure.take()
        {
            return Err(error);
        }
        state.stop(&self.core, meter)?;
        state.failure.take();
        if self.invalidator.is_some() {
            state
                .cleanup
                .loan(None)
                .started_io(ReadOperations.delta(1))?;
        }
        drop(state);
        if let Some(invalidator) = self.invalidator.take() {
            invalidator
                .join()
                .map_err(|panic| NativeMountError::DriverPanicked {
                    operation: "FUSE invalidator",
                    panic: Mutex::new(panic),
                })?;
        }
        Ok(())
    }
}

pub(super) fn run_fuse_session((core, state): (Arc<ProjectionCore>, Arc<Mutex<FuseSessionState>>)) {
    drop(state.lock().unwrap_or_else(PoisonError::into_inner));
    if let Some(notifier) = core.notifier.get() {
        run_invalidator(&core, notifier);
    } else {
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !core.stopping.load(Ordering::Acquire) {
            state = core
                .invalidation
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
    let mut retiring = state.lock().unwrap_or_else(PoisonError::into_inner);
    if let Err(error) = retiring.stop(&core, None) {
        retiring.failure.get_or_insert(error);
    }
    drop(retiring);
    // A refused cleanup keeps the original source, fence and join handles owned.
    while !state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_retired()
    {
        std::thread::park_timeout(Duration::from_millis(100));
    }
}

impl Drop for FuseSession {
    fn drop(&mut self) {
        self.core.stop();
    }
}
