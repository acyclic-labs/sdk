use super::*;
use crate::materializer::NativeResourceCredit::{ReadOperations, WriteOperations};
use crate::native_mount::retirement::SourceRetirementWait;
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
    pub(super) mount: super::mount::Mount,
    pub(super) worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    pub(super) device: Option<FuseDevice>,
    pub(super) notifier: Option<Notifier>,
    pub(super) identity: Option<NativeRootIdentity>,
    pub(super) retirement: Option<NativeMountRetirement>,
    pub(super) cleanup: NativeWorkReservation,
    pub(super) failure: crate::native_mount::retirement::TerminalFailures,
    pub(super) shared: Option<super::shared_instance::SharedFuseView>,
    pub(super) _allocation: NativeAllocation,
}

/// The session's FUSE device. Until fuser's session owns the device this
/// state holds a descriptor; afterwards it holds a notifier, which shares
/// fuser's descriptor and keeps it open, so a mounted view costs one.
pub(super) enum FuseDevice {
    Owned(OwnedFd),
    Session {
        _notifier: Notifier,
        device: std::os::fd::RawFd,
    },
}

impl FuseDevice {
    pub(super) fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        use std::os::fd::AsFd as _;
        match self {
            Self::Owned(device) => device.as_fd(),
            // SAFETY: the notifier shares the open device that owns this
            // descriptor, which therefore stays open while it lives.
            Self::Session { device, .. } => unsafe { std::os::fd::BorrowedFd::borrow_raw(*device) },
        }
    }
}

impl FuseSessionState {
    pub(super) fn is_retired(&self) -> bool {
        self.worker.is_none()
            && self.shared.is_none()
            && !self.mount.pending()
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
        join: SourceRetirementWait,
    ) -> Result<(), NativeMountError> {
        if self.is_retired() {
            return Ok(());
        }
        let credit = &self.cleanup;
        let started = |actual| -> Result<(), NativeMountError> {
            match work {
                Some(work) => work.charge(actual)?,
                None => credit.started_io(actual, None)?,
            }
            Ok(())
        };
        if self.shared.is_some() {
            // Retirement drains the view's admitted requests; dropping the
            // last view of an instance unmounts it.
            started(WriteOperations.delta(1))?;
            let mut view = self.shared.take().ok_or(NativeMountError::ControlPending)?;
            view.retire();
            drop(view);
        }
        if self.mount.needs_unmount() {
            started(WriteOperations.delta(1))?;
            core.stop();
            let path = std::ffi::CStr::from_bytes_with_nul(&self.path)
                .map_err(|_| NativeMountError::FuseShutdownFailed)?;
            self.mount
                .stop(path, self.device.as_ref().map(FuseDevice::as_fd))?;
        }
        if let Some(device) = self.device.as_ref() {
            started(ReadOperations.delta(1))?;
            if !self.mount.wait_device_detached(device.as_fd())? {
                return Err(NativeMountError::FuseShutdownFailed);
            }
        } else if let Some(before) = self.before {
            let path = std::ffi::CStr::from_bytes_with_nul(&self.path)
                .map_err(|_| NativeMountError::FuseShutdownFailed)?;
            let metadata = if let Some(work) = work {
                fuse_recovery_stat(path, &mut &*work)?
            } else {
                started(ReadOperations.delta(1))?;
                rustix::fs::stat(path).map_err(std::io::Error::from)?
            };
            if (metadata.st_dev as u64, metadata.st_ino as u64) != before {
                return Err(NativeMountError::FuseShutdownFailed);
            }
        }
        self.device.take();
        self.before.take();
        if self.worker.is_some() {
            started(ReadOperations.delta(1))?;
            self.failure.producer = self
                .worker
                .take()
                .ok_or(NativeMountError::FuseShutdownFailed)?
                .join()
                .map_err(|panic| NativeMountError::DriverPanicked {
                    operation: "FUSE SDK request worker",
                    panic: Mutex::new(panic),
                })
                .and_then(|result| result.map_err(NativeMountError::from))
                .err();
            // The worker has physically exited even when its terminal value
            // failed. The same owner must still retire its source and fence.
        }
        if let Some(retirement) = self.retirement.as_mut() {
            retirement.retire(None, join)?;
        }
        Ok(())
    }
}

impl FuseSession {
    #[allow(
        clippy::too_many_lines,
        reason = "Startup retains its admitted worker, mount and rollback controls in one frame."
    )]
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
        if request.shared {
            return Self::start_shared_into(request, source, work, out, retirement, root, stamp);
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
            (metadata.st_uid, metadata.st_gid),
            0,
            work,
        )?;
        let _startup = NativeMountWorkScope::new(&core, work)?;
        let observer: Arc<dyn ViewObserver> = core.clone();
        core.source
            .observe_view_with_meter(Arc::downgrade(&observer), work)?;
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
            before: Some((metadata.st_dev, metadata.st_ino)),
            mount: super::mount::Mount::default(),
            worker: None,
            device: None,
            notifier: None,
            identity: None,
            retirement: None,
            cleanup,
            failure: crate::native_mount::retirement::TerminalFailures::default(),
            shared: None,
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
            map_read_buffers();
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
            config.n_threads = Some(crate::native_mount::CALLBACK_THREADS as usize);
            config.clone_fd = false;
            work.charge(WriteOperations.delta(2))?;
            let filesystem = FuseProjection {
                core: Arc::clone(&core),
            };
            let FuseSessionState { mount, device, .. } = &mut *started;
            let mut received = None;
            let opened = mount.open_into(
                &request.destination,
                request.writable,
                || super::lifecycle::fuse_recovery_helper(work),
                &mut received,
            );
            *device = received.map(FuseDevice::Owned);
            opened?;
            // A failed handshake closes the session's descriptor, so the
            // state holds its own until fuser's session owns the device.
            let descriptor = device
                .as_ref()
                .ok_or(NativeMountError::FuseShutdownFailed)?
                .as_fd()
                .try_clone_to_owned()?;
            let session =
                fuser::Session::from_fd(filesystem, descriptor, fuser::SessionACL::Owner, config)
                    .map_err(|error| {
                    core.take_handler_failure()
                        .map_or_else(|| error.into(), |failure| failure.0)
                })?;
            started.notifier = Some(session.notifier());
            let _ = core.notifier.set(session.notifier());
            started.device = Some(FuseDevice::Session {
                _notifier: session.notifier(),
                device: {
                    use std::os::fd::{AsFd as _, AsRawFd as _};
                    session.as_fd().as_raw_fd()
                },
            });
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

    /// Lists the view in the shared instance at the destination's parent.
    /// The view owns no mount, so it takes no destination fence; the shared
    /// root holds the one fence.
    fn start_shared_into(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &NativeMaterializationWork,
        out: &mut Option<Self>,
        retirement: &mut Option<NativeMountRetirement>,
        root: MountLookup,
        stamp: Option<ViewStamp>,
    ) -> Result<(), NativeMountError> {
        let _services = crate::native_mount::adapter::CallbackRuntime::existing()?;
        work.state.lock().check()?;
        let parent = request
            .destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        let metadata =
            crate::native_host::unix_name(parent.as_os_str(), &mut &*work, fuse_recovery_stat)?;
        let allocation = work.claim(
            std::mem::size_of::<Mutex<FuseSessionState>>() as u64,
            1,
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
        let mut startup = None;
        // The core observes its source before the view is listed, as a
        // standalone mount's core does before it mounts.
        let view = super::shared_instance::SharedFuseView::start(&request.destination, |base| {
            let core = ProjectionCore::new(
                source,
                root,
                stamp,
                request.writable,
                (metadata.st_uid, metadata.st_gid),
                base,
                work,
            )?;
            startup = Some(NativeMountWorkScope::new(&core, work)?);
            let observer: Arc<dyn ViewObserver> = core.clone();
            core.source
                .observe_view_with_meter(Arc::downgrade(&observer), work)?;
            Ok(core)
        })?;
        let core = Arc::clone(view.core());
        let (directory, identity) =
            crate::native_host::open_root_directory(&request.destination, &mut &*work)?;
        close_root.attempt(|| drop(directory)).1?;
        let state = Arc::new(Mutex::new(FuseSessionState {
            path,
            before: None,
            mount: super::mount::Mount::default(),
            worker: None,
            device: None,
            notifier: None,
            identity: Some(identity),
            retirement: retirement.take(),
            cleanup,
            failure: crate::native_mount::retirement::TerminalFailures::default(),
            shared: Some(view),
            _allocation: allocation,
        }));
        *out = Some(Self {
            core,
            state,
            invalidator: None,
        });
        drop(startup);
        Ok(())
    }

    pub(in crate::native_mount) fn prepare_destination_cleanup(
        &mut self,
        destination: &std::path::Path,
        work: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        // Lock order: customer session -> driver state -> original work ledger.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // A shared view holds no fence; its retirement removes its directory.
        if state.shared.is_some() {
            return Ok(());
        }
        state
            .retirement
            .as_mut()
            .and_then(|packet| packet.destination_guard.as_mut())
            .ok_or(NativeMountError::ControlPending)?
            .prepare_destination_cleanup(destination, work)
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
        (state.device.is_some() || state.shared.is_some())
            .then_some(state.identity)
            .flatten()
    }
    pub(in crate::native_mount) fn take_callback_failure(
        &self,
    ) -> Option<(NativeMountError, WorkCounters)> {
        self.core.take_handler_failure()
    }
    pub(in crate::native_mount) fn completed_callback_work(&self) -> WorkCounters {
        self.core.completed_callback_work()
    }
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.before.is_none() && state.shared.is_none()
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
    ) -> Result<crate::native_mount::retirement::TerminalFailures, NativeMountError> {
        let meter = match work {
            NativeSessionStopWork::Meter(work) => {
                work.admit(WorkCounters::UNCHARGED)?;
                Some(*work)
            }
            NativeSessionStopWork::AcceptedWait(_) | NativeSessionStopWork::AcceptedDrop => {
                work.check_wait()?;
                None
            }
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.stop(&self.core, meter, SourceRetirementWait::Caller)?;
        if self.invalidator.is_some() {
            state.cleanup.started_io(ReadOperations.delta(1), None)?;
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
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(std::mem::take(&mut state.failure))
    }
}

/// Set in the negative control of the read buffers' test: glibc keeps its
/// adaptive threshold.
#[cfg(test)]
pub(super) const DYNAMIC_THRESHOLD: &str = "ACYCLIC_FS_TEST_DYNAMIC_MMAP_THRESHOLD";

/// Keeps every session's read buffers in their own mappings. glibc raises its
/// mapping threshold past each mapped block it frees, so once one session
/// unmounts, later sessions' buffers would come from the heap: zeroed there,
/// resident whole, and kept by the process after their session ends. A fixed
/// threshold keeps each buffer a mapping that only the requests the kernel
/// writes into make resident, returned whole when its session ends.
pub(super) fn map_read_buffers() {
    #[cfg(test)]
    if std::env::var_os(DYNAMIC_THRESHOLD).is_some() {
        return;
    }
    #[cfg(target_env = "gnu")]
    {
        // fuser reads each request into a buffer of its largest write plus
        // a page, one per callback thread.
        const READ_BUFFER_BYTES: i32 = 16 << 20;
        static FIXED: std::sync::Once = std::sync::Once::new();
        // SAFETY: mallopt only adjusts allocator tuning; it is thread-safe.
        FIXED.call_once(|| unsafe {
            libc::mallopt(libc::M_MMAP_THRESHOLD, READ_BUFFER_BYTES);
        });
    }
}

pub(super) fn run_fuse_session((core, state): (Arc<ProjectionCore>, Arc<Mutex<FuseSessionState>>)) {
    let notifier = state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .notifier
        .take();
    if let Some(notifier) = notifier {
        run_invalidator(&core, &notifier);
    } else {
        let mut projection = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !core.stopping.load(Ordering::Acquire) {
            match core.invalidation.wait(projection) {
                Ok(next) => projection = next,
                Err(poisoned) => {
                    projection = poisoned.into_inner();
                    break;
                }
            }
        }
        drop(projection);
    }
    let mut retiring = state.lock().unwrap_or_else(PoisonError::into_inner);
    if let Err(error) = retiring.stop(&core, None, SourceRetirementWait::Physical) {
        // This physical worker executes the retirement pass once. Its cleanup
        // cause is distinct from the namespace worker's already-owned cause.
        retiring.failure.cleanup = Some(error);
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
