use super::*;

pub(in crate::native_mount) struct FuseSession {
    pub(super) control: Arc<FuseDriverControl>,
    pub(super) invalidator: Option<std::thread::JoinHandle<()>>,
}

impl FuseSession {
    pub(in crate::native_mount) fn start_into(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &crate::NativeMaterializationWork,
        out: &mut Option<Self>,
        retirement: &mut Option<crate::native_mount::NativeMountRetirement>,
        prepare: impl FnOnce(
            &mut crate::native_mount::NativeMountRetirement,
        ) -> Result<(), NativeMountError>,
    ) -> Result<(), NativeMountError> {
        if out.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        fuse_native_source_original(source.as_ref(), Some(work))?;
        work.admit(crate::WorkCounters::UNCHARGED)?;
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
            .observe_view_with_meter(Arc::<ProjectionCore>::downgrade(&core), work)?;
        let mut config = Config::default();
        config.n_threads = Some(8);
        let mount = FuseMount::prepare(&request.destination, work)?;
        let birth_cleanup = work.reserve_work(crate::WorkCounters {
            backend_write_operations: 3,
            backend_read_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let normal = work.allocation_work();
        let retirement_helper = match fuse_recovery_helper(&normal) {
            Ok(program) => Some(FuseRetirementHelper::prepare(program, &normal)?),
            Err(NativeMountError::CapabilityUnavailable(_)) => None,
            Err(error) => return Err(error),
        };
        let control_allocation = normal.claim(
            std::mem::size_of::<FuseDriverControl>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let control = Arc::new(FuseDriverControl {
            state: Mutex::new(FuseDriverControlState {
                sdk_session: None,
                physical: Some(FusePhysicalSession {
                    session: None,
                    sdk_worker: None,
                    active_root: None,
                    native_root_identity: None,
                    retirement_helper,
                    birth_cleanup,
                }),
                retirement: None,
                loan_custody: (false, false),
                startup_finished: false,
                stop_requested: false,
                retired: false,
                failure: None,
            }),
            changed: Condvar::new(),
            core: Arc::clone(&core),
            _allocation: control_allocation,
        });
        let allocation = normal.claim(
            std::mem::size_of::<(Arc<FuseDriverControl>, crate::NativeAllocation)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let job = (Arc::clone(&control), allocation);
        work.charge(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let invalidator = std::thread::Builder::new().spawn(move || run_fuse_retirement(job))?;
        *out = Some(Self {
            control: Arc::clone(&control),
            invalidator: Some(invalidator),
        });
        control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retirement = retirement.take();
        let mut loan = FuseDriverLoan::take(&control)?;
        let allocation = normal.claim(
            std::mem::size_of::<(Arc<FuseDriverControl>, crate::NativeAllocation)>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let job = (Arc::clone(&control), allocation);
        work.charge(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        loan.physical
            .as_mut()
            .expect("constructor owns original driver")
            .sdk_worker = Some(std::thread::Builder::new().spawn(move || run_fuse_sdk(job))?);
        if let Some(packet) = loan.retirement.as_mut() {
            prepare(packet)?;
        }
        work.charge(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let physical = loan
            .physical
            .as_mut()
            .expect("constructor owns original driver");
        physical.session = Some(mount);
        let filesystem = FuseProjection {
            core: Arc::clone(&control.core),
            skips_opendir: false,
            skips_open: false,
        };
        let mount = physical
            .session
            .as_mut()
            .ok_or(NativeMountError::ControlPending)?;
        if let Err(error) = mount.start(request.writable, metadata.st_mode) {
            return Err(core
                .take_handler_failure()
                .map(|failure| failure.0)
                .unwrap_or_else(|| error.into()));
        }
        let fd = mount
            .device
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?
            .try_clone()?;
        let session =
            fuser::Session::from_fd(filesystem, fd, config.acl, config).map_err(|error| {
                core.take_handler_failure()
                    .map(|failure| failure.0)
                    .unwrap_or_else(|| error.into())
            })?;
        let _ = core.notifier.set(session.notifier());
        control.submit_sdk(Some(session));
        if let Some(helper) = mount.helper.as_mut() {
            let status = helper.wait()?;
            mount.helper.take();
            if !status.success() {
                return Err(NativeMountError::FuseShutdownFailed);
            }
        }
        let (directory, identity) =
            crate::native_host::open_root_directory(&request.destination, &mut &*work)?;
        physical.active_root = Some(directory);
        physical.native_root_identity = Some(identity);
        close_fuse_root(&mut physical.active_root, work)?;
        loan.committed = true;
        Ok(())
    }

    pub(in crate::native_mount) fn begin_original_work(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<NativeMountWorkScope, NativeMountError> {
        let loan = FuseDriverLoan::take(&self.control)?;
        if loan
            .physical
            .as_ref()
            .expect("actual physical driver loan")
            .session
            .is_none()
        {
            return Err(NativeMountError::Lifecycle("session is stopped"));
        }
        work.admit(crate::WorkCounters::UNCHARGED)?;
        NativeMountWorkScope::new(&self.control.core, work)
    }

    pub(in crate::native_mount) fn native_root_identity(
        &self,
    ) -> Option<crate::NativeRootIdentity> {
        self.control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .physical
            .as_ref()
            .filter(|physical| physical.session.is_some())
            .and_then(|physical| physical.native_root_identity)
    }

    pub(in crate::native_mount) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let _loan = FuseDriverLoan::take(&self.control)?;
        let core = &self.control.core;
        let result = core.request(|work| {
            let _active = if core.active_meter().is_none() {
                Some(NativeMountWorkScope::new(core, work)?)
            } else {
                None
            };
            let path = path.strip_prefix(b"/").unwrap_or(path);
            let mut components = path.split(|byte| *byte == b'/').peekable();
            let mut parent = FuseOwned::empty(MountPath::root());
            let mut name = None;
            while let Some(component) = components.next() {
                fuse_item(work)?;
                if component.is_empty()
                    || component.contains(&0)
                    || component == b"."
                    || component == b".."
                {
                    return Err(NativeMountError::Lifecycle(
                        "FUSE invalidation requires a canonical non-root path",
                    ));
                }
                if components.peek().is_some() {
                    parent = fuse_child_path(&parent, component, work)?;
                } else {
                    name = Some(component);
                }
            }
            let name = name.ok_or(NativeMountError::Lifecycle(
                "FUSE invalidation requires a canonical non-root path",
            ))?;
            let source = core.source.as_ref();
            let child = ChildName::new(source, fuse_child_path(&parent, name, work)?, work)?;
            let parent = match source.folded_path_with_meter(&parent, work)? {
                Some((path, bytes)) => FuseOwned::adopt(path, bytes, work)?,
                None => parent,
            };
            let mut state = core.state()?;
            fuse_item(work)?;
            let file_inode = state.inode_by_path.get(child.key()).copied();
            fuse_item(work)?;
            let parent_inode = state.inode_by_path.get(&*parent).copied();
            let count = usize::from(file_inode.is_some()) + 2 * usize::from(parent_inode.is_some());
            let mut items = fuse_vec(count, work)?;
            if let Some(inode) = file_inode {
                items.push(KernelCacheItem::Inode(inode));
            }
            if let Some(parent) = parent_inode {
                items.push(KernelCacheItem::Entry {
                    parent,
                    name: fuse_copy_bytes(name, work)?,
                });
                items.push(KernelCacheItem::Inode(parent));
            }
            let copied = items
                .len()
                .checked_mul(std::mem::size_of::<KernelCacheItem>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            fuse_copy_admit(copied, work)?;
            fuse_reserve(&mut state.invalidation.deferred, items.len(), work)?;
            state.invalidation.deferred.append(&mut items);
            fuse_copy_record(copied, work)?;
            drop(items);
            let target = state.invalidation.notified;
            core.invalidation.notify_all();
            loop {
                work.admit(crate::WorkCounters::UNCHARGED)?;
                if core.handler_failed.load(Ordering::Acquire)
                    || state.invalidation.failure.is_some()
                {
                    return Err(NativeMountError::Lifecycle(
                        "FUSE invalidation previously failed",
                    ));
                }
                if core.stopping.load(Ordering::Acquire) {
                    return Err(NativeMountError::Lifecycle("FUSE session is stopped"));
                }
                if !state.invalidation.is_pending(target) {
                    return Ok(());
                }
                state = core
                    .invalidation
                    .wait_timeout(state, Duration::from_millis(10))
                    .map_err(|_| libc::EIO)?
                    .0;
            }
        });
        result.map_err(|failure| {
            if fatal_handler_error(&failure.0) {
                if let Some(cause) = clone_fatal_handler_error(&failure.0) {
                    core.retain_handler_failure((cause, failure.1));
                } else {
                    core.retain_handler_failure(failure);
                    return NativeMountError::Lifecycle("FUSE invalidation handler failed");
                }
            }
            failure.0
        })
    }

    pub(in crate::native_mount) fn revalidate(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let _loan = FuseDriverLoan::take(&self.control)?;
        let core = &self.control.core;
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let _active = NativeMountWorkScope::new(core, work)?;
        core.source.fence_changes_with_meter(work)?;
        let mut state = core
            .state
            .lock()
            .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?;
        let target = state.invalidation.notified;
        loop {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            if core.stopping.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle("session is stopped"));
            }
            if state.invalidation.failure.is_some() {
                return Err(NativeMountError::Lifecycle(
                    "FUSE invalidation previously failed",
                ));
            }
            let condition = if state.invalidation.is_pending(target) {
                &core.invalidation
            } else if target.is_some_and(|target| state.page_stores.precede(target)) {
                &core.page_stored
            } else {
                return Ok(());
            };
            state = condition
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?
                .0;
        }
    }

    pub(in crate::native_mount) fn take_callback_failure(
        &self,
    ) -> Option<(NativeMountError, crate::WorkCounters)> {
        self.control.core.take_handler_failure()
    }
    pub(in crate::native_mount) fn handler_failed(&self) -> bool {
        self.control.core.handler_failed.load(Ordering::Acquire)
    }
    pub(in crate::native_mount) fn completed_callback_work(&self) -> crate::WorkCounters {
        self.control.core.completed_callback_work()
    }
    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        self.control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .physical
            .as_ref()
            .is_some_and(|physical| physical.session.is_none())
    }
    pub(in crate::native_mount) fn controls_pending(&self) -> bool {
        !self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retired
    }
    pub(in crate::native_mount) fn retirement_ownership(&self) -> (bool, bool) {
        let state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state
            .retirement
            .as_ref()
            .map_or(state.loan_custody, |packet| {
                (
                    packet.destination_guard.is_some(),
                    packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
                )
            })
    }

    pub(in crate::native_mount) fn stop(
        &mut self,
        work: &crate::native_mount::NativeSessionStopWork<'_>,
    ) -> Result<(), NativeMountError> {
        let wait = match work {
            crate::native_mount::NativeSessionStopWork::Meter(work) => {
                let mut loan = FuseDriverLoan::take(&self.control)?;
                loan.physical
                    .as_mut()
                    .expect("actual physical driver loan")
                    .stop(&self.control, Some(work))?;
                None
            }
            crate::native_mount::NativeSessionStopWork::AcceptedWait(wait) => Some(*wait),
        };
        if let Some(wait) = wait {
            wait.check().map_err(|_| crate::DemandError::Cancelled)?;
        }
        self.control.core.stopping.store(true, Ordering::Release);
        self.control.core.invalidation.notify_all();
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.stop_requested = true;
        self.control.changed.notify_all();
        while !state.retired {
            if let Some(error) = state.failure.take() {
                return Err(error);
            }
            drop(state);
            if let Some(wait) = wait {
                wait.check().map_err(|_| crate::DemandError::Cancelled)?;
            }
            state = self
                .control
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !state.retired && state.failure.is_none() {
                state = self
                    .control
                    .changed
                    .wait_timeout(state, Duration::from_millis(10))
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        }
        let failure = state.failure.take();
        drop(state);
        if self.invalidator.is_some() {
            {
                let mut driver = FuseDriverLoan::take(&self.control)?;
                let physical = driver
                    .physical
                    .as_mut()
                    .expect("actual physical driver loan");
                physical
                    .birth_cleanup
                    .loan(None)
                    .started_io(crate::WorkCounters {
                        backend_read_operations: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
            }
            let worker = self
                .invalidator
                .take()
                .expect("original invalidator join remains owned");
            worker
                .join()
                .map_err(|panic| NativeMountError::DriverPanicked {
                    operation: "FUSE retirement producer",
                    panic: Mutex::new(panic),
                })?;
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for FuseSession {
    fn drop(&mut self) {
        self.control.core.stopping.store(true, Ordering::Release);
        self.control.core.invalidation.notify_all();
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.stop_requested = true;
        self.control.changed.notify_all();
    }
}
