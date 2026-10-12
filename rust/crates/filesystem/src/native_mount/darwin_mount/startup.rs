use super::*;

impl DarwinMountSession {
    pub(in crate::native_mount) fn start_into<F>(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        original: &crate::NativeMaterializationWork,
        out: &mut Option<Self>,
        retirement: &mut Option<crate::native_mount::NativeMountRetirement>,
        prepare: F,
    ) -> Result<(), NativeMountError>
    where
        F: FnOnce(&mut crate::native_mount::NativeMountRetirement) -> Result<(), NativeMountError>,
    {
        if out.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        original_work(source.as_ref(), Some(original))?;
        original.admit(crate::WorkCounters::UNCHARGED)?;
        static STARTUP: std::sync::LazyLock<Mutex<()>> =
            std::sync::LazyLock::new(|| Mutex::new(()));
        let _startup = STARTUP
            .lock()
            .map_err(|_| NativeMountError::Lifecycle("Darwin mount startup lock is poisoned"))?;
        let root = source
            .lookup_with_meter(&MountPath::root(), original)?
            .ok_or(NativeMountError::Lifecycle("volume root is absent"))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(NativeMountError::Lifecycle(
                "volume root is not a directory",
            ));
        }
        let mut host = original;
        let metadata = crate::native_host::unix_name(
            request.destination.as_os_str(),
            &mut host,
            |path, work| start_stat(path, work),
        )?;
        let parent = request
            .destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        crate::native_host::unix_name(parent.as_os_str(), &mut host, |path, work| {
            start_stat(path, work)
        })?;
        let mut context = DarwinMountContext::new(
            source,
            request.writable,
            metadata.st_uid,
            metadata.st_gid,
            root.node.file_id,
            original,
        )?;
        context
            .native_allocation
            .grow((2 * std::mem::size_of::<usize>()) as u64)?;
        let context = Arc::new(context);
        let _active = NativeMountWorkScope::new(Arc::clone(&context), original)?;
        context
            .source
            .observe_view_with_meter(Arc::downgrade(&context), original)?;
        let startup = StartupBuffers::new(
            &request.destination,
            request.writable,
            context.source.supports_posix_named_attributes(),
            original,
        )?;
        let destination = Arc::clone(&startup.destination);
        let resources_allocation = original.claim(
            (std::mem::size_of::<DriverSessionResources>() + 2 * std::mem::size_of::<usize>())
                as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let cleanup = original.reserve_work(crate::WorkCounters {
            backend_read_operations: 1,
            backend_write_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let resources = Arc::new(DriverSessionResources {
            context,
            native: Mutex::new((0, None, cleanup)),
            startup,
            _allocation: resources_allocation,
        });
        let unmount = DarwinStopController::new(
            &resources,
            Path::new(std::ffi::OsStr::from_bytes(destination.as_bytes())),
            original,
        )?;
        *out = Some(Self {
            handler_failure: Arc::clone(&resources.context.handler_failure),
            resources: Some(resources),
            destination,
            thread: None,
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: true,
            unmount,
            native_root_identity: None,
        });
        let session = out
            .as_mut()
            .expect("logical driver installed before SDK birth");
        let name = "acyclic-fs-darwin-nfs";
        let owned = (
            Arc::clone(session.resources.as_ref().expect("new resources")),
            Arc::clone(&session.unmount),
            Some(original.clone()),
        );
        let allocation = original.claim(
            (std::mem::size_of_val(&owned)
                + std::mem::size_of::<crate::NativeAllocation>()
                + name.len()) as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let owned = (owned.0, owned.1, owned.2, allocation);
        let name = copying(original, copy_work(name.len() as u64), || name.to_owned())?;
        original.charge(WRITE_WORK)?;
        session.thread = Some(
            std::thread::Builder::new()
                .name(name)
                .spawn(move || run_loop(owned))?,
        );
        session.teardown_complete = false;
        let mut packet = retirement.take();
        let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            packet.as_mut().map_or(Ok(()), prepare)
        }));
        {
            let mut state = session
                .unmount
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.retirement = packet;
            if !matches!(prepared, Ok(Ok(()))) {
                state.control.start_aborted = true;
            }
        }
        session.unmount.changed.notify_all();
        match prepared {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let resources = session.resources.as_ref().expect("new resources");
        match DriverSessionResources::new_sdk_session(original) {
            Ok((pointer, allocation)) => {
                let _state = session
                    .unmount
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let mut native = resources
                    .native
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                native.0 = pointer;
                native.1 = Some(allocation);
            }
            Err(error) => {
                session
                    .unmount
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .control
                    .start_aborted = true;
                session.unmount.changed.notify_all();
                return Err(error);
            }
        }
        session.unmount.changed.notify_all();
        session.await_mount(original)
    }

    fn await_mount(
        &mut self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let deadline = Instant::now() + MOUNT_VISIBILITY_TIMEOUT;
        let reported = loop {
            original.charge(scan_work(1))?;
            let mounted = self.unmount.mounted.load(Ordering::Acquire);
            if mounted || self.thread.as_ref().is_none_or(JoinHandle::is_finished) {
                break mounted;
            }
            original.admit(crate::WorkCounters::UNCHARGED)?;
            if Instant::now() >= deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            original.charge(READ_WORK)?;
            self.unmount.wait_until(Duration::from_millis(10), || {
                self.unmount.mounted.load(Ordering::Acquire)
                    || self.thread.as_ref().is_none_or(JoinHandle::is_finished)
            });
        };
        if let Some((error, _)) = self.take_callback_failure() {
            return Err(error);
        }
        if !reported {
            return Err(driver_errno(libc::EIO));
        }
        let mut state = self
            .unmount
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !state.probe.mounted(
            Path::new(std::ffi::OsStr::from_bytes(self.destination.as_bytes())),
            &ProbeWork {
                original,
                cleanup: None,
            },
        )? {
            return Err(driver_errno(libc::EIO));
        }
        drop(state);
        let read = READ_WORK;
        original.admit(read)?;
        let close = original.reserve_cleanup_write()?;
        original.record_admitted(read)?;
        let path = &self.resources()?.startup.destination;
        // SAFETY: startup retains the terminated destination through open.
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        use std::os::fd::{FromRawFd as _, IntoRawFd as _};
        // SAFETY: fd is a newly opened descriptor; its explicit close is prepaid.
        let file = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
        let mut host = original;
        let identity = crate::native_host::file_identity_and_length(&file, &mut host);
        let (closed, recorded) = close.attempt(|| {
            // SAFETY: file represents fd, and ManuallyDrop prevents a second close.
            if unsafe { libc::close(fd) } == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
        let _ = std::mem::ManuallyDrop::into_inner(file).into_raw_fd();
        recorded?;
        let (identity, _) = identity?;
        closed?;
        self.native_root_identity = Some(identity);
        Ok(())
    }
}

fn start_stat(
    path: &CStr,
    work: &crate::NativeMaterializationWork,
) -> Result<libc::stat, crate::NativeTreeMaterializationError> {
    loop {
        work.charge(READ_WORK)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: path is terminated and metadata is writable.
        if unsafe { libc::stat(path.as_ptr(), metadata.as_mut_ptr()) } == 0 {
            return Ok(unsafe { metadata.assume_init() });
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
}

fn run_loop(
    mut owned: (
        Arc<DriverSessionResources>,
        Arc<DarwinStopController>,
        Option<crate::NativeMaterializationWork>,
        crate::NativeAllocation,
    ),
) -> c_int {
    let (resources, controller, original, _) = &owned;
    let original = original
        .as_ref()
        .expect("born loop retains original execution");
    let mut state = controller
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if resources.pointer() == 0 && !state.control.start_aborted {
        state.start_wait.started_drop_io(READ_WORK);
        state = controller
            .changed
            .wait_while(state, |state| {
                resources.pointer() == 0 && !state.control.start_aborted
            })
            .unwrap_or_else(PoisonError::into_inner);
    }
    let aborted = state.control.start_aborted;
    drop(state);
    let admitted = if aborted {
        Err(NativeMountError::ControlPending)
    } else {
        original.charge(WRITE_WORK).map_err(Into::into)
    };
    let status = match admitted {
        Err(error) => {
            controller
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .control
                .start_aborted = true;
            let work = original.state.lock().work;
            resources.context.record_failure(error, work);
            -libc::EIO
        }
        Ok(()) => {
            let buffers = &resources.startup;
            let mut interrupted = 0;
            // SAFETY: this tuple retains SDK, argv, context, controller and output through the blocking loop.
            let status = unsafe {
                acyclic_fs_darwin_mount_run(
                    resources.pointer() as *mut c_void,
                    buffers.argument_count(),
                    buffers.pointers.as_ptr().cast::<*const c_char>(),
                    buffers.destination.as_ptr(),
                    Arc::as_ptr(&resources.context) as usize,
                    report_mounted,
                    Arc::as_ptr(controller).cast_mut().cast(),
                    &mut interrupted,
                )
            };
            if interrupted != 0 {
                controller
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .control
                    .start_aborted = true;
            }
            status
        }
    };
    owned.1.provider_exited.store(true, Ordering::Release);
    owned.1.changed.notify_all();
    drop(owned.2.take());
    owned.1.finish(&owned.0);
    status
}
