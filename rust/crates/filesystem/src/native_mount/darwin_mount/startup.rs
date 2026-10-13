use super::*;
use crate::materializer::NativeWorkReservation;

pub(super) struct DriverSessionResources {
    pub(super) state: Mutex<LoopState>,
    pub(super) changed: Condvar,
    pub(super) requested: AtomicBool,
    pub(super) provider_exited: AtomicBool,
    pub(super) credit: NativeWorkReservation,
    pub(super) native: Mutex<(usize, Option<NativeAllocation>, NativeWorkReservation)>,
    _allocation: NativeAllocation,
}

pub(super) struct LoopState {
    pub(super) resources: Option<(
        Arc<DarwinMountContext>,
        Arc<DarwinOwned<CString>>,
        [usize; 7],
    )>,
    pub(super) mounted: bool,
    pub(super) control: DarwinRecoveryControl,
    pub(super) probe: MountProbe,
    pub(super) retirement: Option<crate::native_mount::NativeMountRetirement>,
    pub(super) failure: Option<NativeMountError>,
}

impl DriverSessionResources {
    pub(super) fn new(
        context: Arc<DarwinMountContext>,
        destination: &Path,
        work: &Work,
    ) -> Result<Arc<Self>, NativeMountError> {
        let name = destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?
            .as_bytes();
        let mut probe = MountProbe::new(destination, work)?;
        probe.preallocate(work)?;
        let probes = control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL)? + 3;
        let mut waits = 0_u64;
        for timeout in [
            DIRECT_UNMOUNT_TIMEOUT,
            DISKUTIL_UNMOUNT_TIMEOUT,
            DISKUTIL_VISIBILITY_TIMEOUT,
            MOUNT_LOOP_EXIT_TIMEOUT,
        ] {
            waits = waits
                .checked_add(control_visits(timeout, STOP_POLL_INTERVAL)?)
                .ok_or(WorkError::Overflow)?;
        }
        let credit = work.reserve_work(WorkCounters {
            backend_read_operations: probes
                .checked_mul(5)
                .and_then(|n| n.checked_add(waits + 2))
                .ok_or(WorkError::Overflow)?,
            backend_write_operations: probes,
            bytes_copied: probes
                .checked_mul(name.len() as u64)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        let destination = shared(work, native_name(destination.as_os_str().as_bytes(), work)?)?;
        let options = [
            Some(c"nobrowse"),
            (!context.writable).then_some(c"ro"),
            context
                .source
                .supports_posix_named_attributes()
                .then_some(c"namedattr"),
        ];
        let mut pointers = [0; 7];
        pointers[0] = c"acyclic-fs".as_ptr() as usize;
        for (index, option) in options.into_iter().flatten().enumerate() {
            pointers[2 * index + 1] = c"-o".as_ptr() as usize;
            pointers[2 * index + 2] = option.as_ptr() as usize;
        }
        let control = DarwinRecoveryControl {
            direct: Some(DirectDetach::new(Arc::clone(&destination), work)?),
            diskutil: Some(Diskutil::prepare(work)?),
            ..DarwinRecoveryControl::default()
        };
        let cleanup = work.reserve_work(WorkCounters {
            backend_read_operations: 1,
            backend_write_operations: 2,
            ..WorkCounters::UNCHARGED
        })?;
        let allocation = work.claim(
            (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        Ok(Arc::new(Self {
            state: Mutex::new(LoopState {
                resources: Some((context, destination, pointers)),
                mounted: false,
                control,
                probe,
                retirement: None,
                failure: None,
            }),
            changed: Condvar::new(),
            requested: AtomicBool::new(false),
            provider_exited: AtomicBool::new(false),
            credit,
            native: Mutex::new((0, None, cleanup)),
            _allocation: allocation,
        }))
    }

    pub(super) fn context(&self) -> Result<Arc<DarwinMountContext>, NativeMountError> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .resources
            .as_ref()
            .map(|resources| Arc::clone(&resources.0))
            .ok_or(NativeMountError::Lifecycle(
                "Darwin mount session has stopped",
            ))
    }
    pub(super) fn birth(&self, work: &Work) -> Result<(), NativeMountError> {
        let created = (|| {
            // SAFETY: the bridge reports its fixed allocation size.
            let bytes = unsafe { acyclic_fs_darwin_mount_session_allocation_size() } as u64;
            let allocation = work.allocation_work().claim(bytes, 1, WRITE_WORK)?;
            // SAFETY: the allocator has no preconditions.
            let pointer = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
            if pointer == 0 {
                return Err(driver_errno(libc::ENOMEM));
            }
            Ok((pointer, allocation))
        })();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let result = match created {
            Ok((pointer, allocation)) => {
                let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
                native.0 = pointer;
                native.1 = Some(allocation);
                Ok(())
            }
            Err(error) => {
                state.control.start_aborted = true;
                Err(error)
            }
        };
        self.changed.notify_all();
        result
    }

    pub(super) fn pointer(&self) -> usize {
        self.native.lock().unwrap_or_else(PoisonError::into_inner).0
    }

    pub(super) fn interrupt(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
        if native.0 == 0 || native.2.remaining_work().backend_write_operations < 2 {
            return Ok(());
        }
        let write = WRITE_WORK;
        if let Some(wait) = wait {
            native.2.loan(Some(wait)).started_io(write)?;
        } else {
            native.2.started_drop_io(write);
        }
        // SAFETY: this locked owner retains the live bridge session.
        unsafe {
            acyclic_fs_darwin_mount_interrupt(native.0 as *mut c_void);
        }
        Ok(())
    }

    pub(super) fn start_join(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        self.native
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .2
            .loan(Some(wait))
            .started_io(READ_WORK)?;
        Ok(())
    }

    pub(super) fn free(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
        if native.0 == 0 {
            return Ok(());
        }
        let write = WRITE_WORK;
        if let Some(wait) = wait {
            native.2.loan(Some(wait)).started_io(write)?;
        } else {
            native.2.started_drop_io(write);
        }
        // SAFETY: only the quiescent loop or its joined owner calls free.
        unsafe {
            acyclic_fs_darwin_mount_session_free(native.0 as *mut c_void);
        }
        native.0 = 0;
        drop(native.1.take());
        Ok(())
    }
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        self.free(None).expect("birth-paid SDK destruction");
    }
}

impl DarwinMountSession {
    pub(in crate::native_mount) fn start_into<F>(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        original: &Work,
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
        original.admit(WorkCounters::UNCHARGED)?;
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
        let resources = DriverSessionResources::new(context, &request.destination, original)?;
        *out = Some(Self {
            handler_failure: Arc::clone(&resources.context()?.handler_failure),
            unmount: resources,
            thread: None,
            loop_finished_before_teardown: None,
            loop_result: None,
            native_root_identity: None,
        });
        let session = out
            .as_mut()
            .expect("logical driver installed before SDK birth");
        let owned = (Arc::clone(&session.unmount), original.clone());
        let allocation = original.claim(
            (std::mem::size_of_val(&owned) + std::mem::size_of::<NativeAllocation>()) as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        original.charge(WRITE_WORK)?;
        session.thread = Some(
            std::thread::Builder::new().spawn(move || run_loop((owned.0, owned.1, allocation)))?,
        );
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
        session.unmount.birth(original)?;
        session.await_mount(original)
    }

    fn await_mount(&mut self, original: &Work) -> Result<(), NativeMountError> {
        let deadline = Instant::now() + MOUNT_VISIBILITY_TIMEOUT;
        let reported = loop {
            original.charge(scan_work(1))?;
            let mounted = self
                .unmount
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .mounted;
            if mounted || self.thread.as_ref().is_none_or(JoinHandle::is_finished) {
                break mounted;
            }
            original.admit(WorkCounters::UNCHARGED)?;
            if Instant::now() >= deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            original.charge(READ_WORK)?;
            self.unmount.wait_until(Duration::from_millis(10), |state| {
                state.mounted || self.thread.as_ref().is_none_or(JoinHandle::is_finished)
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
        let LoopState {
            resources, probe, ..
        } = &mut *state;
        let destination = resources
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?
            .1
            .as_bytes();
        let destination = Path::new(std::ffi::OsStr::from_bytes(destination));
        if !probe.mounted(destination, &ProbeWork::Live(original))? {
            return Err(driver_errno(libc::EIO));
        }
        self.native_root_identity = Some(crate::native_host::root_path_identity(
            destination,
            &mut &*original,
        )?);
        Ok(())
    }
}

fn start_stat(
    path: &CStr,
    work: &Work,
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
    (driver, original, _allocation): (Arc<DriverSessionResources>, Work, NativeAllocation),
) -> c_int {
    let mut state = driver.state.lock().unwrap_or_else(PoisonError::into_inner);
    if driver.pointer() == 0 && !state.control.start_aborted {
        driver.credit.started_drop_io(READ_WORK);
        state = driver
            .changed
            .wait_while(state, |state| {
                driver.pointer() == 0 && !state.control.start_aborted
            })
            .unwrap_or_else(PoisonError::into_inner);
    }
    let aborted = state.control.start_aborted;
    let (context, destination, pointers) = state
        .resources
        .as_ref()
        .expect("provider resources")
        .clone();
    drop(state);
    let admitted = if aborted {
        Err(NativeMountError::ControlPending)
    } else {
        original.charge(WRITE_WORK).map_err(Into::into)
    };
    let status = match admitted {
        Err(error) => {
            driver
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .control
                .start_aborted = true;
            context.record_failure(error, original.receipt());
            -libc::EIO
        }
        Ok(()) => {
            let mut interrupted = 0;
            // SAFETY: this thread retains the SDK owner, context, argv and destination through the loop.
            let status = unsafe {
                acyclic_fs_darwin_mount_run(
                    driver.pointer() as *mut c_void,
                    pointers
                        .iter()
                        .position(|pointer| *pointer == 0)
                        .unwrap_or(7) as c_int,
                    pointers.as_ptr().cast(),
                    destination.as_ptr(),
                    Arc::as_ptr(&context) as usize,
                    report_mounted,
                    Arc::as_ptr(&driver).cast_mut().cast(),
                    &mut interrupted,
                )
            };
            if interrupted != 0 {
                driver
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .control
                    .start_aborted = true;
            }
            status
        }
    };
    driver.provider_exited.store(true, Ordering::Release);
    driver.changed.notify_all();
    drop(original);
    driver.finish();
    status
}
