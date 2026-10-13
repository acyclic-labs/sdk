use super::*;
use crate::materializer::NativeWorkReservation;

pub(super) struct DriverSessionResources {
    pub(super) state: Mutex<LoopState>,
    pub(super) changed: Condvar,
    pub(super) requested: AtomicBool,
    pub(super) provider_exited: AtomicBool,
    pub(super) credit: NativeWorkReservation,
    pub(super) native: Mutex<DarwinOwned<usize>>,
    pub(super) sdk_credit: NativeWorkReservation,
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
        const PROBES: u64 = control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL) + 3;
        const READS: u64 = PROBES * 5
            + 2
            + control_visits(DIRECT_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)
            + control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)
            + control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL)
            + control_visits(MOUNT_LOOP_EXIT_TIMEOUT, STOP_POLL_INTERVAL);
        let credit = work.reserve_work(WorkCounters {
            backend_read_operations: READS,
            backend_write_operations: PROBES,
            bytes_copied: PROBES
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
            native: Mutex::new(DarwinOwned::empty(0)),
            sdk_credit: cleanup,
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
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let result = (|| {
            // SAFETY: the bridge reports its fixed allocation size.
            let bytes = unsafe { acyclic_fs_darwin_mount_session_allocation_size() } as u64;
            let allocation = work.allocation_work().claim(bytes, 1, WRITE_WORK)?;
            // SAFETY: the allocator has no preconditions.
            let pointer = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
            if pointer == 0 {
                return Err(driver_errno(libc::ENOMEM));
            }
            let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
            native.value = pointer;
            native.allocation = Some(allocation);
            Ok(())
        })();
        if result.is_err() {
            state.control.start_aborted = true;
        }
        self.changed.notify_all();
        result
    }

    pub(super) fn pointer(&self) -> usize {
        self.native
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .value
    }

    pub(super) fn interrupt(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
        if native.value == 0 || self.sdk_credit.remaining_work().backend_write_operations < 2 {
            return Ok(());
        }
        if let Some(wait) = wait {
            self.sdk_credit.started_io(WRITE_WORK, Some(wait))?;
        } else {
            self.sdk_credit.started_drop_io(WRITE_WORK);
        }
        // SAFETY: this locked owner retains the live bridge session.
        unsafe {
            acyclic_fs_darwin_mount_interrupt(native.value as *mut c_void);
        }
        Ok(())
    }

    pub(super) fn free(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let mut native = self.native.lock().unwrap_or_else(PoisonError::into_inner);
        if native.value == 0 {
            return Ok(());
        }
        if let Some(wait) = wait {
            self.sdk_credit.started_io(WRITE_WORK, Some(wait))?;
        } else {
            self.sdk_credit.started_drop_io(WRITE_WORK);
        }
        // SAFETY: only the quiescent loop or its joined owner calls free.
        unsafe {
            acyclic_fs_darwin_mount_session_free(native.value as *mut c_void);
        }
        native.value = 0;
        drop(native.allocation.take());
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
        let mut context =
            DarwinMountContext::new(source, request.writable, &request.destination, original)?;
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
