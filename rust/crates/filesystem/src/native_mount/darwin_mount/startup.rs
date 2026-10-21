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
    #[allow(
        clippy::type_complexity,
        reason = "The SDK loop retains these existing resources together until it exits."
    )]
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
    pub(super) fn state(&self) -> std::sync::MutexGuard<'_, LoopState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
    pub(super) fn native(&self) -> std::sync::MutexGuard<'_, DarwinOwned<usize>> {
        self.native.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "At most three options fill three pairs in the seven-element argument array."
    )]
    pub(super) fn new(
        context: Arc<DarwinMountContext>,
        destination: &Path,
        work: &Work,
        owned: bool,
    ) -> Result<Arc<Self>, NativeMountError> {
        let probe = MountProbe::new(destination, work)?;
        const PROBES: u64 = control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL) + 3;
        const READS: u64 = PROBES
            + 2
            + control_visits(DIRECT_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)
            + control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)
            + control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL)
            + control_visits(MOUNT_LOOP_EXIT_TIMEOUT, STOP_POLL_INTERVAL);
        let credit = work.reserve_work(WorkCounters {
            backend_read_operations: READS,
            ..WorkCounters::UNCHARGED
        })?;
        let destination = Arc::clone(&probe.0);
        let options = [
            Some(c"nobrowse"),
            (!context.writable).then_some(c"ro"),
            context
                .source
                .supports_posix_named_attributes()
                .then_some(c"namedattr"),
        ];
        let mut pointers = [0; 7];
        pointers[0] = if owned { c"acyclic-fs" } else { c"darwinfuse" }.as_ptr() as usize;
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
        self.state()
            .resources
            .as_ref()
            .map(|resources| Arc::clone(&resources.0))
            .ok_or(NativeMountError::Lifecycle(
                "Darwin mount session has stopped",
            ))
    }
    pub(super) fn birth(&self, work: &Work) -> Result<(), NativeMountError> {
        let mut state = self.state();
        let result = (|| {
            // SAFETY: the bridge reports its fixed allocation size.
            let bytes = unsafe { acyclic_fs_darwin_mount_session_allocation_size() } as u64;
            let allocation = work.allocation_work().claim(bytes, 1, WRITE_WORK)?;
            // SAFETY: the allocator has no preconditions.
            let pointer = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
            if pointer == 0 {
                return Err(driver_errno(libc::ENOMEM));
            }
            let mut native = self.native();
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
        self.native().value
    }

    fn sdk_write(&self, wait: Option<&crate::CancellationToken>) -> Result<(), NativeMountError> {
        match wait {
            Some(wait) => self.sdk_credit.started_io(WRITE_WORK, Some(wait))?,
            None => self.sdk_credit.started_drop_io(WRITE_WORK),
        }
        Ok(())
    }

    pub(super) fn interrupt(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), NativeMountError> {
        let native = self.native();
        if native.value == 0 || self.sdk_credit.remaining_work().backend_write_operations < 2 {
            return Ok(());
        }
        self.sdk_write(wait)?;
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
        let mut native = self.native();
        if native.value == 0 {
            return Ok(());
        }
        self.sdk_write(wait)?;
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
    #[allow(
        clippy::expect_used,
        reason = "Free without a wait token consumes birth-paid credit and has no fallible path."
    )]
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
        let observer: Arc<dyn ViewObserver> = context.clone();
        context
            .source
            .observe_view_with_meter(Arc::downgrade(&observer), original)?;
        let resources = DriverSessionResources::new(
            context,
            &request.destination,
            original,
            retirement.is_some(),
        )?;
        *out = Some(Self {
            handler_failure: Arc::clone(&resources.context()?.handler_failure),
            unmount: resources,
            thread: None,
            loop_finished_before_teardown: None,
            loop_result: None,
            native_root_identity: None,
        });
        let session = out.as_mut().ok_or(NativeMountError::ControlPending)?;
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
            let mut state = session.unmount.state();
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
        let mut state = self.unmount.state();
        while !state.mounted && !self.unmount.provider_exited.load(Ordering::Acquire) {
            original.charge(READ_WORK)?;
            if Instant::now() >= deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            state = self
                .unmount
                .changed
                .wait_timeout(state, STOP_POLL_INTERVAL)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        if let Some((error, _)) = self.take_callback_failure() {
            return Err(error);
        }
        if !state.mounted {
            return Err(driver_errno(libc::EIO));
        }
        let LoopState {
            resources, probe, ..
        } = &mut *state;
        let destination = Path::new(std::ffi::OsStr::from_bytes(
            resources
                .as_ref()
                .ok_or(NativeMountError::ControlPending)?
                .1
                .as_bytes(),
        ));
        if !probe.mounted(destination, &ProbeWork::Live(original))? {
            return Err(driver_errno(libc::EIO));
        }
        self.native_root_identity =
            Some(crate::native_host::HostRoot::open(destination, &mut &*original)?.identity());
        Ok(())
    }
}

#[allow(
    clippy::expect_used,
    reason = "The provider owns its resources until it announces exit and retires them."
)]
fn run_loop(
    (driver, original, _allocation): (Arc<DriverSessionResources>, Work, NativeAllocation),
) -> c_int {
    let mut state = driver.state();
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
    let result = crate::native_mount::run_native_mount_handler_on_meter(&original, |original| {
        if aborted {
            return Err(NativeMountError::ControlPending);
        }
        original.charge(WRITE_WORK)?;
        let mut interrupted = 0;
        let argc = c_int::try_from(
            pointers
                .iter()
                .position(|pointer| *pointer == 0)
                .unwrap_or(pointers.len()),
        )
        .map_err(|_| driver_errno(libc::EOVERFLOW))?;
        // SAFETY: the provider retains SDK, context, arguments, destination and callback owner.
        let status = unsafe {
            acyclic_fs_darwin_mount_run(
                driver.pointer() as *mut c_void,
                argc,
                pointers.as_ptr().cast(),
                destination.as_ptr(),
                Arc::as_ptr(&context) as usize,
                report_mounted,
                Arc::as_ptr(&driver).cast_mut().cast(),
                &mut interrupted,
            )
        };
        if interrupted != 0 {
            driver.state().control.start_aborted = true;
        }
        Ok(status)
    });
    let status = result.unwrap_or_else(|error| {
        driver.state().control.start_aborted = true;
        -context.record_failure(error, original.receipt())
    });
    driver.provider_exited.store(true, Ordering::Release);
    driver.changed.notify_all();
    drop(original);
    driver.finish();
    status
}
