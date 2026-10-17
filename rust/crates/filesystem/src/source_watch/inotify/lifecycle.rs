use super::*;

pub(super) extern "C" fn run_reader(payload: *mut libc::c_void) -> *mut libc::c_void {
    // SAFETY: startup transfers one Arc reference to this entrypoint.
    let reader = unsafe { Arc::from_raw(payload.cast::<Shared>()) };
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.run())).is_err() {
        reader.delivery.exact.store(false, Ordering::Release);
    }
    if reader.delivery.original.is_some() {
        reader.finish_native_reader();
    }
    std::ptr::null_mut()
}

impl NativeSourceWatch {
    #[allow(
        clippy::too_many_lines,
        reason = "Startup retains every cleanup slot until worker ownership transfers or rollback completes."
    )]
    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let mut cleanup = StartupCleanup::reserve(work)?;
        let prepared = (|| {
            work.admit(READ)?;
            let failure = delivery.prepare_startup_failure(work)?;
            let ownership = SourcePayloadOwnership::new(work)?;
            Ok::<_, DemandError>((failure, ownership))
        })();
        let (startup_failure, ownership) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                cleanup.cancel(work);
                return Err(error);
            }
        };
        let mut inotify = None;
        let mut wake = None;
        let mut root = None;
        let mut ancestors = Vec::new();
        let mut buffer = Vec::new();
        let mut watches = Vec::new();
        let mut shared = None;
        let mut retained = 0_u64;
        let mut transferred = false;
        let control_bytes = arc_control_bytes::<Shared>();
        let mut control_transferred = false;
        let result = (|| {
            work.admit(READ)?;
            work.claim_allocation(
                control_bytes,
                u64::from((control_bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            retained = control_bytes;
            shared = Some(Arc::new(Shared {
                inotify: AtomicI32::new(-1),
                wake: AtomicI32::new(-1),
                root: AtomicI32::new(-1),
                ancestors: Mutex::new(Vec::new()),
                retained_variable_bytes: AtomicU64::new(0),
                reading: Mutex::new(RawReads::default()),
                reading_changed: Condvar::new(),
                watches: Mutex::new(Vec::new()),
                stopping: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                controller_alive: AtomicBool::new(true),
                #[cfg(test)]
                polling: AtomicBool::new(false),
                cleanup: Mutex::new(StartupCleanup::default()),
                ownership,
                delivery,
            }));
            let initialized = shared.as_ref().ok_or(DemandError::InvalidRequest)?;
            initialized.ownership.retain(control_bytes, work)?;
            control_transferred = true;
            work.started_io(READ)?;
            // SAFETY: plain descriptor creation with valid flags.
            inotify = Some(owned(unsafe {
                libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC)
            })?);
            work.started_io(READ)?;
            // SAFETY: as above.
            wake = Some(owned(unsafe {
                libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC)
            })?);
            work.started_io(READ)?;
            // SAFETY: duplicating a live descriptor the root holds.
            root = Some(owned(unsafe {
                libc::fcntl(directory.directory_fd(), libc::F_DUPFD_CLOEXEC, 0)
            })?);
            let held_inotify = inotify.as_ref().ok_or(DemandError::InvalidRequest)?;
            let held_root = root.as_ref().ok_or(DemandError::InvalidRequest)?;
            watch_ancestors(
                held_inotify,
                held_root,
                &mut ancestors,
                &mut retained,
                &mut cleanup,
                work,
            )?;
            let bytes = READ_BUFFER_BYTES as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.claim_allocation(
                bytes,
                u64::from((bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            retained = next_retained;
            buffer = vec![0; READ_BUFFER_BYTES];
            cleanup.extend(ITEM, work)?;
            let bytes = std::mem::size_of::<(PathBuf, i32)>() as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.claim_allocation(
                bytes,
                u64::from((bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            retained = next_retained;
            watches = Vec::with_capacity(1);
            let (root_path, _) = proc_fd_path(held_root.as_raw_fd());
            work.started_io(READ)?;
            // SAFETY: this stack C path names the held root and lives through registration, exactly as for every later admitted directory.
            let descriptor = unsafe {
                libc::inotify_add_watch(
                    held_inotify.as_raw_fd(),
                    root_path.as_ptr().cast(),
                    WATCHED,
                )
            };
            if descriptor < 0 {
                return Err(io::Error::last_os_error().into());
            }
            watches.push((PathBuf::new(), descriptor));
            let initialized = shared.as_ref().ok_or(DemandError::InvalidRequest)?;
            initialized.inotify.store(
                inotify
                    .take()
                    .ok_or(DemandError::InvalidRequest)?
                    .into_raw_fd(),
                Ordering::Release,
            );
            initialized.wake.store(
                wake.take()
                    .ok_or(DemandError::InvalidRequest)?
                    .into_raw_fd(),
                Ordering::Release,
            );
            initialized.root.store(
                root.take()
                    .ok_or(DemandError::InvalidRequest)?
                    .into_raw_fd(),
                Ordering::Release,
            );
            *initialized
                .ancestors
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut ancestors);
            initialized
                .reading
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .buffer = std::mem::take(&mut buffer);
            *initialized
                .watches
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut watches);
            initialized
                .retained_variable_bytes
                .store(retained - control_bytes, Ordering::Release);
            initialized
                .ownership
                .retain(retained - control_bytes, work)?;
            transferred = true;
            *initialized
                .cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut cleanup);
            initialized
                .cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .reserve_worker(work)?;
            let initialized = shared.as_ref().ok_or(DemandError::InvalidRequest)?;
            let _startup_reading = initialized
                .reading
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            work.started_io(WorkCounters {
                backend_read_operations: 1,
                allocation_operations: 1,
                ..WorkCounters::default()
            })?;
            let reader = Arc::into_raw(Arc::clone(
                shared.as_ref().ok_or(DemandError::InvalidRequest)?,
            ));
            let mut thread = std::mem::MaybeUninit::<libc::pthread_t>::uninit();
            // SAFETY: pthread_create writes the thread ID on success, and the raw Arc payload is consumed either by run_reader or by failure.
            let status = unsafe {
                libc::pthread_create(
                    thread.as_mut_ptr(),
                    std::ptr::null(),
                    run_reader,
                    reader.cast_mut().cast(),
                )
            };
            if status != 0 {
                // SAFETY: no thread was created, so startup retains this Arc.
                drop(unsafe { Arc::from_raw(reader) });
                return Err(io::Error::from_raw_os_error(status).into());
            }
            let cancelled = work.check().err();
            if cancelled.is_some() {
                initialized.stopping.store(true, Ordering::Release);
            }
            // SAFETY: successful pthread_create initialized the ID.
            Ok((unsafe { thread.assume_init() }, cancelled))
        })();
        match result {
            Ok((thread, cancelled)) => {
                let watch = Self {
                    shared: shared.ok_or(DemandError::InvalidRequest)?,
                    thread: Mutex::new(Some(thread)),
                };
                if let Some(error) = cancelled {
                    let stopped = watch.stop_admitted(
                        &mut watch.thread.lock().unwrap_or_else(PoisonError::into_inner),
                        work,
                    );
                    if let Err(stop_error) = stopped {
                        if !watch.is_stopped() {
                            if let Some(failure) = &startup_failure {
                                return Err(failure.retain_failed_platform(stop_error, watch));
                            }
                            work.release(retained)?;
                            return Err(stop_error);
                        }
                        if !work.is_native_original() {
                            work.release(retained)?;
                        }
                        return Err(stop_error);
                    }
                    if !work.is_native_original() {
                        work.release(retained)?;
                    }
                    return Err(error);
                }
                if !work.is_native_original() {
                    watch
                        .shared
                        .cleanup
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .cancel(work);
                }
                if !work.is_native_original() {
                    work.release(retained)?;
                }
                Ok(watch)
            }
            Err(error) => {
                let mut cleaned = Ok(());
                if let Some(shared) = shared {
                    cleaned = cleaned.and(shared.clear_measured(work));
                    shared.cancel_cleanup(work);
                }
                cleaned = cleaned.and(cleanup.record(clear_records(&mut ancestors), work));
                cleaned = cleaned.and(cleanup.record(clear_records(&mut watches), work));
                drop(std::mem::take(&mut buffer));
                for descriptor in [&mut inotify, &mut wake, &mut root] {
                    if let Some(descriptor) = descriptor.take() {
                        let closed = close(descriptor.into_raw_fd());
                        cleaned = cleaned.and(cleanup.record(WRITE, work).and(closed));
                    }
                }
                cleanup.cancel(work);
                let released_bytes = if work.is_native_original() && control_transferred {
                    if transferred {
                        0
                    } else {
                        retained - control_bytes
                    }
                } else {
                    retained
                };
                cleaned = cleaned.and(work.release(released_bytes));
                cleaned?;
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn admit(&self, directory: &Path) -> bool {
        let cancellation = crate::cancellation::CancellationToken::new();
        self.admit_with_work(
            directory,
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .unwrap_or(false)
    }

    pub(crate) fn admit_with_work<W: SourceDemandReadWork>(
        &self,
        directory: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        // Registration buffers and cleanup credits outlive the read which
        // discovers this directory. Keep them on the watch's birth ledger.
        match self.shared.ownership.original_work() {
            Some(original) => self
                .shared
                .admit_with_work(directory, &mut original.clone()),
            None => self.shared.admit_with_work(directory, work),
        }
    }

    pub(crate) fn unadmit_with_work<W: SourceDemandReadWork>(
        &self,
        directory: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.unadmit_with_work(directory, work)
    }

    #[cfg(test)]
    pub(crate) fn fence(&self) -> io::Result<()> {
        let mut work = self.shared.delivery.callback_work();
        self.shared
            .read_events_and_deliver(std::time::Duration::MAX, &mut work)
            .map(|_| ())
            .map_err(|error| {
                self.shared.delivery.exact.store(false, Ordering::Release);
                crate::native_host::ordinary_error(error)
            })
    }

    pub(crate) fn fence_events_with_work<W: SourceDemandReadWork>(
        &self,
        timeout: std::time::Duration,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        self.shared.read_events_and_deliver(timeout, work)
    }

    pub(crate) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }
    pub(crate) fn platform_owned_bytes(&self) -> u64 {
        arc_control_bytes::<Shared>() + self.shared.retained_variable_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.shared.completed.load(Ordering::Acquire)
    }

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.stop(work, None, false)
    }

    pub(crate) fn stop_with_cleanup_wait(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        let original = self
            .shared
            .delivery
            .original
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        if wait.is_cancelled() {
            return Err(DemandError::Cancelled);
        }
        self.stop(
            &mut crate::NativeMaterializationWork::clone(original),
            Some(wait),
            true,
        )?;
        if wait.is_cancelled() {
            return Err(DemandError::Cancelled);
        }
        Ok(())
    }

    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.shared.delivery.ensure_stop_available()?;
        let original = self
            .shared
            .delivery
            .original
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        self.stop(
            &mut crate::NativeMaterializationWork::clone(original),
            None,
            true,
        )
    }

    pub(super) fn stop<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
        wait: Option<&crate::CancellationToken>,
        prepaid: bool,
    ) -> Result<(), DemandError> {
        self.shared.delivery.ensure_stop_available()?;
        if !prepaid {
            work.check()?;
        }
        let mut thread = stop_custody(&self.thread, wait)?;
        if wait.is_some_and(crate::CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        if self.is_stopped() {
            if prepaid {
                let cleanup = stop_custody(&self.shared.cleanup, wait)?;
                if cleanup.reads < u64::from(thread.is_some()) {
                    return Err(DemandError::NativeWorkUnavailable);
                }
                drop(cleanup);
                if wait.is_some_and(crate::CancellationToken::is_cancelled) {
                    return Err(DemandError::Cancelled);
                }
            }
            let result = self.join_thread(&mut thread, work);
            if thread.is_none() {
                self.shared.cancel_cleanup(work);
            }
            return result;
        }
        let reading = stop_custody(&self.shared.reading, wait)?;
        let watches = stop_custody(&self.shared.watches, wait)?;
        let ancestors = stop_custody(&self.shared.ancestors, wait)?;
        let records = reading
            .cleanup
            .len()
            .checked_add(watches.len())
            .and_then(|count| count.checked_add(ancestors.len()))
            .ok_or(WorkError::Overflow)?;
        let worker = u64::from(thread.is_some());
        let descriptors = [&self.shared.inotify, &self.shared.wake, &self.shared.root]
            .into_iter()
            .filter(|descriptor| descriptor.load(Ordering::Acquire) >= 0)
            .count() as u64;
        let suffix = WorkCounters {
            backend_read_operations: worker,
            backend_write_operations: descriptors
                + if prepaid {
                    u64::from(thread.is_some() && self.shared.wake.load(Ordering::Acquire) >= 0)
                } else {
                    worker
                },
            items_examined: u64::try_from(records).map_err(|_| WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        if prepaid {
            if !stop_custody(&self.shared.cleanup, wait)?.covers(suffix) {
                return Err(DemandError::NativeWorkUnavailable);
            }
        } else if self.shared.delivery.birth_matches(work) {
            self.shared
                .cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_for(suffix, work)?;
        } else {
            work.admit(suffix)?;
        }
        if !prepaid {
            work.check()?;
        }
        if wait.is_some_and(crate::CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        drop(ancestors);
        drop(watches);
        drop(reading);
        self.stop_admitted(&mut thread, work)
    }
    pub(super) fn stop_admitted<W: SourceDemandReadWork>(
        &self,
        thread: &mut Option<libc::pthread_t>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        let mut result = Ok(());
        if thread.is_some() {
            let reading = self
                .shared
                .reading
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let descriptor = self.shared.wake.load(Ordering::Acquire);
            let wake_error = if descriptor >= 0 {
                self.shared.ensure_cleanup_covered(WRITE, work)?;
                let one = 1_u64.to_ne_bytes();
                // SAFETY: the existing read custody lock excludes terminal descriptor retirement throughout this actual wake attempt.
                let wake = unsafe { libc::write(descriptor, one.as_ptr().cast(), one.len()) };
                let error = (wake < 0).then(io::Error::last_os_error);
                result = self.shared.record_cleanup(WRITE, work);
                error
            } else {
                None
            };
            drop(reading);
            if let Some(error) = wake_error {
                return result.and(Err(error.into()));
            }
            let joined = self.join_thread(thread, work);
            result = result.and(joined);
            if thread.is_some() {
                return result;
            }
        }
        if self.shared.delivery.original.is_none() || !self.shared.completed.load(Ordering::Acquire)
        {
            result = result.and(self.shared.clear_measured(work));
            self.shared.delivery.exact.store(false, Ordering::Release);
            self.shared
                .completed
                .store(self.shared.physically_cleared(), Ordering::Release);
        }
        if self.is_stopped() {
            self.shared.cancel_cleanup(work);
        }
        result
    }

    pub(super) fn join_thread<W: SourceDemandReadWork>(
        &self,
        thread: &mut Option<libc::pthread_t>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        if let Some(worker) = *thread {
            self.shared.ensure_cleanup_covered(READ, work)?;
            // SAFETY: this controller owns the one joinable worker returned by pthread_create.
            let joined = unsafe { libc::pthread_join(worker, std::ptr::null_mut()) };
            result = self.shared.record_cleanup(READ, work);
            if joined != 0 {
                return result.and(Err(io::Error::from_raw_os_error(joined).into()));
            }
            *thread = None;
        }
        result
    }
    #[cfg(test)]
    pub(crate) fn paused(&self) -> impl Sized + '_ {
        self.shared
            .reading
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if self.shared.delivery.original.is_some() {
            if self.shared.delivery.ensure_stop_available().is_err() {
                self.shared.controller_alive.store(false, Ordering::Release);
                self.shared.stopping.store(true, Ordering::Release);
                self.shared.reading_changed.notify_all();
                return;
            }
            let _ = self.stop_accepted_cleanup();
            let completed = {
                let _cleanup = self
                    .shared
                    .cleanup
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                self.shared.controller_alive.store(false, Ordering::Release);
                self.is_stopped()
            };
            if completed {
                let mut work = self.shared.delivery.callback_work();
                let thread = self
                    .thread
                    .get_mut()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(worker) = *thread {
                    if let Err(error) = self.shared.detach_terminal_thread(worker, &mut work) {
                        self.shared.delivery.failed_with_work(error, &mut work);
                    }
                    *thread = None;
                }
                self.shared.cancel_cleanup(&mut work);
            }
            return;
        }
        if self.is_stopped() {
            return;
        }
        let cancellation = crate::cancellation::CancellationToken::new();
        let _ = self.stop_with_work(&mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation));
    }
}

impl Shared {
    pub(super) fn finish_native_reader(&self) {
        self.stopping.store(true, Ordering::Release);
        let mut work = self.delivery.callback_work();
        let result = self.clear_measured(&mut work);
        if let Err(error) = result {
            self.delivery.failed_with_work(error, &mut work);
        }
        self.delivery.exact.store(false, Ordering::Release);
        if !self.physically_cleared() {
            self.reading_changed.notify_all();
            return;
        }
        let detach = {
            let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            let keep = u64::from(self.controller_alive.load(Ordering::Acquire));
            let reads = cleanup.reads.min(keep);
            let writes = cleanup.writes.min(1);
            let unused = WorkCounters {
                backend_read_operations: cleanup.reads - reads,
                backend_write_operations: cleanup.writes - writes,
                items_examined: cleanup.records,
                ..WorkCounters::UNCHARGED
            };
            work.with_original_leaf(|work| super::cancel_cleanup_credits(work, unused));
            cleanup.reads = reads;
            cleanup.writes = writes;
            cleanup.records = 0;
            self.completed.store(true, Ordering::Release);
            keep == 0
        };
        if detach {
            // SAFETY: the actual reader has retired its native payload. With no controller left, detachment releases only pthread bookkeeping.
            let worker = unsafe { libc::pthread_self() };
            if let Err(error) = self.detach_terminal_thread(worker, &mut work) {
                self.delivery.failed_with_work(error, &mut work);
            }
            self.cancel_cleanup(&mut work);
        }
        self.reading_changed.notify_all();
    }

    pub(super) fn detach_terminal_thread<W: SourceDemandReadWork>(
        &self,
        worker: libc::pthread_t,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.ensure_cleanup_covered(WRITE, work)?;
        // SAFETY: cleanup's controller-alive decision selected this sole terminal bookkeeping owner; no live controller can join this thread.
        let detached = unsafe { libc::pthread_detach(worker) };
        let recorded = self.record_cleanup(WRITE, work);
        if detached != 0 {
            return Err(io::Error::from_raw_os_error(detached).into());
        }
        recorded
    }

    pub(super) fn physically_cleared(&self) -> bool {
        self.retained_variable_bytes.load(Ordering::Acquire) == 0
            && [&self.inotify, &self.wake, &self.root]
                .into_iter()
                .all(|descriptor| descriptor.load(Ordering::Acquire) < 0)
    }
}
