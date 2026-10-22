use super::*;

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
        let mut subscribed = false;
        let mut root = None;
        let mut ancestors = Vec::new();
        let mut watches = Vec::new();
        let mut shared: Option<Arc<Shared>> = None;
        let mut retained = 0_u64;
        let mut transferred = false;
        let control_bytes = arc_control_bytes::<Shared>();
        let mut control_transferred = false;
        let result = (|| {
            work.admit(READ)?;
            let hub = hub::hub()?;
            work.claim_allocation(
                control_bytes,
                u64::from((control_bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            retained = control_bytes;
            shared = Some(Arc::new_cyclic(|this| Shared {
                inotify: AtomicI32::new(-1),
                root: AtomicI32::new(-1),
                subscriber: hub.next_subscriber(),
                this: this.clone(),
                inbox: Mutex::new(hub::Inbox::default()),
                retired: Mutex::new(false),
                retiring: AtomicBool::new(false),
                ancestors: Mutex::new(Vec::new()),
                retained_variable_bytes: AtomicU64::new(0),
                reading: Reading::default(),
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
                #[cfg(all(test, feature = "native-mount"))]
                drop_complete: Mutex::new(None),
            }));
            let initialized = shared.as_ref().ok_or(DemandError::InvalidRequest)?;
            initialized.ownership.retain(control_bytes, work)?;
            control_transferred = true;
            // Background delivery waits for this guard: no routed record is
            // consumed before this watch's registrations are recorded.
            let startup_reading = initialized.reading();
            work.started_io(READ)?;
            hub.subscribe(initialized)?;
            subscribed = true;
            work.started_io(READ)?;
            // SAFETY: duplicating a live descriptor the root holds.
            root = Some(owned(unsafe {
                libc::fcntl(directory.directory_fd(), libc::F_DUPFD_CLOEXEC, 0)
            })?);
            let held_root = root.as_ref().ok_or(DemandError::InvalidRequest)?;
            watch_ancestors(
                initialized,
                held_root,
                &mut ancestors,
                &mut retained,
                &mut cleanup,
                work,
            )?;
            // The read buffer and the inbox are reserved here and allocated
            // only while records are pending.
            let bytes = (READ_BUFFER_BYTES + hub::INBOX_BYTES) as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.claim_allocation(bytes, 0, crate::WorkCounters::UNCHARGED)?;
            retained = next_retained;
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
            let (root_path, length) = proc_fd_path(held_root.as_raw_fd());
            let root_path = root_path
                .get(..=length)
                .and_then(|path| std::ffi::CStr::from_bytes_with_nul(path).ok())
                .ok_or(DemandError::InvalidRequest)?;
            work.started_io(READ)?;
            let descriptor = hub.add(initialized, root_path, WATCHED, hub::Interest::All)?;
            watches.push((PathBuf::new(), descriptor));
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
            *initialized
                .watches
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut watches);
            // The watch's registrations now withdraw with its other payload.
            initialized
                .inotify
                .store(hub.raw_descriptor(), Ordering::Release);
            subscribed = false;
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
            work.started_io(READ)?;
            #[cfg(test)]
            initialized.polling.store(true, Ordering::Release);
            drop(startup_reading);
            let cancelled = work.check().err();
            if cancelled.is_some() {
                initialized.stopping.store(true, Ordering::Release);
            }
            Ok((Worker, cancelled))
        })();
        match result {
            Ok((worker, cancelled)) => {
                let watch = Self {
                    shared: shared.ok_or(DemandError::InvalidRequest)?,
                    worker: Mutex::new(Some(worker)),
                };
                if let Some(error) = cancelled {
                    let stopped = watch.stop_admitted(
                        &mut watch.worker.lock().unwrap_or_else(PoisonError::into_inner),
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
                if let Some(shared) = &shared {
                    shared.stopping.store(true, Ordering::Release);
                    *shared
                        .retired
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = true;
                    shared.retiring.store(true, Ordering::Release);
                    cleaned = cleaned.and(shared.clear_measured(work));
                    shared.cancel_cleanup(work);
                }
                if let (true, Some(shared)) = (subscribed, &shared) {
                    let withdrawn = hub::hub().and_then(|hub| {
                        hub.withdraw(shared.subscriber, &watches, &ancestors)
                            .map_err(Into::into)
                    });
                    cleaned = cleaned.and(cleanup.record(WRITE, work).and(withdrawn));
                }
                cleaned = cleaned.and(cleanup.record(clear_records(&mut ancestors), work));
                cleaned = cleaned.and(cleanup.record(clear_records(&mut watches), work));
                if let Some(descriptor) = root.take() {
                    let closed = close(OwnedFd::into_raw_fd(descriptor));
                    cleaned = cleaned.and(cleanup.record(WRITE, work).and(closed));
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

    #[cfg(test)]
    pub(crate) fn is_admitted(&self, directory: &Path) -> bool {
        self.shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == directory)
    }

    pub(crate) fn admit_with_work<W: SourceDemandReadWork>(
        &self,
        directory: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        self.shared.admit_with_work(directory, work)
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
        let mut worker = stop_custody(&self.worker, wait)?;
        if wait.is_some_and(crate::CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        if self.is_stopped() {
            if prepaid {
                let cleanup = stop_custody(&self.shared.cleanup, wait)?;
                if cleanup.reads < u64::from(worker.is_some()) {
                    return Err(DemandError::NativeWorkUnavailable);
                }
                drop(cleanup);
                if wait.is_some_and(crate::CancellationToken::is_cancelled) {
                    return Err(DemandError::Cancelled);
                }
            }
            let result = self.join_worker(&mut worker, work);
            if worker.is_none() {
                self.shared.cancel_cleanup(work);
            }
            return result;
        }
        let reading = match wait {
            Some(wait) if wait.is_cancelled() => return Err(DemandError::Cancelled),
            Some(_) => self
                .shared
                .try_reading()
                .ok_or(DemandError::WorkerUnavailable)?,
            None => self.shared.reading(),
        };
        let watches = stop_custody(&self.shared.watches, wait)?;
        let ancestors = stop_custody(&self.shared.ancestors, wait)?;
        let records = reading
            .cleanup
            .len()
            .checked_add(watches.len())
            .and_then(|count| count.checked_add(ancestors.len()))
            .ok_or(WorkError::Overflow)?;
        let running = u64::from(worker.is_some());
        let registered = self.shared.inotify.load(Ordering::Acquire) >= 0;
        let suffix = WorkCounters {
            backend_read_operations: running,
            backend_write_operations: self.shared.held_descriptors()
                + if prepaid {
                    u64::from(worker.is_some() && registered)
                } else {
                    running
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
        self.stop_admitted(&mut worker, work)
    }

    pub(super) fn stop_admitted<W: SourceDemandReadWork>(
        &self,
        worker: &mut Option<Worker>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        let mut result = Ok(());
        if worker.is_some() {
            // A failed service pass may retire this watch concurrently, and
            // its withdrawal also unsubscribes and settles the cleanup
            // credits. Unsubscribe under the same retirement lock, so exactly
            // one of the two spends this write.
            let retired = self
                .shared
                .retired
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !*retired && self.shared.inotify.load(Ordering::Acquire) >= 0 {
                // Queue overflow records stop scheduling this watch.
                self.shared.ensure_cleanup_covered(WRITE, work)?;
                #[cfg(test)]
                super::admission_tests::before_unsubscribe_record();
                let unsubscribed = hub::hub().map(|hub| hub.unsubscribe(self.shared.subscriber));
                self.shared.record_cleanup(WRITE, work).and(unsubscribed)?;
            }
            drop(retired);
            let joined = self.join_worker(worker, work);
            result = result.and(joined);
            if worker.is_some() {
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

    /// Ends background delivery: returns once no service pass can deliver
    /// for this watch again and native custody, if any, was retired.
    pub(super) fn join_worker<W: SourceDemandReadWork>(
        &self,
        worker: &mut Option<Worker>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        if worker.is_some() {
            self.shared.ensure_cleanup_covered(READ, work)?;
            self.shared.retire();
            result = self.shared.record_cleanup(READ, work);
            *worker = None;
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn paused(&self) -> impl Sized + '_ {
        self.shared.reading()
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if self.shared.delivery.original.is_some() {
            if self.shared.delivery.ensure_stop_available().is_err() {
                self.shared.controller_alive.store(false, Ordering::Release);
                self.shared.stopping.store(true, Ordering::Release);
                self.shared.reading_changed.notify_all();
                // The next service pass retires native custody.
                self.shared.force_service();
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
                *self
                    .worker
                    .get_mut()
                    .unwrap_or_else(PoisonError::into_inner) = None;
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
    /// Ends background delivery once. Delivery runs under the reading lock
    /// and observes `stopping` or retirement first, so once this returns no
    /// service pass delivers for this watch again; a concurrent retirer waits
    /// for the first to finish.
    pub(super) fn retire(&self) {
        let mut retired = self.retired.lock().unwrap_or_else(PoisonError::into_inner);
        if *retired {
            return;
        }
        *retired = true;
        self.retiring.store(true, Ordering::Release);
        let mut reading = self.reading();
        if self.delivery.original.is_some() {
            self.finish_native_reader_locked(&mut reading);
        }
    }

    /// [`Self::retire`] from a service pass that holds the cursor, so no
    /// delivery is in flight. It never waits: a retirer already running
    /// waits for this cursor and completes retirement itself.
    pub(super) fn retire_in_service(&self, mut reading: ReadingGuard<'_>) {
        let mut retired = match self.retired.try_lock() {
            Ok(retired) => retired,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return,
        };
        if *retired {
            return;
        }
        *retired = true;
        self.retiring.store(true, Ordering::Release);
        if self.delivery.original.is_some() {
            self.finish_native_reader_locked(&mut reading);
        }
    }

    /// Schedules a service pass when routed records wait unscheduled.
    pub(super) fn reschedule(&self) {
        let due = self
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reschedule();
        if let (true, Some(watch), Ok(hub)) = (due, self.this.upgrade(), hub::hub()) {
            hub.schedule(watch);
        }
    }

    /// Schedules a service pass regardless of routed records.
    pub(super) fn force_service(&self) {
        if let (Some(watch), Ok(hub)) = (self.this.upgrade(), hub::hub()) {
            hub.schedule(watch);
        }
    }

    fn finish_native_reader_locked(&self, reading: &mut RawReads) {
        self.stopping.store(true, Ordering::Release);
        let mut work = self.delivery.callback_work();
        let result = self.clear_measured_locked(reading, &mut work);
        if let Err(error) = result {
            self.delivery.failed_with_work(error, &mut work);
        }
        self.delivery.exact.store(false, Ordering::Release);
        if !self.physically_cleared() {
            self.reading_changed.notify_all();
            return;
        }
        let orphaned = {
            let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            let keep = u64::from(self.controller_alive.load(Ordering::Acquire));
            let reads = cleanup.reads.min(keep);
            let unused = WorkCounters {
                backend_read_operations: cleanup.reads - reads,
                backend_write_operations: cleanup.writes,
                items_examined: cleanup.records,
                ..WorkCounters::UNCHARGED
            };
            work.with_original_leaf(|work| super::cancel_cleanup_credits(work, unused));
            cleanup.reads = reads;
            cleanup.writes = 0;
            cleanup.records = 0;
            self.completed.store(true, Ordering::Release);
            keep == 0
        };
        if orphaned {
            self.cancel_cleanup(&mut work);
        }
        self.reading_changed.notify_all();
    }

    /// Payload descriptors still held: the hub registrations and the root.
    pub(super) fn held_descriptors(&self) -> u64 {
        [&self.inotify, &self.root]
            .into_iter()
            .filter(|descriptor| descriptor.load(Ordering::Acquire) >= 0)
            .count() as u64
    }

    pub(super) fn physically_cleared(&self) -> bool {
        self.retained_variable_bytes.load(Ordering::Acquire) == 0 && self.held_descriptors() == 0
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Every stop path withdraws first; this keeps an abandoned watch from
        // leaving routes or kernel registrations in the process-wide hub.
        let Ok(hub) = hub::hub() else {
            return;
        };
        if *self.inotify.get_mut() >= 0 {
            let watches = self
                .watches
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner);
            let ancestors = self
                .ancestors
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner);
            let _ = hub.withdraw(self.subscriber, watches, ancestors);
        } else {
            hub.unsubscribe(self.subscriber);
        }
    }
}
