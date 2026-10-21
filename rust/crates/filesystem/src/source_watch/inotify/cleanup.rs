use super::*;

impl StartupCleanup {
    pub(super) fn reserve<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
        let mut cleanup = Self::default();
        // Withdrawing the hub registrations and closing the held root.
        cleanup.extend(
            WorkCounters {
                backend_write_operations: 2,
                ..WorkCounters::UNCHARGED
            },
            work,
        )?;
        Ok(cleanup)
    }

    pub(super) fn extend<W: SourceDemandReadWork>(
        &mut self,
        delta: WorkCounters,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        let reads = self
            .reads
            .checked_add(delta.backend_read_operations)
            .ok_or(WorkError::Overflow)?;
        let writes = self
            .writes
            .checked_add(delta.backend_write_operations)
            .ok_or(WorkError::Overflow)?;
        let records = self
            .records
            .checked_add(delta.items_examined)
            .ok_or(WorkError::Overflow)?;
        work.with_original_leaf(|work| super::reserve_cleanup_credits(work, delta))?;
        self.reads = reads;
        self.writes = writes;
        self.records = records;
        Ok(())
    }

    pub(super) fn reserve_worker<W: SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let attempts = if work.is_native_original() { 2 } else { 1 };
        self.extend(
            WorkCounters {
                backend_read_operations: attempts,
                backend_write_operations: attempts,
                ..WorkCounters::UNCHARGED
            },
            work,
        )
    }

    pub(super) fn record<W: SourceDemandReadWork>(
        &mut self,
        delta: WorkCounters,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if delta == WorkCounters::UNCHARGED {
            return Ok(());
        }
        let reads = self
            .reads
            .checked_sub(delta.backend_read_operations)
            .ok_or(DemandError::NativeWorkUnavailable)?;
        let writes = self
            .writes
            .checked_sub(delta.backend_write_operations)
            .ok_or(DemandError::NativeWorkUnavailable)?;
        let records = self
            .records
            .checked_sub(delta.items_examined)
            .ok_or(DemandError::NativeWorkUnavailable)?;
        self.reads = reads;
        self.writes = writes;
        self.records = records;
        work.with_original_leaf(|work| super::record_cleanup_credits(work, delta))
    }

    pub(super) fn cancel<W: SourceDemandReadWork>(&mut self, work: &mut W) {
        work.with_original_leaf(|work| {
            super::cancel_cleanup_credits(
                work,
                WorkCounters {
                    backend_read_operations: self.reads,
                    backend_write_operations: self.writes,
                    items_examined: self.records,
                    ..WorkCounters::UNCHARGED
                },
            );
        });
        *self = Self::default();
    }

    pub(super) fn covers(&self, delta: WorkCounters) -> bool {
        self.reads >= delta.backend_read_operations
            && self.writes >= delta.backend_write_operations
            && self.records >= delta.items_examined
    }

    pub(super) fn extend_for<W: SourceDemandReadWork>(
        &mut self,
        delta: WorkCounters,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let missing = WorkCounters {
            backend_read_operations: delta.backend_read_operations.saturating_sub(self.reads),
            backend_write_operations: delta.backend_write_operations.saturating_sub(self.writes),
            items_examined: delta.items_examined.saturating_sub(self.records),
            ..WorkCounters::UNCHARGED
        };
        if missing != WorkCounters::UNCHARGED {
            self.extend(missing, work)?;
        }
        Ok(())
    }
}

pub(super) fn stop_custody<'a, T>(
    mutex: &'a Mutex<T>,
    wait: Option<&crate::CancellationToken>,
) -> Result<std::sync::MutexGuard<'a, T>, DemandError> {
    if let Some(wait) = wait {
        if wait.is_cancelled() {
            return Err(DemandError::Cancelled);
        }
        match mutex.try_lock() {
            Ok(guard) => Ok(guard),
            Err(std::sync::TryLockError::Poisoned(error)) => Ok(error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => Err(DemandError::WorkerUnavailable),
        }
    } else {
        Ok(mutex.lock().unwrap_or_else(PoisonError::into_inner))
    }
}
pub(super) fn close(descriptor: i32) -> Result<(), DemandError> {
    // SAFETY: exclusive custody was removed from its owner after quiescence.
    let status = unsafe { libc::close(descriptor) };
    let error = (status < 0).then(io::Error::last_os_error);
    match error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

pub(super) fn close_component<W: SourceDemandReadWork>(
    (directory, reservation): (OwnedFd, u64),
    work: &mut W,
) -> Result<(), DemandError> {
    let descriptor = directory.into_raw_fd();
    let (error, recorded) = work.attempt_cleanup_write(reservation, || {
        // SAFETY: the consumed temporary directory owns this descriptor.
        (unsafe { libc::close(descriptor) } < 0).then(io::Error::last_os_error)
    });
    recorded?;
    match error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

pub(super) fn clear_records<T>(records: &mut Vec<T>) -> WorkCounters {
    let count = records.len() as u64;
    *records = Vec::new();
    WorkCounters {
        items_examined: count,
        ..WorkCounters::UNCHARGED
    }
}

impl Shared {
    pub(super) fn record_cleanup<W: SourceDemandReadWork>(
        &self,
        delta: WorkCounters,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if delta == WorkCounters::UNCHARGED {
            return Ok(());
        }
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if self.delivery.birth_matches(work)
            || (self.delivery.original.is_none() && cleanup.covers(delta))
        {
            cleanup.record(delta, work)
        } else {
            work.record(delta)
        }
    }

    pub(super) fn cancel_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(birth) = &self.delivery.original {
            cleanup.cancel(&mut crate::NativeMaterializationWork::clone(birth));
        } else {
            cleanup.cancel(work);
        }
    }

    pub(super) fn ensure_cleanup_covered<W: SourceDemandReadWork>(
        &self,
        delta: WorkCounters,
        work: &W,
    ) -> Result<(), DemandError> {
        if self.delivery.birth_matches(work)
            && !self
                .cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .covers(delta)
        {
            return Err(DemandError::NativeWorkUnavailable);
        }
        Ok(())
    }

    pub(super) fn clear_measured<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        let mut ancestors = self
            .ancestors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let records = reading
            .cleanup
            .len()
            .checked_add(watches.len())
            .and_then(|count| count.checked_add(ancestors.len()))
            .ok_or(WorkError::Overflow)?;
        self.ensure_cleanup_covered(
            WorkCounters {
                backend_write_operations: self.held_descriptors(),
                items_examined: u64::try_from(records).map_err(|_| WorkError::Overflow)?,
                ..WorkCounters::UNCHARGED
            },
            work,
        )?;
        let retired_bytes = self.retained_variable_bytes.load(Ordering::Acquire);
        let mut result = Ok(());
        if self.inotify.swap(-1, Ordering::AcqRel) >= 0 {
            let withdrawn = hub::hub().and_then(|hub| {
                hub.withdraw(self.subscriber, &watches, &ancestors)
                    .map_err(Into::into)
            });
            result = result.and(self.record_cleanup(WRITE, work).and(withdrawn));
        }
        let descriptor = self.root.swap(-1, Ordering::AcqRel);
        if descriptor >= 0 {
            let closed = close(descriptor);
            result = result.and(self.record_cleanup(WRITE, work).and(closed));
        }
        result = result.and(self.record_cleanup(clear_records(&mut ancestors), work));
        result = result.and(self.record_cleanup(clear_records(&mut watches), work));
        result = result.and(self.record_cleanup(clear_records(&mut reading.cleanup), work));
        reading.buffer = Vec::new();
        reading.filled = 0;
        reading.cursor = 0;
        reading.alias_cursor = 0;
        reading.blocked = true;
        result = result.and(self.ownership.retire(retired_bytes));
        self.retained_variable_bytes.store(0, Ordering::Release);
        result
    }
}
