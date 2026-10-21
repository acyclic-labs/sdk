use super::*;

impl NativeSourceWatch {
    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.stop(work, false, None)
    }
    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.cleanup(None)
    }
    pub(crate) fn stop_with_cleanup_wait(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        self.cleanup(Some(wait))
    }
    fn cleanup(&self, wait: Option<&crate::CancellationToken>) -> Result<(), DemandError> {
        check_wait(wait)?;
        let mut work = self
            .shared
            .delivery
            .original
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?
            .clone();
        self.stop(&mut work, true, wait)
    }
    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    pub(super) fn stop<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
        retiring: bool,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), DemandError> {
        if !retiring {
            work.check()?;
        }
        self.shared.delivery.ensure_stop_available()?;
        check_wait(wait)?;
        if self.is_stopped() {
            return Ok(());
        }
        let native = self.shared.delivery.original.is_some();
        // A shared watch may be stopped through any live ledger, as on every other
        // platform: the native suffix is paid from the cleanup credit its birth reserved,
        // and native failures are recorded on the birth ledger.
        let birth = if native && !self.shared.delivery.birth_matches(work) {
            self.shared.delivery.original.clone()
        } else {
            None
        };
        let mut reads = lock(&self.shared.reads, wait)?;
        let mut thread = lock(&self.shared.thread, wait)?;
        check_wait(wait)?;
        if native && self.shared.stopping.load(Ordering::Acquire) {
            return Err(if self.shared.cleanup_pending() {
                DemandError::WorkerUnavailable
            } else {
                DemandError::NativeWorkUnavailable
            });
        }
        let running = thread.is_some();
        let wake = running && is_open(&self.shared.completed);
        let suffix = io_work(
            u64::from(reads.pending) + u64::from(running && !native),
            u64::from(reads.pending)
                + u64::from(wake)
                + u64::from(running)
                + u64::from(is_open(&self.shared.directory))
                + u64::from(is_open(&self.shared.completed)),
        );
        if native {
            let credit = self
                .shared
                .cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let [active, _] = &*credit;
            let remaining = active
                .as_ref()
                .ok_or(DemandError::NativeWorkUnavailable)?
                .remaining_work();
            suffix
                .remaining(remaining)
                .map_err(|_| DemandError::NativeWorkUnavailable)?;
        } else if !retiring {
            work.admit(suffix)?;
        }
        self.shared.delivery.exact.store(false, Ordering::Release);
        let mut result = if native {
            Ok(())
        } else {
            self.shared.cancel_pending(&mut reads, work)
        };
        if reads.pending && !native {
            return result;
        }
        self.shared.stopping.store(true, Ordering::Release);
        if wake {
            self.shared.cleanup_start(work, WRITE)?;
            // SAFETY: the read lock retains the event through the wake.
            let signalled =
                unsafe { SetEvent(HANDLE(self.shared.completed.load(Ordering::Acquire))) }
                    .map_err(io::Error::from)
                    .map_err(DemandError::from);
            if native {
                #[cfg(test)]
                if let Some(token) = self
                    .shared
                    .native_wait_cancellation
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                {
                    token.cancel();
                }
                if let Err(error) = signalled {
                    self.shared.note_failure(error);
                }
            } else {
                result = result.and(signalled);
            }
        }
        self.shared.reading_changed.notify_all();
        if native {
            drop(thread);
            if !running {
                drop(reads);
                self.shared.finish_native_reader();
                reads = self.shared.lock_reads();
            }
            while self.shared.cleanup_pending() {
                reads = self
                    .shared
                    .reading_changed
                    .wait(reads)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            let failure = match birth {
                Some(mut birth) => birth.with_original_leaf(DemandWork::take_native_io_failure),
                None => work.with_original_leaf(DemandWork::take_native_io_failure),
            };
            if !self.is_stopped() {
                return Err(failure.unwrap_or(DemandError::NativeWorkUnavailable));
            }
            drop(reads);
            return failure.map_or(Ok(()), Err).and(check_wait(wait));
        }
        drop(reads);
        if let Some(handle) = thread.as_ref() {
            self.shared.cleanup_start(work, READ)?;
            // SAFETY: the owned handle stays live through actual quiescence.
            if unsafe { WaitForSingleObject(HANDLE(handle.as_raw_handle()), INFINITE) }
                != WAIT_OBJECT_0
            {
                return result.and(Err(io::Error::last_os_error().into()));
            }
            result = result.and(self.shared.close_thread(&mut thread, work));
        }
        drop(thread);
        let mut reads = self.shared.lock_reads();
        result = result.and(self.shared.retire(&mut reads, work));
        if self.shared.retired(&reads) {
            self.shared.stop_completed.store(true, Ordering::Release);
        }
        result
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if self.is_stopped() {
            return;
        }
        if self.shared.delivery.original.is_none() {
            let mut work = self.shared.delivery.callback_work();
            if let Err(error) = self.stop(&mut work, true, None) {
                self.shared.delivery.failed_with_work(error, &mut work);
            }
            return;
        }
        self.shared.dropped.store(true, Ordering::Release);
        let already_stopping = self.shared.stopping.swap(true, Ordering::AcqRel);
        let reads = self
            .shared
            .delivery
            .ensure_stop_available()
            .is_ok()
            .then(|| self.shared.lock_reads());
        if !already_stopping && is_open(&self.shared.completed) {
            let mut work = self.shared.delivery.callback_work();
            let wake = self.shared.cleanup_start(&mut work, WRITE).and_then(|_| {
                // SAFETY: this controller or its active callback retains the event.
                unsafe { SetEvent(HANDLE(self.shared.completed.load(Ordering::Acquire))) }
                    .map_err(io::Error::from)
                    .map_err(DemandError::from)
            });
            if let Err(error) = wake {
                self.shared.note_failure(error);
            }
        }
        drop(reads);
        self.shared.reading_changed.notify_all();
    }
}

impl Shared {
    fn cleanup_pending(&self) -> bool {
        let cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        let [active, _] = &*cleanup;
        active.is_some()
    }
    fn cleanup_start<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
        delta: WorkCounters,
    ) -> Result<(), DemandError> {
        if self.delivery.original.is_none() {
            return work.record(delta);
        }
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        let credit = cleanup
            .iter_mut()
            .flatten()
            .next()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        // Each serialized cleanup attempt fits the suffix reserved before birth.
        credit.started_drop_io(delta);
        Ok(())
    }
    pub(super) fn cancel_pending<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if !reads.pending {
            return Ok(());
        }
        self.cleanup_start(work, WRITE)?;
        // SAFETY: the pending read owns both OVERLAPPED and buffer until completion.
        let cancelled = unsafe {
            CancelIoEx(
                HANDLE(self.directory.load(Ordering::Acquire)),
                Some(&raw const *reads.overlapped),
            )
        };
        let result = match cancelled {
            Err(error) if error.code() != ERROR_NOT_FOUND.to_hresult() => {
                Err(io::Error::from(error).into())
            }
            _ => Ok(()),
        };
        self.cleanup_start(work, READ)?;
        let mut transferred = 0;
        // SAFETY: the same pending storage stays pinned during the wait.
        let completed = unsafe {
            GetOverlappedResult(
                HANDLE(self.directory.load(Ordering::Acquire)),
                &raw const *reads.overlapped,
                &mut transferred,
                true,
            )
        };
        reads.pending = !matches!(&completed, Ok(()))
            && !completed.as_ref().err().is_some_and(|error| {
                error.code() == ERROR_OPERATION_ABORTED.to_hresult()
                    || error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult()
            });
        result.and(match completed {
            Err(error) if error.code() != ERROR_OPERATION_ABORTED.to_hresult() => {
                Err(io::Error::from(error).into())
            }
            _ => Ok(()),
        })
    }
    fn close_thread<W: SourceDemandReadWork>(
        &self,
        thread: &mut Option<OwnedHandle>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if let Some(handle) = thread.as_ref() {
            self.cleanup_start(work, WRITE)?;
            // SAFETY: failed close keeps this exact owned handle for retry.
            unsafe { CloseHandle(HANDLE(handle.as_raw_handle())) }.map_err(io::Error::from)?;
            let _ = thread
                .take()
                .ok_or(DemandError::NativeWorkUnavailable)?
                .into_raw_handle();
        }
        Ok(())
    }
    fn retire<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = self.cancel_pending(reads, work);
        if reads.pending {
            return result;
        }
        for handle in [&self.directory, &self.completed] {
            if is_open(handle) {
                result = result.and(self.cleanup_start(work, WRITE).and_then(|_| close(handle)));
            }
        }
        if !is_open(&self.directory) && !is_open(&self.completed) {
            if let Some(buffer) = reads.buffer.take() {
                drop(buffer);
                result = result.and(self.ownership.retire(BUFFER_BYTES));
            }
            self.buffer_owned.store(false, Ordering::Release);
            reads.filled = 0;
            reads.cursor = 0;
            if self.delivery.original.is_some() {
                result = result.and(self.close_thread(
                    &mut self.thread.lock().unwrap_or_else(PoisonError::into_inner),
                    work,
                ));
            }
        }
        result
    }
    pub(super) fn retired(&self, reads: &Reads) -> bool {
        !reads.pending
            && reads.buffer.is_none()
            && !is_open(&self.directory)
            && !is_open(&self.completed)
            && self
                .thread
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none()
    }
    pub(super) fn finish_native_reader(&self) {
        self.stopping.store(true, Ordering::Release);
        self.delivery.exact.store(false, Ordering::Release);
        let mut work = self.delivery.callback_work();
        loop {
            let mut reads = self.lock_reads();
            let terminal = !self.cleanup_pending();
            if let Err(error) = self.retire(&mut reads, &mut work) {
                self.note_failure(error);
            }
            let retired = self.retired(&reads);
            let credit = {
                let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
                if retired {
                    std::mem::take(&mut *cleanup)
                } else {
                    let [active, _] = &mut *cleanup;
                    [active.take(), None]
                }
            };
            drop(credit);
            if retired {
                self.stop_completed.store(true, Ordering::Release);
            }
            self.reading_changed.notify_all();
            if retired {
                return;
            }
            while terminal || !self.dropped.load(Ordering::Acquire) {
                reads = self
                    .reading_changed
                    .wait(reads)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
}
fn check_wait(wait: Option<&crate::CancellationToken>) -> Result<(), DemandError> {
    wait.map_or(Ok(()), |wait| {
        wait.check().map_err(|_| DemandError::Cancelled)
    })
}
fn lock<'a, T>(
    mutex: &'a Mutex<T>,
    wait: Option<&crate::CancellationToken>,
) -> Result<MutexGuard<'a, T>, DemandError> {
    if wait.is_none() {
        return Ok(mutex.lock().unwrap_or_else(PoisonError::into_inner));
    }
    match mutex.try_lock() {
        Ok(guard) => Ok(guard),
        Err(std::sync::TryLockError::Poisoned(error)) => Ok(error.into_inner()),
        Err(std::sync::TryLockError::WouldBlock) => Err(DemandError::WorkerUnavailable),
    }
}
