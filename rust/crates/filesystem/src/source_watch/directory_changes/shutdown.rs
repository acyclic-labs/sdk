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
        // A native owner already stopping answers without the reads, which
        // the hub may hold while it serves a stale packet: stopping only
        // ever turns on, and the credits have their own lock.
        self.shared.refuse_restop(native)?;
        let mut reads = self.shared.lock_reads_unless_waiting(wait)?;
        check_wait(wait)?;
        self.shared.refuse_restop(native)?;
        let suffix = io_work(
            u64::from(reads.pending),
            u64::from(reads.pending) + u64::from(is_open(&self.shared.directory)),
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
        if native {
            // The born owner retires here, on its active credit: the hub
            // never waits on a stop, and a stop never waits on the hub.
            self.shared.stopping.store(true, Ordering::Release);
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
            self.shared.retire_native(&mut reads);
            let failure = match birth {
                Some(birth) => birth.with_original_leaf(DemandWork::take_native_io_failure),
                None => work.with_original_leaf(DemandWork::take_native_io_failure),
            };
            if !self.is_stopped() {
                return Err(failure.unwrap_or(DemandError::NativeWorkUnavailable));
            }
            drop(reads);
            return failure.map_or(Ok(()), Err).and(check_wait(wait));
        }
        let mut result = self.shared.cancel_pending(&mut reads, work);
        if reads.pending {
            // Only the hub cannot wait for the cancelled read; its packet
            // finishes the retirement.
            if hub::serving() && result.is_ok() {
                self.shared.stopping.store(true, Ordering::Release);
            }
            return result;
        }
        // Set under the reads, so no later packet drains them.
        self.shared.stopping.store(true, Ordering::Release);
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
        // A dropped native owner retires on the hub, which this wake hands
        // it to.
        self.shared.dropped.store(true, Ordering::Release);
        self.shared.stopping.store(true, Ordering::Release);
        let reads = self
            .shared
            .delivery
            .ensure_stop_available()
            .is_ok()
            .then(|| self.shared.lock_reads());
        if !self.shared.parked.load(Ordering::Acquire) {
            let mut work = self.shared.delivery.callback_work();
            let wake = self
                .shared
                .cleanup_start(&mut work, WRITE)
                .and_then(|()| hub::wake(&self.shared));
            if let Err(error) = wake {
                self.shared.note_failure(error);
            }
        }
        drop(reads);
    }
}

impl Shared {
    pub(super) fn cleanup_pending(&self) -> bool {
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
        let mut result = Ok(());
        if !reads.cancelling {
            self.cleanup_start(work, WRITE)?;
            // SAFETY: the pending read owns both OVERLAPPED and buffer until completion.
            let cancelled = unsafe {
                CancelIoEx(
                    HANDLE(self.directory.load(Ordering::Acquire)),
                    Some(&raw const *reads.overlapped),
                )
            };
            result = match cancelled {
                Err(error) if error.code() != ERROR_NOT_FOUND.to_hresult() => {
                    return Err(io::Error::from(error).into());
                }
                _ => Ok(()),
            };
            reads.cancelling = true;
        }
        // The buffer stays owned until the cancelled read completes.
        if !self.await_completion(reads) {
            return result;
        }
        self.cleanup_start(work, READ)?;
        let mut transferred = 0;
        // SAFETY: the same pending storage stays pinned; it has completed.
        let completed = unsafe {
            GetOverlappedResult(
                HANDLE(self.directory.load(Ordering::Acquire)),
                &raw const *reads.overlapped,
                &mut transferred,
                false,
            )
        };
        reads.cancelling = false;
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
    pub(super) fn retire<W: SourceDemandReadWork>(
        &self,
        reads: &mut Reads,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = self.cancel_pending(reads, work);
        if reads.pending {
            return result;
        }
        if is_open(&self.directory) {
            result = result.and(
                self.cleanup_start(work, WRITE)
                    .and_then(|()| close(&self.directory)),
            );
        }
        if !is_open(&self.directory) {
            if let Some(buffer) = reads.buffer.take() {
                let bytes = size_of_val(&*buffer) as u64;
                drop(buffer);
                result = result.and(self.ownership.retire(bytes));
            }
            self.buffer_bytes.store(0, Ordering::Release);
            reads.filled = 0;
            reads.cursor = 0;
        }
        result
    }
    pub(super) fn retired(&self, reads: &Reads) -> bool {
        !reads.pending && reads.buffer.is_none() && !is_open(&self.directory)
    }
    /// Retires a native owner on its credits: the active one first, then,
    /// once its owner has dropped, the terminal one. An owner whose terminal
    /// attempt fails stays parked with its resources.
    pub(super) fn retire_native(&self, reads: &mut Reads) {
        if self.stop_completed.load(Ordering::Acquire) || self.parked.load(Ordering::Acquire) {
            return;
        }
        self.stopping.store(true, Ordering::Release);
        self.delivery.exact.store(false, Ordering::Release);
        let mut work = self.delivery.callback_work();
        loop {
            let terminal = !self.cleanup_pending();
            if terminal && !self.dropped.load(Ordering::Acquire) {
                // The owner's drop wakes the hub to retry.
                return;
            }
            let outcome = self.retire(reads, &mut work);
            if reads.pending && outcome.is_ok() {
                // The cancelled read's packet resumes this on the hub.
                return;
            }
            if let Err(error) = outcome {
                self.note_failure(error);
            }
            let retired = self.retired(reads);
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
            } else if terminal {
                self.parked.store(true, Ordering::Release);
            }
            if retired || terminal {
                return;
            }
        }
    }
}
fn check_wait(wait: Option<&crate::CancellationToken>) -> Result<(), DemandError> {
    wait.map_or(Ok(()), |wait| {
        wait.check().map_err(|_| DemandError::Cancelled)
    })
}
impl Shared {
    /// A native owner that is already stopping cannot stop again: it is
    /// busy while a credit remains, and out of native work once none does.
    fn refuse_restop(&self, native: bool) -> Result<(), DemandError> {
        if !native || !self.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(if self.cleanup_pending() {
            DemandError::WorkerUnavailable
        } else {
            DemandError::NativeWorkUnavailable
        })
    }
    /// The reads; under a cancellable wait, only if no one holds them.
    fn lock_reads_unless_waiting(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<ReadsGuard<'_>, DemandError> {
        if wait.is_none() {
            return Ok(self.lock_reads());
        }
        self.try_lock_reads().ok_or(DemandError::WorkerUnavailable)
    }
}
